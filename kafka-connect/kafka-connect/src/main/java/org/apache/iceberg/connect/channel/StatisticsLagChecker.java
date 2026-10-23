/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.channel;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.HealthTransition;
import org.apache.iceberg.connect.data.StatisticsColumns;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsRecords;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the tables of the statistics lag on a {@code commit()} of a request without responses,
 * alike in merge-on-read and copy-on-write: loads the table of the lag, telling a table that is
 * gone from a failure, and runs the quick check by the metadata of the head; a health re-check of a
 * table not in the lag loads and evaluates the same way. No data file is read.
 *
 * <p>The token of the lag and the version of the health record are the caller's, taken at the start
 * of {@code commit()} before the table is loaded: the lag is removed here only by that token, and
 * an evaluation is applied only if the record still has that version.
 */
class StatisticsLagChecker {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsLagChecker.class);

  /** What a committer adds to the check. */
  interface Committer {
    /** The table of the lag is gone from the catalog: whatever the committer keeps of it goes. */
    void tableGone(TableIdentifier id);

    /**
     * Whether the table of the lag, just loaded, goes on to the quick check; {@code false} keeps it
     * in the lag without a step.
     */
    boolean mayCheck(TableIdentifier id, Table table);

    /** Sets the lag step of a table verified and admitted in this cycle. */
    void setLagStep(TableCommitRequest request, Table table);
  }

  enum QuickCheck {
    /**
     * The lag is removed by the token: the branch has no head, or its head has records of every
     * kind on.
     */
    REMOVED,
    /** The lag stays without a step: the head is an intermediate slice of copy-on-write. */
    KEPT,
    /** The entry is verified: the head lacks a record of a kind on, so it needs a lag step. */
    VERIFIED
  }

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final StatisticsLag lag;
  private final StatisticsHealth health;

  StatisticsLagChecker(
      Catalog catalog, IcebergSinkConfig config, StatisticsLag lag, StatisticsHealth health) {
    this.catalog = catalog;
    this.config = config;
    this.lag = lag;
    this.health = health;
  }

  /**
   * Checks the table of a request without responses: a table in the lag gets the quick check and,
   * verified and admitted, the lag step; a table out of it gets a health re-check if one is
   * assigned in this cycle. Never throws.
   *
   * @param loaded the table if the committer has loaded it already, else {@code null}
   */
  void visit(
      TableCommitRequest request,
      Table loaded,
      long lagToken,
      long healthVersion,
      Committer committer) {
    TableIdentifier id = request.tableReference().identifier();
    try {
      if (lagToken != 0L) {
        visitLag(request, loaded, lagToken, healthVersion, committer);
      } else if (health.takeRecheck(id)) {
        recheck(id, healthVersion, committer::tableGone);
      }
    } catch (RuntimeException e) {
      LOG.warn("Cannot check the statistics lag of table {}", id, e);
    }
  }

  /**
   * The quick check and, verified and admitted, the lag step of a table of the lag the committer
   * has loaded for a commit of its own. Never throws.
   */
  void visitLoaded(
      TableCommitRequest request,
      Table table,
      long lagToken,
      long healthVersion,
      Committer committer) {
    TableIdentifier id = request.tableReference().identifier();
    try {
      checkLag(request, table, lagToken, healthVersion, lag.takeAdmission(id), committer);
    } catch (RuntimeException e) {
      LOG.warn("Cannot check the statistics lag of table {}", id, e);
    }
  }

  private void visitLag(
      TableCommitRequest request,
      Table loaded,
      long lagToken,
      long healthVersion,
      Committer committer) {
    TableIdentifier id = request.tableReference().identifier();
    // the first commit() of the table in the cycle spends its admission. A verified entry not
    // admitted waits for a later cycle without the table being loaded
    boolean admitted = lag.takeAdmission(id);
    if (!admitted && lag.isVerified(id)) {
      return;
    }

    Table table = loaded != null ? loaded : loadTable(id, lagToken, committer::tableGone);
    if (table == null || !committer.mayCheck(id, table)) {
      return;
    }

    checkLag(request, table, lagToken, healthVersion, admitted, committer);
  }

  private void checkLag(
      TableCommitRequest request,
      Table table,
      long lagToken,
      long healthVersion,
      boolean admitted,
      Committer committer) {
    TableIdentifier id = request.tableReference().identifier();
    if (quickCheck(id, table, lagToken, healthVersion) == QuickCheck.VERIFIED && admitted) {
      committer.setLagStep(request, table);
    }
  }

  /**
   * Loads the table of the lag. A table that is gone leaves the lag by the token; any other failure
   * keeps it.
   *
   * @return the table, or {@code null} if it did not load
   */
  Table loadTable(TableIdentifier id, long lagToken, Consumer<TableIdentifier> tableGone) {
    try {
      return catalog.loadTable(id);
    } catch (NoSuchTableException e) {
      LOG.debug("Table {} of the statistics lag is gone, it leaves the lag", id, e);
      lag.removeIfToken(id, lagToken);
      health.remove(id);
      tableGone.accept(id);
      return null;
    } catch (RuntimeException e) {
      if (lag.recordFailure(id)) {
        LOG.warn(
            "Cannot load table {} of the statistics lag, it stays in the lag and is tried again "
                + "in a later commit cycle",
            id,
            e);
      } else {
        LOG.debug("Cannot load table {} of the statistics lag again, it stays in the lag", id, e);
      }

      return null;
    }
  }

  /** The quick check by the metadata of the loaded table, after its health flags are evaluated. */
  QuickCheck quickCheck(TableIdentifier id, Table table, long lagToken, long healthVersion) {
    TableSinkConfig tableConfig = config.tableConfig(id.toString());
    String branch = tableConfig.commitBranch();
    evaluate(id, table, tableConfig, healthVersion);

    Snapshot head = SnapshotUtil.latestSnapshot(table, branch);
    if (head == null) {
      LOG.debug("Table {} has no head, it leaves the statistics lag", id);
      lag.removeIfToken(id, lagToken);
      return QuickCheck.REMOVED;
    }

    Map<String, String> summary = head.summary();
    if (summary != null && summary.containsKey(ChangeSetStore.CHANGE_SET_ID_PROP)) {
      LOG.debug(
          "Head {} of table {} is an intermediate slice of copy-on-write, the table stays in the "
              + "statistics lag without a step",
          head.snapshotId(),
          id);
      return QuickCheck.KEPT;
    }

    if (hasEveryKindOn(id, table, head, tableConfig)) {
      LOG.debug(
          "Head {} of table {} has statistics of every kind on, the table leaves the statistics "
              + "lag without a step",
          head.snapshotId(),
          id);
      lag.removeIfToken(id, lagToken);
      return QuickCheck.REMOVED;
    }

    lag.markVerified(id);
    return QuickCheck.VERIFIED;
  }

  /** The health re-check of a table not in the lag: loads it and evaluates its flags. */
  void recheck(TableIdentifier id, long healthVersion, Consumer<TableIdentifier> tableGone) {
    Table table;
    try {
      table = catalog.loadTable(id);
    } catch (NoSuchTableException e) {
      LOG.debug("Table {} to re-check the statistics health of is gone", id, e);
      health.remove(id);
      tableGone.accept(id);
      return;
    } catch (RuntimeException e) {
      LOG.debug("Cannot load table {} to re-check its statistics health", id, e);
      return;
    }

    evaluate(id, table, config.tableConfig(id.toString()), healthVersion);
  }

  /**
   * Evaluates the health flags by the head and applies them only if the record still has {@code
   * healthVersion}: a step that ended since the start of {@code commit()} saw a newer table. Never
   * throws.
   */
  private void evaluate(
      TableIdentifier id, Table table, TableSinkConfig tableConfig, long healthVersion) {
    try {
      String branch = tableConfig.commitBranch();
      HealthTransition transition =
          HealthTransition.fromHead(id, table, branch, config, tableConfig);
      Flags previous = health.updateIfUnchanged(id, healthVersion, transition);
      if (previous == null) {
        LOG.debug(
            "Statistics health of table {} changed while its head was evaluated, the evaluation "
                + "is dropped",
            id);
        return;
      }

      Snapshot head = SnapshotUtil.latestSnapshot(table, branch);
      StatisticsHealth.logTransitions(
          id,
          head == null ? null : head.snapshotId(),
          previous,
          transition.apply(previous),
          transition.details());
    } catch (RuntimeException e) {
      LOG.warn("Cannot evaluate the statistics health of table {} by its head", id, e);
    }
  }

  /** Whether the head has a record of every kind on; with no kind on, it has. */
  private boolean hasEveryKindOn(
      TableIdentifier id, Table table, Snapshot head, TableSinkConfig tableConfig) {
    long headId = head.snapshotId();
    boolean partitionsMissing =
        tableConfig.partitionStatsEnabled()
            && Partitioning.isPartitioned(table)
            && !StatisticsRecords.hasPartitionStatistics(table, headId);
    if (partitionsMissing) {
      return false;
    }

    return StatisticsRecords.hasNdv(table, headId)
        || StatisticsColumns.resolve(
                id,
                SnapshotUtil.schemaFor(table, headId),
                tableConfig.statisticsColumns(),
                config.schemaCaseInsensitive())
            .isEmpty();
  }
}
