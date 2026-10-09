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
package org.apache.iceberg.connect.data;

import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.StatisticsRetention.Removal;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;

/**
 * The transaction of a statistics step: the new records of snapshot S together with the removal of
 * the records beyond the last N, then the deletion of the files of the removed records.
 */
final class StatisticsCommit {
  enum Status {
    COMMITTED,
    FAILED,
    STOPPED
  }

  private final Table table;
  private final TableIdentifier identifier;
  private final long snapshotId;
  private final StepFileTracker files;
  private final BooleanSupplier stopped;
  private final Logger log;
  private RuntimeException failure = null;

  StatisticsCommit(
      Table table,
      TableIdentifier identifier,
      long snapshotId,
      StepFileTracker files,
      BooleanSupplier stopped,
      Logger log) {
    this.table = table;
    this.identifier = identifier;
    this.snapshotId = snapshotId;
    this.files = files;
    this.stopped = stopped;
    this.log = log;
  }

  /** Commits the records of the files that are not {@code null}. */
  Status commit(
      StatisticsFile ndvFile, PartitionStatisticsFile partitionFile, int retainSnapshots) {
    boolean withNdv = ndvFile != null;
    boolean withPartitions = partitionFile != null;
    Removal removal =
        StatisticsRetention.plan(table, snapshotId, retainSnapshots)
            .limitedTo(withNdv, withPartitions);
    Transaction transaction = table.newTransaction();
    List<String> handedOver = Lists.newArrayList();
    if (withNdv) {
      transaction.updateStatistics().setStatistics(ndvFile).commit();
      handedOver.add(ndvFile.path());
    }

    if (withPartitions) {
      transaction.updatePartitionStatistics().setPartitionStatistics(partitionFile).commit();
      handedOver.add(partitionFile.path());
    }

    StatisticsRetention.apply(transaction, removal);
    if (stopped()) {
      return Status.STOPPED;
    }

    files.handOver(handedOver);
    try {
      transaction.commitTransaction();
    } catch (RuntimeException e) {
      if (stopped()) {
        log.debug("Statistics commit of table {} stopped", identifier, e);
        return Status.STOPPED;
      }

      if (knownFailure(table, e)) {
        this.failure = e;
        for (String path : handedOver) {
          files.delete(path, "the statistics file of the failed commit");
        }

        return Status.FAILED;
      }

      if (!confirmed(ndvFile, partitionFile, e)) {
        return unconfirmed(e);
      }
    }

    deleteRemoved(removal);
    return Status.COMMITTED;
  }

  /** The failure of a {@link Status#FAILED} commit; {@code null} otherwise. */
  RuntimeException failure() {
    return failure;
  }

  /** The catalog may have applied an unconfirmed commit, so its files stay. */
  private Status unconfirmed(RuntimeException unknown) {
    if (stopped()) {
      log.debug(
          "Statistics commit of table {} stopped after an unknown outcome", identifier, unknown);
      return Status.STOPPED;
    }

    this.failure = unknown;
    return Status.FAILED;
  }

  /** After an unknown outcome: whether the records of S point to the files of the step. */
  private boolean confirmed(
      StatisticsFile ndvFile, PartitionStatisticsFile partitionFile, RuntimeException unknown) {
    try {
      table.refresh();
    } catch (RuntimeException e) {
      unknown.addSuppressed(e);
      return false;
    }

    boolean ndvThere =
        ndvFile != null
            && table.statisticsFiles().stream()
                .anyMatch(
                    file -> file.snapshotId() == snapshotId && file.path().equals(ndvFile.path()));
    boolean partitionsThere =
        partitionFile != null && PartitionStatisticsWriter.isRegistered(table, partitionFile);
    if (ndvThere || partitionsThere) {
      log.debug(
          "Statistics commit of table {} at snapshot {} had an unknown outcome and is applied",
          identifier,
          snapshotId,
          unknown);
      return true;
    }

    return false;
  }

  /** Deletes the files of the removed records that the committed metadata does not refer to. */
  private void deleteRemoved(Removal removal) {
    if (removal.isEmpty() || stopped()) {
      return;
    }

    Set<String> paths;
    try {
      table.refresh();
      paths =
          StatisticsRetention.filesToDelete(
              ((HasTableOperations) table).operations().current(), removal);
    } catch (RuntimeException e) {
      if (stopped()) {
        log.debug(
            "Search for the files of the removed statistics records of table {} stopped",
            identifier,
            e);
        return;
      }

      log.warn(
          "Cannot find the files of the statistics records removed from table {} at snapshot {}",
          identifier,
          snapshotId,
          e);
      return;
    }

    for (String path : paths) {
      files.delete(path, "the statistics file of a removed record");
    }
  }

  private boolean stopped() {
    return stopped.getAsBoolean();
  }

  /**
   * Whether the commit is known to have failed, decided as {@code BaseTransaction} decides on
   * cleanup. Only the exception itself counts, not its causes; anything else, a lost response of
   * the catalog included, may have been applied.
   */
  private static boolean knownFailure(Table table, RuntimeException failure) {
    if (failure instanceof CommitStateUnknownException) {
      return false;
    }

    if (failure instanceof CleanableFailure) {
      return true;
    }

    if (table instanceof HasTableOperations) {
      TableOperations operations = ((HasTableOperations) table).operations();
      return !operations.requireStrictCleanup();
    }

    return false;
  }
}
