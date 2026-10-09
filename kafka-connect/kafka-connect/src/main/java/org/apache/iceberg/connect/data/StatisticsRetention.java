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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionStatistics;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The N last statistics records of the branch chain: which records the step takes off in its
 * transaction and which files may be deleted after the commit.
 *
 * <p>The chain is the ancestors of the snapshot S the step writes for, S first. N is counted by the
 * records of each kind (NDV and partition statistics separately): S is always the first of N,
 * snapshots without a record of the kind are skipped, and the snapshots that are the head of a ref
 * other than S are neither counted nor removed. The author of a record does not matter.
 *
 * <p>The class deletes no files.
 */
public final class StatisticsRetention {
  private StatisticsRetention() {}

  /**
   * Plans the records to take off for S, by the table as of this call. If Iceberg retries the
   * transaction on a concurrent commit, the records are taken off again without looking at the
   * heads of the refs: a tag created in between on a snapshot of the plan loses its record.
   *
   * @param retainSnapshots N: -1 or a positive number; -1 removes nothing
   */
  public static Removal plan(Table table, long snapshotId, int retainSnapshots) {
    Preconditions.checkArgument(
        retainSnapshots == -1 || retainSnapshots >= 1,
        "Invalid number of statistics records to retain: %s",
        retainSnapshots);
    if (retainSnapshots == -1) {
      return Removal.NONE;
    }

    Set<Long> heads = headsOfOtherRefs(table, snapshotId);
    Map<Long, String> statistics = Maps.newHashMap();
    for (StatisticsFile file : table.statisticsFiles()) {
      statistics.put(file.snapshotId(), file.path());
    }

    Map<Long, String> partitionStatistics = Maps.newHashMap();
    for (PartitionStatisticsFile file : table.partitionStatisticsFiles()) {
      partitionStatistics.put(file.snapshotId(), file.path());
    }

    Window statisticsWindow = new Window(retainSnapshots);
    Window partitionWindow = new Window(retainSnapshots);
    Snapshot current = table.snapshot(snapshotId);
    while (current != null) {
      long id = current.snapshotId();
      if (id != snapshotId && !heads.contains(id)) {
        statisticsWindow.visit(id, statistics.get(id));
        partitionWindow.visit(id, partitionStatistics.get(id));
      }

      current = current.parentId() == null ? null : table.snapshot(current.parentId());
    }

    return new Removal(statisticsWindow.excess(), partitionWindow.excess());
  }

  /** Takes the planned records off in the transaction. */
  public static void apply(Transaction transaction, Removal removal) {
    if (!removal.statistics.isEmpty()) {
      UpdateStatistics update = transaction.updateStatistics();
      removal.statistics.keySet().forEach(update::removeStatistics);
      update.commit();
    }

    if (!removal.partitionStatistics.isEmpty()) {
      UpdatePartitionStatistics update = transaction.updatePartitionStatistics();
      removal.partitionStatistics.keySet().forEach(update::removePartitionStatistics);
      update.commit();
    }
  }

  /**
   * The paths of the removed records that no record of the committed metadata refers to; empty
   * while the metadata still has the records, as when the removal was not committed.
   *
   * @param committed the metadata read after the commit
   */
  public static Set<String> filesToDelete(TableMetadata committed, Removal removal) {
    Set<String> referenced = Sets.newHashSet();
    for (StatisticsFile file : committed.statisticsFiles()) {
      referenced.add(file.path());
    }

    for (PartitionStatisticsFile file : committed.partitionStatisticsFiles()) {
      referenced.add(file.path());
    }

    Set<String> files = Sets.newLinkedHashSet();
    files.addAll(removal.statistics.values());
    files.addAll(removal.partitionStatistics.values());
    files.removeAll(referenced);
    return files;
  }

  private static Set<Long> headsOfOtherRefs(Table table, long snapshotId) {
    Set<Long> heads = Sets.newHashSet();
    for (SnapshotRef ref : table.refs().values()) {
      if (ref.snapshotId() != snapshotId) {
        heads.add(ref.snapshotId());
      }
    }

    return heads;
  }

  /** Counts the N records of one kind and collects the excess. */
  private static final class Window {
    private final int retain;
    private final ImmutableMap.Builder<Long, String> excess = ImmutableMap.builder();
    // S counts first: the step adds its record in the same transaction
    private int kept = 1;

    private Window(int retain) {
      this.retain = retain;
    }

    private void visit(long snapshotId, String path) {
      if (path == null) {
        return;
      }

      if (kept < retain) {
        kept += 1;
      } else {
        excess.put(snapshotId, path);
      }
    }

    private ImmutableMap<Long, String> excess() {
      return excess.build();
    }
  }

  /** The records taken off for one step: the snapshot id and the path of the file of each. */
  public static final class Removal {
    static final Removal NONE = new Removal(ImmutableMap.of(), ImmutableMap.of());

    private final ImmutableMap<Long, String> statistics;
    private final ImmutableMap<Long, String> partitionStatistics;

    private Removal(
        ImmutableMap<Long, String> statistics, ImmutableMap<Long, String> partitionStatistics) {
      this.statistics = statistics;
      this.partitionStatistics = partitionStatistics;
    }

    public boolean isEmpty() {
      return statistics.isEmpty() && partitionStatistics.isEmpty();
    }

    /** Snapshots whose NDV record is taken off, the newest first. */
    public Set<Long> statisticsSnapshotIds() {
      return statistics.keySet();
    }

    /** Snapshots whose partition statistics record is taken off, the newest first. */
    public Set<Long> partitionStatisticsSnapshotIds() {
      return partitionStatistics.keySet();
    }

    /**
     * The part of the removal for the kinds written: records are taken off only together with a new
     * record of the same kind.
     */
    public Removal limitedTo(boolean ndv, boolean partitions) {
      return new Removal(
          ndv ? statistics : ImmutableMap.of(),
          partitions ? partitionStatistics : ImmutableMap.of());
    }

    @Override
    public String toString() {
      return "Removal{statistics="
          + statistics.keySet()
          + ", partitionStatistics="
          + partitionStatistics.keySet()
          + "}";
    }
  }
}
