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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base and the gap of the NDV of snapshot S: the walk goes from S itself along the parents,
 * skipping the own snapshots O, to the nearest not own snapshot with a statistics record that
 * describes the snapshot itself, or to the empty point behind a gap of the connector's commits made
 * with statistics enabled.
 *
 * <p>Only metadata is read.
 */
public class NdvChain {
  private static final Logger LOG = LoggerFactory.getLogger(NdvChain.class);

  /** Summary property of a data commit the connector made with statistics enabled. */
  public static final String STATISTICS_ENABLED_PROP = "kafka.connect.statistics-enabled";

  // the change set of a copy-on-write slice (ChangeSetStore.CHANGE_SET_ID_PROP)
  static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";

  public enum Kind {
    /** The nearest not own snapshot with a statistics record of its own: {@link #base()}. */
    BASE,
    /** The sketches start empty: the chain reached an empty table behind a marked gap. */
    EMPTY_BASE,
    /** The chain broke before a base: no statistics to build on. */
    NO_BASELINE
  }

  private final Kind kind;
  private final StatisticsFile base;
  private final Schema baseSchema;
  private final List<Snapshot> gap;
  private final List<StatisticsAnswer> answers;

  private NdvChain(
      Kind kind,
      StatisticsFile base,
      Schema baseSchema,
      List<Snapshot> gap,
      List<StatisticsAnswer> answers) {
    this.kind = kind;
    this.base = base;
    this.baseSchema = baseSchema;
    this.gap = ImmutableList.copyOf(gap);
    this.answers = answers;
  }

  /**
   * Finds the base and the gap of the NDV of snapshot S.
   *
   * @param ownChangeSetIds change sets whose slices are own snapshots (copy-on-write)
   */
  public static NdvChain resolve(
      Table table,
      long snapshotId,
      Set<Long> ownSnapshotIds,
      Set<String> ownChangeSetIds,
      List<StatisticsAnswer> answers) {
    return resolve(LOG, table, snapshotId, ownSnapshotIds, ownChangeSetIds, answers);
  }

  static NdvChain resolve(
      Logger logger,
      Table table,
      long snapshotId,
      Set<Long> ownSnapshotIds,
      Set<String> ownChangeSetIds,
      List<StatisticsAnswer> answers) {
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(
        snapshot != null, "Cannot find snapshot %s of table %s", snapshotId, table.name());

    // own snapshots of a rolled back branch are not in the chain of S, nor are their rows
    Set<Long> chain = Sets.newHashSet(SnapshotUtil.ancestorIds(snapshot, table::snapshot));
    Set<Long> own = ownSnapshotIds.stream().filter(chain::contains).collect(Collectors.toSet());
    List<StatisticsAnswer> kept =
        answers.stream()
            .filter(answer -> own.contains(answer.snapshotId()))
            .collect(ImmutableList.toImmutableList());

    Map<Long, StatisticsFile> records = Maps.newHashMap();
    for (StatisticsFile file : table.statisticsFiles()) {
      records.putIfAbsent(file.snapshotId(), file);
    }

    List<Snapshot> gap = Lists.newArrayList();
    int gapSnapshots = 0;
    boolean marked = true; // every snapshot of the gap but a replace carries the marker
    Snapshot current = snapshot;
    while (true) {
      if (!isOwn(current, own, ownChangeSetIds)) {
        StatisticsFile record = recordOf(table, records, current);
        if (record != null) {
          Schema baseSchema = SnapshotUtil.schemaFor(table, current.snapshotId());
          return new NdvChain(Kind.BASE, record, baseSchema, gap, kept);
        } else if (marked && totalDataFiles(current) == 0L) {
          return emptyBase(logger, table, snapshotId, gap, gapSnapshots, kept);
        }

        gapSnapshots += 1;
        if (!DataOperations.REPLACE.equals(current.operation())) {
          gap.add(current);
          marked = marked && isMarked(current);
        }
      }

      Long parentId = current.parentId();
      if (parentId == null) {
        return marked
            ? emptyBase(logger, table, snapshotId, gap, gapSnapshots, kept)
            : new NdvChain(Kind.NO_BASELINE, null, null, gap, kept);
      }

      Snapshot parent = table.snapshot(parentId);
      if (parent == null) {
        // expired: whatever the gap, what is behind it is unknown
        return new NdvChain(Kind.NO_BASELINE, null, null, gap, kept);
      }

      current = parent;
    }
  }

  private static NdvChain emptyBase(
      Logger logger,
      Table table,
      long snapshotId,
      List<Snapshot> gap,
      int gapSnapshots,
      List<StatisticsAnswer> answers) {
    if (gapSnapshots > 0) {
      long files = gap.stream().mapToLong(NdvChain::addedDataFiles).sum();
      logger.info(
          "Statistics of table {} at snapshot {} start from an empty table behind {} snapshots "
              + "({} data files) the connector committed with statistics enabled: a failed start "
              + "of statistics is recovered",
          table.name(),
          snapshotId,
          gapSnapshots,
          files);
    }

    return new NdvChain(Kind.EMPTY_BASE, null, null, gap, answers);
  }

  /** The statistics record of the snapshot, {@code null} if none describes the snapshot itself. */
  private static StatisticsFile recordOf(
      Table table, Map<Long, StatisticsFile> records, Snapshot snapshot) {
    StatisticsFile record = records.get(snapshot.snapshotId());
    return record != null && describes(table, record, snapshot) ? record : null;
  }

  /**
   * Whether the record describes the snapshot it is attached to: every theta sketch blob was built
   * at the snapshot itself, or no snapshot from the source of the blob (exclusive) to the snapshot
   * (inclusive) added values. Trino copies the blobs of the nearest record with statistics to a new
   * snapshot (OPTIMIZE, a partial ANALYZE), so a copy over a foreign append misses its values.
   */
  static boolean describes(Table table, StatisticsFile record, Snapshot snapshot) {
    Set<Long> sources = Sets.newHashSet();
    for (BlobMetadata blob : record.blobMetadata()) {
      if (NdvBaseline.thetaFieldId(blob.type(), blob.fields()) != null
          && blob.sourceSnapshotId() != snapshot.snapshotId()) {
        sources.add(blob.sourceSnapshotId());
      }
    }

    for (long source : sources) {
      if (!noValuesAddedSince(table, source, snapshot)) {
        return false;
      }
    }

    return true;
  }

  private static boolean noValuesAddedSince(Table table, long sourceId, Snapshot snapshot) {
    Snapshot current = snapshot;
    while (current.snapshotId() != sourceId) {
      if (!DataOperations.REPLACE.equals(current.operation()) && addedDataFilesOf(current)) {
        return false;
      }

      Long parentId = current.parentId();
      current = parentId == null ? null : table.snapshot(parentId);
      if (current == null) {
        return false; // the source is not in the chain, or expired
      }
    }

    return true;
  }

  private static boolean addedDataFilesOf(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    if (summary != null && !summary.containsKey(SnapshotSummary.ADDED_FILES_PROP)) {
      return totalDataFiles(snapshot) < 0L;
    }

    return summaryLong(snapshot, SnapshotSummary.ADDED_FILES_PROP) != 0L;
  }

  private static boolean isOwn(Snapshot snapshot, Set<Long> own, Set<String> ownChangeSetIds) {
    if (own.contains(snapshot.snapshotId())) {
      return true;
    }

    Map<String, String> summary = snapshot.summary();
    String changeSetId = summary == null ? null : summary.get(CHANGE_SET_ID_PROP);
    return changeSetId != null && ownChangeSetIds.contains(changeSetId);
  }

  private static boolean isMarked(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    return summary != null && Boolean.parseBoolean(summary.get(STATISTICS_ENABLED_PROP));
  }

  private static long totalDataFiles(Snapshot snapshot) {
    return summaryLong(snapshot, SnapshotSummary.TOTAL_DATA_FILES_PROP);
  }

  private static long addedDataFiles(Snapshot snapshot) {
    return Math.max(0L, summaryLong(snapshot, SnapshotSummary.ADDED_FILES_PROP));
  }

  /** The value of a numeric summary property, or -1 when it is missing or not a number. */
  private static long summaryLong(Snapshot snapshot, String property) {
    Map<String, String> summary = snapshot.summary();
    String value = summary == null ? null : summary.get(property);
    if (value == null) {
      return -1L;
    }

    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  public Kind kind() {
    return kind;
  }

  /** The statistics record of the base, {@code null} unless {@link Kind#BASE}. */
  public StatisticsFile base() {
    return base;
  }

  /** The schema of the base snapshot, {@code null} unless {@link Kind#BASE}. */
  public Schema baseSchema() {
    return baseSchema;
  }

  /**
   * The snapshots of the gap from S towards the base, without {@code replace} snapshots; meaningful
   * for {@link Kind#BASE} and {@link Kind#EMPTY_BASE}.
   */
  public List<Snapshot> gap() {
    return gap;
  }

  /** The answers of the own snapshots that are ancestors of S (S included). */
  public List<StatisticsAnswer> answers() {
    return answers;
  }
}
