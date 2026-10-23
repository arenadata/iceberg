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
package org.apache.iceberg.connect;

import static org.apache.iceberg.connect.StatisticsItSupport.METRIC_STALE;
import static org.apache.iceberg.connect.StatisticsItSupport.assertFinalFile;
import static org.apache.iceberg.connect.StatisticsItSupport.assertNdvWithinBounds;
import static org.apache.iceberg.connect.StatisticsItSupport.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsItSupport.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitConnectLog;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitLanded;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitMetric;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitNoMetrics;
import static org.apache.iceberg.connect.StatisticsItSupport.blobOf;
import static org.apache.iceberg.connect.StatisticsItSupport.changeSetSketchFiles;
import static org.apache.iceberg.connect.StatisticsItSupport.defaultStagingLocation;
import static org.apache.iceberg.connect.StatisticsItSupport.filesUnder;
import static org.apache.iceberg.connect.StatisticsItSupport.ndv;
import static org.apache.iceberg.connect.StatisticsItSupport.plantOrphanPuffin;
import static org.apache.iceberg.connect.StatisticsItSupport.recounted;
import static org.apache.iceberg.connect.StatisticsItSupport.stagedPuffins;
import static org.apache.iceberg.connect.StatisticsItSupport.stateCleared;
import static org.apache.iceberg.connect.StatisticsItSupport.stateRaised;
import static org.apache.iceberg.connect.StatisticsItSupport.statisticsFile;
import static org.apache.iceberg.connect.StatisticsItSupport.trinoAnalyze;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.StatisticsItSupport.Kind;
import org.apache.iceberg.connect.StatisticsItSupport.Row;
import org.apache.iceberg.connect.StatisticsItSupport.SentValues;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Integration tests of the statistics of the sink in copy-on-write: a drain of several slices,
 * copy-on-write to merge-on-read and back, the state {@code stale}, the staging settings and the
 * default staging location.
 *
 * <p>A drain commits a snapshot for every slice of the change set; only the last one, the final
 * snapshot, gets the statistics, because a record of an intermediate slice would describe the table
 * in the middle of a rewrite.
 */
public class TestIntegrationStatisticsCopyOnWrite extends StatisticsTestBase {

  private static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";
  private static final Duration RECHECK_TIMEOUT = Duration.ofSeconds(30);
  // the TTL of the orphans is 20 s; the connector starts, and the table reaches the cleanup at the
  // first cycle, which waits the commit timeout of 10 s
  private static final Duration CLEANUP_TIMEOUT = Duration.ofSeconds(90);

  private static final List<String> COLUMNS = ImmutableList.of("id", "name", "amount");
  private static final Schema SIMPLE =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(3, "amount", Types.IntegerType.get())),
          ImmutableSet.of(1));

  @Override
  protected String tablePrefix() {
    return "stats_cow";
  }

  @Test
  public void testDrainOfSeveralSlicesWritesStatisticsOnlyAtFinalSnapshot()
      throws InterruptedException {
    Table table = catalog().createTable(tableId(), SIMPLE);
    startConnector(copyOnWrite(ImmutableMap.of("iceberg.tables.copy-on-write.max-slice-keys", 2)));

    SentValues sent = new SentValues();
    List<Row> inserts = Lists.newArrayList();
    for (int id = 1; id <= 10; id++) {
      inserts.add(cdcRow("c", id, "a" + id, id % 5));
    }

    inserts.forEach(row -> sent.add(row.fields()));
    sendRows(inserts);
    long insertSnapshotId = awaitNames(table, namesOf(inserts));
    awaitStats(table, insertSnapshotId, Kind.NDV);

    // eight keys get new versions, two are deleted: the old values of the rows are in no live row
    List<Row> changes = Lists.newArrayList();
    Map<Long, String> expected = Maps.newHashMap();
    for (int id = 1; id <= 8; id++) {
      Row update = cdcRow("u", id, "b" + id, 10 + id % 5);
      sent.add(update.fields());
      changes.add(update);
      expected.put((long) id, "b" + id);
    }

    changes.add(cdcRow("d", 9, "a9", 9 % 5));
    changes.add(cdcRow("d", 10, "a10", 10 % 5));
    sendRowsToOnePartition(changes);
    long finalSnapshotId = awaitNames(table, expected);

    // the change set was cut into slices: the snapshots of the drain but the last carry its id
    List<Snapshot> drain = drainSnapshots(table, finalSnapshotId, insertSnapshotId);
    assertThat(drain).hasSizeGreaterThanOrEqualTo(2);
    List<Snapshot> slices = slicesOf(drain);
    assertThat(slices).isNotEmpty();

    awaitStats(table, finalSnapshotId, Kind.NDV);
    StatisticsFile file = assertFinalFile(table, finalSnapshotId, COLUMNS);
    assertThat(slices)
        .as("an intermediate slice is a table in the middle of a rewrite: no record")
        .allSatisfy(
            snapshot ->
                assertThat(statisticsFile(table, snapshot.snapshotId()))
                    .as("record of the slice %s", snapshot.snapshotId())
                    .isNull());
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);

    Map<String, Long> live = assertNdvWithinBounds(table, tableId(), finalSnapshotId, file, sent);
    // fewer live values than sent: the deleted rows and the old versions of the updated ones
    // count in the sketch only
    assertThat(live.get("id")).isEqualTo(8L);
    assertThat(sent.distinct("id")).isEqualTo(10L);
    assertThat(live.get("name")).isEqualTo(8L);
    assertThat(sent.distinct("name")).isEqualTo(18L);
    assertThat(live.get("amount")).isEqualTo(5L);
    assertThat(sent.distinct("amount")).isEqualTo(10L);
  }

  @Test
  public void testRewriteOfThirdOfRowsRaisesStaleUntilTrinoAnalyze() throws InterruptedException {
    Table table = catalog().createTable(tableId(), SIMPLE);
    startConnector(
        copyOnWrite(
            ImmutableMap.of(
                "iceberg.tables.statistics.stale-deleted-ratio",
                0.2,
                "iceberg.tables.statistics.health-recheck-interval-ms",
                3000)));

    SentValues sent = new SentValues();
    long snapshotId = rewriteThirdOfRows(table, sent);

    awaitMetric(connectorName(), METRIC_STALE, 1);
    // the WARN names the table and the snapshot
    awaitConnectLog(
        testStart(),
        stateRaised("stale", tableId(), snapshotId),
        "the state stale is raised with the table and the snapshot");
    StatisticsFile file = assertFinalFile(table, snapshotId, COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    // the old versions of the rewritten rows are in the sketch, but the live rows cap the ndv
    Map<String, Long> live = assertNdvWithinBounds(table, tableId(), snapshotId, file, sent);
    assertThat(live.get("name")).isEqualTo(20L);
    assertThat(sent.distinct("name")).isEqualTo(26L);
    Schema schema = table.schemas().get(table.snapshot(snapshotId).schemaId());
    assertThat(ndv(blobOf(file, schema, "name")))
        .as("the ndv never exceeds the live rows")
        .isEqualTo(20L);

    // Trino recounts the statistics of the table: the sketch is exact again
    trinoAnalyze(tableId());
    awaitMetric(connectorName(), METRIC_STALE, 0, RECHECK_TIMEOUT);
    awaitConnectLog(
        testStart(),
        stateCleared("stale", tableId()),
        "the state stale is cleared after ANALYZE",
        RECHECK_TIMEOUT);

    // the next commit goes on from the record of Trino and the state stays cleared: the values
    // that are sent count from the rows alive at ANALYZE
    SentValues fromAnalyze = liveValues(table);
    Row update = cdcRow("u", 7, "c7", 200);
    fromAnalyze.add(update.fields());
    sendRowsToOnePartition(ImmutableList.of(update));
    long nextSnapshotId =
        awaitNames(table, namesAfter(namesOfLive(table), ImmutableList.of(update)));
    awaitStats(table, nextSnapshotId, Kind.NDV);
    StatisticsFile next = assertFinalFile(table, nextSnapshotId, COLUMNS);
    assertNdvWithinBounds(table, tableId(), nextSnapshotId, next, fromAnalyze);
    awaitMetric(connectorName(), METRIC_STALE, 0);
  }

  @Test
  public void testStaleStateIsRaisedAgainAfterRestartWithNoNewData() {
    Table table = catalog().createTable(tableId(), SIMPLE);
    Map<String, Object> settings = copyOnWrite(ImmutableMap.of());
    startConnector(settings);

    long snapshotId = rewriteThirdOfRows(table, new SentValues());
    awaitMetric(connectorName(), METRIC_STALE, 1);

    // the states are in memory: a restart starts with none, and the table is quiet
    context().stopConnector(connectorName());
    awaitNoMetrics(connectorName());
    Instant restart = Instant.now().minusSeconds(2);
    startConnector(settings);

    awaitMetric(connectorName(), METRIC_STALE, 1);
    awaitConnectLog(
        restart,
        stateRaised("stale", tableId(), snapshotId),
        "the state stale is raised again at the head after the restart");
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("the restarted connector commits no data")
        .isEqualTo(snapshotId);
  }

  /**
   * A drain stopped in the middle, the connector switched to merge-on-read and back: merge-on-read
   * cleans up the rejected table but keeps every file of the change set; once the storage loses the
   * Puffin files of the answers, copy-on-write ends the drain and the step of the final snapshot
   * recounts the answers from their staged files.
   */
  @Test
  public void testSwitchToMergeOnReadAndBackFinishesDrainWithStatistics()
      throws InterruptedException {
    Table table = catalog().createTable(tableId(), SIMPLE);
    // one key to a slice, so that the drain outlasts the stop of the connector; the TTL is the
    // least that the check at start lets through (it exceeds the interval plus the timeout) and the
    // cleanup of the orphans runs every two seconds; the names are those of the staging settings
    Map<String, Object> settings =
        ImmutableMap.of(
            "iceberg.tables.copy-on-write.max-slice-keys", 1,
            "iceberg.control.commit.timeout-ms", 10_000,
            "iceberg.tables.staging-orphan-ttl-ms", 20_000,
            "iceberg.tables.staging-orphan-cleanup-interval-ms", 2_000);
    int keys = 80;
    startConnector(copyOnWrite(settings));

    SentValues sent = new SentValues();
    List<Row> inserts = Lists.newArrayList();
    for (int id = 1; id <= keys; id++) {
      inserts.add(cdcRow("c", id, "a" + id, id % 4));
    }

    inserts.forEach(row -> sent.add(row.fields()));
    sendRows(inserts);
    long insertSnapshotId = awaitNames(table, namesOf(inserts));
    awaitStats(table, insertSnapshotId, Kind.NDV);

    List<Row> updates = Lists.newArrayList();
    for (int id = 1; id <= keys; id++) {
      Row update = cdcRow("u", id, "b" + id, 10 + id % 4);
      sent.add(update.fields());
      updates.add(update);
    }

    sendRowsToOnePartition(updates);
    awaitIntermediateSlice(table, insertSnapshotId);

    // the connector stops after the first slice: the change set is not applied to the end
    context().stopConnector(connectorName());
    awaitNoMetrics(connectorName());
    table.refresh();
    Snapshot unfinished = table.currentSnapshot();
    assertThat(unfinished.summary())
        .as(
            "the head is a slice with the id of the change set (if the drain finished before the"
                + " connector stopped: raise the number of keys)")
        .containsKey(CHANGE_SET_ID_PROP);
    assertThat(statisticsFile(table, unfinished.snapshotId())).isNull();

    // the Puffin files of the answers that the change set froze lie under stats/
    List<String> sketchFiles =
        changeSetSketchFiles(table, target(), unfinished.summary().get(CHANGE_SET_ID_PROP));
    assertThat(sketchFiles).as("Puffin files of the answers in the manifest").isNotEmpty();
    assertThat(stagedPuffins(table, target()))
        .as("the Puffin files of the answers are under stats/")
        .containsAll(sketchFiles);

    // the staging of the change set is under the default location: all the files of the
    // connector outside stats/, the manifest among them
    Set<String> changeSetFiles =
        Sets.difference(
            Sets.newHashSet(filesUnder(table, defaultStagingLocation(table))),
            Sets.newHashSet(stagedPuffins(table, target())));
    assertThat(changeSetFiles)
        .as("staged files of the change set under %s", defaultStagingLocation(table))
        .isNotEmpty();

    // merge-on-read: the table is rejected, because the answers of the copy-on-write cycles come to
    // the new coordinator again, and the drain is not applied. The orphan planted now is older
    // than the TTL after 20 s: a cleanup that deletes it is the proof that the pass runs on a
    // rejected table
    String orphan = plantOrphanPuffin(table, target());
    startConnector(mergeOnRead(settings));
    awaitOrphanCleanedUp(table, orphan);

    // the same pass leaves the Puffin files of the answers of the change set, although they are
    // older than the orphan: the answers are in the commit buffer of the rejected table, and what a
    // buffered answer names is live. The other files of the change set stay as well, and the head
    // does not move
    assertMergeOnReadLeavesDrain(table, sketchFiles, changeSetFiles, unfinished.snapshotId());

    // the storage loses the Puffin files of the answers before the drain goes on (a lifecycle rule
    // of the bucket: the staging is temporary): the answers then have no sketch to merge
    context().stopConnector(connectorName());
    awaitNoMetrics(connectorName());
    sketchFiles.forEach(location -> table.io().deleteFile(location));

    // back in copy-on-write the drain goes on, the data are whole, and the final snapshot
    // has its statistics
    startConnector(copyOnWrite(settings));
    Map<Long, String> expected = Maps.newHashMap();
    updates.forEach(update -> expected.put(update.id(), "b" + update.id()));
    long finalSnapshotId = awaitNames(table, expected);

    List<Snapshot> drain = drainSnapshots(table, finalSnapshotId, insertSnapshotId);
    assertThat(drain).hasSizeGreaterThanOrEqualTo(2);
    awaitStats(table, finalSnapshotId, Kind.NDV);
    // the answers have no Puffin file, so the step recounts them from the staged files
    awaitConnectLog(
        testStart(),
        recounted(tableId(), finalSnapshotId),
        "the step of the final snapshot read data files to recount the answers without a Puffin");
    StatisticsFile file = assertFinalFile(table, finalSnapshotId, COLUMNS);
    assertThat(slicesOf(drain))
        .as("no slice of the drain has a record")
        .allSatisfy(
            snapshot ->
                assertThat(statisticsFile(table, snapshot.snapshotId()))
                    .as("record of the slice %s", snapshot.snapshotId())
                    .isNull());
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    Map<String, Long> live = assertNdvWithinBounds(table, tableId(), finalSnapshotId, file, sent);
    assertThat(live.get("amount")).isEqualTo(4L);
    // the sketch of the insert and the recount of the change set: every value ever sent
    Schema schema = table.schemas().get(table.snapshot(finalSnapshotId).schemaId());
    assertThat(ndv(blobOf(file, schema, "amount")))
        .as("ndv of amount: the base of the insert and the update of the change set")
        .isEqualTo(sent.distinct("amount"))
        .isEqualTo(8L);
    assertThat(ndv(blobOf(file, schema, "id"))).isEqualTo((long) keys);

    // the change set is applied: its staged files are deleted
    Awaitility.await()
        .atMost(StatisticsItSupport.AWAIT_TIMEOUT)
        .pollInterval(StatisticsItSupport.POLL_INTERVAL)
        .untilAsserted(
            () ->
                assertThat(filesUnder(table, defaultStagingLocation(table)))
                    .as("files under the staging location once the change set is applied")
                    .isEmpty());
  }

  /** Waits until the cleanup of merge-on-read deletes the orphan planted under {@code stats/}. */
  private void awaitOrphanCleanedUp(Table table, String orphan) {
    Awaitility.await()
        .atMost(CLEANUP_TIMEOUT)
        .pollInterval(StatisticsItSupport.POLL_INTERVAL)
        .untilAsserted(
            () ->
                assertThat(stagedPuffins(table, target()))
                    .as(
                        "the orphan planted under stats/ is deleted by the cleanup of merge-on-read")
                    .doesNotContain(orphan));
  }

  private void assertMergeOnReadLeavesDrain(
      Table table, List<String> sketchFiles, Set<String> changeSetFiles, long headSnapshotId) {
    Awaitility.await()
        .during(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              assertThat(stagedPuffins(table, target()))
                  .as("merge-on-read leaves the Puffin files of the buffered answers")
                  .containsAll(sketchFiles);
              assertThat(filesUnder(table, defaultStagingLocation(table)))
                  .as("merge-on-read leaves the files of the change set")
                  .containsAll(changeSetFiles);
              table.refresh();
              assertThat(table.currentSnapshot().snapshotId())
                  .as("merge-on-read commits nothing over the unfinished drain")
                  .isEqualTo(headSnapshotId);
            });
  }

  private Map<String, Object> copyOnWrite(Map<String, Object> extra) {
    return rowLevel("copy-on-write", extra);
  }

  private Map<String, Object> mergeOnRead(Map<String, Object> extra) {
    return rowLevel("merge-on-read", extra);
  }

  private Map<String, Object> rowLevel(String mode, Map<String, Object> extra) {
    Map<String, Object> settings = Maps.newHashMap();
    settings.put("iceberg.tables.cdc-field", "op");
    settings.put("iceberg.tables.row-level-mode", mode);
    settings.putAll(extra);
    return settings;
  }

  /**
   * 20 rows, then a rewrite of 6 of them (30 %): the share of the rows that went through the sketch
   * and are not alive is 1 − 20 / 26, above the threshold of 0.2. Returns the final snapshot of the
   * rewrite, once its statistics are written.
   */
  private long rewriteThirdOfRows(Table table, SentValues sent) {
    List<Row> inserts = Lists.newArrayList();
    for (int id = 1; id <= 20; id++) {
      inserts.add(cdcRow("c", id, "a" + id, id % 5));
    }

    inserts.forEach(row -> sent.add(row.fields()));
    sendRows(inserts);
    long insertSnapshotId = awaitNames(table, namesOf(inserts));
    awaitStats(table, insertSnapshotId, Kind.NDV);
    awaitMetric(connectorName(), METRIC_STALE, 0);

    List<Row> updates = Lists.newArrayList();
    for (int id = 1; id <= 6; id++) {
      updates.add(cdcRow("u", id, "b" + id, 100 + id));
    }

    updates.forEach(row -> sent.add(row.fields()));
    sendRowsToOnePartition(updates);
    long snapshotId = awaitNames(table, namesAfter(namesOf(inserts), updates));
    awaitStats(table, snapshotId, Kind.NDV);
    return snapshotId;
  }

  /** Waits until the live rows are the names by id, in a snapshot that is not a slice. */
  private long awaitNames(Table table, Map<Long, String> expected) {
    return awaitLanded(
        table,
        current ->
            current.currentSnapshot() != null
                && !current.currentSnapshot().summary().containsKey(CHANGE_SET_ID_PROP)
                && namesOfLive(current).equals(expected),
        "the live rows are " + expected,
        LANDING_TIMEOUT);
  }

  /** Waits until the head of the table is a slice of a drain after the snapshot. */
  private void awaitIntermediateSlice(Table table, long afterSnapshotId) {
    Awaitility.await()
        .atMost(LANDING_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> {
              table.refresh();
              Snapshot head = table.currentSnapshot();
              assertThat(head.snapshotId()).isNotEqualTo(afterSnapshotId);
              assertThat(head.summary()).containsKey(CHANGE_SET_ID_PROP);
            });
  }

  /** The snapshots after the base up to the final one, the oldest first. */
  private static List<Snapshot> drainSnapshots(Table table, long finalSnapshotId, long baseId) {
    table.refresh();
    return Lists.reverse(
        Lists.newArrayList(SnapshotUtil.ancestorsBetween(table, finalSnapshotId, baseId)));
  }

  /** The slices of the drain: the snapshots that carry the id of the change set. */
  private static List<Snapshot> slicesOf(List<Snapshot> drain) {
    return drain.stream()
        .filter(snapshot -> snapshot.summary().containsKey(CHANGE_SET_ID_PROP))
        .toList();
  }

  private static Map<Long, String> namesOf(List<Row> rows) {
    Map<Long, String> names = Maps.newHashMap();
    rows.forEach(row -> names.put(row.id(), (String) row.fields().get("name")));
    return names;
  }

  private static Map<Long, String> namesAfter(Map<Long, String> names, List<Row> changes) {
    Map<Long, String> result = Maps.newHashMap(names);
    changes.forEach(row -> result.put(row.id(), (String) row.fields().get("name")));
    return result;
  }

  private static Map<Long, String> namesOfLive(Table table) {
    Map<Long, String> names = Maps.newHashMap();
    readRecords(table)
        .forEach(
            record -> names.put((Long) record.getField("id"), (String) record.getField("name")));
    return names;
  }

  private static SentValues liveValues(Table table) {
    table.refresh();
    SentValues values = new SentValues();
    for (Record record : readRecords(table)) {
      Map<String, Object> fields = Maps.newLinkedHashMap();
      COLUMNS.forEach(column -> fields.put(column, record.getField(column)));
      values.add(fields);
    }

    return values;
  }

  private static Row cdcRow(String op, long id, String name, int amount) {
    Map<String, Object> fields = Maps.newLinkedHashMap();
    fields.put("id", id);
    fields.put("name", name);
    fields.put("amount", amount);
    fields.put("op", op);
    return new Row(id, fields);
  }
}
