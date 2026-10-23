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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.connect.data.StatisticsHealth.Flag;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The health flags by the metadata of the head of the branch, and the transitions they give. */
public class TestHealthTransition {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final double STALE_RATIO = 0.2;
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()),
              optional(2, "data", Types.StringType.get()),
              optional(3, "score", Types.IntegerType.get())),
          Set.of(1));
  private static final Map<String, String> TRINO_PROPERTIES = ImmutableMap.of("ndv", "1");
  private static final Flags ALL_UP = new Flags(true, true, true);

  private InMemoryCatalog catalog;
  private Table table;
  private int fileCount = 0;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void connectorNdvOfHeadOverStaleRatioIsStale() {
    upgradeToV3();
    long baseId = append(100L);
    long head = deletePositions(30L); // live(H) = 70 of 100 sketched rows
    setRecord(head, everyColumn(connector(baseId, 100L)));

    HealthTransition flags = fromHead(null, null, false);

    assertThat(flags.apply(Flags.NONE)).isEqualTo(new Flags(false, false, true));
    assertThat(flags.details().get(Flag.STALE))
        .contains("id (field id 1)", "0.30", String.valueOf(baseId));
    assertThat(flags.details()).doesNotContainKey(Flag.MISSING_BASELINE);
  }

  @Test
  public void danglingPositionDeletesOfAV2HeadAreNotStale() {
    DataFile old = fakeFile(10L);
    table.newAppend().appendFile(old).commit();
    long baseId = table.currentSnapshot().snapshotId();
    long deleteId = deletePositions(4L);
    // a compaction drops the deleted rows, but not the position delete file
    table
        .newRewrite()
        .validateFromSnapshot(deleteId)
        .deleteFile(old)
        .addFile(fakeFile(6L))
        .commit();
    long head = table.currentSnapshot().snapshotId();
    setRecord(head, everyColumn(connector(baseId, 7L))); // 1 - 6 / 7 < 0.2

    HealthTransition flags = fromHead(null, null, false);

    assertThat(flags.details()).as("live(H) is the total records").doesNotContainKey(Flag.STALE);
    assertThat(flags.apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void connectorNdvWithinStaleRatioPutsFlagsDown() {
    long head = append(100L);
    setRecord(head, everyColumn(connector(head, 110L))); // 1 - 100 / 110 < 0.2

    assertThat(fromHead(null, null, false).apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void headRecordWithoutBlobsOfSomeColumnsIsMissingBaseline() {
    long head = append(10L);
    setRecord(head, ImmutableMap.of(1, connector(head, 10L)));

    HealthTransition flags = fromHead(null, null, false);

    assertThat(flags.apply(Flags.NONE)).isEqualTo(new Flags(false, true, false));
    assertThat(flags.details().get(Flag.MISSING_BASELINE))
        .contains("data (field id 2): no blob", "score (field id 3): no blob");
  }

  @Test
  public void foreignRecordOfEveryColumnPutsFlagsDown() {
    append(100L);
    long head = deletePositions(90L); // a blob without the connector's properties is not stale
    setRecord(head, everyColumn(TRINO_PROPERTIES));

    HealthTransition flags = fromHead(null, null, false);

    assertThat(flags.apply(ALL_UP)).isEqualTo(Flags.NONE);
    assertThat(flags.apply(new Flags(true, false, false)).gapOverLimit()).isFalse();
    assertThat(flags.details()).isEmpty();
  }

  @Test
  public void headWithoutRecordLeavesFlags() {
    long older = append(10L);
    setRecord(older, everyColumn(TRINO_PROPERTIES));
    append(10L);

    HealthTransition flags = fromHead(null, null, false);

    assertThat(flags.apply(ALL_UP)).isEqualTo(ALL_UP);
    assertThat(flags.apply(new Flags(false, true, false))).isEqualTo(new Flags(false, true, false));
    assertThat(flags.apply(Flags.NONE)).isEqualTo(Flags.NONE);
  }

  @Test
  public void headCopyingOlderRecordOverForeignAppendLeavesFlags() {
    DataFile first = fakeFile(10L);
    table.newAppend().appendFile(first).commit();
    long analyzed = table.currentSnapshot().snapshotId();
    setRecord(analyzed, everyColumn(TRINO_PROPERTIES));
    DataFile second = fakeFile(10L);
    table.newAppend().appendFile(second).commit();
    // OPTIMIZE: Trino copies the blobs of the nearest record, which miss the values of the append
    table.newRewrite().deleteFile(first).deleteFile(second).addFile(fakeFile(20L)).commit();
    long head = table.currentSnapshot().snapshotId();
    setRecord(head, analyzed, everyColumn(TRINO_PROPERTIES));

    HealthTransition flags = fromHead(null, null, false);

    assertThat(flags.apply(ALL_UP)).as("the record does not describe the head").isEqualTo(ALL_UP);
    assertThat(flags.apply(Flags.NONE)).isEqualTo(Flags.NONE);
    assertThat(flags.details()).isEmpty();
  }

  @Test
  public void noHeadPutsFlagsDown() {
    assertThat(fromHead(null, null, false).apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void emptySetOfColumnsPutsFlagsDown() {
    long head = append(10L);
    setRecord(head, ImmutableMap.of(1, connector(head, 10L)));

    assertThat(fromHead(null, List.of(), false).apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void headOfBranchOfCommitsIsEvaluated() {
    long mainHead = append(10L);
    setRecord(mainHead, everyColumn(TRINO_PROPERTIES));
    table.manageSnapshots().createBranch("audit", mainHead).commit();
    table.newAppend().appendFile(fakeFile(10L)).toBranch("audit").commit();
    long branchHead = table.snapshot("audit").snapshotId();
    setRecord(branchHead, ImmutableMap.of(1, TRINO_PROPERTIES));

    assertThat(fromHead("audit", null, false).apply(Flags.NONE))
        .isEqualTo(new Flags(false, true, false));
    assertThat(fromHead(null, null, false).apply(ALL_UP)).isEqualTo(Flags.NONE);
    assertThat(fromHead("main", null, false).apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void headWithoutTotalsIsNotStale() {
    upgradeToV3();
    long baseId = append(100L);
    long head = deletePositions(90L);
    setRecord(head, everyColumn(connector(baseId, 100L)));
    Snapshot snapshot = spy(table.snapshot(head));
    Map<String, String> summary = Maps.newHashMap(snapshot.summary());
    summary.remove(SnapshotSummary.TOTAL_POS_DELETES_PROP);
    doReturn(summary).when(snapshot).summary();
    Table withoutTotals = reading();
    doReturn(snapshot).when(withoutTotals).currentSnapshot();
    doReturn(snapshot).when(withoutTotals).snapshot(head);

    HealthTransition flags =
        HealthTransition.fromHead(TABLE, withoutTotals, null, config(false), tableConfig(null));

    assertThat(flags.apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void columnsOfSetAreResolvedAsInStep() {
    long head = append(10L);
    setRecord(head, ImmutableMap.of(1, connector(head, 10L)));

    // matched ignoring case: the set is [data], which has no blob
    assertThat(fromHead(null, List.of("DATA"), true).apply(Flags.NONE))
        .isEqualTo(new Flags(false, true, false));
    // not matched: the set is empty
    assertThat(fromHead(null, List.of("DATA"), false).apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void staleBlobOfColumnOutsideSetDoesNotCount() {
    upgradeToV3();
    long baseId = append(100L);
    long head = deletePositions(30L);
    setRecord(head, ImmutableMap.of(1, connector(baseId, 70L), 3, connector(baseId, 1_000L)));

    assertThat(fromHead(null, List.of("id"), false).apply(ALL_UP)).isEqualTo(Flags.NONE);
    assertThat(fromHead(null, List.of("id", "score"), false).apply(Flags.NONE))
        .isEqualTo(new Flags(false, false, true));
  }

  /** Evaluates the head; nothing is read: every read of a file throws. */
  private HealthTransition fromHead(String branch, List<String> columns, boolean caseInsensitive) {
    return HealthTransition.fromHead(
        TABLE, reading(), branch, config(caseInsensitive), tableConfig(columns));
  }

  private Table reading() {
    Table reading = spy(table);
    doReturn(new ReadFileIO().refuseAll()).when(reading).io();
    return reading;
  }

  private static IcebergSinkConfig config(boolean caseInsensitive) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.statisticsStaleDeletedRatio()).thenReturn(STALE_RATIO);
    when(config.schemaCaseInsensitive()).thenReturn(caseInsensitive);
    return config;
  }

  private static TableSinkConfig tableConfig(List<String> columns) {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.statisticsColumns()).thenReturn(columns);
    return tableConfig;
  }

  private long append(long records) {
    table.newAppend().appendFile(fakeFile(records)).commit();
    return table.currentSnapshot().snapshotId();
  }

  /** An external DELETE: a position delete file of the rows, from v3 a deletion vector. */
  private long deletePositions(long rows) {
    fileCount += 1;
    FileMetadata.Builder deletes =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("mem://bucket/data/pos-deletes-" + fileCount + "-" + UUID.randomUUID())
            .withFileSizeInBytes(100L)
            .withRecordCount(rows);
    if (TableUtil.formatVersion(table) < 3) {
      deletes.withFormat(FileFormat.PARQUET);
    } else {
      deletes
          .withFormat(FileFormat.PUFFIN)
          .withReferencedDataFile("mem://bucket/data/deleted-" + fileCount + ".parquet")
          .withContentOffset(4L)
          .withContentSizeInBytes(40L);
    }

    table.newRowDelta().addDeletes(deletes.build()).commit();
    return table.currentSnapshot().snapshotId();
  }

  /** Position deletes from now on are deletion vectors, removed together with their data file. */
  private void upgradeToV3() {
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
  }

  private DataFile fakeFile(long records) {
    fileCount += 1;
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mem://bucket/data/fake-" + fileCount + "-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1_000L)
        .withRecordCount(records)
        .build();
  }

  private static Map<Integer, Map<String, String>> everyColumn(Map<String, String> properties) {
    return ImmutableMap.of(1, properties, 2, properties, 3, properties);
  }

  /** Registers an NDV record of the snapshot whose file does not exist: it must not be read. */
  private void setRecord(long snapshotId, Map<Integer, Map<String, String>> blobs) {
    setRecord(snapshotId, snapshotId, blobs);
  }

  /** Registers an NDV record of the snapshot with the blobs built at the source snapshot. */
  private void setRecord(
      long snapshotId, long sourceSnapshotId, Map<Integer, Map<String, String>> blobs) {
    long sequenceNumber = table.snapshot(sourceSnapshotId).sequenceNumber();
    List<BlobMetadata> metadata = Lists.newArrayList();
    blobs.forEach(
        (fieldId, properties) ->
            metadata.add(
                new GenericBlobMetadata(
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    sourceSnapshotId,
                    sequenceNumber,
                    List.of(fieldId),
                    properties)));
    table
        .updateStatistics()
        .setStatistics(
            new GenericStatisticsFile(
                snapshotId,
                "mem://bucket/nowhere/stats-" + snapshotId + "-" + UUID.randomUUID() + ".puffin",
                100L,
                10L,
                metadata))
        .commit();
  }

  private static Map<String, String> connector(long recomputeSnapshotId, long sketchedRows) {
    return ImmutableMap.of(
        NdvComputation.NDV_PROPERTY,
        "1",
        NdvComputation.RECOMPUTE_SNAPSHOT_ID_PROPERTY,
        String.valueOf(recomputeSnapshotId),
        NdvComputation.SKETCHED_ROWS_PROPERTY,
        String.valueOf(sketchedRows));
  }
}
