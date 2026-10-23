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
package org.apache.iceberg.connect.data.copyonwrite;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAffectedFilePlanner {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);

  /** The connector's default, which mirrors Iceberg's private {@code IN_PREDICATE_LIMIT}. */
  private static final int DEFAULT_MAX_IN_CARDINALITY = 200;

  private static final long MAX = Long.MAX_VALUE;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testPruningExcludesFilesOutsideKeyBounds() {
    DataFile inRange = syntheticFile("a.parquet", 100L, 1L, 5L);
    DataFile outOfRange = syntheticFile("b.parquet", 200L, 10L, 20L);
    DataFile noBounds = syntheticFileWithoutBounds("c.parquet", 50L);
    table.newAppend().appendFile(inRange).appendFile(outOfRange).appendFile(noBounds).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceWithKeys(1L, 3L);

    PlanResult result = AffectedFilePlanner.plan(table, baseSnapshotId, slice, 10, Long.MAX_VALUE);

    List<String> planned =
        result.fileScanTasks().stream().map(FileScanTask::file).map(DataFile::location).toList();
    assertThat(planned).containsExactlyInAnyOrder(inRange.location(), noBounds.location());

    PlanSummary summary = result.summary();
    assertThat(summary.filesCount()).isEqualTo(2);
    assertThat(summary.totalRewriteBytes()).isEqualTo(150L);
    assertThat(summary.changeSetRecords()).isEqualTo(2);
    assertThat(summary.fractionFilesWithoutBounds()).isEqualTo(0.5);
    assertThat(summary.exceedsMaxRewriteBytes()).isFalse();
  }

  @Test
  public void testExceedingMaxRewriteBytesIsReportedNotEnforced() {
    // three matching files, each on its own far over the quota
    DataFile first = syntheticFile("a.parquet", 100L, 1L, 1L);
    DataFile second = syntheticFile("b.parquet", 100L, 2L, 2L);
    DataFile third = syntheticFile("c.parquet", 100L, 3L, 3L);
    table.newAppend().appendFile(first).appendFile(second).appendFile(third).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceWithKeys(1L, 2L, 3L);

    PlanResult result = AffectedFilePlanner.plan(table, baseSnapshotId, slice, 10, 50L);

    // over quota is reported, and every matching file is still planned. Dropping one to fit would
    // leave a key of the slice in a file nobody rewrites, while its owner writes the new version
    // beside it: a silent duplicate no single-task test could catch
    assertThat(
            result.fileScanTasks().stream()
                .map(FileScanTask::file)
                .map(DataFile::location)
                .toList())
        .containsExactlyInAnyOrder(first.location(), second.location(), third.location());
    assertThat(result.summary().totalRewriteBytes()).isEqualTo(300L);
    assertThat(result.summary().exceedsMaxRewriteBytes()).isTrue();
  }

  @Test
  public void testCardinalityFallbackToRangeIsOverInclusive() {
    DataFile lowFile = syntheticFile("a.parquet", 100L, 1L, 1L);
    DataFile midFile = syntheticFile("b.parquet", 100L, 50L, 50L);
    DataFile highFile = syntheticFile("c.parquet", 100L, 999L, 999L);
    table.newAppend().appendFile(lowFile).appendFile(midFile).appendFile(highFile).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // two distinct keys (1, 999) but maxInCardinality=1 forces the range fallback [1,999],
    // which over-includes the file at 50 (correct, just not tight)
    ChangeSetSlice slice = sliceWithKeys(1L, 999L);

    PlanResult result = AffectedFilePlanner.plan(table, baseSnapshotId, slice, 1, Long.MAX_VALUE);

    assertThat(result.fileScanTasks()).hasSize(3);
  }

  @Test
  public void testDefaultCardinalityMatchesTheLimitIcebergStopsEvaluatingInAt() {
    // one file inside the slice's key range, one far outside it
    DataFile inRange = syntheticFile("a.parquet", 100L, 1L, 300L);
    DataFile outOfRange = syntheticFile("b.parquet", 100L, 900_000L, 900_001L);
    table.newAppend().appendFile(inRange).appendFile(outOfRange).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // 201 distinct keys: one past Iceberg's IN_PREDICATE_LIMIT, which is private in both
    // ManifestEvaluator and InclusiveMetricsEvaluator
    long[] keys = new long[201];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = i + 1L;
    }
    ChangeSetSlice slice = sliceWithKeys(keys);

    // at the configured default the range fallback engages and the distant file is pruned
    PlanResult pruned =
        AffectedFilePlanner.plan(table, baseSnapshotId, slice, DEFAULT_MAX_IN_CARDINALITY, MAX);
    assertThat(locationsOf(pruned)).containsExactly(inRange.location());

    // above it an IN predicate is built instead, and Iceberg answers ROWS_MIGHT_MATCH without
    // evaluating it: nothing is pruned. This is what pins the default to 200: if a future
    // Iceberg raises its limit, this assertion fails and the default can be raised with it
    PlanResult unpruned = AffectedFilePlanner.plan(table, baseSnapshotId, slice, 1000, MAX);
    assertThat(locationsOf(unpruned))
        .containsExactlyInAnyOrder(inRange.location(), outOfRange.location());
  }

  /**
   * The predicate names an identifier column by its field id in the table's current schema, not in
   * the base snapshot's. A schema change commits no snapshot, so after a rename the base snapshot
   * still carries the old name, but the scan binds its filter through the table's specs, which are
   * bound to the current schema, and the old name fails to bind on every planning attempt.
   */
  @Test
  public void testAnIdentifierColumnRenamedAfterTheBaseSnapshotStillPlans() {
    DataFile inRange = syntheticFile("a.parquet", 100L, 1L, 5L);
    DataFile outOfRange = syntheticFile("b.parquet", 200L, 10L, 20L);
    table.newAppend().appendFile(inRange).appendFile(outOfRange).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    table.updateSchema().renameColumn("id", "key").commit();
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(baseSnapshotId);
    assertThat(table.schemas().get(table.currentSnapshot().schemaId()).findColumnName(1))
        .isEqualTo("id");
    assertThat(table.schema().findColumnName(1)).isEqualTo("key");

    ChangeSetSlice slice = sliceWithKeys(1L, 3L);

    PlanResult result =
        AffectedFilePlanner.plan(table, baseSnapshotId, slice, DEFAULT_MAX_IN_CARDINALITY, MAX);

    assertThat(locationsOf(result)).containsExactly(inRange.location());
  }

  private static List<String> locationsOf(PlanResult result) {
    return result.fileScanTasks().stream()
        .map(FileScanTask::file)
        .map(DataFile::location)
        .collect(Collectors.toList());
  }

  /**
   * A plan spanning partition specs is refused, because {@code RewriteAssigned} and {@code
   * RewriteComplete} build their {@code DataFile} schema from a single partition type and would
   * encode one spec's tuple through another's schema. On a v2 table this is not exotic: {@code
   * BaseUpdatePartitionSpec} drops a removed field outright instead of voiding it, so the fields of
   * every later spec have shifted.
   */
  @Test
  public void testAPlanSpanningPartitionSpecsIsRefused() {
    Table partitioned =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "evolving"),
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("data").build());

    DataFile underOldSpec =
        DataFiles.builder(partitioned.spec())
            .withPath(partitioned.location() + "/data/old.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(1)
            .withPartitionPath("data=v")
            .build();
    partitioned.newAppend().appendFile(underOldSpec).commit();

    // ... and now the spec changes, which on a v2 table renumbers what is left
    partitioned.updateSpec().removeField("data").commit();
    partitioned.refresh();
    long baseSnapshotId = partitioned.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceWithKeys(partitioned, 1L);

    assertThatThrownBy(
            () ->
                AffectedFilePlanner.plan(
                    partitioned, baseSnapshotId, slice, DEFAULT_MAX_IN_CARDINALITY, MAX))
        .isInstanceOf(PermanentCopyOnWriteException.class)
        .hasMessageContaining("more than one partition spec")
        .hasMessageContaining("rewrite_data_files");
  }

  /** The ordinary case: one spec, however many partitions. */
  @Test
  public void testAPlanWithinOnePartitionSpecIsFine() {
    Table partitioned =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "stable"),
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("data").build());
    partitioned
        .newAppend()
        .appendFile(
            DataFiles.builder(partitioned.spec())
                .withPath(partitioned.location() + "/data/one.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100L)
                .withRecordCount(1)
                .withPartitionPath("data=v")
                .build())
        .commit();
    long baseSnapshotId = partitioned.currentSnapshot().snapshotId();

    PlanResult result =
        AffectedFilePlanner.plan(
            partitioned,
            baseSnapshotId,
            sliceWithKeys(partitioned, 1L),
            DEFAULT_MAX_IN_CARDINALITY,
            MAX);

    assertThat(result.fileScanTasks()).hasSize(1);
  }

  /**
   * A planned file keeps every equality delete that may apply to it, not only those whose own
   * bounds meet the slice's keys. The rewrite carries the file's other rows over into a replacement
   * with a newer sequence number, out of every older delete's reach: a delete missing from the task
   * brings its rows back for good.
   */
  @Test
  public void testAPlannedFileKeepsAnEqualityDeleteWhoseBoundsMissTheSliceKeys() {
    // a merge-on-read period deleted key 3 from a file that also holds key 1
    DataFile planned = syntheticFile("a.parquet", 100L, 1L, 3L);
    table.newAppend().appendFile(planned).commit();
    DeleteFile equality = syntheticEqualityDelete("a-deletes.parquet", 3L, 3L);
    table.newRowDelta().addDeletes(equality).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    PlanResult result =
        AffectedFilePlanner.plan(
            table, baseSnapshotId, sliceWithKeys(1L), DEFAULT_MAX_IN_CARDINALITY, MAX);

    assertThat(locationsOf(result)).containsExactly(planned.location());
    assertThat(Iterables.getOnlyElement(result.fileScanTasks()).deletes())
        .extracting(DeleteFile::location)
        .containsExactly(equality.location());
  }

  private ChangeSetSlice sliceWithKeys(long... ids) {
    return sliceWithKeys(table, ids);
  }

  private ChangeSetSlice sliceWithKeys(Table target, long... ids) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            target,
            TABLE_REFERENCE,
            ID_FIELDS,
            target.location() + "/_staging",
            "cg-connect",
            "task-0");
    // rows under the target's current names: staging copies columns by name
    Schema schema = target.schema();
    long offset = 0;
    for (long id : ids) {
      GenericRecord tableRow = GenericRecord.create(schema);
      tableRow.setField(schema.findColumnName(1), id);
      tableRow.setField(schema.findColumnName(2), "v");
      writer.write(writer.stagedRow(tableRow, StagedChangeSchema.OP_INSERT, "t", 0, offset++));
    }
    List<StagedChangeFile> files = writer.complete();
    return ChangeSetNormalizer.normalize(target, ID_FIELDS, files, null, 1000);
  }

  private DataFile syntheticFile(String name, long sizeBytes, long lowerId, long upperId) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(1)
        .withMetrics(idBounds(lowerId, upperId))
        .build();
  }

  private DeleteFile syntheticEqualityDelete(String name, long lowerId, long upperId) {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofEqualityDeletes(1)
        .withPath(table.location() + "/data/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10L)
        .withRecordCount(1)
        .withMetrics(idBounds(lowerId, upperId))
        .build();
  }

  private static Metrics idBounds(long lowerId, long upperId) {
    return new Metrics(
        1L,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.<Integer, ByteBuffer>of(
            1, Conversions.toByteBuffer(Types.LongType.get(), lowerId)),
        ImmutableMap.<Integer, ByteBuffer>of(
            1, Conversions.toByteBuffer(Types.LongType.get(), upperId)));
  }

  private DataFile syntheticFileWithoutBounds(String name, long sizeBytes) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(1)
        .build();
  }
}
