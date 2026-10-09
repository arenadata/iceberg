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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.NdvBounds;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.NdvBaseline.Reason;
import org.apache.iceberg.connect.data.NdvResult.ColumnWithoutBaseline;
import org.apache.iceberg.connect.data.NdvResult.Kind;
import org.apache.iceberg.connect.data.NdvResult.StaleColumn;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.connect.data.StatisticsHealth.Flag;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.puffin.StandardPuffinProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The NDV of snapshot S from start to end: union, final blobs and file, staleness, the result and
 * its health transition; and the NDV bounds, the lower and upper bounds the tests check against.
 */
public class TestNdvComputation {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final String STAGING = "mem://bucket/kc-staging/db.tbl/group";
  private static final int LG_K = 12;
  private static final double STALE_RATIO = 0.2;
  private static final BooleanSupplier RUNNING = () -> false;
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()),
              optional(2, "data", Types.StringType.get()),
              optional(3, "score", Types.IntegerType.get())),
          Set.of(1));
  private static final Map<String, String> TRINO_PROPERTIES = ImmutableMap.of("ndv", "1");
  private static final Flags ALL_UP = new Flags(true, true, true);

  @TempDir private File temp;

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
  public void finalFileAndBlobsHaveFormatOfDesign() throws IOException {
    DataFile file = writeDataFile(table, row(1, "a", 10), row(2, "b", 10), row(3, "b", null));
    long snapshotId = append(file);
    NdvResult result = compute(snapshotId, Set.of(snapshotId), List.of(answer(snapshotId, file)));
    assertThat(result.kind()).isEqualTo(Kind.BLOBS);

    UUID commitId = UUID.randomUUID();
    StatisticsFile stats = NdvComputation.writeFile(table, snapshotId, commitId, result.blobs());

    String name = "stats-" + snapshotId + "-" + commitId + ".puffin";
    assertThat(stats.path())
        .isEqualTo(((HasTableOperations) table).operations().metadataFileLocation(name))
        .endsWith("/metadata/" + name);
    assertThat(stats.snapshotId()).isEqualTo(snapshotId);
    InputFile input = table.io().newInputFile(stats.path());
    assertThat(stats.fileSizeInBytes()).isEqualTo(input.getLength());

    long sequenceNumber = table.snapshot(snapshotId).sequenceNumber();
    try (PuffinReader reader =
        Puffin.read(input)
            .withFileSize(stats.fileSizeInBytes())
            .withFooterSize(stats.fileFooterSizeInBytes())
            .build()) {
      assertThat(reader.fileMetadata().properties())
          .containsEntry(
              StandardPuffinProperties.CREATED_BY_PROPERTY,
              "iceberg-kafka-connect " + IcebergBuild.version());
      List<BlobMetadata> blobs = reader.fileMetadata().blobs();
      assertThat(blobs)
          .extracting(BlobMetadata::inputFields)
          .containsExactly(List.of(1), List.of(2), List.of(3));
      assertThat(blobs)
          .allSatisfy(
              blob -> {
                assertThat(blob.type()).isEqualTo(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);
                assertThat(blob.snapshotId()).isEqualTo(snapshotId);
                assertThat(blob.sequenceNumber()).isEqualTo(sequenceNumber);
                assertThat(blob.compressionCodec())
                    .isEqualTo(PuffinCompressionCodec.ZSTD.codecName());
                assertThat(blob.properties())
                    .containsEntry(
                        NdvComputation.RECOMPUTE_SNAPSHOT_ID_PROPERTY, String.valueOf(snapshotId))
                    .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "3");
              });
      assertThat(blobs)
          .extracting(blob -> blob.properties().get(NdvComputation.NDV_PROPERTY))
          .containsExactly("3", "2", "1");
      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(blobs)) {
        Sketch sketch = wrap(ByteBuffers.toByteArray(blob.second()));
        assertThat(blob.first().properties().get(NdvComputation.NDV_PROPERTY))
            .isEqualTo(String.valueOf((long) sketch.getEstimate()));
      }
    }

    // the footer is not compressed: bit 0 of the flags before the closing magic
    byte[] bytes = readAll(input);
    assertThat(bytes[bytes.length - 8] & 1).isZero();
    assertThat(stats.blobMetadata())
        .extracting(blob -> blob.fields())
        .containsExactly(List.of(1), List.of(2), List.of(3));

    table.updateStatistics().setStatistics(stats).commit();
    NdvBounds.of(table, snapshotId, "id").written(1L, 2L, 3L).checkRecorded(LG_K);
    NdvBounds.of(table, snapshotId, "data").written("a", "b", "b").checkRecorded(LG_K);
    NdvBounds.of(table, snapshotId, "score").written(10, 10, null).checkRecorded(LG_K);
  }

  @Test
  public void writeFileNeverOverwritesExistingFile() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);
    NdvResult result = compute(snapshotId, Set.of(snapshotId), List.of(answer(snapshotId, file)));
    UUID commitId = UUID.randomUUID();
    StatisticsFile first = NdvComputation.writeFile(table, snapshotId, commitId, result.blobs());

    assertThat(NdvComputation.fileLocation(table, snapshotId, commitId)).isEqualTo(first.path());
    assertThatThrownBy(() -> NdvComputation.writeFile(table, snapshotId, commitId, result.blobs()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Already exists");
    assertThat(table.io().newInputFile(first.path()).getLength())
        .isEqualTo(first.fileSizeInBytes());
  }

  @Test
  public void writeFileWithoutBlobsIsRefusedAndWritesNothing() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);
    UUID commitId = UUID.randomUUID();
    AtomicInteger created = new AtomicInteger();

    // the refusal comes before create(): the caller is not told the path is its own
    assertThatThrownBy(
            () ->
                NdvComputation.writeFile(
                    table, snapshotId, commitId, List.of(), created::incrementAndGet))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without blobs");
    assertThat(created).as("the calls of onCreated").hasValue(0);
    assertThatThrownBy(
            () -> table.io().newInputFile(NdvComputation.fileLocation(table, snapshotId, commitId)))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("No in-memory file found");
  }

  @Test
  public void writeFileTellsOfCreatedFileOnce() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);
    NdvResult result = compute(snapshotId, Set.of(snapshotId), List.of(answer(snapshotId, file)));
    AtomicInteger created = new AtomicInteger();

    NdvComputation.writeFile(
        table, snapshotId, UUID.randomUUID(), result.blobs(), created::incrementAndGet);

    assertThat(created).as("the calls of onCreated").hasValue(1);
  }

  @Test
  public void finalBlobIsBuiltWithConfiguredLgK() {
    Record[] rows = rows(0, 200);
    DataFile file = writeDataFile(table, rows);
    long snapshotId = append(file);
    List<StatisticsAnswer> answers = List.of(answer(snapshotId, file));

    NdvResult small =
        compute(
            table,
            config(4),
            tableConfig(List.of("id")),
            snapshotId,
            Set.of(snapshotId),
            answers,
            RUNNING);
    Sketch smallSketch = sketchOf(small, 1);
    assertThat(smallSketch.getRetainedEntries(true)).isLessThanOrEqualTo(16);
    assertThat(smallSketch.getTheta()).isLessThan(1.0);
    NdvBounds.of(table, snapshotId, "id").written(ids(0, 200)).check(ndv(small, 1), 4);

    NdvResult exact =
        compute(
            table,
            config(LG_K),
            tableConfig(List.of("id")),
            snapshotId,
            Set.of(snapshotId),
            answers,
            RUNNING);
    assertThat(sketchOf(exact, 1).getRetainedEntries(true)).isEqualTo(200);
    assertThat(ndv(exact, 1)).isEqualTo(200L);
    NdvBounds.of(table, snapshotId, "id").written(ids(0, 200)).check(ndv(exact, 1), LG_K);
  }

  @Test
  public void trinoBaseOfLgK14UnitesWithConfiguredLgK12() {
    long baseId = append(writeDataFile(table, rows(0, 10_000)));
    setBase(baseId, ImmutableMap.of(1, longSketch(14, 0, 10_000)), TRINO_PROPERTIES);
    long snapshotId = append(writeDataFile(table, rows(10_000, 13_000))); // not own: the gap

    NdvResult result =
        compute(table, config(LG_K), tableConfig(List.of("id")), snapshotId, List.of(), RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    long ndv = ndv(result, 1);
    assertThat(ndv).isBetween((long) (13_000 * (1 - 3.0 / 64)), 13_000L);
    NdvBounds.of(table, snapshotId, "id")
        .base(ids(0, 10_000))
        .written(ids(10_000, 13_000))
        .check(ndv, LG_K);
    // a blob without the connector's properties is a recompute at the base
    assertThat(properties(result, 1))
        .containsEntry(NdvComputation.RECOMPUTE_SNAPSHOT_ID_PROPERTY, String.valueOf(baseId))
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "13000");
    assertThat(result.filesRead()).isEqualTo(1L);
  }

  @Test
  public void ndvAboveLiveRowsOfSIsLiveRows() {
    long baseId = append(fakeFile(10L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 50)), TRINO_PROPERTIES);
    long snapshotId = append(writeDataFile(table, rows(100, 105)));

    NdvResult result = compute(snapshotId, Set.of(), List.of(), List.of("id"));

    assertThat(sketchOf(result, 1).getEstimate()).isEqualTo(55.0);
    assertThat(ndv(result, 1)).isEqualTo(15L);
  }

  @Test
  public void ndvWithoutTotalsOfSIsEstimate() {
    long baseId = append(fakeFile(10L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 50)), TRINO_PROPERTIES);
    long snapshotId = append(writeDataFile(table, rows(100, 103)));
    Snapshot snapshot = spy(table.snapshot(snapshotId));
    Map<String, String> summary = Maps.newHashMap(snapshot.summary());
    summary.remove(SnapshotSummary.TOTAL_RECORDS_PROP);
    doReturn(summary).when(snapshot).summary();
    Table withoutTotals = spy(table);
    doReturn(snapshot).when(withoutTotals).snapshot(snapshotId);

    NdvResult result =
        compute(
            withoutTotals,
            config(LG_K),
            tableConfig(List.of("id")),
            snapshotId,
            List.of(),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(ndv(result, 1)).isEqualTo(53L);
    assertThat(result.staleColumns()).isEmpty();
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(Flags.NONE);
  }

  @Test
  public void externalDeleteOfAV3TableRaisesStaleRatioOnNextStep() {
    upgradeToV3();
    long baseId = append(fakeFile(100L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 100)), connector(baseId, 100L));
    long snapshotId = deletePositions(30L); // not own: the gap, adds no data files

    NdvResult result = compute(snapshotId, Set.of(), List.of(), List.of("id"));

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(properties(result, 1))
        .containsEntry(NdvComputation.RECOMPUTE_SNAPSHOT_ID_PROPERTY, String.valueOf(baseId))
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "100")
        .containsEntry(NdvComputation.NDV_PROPERTY, "70");
    assertThat(result.staleColumns()).hasSize(1);
    StaleColumn stale = result.staleColumns().get(0);
    assertThat(stale.column().name()).isEqualTo("id");
    assertThat(stale.ratio()).isCloseTo(0.3, within(1e-9));
    assertThat(stale.recomputeSnapshotId()).isEqualTo(baseId);
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(new Flags(false, false, true));
    assertThat(result.transition().details().get(Flag.STALE))
        .contains("id (field id 1)", "0.30", String.valueOf(baseId));
    assertThat(result.filesRead()).isZero();
  }

  @Test
  public void danglingPositionDeletesOfAV2TableDoNotCutNdv() {
    DataFile old = fakeFile(10L);
    long baseId = append(old);
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), connector(baseId, 10L));
    long deleteId = deletePositions(4L);
    // a compaction drops the deleted rows, but not the position delete file
    table
        .newRewrite()
        .validateFromSnapshot(deleteId)
        .deleteFile(old)
        .addFile(fakeFile(6L))
        .commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    assertThat(table.snapshot(snapshotId).summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "6")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "4");

    NdvResult result = compute(snapshotId, Set.of(), List.of(), List.of("id"));

    assertThat(ndv(result, 1)).as("ndv bounded by the total records only").isEqualTo(6L);
    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(result.staleColumns())
        .singleElement()
        .satisfies(stale -> assertThat(stale.ratio()).isCloseTo(0.4, within(1e-9)));
    assertThat(result.filesRead()).isZero();
  }

  @Test
  public void copyOnWriteRewriteKeepsRecomputeSnapshotAndAddsCoveredRows() {
    DataFile old = fakeFile(100L);
    long baseId = append(old);
    long recomputeId = 777L;
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 100)), connector(recomputeId, 100L));
    SketchFile puffin = writerFile(table, SCHEMA, rows(1_000, 1_030)); // 30 updated rows
    table.newOverwrite().deleteFile(old).addFile(fakeFile(100L)).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    StatisticsAnswer answer =
        StatisticsAnswer.ofStagedFiles(snapshotId, List.of(fakeStagedFile()), puffin);

    NdvResult result = compute(snapshotId, Set.of(snapshotId), List.of(answer), List.of("id"));

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(properties(result, 1))
        .containsEntry(NdvComputation.RECOMPUTE_SNAPSHOT_ID_PROPERTY, String.valueOf(recomputeId))
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "130")
        .containsEntry(NdvComputation.NDV_PROPERTY, "100");
    assertThat(result.staleColumns())
        .singleElement()
        .satisfies(stale -> assertThat(stale.ratio()).isCloseTo(1 - 100.0 / 130, within(1e-9)));
    assertThat(result.filesRead()).isZero();
    assertThat(result.transition().apply(Flags.NONE).stale()).isTrue();
  }

  @Test
  public void upsertOfNewKeysWithoutCompactionIsNotStale() {
    long baseId = append(fakeFile(100L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 100)), connector(baseId, 100L));
    DataFile added = writeDataFile(table, rows(100, 110));
    table.newRowDelta().addRows(added).addDeletes(equalityDeletes(10L)).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    NdvResult result = compute(snapshotId, Set.of(), List.of(), List.of("id"));

    assertThat(properties(result, 1))
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "110")
        .containsEntry(NdvComputation.NDV_PROPERTY, "110");
    assertThat(result.staleColumns()).isEmpty();
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(new Flags(false, false, false));
  }

  @Test
  public void newColumnWithInitialDefaultInV3HasDefaultInItsSketch() {
    Schema before =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
    TableIdentifier v3Identifier = TableIdentifier.of(NAMESPACE, "v3");
    Table v3 =
        catalog.createTable(
            v3Identifier,
            before,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
    v3.newAppend().appendFile(fakeFile(10L)).commit();
    long baseId = v3.currentSnapshot().snapshotId();
    setBase(
        v3,
        baseId,
        ImmutableMap.of(1, longSketch(LG_K, 0, 10), 2, longSketch(LG_K, 0, 1)),
        TRINO_PROPERTIES);
    v3.updateSchema().addColumn("score", Types.IntegerType.get(), Literal.of(7)).commit();
    // the writer did not know the column: its answer adds no values of it (EMPTY coverage)
    Record first = GenericRecord.create(before);
    first.set(0, 10L);
    first.set(1, "x");
    SketchFile puffin = writerFile(v3, before, first);
    DataFile file = fakeFile(1L);
    v3.newAppend().appendFile(file).commit();
    long snapshotId = v3.currentSnapshot().snapshotId();

    NdvResult result =
        compute(
            v3Identifier,
            v3,
            config(LG_K),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            List.of(StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), puffin)),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(blobs(result).keySet()).containsExactly(1, 2, 3);
    Sketch score = sketchOf(result, 3);
    assertThat(score.getEstimate()).isEqualTo(1.0);
    UpdateSketch seven = SketchValues.newSketch(LG_K);
    SketchValues.update(seven, SketchValues.toByteBuffer(Types.IntegerType.get(), 7));
    assertThat(SetOperation.builder().buildANotB().aNotB(seven, score).getRetainedEntries(true))
        .as("the initial-default is in the sketch")
        .isZero();
    assertThat(properties(result, 3))
        .containsEntry(NdvComputation.NDV_PROPERTY, "1")
        // a new column starts at the base over its live rows
        .containsEntry(NdvComputation.RECOMPUTE_SNAPSHOT_ID_PROPERTY, String.valueOf(baseId))
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "10");
    assertThat(result.filesRead()).isZero();
  }

  @Test
  public void columnWhoseTypeWidenedHasNoBlobAndIsWithoutBaselineByTypeChanged() {
    long baseId = append(fakeFile(10L));
    setBase(
        baseId,
        ImmutableMap.of(
            1, longSketch(LG_K, 0, 10), 2, longSketch(LG_K, 0, 3), 3, longSketch(LG_K, 0, 2)),
        TRINO_PROPERTIES);
    table.updateSchema().updateColumn("score", Types.LongType.get()).commit();
    long snapshotId = append(writeDataFile(table, rows(10, 12)));

    NdvResult result = compute(snapshotId, Set.of(), List.of(), null);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(blobs(result).keySet()).containsExactly(1, 2);
    assertThat(result.withoutBaseline())
        .extracting(column -> column.column().name(), ColumnWithoutBaseline::reason)
        .containsExactly(tuple("score", Reason.TYPE_CHANGED));
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(new Flags(false, true, false));
    assertThat(result.transition().details().get(Flag.MISSING_BASELINE))
        .contains("score (field id 3): type changed");
  }

  @Test
  public void gapFileWithoutFieldIdsIsReadByNameMapping() throws IOException {
    // a file added by add_files, written before "payload" was renamed to "data"
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            FilesWithoutFieldIds.mappingWithAlias(SCHEMA, "data", "payload"))
        .commit();
    long baseId = append(fakeFile(1L));
    byte[] empty = SketchValues.newSketch(LG_K).compact().toByteArray();
    setBase(baseId, ImmutableMap.of(2, empty), TRINO_PROPERTIES);
    DataFile added =
        FilesWithoutFieldIds.write(
            table,
            temp,
            FileFormat.PARQUET,
            List.of(
                FilesWithoutFieldIds.row(1L, "n1", "a"),
                FilesWithoutFieldIds.row(2L, "n2", "b"),
                FilesWithoutFieldIds.row(3L, "n3", "b")));
    long snapshotId = append(added);

    NdvResult result = compute(snapshotId, Set.of(), List.of(), List.of("data"));

    assertThat(ndv(result, 2)).as("the values of the column the mapping names").isEqualTo(2L);
    assertThat(result.filesRead()).isEqualTo(1L);
  }

  @Test
  public void rejectedColumnWithOtherBlobsWrittenRaisesGapOverLimit() {
    DataFile file = writeDataFile(table, row(1, "a", 1), row(2, "b", 2), row(3, "c", 3));
    long snapshotId = append(file);
    // the writer's file covers id and data, not score
    SketchFile puffin =
        manualFile(
            3,
            3L,
            ImmutableMap.of(
                1, typed("long", longSketch(LG_K, 1, 4)),
                2, typed("string", longSketch(LG_K, 0, 3))));
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), puffin);

    NdvResult result =
        compute(
            table,
            config(LG_K, 1_000, file.fileSizeInBytes() - 1),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            List.of(answer),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(blobs(result).keySet()).containsExactly(1, 2);
    assertThat(result.rejected())
        .singleElement()
        .satisfies(
            rejected -> {
              assertThat(rejected.column().name()).isEqualTo("score");
              assertThat(rejected.files()).isEqualTo(1L);
              assertThat(rejected.bytes()).isEqualTo(file.fileSizeInBytes());
            });
    assertThat(properties(result, 1)).containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "3");
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(new Flags(true, false, false));
    assertThat(result.transition().details().get(Flag.GAP_OVER_LIMIT))
        .contains("score (field id 3)", String.valueOf(file.fileSizeInBytes()));
  }

  @Test
  public void explicitColumnsWithoutBlobsInPartialSparkBaseGiveNoBlobs() {
    long baseId = append(fakeFile(10L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), TRINO_PROPERTIES);
    long snapshotId = append(fakeFile(5L));
    ReadFileIO io = new ReadFileIO().refuseAll(); // nothing is read

    NdvResult result =
        compute(
            reading(io),
            config(LG_K),
            tableConfig(List.of("data", "score")),
            snapshotId,
            List.of(),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.NO_BLOBS);
    assertThat(result.blobs()).isEmpty();
    assertThat(result.withoutBaseline())
        .extracting(column -> column.column().name(), ColumnWithoutBaseline::reason)
        .containsExactly(tuple("data", Reason.NO_BLOB), tuple("score", Reason.NO_BLOB));
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(new Flags(false, true, true));
    assertThat(result.transition().details().get(Flag.MISSING_BASELINE))
        .contains("data (field id 2): no blob", "score (field id 3): no blob");
  }

  @Test
  public void oneColumnWhoseAnswerFileIsOverLimitGivesNoBlobsAndGapOverLimit() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);

    NdvResult result =
        compute(
            table,
            config(LG_K, 1_000, file.fileSizeInBytes() - 1),
            tableConfig(List.of("id")),
            snapshotId,
            Set.of(snapshotId),
            List.of(answer(snapshotId, file)),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.NO_BLOBS);
    assertThat(result.rejected())
        .extracting(rejected -> rejected.column().name())
        .containsExactly("id");
    assertThat(result.withoutBaseline()).isEmpty();
    assertThat(result.transition().apply(new Flags(false, true, true)))
        .isEqualTo(new Flags(true, false, true));
    assertThat(result.filesRead()).isZero();
  }

  @Test
  public void columnAddedToSetWithoutBlobInBaseIsMissingBaselineAndOthersAreWritten() {
    long baseId = append(fakeFile(10L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), connector(baseId, 10L));
    DataFile file = writeDataFile(table, rows(10, 13));
    long snapshotId = append(file);

    NdvResult result =
        compute(
            snapshotId,
            Set.of(snapshotId),
            List.of(answer(snapshotId, file)),
            List.of("id", "data"));

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(blobs(result).keySet()).containsExactly(1);
    assertThat(properties(result, 1)).containsEntry(NdvComputation.NDV_PROPERTY, "13");
    assertThat(result.withoutBaseline())
        .extracting(column -> column.column().name(), ColumnWithoutBaseline::reason)
        .containsExactly(tuple("data", Reason.NO_BLOB));
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(new Flags(false, true, false));
    assertThat(result.transition().details().get(Flag.MISSING_BASELINE))
        .contains("data (field id 2): no blob");
  }

  @Test
  public void partialSparkBaseKeepsSameColumnsWithoutBaselineWhenGapIsOverLimit() {
    long baseId = append(fakeFile(10L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), TRINO_PROPERTIES);
    append(writeDataFile(table, rows(10, 12)));
    long snapshotId = append(writeDataFile(table, rows(12, 14)));

    NdvResult within =
        compute(table, config(LG_K), tableConfig(null), snapshotId, List.of(), RUNNING);
    NdvResult over =
        compute(
            table,
            config(LG_K, 1, Long.MAX_VALUE),
            tableConfig(null),
            snapshotId,
            List.of(),
            RUNNING);

    assertThat(within.kind()).isEqualTo(Kind.BLOBS);
    assertThat(blobs(within).keySet()).containsExactly(1);
    assertThat(over.kind()).isEqualTo(Kind.OVER_LIMIT);
    assertThat(over.blobs()).isEmpty();
    assertThat(over.gapFiles()).isEqualTo(2L);
    assertThat(over.gapBytes()).as("counted by the manifest lists: no file listed").isZero();
    assertThat(over.withoutBaseline())
        .extracting(column -> column.column().name(), ColumnWithoutBaseline::reason)
        .containsExactly(tuple("data", Reason.NO_BLOB), tuple("score", Reason.NO_BLOB));
    assertThat(over.withoutBaseline())
        .extracting(ColumnWithoutBaseline::toString)
        .isEqualTo(
            within.withoutBaseline().stream()
                .map(ColumnWithoutBaseline::toString)
                .collect(Collectors.toList()));
    assertThat(over.transition().apply(new Flags(false, false, true)))
        .isEqualTo(new Flags(true, true, true));
    assertThat(over.transition().details().get(Flag.GAP_OVER_LIMIT))
        .contains("at least 2 data files", String.valueOf(over.gapBytes()));
  }

  @Test
  public void skippedColumnIsWarnedAboutUnderIdentifierOfStep() {
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "skipped_column");
    Table target = catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
    // the name of a catalog's table starts with the name of the catalog
    assertThat(target.name()).isEqualTo("mem." + identifier);
    DataFile file = writeDataFile(target, row(1, "a", 1));
    target.newAppend().appendFile(file).commit();
    long snapshotId = target.currentSnapshot().snapshotId();
    List<String> columns = List.of("id", "missing");
    // the worker warns first, about the column it does not find in the schema
    int before = StatisticsColumns.warnedKeyCount();
    StatisticsColumns.resolve(identifier, target.schema(), columns, false);
    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before + 1);

    NdvResult result =
        compute(
            identifier,
            target,
            config(LG_K),
            tableConfig(columns),
            snapshotId,
            Set.of(snapshotId),
            List.of(answer(snapshotId, file)),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(blobs(result).keySet()).containsExactly(1);
    assertThat(StatisticsColumns.warnedKeyCount())
        .as("the same table, schema and column: no second warning")
        .isEqualTo(before + 1);
  }

  @Test
  public void emptySetOfColumnsIsNotNeededAndPutsFlagsDown() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);

    NdvResult result =
        compute(snapshotId, Set.of(snapshotId), List.of(answer(snapshotId, file)), List.of());

    assertThat(result.kind()).isEqualTo(Kind.NOT_NEEDED);
    assertThat(result.blobs()).isEmpty();
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(Flags.NONE);
  }

  @Test
  public void chainBrokenBeforeBaseIsMissingBaseline() {
    long snapshotId = append(fakeFile(10L)); // not marked, no parent: no statistics to build on

    NdvResult result = compute(snapshotId, Set.of(), List.of(), null);

    assertThat(result.kind()).isEqualTo(Kind.MISSING_BASELINE);
    assertThat(result.blobs()).isEmpty();
    assertThat(result.transition().apply(new Flags(true, false, true)))
        .isEqualTo(new Flags(false, true, true));
    assertThat(result.transition().details().get(Flag.MISSING_BASELINE)).isNotBlank();
  }

  @Test
  public void baseFileThatCannotBeReadThisTimeIsErrorThenNextStepWritesBlobs() {
    long baseId = append(fakeFile(10L));
    StatisticsFile base =
        setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), TRINO_PROPERTIES);
    long snapshotId = append(writeDataFile(table, rows(10, 12)));
    ReadFileIO io = new ReadFileIO().failReads(base.path(), () -> new IOException("footer read"));

    // data has no blob in the base: it is without a baseline whether the base is read or not
    NdvResult failed =
        compute(
            reading(io),
            config(LG_K),
            tableConfig(List.of("id", "data")),
            snapshotId,
            List.of(),
            RUNNING);
    NdvResult next =
        compute(
            table,
            config(LG_K),
            tableConfig(List.of("id", "data")),
            snapshotId,
            List.of(),
            RUNNING);

    assertThat(failed.kind()).isEqualTo(Kind.ERROR);
    assertThat(failed.failure()).isNotNull();
    assertThat(failed.withoutBaseline())
        .extracting(column -> column.column().name(), ColumnWithoutBaseline::reason)
        .containsExactly(tuple("data", Reason.NO_BLOB));
    assertThat(failed.transition().apply(Flags.NONE)).isEqualTo(Flags.NONE);
    assertThat(failed.transition().apply(ALL_UP)).isEqualTo(ALL_UP);
    assertThat(next.kind()).isEqualTo(Kind.BLOBS);
    assertThat(ndv(next, 1)).isEqualTo(12L);
  }

  @Test
  public void baseFileGoneWithItsRecordInPlaceIsMissingBaseline() {
    long baseId = append(fakeFile(10L));
    StatisticsFile base =
        setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), TRINO_PROPERTIES);
    long snapshotId = append(fakeFile(2L));
    table.io().deleteFile(base.path());

    NdvResult result = compute(snapshotId, Set.of(), List.of(), List.of("id"));

    assertThat(result.kind()).isEqualTo(Kind.MISSING_BASELINE);
    assertThat(result.transition().apply(new Flags(true, false, false)))
        .isEqualTo(new Flags(false, true, false));
    assertThat(result.transition().details().get(Flag.MISSING_BASELINE)).contains(base.path());
  }

  @Test
  public void stoppedStepIsStoppedAndLeavesFlags() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);

    NdvResult result =
        compute(
            table,
            config(LG_K),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            List.of(answer(snapshotId, file)),
            () -> true);

    assertThat(result.kind()).isEqualTo(Kind.STOPPED);
    assertThat(result.blobs()).isEmpty();
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(ALL_UP);
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(Flags.NONE);
  }

  @Test
  public void failureToReadFileOfListIsError() {
    DataFile file = writeDataFile(table, row(1, "a", 1));
    long snapshotId = append(file);
    ReadFileIO io =
        new ReadFileIO().failReads(file.location(), () -> new IOException("503 Slow Down"));

    NdvResult result =
        compute(
            reading(io),
            config(LG_K),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            List.of(answer(snapshotId, file)),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.ERROR);
    assertThat(causes(result.failure())).anyMatch(message -> message.contains("503 Slow Down"));
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(ALL_UP);
  }

  @Test
  public void failureAfterFileWasReadKeepsCountersOfFilesRead() {
    DataFile first = writeDataFile(table, row(1, "a", 1));
    DataFile second = writeDataFile(table, row(2, "b", 2));
    DataFile third = writeDataFile(table, row(3, "c", 3));
    List<StatisticsAnswer> answers = afterBaseWithoutScore(first, second, third);
    long snapshotId = table.currentSnapshot().snapshotId();
    ReadFileIO io =
        new ReadFileIO().failReads(second.location(), () -> new IOException("503 Slow Down"));

    NdvResult result =
        compute(
            reading(io),
            config(LG_K, 2, Long.MAX_VALUE),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            answers,
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.ERROR);
    assertThat(result.filesRead()).isEqualTo(1L);
    assertThat(result.bytesRead()).isEqualTo(first.fileSizeInBytes());
    assertColumnsLeftOut(result, third);
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(ALL_UP);
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(Flags.NONE);
  }

  @Test
  public void stopAfterFileWasReadKeepsCountersOfFilesRead() {
    DataFile first = writeDataFile(table, row(1, "a", 1));
    DataFile second = writeDataFile(table, row(2, "b", 2));
    DataFile third = writeDataFile(table, row(3, "c", 3));
    List<StatisticsAnswer> answers = afterBaseWithoutScore(first, second, third);
    long snapshotId = table.currentSnapshot().snapshotId();
    // the step is stopped while the second file is read, and its read gives up
    AtomicBoolean stopped = new AtomicBoolean(false);
    ReadFileIO io =
        new ReadFileIO()
            .failReads(
                second.location(),
                () -> {
                  stopped.set(true);
                  return new IOException("the read is cut short");
                });

    NdvResult result =
        compute(
            reading(io),
            config(LG_K, 2, Long.MAX_VALUE),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            answers,
            stopped::get);

    assertThat(result.kind()).isEqualTo(Kind.STOPPED);
    assertThat(result.blobs()).isEmpty();
    assertThat(result.filesRead()).isEqualTo(1L);
    assertThat(result.bytesRead()).isEqualTo(first.fileSizeInBytes());
    assertColumnsLeftOut(result, third);
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(ALL_UP);
    assertThat(result.transition().apply(Flags.NONE)).isEqualTo(Flags.NONE);
  }

  @Test
  public void deletedIntermediatePuffinIsRecountedFromDataFiles() {
    Record[] rows = {row(1, "a", 1), row(2, "a", 2), row(3, "b", null)};
    DataFile file = writeDataFile(table, rows);
    SketchFile puffin = writerFile(table, SCHEMA, rows);
    long snapshotId = append(file);
    table.io().deleteFile(puffin.location());

    NdvResult result =
        compute(
            snapshotId,
            Set.of(snapshotId),
            List.of(StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), puffin)),
            null);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(result.filesRead()).isEqualTo(1L);
    assertThat(result.bytesRead()).isEqualTo(file.fileSizeInBytes());
    assertThat(properties(result, 2))
        .containsEntry(NdvComputation.NDV_PROPERTY, "2")
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "3");
    NdvBounds.of(table, snapshotId, "score").written(1, 2, null).check(ndv(result, 3), LG_K);
  }

  @Test
  public void failureToReadIntermediatePuffinIsErrorNotRecount() {
    Record[] rows = {row(1, "a", 1), row(2, "a", 2), row(3, "b", null)};
    DataFile file = writeDataFile(table, rows);
    SketchFile puffin = writerFile(table, SCHEMA, rows);
    long snapshotId = append(file);
    ReadFileIO io =
        new ReadFileIO().failReads(puffin.location(), () -> new IOException("503 Slow Down"));

    NdvResult result =
        compute(
            reading(io),
            config(LG_K),
            tableConfig(null),
            snapshotId,
            Set.of(snapshotId),
            List.of(StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), puffin)),
            RUNNING);

    assertThat(result.kind()).as("a failure that a retry may fix").isEqualTo(Kind.ERROR);
    assertThat(causes(result.failure())).anyMatch(message -> message.contains("503 Slow Down"));
    assertThat(result.blobs()).isEmpty();
    assertThat(io.opened(file.location())).as("no recount of the data files").isZero();
    assertThat(result.transition().apply(ALL_UP)).isEqualTo(ALL_UP);
  }

  @Test
  public void twoAnswersWithOneSketchFileCountItsRowCountOnce() {
    Record[] firstRows = rows(0, 3);
    Record[] secondRows = rows(3, 6);
    DataFile first = writeDataFile(table, firstRows);
    DataFile second = writeDataFile(table, secondRows);
    SketchFile puffin = writerFile(table, SCHEMA, rows(0, 6));
    table.newAppend().appendFile(first).appendFile(second).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    ReadFileIO io = new ReadFileIO();

    NdvResult result =
        compute(
            reading(io),
            config(LG_K),
            tableConfig(List.of("id")),
            snapshotId,
            Set.of(snapshotId),
            List.of(
                StatisticsAnswer.ofDataFiles(snapshotId, List.of(first), puffin),
                StatisticsAnswer.ofDataFiles(snapshotId, List.of(second), puffin)),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(io.opened(puffin.location())).isEqualTo(1);
    assertThat(result.filesRead()).isZero();
    assertThat(properties(result, 1))
        .containsEntry(NdvComputation.SKETCHED_ROWS_PROPERTY, "6")
        .containsEntry(NdvComputation.NDV_PROPERTY, "6");
  }

  @Test
  public void answersOfRolledBackOwnSnapshotAreNotUnited() {
    long baseId = append(fakeFile(10L));
    setBase(baseId, ImmutableMap.of(1, longSketch(LG_K, 0, 10)), connector(baseId, 10L));
    Record[] rolledBackRows = rows(1_000, 1_005);
    DataFile rolledBackFile = writeDataFile(table, rolledBackRows);
    SketchFile puffin = writerFile(table, SCHEMA, rolledBackRows);
    long rolledBack = append(rolledBackFile);
    table.manageSnapshots().rollbackTo(baseId).commit();
    long snapshotId = append(writeDataFile(table, rows(20, 23))); // not own: the gap
    ReadFileIO io = new ReadFileIO();

    NdvResult result =
        compute(
            reading(io),
            config(LG_K),
            tableConfig(List.of("id")),
            snapshotId,
            Set.of(rolledBack),
            List.of(StatisticsAnswer.ofDataFiles(rolledBack, List.of(rolledBackFile), puffin)),
            RUNNING);

    assertThat(result.kind()).isEqualTo(Kind.BLOBS);
    assertThat(sketchOf(result, 1).getEstimate()).isEqualTo(13.0);
    assertThat(io.opened(puffin.location())).isZero();
    assertThat(io.opened(rolledBackFile.location())).isZero();
  }

  @Test
  public void boundsExactModeIsEqualityForAppendFromEmptyBase() {
    long snapshotId = append(writeDataFile(table, row(1, "a", 1), row(2, "a", 1), row(2, "b", 1)));
    NdvBounds bounds = NdvBounds.of(table, snapshotId, "id").written(1L, 2L, 2L);

    assertThat(bounds.lower()).isEqualTo(2L);
    assertThat(bounds.upper()).isEqualTo(2L);
    bounds.check(2L, LG_K);
    assertThatThrownBy(() -> bounds.check(1L, LG_K))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("exact");
    assertThatThrownBy(() -> bounds.check(3L, LG_K))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("exact");
  }

  @Test
  public void boundsExactModeAllowsOldVersionsUpToWrittenAndLiveRows() {
    DataFile first = writeDataFile(table, row(1, "a", 1), row(2, "a", 1), row(3, "a", 1));
    append(first);
    DataFile second = writeDataFile(table, row(2, "a", 1), row(3, "a", 1), row(3, "b", 1));
    table.newOverwrite().deleteFile(first).addFile(second).commit(); // 1 is gone
    long snapshotId = table.currentSnapshot().snapshotId();
    NdvBounds bounds = NdvBounds.of(table, snapshotId, "id").written(1L, 2L, 3L, 2L, 3L, 3L);

    assertThat(bounds.lower()).isEqualTo(2L);
    assertThat(bounds.upper()).isEqualTo(3L);
    assertThat(bounds.live()).isEqualTo(3L);
    bounds.check(2L, LG_K);
    bounds.check(3L, LG_K);
    assertThatThrownBy(() -> bounds.check(4L, LG_K))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("exact");
    assertThatThrownBy(() -> NdvBounds.check(3L, 2L, 3L, 2L, LG_K))
        .as("ndv is bounded by live(S)")
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("live = 2");
    assertThatThrownBy(() -> NdvBounds.check(2L, 3L, 2L, 3L, LG_K))
        .as("the test data do not agree")
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Inconsistent");
  }

  @Test
  public void boundsApproximateModeUsesThreeSigmaOfK() {
    Record[] rows = new Record[200];
    for (int index = 0; index < 200; index += 1) {
      rows[index] = row(index % 100, "a", 1);
    }
    long snapshotId = append(writeDataFile(table, rows));
    NdvBounds bounds = NdvBounds.of(table, snapshotId, "id").written(ids(0, 100));

    // K = 16: 3 / sqrt(K) = 0.75, bounds [25, min(175, live = 200)]
    assertThat(bounds.lower()).isEqualTo(100L);
    assertThat(bounds.upper()).isEqualTo(100L);
    bounds.check(25L, 4);
    bounds.check(30L, 4);
    bounds.check(175L, 4);
    assertThatThrownBy(() -> bounds.check(24L, 4))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("approximate");
    assertThatThrownBy(() -> bounds.check(176L, 4))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("approximate");
    // K = 4096: U < K, the exact mode
    assertThatThrownBy(() -> bounds.check(30L, LG_K))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("exact");
  }

  private NdvResult compute(
      long snapshotId, Set<Long> own, List<StatisticsAnswer> answers, List<String> columns) {
    return compute(table, config(LG_K), tableConfig(columns), snapshotId, own, answers, RUNNING);
  }

  private NdvResult compute(long snapshotId, Set<Long> own, List<StatisticsAnswer> answers) {
    return compute(snapshotId, own, answers, null);
  }

  /** A lag step: no own snapshots, S itself is in the gap. */
  private static NdvResult compute(
      Table target,
      IcebergSinkConfig config,
      TableSinkConfig tableConfig,
      long snapshotId,
      List<StatisticsAnswer> answers,
      BooleanSupplier stopped) {
    return compute(target, config, tableConfig, snapshotId, Set.of(), answers, stopped);
  }

  /** The table of the test, or a view of it: the step knows it as {@link #TABLE}. */
  private static NdvResult compute(
      Table target,
      IcebergSinkConfig config,
      TableSinkConfig tableConfig,
      long snapshotId,
      Set<Long> own,
      List<StatisticsAnswer> answers,
      BooleanSupplier stopped) {
    return compute(TABLE, target, config, tableConfig, snapshotId, own, answers, stopped);
  }

  /** As the step calculates it: with the identifier the table was created under. */
  private static NdvResult compute(
      TableIdentifier identifier,
      Table target,
      IcebergSinkConfig config,
      TableSinkConfig tableConfig,
      long snapshotId,
      Set<Long> own,
      List<StatisticsAnswer> answers,
      BooleanSupplier stopped) {
    return NdvComputation.compute(
        identifier, target, snapshotId, own, Set.of(), answers, config, tableConfig, stopped);
  }

  private static IcebergSinkConfig config(int lgK) {
    return config(lgK, 1_000, Long.MAX_VALUE);
  }

  private static IcebergSinkConfig config(int lgK, int maxFiles, long maxBytes) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.statisticsLgNominalEntries()).thenReturn(lgK);
    when(config.statisticsGapMaxFiles()).thenReturn(maxFiles);
    when(config.statisticsGapMaxBytes()).thenReturn(maxBytes);
    when(config.statisticsStaleDeletedRatio()).thenReturn(STALE_RATIO);
    when(config.schemaCaseInsensitive()).thenReturn(false);
    return config;
  }

  /** {@code null} columns mean the default set. */
  private static TableSinkConfig tableConfig(List<String> columns) {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.statisticsColumns()).thenReturn(columns);
    return tableConfig;
  }

  private Table reading(ReadFileIO io) {
    Table reading = spy(table);
    doReturn(io).when(reading).io();
    return reading;
  }

  private static StatisticsAnswer answer(long snapshotId, DataFile file) {
    return StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), null);
  }

  /**
   * A base without a blob of score, then an own snapshot of the three files. The first two have no
   * sketch file, the third has one of id only: id is recounted from the first two, data needs the
   * third as well and exceeds a limit of two files.
   *
   * @return the answers of the own snapshot, now the current one
   */
  private List<StatisticsAnswer> afterBaseWithoutScore(
      DataFile first, DataFile second, DataFile third) {
    long baseId = append(fakeFile(10L));
    setBase(
        baseId,
        ImmutableMap.of(1, longSketch(LG_K, 0, 10), 2, longSketch(LG_K, 0, 3)),
        TRINO_PROPERTIES);
    table.newAppend().appendFile(first).appendFile(second).appendFile(third).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    SketchFile idOnly =
        manualFile(3, 1L, ImmutableMap.of(1, typed("long", longSketch(LG_K, 3, 4))));
    return List.of(
        StatisticsAnswer.ofDataFiles(snapshotId, List.of(first, second), null),
        StatisticsAnswer.ofDataFiles(snapshotId, List.of(third), idOnly));
  }

  /** Score is without a baseline and data is over the limit by the third file. */
  private static void assertColumnsLeftOut(NdvResult result, DataFile third) {
    assertThat(result.withoutBaseline())
        .extracting(column -> column.column().name(), ColumnWithoutBaseline::reason)
        .containsExactly(tuple("score", Reason.NO_BLOB));
    assertThat(result.rejected())
        .singleElement()
        .satisfies(
            rejected -> {
              assertThat(rejected.column().name()).isEqualTo("data");
              assertThat(rejected.files()).isEqualTo(1L);
              assertThat(rejected.bytes()).isEqualTo(third.fileSizeInBytes());
            });
  }

  private long append(DataFile file) {
    table.newAppend().appendFile(file).commit();
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

  private DeleteFile equalityDeletes(long rows) {
    fileCount += 1;
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofEqualityDeletes(1)
        .withPath("mem://bucket/data/eq-deletes-" + fileCount + "-" + UUID.randomUUID())
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(rows)
        .build();
  }

  /** A data file that exists only in the metadata: not to be read. */
  private DataFile fakeFile(long records) {
    fileCount += 1;
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mem://bucket/data/fake-" + fileCount + "-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1_000L)
        .withRecordCount(records)
        .build();
  }

  private StagedChangeFile fakeStagedFile() {
    fileCount += 1;
    return new StagedChangeFile(
        STAGING + "/staged-" + fileCount + "-" + UUID.randomUUID() + ".avro",
        1_000L,
        30L,
        0,
        StagedChangeSchema.FORMAT_VERSION,
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  private static DataFile writeDataFile(Table target, Record... rows) {
    try {
      DataWriter<Record> writer =
          new GenericFileWriterFactory.Builder(target)
              .dataSchema(target.schema())
              .build()
              .newDataWriter(
                  OutputFileFactory.builderFor(target, 1, 1).build().newOutputFile(),
                  target.spec(),
                  null);
      try {
        for (Record row : rows) {
          writer.write(row);
        }
      } finally {
        writer.close();
      }

      return writer.toDataFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Record row(long id, String data, Integer score) {
    Record record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, data);
    record.set(2, score);
    return record;
  }

  /** Rows with ids from {@code from} (inclusive) to {@code to} (exclusive), no data, no score. */
  private static Record[] rows(long from, long to) {
    return LongStream.range(from, to).mapToObj(id -> row(id, null, null)).toArray(Record[]::new);
  }

  private static List<Long> ids(long from, long to) {
    return LongStream.range(from, to).boxed().collect(Collectors.toList());
  }

  private SketchFile writerFile(Table target, Schema writerSchema, Record... rows) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(TABLE, target, STAGING, "0", LG_K, null, false, writerSchema);
    for (Record row : rows) {
      sketches.update(row);
    }

    SketchFile file = sketches.write(UUID.randomUUID());
    assertThat(file).as("the writer's sketch file").isNotNull();
    return file;
  }

  private static Map.Entry<String, byte[]> typed(String type, byte[] sketch) {
    return Map.entry(type, sketch);
  }

  /** An intermediate Puffin file with the blobs given, by field id: type and sketch. */
  private SketchFile manualFile(
      int lastColumnId, long rowCount, Map<Integer, Map.Entry<String, byte[]>> blobs) {
    fileCount += 1;
    String path = STAGING + "/stats/manual-" + fileCount + "-" + UUID.randomUUID() + ".puffin";
    try (PuffinWriter writer =
        Puffin.write(table.io().newOutputFile(path))
            .createdBy("test")
            .set(ColumnSketchCollector.LAST_COLUMN_ID_PROPERTY, String.valueOf(lastColumnId))
            .set(ColumnSketchCollector.ROW_COUNT_PROPERTY, String.valueOf(rowCount))
            .build()) {
      blobs.forEach(
          (fieldId, blob) ->
              writer.add(
                  new Blob(
                      StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                      ImmutableList.of(fieldId),
                      -1L,
                      -1L,
                      ByteBuffer.wrap(blob.getValue()),
                      PuffinCompressionCodec.NONE,
                      ImmutableMap.of(ColumnSketchCollector.TYPE_PROPERTY, blob.getKey()))));
      writer.finish();
      return new SketchFile(path, writer.fileSize(), writer.footerSize());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private StatisticsFile setBase(
      long snapshotId, Map<Integer, byte[]> blobs, Map<String, String> properties) {
    return setBase(table, snapshotId, blobs, properties);
  }

  /** Writes and registers the statistics file of the snapshot, one blob per field id. */
  private StatisticsFile setBase(
      Table target, long snapshotId, Map<Integer, byte[]> blobs, Map<String, String> properties) {
    String path =
        target.location() + "/metadata/base-" + snapshotId + "-" + UUID.randomUUID() + ".puffin";
    long sequenceNumber = target.snapshot(snapshotId).sequenceNumber();
    try (PuffinWriter writer =
        Puffin.write(target.io().newOutputFile(path)).createdBy("Trino version 479").build()) {
      blobs.forEach(
          (fieldId, data) ->
              writer.add(
                  new Blob(
                      StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                      ImmutableList.of(fieldId),
                      snapshotId,
                      sequenceNumber,
                      ByteBuffer.wrap(data),
                      PuffinCompressionCodec.ZSTD,
                      properties)));
      writer.finish();
      StatisticsFile file =
          new GenericStatisticsFile(
              snapshotId,
              path,
              writer.fileSize(),
              writer.footerSize(),
              GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
      target.updateStatistics().setStatistics(file).commit();
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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

  /** A sketch of the longs from {@code from} (inclusive) to {@code to}, as a column of long. */
  private static byte[] longSketch(int lgK, long from, long to) {
    UpdateSketch sketch = SketchValues.newSketch(lgK);
    Type.PrimitiveType type = Types.LongType.get();
    for (long value = from; value < to; value += 1) {
      SketchValues.update(sketch, SketchValues.toByteBuffer(type, value));
    }

    return sketch.compact().toByteArray();
  }

  private static Map<Integer, Blob> blobs(NdvResult result) {
    Map<Integer, Blob> blobs = Maps.newLinkedHashMap();
    for (Blob blob : result.blobs()) {
      assertThat(blob.inputFields()).hasSize(1);
      blobs.put(blob.inputFields().get(0), blob);
    }

    return blobs;
  }

  private static Blob blob(NdvResult result, int fieldId) {
    Blob blob = blobs(result).get(fieldId);
    assertThat(blob).as("the blob of field %s in %s", fieldId, result).isNotNull();
    return blob;
  }

  private static Map<String, String> properties(NdvResult result, int fieldId) {
    return blob(result, fieldId).properties();
  }

  private static long ndv(NdvResult result, int fieldId) {
    return Long.parseLong(properties(result, fieldId).get(NdvComputation.NDV_PROPERTY));
  }

  private static Sketch sketchOf(NdvResult result, int fieldId) {
    return wrap(ByteBuffers.toByteArray(blob(result, fieldId).blobData()));
  }

  private static Sketch wrap(byte[] bytes) {
    return Sketches.wrapSketch(Memory.wrap(bytes), ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  private static byte[] readAll(InputFile input) throws IOException {
    byte[] bytes = new byte[(int) input.getLength()];
    try (SeekableInputStream stream = input.newStream()) {
      ByteStreams.readFully(stream, bytes);
    }

    return bytes;
  }

  private static List<String> causes(Throwable failure) {
    List<String> messages = Lists.newArrayList();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.add(String.valueOf(cause.getMessage()));
    }

    return messages;
  }
}
