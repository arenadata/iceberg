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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IntermediateSketchSet.Coverage;
import org.apache.iceberg.connect.data.NdvReadPlan.Kind;
import org.apache.iceberg.connect.data.NdvReadPlan.PlannedFile;
import org.apache.iceberg.connect.data.NdvReadPlan.Recount;
import org.apache.iceberg.connect.data.NdvReadPlan.Recount.Status;
import org.apache.iceberg.connect.data.NdvReadPlan.RejectedColumn;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The list of files to read within the limit and the reading of it into sketches, with the checks
 * whether the step is stopped.
 */
public class TestNdvReadPlan {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final String STAGING = "mem://bucket/kc-staging/db.tbl/group";
  private static final int LG_K = 12;
  private static final BooleanSupplier RUNNING = () -> false;
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()),
              optional(2, "data", Types.StringType.get()),
              optional(3, "score", Types.IntegerType.get())),
          Set.of(1));
  private static final List<Types.NestedField> COLUMNS = SCHEMA.columns();

  private InMemoryCatalog catalog;
  private ReadFileIO io;
  private int fileCount = 0;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    io = new ReadFileIO();
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void gapWithinLimitIsReadForEveryColumn() {
    DataFile first = fakeFile(100L);
    DataFile second = fakeFile(100L);
    DataFile third = fakeFile(100L);
    Table table = createTable();
    Snapshot older = append(table, third);
    Snapshot newer = append(table, first, second);

    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(newer, older), List.of(), coverage(Map.of()), COLUMNS, 3L, 300L);

    assertThat(plan.kind()).isEqualTo(Kind.PLANNED);
    assertThat(plan.gapFiles()).isEqualTo(3L);
    assertThat(plan.gapBytes()).isEqualTo(300L);
    assertThat(fieldIds(plan.accepted())).containsExactly(1, 2, 3);
    assertThat(plan.rejected()).isEmpty();
    assertThat(filesAndColumns(plan))
        .containsExactly(
            Map.entry(first.location(), Set.of(1, 2, 3)),
            Map.entry(second.location(), Set.of(1, 2, 3)),
            Map.entry(third.location(), Set.of(1, 2, 3)));
  }

  @Test
  public void gapOverFileLimitStopsListingAndLooksAtNoColumn() {
    Table table = createTable();
    Snapshot oldest = append(table, fakeFile(10L));
    Snapshot middle = append(table, fakeFile(10L), fakeFile(10L));
    Snapshot newest = append(table, fakeFile(10L), fakeFile(10L));
    IntermediateSketchSet coverage = mock(IntermediateSketchSet.class);
    StatisticsAnswer uncovered = StatisticsAnswer.ofDataFiles(1L, List.of(fakeFile(10L)), null);

    NdvReadPlan plan =
        NdvReadPlan.build(
            table,
            List.of(newest, middle, oldest),
            List.of(uncovered),
            coverage,
            COLUMNS,
            3L,
            1_000L);

    assertThat(plan.kind()).isEqualTo(Kind.OVER_LIMIT);
    assertThat(plan.gapFiles()).isEqualTo(4L);
    assertThat(plan.gapBytes()).isZero();
    assertThat(plan.files()).isEmpty();
    assertThat(plan.accepted()).isEmpty();
    assertThat(plan.rejected()).isEmpty();
    assertThat(io.opened(oldest.manifestListLocation())).isZero();
    verifyNoInteractions(coverage);
  }

  @Test
  public void gapOverByteLimitStopsListing() {
    Table table = createTable();
    Snapshot oldest = append(table, fakeFile(100L));
    Snapshot middle = append(table, fakeFile(100L), fakeFile(100L));
    Snapshot newest = append(table, fakeFile(100L));

    NdvReadPlan plan =
        NdvReadPlan.build(
            table,
            List.of(newest, middle, oldest),
            List.of(),
            coverage(Map.of()),
            COLUMNS,
            1_000L,
            250L);

    assertThat(plan.kind()).isEqualTo(Kind.OVER_LIMIT);
    assertThat(plan.gapFiles()).isEqualTo(3L);
    assertThat(plan.gapBytes()).isEqualTo(300L);
    assertThat(addedManifests(oldest).stream().mapToInt(io::opened).sum()).isZero();
  }

  @Test
  public void gapOverFileLimitByItsManifestListReadsNoManifest() {
    Table table = createTable();
    Snapshot older = append(table, fakeFile(10L), fakeFile(10L), fakeFile(10L));
    Snapshot newer = append(table, fakeFile(10L), fakeFile(10L));
    List<String> manifests = addedManifests(newer, older);

    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(newer, older), List.of(), coverage(Map.of()), COLUMNS, 4L, 1_000L);

    assertThat(manifests.stream().mapToInt(io::opened).sum())
        .as("manifests opened for a gap whose added files alone exceed the limit")
        .isZero();
    assertThat(plan.kind()).isEqualTo(Kind.OVER_LIMIT);
    assertThat(plan.gapFiles()).as("the added files of the manifest lists").isEqualTo(5L);
    assertThat(plan.gapBytes()).as("no file is listed").isZero();
  }

  @Test
  public void filesOfGapAreListedWithoutTheirColumnStatistics() {
    Table table = createTable();
    ByteBuffer bound = ByteBuffer.allocate(8).putLong(0, 7L);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("mem://bucket/data/with-metrics-" + UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10L)
            .withMetrics(
                new Metrics(
                    1L,
                    Map.of(1, 8L),
                    Map.of(1, 1L),
                    Map.of(1, 0L),
                    null,
                    Map.of(1, bound),
                    Map.of(1, bound)))
            .build();
    Snapshot snapshot = append(table, file);

    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(snapshot), List.of(), coverage(Map.of()), COLUMNS, 10L, 1_000L);

    DataFile listed = plan.files().get(0).dataFile();
    assertThat(listed.valueCounts())
        .as("the gap is listed without the column statistics of its files")
        .isNull();
    assertThat(listed.lowerBounds()).isNull();
    assertThat(listed.location()).isEqualTo(file.location());
    assertThat(listed.fileSizeInBytes()).isEqualTo(10L);
    assertThat(listed.format()).isEqualTo(FileFormat.PARQUET);
  }

  @Test
  public void mergedManifestListsOnlyFilesItsSnapshotAdded() {
    assertOnlyAddedFilesOfMergedManifest("2");
  }

  @Test
  public void mergedManifestOfAV1TableListsOnlyFilesItsSnapshotAdded() {
    assertOnlyAddedFilesOfMergedManifest("1");
  }

  @Test
  public void listingOfGapStopsBetweenSnapshotsOnceStopped() {
    // snapshots with no added data file count nothing toward the limit, so only the stop ends the
    // listing of a long gap
    Table table = createTable();
    Snapshot oldest = append(table);
    Snapshot middle = append(table);
    Snapshot newest = append(table);
    BooleanSupplier stopped = () -> io.opened(newest.manifestListLocation()) > 0;

    assertThatThrownBy(
            () ->
                NdvReadPlan.build(
                    table,
                    List.of(newest, middle, oldest),
                    List.of(),
                    coverage(Map.of()),
                    COLUMNS,
                    1_000L,
                    1_000L,
                    stopped))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("The listing of the gap is stopped");

    assertThat(io.opened(newest.manifestListLocation())).isEqualTo(1);
    assertThat(io.opened(middle.manifestListLocation())).isZero();
    assertThat(io.opened(oldest.manifestListLocation())).isZero();
  }

  @Test
  public void zeroLimitWithGapIsOverLimit() {
    Table table = createTable();
    Snapshot snapshot = append(table, fakeFile(1L));

    NdvReadPlan plan =
        NdvReadPlan.build(table, List.of(snapshot), List.of(), coverage(Map.of()), COLUMNS, 0L, 0L);

    assertThat(plan.kind()).isEqualTo(Kind.OVER_LIMIT);
    assertThat(plan.gapFiles()).isEqualTo(1L);
    assertThat(plan.gapBytes()).isZero();
  }

  @Test
  public void zeroLimitWithoutGapAcceptsCoveredColumns() {
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, List.of(fakeFile(100L)), null);
    IntermediateSketchSet coverage =
        coverage(
            Map.of(answer, Map.of(1, Coverage.COVERED, 2, Coverage.EMPTY, 3, Coverage.COVERED)));

    NdvReadPlan plan =
        NdvReadPlan.build(mockTable(), List.of(), List.of(answer), coverage, COLUMNS, 0L, 0L);

    assertThat(plan.kind()).isEqualTo(Kind.PLANNED);
    assertThat(fieldIds(plan.accepted())).containsExactly(1, 2, 3);
    assertThat(plan.rejected()).isEmpty();
    assertThat(plan.files()).isEmpty();
  }

  @Test
  public void columnOverLimitIsRejectedAndOthersAccepted() {
    DataFile gapFile = fakeFile(100L);
    DataFile small = fakeFile(100L);
    DataFile large = fakeFile(5_000L);
    // no intermediate file: uncovered by every column
    StatisticsAnswer withoutSketches = StatisticsAnswer.ofDataFiles(1L, List.of(small), null);
    StatisticsAnswer withoutColumnTwo = StatisticsAnswer.ofDataFiles(2L, List.of(large), null);
    IntermediateSketchSet coverage =
        coverage(
            Map.of(
                withoutColumnTwo,
                Map.of(1, Coverage.COVERED, 2, Coverage.UNCOVERED, 3, Coverage.COVERED)));

    Table table = createTable();
    NdvReadPlan plan =
        NdvReadPlan.build(
            table,
            List.of(append(table, gapFile)),
            List.of(withoutSketches, withoutColumnTwo),
            coverage,
            COLUMNS,
            10L,
            1_000L);

    assertThat(plan.kind()).isEqualTo(Kind.PLANNED);
    assertThat(fieldIds(plan.accepted())).containsExactly(1, 3);
    assertThat(plan.rejected()).hasSize(1);
    RejectedColumn rejected = plan.rejected().get(0);
    assertThat(rejected.column().fieldId()).isEqualTo(2);
    assertThat(rejected.files()).isEqualTo(1L);
    assertThat(rejected.bytes()).isEqualTo(5_000L);
    assertThat(filesAndColumns(plan))
        .containsExactly(
            Map.entry(gapFile.location(), Set.of(1, 3)), Map.entry(small.location(), Set.of(1, 3)));
  }

  @Test
  public void filesOfSeveralColumnsCountOnce() {
    DataFile first = fakeFile(100L);
    DataFile second = fakeFile(100L);
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, List.of(first, second), null);

    // two files, 200 bytes: the three columns fit only if their common files count once
    NdvReadPlan plan =
        NdvReadPlan.build(
            mockTable(), List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 2L, 200L);

    assertThat(fieldIds(plan.accepted())).containsExactly(1, 2, 3);
    assertThat(plan.rejected()).isEmpty();
    assertThat(filesAndColumns(plan))
        .containsExactly(
            Map.entry(first.location(), Set.of(1, 2, 3)),
            Map.entry(second.location(), Set.of(1, 2, 3)));
  }

  @Test
  public void columnsAtEndOfSetAreRejectedFirst() {
    DataFile forFirst = fakeFile(100L);
    DataFile forSecond = fakeFile(100L);
    StatisticsAnswer first = StatisticsAnswer.ofDataFiles(1L, List.of(forFirst), null);
    StatisticsAnswer second = StatisticsAnswer.ofDataFiles(2L, List.of(forSecond), null);
    IntermediateSketchSet coverage =
        coverage(
            Map.of(
                first, Map.of(2, Coverage.COVERED, 3, Coverage.COVERED),
                second, Map.of(1, Coverage.COVERED, 3, Coverage.COVERED)));

    NdvReadPlan plan =
        NdvReadPlan.build(
            mockTable(), List.of(), List.of(first, second), coverage, COLUMNS, 1L, 1_000L);

    assertThat(fieldIds(plan.accepted())).containsExactly(1, 3);
    assertThat(fieldIds(Lists.transform(plan.rejected(), RejectedColumn::column)))
        .containsExactly(2);
    assertThat(filesAndColumns(plan)).containsExactly(Map.entry(forFirst.location(), Set.of(1)));
  }

  @Test
  public void stagedFilesCountTheirFileSizeBytes() {
    StagedChangeFile first = fakeStagedFile(300L);
    StagedChangeFile second = fakeStagedFile(300L);
    StatisticsAnswer answer = StatisticsAnswer.ofStagedFiles(1L, List.of(first, second), null);
    List<Types.NestedField> dataOnly = List.of(SCHEMA.findField(2));

    NdvReadPlan tooSmall =
        NdvReadPlan.build(
            mockTable(), List.of(), List.of(answer), coverage(Map.of()), dataOnly, 10L, 599L);
    NdvReadPlan enough =
        NdvReadPlan.build(
            mockTable(), List.of(), List.of(answer), coverage(Map.of()), dataOnly, 10L, 600L);

    assertThat(tooSmall.accepted()).isEmpty();
    assertThat(tooSmall.rejected()).hasSize(1);
    assertThat(tooSmall.rejected().get(0).files()).isEqualTo(2L);
    assertThat(tooSmall.rejected().get(0).bytes()).isEqualTo(600L);
    assertThat(fieldIds(enough.accepted())).containsExactly(2);
    assertThat(filesAndColumns(enough))
        .containsExactly(
            Map.entry(first.location(), Set.of(2)), Map.entry(second.location(), Set.of(2)));
    assertThat(enough.files().get(0).stagedFile()).isSameAs(first);
  }

  @Test
  public void coveredAnswersNeedNoFileAndNothingIsOpened() throws IOException {
    // the intermediate file covers every column, so no data file is opened
    Table table = createTable();
    DataFile written = writeDataFile(table, row(1L, "a", 1), row(2L, "b", 2));
    SketchFile sketchFile = writerFile(table, row(1L, "a", 1), row(2L, "b", 2));
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, List.of(written), sketchFile);
    IntermediateSketchSet coverage =
        IntermediateSketchSet.read(table, List.of(answer), SCHEMA, COLUMNS, LG_K, RUNNING);
    int openedBefore = io.openedTotal();

    NdvReadPlan plan =
        NdvReadPlan.build(table, List.of(), List.of(answer), coverage, COLUMNS, 10L, 1_000_000L);
    Recount recount = plan.read(table, SCHEMA, LG_K, RUNNING);

    assertThat(plan.kind()).isEqualTo(Kind.PLANNED);
    assertThat(plan.files()).isEmpty();
    assertThat(fieldIds(plan.accepted())).containsExactly(1, 2, 3);
    assertThat(recount.status()).isEqualTo(Status.READ);
    assertThat(recount.filesRead()).isZero();
    assertThat(io.openedTotal()).isEqualTo(openedBefore);
    assertThat(io.opened(written.location())).isZero();
  }

  @Test
  public void readsGapForAllColumnsAndAnswerForItsUncoveredColumn() throws IOException {
    Table table = createTable();
    DataFile gapFile = writeDataFile(table, row(1L, "a", 10), row(2L, null, 10));
    table.newAppend().appendFile(gapFile).commit();
    Snapshot gapSnapshot = table.currentSnapshot();
    DataFile answerFile = writeDataFile(table, row(3L, "c", 30), row(4L, "d", 40));
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(2L, List.of(answerFile), null);
    IntermediateSketchSet coverage =
        coverage(Map.of(answer, Map.of(1, Coverage.COVERED, 3, Coverage.EMPTY)));

    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(gapSnapshot), List.of(answer), coverage, COLUMNS, 10L, 1_000_000L);
    Recount recount = plan.read(table, SCHEMA, LG_K, RUNNING);

    assertThat(recount.status()).isEqualTo(Status.READ);
    assertThat(recount.sketches().keySet()).containsExactly(1, 2, 3);
    assertThat(estimate(recount, 1)).as("ids of the gap only").isEqualTo(2.0);
    assertThat(estimate(recount, 2)).as("data of both, null skipped").isEqualTo(3.0);
    assertThat(estimate(recount, 3)).as("scores of the gap only").isEqualTo(1.0);
    assertThat(recount.rowsRead(1)).isEqualTo(2L);
    assertThat(recount.rowsRead(2)).isEqualTo(4L);
    assertThat(recount.rowsRead(3)).isEqualTo(2L);
    assertThat(recount.filesRead()).isEqualTo(2L);
    assertThat(recount.bytesRead())
        .isEqualTo(gapFile.fileSizeInBytes() + answerFile.fileSizeInBytes());
    assertThat(io.opened(gapFile.location())).isEqualTo(1);
    assertThat(io.opened(answerFile.location())).isEqualTo(1);
  }

  @Test
  public void stagedFilesAddOnlyInsertsAndUpdates() throws IOException {
    // a staged file adds the values of its inserts and updates only, none of its deletes
    Table table = createTable();
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table, TableReference.of("mem", TABLE), Set.of(1), null, "cg-connect", "task-0");
    writer.write(writer.stagedRow(row(1L, "a", 1), StagedChangeSchema.OP_INSERT, "t", 0, 0));
    writer.write(writer.stagedRow(row(2L, "b", 2), StagedChangeSchema.OP_UPDATE, "t", 0, 1));
    writer.write(writer.stagedRow(row(3L, "c", 3), StagedChangeSchema.OP_DELETE, "t", 0, 2));
    writer.write(writer.stagedRow(row(4L, "d", 4), StagedChangeSchema.OP_DELETE, "t", 0, 3));
    List<StagedChangeFile> staged = writer.complete();
    StatisticsAnswer answer = StatisticsAnswer.ofStagedFiles(1L, staged, null);

    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 10L, 1_000_000L);
    Recount recount = plan.read(table, SCHEMA, LG_K, RUNNING);

    assertThat(recount.status()).isEqualTo(Status.READ);
    assertThat(estimate(recount, 1)).isEqualTo(2.0);
    assertThat(estimate(recount, 2)).isEqualTo(2.0);
    assertThat(estimate(recount, 3)).isEqualTo(2.0);
    assertThat(recount.rowsRead(2)).isEqualTo(2L);
    assertThat(recount.bytesRead()).isEqualTo(staged.get(0).fileSizeBytes());
  }

  @Test
  public void recountEqualsWriterSketchByteForByte() throws IOException {
    // the same rows give the same compact sketch on the worker and in the recount
    Schema schema =
        new Schema(
            required(1, "key", Types.UUIDType.get()),
            optional(2, "at", Types.TimestampNanoType.withoutZone()),
            optional(3, "payload", Types.BinaryType.get()));
    Table table =
        spy(
            catalog.createTable(
                TABLE,
                schema,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3")));
    doReturn(io).when(table).io();
    List<Record> rows = Lists.newArrayList();
    for (int index = 0; index < 50; index += 1) {
      Record record = GenericRecord.create(schema);
      record.set(0, UUID.nameUUIDFromBytes(new byte[] {(byte) index}));
      record.set(1, LocalDateTime.of(2026, 9, 30, 12, 0).plusNanos(index * 7L));
      record.set(2, index % 5 == 0 ? null : ByteBuffer.wrap(new byte[] {(byte) index, 1, 2}));
      rows.add(record);
    }

    ColumnSketchCollector worker =
        new ColumnSketchCollector(TABLE, table, STAGING, "0", LG_K, null, false, schema);
    rows.forEach(worker::update);
    Map<Integer, byte[]> workerBlobs = blobs(worker.write(UUID.randomUUID()));
    DataFile file = writeDataFile(table, rows.toArray(new Record[0]));
    table.newAppend().appendFile(file).commit();

    NdvReadPlan plan =
        NdvReadPlan.build(
            table,
            List.of(table.currentSnapshot()),
            List.of(),
            coverage(Map.of()),
            schema.columns(),
            10L,
            1_000_000L);
    Recount recount = plan.read(table, schema, LG_K, RUNNING);

    assertThat(recount.status()).isEqualTo(Status.READ);
    assertThat(workerBlobs.keySet()).containsExactly(1, 2, 3);
    assertThat(recount.sketches()).containsOnlyKeys(1, 2, 3);
    for (int fieldId : List.of(1, 2, 3)) {
      assertThat(recount.sketches().get(fieldId).compact().toByteArray())
          .as("the sketch of field %s", fieldId)
          .isEqualTo(workerBlobs.get(fieldId));
    }
  }

  @Test
  public void stoppedBeforeFirstFileOpensNothing() throws IOException {
    Table table = createTable();
    DataFile file = writeDataFile(table, row(1L, "a", 1));
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, List.of(file), null);
    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 10L, 1_000_000L);
    int openedBefore = io.openedTotal();

    Recount recount = plan.read(table, SCHEMA, LG_K, () -> true);

    assertThat(recount.status()).isEqualTo(Status.STOPPED);
    assertThat(recount.sketches()).isEmpty();
    assertThat(io.openedTotal()).isEqualTo(openedBefore);
  }

  @Test
  public void stopDuringEndlessFileLeavesItUnread() {
    EndlessRows rows = new EndlessRows(-1L);
    NdvReadPlan plan = planOfOneFakeFile();

    Recount recount =
        plan.read(SCHEMA, LG_K, () -> rows.produced.get() >= 15_000L, (file, projection) -> rows);

    assertThat(recount.status()).isEqualTo(Status.STOPPED);
    assertThat(rows.produced.get()).isBetween(15_000L, 20_000L);
    assertThat(rows.closed.get()).as("the reader is closed").isTrue();
  }

  @Test
  public void interruptDuringEndlessFileLeavesItUnread() {
    EndlessRows rows = new EndlessRows(12_345L);
    NdvReadPlan plan = planOfOneFakeFile();

    Recount recount;
    try {
      recount = plan.read(SCHEMA, LG_K, RUNNING, (file, projection) -> rows);
    } finally {
      assertThat(Thread.interrupted()).as("the interrupt is left in place").isTrue();
    }

    assertThat(recount.status()).isEqualTo(Status.STOPPED);
    assertThat(rows.produced.get()).isBetween(12_345L, 20_000L);
    assertThat(rows.closed.get()).as("the reader is closed").isTrue();
  }

  @Test
  public void deletesOfStagedFileCountTowardsStopCheck() {
    // a staged file of deletes alone is not read to its end once the step is stopped
    Table table = createTable();
    StatisticsAnswer answer =
        StatisticsAnswer.ofStagedFiles(1L, deletesAndOneInsert(table, 25_000), null);
    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 10L, 100_000_000L);
    AtomicInteger checks = new AtomicInteger();

    Recount recount = plan.read(table, SCHEMA, LG_K, () -> checks.incrementAndGet() > 1);

    assertThat(recount.status()).isEqualTo(Status.STOPPED);
    assertThat(checks.get()).as("checked before the file and after 10 000 rows").isEqualTo(2);
  }

  @Test
  public void stagedFileRowsReadCountOnlyInsertsAndUpdates() {
    Table table = createTable();
    StatisticsAnswer answer =
        StatisticsAnswer.ofStagedFiles(1L, deletesAndOneInsert(table, 25_000), null);
    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 10L, 100_000_000L);

    Recount recount = plan.read(table, SCHEMA, LG_K, RUNNING);

    assertThat(recount.status()).isEqualTo(Status.READ);
    assertThat(recount.rowsRead(1)).isEqualTo(1L);
    assertThat(estimate(recount, 1)).isEqualTo(1.0);
    assertThat(estimate(recount, 2)).isEqualTo(1.0);
  }

  @Test
  public void failedReadIsFailureWithException() throws IOException {
    Table table = createTable();
    DataFile first = writeDataFile(table, row(1L, "a", 1));
    DataFile broken = writeDataFile(table, row(2L, "b", 2));
    io.failReads(broken.location(), () -> new IOException("503 Slow Down"));
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, List.of(first, broken), null);
    NdvReadPlan plan =
        NdvReadPlan.build(
            table, List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 10L, 1_000_000L);

    Recount recount = plan.read(table, SCHEMA, LG_K, RUNNING);

    assertThat(recount.status()).isEqualTo(Status.FAILED);
    assertThat(recount.failure()).isNotNull();
    assertThat(causes(recount.failure())).contains("503 Slow Down");
    assertThat(recount.sketches()).isEmpty();
  }

  @Test
  public void valuesOfNarrowerTypeAreReadAsWidenedTypeOfS() {
    // the column was widened after the worker wrote the event: int -> long, float -> double
    Schema widened =
        new Schema(
            List.of(
                required(1, "id", Types.LongType.get()),
                optional(2, "data", Types.StringType.get()),
                optional(3, "score", Types.DoubleType.get())),
            Set.of(1));
    NdvReadPlan plan = planOfOneFakeFile();

    Recount recount =
        plan.read(
            widened,
            LG_K,
            RUNNING,
            (file, projection) -> {
              Record record = GenericRecord.create(projection);
              record.set(0, 7);
              record.set(1, "a");
              record.set(2, 1.5f);
              return CloseableIterable.withNoopClose(List.of(record));
            });

    assertThat(recount.status()).isEqualTo(Status.READ);
    UpdateSketch id = SketchValues.newSketch(LG_K);
    SketchValues.update(id, SketchValues.toByteBuffer(Types.LongType.get(), 7L));
    UpdateSketch score = SketchValues.newSketch(LG_K);
    SketchValues.update(score, SketchValues.toByteBuffer(Types.DoubleType.get(), 1.5d));
    assertThat(recount.sketches().get(1).compact().toByteArray())
        .isEqualTo(id.compact().toByteArray());
    assertThat(recount.sketches().get(3).compact().toByteArray())
        .isEqualTo(score.compact().toByteArray());
  }

  @Test
  public void failureAfterStopIsStop() {
    AtomicBoolean stopped = new AtomicBoolean(false);
    NdvReadPlan plan = planOfOneFakeFile();

    Recount recount =
        plan.read(
            SCHEMA,
            LG_K,
            stopped::get,
            (file, projection) -> {
              stopped.set(true);
              throw new UncheckedIOException(new InterruptedIOException("interrupted"));
            });

    assertThat(recount.status()).isEqualTo(Status.STOPPED);
    assertThat(recount.failure()).isNull();
  }

  private static List<StagedChangeFile> deletesAndOneInsert(Table table, int deletes) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table, TableReference.of("mem", TABLE), Set.of(1), null, "cg-connect", "task-0");
    for (int index = 0; index < deletes; index += 1) {
      writer.write(
          writer.stagedRow(row(index, "d", index), StagedChangeSchema.OP_DELETE, "t", 0, index));
    }
    writer.write(
        writer.stagedRow(row(-1L, "a", -1), StagedChangeSchema.OP_INSERT, "t", 0, deletes));
    return writer.complete();
  }

  private NdvReadPlan planOfOneFakeFile() {
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, List.of(fakeFile(100L)), null);
    NdvReadPlan plan =
        NdvReadPlan.build(
            mockTable(), List.of(), List.of(answer), coverage(Map.of()), COLUMNS, 10L, 1_000L);
    assertThat(plan.files()).hasSize(1);
    return plan;
  }

  /** Coverage by answer and field id; a pair not given is uncovered. */
  private static IntermediateSketchSet coverage(
      Map<StatisticsAnswer, Map<Integer, Coverage>> byAnswer) {
    IntermediateSketchSet coverage = mock(IntermediateSketchSet.class);
    when(coverage.coverage(any(), anyInt()))
        .thenAnswer(
            invocation -> {
              Map<Integer, Coverage> columns =
                  byAnswer.getOrDefault(invocation.getArgument(0), Map.of());
              return columns.getOrDefault(invocation.<Integer>getArgument(1), Coverage.UNCOVERED);
            });
    return coverage;
  }

  private Table mockTable() {
    Table table = mock(Table.class);
    when(table.io()).thenReturn(io.refuseAll());
    when(table.name()).thenReturn("mem." + TABLE);
    return table;
  }

  private Table createTable() {
    return createTable(ImmutableMap.of());
  }

  private Table createTable(Map<String, String> properties) {
    Table table =
        spy(catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned(), properties));
    doReturn(io).when(table).io();
    return table;
  }

  private void assertOnlyAddedFilesOfMergedManifest(String formatVersion) {
    Table table =
        createTable(
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                formatVersion,
                TableProperties.MANIFEST_MIN_MERGE_COUNT,
                "2"));
    DataFile existing = fakeFile(10L);
    append(table, existing);
    DataFile added = fakeFile(20L);
    Snapshot snapshot = append(table, added);
    List<ManifestFile> manifests = snapshot.dataManifests(new InMemoryFileIO());
    assertThat(manifests).as("one manifest of the snapshot, merged").hasSize(1);
    assertThat(manifests.get(0).snapshotId()).isEqualTo(snapshot.snapshotId());
    assertThat(manifests.get(0).existingFilesCount()).isEqualTo(1);

    NdvReadPlan plan =
        NdvReadPlan.build(
            table,
            List.of(table.snapshot(snapshot.snapshotId())),
            List.of(),
            coverage(Map.of()),
            COLUMNS,
            10L,
            1_000L);

    assertThat(plan.kind()).isEqualTo(Kind.PLANNED);
    assertThat(Lists.transform(plan.files(), PlannedFile::location))
        .containsExactly(added.location());
    assertThat(plan.gapFiles()).isEqualTo(1L);
    assertThat(plan.gapBytes()).isEqualTo(20L);
  }

  /** Commits the files in one append; its snapshot comes as a newly loaded table has it. */
  private Snapshot append(Table table, DataFile... files) {
    AppendFiles append = table.newAppend();
    for (DataFile file : files) {
      append.appendFile(file);
    }

    append.commit();
    return catalog.loadTable(TABLE).snapshot(table.currentSnapshot().snapshotId());
  }

  /** The manifests the snapshots wrote, read with an IO the test does not count. */
  private static List<String> addedManifests(Snapshot... snapshots) {
    FileIO plain = new InMemoryFileIO();
    List<String> paths = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      for (ManifestFile manifest : snapshot.dataManifests(plain)) {
        if (manifest.snapshotId() == snapshot.snapshotId()) {
          paths.add(manifest.path());
        }
      }
    }

    return paths;
  }

  private DataFile fakeFile(long sizeBytes) {
    fileCount += 1;
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mem://bucket/data/fake-" + fileCount + "-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(1L)
        .build();
  }

  private StagedChangeFile fakeStagedFile(long sizeBytes) {
    fileCount += 1;
    return new StagedChangeFile(
        STAGING + "/staged-" + fileCount + "-" + UUID.randomUUID() + ".avro",
        sizeBytes,
        1L,
        0,
        StagedChangeSchema.FORMAT_VERSION,
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  private static Record row(long id, String data, Integer score) {
    Record record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, data);
    record.set(2, score);
    return record;
  }

  private static DataFile writeDataFile(Table table, Record... rows) throws IOException {
    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(table.schema())
            .build()
            .newDataWriter(
                OutputFileFactory.builderFor(table, 1, 1).build().newOutputFile(),
                table.spec(),
                null);
    try {
      for (Record row : rows) {
        writer.write(row);
      }
    } finally {
      writer.close();
    }
    return writer.toDataFile();
  }

  private SketchFile writerFile(Table table, Record... rows) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(TABLE, table, STAGING, "0", LG_K, null, false, SCHEMA);
    for (Record row : rows) {
      sketches.update(row);
    }

    SketchFile file = sketches.write(UUID.randomUUID());
    assertThat(file).as("the writer's sketch file").isNotNull();
    return file;
  }

  private Map<Integer, byte[]> blobs(SketchFile file) throws IOException {
    assertThat(file).as("the writer's sketch file").isNotNull();
    Map<Integer, byte[]> blobs = Maps.newTreeMap();
    try (PuffinReader reader =
        Puffin.read(io.newInputFile(file.location()))
            .withFileSize(file.fileSizeBytes())
            .withFooterSize(file.footerSizeBytes())
            .build()) {
      List<BlobMetadata> metadata = reader.fileMetadata().blobs();
      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(metadata)) {
        blobs.put(blob.first().inputFields().get(0), ByteBuffers.toByteArray(blob.second()));
      }
    }
    return blobs;
  }

  private static double estimate(Recount recount, int fieldId) {
    UpdateSketch sketch = recount.sketches().get(fieldId);
    assertThat(sketch).as("the sketch of field %s", fieldId).isNotNull();
    return sketch.getEstimate();
  }

  private static List<Integer> fieldIds(List<Types.NestedField> columns) {
    return columns.stream().map(Types.NestedField::fieldId).collect(Collectors.toList());
  }

  private static List<Map.Entry<String, Set<Integer>>> filesAndColumns(NdvReadPlan plan) {
    List<Map.Entry<String, Set<Integer>>> files = Lists.newArrayList();
    for (PlannedFile file : plan.files()) {
      files.add(Map.entry(file.location(), ImmutableSet.copyOf(file.fieldIds())));
    }
    return files;
  }

  private static List<String> causes(Throwable failure) {
    List<String> messages = Lists.newArrayList();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.add(cause.getMessage());
    }
    return messages;
  }

  /** A reader that never runs out of rows, and can interrupt its thread on a given row. */
  private static class EndlessRows implements CloseableIterable<Record> {
    private final AtomicLong produced = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long interruptAt;

    private EndlessRows(long interruptAt) {
      this.interruptAt = interruptAt;
    }

    @Override
    public CloseableIterator<Record> iterator() {
      Iterator<Record> rows =
          new Iterator<>() {
            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public Record next() {
              long count = produced.incrementAndGet();
              if (count == interruptAt) {
                Thread.currentThread().interrupt();
              }

              Record record = GenericRecord.create(SCHEMA.select("id", "data", "score"));
              record.set(0, count);
              record.set(1, "value-" + count);
              record.set(2, (int) count);
              return record;
            }
          };
      return CloseableIterator.withClose(rows);
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }
}
