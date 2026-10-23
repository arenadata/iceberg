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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestCopyOnWriteRewriter {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
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
  public void testUpdateReplacesRow() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(update(1L, "a2"));

    List<Record> result = rewriteAndRead(baseSnapshotId, slice, 1);

    assertThat(result)
        .extracting(r -> r.getField("id"), r -> r.getField("data"))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"), tuple(3L, "c"));
  }

  @Test
  public void testDeleteRemovesRow() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(delete(2L));

    List<Record> result = rewriteAndRead(baseSnapshotId, slice, 1);

    assertThat(result)
        .extracting(r -> r.getField("id"), r -> r.getField("data"))
        .containsExactlyInAnyOrder(tuple(1L, "a"), tuple(3L, "c"));
  }

  @Test
  public void testInsertOnlyKeyNotFoundIsAppended() {
    appendRows(row(1L, "a"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(insert(100L, "new"));

    List<Record> result = rewriteAndRead(baseSnapshotId, slice, 1);

    assertThat(result)
        .extracting(r -> r.getField("id"), r -> r.getField("data"))
        .containsExactlyInAnyOrder(tuple(1L, "a"), tuple(100L, "new"));
  }

  @Test
  public void testInsertedThenDeletedFoundIsRemoved() {
    // an odd but legal chain: the change set says this key was inserted then deleted, even though
    // it physically pre-exists: the rewriter must still remove it if found, per the idempotent
    // rule the normalizer relies on
    appendRows(row(1L, "a"), row(2L, "b"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(insert(1L, "x"), delete(1L));
    assertThat(onlyState(slice)).isEqualTo(ChangeRecord.State.INSERTED_THEN_DELETED);

    List<Record> result = rewriteAndRead(baseSnapshotId, slice, 1);

    assertThat(result).extracting(r -> r.getField("id")).containsExactly(2L);
  }

  @Test
  public void testInsertedThenDeletedNotFoundIsNoOp() {
    appendRows(row(1L, "a"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(insert(999L, "x"), delete(999L));
    assertThat(onlyState(slice)).isEqualTo(ChangeRecord.State.INSERTED_THEN_DELETED);

    List<Record> result = rewriteAndRead(baseSnapshotId, slice, 1);

    assertThat(result).extracting(r -> r.getField("id")).containsExactly(1L);
  }

  @Test
  public void testOutputRowsCarryOnlyTableColumns() {
    appendRows(row(1L, "a"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(update(1L, "a2"));

    List<FileScanTask> plan = planFiles();
    List<DataFile> result = new CopyOnWriteRewriter(table, 1).rewrite(baseSnapshotId, plan, slice);

    assertThat(result).hasSize(1);
    FileScanTask outputTask = mock(FileScanTask.class);
    when(outputTask.file()).thenReturn(result.get(0));
    when(outputTask.deletes()).thenReturn(ImmutableList.of());
    when(outputTask.spec()).thenReturn(table.spec());
    when(outputTask.start()).thenReturn(0L);
    when(outputTask.length()).thenReturn(result.get(0).fileSizeInBytes());
    try (CloseableIterable<Record> records =
        AffectedFileReader.open(table.io(), outputTask, table.schema(), table.schema())) {
      Record raw = records.iterator().next();
      List<String> fieldNames = raw.struct().fields().stream().map(f -> f.name()).toList();
      assertThat(fieldNames).containsExactlyInAnyOrder("id", "data");
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  @Test
  public void testEquivalentAtOneAndMultipleRewriteThreads() {
    appendRows(row(1L, "a"));
    appendRows(row(2L, "b"));
    appendRows(row(3L, "c"));
    appendRows(row(4L, "d"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    ChangeSetSlice slice = sliceOf(update(1L, "a2"), delete(3L), insert(100L, "new"));

    List<Record> singleThreaded = rewriteAndRead(baseSnapshotId, slice, 1);
    List<Record> multiThreaded = rewriteAndRead(baseSnapshotId, slice, 4);

    assertThat(toTuples(singleThreaded))
        .containsExactlyInAnyOrderElementsOf(toTuples(multiThreaded));
    assertThat(toTuples(singleThreaded))
        .containsExactlyInAnyOrder(
            tuple(1L, "a2"), tuple(2L, "b"), tuple(4L, "d"), tuple(100L, "new"));
  }

  @ParameterizedTest(name = "rewriteThreads = {0}")
  @ValueSource(ints = {1, 2})
  public void testAMissingDataFileTheTableStillReferencesIsAPermanentFailure(int rewriteThreads) {
    appendRows(row(1L, "a"));
    appendRows(row(2L, "b"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    ChangeSetSlice slice = sliceOf(update(1L, "a2"));
    List<FileScanTask> plan = planFiles();
    assertThat(plan).hasSize(2);

    // gone from storage behind Iceberg's back (a lifecycle rule on the bucket, or
    // remove_orphan_files with too short an older-than) while the table still lists it: every
    // replan hands out the same file, and every rewrite fails on it the same way
    String missing = plan.get(0).file().location();
    table.io().deleteFile(missing);

    assertThatThrownBy(
            () ->
                new CopyOnWriteRewriter(table, rewriteThreads).rewrite(baseSnapshotId, plan, slice))
        .isInstanceOf(PermanentCopyOnWriteException.class)
        .hasMessageContaining(missing)
        .hasMessageContaining("Restore the file");
  }

  @Test
  public void testADataFileCompactedAwayBeforeItWasReadIsNotAPermanentFailure() {
    appendRows(row(1L, "a"), row(2L, "b"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    ChangeSetSlice slice = sliceOf(update(1L, "a2"));
    List<FileScanTask> plan = planFiles();
    // a worker loads its handle when the assignment arrives, before anything below happens
    Table workerTable = catalog.loadTable(TABLE_IDENTIFIER);

    // between planning and reading, a compaction replaced the file and expire_snapshots removed it:
    // the next slice is planned on the new snapshot and never asks for it again
    DataFile compacted = plan.get(0).file();
    RewriteFiles compaction = table.newRewrite().deleteFile(compacted);
    writeRows(row(1L, "a"), row(2L, "b")).forEach(compaction::addFile);
    compaction.commit();
    table.io().deleteFile(compacted.location());

    assertThatThrownBy(
            () -> new CopyOnWriteRewriter(workerTable, 1).rewrite(baseSnapshotId, plan, slice))
        .isNotInstanceOf(PermanentCopyOnWriteException.class)
        .hasMessageContaining(compacted.location());
  }

  @Test
  public void testAMissingDeleteFileIsNotTakenForAMissingDataFile() throws IOException {
    appendRows(row(1L, "a"), row(2L, "b"));
    DeleteFile delete = positionDelete(planFiles().get(0).file(), 1L);
    table.newRowDelta().addDeletes(delete).commit();
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    ChangeSetSlice slice = sliceOf(update(1L, "a2"));
    List<FileScanTask> plan = planFiles();

    // the data file is in storage and in the table; the remedy a permanent failure names is about
    // a data file, and would send the operator after the wrong one
    table.io().deleteFile(delete.location());

    assertThatThrownBy(() -> new CopyOnWriteRewriter(table, 1).rewrite(baseSnapshotId, plan, slice))
        .isNotInstanceOf(PermanentCopyOnWriteException.class)
        .hasMessageContaining(delete.location());
  }

  @Test
  public void testAFailedChunkLeavesNoFileOfTheCallBehind() {
    table =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "bydata"),
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("data").build());
    // one file per partition, a to f: two chunks, a-c and d-f
    for (long id = 1; id <= 6; id++) {
      appendRows(row(id, String.valueOf((char) ('a' + id - 1))));
    }
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    // a key in no file: every row is carried over, so each file read means a file written
    ChangeSetSlice slice = sliceOf(insert(100L, "z"));
    List<FileScanTask> plan = planFiles();
    String missing =
        plan.stream()
            .filter(task -> "f".equals(task.file().partition().get(0, String.class)))
            .findFirst()
            .orElseThrow()
            .file()
            .location();
    table.io().deleteFile(missing);

    RecordingFileIO io = new RecordingFileIO(table.io());
    Table recorded = spy(table);
    doReturn(io).when(recorded).io();
    // the task's other assignment holds the other slot, so the chunks run one after another: the
    // failing chunk runs after its sibling closed every file, and fails on f with d closed, e open.
    // On a pool of their own a chunk that had not started yet would be skipped, writing nothing
    ExecutorService oneFreeSlot = Executors.newSingleThreadExecutor();
    try {
      assertThatThrownBy(
              () ->
                  new CopyOnWriteRewriter(recorded, 2, oneFreeSlot)
                      .rewrite(baseSnapshotId, plan, slice))
          .hasMessageContaining(missing);
    } finally {
      oneFreeSlot.shutdownNow();
    }

    InMemoryFileIO storage = (InMemoryFileIO) table.io();
    assertThat(io.created())
        .as("a, b, c by the chunk that finished; d, closed as e began; e, open when f failed")
        .hasSize(5);
    assertThat(io.created().stream().filter(storage::fileExists).toList())
        .as("files the failed call wrote, still in the table's data location")
        .isEmpty();
  }

  @Test
  public void testAnErrorDuringAChunkLeavesNoFileOfTheCallBehind() {
    // an OutOfMemoryError from deep inside a read (row group decoding, say) is not a
    // RuntimeException: the failure of any part must still delete what the call had written so far
    table =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "byerror"),
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("data").build());
    // one file per partition, a to f: two chunks, a-c and d-f
    for (long id = 1; id <= 6; id++) {
      appendRows(row(id, String.valueOf((char) ('a' + id - 1))));
    }
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    // a key in no file: every row is carried over, so each file read means a file written
    ChangeSetSlice slice = sliceOf(insert(100L, "z"));
    List<FileScanTask> plan = planFiles();
    String failing =
        plan.stream()
            .filter(task -> "f".equals(task.file().partition().get(0, String.class)))
            .findFirst()
            .orElseThrow()
            .file()
            .location();

    FaultyFileIO io = new FaultyFileIO(table.io(), failing);
    Table recorded = spy(table);
    doReturn(io).when(recorded).io();
    // the task's other assignment holds the other slot, so the chunks run one after another: the
    // failing chunk runs after its sibling closed every file, and errors reading f with d, e closed
    ExecutorService oneFreeSlot = Executors.newSingleThreadExecutor();
    try {
      assertThatThrownBy(
              () ->
                  new CopyOnWriteRewriter(recorded, 2, oneFreeSlot)
                      .rewrite(baseSnapshotId, plan, slice))
          .isInstanceOf(OutOfMemoryError.class)
          .hasMessageContaining(failing);
    } finally {
      oneFreeSlot.shutdownNow();
    }

    InMemoryFileIO storage = (InMemoryFileIO) table.io();
    assertThat(io.created())
        .as("a, b, c by the chunk that finished; d, closed as e began; e, open when f errored")
        .hasSize(5);
    assertThat(io.created().stream().filter(storage::fileExists).toList())
        .as("files the errored call wrote, still in the table's data location")
        .isEmpty();
  }

  @Test
  public void testACancelledCallStopsBetweenRowsAndLeavesNoFileBehind() {
    appendRows(row(1L, "a"), row(2L, "b"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    // row 2 is carried over, a2 and c are written back: a file from each phase
    ChangeSetSlice slice = sliceOf(update(1L, "a2"), insert(3L, "c"));
    List<FileScanTask> plan = planFiles();

    RecordingFileIO io = new RecordingFileIO(table.io());
    Table recorded = spy(table);
    doReturn(io).when(recorded).io();

    // the caller gives up once the emit phase has opened its file: the carry-over file is closed by
    // then, and the emit file has one of its two rows in it
    assertThatThrownBy(
            () ->
                new CopyOnWriteRewriter(recorded, 1)
                    .rewrite(baseSnapshotId, plan, slice, 0, 1, () -> io.created().size() >= 2))
        .isInstanceOf(CancelledCopyOnWriteException.class)
        .hasMessageContaining(table.name());

    InMemoryFileIO storage = (InMemoryFileIO) table.io();
    assertThat(io.created())
        .as("the carry-over file, and the emit file open when the call stopped")
        .hasSize(2);
    assertThat(io.created().stream().filter(storage::fileExists).toList())
        .as("files the cancelled call wrote, still in the table's data location")
        .isEmpty();
  }

  @Test
  public void testTwoCallsOverTheSameSliceNeverShareAFileName() {
    appendRows(row(1L, "a"));
    appendRows(row(2L, "b"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();
    ChangeSetSlice slice = sliceOf(update(1L, "a2"), insert(3L, "c"));
    List<FileScanTask> plan = planFiles();

    // two attempts at one slice on one task: a run superseded by a newer assignment, and the run
    // replacing it: same slice, same plan, same writers. Data writers open their files to
    // overwrite, so a shared name is not refused: whichever run closes it last owns its content
    List<DataFile> superseded =
        new CopyOnWriteRewriter(table, 2).rewrite(baseSnapshotId, plan, slice);
    List<DataFile> replacement =
        new CopyOnWriteRewriter(table, 2).rewrite(baseSnapshotId, plan, slice);
    // the superseded run does not answer, and discards what it wrote
    superseded.forEach(file -> table.io().deleteFile(file.location()));

    InMemoryFileIO storage = (InMemoryFileIO) table.io();
    assertThat(locations(replacement).stream().filter(file -> !storage.fileExists(file)).toList())
        .as("files the replacement answers with, gone once the superseded run discarded its own")
        .isEmpty();
    assertThat(locations(replacement))
        .isNotEmpty()
        .doesNotContainAnyElementsOf(locations(superseded));
  }

  private static List<String> locations(List<DataFile> files) {
    return files.stream().map(DataFile::location).toList();
  }

  /** Remembers every file opened for writing through it. */
  private static final class RecordingFileIO implements FileIO {
    private final FileIO delegate;
    private final List<String> created = Lists.newCopyOnWriteArrayList();

    private RecordingFileIO(FileIO delegate) {
      this.delegate = delegate;
    }

    List<String> created() {
      return ImmutableList.copyOf(created);
    }

    @Override
    public InputFile newInputFile(String path) {
      return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return delegate.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      created.add(path);
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }
  }

  /** Records created files and raises an {@link Error} reading one chosen path. */
  private static final class FaultyFileIO implements FileIO {
    private final FileIO delegate;
    private final String failingPath;
    private final List<String> created = Lists.newCopyOnWriteArrayList();

    private FaultyFileIO(FileIO delegate, String failingPath) {
      this.delegate = delegate;
      this.failingPath = failingPath;
    }

    List<String> created() {
      return ImmutableList.copyOf(created);
    }

    @Override
    public InputFile newInputFile(String path) {
      return newInputFile(path, -1L);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      if (failingPath.equals(path)) {
        throw new OutOfMemoryError("Injected for test: " + path);
      }
      return delegate.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      created.add(path);
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }
  }

  @Test
  public void testUpdateChangingPartitionRoutesToNewPartition() {
    // the old row is removed from wherever it is actually found, and the new version is written
    // to the partition computed from the new row, never the file's own partition. This is the
    // case merge-on-read gets wrong: it routes the delete by the new row and leaves the old one
    Schema regionSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "region", Types.StringType.get()));
    PartitionSpec regionSpec = PartitionSpec.builderFor(regionSchema).identity("region").build();
    Table regionTable =
        catalog.createTable(TableIdentifier.of(NAMESPACE, "regiontbl"), regionSchema, regionSpec);
    Set<Integer> regionIdFields = Set.of(1);

    appendTo(regionTable, regionSpec, regionRow(regionSchema, 1L, "us"));
    appendTo(regionTable, regionSpec, regionRow(regionSchema, 2L, "us"));
    long baseSnapshotId = regionTable.currentSnapshot().snapshotId();

    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            regionTable,
            TABLE_REFERENCE,
            regionIdFields,
            regionTable.location() + "/_staging",
            "cg-connect",
            "task-0");
    GenericRecord updated = GenericRecord.create(regionSchema);
    updated.setField("id", 1L);
    updated.setField("region", "eu");
    writer.write(writer.stagedRow(updated, StagedChangeSchema.OP_UPDATE, "t", 0, 0L));
    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(regionTable, regionIdFields, writer.complete(), null, 1000);

    List<FileScanTask> plan = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = regionTable.newScan().planFiles()) {
      planned.forEach(plan::add);
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    List<DataFile> result =
        new CopyOnWriteRewriter(regionTable, 1).rewrite(baseSnapshotId, plan, slice);

    assertThat(result).hasSize(2);
    boolean sawUsWithOnlyId2 = false;
    boolean sawEuWithId1 = false;
    for (DataFile file : result) {
      String region = file.partition().get(0, String.class);
      List<Record> rows = readRegionFile(regionTable, regionSchema, file);
      if ("us".equals(region)) {
        assertThat(rows).extracting(r -> r.getField("id")).containsExactly(2L);
        sawUsWithOnlyId2 = true;
      } else if ("eu".equals(region)) {
        assertThat(rows).extracting(r -> r.getField("id")).containsExactly(1L);
        sawEuWithId1 = true;
      }
    }
    assertThat(sawUsWithOnlyId2).isTrue();
    assertThat(sawEuWithId1).isTrue();
  }

  @Test
  public void testEmitPhaseWritesOnePartitionInOneFileAcrossTheWholeAssignment() {
    // phase B is one sequential clustered pass per assignment: splitting owned keys into
    // rewrite-threads blocks would multiply small files by rewrite-threads whenever the key does
    // not correlate with the partition, since the same partition then recurs across several blocks
    Schema regionSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "region", Types.StringType.get()));
    PartitionSpec regionSpec = PartitionSpec.builderFor(regionSchema).identity("region").build();
    Table regionTable =
        catalog.createTable(TableIdentifier.of(NAMESPACE, "regionemit"), regionSchema, regionSpec);
    Set<Integer> regionIdFields = Set.of(1);

    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            regionTable,
            TABLE_REFERENCE,
            regionIdFields,
            regionTable.location() + "/_staging",
            "cg-connect",
            "task-0");
    // key order does not correlate with region: three keys land in each partition, spread across
    // every rewrite-threads block a naive split would make
    String[] regions = {"us", "eu", "us", "eu", "us", "eu"};
    for (int i = 0; i < regions.length; i++) {
      GenericRecord inserted = GenericRecord.create(regionSchema);
      inserted.setField("id", (long) (i + 1));
      inserted.setField("region", regions[i]);
      writer.write(writer.stagedRow(inserted, StagedChangeSchema.OP_INSERT, "t", 0, i));
    }
    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(regionTable, regionIdFields, writer.complete(), null, 1000);

    List<DataFile> result =
        new CopyOnWriteRewriter(regionTable, 3).rewrite(0L, Lists.newArrayList(), slice);

    assertThat(result).as("one file per partition, not one per (partition, block)").hasSize(2);
    for (DataFile file : result) {
      String region = file.partition().get(0, String.class);
      List<Record> rows = readRegionFile(regionTable, regionSchema, file);
      List<Long> expectedIds = "us".equals(region) ? List.of(1L, 3L, 5L) : List.of(2L, 4L, 6L);
      assertThat(rows)
          .extracting(r -> r.getField("id"))
          .containsExactlyInAnyOrderElementsOf(expectedIds);
    }
  }

  /**
   * Partition source columns whose value in a generic row is not Iceberg's internal one: a date is
   * a {@code LocalDate} in a row and a day count in a partition tuple. Two values, two partitions.
   */
  private static Stream<Arguments> partitionSources() {
    return Stream.of(
        Arguments.of(
            "identity", Types.DateType.get(), LocalDate.of(2026, 1, 1), LocalDate.of(2026, 2, 2)),
        Arguments.of(
            "identity", Types.TimeType.get(), LocalTime.of(1, 2, 3), LocalTime.of(4, 5, 6)),
        Arguments.of(
            "identity",
            Types.TimestampType.withoutZone(),
            LocalDateTime.of(2026, 1, 1, 1, 2, 3),
            LocalDateTime.of(2026, 2, 2, 4, 5, 6)),
        Arguments.of(
            "identity",
            Types.TimestampType.withZone(),
            OffsetDateTime.of(2026, 1, 1, 1, 2, 3, 0, ZoneOffset.UTC),
            OffsetDateTime.of(2026, 2, 2, 4, 5, 6, 0, ZoneOffset.UTC)),
        Arguments.of("identity", Types.FixedType.ofLength(2), new byte[] {1, 2}, new byte[] {3, 4}),
        Arguments.of(
            "day",
            Types.TimestampType.withZone(),
            OffsetDateTime.of(2026, 1, 1, 1, 2, 3, 0, ZoneOffset.UTC),
            OffsetDateTime.of(2026, 2, 2, 4, 5, 6, 0, ZoneOffset.UTC)),
        Arguments.of(
            "bucket", Types.DateType.get(), LocalDate.of(2026, 1, 1), LocalDate.of(2026, 2, 2)),
        Arguments.of("bucket", Types.FixedType.ofLength(2), new byte[] {1, 2}, new byte[] {3, 4}));
  }

  /** {@link #partitionSources()} under every data file format: each has its own read branch. */
  private static Stream<Arguments> partitionSourcesInEveryFormat() {
    return Stream.of(FileFormat.PARQUET, FileFormat.ORC, FileFormat.AVRO)
        .flatMap(
            format ->
                partitionSources()
                    .map(
                        source ->
                            Arguments.of(
                                Stream.concat(Stream.of(format), Stream.of(source.get()))
                                    .toArray())));
  }

  @ParameterizedTest(name = "{0} {1}({2})")
  @MethodSource("partitionSourcesInEveryFormat")
  public void testCarriedOverRowsKeepTheirPartitionSourceValue(
      FileFormat format, String transform, Type sourceType, Object value, Object unused) {
    // a reader puts an identity partition's value in place of the column, even where the file has
    // the column. Taken raw from the partition tuple (days for a date) it reaches the remainder
    // writer as an Integer where a LocalDate belongs. A delete-only slice keeps the emit phase out
    Table target = partitionedTable(transform, sourceType, format);
    appendTo(
        target,
        target.spec(),
        sourceRow(target, 1L, value, "a"),
        sourceRow(target, 2L, value, "b"));
    long baseSnapshotId = target.currentSnapshot().snapshotId();

    ChangeSetSlice slice =
        stagedSlice(
            target,
            Set.of(1),
            List.of(Map.entry(StagedChangeSchema.OP_DELETE, sourceRow(target, 1L, value, null))));

    List<DataFile> written =
        new CopyOnWriteRewriter(target, 1).rewrite(baseSnapshotId, planFiles(target), slice);

    assertThat(sourceColumns(target, written)).containsExactly(tuple(2L, comparable(value), "b"));
    assertEachRowInItsOwnPartition(target, written);
  }

  @Test
  public void testIdentityPartitionSourceInTheIdentifierKeyIsMatched() {
    // the key builder converts a date column to days itself and casts it to LocalDate first: a raw
    // day count from the partition tuple fails there, before the row is written at all
    Table target = partitionedTable("identity", Types.DateType.get());
    LocalDate day = LocalDate.of(2026, 1, 1);
    appendTo(
        target, target.spec(), sourceRow(target, 1L, day, "a"), sourceRow(target, 2L, day, "b"));
    long baseSnapshotId = target.currentSnapshot().snapshotId();

    ChangeSetSlice slice =
        stagedSlice(
            target,
            Set.of(1, 2),
            List.of(Map.entry(StagedChangeSchema.OP_DELETE, sourceRow(target, 1L, day, null))));

    List<DataFile> written =
        new CopyOnWriteRewriter(target, 1).rewrite(baseSnapshotId, planFiles(target), slice);

    assertThat(sourceColumns(target, written)).containsExactly(tuple(2L, day, "b"));
  }

  @ParameterizedTest(name = "{0}({1})")
  @MethodSource("partitionSources")
  public void testEmittedRowsArePartitionedByTheirNewSourceValue(
      String transform, Type sourceType, Object before, Object after) {
    // the emit phase computes each row's partition from the row itself. Partition transforms take
    // the internal representation (days, micros, ByteBuffer) and given a LocalDate,
    // OffsetDateTime or byte[] they throw: a slice with an upsert would never commit
    Table target = partitionedTable(transform, sourceType);
    appendTo(
        target,
        target.spec(),
        sourceRow(target, 1L, before, "a"),
        sourceRow(target, 2L, before, "b"));
    long baseSnapshotId = target.currentSnapshot().snapshotId();

    ChangeSetSlice slice =
        stagedSlice(
            target,
            Set.of(1),
            List.of(
                Map.entry(StagedChangeSchema.OP_UPDATE, sourceRow(target, 1L, after, "a2")),
                Map.entry(StagedChangeSchema.OP_INSERT, sourceRow(target, 3L, after, "c"))));

    List<DataFile> written =
        new CopyOnWriteRewriter(target, 1).rewrite(baseSnapshotId, planFiles(target), slice);

    assertThat(sourceColumns(target, written))
        .containsExactlyInAnyOrder(
            tuple(1L, comparable(after), "a2"),
            tuple(2L, comparable(before), "b"),
            tuple(3L, comparable(after), "c"));
    assertEachRowInItsOwnPartition(target, written);
  }

  private Table partitionedTable(String transform, Type sourceType) {
    return partitionedTable(transform, sourceType, FileFormat.PARQUET);
  }

  private Table partitionedTable(String transform, Type sourceType, FileFormat format) {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "source", sourceType),
            optional(3, "data", Types.StringType.get()));
    PartitionSpec.Builder spec = PartitionSpec.builderFor(schema);
    if ("identity".equals(transform)) {
      spec.identity("source");
    } else if ("day".equals(transform)) {
      spec.day("source");
    } else {
      spec.bucket("source", 4);
    }
    return catalog.createTable(
        TableIdentifier.of(NAMESPACE, "partitioned"),
        schema,
        spec.build(),
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, format.name().toLowerCase(Locale.ROOT)));
  }

  private static Record sourceRow(Table target, long id, Object source, String data) {
    GenericRecord record = GenericRecord.create(target.schema());
    record.setField("id", id);
    record.setField("source", source);
    record.setField("data", data);
    return record;
  }

  private static ChangeSetSlice stagedSlice(
      Table target, Set<Integer> idFields, List<Map.Entry<Integer, Record>> ops) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            target,
            TABLE_REFERENCE,
            idFields,
            target.location() + "/_staging",
            "cg-connect",
            "task-0");
    long offset = 0;
    for (Map.Entry<Integer, Record> op : ops) {
      writer.write(writer.stagedRow(op.getValue(), op.getKey(), "t", 0, offset++));
    }
    return ChangeSetNormalizer.normalize(target, idFields, writer.complete(), null, 1000);
  }

  /** {@code (id, source, data)} as the file stores them, not as a scan would substitute them. */
  private static List<org.assertj.core.groups.Tuple> sourceColumns(
      Table target, List<DataFile> files) {
    List<org.assertj.core.groups.Tuple> rows = Lists.newArrayList();
    for (DataFile file : files) {
      for (Record row : fileRows(target, file)) {
        rows.add(
            tuple(row.getField("id"), comparable(row.getField("source")), row.getField("data")));
      }
    }
    return rows;
  }

  /** Every file's partition is the one its own rows give under the table's spec. */
  private static void assertEachRowInItsOwnPartition(Table target, List<DataFile> files) {
    InternalRecordWrapper internal = new InternalRecordWrapper(target.schema().asStruct());
    for (DataFile file : files) {
      for (Record row : fileRows(target, file)) {
        PartitionKey expected = new PartitionKey(target.spec(), target.schema());
        expected.partition(internal.wrap(row));
        assertThat(file.partition().get(0, Object.class))
            .as("partition of the file holding id %s", row.getField("id"))
            .isEqualTo(expected.get(0, Object.class));
      }
    }
  }

  /** Reads the file itself: no partition constants, no delete filter. */
  private static List<Record> fileRows(Table target, DataFile file) {
    List<Record> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        openWithoutConstants(target.schema(), target.io().newInputFile(file), file.format())) {
      records.forEach(rows::add);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private static CloseableIterable<Record> openWithoutConstants(
      Schema schema, InputFile input, FileFormat format) {
    switch (format) {
      case PARQUET:
        return Parquet.read(input)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build();
      case ORC:
        return ORC.read(input)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
            .build();
      case AVRO:
        return Avro.read(input)
            .project(schema)
            .createResolvingReader(PlannedDataReader::create)
            .build();
      default:
        throw new IllegalArgumentException("Unexpected data file format: " + format);
    }
  }

  private static Object comparable(Object value) {
    return value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) : value;
  }

  @Test
  public void testNestedStructValuesAreNotSharedBetweenEmittedRows() {
    // a projection reuses one view object per nested struct column, and emitBlock buffers its rows
    // to sort them by partition before writing. Without a deep copy every row in the block ends up
    // holding the last row's nested values: silently, and only on a schema with a struct column
    Types.StructType payloadType = Types.StructType.of(optional(3, "k", Types.StringType.get()));
    Schema nestedSchema =
        new Schema(
            List.of(required(1, "id", Types.LongType.get()), optional(2, "payload", payloadType)),
            Set.of(1));
    Table nestedTable =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "nestedtbl"),
            nestedSchema,
            PartitionSpec.unpartitioned());
    nestedTable.newAppend().commit();
    long baseSnapshotId = nestedTable.currentSnapshot().snapshotId();

    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            nestedTable,
            TABLE_REFERENCE,
            Set.of(1),
            nestedTable.location() + "/_staging",
            "cg-connect",
            "task-0");
    // three, not two: with two rows a shared view still gives one of them the right value
    for (long key = 1; key <= 3; key++) {
      GenericRecord payload = GenericRecord.create(payloadType);
      payload.setField("k", "v" + key);
      GenericRecord row = GenericRecord.create(nestedSchema);
      row.setField("id", key);
      row.setField("payload", payload);
      writer.write(writer.stagedRow(row, StagedChangeSchema.OP_INSERT, "t", 0, key));
    }
    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(nestedTable, Set.of(1), writer.complete(), null, 1000);

    List<DataFile> written =
        new CopyOnWriteRewriter(nestedTable, 1)
            .rewrite(baseSnapshotId, ImmutableList.of(), slice, 0, 1);

    assertThat(readNested(nestedTable, written))
        .containsExactlyInAnyOrder(tuple(1L, "v1"), tuple(2L, "v2"), tuple(3L, "v3"));
  }

  /** Reads back {@code (id, payload.k)}, copying every row out of the reader's reused view. */
  private List<org.assertj.core.groups.Tuple> readNested(Table target, List<DataFile> files) {
    List<org.assertj.core.groups.Tuple> rows = Lists.newArrayList();
    for (DataFile file : files) {
      FileScanTask task = mock(FileScanTask.class);
      when(task.file()).thenReturn(file);
      when(task.deletes()).thenReturn(ImmutableList.of());
      when(task.spec()).thenReturn(target.spec());
      when(task.start()).thenReturn(0L);
      when(task.length()).thenReturn(file.fileSizeInBytes());
      try (CloseableIterable<Record> records =
          AffectedFileReader.open(target.io(), task, target.schema(), target.schema())) {
        for (Record record : records) {
          Record nested = (Record) record.get(1, Object.class);
          rows.add(tuple(record.get(0, Object.class), nested == null ? null : nested.get(0)));
        }
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
    return rows;
  }

  private Record regionRow(Schema schema, long id, String region) {
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", id);
    record.setField("region", region);
    return record;
  }

  /** One file, one partition: every row must fall into the partition of the first. */
  private void appendTo(Table target, PartitionSpec spec, Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            new GenericFileWriterFactory.Builder(target).dataSchema(target.schema()).build(),
            OutputFileFactory.builderFor(target, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            target.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      InternalRecordWrapper internal = new InternalRecordWrapper(target.schema().asStruct());
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(spec, target.schema());
        key.partition(internal.wrap(row));
        writer.write(row, spec, key);
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
    AppendFiles append = target.newAppend();
    writer.result().dataFiles().forEach(append::appendFile);
    append.commit();
  }

  private List<Record> readRegionFile(Table target, Schema schema, DataFile file) {
    FileScanTask task = mock(FileScanTask.class);
    when(task.file()).thenReturn(file);
    when(task.deletes()).thenReturn(ImmutableList.of());
    when(task.spec()).thenReturn(target.spec());
    when(task.start()).thenReturn(0L);
    when(task.length()).thenReturn(file.fileSizeInBytes());
    List<Record> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        AffectedFileReader.open(target.io(), task, schema, schema)) {
      for (Record record : records) {
        rows.add(
            regionRow(
                schema, (Long) record.get(0, Object.class), (String) record.get(1, Object.class)));
      }
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return rows;
  }

  private List<org.assertj.core.groups.Tuple> toTuples(List<Record> rows) {
    return rows.stream().map(r -> tuple(r.getField("id"), r.getField("data"))).toList();
  }

  private ChangeRecord.State onlyState(ChangeSetSlice slice) {
    assertThat(slice.size()).isEqualTo(1);
    return slice.changes().firstEntry().getValue().state();
  }

  @Test
  public void testDistributedRewriteMatchesSingleOwner() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"));
    // four files, so a plan that can actually be split across owners
    appendRows(row(5L, "e"));
    appendRows(row(6L, "f"));
    appendRows(row(7L, "g"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // every kind of change at once: updates, a delete, and inserts of keys in no file at all
    ChangeSetSlice slice =
        sliceOf(
            update(1L, "a2"),
            update(5L, "e2"),
            delete(3L),
            insert(100L, "new"),
            insert(101L, "newer"));
    List<FileScanTask> plan = planFiles();

    List<Record> single = readAll(rewriteAs(baseSnapshotId, plan, slice, 0, 1));

    for (int owners = 2; owners <= 4; owners++) {
      List<DataFile> distributed = Lists.newArrayList();
      for (int owner = 0; owner < owners; owner++) {
        distributed.addAll(
            rewriteAs(baseSnapshotId, filesOf(plan, owner, owners), slice, owner, owners));
      }

      assertThat(readAll(distributed))
          .as("%d owners must produce the same rows as one", owners)
          .extracting(r -> r.getField("id"), r -> r.getField("data"))
          .containsExactlyInAnyOrderElementsOf(
              single.stream()
                  .map(r -> tuple(r.getField("id"), r.getField("data")))
                  .collect(java.util.stream.Collectors.toList()));
    }
  }

  @Test
  public void testOwnerWithNoFilesStillEmitsItsKeys() {
    appendRows(row(1L, "a"));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    // two keys, two owners, and every file goes to owner 0: owner 1 has nothing to read but still
    // owns half the slice's keys
    ChangeSetSlice slice = sliceOf(update(1L, "a2"), insert(2L, "b"));
    List<FileScanTask> plan = planFiles();

    List<DataFile> written = Lists.newArrayList();
    written.addAll(rewriteAs(baseSnapshotId, plan, slice, 0, 2));
    written.addAll(rewriteAs(baseSnapshotId, ImmutableList.of(), slice, 1, 2));

    assertThat(readAll(written))
        .extracting(r -> r.getField("id"), r -> r.getField("data"))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"));
  }

  @Test
  public void testOwnershipBlocksPartitionTheSliceExactlyOnce() {
    ChangeSetSlice slice =
        sliceOf(
            update(1L, "a"), update(2L, "b"), update(3L, "c"), update(4L, "d"), update(5L, "e"));

    for (int owners = 1; owners <= 7; owners++) {
      List<Object> seen = Lists.newArrayList();
      for (int owner = 0; owner < owners; owner++) {
        slice.ownedEntries(owner, owners).forEach(entry -> seen.add(entry.getKey()));
      }
      assertThat(seen).as("%d owners", owners).hasSize(slice.size()).doesNotHaveDuplicates();
    }
  }

  private List<DataFile> rewriteAs(
      long baseSnapshotId,
      List<FileScanTask> files,
      ChangeSetSlice slice,
      int ownerIndex,
      int ownerCount) {
    return new CopyOnWriteRewriter(table, 1)
        .rewrite(baseSnapshotId, files, slice, ownerIndex, ownerCount);
  }

  /** Round-robin split of the plan: any disjoint cover will do, the rule must not depend on it. */
  private static List<FileScanTask> filesOf(List<FileScanTask> plan, int owner, int owners) {
    List<FileScanTask> mine = Lists.newArrayList();
    for (int i = owner; i < plan.size(); i += owners) {
      mine.add(plan.get(i));
    }
    return mine;
  }

  private List<Record> rewriteAndRead(
      long baseSnapshotId, ChangeSetSlice slice, int rewriteThreads) {
    List<FileScanTask> plan = planFiles();
    List<DataFile> result =
        new CopyOnWriteRewriter(table, rewriteThreads).rewrite(baseSnapshotId, plan, slice);
    return readAll(result);
  }

  private List<FileScanTask> planFiles() {
    return planFiles(table);
  }

  private static List<FileScanTask> planFiles(Table target) {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = target.newScan().planFiles()) {
      planned.forEach(tasks::add);
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return tasks;
  }

  private List<Record> readAll(List<DataFile> files) {
    List<Record> rows = Lists.newArrayList();
    for (DataFile file : files) {
      FileScanTask task = mock(FileScanTask.class);
      when(task.file()).thenReturn(file);
      when(task.deletes()).thenReturn(ImmutableList.of());
      when(task.spec()).thenReturn(table.spec());
      when(task.start()).thenReturn(0L);
      when(task.length()).thenReturn(file.fileSizeInBytes());
      try (CloseableIterable<Record> records =
          AffectedFileReader.open(table.io(), task, table.schema(), table.schema())) {
        // AffectedFileReader returns rows through a reused RecordProjection view, which only
        // supports positional access; copy each one out before it is overwritten by the next
        for (Record record : records) {
          rows.add(row((Long) record.get(0, Object.class), (String) record.get(1, Object.class)));
        }
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
    return rows;
  }

  private void appendRows(Record... rows) {
    AppendFiles append = table.newAppend();
    writeRows(rows).forEach(append::appendFile);
    append.commit();
  }

  private DeleteFile positionDelete(DataFile dataFile, long position) throws IOException {
    PositionDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .build()
            .newPositionDeleteWriter(
                OutputFileFactory.builderFor(table, 1, 1).build().newOutputFile(),
                table.spec(),
                null);
    try {
      writer.write(PositionDelete.<Record>create().set(dataFile.location(), position, null));
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private List<DataFile> writeRows(Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            writerFactory(),
            OutputFileFactory.builderFor(table, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            table.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(table.spec(), table.schema());
        key.partition(row);
        writer.write(row, table.spec(), key);
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }

    return writer.result().dataFiles();
  }

  private FileWriterFactory<Record> writerFactory() {
    return new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).build();
  }

  private Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private ChangeOp insert(long id, String data) {
    return new ChangeOp(id, StagedChangeSchema.OP_INSERT, data);
  }

  private ChangeOp update(long id, String data) {
    return new ChangeOp(id, StagedChangeSchema.OP_UPDATE, data);
  }

  private ChangeOp delete(long id) {
    return new ChangeOp(id, StagedChangeSchema.OP_DELETE, null);
  }

  private ChangeSetSlice sliceOf(ChangeOp... ops) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table,
            TABLE_REFERENCE,
            ID_FIELDS,
            table.location() + "/_staging",
            "cg-connect",
            "task-0");
    long offset = 0;
    for (ChangeOp op : ops) {
      GenericRecord tableRow = GenericRecord.create(SCHEMA);
      tableRow.setField("id", op.id);
      tableRow.setField("data", op.data);
      writer.write(writer.stagedRow(tableRow, op.code, "t", 0, offset++));
    }
    List<StagedChangeFile> files = writer.complete();
    return ChangeSetNormalizer.normalize(table, ID_FIELDS, files, null, 1_000_000);
  }

  private static final class ChangeOp {
    private final long id;
    private final int code;
    private final String data;

    private ChangeOp(long id, int code, String data) {
      this.id = id;
      this.code = code;
      this.data = data;
    }
  }
}
