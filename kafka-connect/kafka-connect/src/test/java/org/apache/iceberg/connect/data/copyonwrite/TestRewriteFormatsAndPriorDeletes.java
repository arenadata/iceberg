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
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The rewrite over the files a real table has: any of the three data file formats, and the deletes
 * a merge-on-read period left behind.
 *
 * <p>Every other test in this package writes Parquet with the default write properties and no
 * delete files, and reads each file with an empty {@code task.deletes()}. Here the rewrite is
 * committed as a plain overwrite and the table is read back through Iceberg's own reader: whatever
 * the rewriter failed to apply or to honour shows up in the table, not in a file.
 */
public class TestRewriteFormatsAndPriorDeletes {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  public void testRowsDeletedBeforeTheRewriteStayDeleted(FileFormat format) throws IOException {
    // a table leaving a merge-on-read period. The replacement files get a newer sequence number
    // than every delete already in the table, so a delete the rewrite did not apply while reading
    // is never applied again: the row comes back
    Table target = createTable(format, 2, ImmutableMap.of());
    DataFile rewritten =
        appendFile(target, row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"));
    DataFile neighbour = appendFile(target, row(11L, "k"), row(12L, "l"));
    DeleteFile equality = equalityDelete(target, 2L);
    DeleteFile fileScoped = positionDelete(target, List.of(Pair.of(rewritten.location(), 2L)));
    DeleteFile partitionScoped =
        positionDelete(
            target, List.of(Pair.of(rewritten.location(), 3L), Pair.of(neighbour.location(), 0L)));
    commitDeletes(target, equality, fileScoped, partitionScoped);
    assertThat(ContentFileUtil.isFileScoped(fileScoped)).isTrue();
    assertThat(ContentFileUtil.isFileScoped(partitionScoped)).isFalse();
    long baseSnapshotId = target.currentSnapshot().snapshotId();

    List<FileScanTask> plan = planFiles(target);
    assertThat(deleteLocations(plan, rewritten))
        .as("deletes of the rewritten file")
        .containsExactlyInAnyOrder(
            equality.location(), fileScoped.location(), partitionScoped.location());
    // whether the equality delete reaches the neighbour depends on the id bounds its format keeps
    assertThat(deleteLocations(plan, neighbour))
        .as("deletes of the neighbour")
        .contains(partitionScoped.location())
        .doesNotContain(fileScoped.location());
    ChangeSetSlice slice =
        sliceOf(
            target,
            List.of(
                Map.entry(StagedChangeSchema.OP_UPDATE, row(1L, "a2")),
                Map.entry(StagedChangeSchema.OP_INSERT, row(100L, "new"))));

    List<DataFile> written =
        new CopyOnWriteRewriter(target, 1).rewrite(baseSnapshotId, plan, slice);

    assertThat(committedRows(target, plan, written))
        .containsExactlyInAnyOrder(
            tuple(1L, "a2"), tuple(5L, "e"), tuple(12L, "l"), tuple(100L, "new"));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  public void testRowsDeletedByDeletionVectorsStayDeleted(FileFormat format) throws IOException {
    // v3 has no position delete files, only deletion vectors, which the commit drops with the
    // file they point at, so the deletion survives only in what the rewrite read
    Table target = createTable(format, 3, ImmutableMap.of());
    DataFile rewritten = appendFile(target, row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"));
    DeleteFile vector = deletionVector(target, rewritten, 2L);
    DeleteFile equality = equalityDelete(target, 2L);
    commitDeletes(target, vector, equality);
    assertThat(ContentFileUtil.isDV(vector)).isTrue();
    long baseSnapshotId = target.currentSnapshot().snapshotId();

    List<FileScanTask> plan = planFiles(target);
    assertThat(plan)
        .as("deletes attached to the planned file")
        .extracting(task -> task.deletes().size())
        .containsExactly(2);
    ChangeSetSlice slice =
        sliceOf(target, List.of(Map.entry(StagedChangeSchema.OP_UPDATE, row(1L, "a2"))));

    List<DataFile> written =
        new CopyOnWriteRewriter(target, 1).rewrite(baseSnapshotId, plan, slice);

    assertThat(committedRows(target, plan, written))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(4L, "d"));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  public void testReplacementFilesFollowTheTableWriteProperties(FileFormat format)
      throws IOException {
    // replacement files become ordinary data files of the table: its format, its target size and
    // its metrics. The planner prunes by those bounds and must see the same ones on every file
    Table target =
        createTable(
            format,
            2,
            ImmutableMap.of(
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                "1",
                TableProperties.DEFAULT_WRITE_METRICS_MODE,
                "none",
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id",
                "full"));
    // a rolling writer looks at the size every 1000 rows: enough rows for it to roll more than once
    int rowCount = 2500;
    Record[] rows = new Record[rowCount];
    List<Tuple> expected = Lists.newArrayList(tuple(1L, "a2"), tuple(10_000L, "new"));
    for (int i = 0; i < rowCount; i++) {
      long id = i + 1;
      rows[i] = row(id, "v" + id);
      if (id != 1L) {
        expected.add(tuple(id, "v" + id));
      }
    }
    appendFile(target, rows);
    long baseSnapshotId = target.currentSnapshot().snapshotId();

    List<FileScanTask> plan = planFiles(target);
    ChangeSetSlice slice =
        sliceOf(
            target,
            List.of(
                Map.entry(StagedChangeSchema.OP_UPDATE, row(1L, "a2")),
                Map.entry(StagedChangeSchema.OP_INSERT, row(10_000L, "new"))));

    List<DataFile> written =
        new CopyOnWriteRewriter(target, 1).rewrite(baseSnapshotId, plan, slice);

    assertThat(written).as("format").extracting(DataFile::format).containsOnly(format);
    // Avro files carry no column bounds whatever the metrics mode
    Set<Integer> boundedColumns = format == FileFormat.AVRO ? Set.of() : Set.of(1);
    for (DataFile file : written) {
      assertThat(columnsOf(file.lowerBounds()))
          .as("lower bounds of %s", file.location())
          .isEqualTo(boundedColumns);
      assertThat(columnsOf(file.upperBounds()))
          .as("upper bounds of %s", file.location())
          .isEqualTo(boundedColumns);
    }
    // one file for the emitted keys; under the table's default target the remainder is one more
    assertThat(written).as("target file size").hasSizeGreaterThan(2);
    assertThat(committedRows(target, plan, written)).containsExactlyInAnyOrderElementsOf(expected);
  }

  private Table createTable(FileFormat format, int formatVersion, Map<String, String> properties) {
    return catalog.createTable(
        TABLE_IDENTIFIER,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion))
            .put(TableProperties.DEFAULT_FILE_FORMAT, format.name().toLowerCase(Locale.ROOT))
            // only the data files vary: an Avro position delete file has no bounds, so a delete
            // naming one data file could not be told from a partition-scoped one
            .put(TableProperties.DELETE_DEFAULT_FILE_FORMAT, "parquet")
            .buildOrThrow());
  }

  private static Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  /** One data file, rows at positions 0, 1, 2... in the order given. */
  private static DataFile appendFile(Table target, Record... rows) throws IOException {
    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder(target)
            .dataSchema(target.schema())
            .build()
            .newDataWriter(fileFactory(target).newOutputFile(), target.spec(), null);
    try {
      for (Record row : rows) {
        writer.write(row);
      }
    } finally {
      writer.close();
    }
    DataFile file = writer.toDataFile();
    target.newAppend().appendFile(file).commit();
    return file;
  }

  private static DeleteFile equalityDelete(Table target, long id) throws IOException {
    Schema idSchema = target.schema().select("id");
    EqualityDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(target)
            .equalityDeleteRowSchema(idSchema)
            .equalityFieldIds(new int[] {1})
            .build()
            .newEqualityDeleteWriter(fileFactory(target).newOutputFile(), target.spec(), null);
    GenericRecord key = GenericRecord.create(idSchema);
    key.setField("id", id);
    try {
      writer.write(key);
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  /**
   * A position delete file: scoped to one data file when it names one, to the partition if more.
   */
  private static DeleteFile positionDelete(Table target, List<Pair<String, Long>> positions)
      throws IOException {
    List<Pair<String, Long>> sorted = Lists.newArrayList(positions);
    sorted.sort(
        Comparator.comparing((Pair<String, Long> position) -> position.first())
            .thenComparing(Pair::second));
    PositionDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(target)
            .build()
            .newPositionDeleteWriter(fileFactory(target).newOutputFile(), target.spec(), null);
    PositionDelete<Record> delete = PositionDelete.create();
    try {
      for (Pair<String, Long> position : sorted) {
        writer.write(delete.set(position.first(), position.second(), null));
      }
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private static DeleteFile deletionVector(Table target, DataFile file, long position)
      throws IOException {
    BaseDVFileWriter writer =
        new BaseDVFileWriter(
            OutputFileFactory.builderFor(target, 1, 1).format(FileFormat.PUFFIN).build(),
            path -> null);
    try {
      writer.delete(file.location(), position, target.spec(), null);
    } finally {
      writer.close();
    }
    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  private static void commitDeletes(Table target, DeleteFile... deletes) {
    RowDelta rowDelta = target.newRowDelta();
    for (DeleteFile delete : deletes) {
      rowDelta.addDeletes(delete);
    }
    rowDelta.commit();
  }

  private static OutputFileFactory fileFactory(Table target) {
    return OutputFileFactory.builderFor(target, 1, 1).build();
  }

  private static List<FileScanTask> planFiles(Table target) throws IOException {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = target.newScan().planFiles()) {
      planned.forEach(tasks::add);
    }
    return tasks;
  }

  private static List<String> deleteLocations(List<FileScanTask> plan, DataFile file) {
    return plan.stream()
        .filter(task -> task.file().location().equals(file.location()))
        .flatMap(task -> task.deletes().stream())
        .map(DeleteFile::location)
        .toList();
  }

  private static ChangeSetSlice sliceOf(Table target, List<Map.Entry<Integer, Record>> ops) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            target,
            TABLE_REFERENCE,
            ID_FIELDS,
            target.location() + "/_staging",
            "cg-connect",
            "task-0");
    long offset = 0;
    for (Map.Entry<Integer, Record> op : ops) {
      writer.write(writer.stagedRow(op.getValue(), op.getKey(), "t", 0, offset++));
    }
    return ChangeSetNormalizer.normalize(target, ID_FIELDS, writer.complete(), null, 1000);
  }

  /**
   * Commits the rewrite as a plain overwrite of the planned files and reads the table back with
   * Iceberg's reader: every delete still in the table applies wherever its sequence number reaches.
   */
  private static List<Tuple> committedRows(
      Table target, List<FileScanTask> plan, List<DataFile> written) throws IOException {
    OverwriteFiles overwrite = target.newOverwrite();
    plan.forEach(task -> overwrite.deleteFile(task.file()));
    written.forEach(overwrite::addFile);
    overwrite.commit();

    List<Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(target).build()) {
      records.forEach(record -> rows.add(tuple(record.getField("id"), record.getField("data"))));
    }
    return rows;
  }

  private static Set<Integer> columnsOf(Map<Integer, ByteBuffer> bounds) {
    return bounds == null ? Set.of() : bounds.keySet();
  }
}
