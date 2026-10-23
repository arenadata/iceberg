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
import static org.assertj.core.api.Assertions.tuple;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The readers of the files the statistics step recounts. */
public class TestStatisticsFileReaders {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()),
              optional(2, "data", Types.StringType.get()),
              optional(3, "day", Types.DateType.get())),
          Set.of(1));
  private static final LocalDate DAY_ONE = LocalDate.of(2026, 9, 1);
  private static final LocalDate DAY_TWO = LocalDate.of(2026, 9, 2);

  @TempDir private File temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of());
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
  public void dataFileReadsEveryRowProjected(FileFormat format) throws IOException {
    Table table = createTable(format, PartitionSpec.unpartitioned());
    DataFile file =
        appendFile(table, row(1L, "a", DAY_ONE), row(2L, null, DAY_TWO), row(3L, "c", null));

    List<Tuple> rows = read(table, file, SCHEMA.select("id", "data"));

    assertThat(rows).containsExactly(tuple(1L, "a"), tuple(2L, null), tuple(3L, "c"));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  public void identityPartitionValueComesAsRowHoldsIt(FileFormat format) throws IOException {
    Table table = createTable(format, PartitionSpec.builderFor(SCHEMA).identity("day").build());
    DataFile file = appendFile(table, row(1L, "a", DAY_ONE), row(2L, "b", DAY_ONE));

    List<Tuple> rows = read(table, file, SCHEMA.select("day"));

    assertThat(rows).containsExactly(tuple(DAY_ONE), tuple(DAY_ONE));
  }

  @Test
  public void deletesOfFileAreNotApplied() throws IOException {
    // a sketch counts the values ever inserted: rows deleted since are still read
    Table table = createTable(FileFormat.PARQUET, PartitionSpec.unpartitioned());
    DataFile file =
        appendFile(table, row(1L, "a", DAY_ONE), row(2L, "b", DAY_ONE), row(3L, "c", DAY_ONE));
    DeleteFile equality = equalityDelete(table, 1L);
    DeleteFile position = positionDelete(table, file, 1L);
    table.newRowDelta().addDeletes(equality).addDeletes(position).commit();
    assertThat(liveIds(table)).as("the table's own reader applies the deletes").containsExactly(3L);

    List<Tuple> rows = read(table, file, SCHEMA.select("id"));

    assertThat(rows).containsExactly(tuple(1L), tuple(2L), tuple(3L));
  }

  @Test
  public void columnAddedAfterFileComesBackNull() throws IOException {
    Table table = createTable(FileFormat.PARQUET, PartitionSpec.unpartitioned());
    DataFile file = appendFile(table, row(1L, "a", DAY_ONE));
    table.updateSchema().addColumn("score", Types.IntegerType.get()).commit();

    List<Tuple> rows = read(table, file, table.schema().select("id", "score"));

    assertThat(rows).containsExactly(tuple(1L, null));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  public void fileWithoutFieldIdsIsReadByNameMapping(FileFormat format) throws IOException {
    // a file added by add_files, written before "payload" was renamed to "data"
    Table table = createTable(format, PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            FilesWithoutFieldIds.mappingWithAlias(SCHEMA, "data", "payload"))
        .commit();
    DataFile file =
        FilesWithoutFieldIds.write(
            table,
            temp,
            format,
            List.of(
                FilesWithoutFieldIds.row(1L, "n1", "a"), FilesWithoutFieldIds.row(2L, "n2", "b")));
    table.newAppend().appendFile(file).commit();

    List<Tuple> rows = read(table, file, SCHEMA.select("id", "data"));

    assertThat(rows)
        .as("the columns of the file resolved by the name mapping")
        .containsExactly(tuple(1L, "a"), tuple(2L, "b"));
  }

  @Test
  public void stagedFileYieldsOnlyInsertsAndUpdates() throws IOException {
    // a delete carries the values of the row it deletes, and adds none of them
    Table table = createTable(FileFormat.PARQUET, PartitionSpec.unpartitioned());
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table, TableReference.of("mem", TABLE), Set.of(1), null, "cg-connect", "task-0");
    writer.write(writer.stagedRow(row(1L, "a", DAY_ONE), StagedChangeSchema.OP_INSERT, "t", 0, 0));
    writer.write(writer.stagedRow(row(2L, "b", DAY_ONE), StagedChangeSchema.OP_UPDATE, "t", 0, 1));
    writer.write(writer.stagedRow(row(3L, "c", DAY_TWO), StagedChangeSchema.OP_DELETE, "t", 0, 2));
    writer.write(writer.stagedRow(row(4L, "d", DAY_TWO), StagedChangeSchema.OP_INSERT, "t", 0, 3));
    List<StagedChangeFile> files = writer.complete();
    assertThat(files).hasSize(1);

    Schema projection = SCHEMA.select("id", "data");
    List<Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        StatisticsFileReaders.stagedFile(table.io(), files.get(0), projection)) {
      for (Record record : records) {
        assertThat(record.size())
            .as("the row has the columns of the projection alone")
            .isEqualTo(2);
        rows.add(tuple(record.get(0), record.get(1)));
      }
    }

    assertThat(rows).containsExactly(tuple(1L, "a"), tuple(2L, "b"), tuple(4L, "d"));
  }

  private Table createTable(FileFormat format, PartitionSpec spec) {
    return catalog.createTable(
        TABLE,
        SCHEMA,
        spec,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.DEFAULT_FILE_FORMAT,
            format.name().toLowerCase(Locale.ROOT)));
  }

  private static List<Tuple> read(Table table, DataFile file, Schema projection)
      throws IOException {
    List<Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        StatisticsFileReaders.dataFile(
            table.io(),
            file,
            table.specs().get(file.specId()),
            projection,
            StatisticsFileReaders.nameMapping(table))) {
      for (Record record : records) {
        Object[] values = new Object[record.size()];
        for (int pos = 0; pos < values.length; pos += 1) {
          values[pos] = record.get(pos);
        }
        rows.add(tuple(values));
      }
    }

    return rows;
  }

  private static List<Long> liveIds(Table table) throws IOException {
    List<Long> ids = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      records.forEach(record -> ids.add((Long) record.getField("id")));
    }
    return ids;
  }

  private static Record row(long id, String data, LocalDate day) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("day", day);
    return record;
  }

  private static DataFile appendFile(Table table, Record... rows) throws IOException {
    PartitionKey partition = null;
    if (table.spec().isPartitioned()) {
      partition = new PartitionKey(table.spec(), table.schema());
      // the partition key wants the internal representation: a date as its day count
      partition.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(rows[0]));
    }

    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(table.schema())
            .build()
            .newDataWriter(fileFactory(table).newOutputFile(), table.spec(), partition);
    try {
      for (Record row : rows) {
        writer.write(row);
      }
    } finally {
      writer.close();
    }

    DataFile file = writer.toDataFile();
    table.newAppend().appendFile(file).commit();
    return file;
  }

  private static DeleteFile equalityDelete(Table table, long id) throws IOException {
    Schema idSchema = table.schema().select("id");
    EqualityDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .equalityDeleteRowSchema(idSchema)
            .equalityFieldIds(new int[] {1})
            .build()
            .newEqualityDeleteWriter(fileFactory(table).newOutputFile(), table.spec(), null);
    GenericRecord key = GenericRecord.create(idSchema);
    key.setField("id", id);
    try {
      writer.write(key);
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private static DeleteFile positionDelete(Table table, DataFile file, long position)
      throws IOException {
    PositionDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .build()
            .newPositionDeleteWriter(fileFactory(table).newOutputFile(), table.spec(), null);
    try {
      writer.write(PositionDelete.<Record>create().set(file.location(), position));
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private static OutputFileFactory fileFactory(Table table) {
    return OutputFileFactory.builderFor(table, 1, 1).build();
  }
}
