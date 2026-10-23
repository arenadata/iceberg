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
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.SketchFiles.RecordingFileIO;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * What the copy-on-write staging writer puts into a staged file, read back from the file.
 *
 * <p>The normalizer, rewriter and committer tests build staged rows through {@code
 * StagedChangeFileWriter.stagedRow} with literal operations and coordinates, so the mapping from a
 * {@link SinkRecord} is asserted only here.
 */
public class TestCopyOnWriteStagingWriter {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = ImmutableSet.of(1, 2);
  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "tenant_id", Types.LongType.get()),
              required(2, "order_id", Types.LongType.get()),
              optional(3, "status", Types.StringType.get())),
          ID_FIELDS);
  private static final String OP_FIELD = "op";
  private static final UUID COMMIT_ID = UUID.randomUUID();
  private static final int LG_K = 12;

  // a required column outside the key: the table a delete carrying only the key must still fit
  private static final TableIdentifier ORDERS_IDENTIFIER = TableIdentifier.of(NAMESPACE, "orders");
  private static final Set<Integer> ORDERS_ID_FIELDS = ImmutableSet.of(1);
  private static final Schema ORDERS_SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()),
              required(2, "status", Types.StringType.get())),
          ORDERS_ID_FIELDS);

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
  public void testCdcOperationsMapOntoOpCodes() throws IOException {
    CopyOnWriteStagingWriter writer = writer(cdcConfig(false));

    writer.write(record("orders", 0, 10L, row(1L, 1L, "c", "new")));
    writer.write(record("orders", 0, 11L, row(1L, 2L, "u", "paid")));
    // a delete carries the key and nothing else
    writer.write(record("orders", 0, 12L, row(1L, 3L, "d", null)));

    assertThat(stagedRows(writer))
        .extracting(row -> row.getField("order_id"), row -> row.getField(StagedChangeSchema.OP))
        .containsExactly(
            tuple(1L, StagedChangeSchema.OP_INSERT),
            tuple(2L, StagedChangeSchema.OP_UPDATE),
            tuple(3L, StagedChangeSchema.OP_DELETE));
  }

  @ParameterizedTest(name = "upsert {0}")
  @ValueSource(booleans = {false, true})
  public void testIgnoredOperationsAndTombstonesStageNoRow(boolean upsert) throws IOException {
    CopyOnWriteStagingWriter writer = writer(cdcConfig(upsert));

    writer.write(record("orders", 0, 20L, row(1L, 1L, "m", "ignored")));
    writer.write(record("orders", 0, 21L, null));
    writer.write(record("orders", 0, 22L, row(1L, 2L, "c", "new")));

    assertThat(stagedRows(writer))
        .extracting(row -> row.getField(StagedChangeSchema.OFFSET))
        .containsExactly(22L);
  }

  @ParameterizedTest(name = "upsert {0}")
  @ValueSource(booleans = {false, true})
  public void testWithoutAMappedOperationUpsertStagesAnUpdateOtherwiseAnInsert(boolean upsert)
      throws IOException {
    int expected = upsert ? StagedChangeSchema.OP_UPDATE : StagedChangeSchema.OP_INSERT;

    CopyOnWriteStagingWriter withCdcField = writer(cdcConfig(upsert));
    withCdcField.write(record("orders", 0, 30L, row(1L, 1L, null, "no operation")));
    withCdcField.write(record("orders", 0, 31L, row(1L, 2L, "x", "unmapped operation")));
    assertThat(stagedRows(withCdcField))
        .extracting(row -> row.getField(StagedChangeSchema.OP))
        .containsExactly(expected, expected);

    // without the CDC field a value that would map to a delete is just data
    CopyOnWriteStagingWriter withoutCdcField = writer(config(upsert));
    withoutCdcField.write(record("orders", 0, 32L, row(1L, 3L, "d", "no cdc field")));
    assertThat(stagedRows(withoutCdcField))
        .extracting(row -> row.getField(StagedChangeSchema.OP))
        .containsExactly(expected);
  }

  @Test
  public void testSourceCoordinatesComeFromTheSinkRecord() throws IOException {
    CopyOnWriteStagingWriter writer = writer(cdcConfig(true));

    // the same offset in two partitions: only the partition keeps the two rows apart
    writer.write(record("orders", 0, 5L, row(1L, 1L, "u", "a")));
    writer.write(record("orders", 1, 5L, row(1L, 2L, "u", "b")));
    writer.write(record("payments", 3, 7L, row(1L, 3L, "u", "c")));

    assertThat(stagedRows(writer))
        .extracting(
            row -> row.getField(StagedChangeSchema.TOPIC),
            row -> row.getField(StagedChangeSchema.PARTITION),
            row -> row.getField(StagedChangeSchema.OFFSET))
        .containsExactly(tuple("orders", 0, 5L), tuple("orders", 1, 5L), tuple("payments", 3, 7L));
  }

  @Test
  public void testARecordWithoutAnIdentifierValueFailsNamingTheField() {
    Map<String, Object> missing = row(1L, 1L, "u", "a");
    missing.remove("order_id");
    assertFailsNaming(missing, "order_id");

    Map<String, Object> nullValue = row(1L, 1L, "u", "a");
    nullValue.put("order_id", null);
    assertFailsNaming(nullValue, "order_id");

    Map<String, Object> neither = row(1L, 1L, "u", "a");
    neither.remove("tenant_id");
    neither.remove("order_id");
    assertFailsNaming(neither, "tenant_id", "order_id");
  }

  @Test
  public void testTheStagedFileKeepsTheKeyRequiredAndMakesEveryOtherColumnOptional()
      throws IOException {
    Table orders = catalog.createTable(ORDERS_IDENTIFIER, ORDERS_SCHEMA);
    CopyOnWriteStagingWriter writer = ordersWriter(orders);

    // a full row image, so the file is written whatever the schema says about nulls
    writer.write(record("orders", 0, 40L, ordersRow(7L, "u", "paid")));

    List<StagedChangeFile> files = stagedFiles(writer);
    assertThat(files).hasSize(1);
    assertThat(fileSchema(orders, files.get(0)).columns())
        .extracting(
            NestedField::fieldId, NestedField::name, NestedField::isOptional, NestedField::type)
        .containsExactly(
            tuple(1, "id", false, Types.LongType.get()),
            tuple(2, "status", true, Types.StringType.get()),
            tuple(Integer.MAX_VALUE - 501, "_op", false, Types.IntegerType.get()),
            tuple(Integer.MAX_VALUE - 502, "_topic", false, Types.StringType.get()),
            tuple(Integer.MAX_VALUE - 503, "_partition", false, Types.IntegerType.get()),
            tuple(Integer.MAX_VALUE - 504, "_offset", false, Types.LongType.get()));
  }

  @Test
  public void testACdcDeleteStagesIntoATableWithARequiredNonIdentifierColumn() throws IOException {
    Table orders = catalog.createTable(ORDERS_IDENTIFIER, ORDERS_SCHEMA);
    CopyOnWriteStagingWriter writer = ordersWriter(orders);

    // the delete carries the key alone; status is required in the table
    writer.write(record("orders", 0, 41L, ordersRow(7L, "d", null)));

    assertThat(readRows(orders, ORDERS_ID_FIELDS, stagedFiles(writer)))
        .extracting(
            row -> row.getField("id"),
            row -> row.getField("status"),
            row -> row.getField(StagedChangeSchema.OP))
        .containsExactly(tuple(7L, null, StagedChangeSchema.OP_DELETE));
  }

  @Test
  public void testKeyBoundsAreTheMinimumAndMaximumOfEachIdentifierColumn() {
    CopyOnWriteStagingWriter writer = writer(cdcConfig(true));

    // Kafka order is not key order: no bound sits in the first or the last row, and the two key
    // columns take theirs from different rows
    writer.write(record("orders", 0, 50L, row(2L, 5L, "u", "a")));
    writer.write(record("orders", 0, 51L, row(1L, 9L, "u", "b")));
    writer.write(record("orders", 0, 52L, row(3L, 1L, "u", "c")));
    writer.write(record("orders", 0, 53L, row(2L, 4L, "u", "d")));

    List<StagedChangeFile> files = stagedFiles(writer);
    assertThat(files).hasSize(1);
    Map<Integer, ByteBuffer> lower = files.get(0).lowerBounds();
    Map<Integer, ByteBuffer> upper = files.get(0).upperBounds();
    assertThat(lower)
        .as("lower bounds, decoded: %s", decoded(lower))
        .isEqualTo(ImmutableMap.of(1, longBound(1L), 2, longBound(1L)));
    assertThat(upper)
        .as("upper bounds, decoded: %s", decoded(upper))
        .isEqualTo(ImmutableMap.of(1, longBound(3L), 2, longBound(9L)));
  }

  @Test
  public void testDeleteIsNotSketchedInsertAndUpdateAre() {
    RecordingFileIO io = new RecordingFileIO();
    CopyOnWriteStagingWriter writer = writerWithSketches(cdcConfig(false), io);

    writer.write(record("orders", 0, 60L, row(1L, 1L, "c", "new")));
    writer.write(record("orders", 0, 61L, row(1L, 2L, "u", "paid")));
    // a delete carries the key and nothing else, and is not a value that entered the table
    writer.write(record("orders", 0, 62L, row(1L, 3L, "d", null)));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    assertThat(result.stagedFiles()).hasSize(1);
    SketchFiles.Content content = SketchFiles.read(io, result.sketchFile());
    assertThat(content.properties()).containsEntry("row-count", "2");
    assertThat(content.ndv()).containsOnlyKeys(1, 2, 3);
    assertThat(content.ndv()).containsEntry(1, 1.0).containsEntry(2, 2.0).containsEntry(3, 2.0);
    assertThat(result.sketchFile().location())
        .startsWith(stagingDirectory() + "/stats/" + COMMIT_ID + "-task-0-")
        .endsWith(".puffin");
  }

  @ParameterizedTest(name = "upsert {0}")
  @ValueSource(booleans = {false, true})
  public void testRowWithoutMappedOperationIsSketched(boolean upsert) {
    RecordingFileIO io = new RecordingFileIO();
    CopyOnWriteStagingWriter writer = writerWithSketches(cdcConfig(upsert), io);

    writer.write(record("orders", 0, 63L, row(1L, 1L, null, "no operation")));
    writer.write(record("orders", 0, 64L, row(1L, 2L, "x", "unmapped operation")));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    SketchFiles.Content content = SketchFiles.read(io, result.sketchFile());
    assertThat(content.properties()).containsEntry("row-count", "2");
    assertThat(content.ndv()).containsEntry(2, 2.0).containsEntry(3, 2.0);
  }

  @Test
  public void testOnlyDeletesStagePuffinWithEmptyBlobs() {
    RecordingFileIO io = new RecordingFileIO();
    CopyOnWriteStagingWriter writer = writerWithSketches(cdcConfig(false), io);

    writer.write(record("orders", 0, 65L, row(1L, 1L, "d", null)));
    writer.write(record("orders", 0, 66L, row(1L, 2L, "d", null)));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    // the coordinator cannot tell from the staged files that no row was inserted: the file says so
    assertThat(result.stagedFiles()).hasSize(1);
    SketchFiles.Content content = SketchFiles.read(io, result.sketchFile());
    assertThat(content.properties())
        .containsEntry("last-column-id", "3")
        .containsEntry("row-count", "0");
    assertThat(content.ndv()).containsEntry(1, 0.0).containsEntry(2, 0.0).containsEntry(3, 0.0);
  }

  @Test
  public void testNothingStagedWritesNoSketchFile() {
    RecordingFileIO io = new RecordingFileIO();
    CopyOnWriteStagingWriter staging = writerWithSketches(cdcConfig(false), io);
    staging.write(record("orders", 0, 66L, row(1L, 1L, "c", "new")));
    assertThat(onlyResult(staging.complete(COMMIT_ID)).sketchFile())
        .as("a file for a staged row")
        .isNotNull();

    RecordingFileIO otherIo = new RecordingFileIO();
    CopyOnWriteStagingWriter writer = writerWithSketches(cdcConfig(false), otherIo);
    writer.write(record("orders", 0, 67L, row(1L, 1L, "m", "ignored")));
    writer.write(record("orders", 0, 68L, null));

    assertThat(writer.complete(COMMIT_ID)).isEmpty();
    assertThat(otherIo.statsFilesAttempted()).isEmpty();
  }

  @Test
  public void testWithoutSketchesNoSketchFileIsWritten() {
    RecordingFileIO io = new RecordingFileIO();
    CopyOnWriteStagingWriter writer =
        new CopyOnWriteStagingWriter(
            tableWithFileIo(io), TABLE_REFERENCE, cdcConfig(false), ID_FIELDS);

    writer.write(record("orders", 0, 69L, row(1L, 1L, "c", "new")));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    assertThat(result.stagedFiles()).hasSize(1);
    assertThat(result.sketchFile()).isNull();
    assertThat(io.otherFiles()).as("the staged file was written").isNotEmpty();
    assertThat(io.statsFiles()).isEmpty();
  }

  @Test
  public void testFailingSketchUpdateDoesNotFailWrite() {
    RecordingFileIO io = new RecordingFileIO();
    AtomicInteger updates = new AtomicInteger();
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER,
            SketchFiles.tableWith(io),
            stagingDirectory(),
            "task-0",
            LG_K,
            null,
            false,
            table.schema()) {
          @Override
          public void update(Record row) {
            updates.incrementAndGet();
            throw new IllegalStateException("update failed");
          }
        };
    CopyOnWriteStagingWriter writer =
        new CopyOnWriteStagingWriter(
            tableWithFileIo(io), TABLE_REFERENCE, cdcConfig(false), ID_FIELDS, sketches);

    writer.write(record("orders", 0, 70L, row(1L, 1L, "c", "new")));
    writer.write(record("orders", 0, 71L, row(1L, 2L, "c", "new")));
    writer.write(record("orders", 0, 72L, row(1L, 3L, "u", "paid")));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    // the sketches lack the rows of the failed updates: their file would understate the NDV
    assertThat(result.sketchFile()).as("no file of sketches that missed rows").isNull();
    assertThat(io.statsFilesAttempted()).isEmpty();
    assertThat(updates).as("the collector is not asked again in the cycle").hasValue(1);
    assertThat(result.stagedFiles()).as("the rows are staged").hasSize(1);
    assertThat(result.stagedFiles().get(0).recordCount()).isEqualTo(3L);
    assertThat(result.sourceTopics()).containsExactly("orders");
  }

  @Test
  public void testFailingSketchFileWriteKeepsStagedFiles() {
    RecordingFileIO io = new RecordingFileIO().failOnStatsFiles();
    CopyOnWriteStagingWriter writer = writerWithSketches(cdcConfig(false), io);

    writer.write(record("orders", 0, 70L, row(1L, 1L, "c", "new")));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    assertThat(io.statsFilesAttempted()).as("the writer tried the file").hasSize(1);
    assertThat(result.stagedFiles()).as("the changes are reported").hasSize(1);
    assertThat(result.sketchFile()).as("no reference to a file that is not there").isNull();
    assertThat(result.sourceTopics()).containsExactly("orders");
  }

  @Test
  public void testStatisticsThatFailInEveryWayLeaveStagedResultAsWithoutThem() throws IOException {
    RecordingFileIO io = new RecordingFileIO().failOnStatsFiles();
    CopyOnWriteStagingWriter failing =
        new CopyOnWriteStagingWriter(
            tableWithFileIo(io),
            TABLE_REFERENCE,
            cdcConfig(false),
            ID_FIELDS,
            unhashableSketches(io));
    CopyOnWriteStagingWriter plain = writer(cdcConfig(false));
    // the control: where the file can be written, it has no blob, so every value failed to hash
    RecordingFileIO healthy = new RecordingFileIO();
    CopyOnWriteStagingWriter control =
        new CopyOnWriteStagingWriter(
            tableWithFileIo(healthy),
            TABLE_REFERENCE,
            cdcConfig(false),
            ID_FIELDS,
            unhashableSketches(healthy));

    for (CopyOnWriteStagingWriter writer : ImmutableList.of(failing, plain, control)) {
      writer.write(record("orders", 0, 80L, row(1L, 1L, "c", "new")));
      writer.write(record("orders", 0, 81L, row(1L, 2L, "u", "paid")));
      writer.write(record("orders", 0, 82L, row(1L, 3L, "d", null)));
    }

    SketchFiles.Content controlContent =
        SketchFiles.read(healthy, onlyResult(control.complete(COMMIT_ID)).sketchFile());
    assertThat(controlContent.properties()).containsEntry("row-count", "2");
    assertThat(controlContent.ndv()).as("the blobs of the control").isEmpty();

    List<RecordWriteResult> failedResults = failing.complete(COMMIT_ID);
    assertThat(io.statsFilesAttempted()).as("the writer tried the file").hasSize(1);
    StagedChangesResult actual = onlyResult(failedResults);
    StagedChangesResult expected = onlyResult(plain.complete(COMMIT_ID));

    assertThat(actual.sketchFile()).as("no reference to a file that is not there").isNull();
    assertThat(expected.sketchFile()).isNull();
    assertThat(actual.tableReference()).isEqualTo(expected.tableReference());
    assertThat(actual.taskId()).isEqualTo(expected.taskId());
    assertThat(actual.sourceTopics()).isEqualTo(expected.sourceTopics());
    assertThat(shapes(actual.stagedFiles())).hasSize(1).isEqualTo(shapes(expected.stagedFiles()));
    assertThat(readRows(table, ID_FIELDS, actual.stagedFiles()))
        .as("the same rows are staged")
        .hasSize(3)
        .isEqualTo(readRows(table, ID_FIELDS, expected.stagedFiles()));
  }

  @Test
  public void testSchemaChangeMovesSketchesToNewSchema() {
    RecordingFileIO io = new RecordingFileIO();
    IcebergSinkConfig evolving = config(true);
    when(evolving.evolveSchemaEnabled()).thenReturn(true);
    CopyOnWriteStagingWriter writer = writerWithSketches(evolving, io);

    writer.write(record("orders", 0, 71L, row(1L, 1L, null, "a")));
    // the table gets a column here: the staged file in progress ends and a new one starts
    Map<String, Object> withExtra = row(1L, 2L, null, "b");
    withExtra.put("extra", "x");
    writer.write(record("orders", 0, 72L, withExtra));
    StagedChangesResult result = onlyResult(writer.complete(COMMIT_ID));

    assertThat(result.stagedFiles()).as("one staged file per schema").hasSize(2);
    SketchFiles.Content content = SketchFiles.read(io, result.sketchFile());
    assertThat(content.properties())
        .containsEntry("last-column-id", "4")
        .containsEntry("row-count", "2");
    // the added column has seen only the row written since
    assertThat(content.ndv())
        .containsEntry(1, 1.0)
        .containsEntry(2, 2.0)
        .containsEntry(3, 2.0)
        .containsEntry(4, 1.0);
  }

  private ColumnSketchCollector unhashableSketches(RecordingFileIO io) {
    Schema mismatched =
        new Schema(
            required(1, "tenant_id", Types.StringType.get()),
            required(2, "order_id", Types.StringType.get()),
            optional(3, "status", Types.LongType.get()));
    return new ColumnSketchCollector(
        TABLE_IDENTIFIER,
        SketchFiles.tableWith(io),
        stagingDirectory(),
        "task-0",
        LG_K,
        null,
        false,
        mismatched);
  }

  /** What of a staged file does not depend on the name it was given. */
  private static List<List<Object>> shapes(List<StagedChangeFile> files) {
    List<List<Object>> shapes = Lists.newArrayList();
    for (StagedChangeFile file : files) {
      shapes.add(
          ImmutableList.of(
              file.recordCount(),
              file.schemaId(),
              file.formatVersion(),
              file.lowerBounds(),
              file.upperBounds()));
    }

    return shapes;
  }

  private static StagedChangesResult onlyResult(List<RecordWriteResult> results) {
    assertThat(results).hasSize(1);
    return (StagedChangesResult) results.get(0);
  }

  private CopyOnWriteStagingWriter writerWithSketches(
      IcebergSinkConfig config, RecordingFileIO io) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER,
            SketchFiles.tableWith(io),
            stagingDirectory(),
            "task-0",
            LG_K,
            null,
            false,
            table.schema());
    return new CopyOnWriteStagingWriter(
        tableWithFileIo(io), TABLE_REFERENCE, config, ID_FIELDS, sketches);
  }

  private Table tableWithFileIo(RecordingFileIO io) {
    Table wrapped = mock(Table.class, delegatesTo(table));
    doReturn(io).when(wrapped).io();
    return wrapped;
  }

  private String stagingDirectory() {
    return StagedChangeFileWriter.stagingDirectory(
        StagedChangeFileWriter.stagingLocation(table, null), TABLE_REFERENCE, "cg-connect");
  }

  private void assertFailsNaming(Map<String, Object> value, String... fields) {
    CopyOnWriteStagingWriter writer = writer(cdcConfig(true));

    assertThatThrownBy(() -> writer.write(record("orders", 1, 5L, value)))
        .isInstanceOf(DataException.class)
        .hasMessageContaining(String.join(", ", fields))
        .hasMessageContaining("topic: orders")
        .hasMessageContaining("partition: 1")
        .hasMessageContaining("offset: 5");
    assertThat(writer.complete(COMMIT_ID)).as("the record is not staged").isEmpty();
  }

  private CopyOnWriteStagingWriter writer(IcebergSinkConfig config) {
    return new CopyOnWriteStagingWriter(table, TABLE_REFERENCE, config, ID_FIELDS);
  }

  private static CopyOnWriteStagingWriter ordersWriter(Table orders) {
    return new CopyOnWriteStagingWriter(
        orders,
        TableReference.of("catalog", ORDERS_IDENTIFIER),
        cdcConfig(false),
        ORDERS_ID_FIELDS);
  }

  private List<Record> stagedRows(CopyOnWriteStagingWriter writer) throws IOException {
    return readRows(table, ID_FIELDS, stagedFiles(writer));
  }

  private static List<StagedChangeFile> stagedFiles(CopyOnWriteStagingWriter writer) {
    List<StagedChangeFile> files = Lists.newArrayList();
    for (RecordWriteResult result : writer.complete(COMMIT_ID)) {
      files.addAll(((StagedChangesResult) result).stagedFiles());
    }
    return files;
  }

  private static List<Record> readRows(
      Table target, Set<Integer> identifierFieldIds, List<StagedChangeFile> files)
      throws IOException {
    Schema projection = StagedChangeSchema.stagedSchema(target.schema(), identifierFieldIds);
    List<Record> rows = Lists.newArrayList();
    for (StagedChangeFile file : files) {
      try (CloseableIterable<Record> reader =
          Avro.read(target.io().newInputFile(file.location()))
              .project(projection)
              .createResolvingReader(PlannedDataReader::create)
              .build()) {
        reader.forEach(rows::add);
      }
    }
    return rows;
  }

  /** The schema a staged file was written with, as its header carries it. */
  private static Schema fileSchema(Table target, StagedChangeFile file) throws IOException {
    try (DataFileStream<Object> stream =
        new DataFileStream<>(
            target.io().newInputFile(file.location()).newStream(), new GenericDatumReader<>())) {
      return new Schema(AvroSchemaUtil.convert(stream.getSchema()).asStructType().fields());
    }
  }

  private static ByteBuffer longBound(long value) {
    return Conversions.toByteBuffer(Types.LongType.get(), value);
  }

  private static Map<Integer, Object> decoded(Map<Integer, ByteBuffer> bounds) {
    Map<Integer, Object> values = Maps.newTreeMap();
    bounds.forEach(
        (fieldId, bound) ->
            values.put(fieldId, Conversions.fromByteBuffer(SCHEMA.findType(fieldId), bound)));
    return values;
  }

  private static Map<String, Object> row(long tenantId, long orderId, String op, String status) {
    Map<String, Object> value = Maps.newHashMap();
    value.put("tenant_id", tenantId);
    value.put("order_id", orderId);
    if (op != null) {
      value.put(OP_FIELD, op);
    }
    if (status != null) {
      value.put("status", status);
    }
    return value;
  }

  private static Map<String, Object> ordersRow(long id, String op, String status) {
    Map<String, Object> value = Maps.newHashMap();
    value.put("id", id);
    value.put(OP_FIELD, op);
    if (status != null) {
      value.put("status", status);
    }
    return value;
  }

  private static SinkRecord record(
      String topic, int partition, long offset, Map<String, Object> value) {
    return new SinkRecord(
        topic, partition, null, "key", null, value, offset, 0L, TimestampType.LOG_APPEND_TIME);
  }

  private static IcebergSinkConfig cdcConfig(boolean upsert) {
    IcebergSinkConfig config = config(upsert);
    when(config.tablesCdcField()).thenReturn(OP_FIELD);
    when(config.tablesCdcOpsInsert()).thenReturn(ImmutableList.of("c"));
    when(config.tablesCdcOpsUpdate()).thenReturn(ImmutableList.of("u"));
    when(config.tablesCdcOpsDelete()).thenReturn(ImmutableList.of("d"));
    when(config.tablesCdcIgnoredOps()).thenReturn(ImmutableList.of("m"));
    return config;
  }

  private static IcebergSinkConfig config(boolean upsert) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("cg-connect");
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.taskId()).thenReturn("task-0");
    when(config.isUpsertMode()).thenReturn(upsert);
    when(config.evolveSchemaEnabled()).thenReturn(false);
    when(config.stagingLocation()).thenReturn(null);
    return config;
  }
}
