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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.SketchFiles.RecordingFileIO;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergWriter {
  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "__op", Types.StringType.get()),
              Types.NestedField.required(4, "_op2", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final Schema ID_AND_OP_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "operation", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final TableIdentifier IGNORED = TableIdentifier.of("ignored");
  private static final String STAGING = "mem://bucket/kc-staging/ignored/group";
  private static final UUID COMMIT_ID = UUID.randomUUID();
  private static final int LG_K = 12;

  private MockTaskWriter mockTaskWriter;
  private IcebergSinkConfig config;
  private Table table;

  @BeforeEach
  public void before() {
    this.mockTaskWriter = new MockTaskWriter();
    this.config = mock(IcebergSinkConfig.class);
    this.table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
  }

  @Test
  public void testWrapRecordsWithFirstOpField() {
    when(config.tablesCdcField()).thenReturn("__op");

    when(config.tablesCdcOpsInsert()).thenReturn(ImmutableList.of("c", "r"));
    when(config.tablesCdcOpsUpdate()).thenReturn(Collections.singletonList("u"));
    when(config.tablesCdcOpsDelete()).thenReturn(ImmutableList.of("d", "rm"));
    when(config.tablesCdcIgnoredOps()).thenReturn(ImmutableList.of("m", "t"));

    IcebergWriter icebergWriter =
        new IcebergWriter(
            table,
            TableReference.of("catalog", TableIdentifier.of("ignored"), UUID.randomUUID()),
            config,
            mockTaskWriter);

    Stream.of(
            record(1, "one", "c", ""),
            record(2, "two", "r", ""),
            record(2, "three", "u", "c"),
            record(143, "three", "m", "message"),
            record(3, "four", "c", "u"),
            record(111, "13", "t", "truncate"),
            record(32, "311", "m", "message"),
            record(1, "one", "d", ""),
            record(77, "unknown_op", "unknown", "unknown_op"),
            record(2, "two", "rm", ""))
        .forEach(icebergWriter::write);

    assertResults(
        idAndOp(1, Operation.INSERT),
        idAndOp(2, Operation.INSERT),
        idAndOp(2, Operation.UPDATE),
        idAndOp(3, Operation.INSERT),
        idAndOp(1, Operation.DELETE),
        idAndOp(77, null),
        idAndOp(2, Operation.DELETE));
  }

  @Test
  public void testWrapRecordsWithSecondOpField() {
    when(config.tablesCdcField()).thenReturn("_op2");

    when(config.tablesCdcOpsInsert()).thenReturn(ImmutableList.of("INSERT", "READ"));
    when(config.tablesCdcOpsUpdate()).thenReturn(Collections.singletonList("UPDATE"));
    when(config.tablesCdcOpsDelete()).thenReturn(ImmutableList.of("DELETE", "REMOVE"));
    when(config.tablesCdcIgnoredOps()).thenReturn(ImmutableList.of("MESSAGE", "truncate"));

    IcebergWriter icebergWriter =
        new IcebergWriter(
            table,
            TableReference.of("catalog", TableIdentifier.of("ignored"), UUID.randomUUID()),
            config,
            mockTaskWriter);

    Stream.of(
            record(1, "one", "c", "insert"),
            record(2, "two", "c", "insert"),
            record(2, "three", "c", "update"),
            record(143, "three", "c", "message"),
            record(3, "four", "r", "read"),
            record(111, "13", "t", "truncate"),
            record(32, "311", "f", "truncate"),
            record(1, "one", "c", "delete"),
            record(77, "unknown_op", "w", "unknown_op"),
            record(2, "two", "r", "remove"))
        .forEach(icebergWriter::write);

    assertResults(
        idAndOp(1, Operation.INSERT),
        idAndOp(2, Operation.INSERT),
        idAndOp(2, Operation.UPDATE),
        idAndOp(3, Operation.INSERT),
        idAndOp(1, Operation.DELETE),
        idAndOp(77, null),
        idAndOp(2, Operation.DELETE));
  }

  @Test
  public void testWrapRecordsWithMissingOpField() {
    when(config.tablesCdcField()).thenReturn("other_op_field");

    when(config.tablesCdcOpsInsert()).thenReturn(Collections.singletonList("c"));
    when(config.tablesCdcOpsUpdate()).thenReturn(Collections.singletonList("u"));
    when(config.tablesCdcOpsDelete()).thenReturn(Collections.singletonList("r"));

    IcebergWriter icebergWriter =
        new IcebergWriter(
            table,
            TableReference.of("catalog", TableIdentifier.of("ignored"), UUID.randomUUID()),
            config,
            mockTaskWriter);

    Stream.of(
            record(1, "one", "c", "insert"),
            record(2, "two", "u", "update"),
            record(3, "three", "r", "delete"))
        .forEach(icebergWriter::write);

    assertResults(idAndOp(1, null), idAndOp(2, null), idAndOp(3, null));
  }

  @Test
  public void testCdcDeleteIsNotSketchedInsertAndUpdateAre() {
    cdcConfig();
    RecordingFileIO io = new RecordingFileIO();
    IcebergWriter icebergWriter = writerWithSketches(io);

    Stream.of(
            record(1, "one", "c", ""),
            record(2, "two", "u", ""),
            // the delete row reaches the writer, which writes only its key; it holds no new value
            record(3, "gone", "d", ""),
            record(4, "ignored", "m", ""),
            tombstone())
        .forEach(icebergWriter::write);
    List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);

    assertThat(mockTaskWriter.records).as("the writer gets the delete").hasSize(3);
    SketchFile sketchFile = onlySketchFile(results);
    SketchFiles.Content content = SketchFiles.read(io, sketchFile);
    assertThat(content.properties()).containsEntry("row-count", "2");
    assertThat(content.ndv()).containsEntry(1, 2.0).containsEntry(2, 2.0);
    assertThat(sketchFile.location()).contains("/stats/" + COMMIT_ID + "-0-");
  }

  @Test
  public void testAppendSketchesEveryRow() {
    RecordingFileIO io = new RecordingFileIO();
    IcebergWriter icebergWriter = writerWithSketches(io);

    Stream.of(
            record(1, "one", "c", ""),
            record(2, "two", "d", ""),
            // an append writer takes an operation column for data
            record(3, "three", "u", ""))
        .forEach(icebergWriter::write);
    List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);

    SketchFiles.Content content = SketchFiles.read(io, onlySketchFile(results));
    assertThat(content.properties()).containsEntry("row-count", "3");
    assertThat(content.ndv()).containsEntry(1, 3.0).containsEntry(2, 3.0).containsEntry(3, 3.0);
  }

  @Test
  public void testRowIsSketchedOnlyWhenWriterTookIt() {
    RecordingFileIO io = new RecordingFileIO();
    mockTaskWriter.failOn = 2L;
    IcebergWriter icebergWriter = writerWithSketches(io);

    icebergWriter.write(record(1, "one", "c", ""));
    assertThatWriteFails(icebergWriter, record(2, "two", "c", ""));
    icebergWriter.write(record(3, "three", "c", ""));
    List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);

    SketchFiles.Content content = SketchFiles.read(io, onlySketchFile(results));
    assertThat(content.properties()).containsEntry("row-count", "2");
    assertThat(content.ndv()).containsEntry(1, 2.0);
  }

  @Test
  public void testFailingSketchUpdateDoesNotFailWrite() {
    RecordingFileIO io = new RecordingFileIO();
    AtomicInteger updates = new AtomicInteger();
    IcebergWriter icebergWriter = writerWithSketches(io, throwingSketches(io, updates));

    icebergWriter.write(record(1, "one", "c", ""));
    icebergWriter.write(record(2, "two", "c", ""));
    icebergWriter.write(record(3, "three", "c", ""));
    List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);

    assertThat(results).hasSize(1);
    IcebergWriterResult result = (IcebergWriterResult) results.get(0);
    // the sketches lack the rows of the failed updates: their file would understate the NDV
    assertThat(result.sketchFile()).as("no file of sketches that missed rows").isNull();
    assertThat(io.statsFilesAttempted()).isEmpty();
    assertThat(updates).as("the collector is not asked again in the cycle").hasValue(1);
    assertThat(mockTaskWriter.records).as("every row is in the writer").hasSize(3);
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.sourceTopics()).containsExactly("topic");
  }

  @Test
  public void testNoDataFilesNoSketchFile() {
    RecordingFileIO io = new RecordingFileIO();
    IcebergWriter withData = writerWithSketches(io);
    withData.write(record(1, "one", "c", ""));
    assertThat(onlySketchFile(withData.complete(COMMIT_ID)))
        .as("a file for a data file")
        .isNotNull();
    assertThat(io.statsFiles()).hasSize(1);

    // a cycle of deletes only: nothing was inserted, so there is nothing to cover
    RecordingFileIO otherIo = new RecordingFileIO();
    mockTaskWriter = new MockTaskWriter();
    mockTaskWriter.result = WriteResult.builder().addDeleteFiles(mock(DeleteFile.class)).build();
    IcebergWriter deletesOnly = writerWithSketches(otherIo);
    deletesOnly.write(record(1, "one", "c", ""));
    List<RecordWriteResult> results = deletesOnly.complete(COMMIT_ID);

    assertThat(results).hasSize(1);
    assertThat(((IcebergWriterResult) results.get(0)).sketchFile()).isNull();
    assertThat(otherIo.statsFilesAttempted()).isEmpty();
  }

  @Test
  public void testFailingSketchFileWriteLeavesDataResults() {
    RecordingFileIO io = new RecordingFileIO().failOnStatsFiles();
    IcebergWriter icebergWriter = writerWithSketches(io);

    icebergWriter.write(record(1, "one", "c", ""));
    List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);

    assertThat(io.statsFilesAttempted()).as("the writer tried the file").hasSize(1);
    assertThat(results).hasSize(1);
    IcebergWriterResult result = (IcebergWriterResult) results.get(0);
    assertThat(result.sketchFile()).as("no reference to a file that is not there").isNull();
    assertThat(result.dataFiles()).as("the data goes on").hasSize(1);
    assertThat(result.sourceTopics()).containsExactly("topic");
  }

  @Test
  public void testStatisticsThatFailInEveryWayLeaveResultsAsWithoutThem() {
    // the collector does throw on every value: with a file that can be written it has no blob
    RecordingFileIO healthy = new RecordingFileIO();
    ColumnSketchCollector probe = unhashableSketches(healthy);
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", "one");
    row.setField("__op", "c");
    row.setField("_op2", "");
    probe.update(row);
    assertThat(SketchFiles.read(healthy, probe.write(COMMIT_ID)).ndv()).isEmpty();

    RecordingFileIO io = new RecordingFileIO().failOnStatsFiles();
    List<SinkRecord> records =
        ImmutableList.of(
            record(1, "one", "c", ""), record(2, "two", "c", ""), record(3, "three", "c", ""));

    TableReference reference = TableReference.of("catalog", IGNORED, UUID.randomUUID());
    when(table.io()).thenReturn(io);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    IcebergWriter withStatistics =
        new IcebergWriter(table, reference, config, mockTaskWriter, unhashableSketches(io));
    records.forEach(withStatistics::write);
    List<RecordWriteResult> failed = withStatistics.complete(COMMIT_ID);
    List<Record> failedRows = mockTaskWriter.records;
    assertThat(io.statsFilesAttempted()).as("the writer tried the file").hasSize(1);

    // the same cycle of a writer without statistics, over the same result of the task writer
    WriteResult writeResult = mockTaskWriter.result;
    mockTaskWriter = new MockTaskWriter();
    mockTaskWriter.result = writeResult;
    IcebergWriter without = new IcebergWriter(table, reference, config, mockTaskWriter);
    records.forEach(without::write);
    List<RecordWriteResult> plain = without.complete(COMMIT_ID);

    assertThat(failedRows).as("every row is written").hasSize(3).isEqualTo(mockTaskWriter.records);
    assertThat(failed).hasSize(1);
    assertThat(plain).hasSize(1);
    IcebergWriterResult actual = (IcebergWriterResult) failed.get(0);
    IcebergWriterResult expected = (IcebergWriterResult) plain.get(0);
    assertThat(actual.sketchFile()).as("no reference to a file that is not there").isNull();
    assertThat(expected.sketchFile()).isNull();
    assertThat(actual.tableReference()).isEqualTo(expected.tableReference());
    assertThat(actual.dataFiles()).isEqualTo(expected.dataFiles()).hasSize(1);
    assertThat(actual.deleteFiles()).isEqualTo(expected.deleteFiles());
    assertThat(actual.partitionStruct()).isEqualTo(expected.partitionStruct());
    assertThat(actual.sourceTopics()).isEqualTo(expected.sourceTopics());
  }

  @Test
  public void testWithoutSketchesWriterKeepsNoStatisticsFile() {
    RecordingFileIO io = new RecordingFileIO();
    when(table.io()).thenReturn(io);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    IcebergWriter icebergWriter =
        new IcebergWriter(
            table,
            TableReference.of("catalog", IGNORED, UUID.randomUUID()),
            config,
            mockTaskWriter);

    icebergWriter.write(record(1, "one", "c", ""));
    List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);

    assertThat(results).hasSize(1);
    assertThat(((IcebergWriterResult) results.get(0)).sketchFile()).isNull();
    assertThat(io.statsFiles()).isEmpty();
  }

  @Test
  public void testResultsOfSchemaChangeShareOneSketchFile() throws IOException {
    Namespace namespace = Namespace.of("db");
    TableIdentifier identifier = TableIdentifier.of(namespace, "evolving");
    try (InMemoryCatalog catalog = new InMemoryCatalog()) {
      catalog.initialize(null, ImmutableMap.of());
      catalog.createNamespace(namespace);
      Table evolving =
          catalog.createTable(
              identifier,
              new Schema(
                  required(1, "id", Types.LongType.get()),
                  optional(2, "data", Types.StringType.get())));
      IcebergSinkConfig evolvingConfig = mock(IcebergSinkConfig.class);
      when(evolvingConfig.evolveSchemaEnabled()).thenReturn(true);
      when(evolvingConfig.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
      RecordingFileIO io = new RecordingFileIO();
      ColumnSketchCollector sketches =
          new ColumnSketchCollector(
              identifier,
              SketchFiles.tableWith(io),
              STAGING,
              "0",
              LG_K,
              null,
              false,
              evolving.schema());
      IcebergWriter icebergWriter =
          new IcebergWriter(
              evolving,
              TableReference.of("catalog", identifier, UUID.randomUUID()),
              evolvingConfig,
              sketches);

      icebergWriter.write(mapRecord(ImmutableMap.of("id", 1L, "data", "a")));
      // the table gets a column here: the file in progress ends and a writer of the
      // new schema starts
      icebergWriter.write(mapRecord(ImmutableMap.of("id", 2L, "data", "b", "extra", "x")));
      icebergWriter.write(mapRecord(ImmutableMap.of("id", 3L, "data", "c", "extra", "y")));
      List<RecordWriteResult> results = icebergWriter.complete(COMMIT_ID);
      icebergWriter.close();

      assertThat(results).as("a result per schema").hasSize(2);
      assertThat(results)
          .extracting(result -> ((IcebergWriterResult) result).dataFiles().size())
          .allMatch(count -> count > 0);
      SketchFile sketchFile = ((IcebergWriterResult) results.get(0)).sketchFile();
      assertThat(sketchFile).isNotNull();
      assertThat(results)
          .extracting(result -> ((IcebergWriterResult) result).sketchFile().location())
          .containsOnly(sketchFile.location());
      assertThat(io.statsFiles())
          .as("one file for the cycle")
          .containsExactly(sketchFile.location());

      SketchFiles.Content content = SketchFiles.read(io, sketchFile);
      assertThat(content.properties())
          .containsEntry("last-column-id", "3")
          .containsEntry("row-count", "3");
      // the column added by the second record has seen only the rows written since
      assertThat(content.ndv()).containsEntry(1, 3.0).containsEntry(2, 3.0).containsEntry(3, 2.0);
    }
  }

  private void cdcConfig() {
    when(config.tablesCdcField()).thenReturn("__op");
    when(config.tablesCdcOpsInsert()).thenReturn(ImmutableList.of("c", "r"));
    when(config.tablesCdcOpsUpdate()).thenReturn(Collections.singletonList("u"));
    when(config.tablesCdcOpsDelete()).thenReturn(ImmutableList.of("d", "rm"));
    when(config.tablesCdcIgnoredOps()).thenReturn(ImmutableList.of("m", "t"));
  }

  private IcebergWriter writerWithSketches(RecordingFileIO io) {
    return writerWithSketches(
        io,
        new ColumnSketchCollector(
            IGNORED, SketchFiles.tableWith(io), STAGING, "0", LG_K, null, false, SCHEMA));
  }

  private static ColumnSketchCollector throwingSketches(RecordingFileIO io, AtomicInteger updates) {
    return new ColumnSketchCollector(
        IGNORED, SketchFiles.tableWith(io), STAGING, "0", LG_K, null, false, SCHEMA) {
      @Override
      public void update(Record row) {
        updates.incrementAndGet();
        throw new IllegalStateException("update failed");
      }
    };
  }

  private static ColumnSketchCollector unhashableSketches(RecordingFileIO io) {
    Schema mismatched =
        new Schema(
            required(1, "id", Types.StringType.get()),
            required(2, "data", Types.LongType.get()),
            required(3, "__op", Types.LongType.get()),
            required(4, "_op2", Types.LongType.get()));
    return new ColumnSketchCollector(
        IGNORED, SketchFiles.tableWith(io), STAGING, "0", LG_K, null, false, mismatched);
  }

  private IcebergWriter writerWithSketches(RecordingFileIO io, ColumnSketchCollector sketches) {
    when(table.io()).thenReturn(io);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    return new IcebergWriter(
        table,
        TableReference.of("catalog", IGNORED, UUID.randomUUID()),
        config,
        mockTaskWriter,
        sketches);
  }

  private static SketchFile onlySketchFile(List<RecordWriteResult> results) {
    assertThat(results).hasSize(1);
    SketchFile sketchFile = ((IcebergWriterResult) results.get(0)).sketchFile();
    assertThat(sketchFile).isNotNull();
    return sketchFile;
  }

  private static void assertThatWriteFails(IcebergWriter icebergWriter, SinkRecord record) {
    assertThatThrownBy(() -> icebergWriter.write(record))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("An error occurred converting record");
  }

  private static SinkRecord tombstone() {
    return new SinkRecord(
        "topic", 1, null, "key", null, null, 100L, 0L, TimestampType.LOG_APPEND_TIME);
  }

  private static SinkRecord mapRecord(Map<String, Object> value) {
    return new SinkRecord(
        "topic", 1, null, "key", null, value, 100L, 0L, TimestampType.LOG_APPEND_TIME);
  }

  private void assertResults(Record... expectedRecords) {
    List<Record> records = Arrays.asList(expectedRecords);
    assertThat(records).isEqualTo(actualIdAndOps());
  }

  private List<Record> actualIdAndOps() {
    return mockTaskWriter.records.stream().map(this::toIdAndOp).collect(Collectors.toList());
  }

  private Record toIdAndOp(Record record) {
    Operation operation =
        Optional.of(record)
            .filter(RecordWrapper.class::isInstance)
            .map(RecordWrapper.class::cast)
            .map(RecordWrapper::op)
            .orElse(null);

    return idAndOp((long) record.getField("id"), operation);
  }

  private Record idAndOp(long id, Operation op) {
    GenericRecord genericRecord = GenericRecord.create(ID_AND_OP_SCHEMA);
    genericRecord.setField("id", id);
    genericRecord.setField("operation", Objects.toString(op, null));
    return genericRecord;
  }

  private SinkRecord record(long id, String data, String operation1, String operation2) {
    Map<String, Object> value =
        ImmutableMap.of(
            "id", id,
            "data", data,
            "__op", operation1,
            "_op2", operation2);

    return new SinkRecord(
        "topic",
        1,
        null,
        "key",
        null,
        value,
        100L,
        System.currentTimeMillis(),
        TimestampType.LOG_APPEND_TIME);
  }

  private static class MockTaskWriter implements TaskWriter<Record> {
    private final List<Record> records = Lists.newArrayList();
    private WriteResult result = WriteResult.builder().addDataFiles(mock(DataFile.class)).build();
    // the id of the row that makes the writer fail
    private Long failOn = null;

    @Override
    public void write(Record row) {
      if (failOn != null && failOn.equals(row.getField("id"))) {
        throw new IllegalStateException("cannot write row " + failOn);
      }

      records.add(row);
    }

    @Override
    public void abort() {}

    @Override
    public WriteResult complete() {
      return result;
    }

    @Override
    public void close() {}
  }
}
