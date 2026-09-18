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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private void assertFailsNaming(Map<String, Object> value, String... fields) {
    CopyOnWriteStagingWriter writer = writer(cdcConfig(true));

    assertThatThrownBy(() -> writer.write(record("orders", 1, 5L, value)))
        .isInstanceOf(DataException.class)
        .hasMessageContaining(String.join(", ", fields))
        .hasMessageContaining("topic: orders")
        .hasMessageContaining("partition: 1")
        .hasMessageContaining("offset: 5");
    assertThat(writer.complete()).as("the record is not staged").isEmpty();
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
    for (RecordWriteResult result : writer.complete()) {
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
    when(config.copyOnWriteStagingLocation()).thenReturn(null);
    return config;
  }
}
