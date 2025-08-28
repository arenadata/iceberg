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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergWriterTest {
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

    IcebergWriter icebergWriter = new IcebergWriter(table, mockTaskWriter, "ignored", config);

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

    IcebergWriter icebergWriter = new IcebergWriter(table, mockTaskWriter, "ignored", config);

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

    IcebergWriter icebergWriter = new IcebergWriter(table, mockTaskWriter, "ignored", config);

    Stream.of(
            record(1, "one", "c", "insert"),
            record(2, "two", "u", "update"),
            record(3, "three", "r", "delete"))
        .forEach(icebergWriter::write);

    assertResults(idAndOp(1, null), idAndOp(2, null), idAndOp(3, null));
  }

  private void assertResults(Record... expectedRecords) {
    List<Record> records = Arrays.asList(expectedRecords);
    assertEquals(records, actualIdAndOps());
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

    @Override
    public void write(Record row) {
      records.add(row);
    }

    @Override
    public void abort() {}

    @Override
    public WriteResult complete() {
      return null;
    }

    @Override
    public void close() {}
  }
}
