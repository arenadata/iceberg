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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

public class TestCopyOnWriteValidations {

  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "tbl");

  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", IDENTIFIER, UUID.randomUUID());

  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "amount", Types.DoubleType.get()),
              Types.NestedField.required(4, "price", Types.FloatType.get()),
              Types.NestedField.required(
                  5,
                  "meta",
                  Types.StructType.of(
                      Types.NestedField.required(6, "tenant", Types.StringType.get())))),
          ImmutableSet.of(1));

  @ParameterizedTest
  @ValueSource(
      strings = {
        TableProperties.UPDATE_MODE,
        TableProperties.DELETE_MODE,
        TableProperties.MERGE_MODE
      })
  public void testMergeOnReadOnlyWarnsOnTableModeMismatch(String prop) {
    Table table = mockTable(ImmutableMap.of(prop, "copy-on-write"));
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.MERGE_ON_READ, "0", List.of());
    Logger log = mock(Logger.class);

    assertThatCode(
            () -> IcebergWriterFactory.checkRowLevelModeTableProps(table, IDENTIFIER, config, log))
        .doesNotThrowAnyException();
    assertThat(warnings(log))
        .singleElement()
        .asString()
        .contains(IDENTIFIER.toString(), prop + "=copy-on-write", "row-level-mode=merge-on-read");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        TableProperties.UPDATE_MODE,
        TableProperties.DELETE_MODE,
        TableProperties.MERGE_MODE
      })
  public void testCopyOnWriteFailsOnTableModeMismatch(String prop) {
    Table table = mockTable(ImmutableMap.of(prop, "merge-on-read"));
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", List.of());

    assertThatThrownBy(
            () -> IcebergWriterFactory.checkRowLevelModeTableProps(table, IDENTIFIER, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(IDENTIFIER.toString())
        .hasMessageContaining(prop + "=merge-on-read")
        .hasMessageContaining("row-level-mode=copy-on-write");
  }

  @Test
  public void testMatchingTableModeIsNotAMismatch() {
    Table table = mockTable(ImmutableMap.of("write.update.mode", "COPY-ON-WRITE"));
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", List.of());

    assertThatCode(
            () -> IcebergWriterFactory.checkRowLevelModeTableProps(table, IDENTIFIER, config))
        .doesNotThrowAnyException();
  }

  @Test
  public void testAbsentTablePropertyIsNotAMismatch() {
    // the core defaults are read-time only, so an unset property must not be read as copy-on-write:
    // in merge-on-read, where a copy-on-write default would mismatch every such table
    Table table = mockTable(ImmutableMap.of());
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.MERGE_ON_READ, "0", List.of());
    Logger log = mock(Logger.class);

    assertThatCode(
            () -> IcebergWriterFactory.checkRowLevelModeTableProps(table, IDENTIFIER, config, log))
        .doesNotThrowAnyException();
    assertThat(warnings(log)).isEmpty();
  }

  @Test
  public void testCopyOnWriteRequiresTaskId() {
    Table table = mockTable(ImmutableMap.of());
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.COPY_ON_WRITE, null, List.of());

    assertThatThrownBy(
            () ->
                IcebergWriterFactory.checkCopyOnWritePrerequisites(table, TABLE_REFERENCE, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("task.id")
        .hasMessageContaining(IDENTIFIER.toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"data", "amount", "price", "meta", "nope"})
  public void testCopyOnWriteRejectsAnIdColumnIcebergWouldNotAcceptAsAnIdentifierField(
      String column) {
    // optional, double, float, a struct, a missing column: each is a rule of its own, and a check
    // that lists the rules instead of building a Schema can forget any of them. A task fanning out
    // to many tables fails on the first, so the message has to name the table
    Table table = mockTable(ImmutableMap.of());
    IcebergSinkConfig config =
        mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", ImmutableList.of(column));

    assertThatThrownBy(
            () ->
                IcebergWriterFactory.checkCopyOnWritePrerequisites(table, TABLE_REFERENCE, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(IDENTIFIER.toString())
        .hasMessageContaining(column);
  }

  @Test
  public void testCopyOnWriteAcceptsRequiredPrimitiveIdColumn() {
    Table table = mockTable(ImmutableMap.of());
    IcebergSinkConfig config =
        mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", ImmutableList.of("id"));

    assertThatCode(
            () ->
                IcebergWriterFactory.checkCopyOnWritePrerequisites(table, TABLE_REFERENCE, config))
        .doesNotThrowAnyException();
  }

  @Test
  public void testCopyOnWriteWithoutIdColumnsUsesTableIdentifierFields() {
    Table table = mockTable(ImmutableMap.of());
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", List.of());

    assertThatCode(
            () ->
                IcebergWriterFactory.checkCopyOnWritePrerequisites(table, TABLE_REFERENCE, config))
        .doesNotThrowAnyException();
  }

  @Test
  public void testCopyOnWriteRejectsANestedIdentifierField() {
    // Iceberg allows an identifier field inside a required struct; copy-on-write cannot address
    // one:
    // the pruning predicate binds a bare name, and the key builder reads a top-level position.
    // The refusal has to happen here, not as a stack trace from the middle of a drain
    Schema nested =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(
                    4,
                    "meta",
                    Types.StructType.of(
                        Types.NestedField.required(5, "tenant", Types.StringType.get())))),
            ImmutableSet.of(1, 5));
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(nested);
    when(table.properties()).thenReturn(ImmutableMap.of());
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", List.of());

    assertThatThrownBy(
            () ->
                IcebergWriterFactory.checkCopyOnWritePrerequisites(table, TABLE_REFERENCE, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(IDENTIFIER.toString())
        .hasMessageContaining("nested identifier fields")
        .hasMessageContaining("5");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCopyOnWriteRejectsANanosecondTimestampIdColumn(boolean withZone) {
    // Iceberg accepts a timestamp_ns identifier field, so building a Schema lets id-columns naming
    // one through. The pruning predicate would read its nanoseconds as microseconds: a long
    // overflow on every plan, or the file holding the key pruned away, so the refusal is ours
    Table table = nanosecondTimestampTable(withZone, 1);
    IcebergSinkConfig config =
        mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", ImmutableList.of("created_at"));

    assertThatThrownBy(
            () ->
                IcebergWriterFactory.checkCopyOnWritePrerequisites(table, TABLE_REFERENCE, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(IDENTIFIER.toString())
        .hasMessageContaining("created_at")
        .hasMessageContaining("timestamp_ns");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCopyOnWriteRejectsANanosecondTimestampIdentifierField(boolean withZone) {
    // the table's own identifier fields, through the call the coordinator makes when it starts a
    // drain: a table keyed by timestamp_ns before the connector ever saw it has to stop there too
    Table table = nanosecondTimestampTable(withZone, 2);
    IcebergSinkConfig config = mockConfig(RowLevelOperationMode.COPY_ON_WRITE, "0", List.of());

    assertThatThrownBy(() -> IdentifierFields.resolveForCopyOnWrite(table, TABLE_REFERENCE, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(IDENTIFIER.toString())
        .hasMessageContaining("created_at")
        .hasMessageContaining("timestamp_ns");
  }

  private static Table nanosecondTimestampTable(boolean withZone, int identifierFieldId) {
    Schema schema =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(
                    2,
                    "created_at",
                    withZone
                        ? Types.TimestampNanoType.withZone()
                        : Types.TimestampNanoType.withoutZone())),
            ImmutableSet.of(identifierFieldId));
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(schema);
    when(table.properties()).thenReturn(ImmutableMap.of());
    return table;
  }

  private static List<String> warnings(Logger log) {
    return mockingDetails(log).getInvocations().stream()
        .filter(invocation -> invocation.getMethod().getName().equals("warn"))
        .map(
            invocation -> {
              Object[] args = invocation.getArguments();
              return MessageFormatter.arrayFormat(
                      String.valueOf(args[0]), Arrays.copyOfRange(args, 1, args.length))
                  .getMessage();
            })
        .collect(Collectors.toList());
  }

  private Table mockTable(Map<String, String> properties) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    when(table.properties()).thenReturn(properties);
    return table;
  }

  private IcebergSinkConfig mockConfig(
      RowLevelOperationMode mode, String taskId, List<String> idColumns) {
    TableSinkConfig tableSinkConfig = mock(TableSinkConfig.class);
    when(tableSinkConfig.idColumns()).thenReturn(idColumns);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.rowLevelMode()).thenReturn(mode);
    when(config.isCopyOnWriteMode()).thenReturn(mode == RowLevelOperationMode.COPY_ON_WRITE);
    when(config.taskId()).thenReturn(taskId);
    when(config.tableConfig(any())).thenReturn(tableSinkConfig);
    return config;
  }
}
