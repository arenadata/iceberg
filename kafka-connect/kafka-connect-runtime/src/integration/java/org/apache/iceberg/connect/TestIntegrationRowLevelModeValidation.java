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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Fail-fast checks of the row-level mode configuration (6.1). */
public class TestIntegrationRowLevelModeValidation extends IntegrationTestBaseRowLevel {

  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);
  private static final Instant NOW = Instant.ofEpochMilli(System.currentTimeMillis());
  private static final boolean USE_SCHEMA = true;

  @Test
  public void testCopyOnWriteRequiresChangeStream() {
    KafkaConnectUtils.Config connectorConfig =
        rowLevelConfig(USE_SCHEMA, COPY_ON_WRITE, null, ImmutableMap.of());
    connectorConfig.getConfig().remove("iceberg.tables.cdc-field");
    KafkaConnectUtils.startConnector(connectorConfig);

    KafkaConnectUtils.awaitTaskFailed(
        connectorName(), "ConfigException: iceberg.tables.row-level-mode");
  }

  @ParameterizedTest
  @ValueSource(strings = {COPY_ON_WRITE, MERGE_ON_READ})
  public void testTablePropertyMismatch(String rowLevelMode) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.DELETE_MODE, MERGE_ON_READ));
    context().startConnector(rowLevelConfig(USE_SCHEMA, rowLevelMode, null, ImmutableMap.of()));

    List<CdcTestEvent> inserts =
        List.of(
            CdcTestEvent.insert(1, "type1", NOW, "first"),
            CdcTestEvent.insert(2, "type2", NOW, "second"));
    if (COPY_ON_WRITE.equals(rowLevelMode)) {
      inserts.forEach(event -> send(testTopic(), event, USE_SCHEMA));
      flush();
      KafkaConnectUtils.awaitTaskFailed(
          connectorName(),
          String.format(
              "Table %s declares %s=%s",
              TABLE_IDENTIFIER, TableProperties.DELETE_MODE, MERGE_ON_READ));
      assertThat(catalog().loadTable(TABLE_IDENTIFIER).snapshots()).isEmpty();
    } else {
      sendAndAwait(TABLE_IDENTIFIER, null, USE_SCHEMA, inserts, inserts);
    }
  }

  @Test
  public void testAutoCreatePropsConflict() {
    KafkaConnectUtils.startConnector(
        rowLevelConfig(
            USE_SCHEMA,
            COPY_ON_WRITE,
            null,
            ImmutableMap.of(
                "iceberg.tables.auto-create-props." + TableProperties.UPDATE_MODE, MERGE_ON_READ)));

    KafkaConnectUtils.awaitTaskFailed(
        connectorName(), TableProperties.UPDATE_MODE + "=" + MERGE_ON_READ + " conflicts with");
  }

  @ParameterizedTest
  @ValueSource(strings = {COPY_ON_WRITE, MERGE_ON_READ})
  public void testNestedIdentifierField(String rowLevelMode) {
    catalog().createTable(TABLE_IDENTIFIER, NestedKeyEvent.TABLE_SCHEMA);
    context().startConnector(rowLevelConfig(USE_SCHEMA, rowLevelMode, null, ImmutableMap.of()));

    List<NestedKeyEvent> inserts =
        List.of(new NestedKeyEvent(1, "first"), new NestedKeyEvent(2, "second"));
    inserts.forEach(event -> send(testTopic(), event, USE_SCHEMA));
    flush();

    if (COPY_ON_WRITE.equals(rowLevelMode)) {
      KafkaConnectUtils.awaitTaskFailed(connectorName(), "nested identifier fields");
    } else {
      List<String> expected = inserts.stream().map(NestedKeyEvent::row).toList();
      Awaitility.await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(1))
          .untilAsserted(
              () -> assertThat(nestedKeyRows()).containsExactlyInAnyOrderElementsOf(expected));
    }
  }

  private List<String> nestedKeyRows() {
    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    if (table.currentSnapshot() == null) {
      return List.of();
    }
    return Lists.newArrayList(IcebergGenerics.read(table).build()).stream()
        .map(NestedKeyEvent::row)
        .toList();
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("routing.strategy", "all-tables")
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE));
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }

  private static class NestedKeyEvent extends BaseTestEvent {

    private static final Schema TABLE_SCHEMA =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(
                    1,
                    "key",
                    Types.StructType.of(Types.NestedField.required(2, "id", Types.LongType.get()))),
                Types.NestedField.optional(3, "payload", Types.StringType.get())),
            ImmutableSet.of(2));

    private static final org.apache.kafka.connect.data.Schema KEY_CONNECT_SCHEMA =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();

    private static final org.apache.kafka.connect.data.Schema CONNECT_SCHEMA =
        SchemaBuilder.struct()
            .field("key", KEY_CONNECT_SCHEMA)
            .field("payload", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("op", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private final String payload;

    NestedKeyEvent(long id, String payload) {
      super(id);
      this.payload = payload;
    }

    String row() {
      return id() + "|" + payload;
    }

    static String row(Record record) {
      return ((Record) record.getField("key")).getField("id") + "|" + record.getField("payload");
    }

    @Override
    protected String serialize(boolean useSchema) {
      try {
        Struct value =
            new Struct(CONNECT_SCHEMA)
                .put("key", new Struct(KEY_CONNECT_SCHEMA).put("id", id()))
                .put("payload", payload)
                .put("op", "c");

        String convertMethod =
            useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
        JsonNode json =
            DynMethods.builder(convertMethod)
                .hiddenImpl(
                    JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
                .build(JSON_CONVERTER)
                .invoke(CONNECT_SCHEMA, value);
        return TestContext.MAPPER.writeValueAsString(json);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
