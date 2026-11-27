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

import static java.lang.String.format;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecords;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecordsAsString;
import static org.apache.iceberg.connect.service.IcebergTableClient.loadCatalogTable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.service.IcebergTableClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.AssertionsForClassTypes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

public class TestIntegrationTableStatistics extends AbstractTestBase {
  private static final String TEST_TABLE_1 = "test_table_1";
  private static final String TEST_TABLE_2 = "test_table_2";
  private static final TableIdentifier TEST_TABLE_IDENTIFIER_1 =
      TableIdentifier.of(TEST_DB, TEST_TABLE_1);
  private static final TableIdentifier TEST_TABLE_IDENTIFIER_2 =
      TableIdentifier.of(TEST_DB, TEST_TABLE_2);
  private static final List<TableIdentifier> TABLE_IDENTIFIERS =
      List.of(TEST_TABLE_IDENTIFIER_1, TEST_TABLE_IDENTIFIER_2);
  private static final Schema TEST_TABLE_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "username", Types.StringType.get()),
              Types.NestedField.required(3, "table", Types.StringType.get()),
              Types.NestedField.optional(4, "year", Types.IntegerType.get())),
          ImmutableSet.of(1));
  private static final List<String> COLUMN_LIST =
      TEST_TABLE_SCHEMA.columns().stream().map(Types.NestedField::name).toList();
  private static final List<TableUserEventExtended> TEST_TABLE_1_EVENTS =
      List.of(
          new TableUserEventExtended(1, "Ann", TEST_TABLE_IDENTIFIER_1.toString(), 2000),
          new TableUserEventExtended(2, "Sue", TEST_TABLE_IDENTIFIER_1.toString(), 2003));

  private static final List<String> TEST_TABLE_1_EVENTS_STRING =
      TEST_TABLE_1_EVENTS.stream().map(TableUserEventExtended::castToString).toList();

  private static final List<TableUserEventExtended> TEST_TABLE_2_EVENTS =
      List.of(
          new TableUserEventExtended(1, "John", TEST_TABLE_IDENTIFIER_2.toString(), 2007),
          new TableUserEventExtended(2, "Sam", TEST_TABLE_IDENTIFIER_2.toString(), 2010));

  private static final List<String> TEST_TABLE_2_EVENTS_STRING =
      TEST_TABLE_2_EVENTS.stream().map(TableUserEventExtended::castToString).toList();
  private static final List<TableUserEventExtended> EVENTS =
      Stream.concat(TEST_TABLE_1_EVENTS.stream(), TEST_TABLE_2_EVENTS.stream()).toList();

  private String spareConnector;
  private String spareTopic;

  @Test
  public void testTableStatisticsEnabled() {
    catalog().createTable(TEST_TABLE_IDENTIFIER_1, TEST_TABLE_SCHEMA);
    catalog().createTable(TEST_TABLE_IDENTIFIER_2, TEST_TABLE_SCHEMA);
    runTest(
        createConfig(false),
        Map.of(format("iceberg.table.%s.statistics.enabled", TEST_TABLE_IDENTIFIER_1), true),
        TABLE_IDENTIFIERS,
        EVENTS);
    Table testTable1 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_1);
    Table testTable2 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_2);
    assertThat(extractTableRecordsAsString(extractTableRecords(testTable1)))
        .containsExactlyInAnyOrderElementsOf(TEST_TABLE_1_EVENTS_STRING);
    assertThat(extractTableRecordsAsString(extractTableRecords(testTable2)))
        .containsExactlyInAnyOrderElementsOf(TEST_TABLE_2_EVENTS_STRING);
    assertAddedStatistics(testTable1, 1);
    assertThat(testTable1.statisticsFiles().get(0).snapshotId())
        .isEqualTo(testTable1.currentSnapshot().snapshotId());
    assertThat(testTable2.statisticsFiles()).isEmpty();
  }

  @Test
  public void testTablePartitionStatisticsEnabled() {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(TEST_TABLE_SCHEMA).identity("year").build();
    catalog().createTable(TEST_TABLE_IDENTIFIER_1, TEST_TABLE_SCHEMA, partitionSpec);
    catalog().createTable(TEST_TABLE_IDENTIFIER_2, TEST_TABLE_SCHEMA, partitionSpec);
    runTest(
        createConfig(false),
        Map.of(
            "iceberg.tables.statistics.enabled",
            true,
            format("iceberg.table.%s.statistics.partition-stats-enabled", TEST_TABLE_IDENTIFIER_1),
            true),
        TABLE_IDENTIFIERS,
        EVENTS);
    Table testTable1 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_1);
    Table testTable2 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_2);
    assertThat(extractTableRecordsAsString(extractTableRecords(testTable1)))
        .containsExactlyInAnyOrderElementsOf(TEST_TABLE_1_EVENTS_STRING);
    assertThat(extractTableRecordsAsString(extractTableRecords(testTable2)))
        .containsExactlyInAnyOrderElementsOf(TEST_TABLE_2_EVENTS_STRING);
    assertAddedStatistics(testTable1, 1);
    assertThat(testTable1.partitionStatisticsFiles().get(0).snapshotId())
        .isEqualTo(testTable1.currentSnapshot().snapshotId());
    assertThat(testTable2.partitionStatisticsFiles()).isEmpty();
  }

  @Test
  public void testTableStatisticsColumns() {
    catalog().createTable(TEST_TABLE_IDENTIFIER_1, TEST_TABLE_SCHEMA);
    catalog().createTable(TEST_TABLE_IDENTIFIER_2, TEST_TABLE_SCHEMA);
    List<String> testTable1StatisticsColumns =
        COLUMN_LIST.stream().filter(col -> !Objects.equals(col, "year")).toList();
    runTest(
        createCustomConfig(false, connectorName(), testTopic()),
        Map.of(
            "iceberg.tables.statistics.enabled",
            true,
            format("iceberg.table.%s.statistics.columns", TEST_TABLE_IDENTIFIER_1),
            String.join(",", testTable1StatisticsColumns)),
        TABLE_IDENTIFIERS,
        EVENTS);
    Table testTable1 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_1);
    Table testTable2 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_2);
    assertAddedStatistics(testTable1, 1);
    assertAddedStatistics(testTable2, 1);
    assertThat(extractTableRecordsAsString(extractTableRecords(testTable1)))
        .containsExactlyInAnyOrderElementsOf(TEST_TABLE_1_EVENTS_STRING);
    assertThat(extractTableRecordsAsString(extractTableRecords(testTable2)))
        .containsExactlyInAnyOrderElementsOf(TEST_TABLE_2_EVENTS_STRING);
    assertThat(getTableStatisticsColumns(testTable2))
        .containsExactlyInAnyOrderElementsOf(COLUMN_LIST);
    assertThat(getTableStatisticsColumns(testTable1))
        .containsExactlyInAnyOrderElementsOf(testTable1StatisticsColumns);
  }

  @Test
  public void testTableStatisticsNegative() {
    catalog().createTable(TEST_TABLE_IDENTIFIER_1, TEST_TABLE_SCHEMA);
    catalog().createTable(TEST_TABLE_IDENTIFIER_2, TEST_TABLE_SCHEMA);
    int rowsCount = 73;
    spareTopic = initTopicName();
    generateTopicRows(TEST_TABLE_IDENTIFIER_1, rowsCount, testTopic());
    generateTopicRows(TEST_TABLE_IDENTIFIER_2, rowsCount, spareTopic);
    launchConnector(
        createCustomConfig(false, connectorName(), testTopic()),
        Map.of(
            "iceberg.tables.statistics.enabled",
            true,
            "iceberg.tables.statistics.lg-nominal-entries",
            4),
        List.of(TEST_TABLE_IDENTIFIER_1));
    Table testTable1 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_1);
    assertAddedStatistics(testTable1, 1);
    assertThat(IcebergTableClient.extractTableRecords(testTable1).size()).isEqualTo(rowsCount);
    assertThat(testTable1.statisticsFiles().get(0).fileSizeInBytes()).isLessThan(3000);
    spareConnector = initConnectorName();
    launchConnector(
        createCustomConfig(false, spareConnector, spareTopic),
        Map.of(
            "iceberg.tables.statistics.enabled",
            true,
            format("iceberg.table.%s.statistics.lg-nominal-entries", TEST_TABLE_IDENTIFIER_2),
            4),
        List.of(TEST_TABLE_IDENTIFIER_2));
    Table testTable2 = loadCatalogTable(catalog(), TEST_TABLE_IDENTIFIER_2);
    assertAddedStatistics(testTable2, 1);
    assertThat(IcebergTableClient.extractTableRecords(testTable2).size()).isEqualTo(rowsCount);
    assertThat(testTable2.statisticsFiles().get(0).fileSizeInBytes()).isGreaterThanOrEqualTo(3000);
  }

  private List<String> getTableStatisticsColumns(Table table) {
    Schema schema = table.schema();
    return table.statisticsFiles().stream()
        .flatMap(statsFile -> statsFile.blobMetadata().stream())
        .flatMap(blob -> blob.fields().stream())
        .map(schema::findField)
        .filter(Objects::nonNull)
        .distinct()
        .map(Types.NestedField::name)
        .toList();
  }

  private void assertAddedStatistics(Table table, int statisticFilesSizeExpected) {
    table.refresh();
    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              table.refresh();
              AssertionsForClassTypes.assertThat(table.statisticsFiles().size())
                  .isEqualTo(statisticFilesSizeExpected);
            });
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCustomConfig(useSchema, connectorName(), testTopic());
  }

  private KafkaConnectUtils.Config createCustomConfig(
      boolean useSchema, String connectorName, String topicName) {
    return createKafkaConnectConfig(useSchema, topicName, connectorName)
        .config("iceberg.tables.dynamic-enabled", "true")
        .config("iceberg.tables.route-field", "table")
        .config("iceberg.tables.staging-orphan-ttl-ms", 2160000000L);
  }

  @Override
  protected void dropTables() {
    if (spareConnector != null) {
      context().stopConnector(spareConnector);
    }
    if (spareTopic != null) {
      deleteTopic(spareTopic);
    }
    catalog().dropTable(TEST_TABLE_IDENTIFIER_1, true);
    catalog().dropTable(TEST_TABLE_IDENTIFIER_2, true);
  }

  private void runTest(
      KafkaConnectUtils.Config connectorConfig,
      Map<String, Object> extraConfig,
      List<TableIdentifier> tableIdentifiers,
      List<? extends BaseTestEvent> events) {
    events.forEach(event -> send(testTopic(), event, false));
    launchConnector(connectorConfig, extraConfig, tableIdentifiers);
  }

  private void launchConnector(
      KafkaConnectUtils.Config connectorConfig,
      Map<String, Object> extraConfig,
      List<TableIdentifier> tableIdentifiers) {
    initConnectorConfig(connectorConfig, extraConfig);
    context().startConnector(connectorConfig);

    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(tableIdentifiers));
  }

  private void generateTopicRows(TableIdentifier tableIdentifier, int rowsCount, String topicName) {
    int year = 1900;
    for (int i = 1; i <= rowsCount; i++) {
      send(
          topicName,
          new TableUserEventExtended(
              i, UUID.randomUUID().toString().substring(0, 6), tableIdentifier.toString(), year),
          false);
      year++;
    }
  }
}
