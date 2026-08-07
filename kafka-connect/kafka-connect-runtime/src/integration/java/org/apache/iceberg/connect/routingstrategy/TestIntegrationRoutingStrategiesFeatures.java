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
package org.apache.iceberg.connect.routingstrategy;

import static java.lang.String.format;
import static org.apache.iceberg.connect.service.ConnectorService.addConnectorConfigs;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecords;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecordsAsString;
import static org.apache.iceberg.connect.service.IcebergTableClient.loadCatalogTable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.KafkaConnectUtils;
import org.apache.iceberg.connect.TestContext;
import org.apache.iceberg.connect.routingstrategy.dto.TableUserEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestIntegrationRoutingStrategiesFeatures
    extends IntegrationTestBaseRoutingStrategiesFeatures {

  private static final List<TableUserEvent> KAFKA_USER_EVENTS =
      List.of(
          new TableUserEvent(1, "Sam", CUSTOMERS_1_TABLE_IDENTIFIER.toString()),
          new TableUserEvent(2, "Ann", CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString()),
          new TableUserEvent(3, "Susan", CUSTOMERS_2_TABLE_IDENTIFIER.toString()),
          new TableUserEvent(4, "Emily", CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString()));

  private static final List<String> TABLE_USERS_DATA =
      KAFKA_USER_EVENTS.stream().map(TableUserEvent::castToString).collect(Collectors.toList());

  private static final List<TableIdentifier> FULL_TARGET_TABLES_LIST =
      List.of(
          CUSTOMERS_1_TABLE_IDENTIFIER,
          CUSTOMERS_1_TABLE_ALT_IDENTIFIER,
          CUSTOMERS_2_TABLE_IDENTIFIER,
          CUSTOMERS_2_TABLE_ALT_IDENTIFIER);

  private static final String CONNECTOR_CONFIGS_TABLES_LIST =
      FULL_TARGET_TABLES_LIST.stream()
          .map(TableIdentifier::toString)
          .collect(Collectors.joining(","));

  @ParameterizedTest
  @MethodSource("baseRoutingStrategiesArgsProvider")
  public void testBaseRoutingStrategiesSwitch(
      boolean useSchema,
      Map<String, Object> customConfig,
      List<TableIdentifier> expectedTargetTables,
      List<String> expectedCustomersFirstTableRows,
      List<String> expectedCustomersFirstAltTableRows,
      List<String> expectedCustomersSecTableRows,
      List<String> expectedCustomersSecAltTableRows) {
    runTest(
        useSchema,
        addConnectorConfigs(connectorConfigs(), customConfig),
        expectedTargetTables,
        KAFKA_USER_EVENTS);
    assertThat(
            extractTableRecordsAsString(
                extractTableRecords(loadCatalogTable(catalog(), CUSTOMERS_1_TABLE_IDENTIFIER))))
        .containsExactlyInAnyOrderElementsOf(expectedCustomersFirstTableRows);
    assertThat(
            extractTableRecordsAsString(
                extractTableRecords(loadCatalogTable(catalog(), CUSTOMERS_1_TABLE_ALT_IDENTIFIER))))
        .containsExactlyInAnyOrderElementsOf(expectedCustomersFirstAltTableRows);
    assertThat(
            extractTableRecordsAsString(
                extractTableRecords(loadCatalogTable(catalog(), CUSTOMERS_2_TABLE_IDENTIFIER))))
        .containsExactlyInAnyOrderElementsOf(expectedCustomersSecTableRows);
    assertThat(
            extractTableRecordsAsString(
                extractTableRecords(loadCatalogTable(catalog(), CUSTOMERS_2_TABLE_ALT_IDENTIFIER))))
        .containsExactlyInAnyOrderElementsOf(expectedCustomersSecAltTableRows);
  }

  @Test
  public void testTopicToTableRoutingStrategyNegative() {
    KafkaConnectUtils.Config connectorConfig =
        initConnectorConfig(
            true,
            addConnectorConfigs(
                connectorConfigs(),
                Map.of(
                    "routing.strategy",
                    "topic-to-table",
                    "iceberg.tables.topic-to-table-mapping-file",
                    "test-file.json")));
    KAFKA_USER_EVENTS.forEach(event -> send(testTopic(), event, true));
    KafkaConnectUtils.startConnector(connectorConfig);

    HttpGet request =
        new HttpGet(
            String.format(
                Locale.ROOT,
                "http://localhost:%d/connectors/%s/status",
                TestContext.CONNECT_PORT,
                connectorConfig.getName()));

    HttpClient httpClient = HttpClients.createDefault();
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .until(
            () ->
                httpClient.execute(
                    request,
                    response -> {
                      if (response.getCode() == HttpStatus.SC_OK) {
                        JsonNode root =
                            TestContext.MAPPER.readTree(response.getEntity().getContent());
                        ArrayNode taskNodes = (ArrayNode) root.get("tasks");
                        JsonNode firstTaskNode = taskNodes.get(0);
                        assertThat(firstTaskNode.get("state").asText()).isEqualTo("FAILED");
                        String trace = firstTaskNode.get("trace").asText();
                        assertThat(trace)
                            .containsIgnoringCase(
                                "org.apache.kafka.common.config.ConfigException: "
                                    + "Cannot specify both iceberg.tables.topic-to-table-mapping and iceberg.tables.topic-to-table-mapping-file");
                      }
                      return true;
                    }));
    flush();
  }

  private static Stream<Arguments> baseRoutingStrategiesArgsProvider() {
    return Stream.of(
            new Object[] {
              Map.of(
                  "routing.strategy",
                  "all-tables",
                  "iceberg.tables",
                  CONNECTOR_CONFIGS_TABLES_LIST),
              FULL_TARGET_TABLES_LIST,
              TABLE_USERS_DATA,
              TABLE_USERS_DATA,
              TABLE_USERS_DATA,
              TABLE_USERS_DATA
            },
            new Object[] {
              Map.of(
                  "routing.strategy",
                  "regex",
                  "iceberg.tables.route-field",
                  "table",
                  "iceberg.tables",
                  CONNECTOR_CONFIGS_TABLES_LIST,
                  format("iceberg.table.%s.route-regex", CUSTOMERS_1_TABLE_IDENTIFIER),
                  format("^%s.*$", CUSTOMERS_1_TABLE_IDENTIFIER),
                  format("iceberg.table.%s.route-regex", CUSTOMERS_1_TABLE_ALT_IDENTIFIER),
                  format("^%s$", CUSTOMERS_1_TABLE_ALT_IDENTIFIER),
                  format("iceberg.table.%s.route-regex", CUSTOMERS_2_TABLE_IDENTIFIER),
                  format("^%s.*$", CUSTOMERS_2_TABLE_IDENTIFIER),
                  format("iceberg.table.%s.route-regex", CUSTOMERS_2_TABLE_ALT_IDENTIFIER),
                  format("^%s$", CUSTOMERS_2_TABLE_ALT_IDENTIFIER)),
              FULL_TARGET_TABLES_LIST,
              getExpectedUserEvents(
                  List.of(
                      CUSTOMERS_1_TABLE_IDENTIFIER.toString(),
                      CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(
                  List.of(
                      CUSTOMERS_2_TABLE_IDENTIFIER.toString(),
                      CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString()))
            },
            new Object[] {
              Map.of("routing.strategy", "dynamic-field", "iceberg.tables.route-field", "table"),
              FULL_TARGET_TABLES_LIST,
              getExpectedUserEvents(List.of(CUSTOMERS_1_TABLE_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_2_TABLE_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString()))
            },
            new Object[] {
              Map.of("routing.strategy", "topic-to-table"),
              List.of(CUSTOMERS_1_TABLE_IDENTIFIER),
              TABLE_USERS_DATA,
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()
            },
            new Object[] {
              Map.of("iceberg.tables", CONNECTOR_CONFIGS_TABLES_LIST),
              FULL_TARGET_TABLES_LIST,
              TABLE_USERS_DATA,
              TABLE_USERS_DATA,
              TABLE_USERS_DATA,
              TABLE_USERS_DATA
            },
            new Object[] {
              Map.of(
                  "iceberg.tables.dynamic-enabled", "true", "iceberg.tables.route-field", "table"),
              FULL_TARGET_TABLES_LIST,
              getExpectedUserEvents(List.of(CUSTOMERS_1_TABLE_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_2_TABLE_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString()))
            },
            new Object[] {
              Map.of(
                  "iceberg.tables.route-field",
                  "table",
                  "iceberg.tables",
                  CONNECTOR_CONFIGS_TABLES_LIST,
                  format("iceberg.table.%s.route-regex", CUSTOMERS_1_TABLE_IDENTIFIER),
                  format("^%s.*$", CUSTOMERS_1_TABLE_IDENTIFIER),
                  format("iceberg.table.%s.route-regex", CUSTOMERS_1_TABLE_ALT_IDENTIFIER),
                  format("^%s$", CUSTOMERS_1_TABLE_ALT_IDENTIFIER),
                  format("iceberg.table.%s.route-regex", CUSTOMERS_2_TABLE_IDENTIFIER),
                  format("^%s.*$", CUSTOMERS_2_TABLE_IDENTIFIER),
                  format("iceberg.table.%s.route-regex", CUSTOMERS_2_TABLE_ALT_IDENTIFIER),
                  format("^%s$", CUSTOMERS_2_TABLE_ALT_IDENTIFIER)),
              FULL_TARGET_TABLES_LIST,
              getExpectedUserEvents(
                  List.of(
                      CUSTOMERS_1_TABLE_IDENTIFIER.toString(),
                      CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_1_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(
                  List.of(
                      CUSTOMERS_2_TABLE_IDENTIFIER.toString(),
                      CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString())),
              getExpectedUserEvents(List.of(CUSTOMERS_2_TABLE_ALT_IDENTIFIER.toString()))
            })
        .flatMap(
            (row ->
                Stream.of(
                    Arguments.of(true, row[0], row[1], row[2], row[3], row[4], row[5]),
                    Arguments.of(false, row[0], row[1], row[2], row[3], row[4], row[5]))));
  }

  private static List<String> getExpectedUserEvents(List<String> tableNames) {
    return KAFKA_USER_EVENTS.stream()
        .filter(tableUserEvent -> tableNames.contains(tableUserEvent.table()))
        .map(TableUserEvent::castToString)
        .collect(Collectors.toList());
  }

  private Map<String, Object> connectorConfigs() {
    return Map.of(
        "iceberg.tables.auto-update-enabled",
        "false",
        "iceberg.tables.topic-to-table-mapping",
        format("%s:%s", testTopic(), CUSTOMERS_1_TABLE_IDENTIFIER));
  }
}
