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

import static org.apache.iceberg.connect.routingstrategy.dto.UserEvent.USER_SCHEMA;
import static org.apache.iceberg.connect.routingstrategy.dto.UserEvent.USER_SPEC;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.AbstractTestBase;
import org.apache.iceberg.connect.KafkaConnectUtils;
import org.apache.iceberg.connect.routingstrategy.dto.UserEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;

public abstract class IntegrationTestBaseRoutingStrategiesFeatures extends AbstractTestBase {
  private static final String TABLE_TEST_SUFFIX = "123";
  protected static final String CUSTOMERS_1_TABLE = "customers_first";
  protected static final String CUSTOMERS_1_TABLE_ALT = CUSTOMERS_1_TABLE + TABLE_TEST_SUFFIX;
  protected static final TableIdentifier CUSTOMERS_1_TABLE_IDENTIFIER =
      TableIdentifier.of(TEST_DB, CUSTOMERS_1_TABLE);
  protected static final TableIdentifier CUSTOMERS_1_TABLE_ALT_IDENTIFIER =
      TableIdentifier.of(TEST_DB, CUSTOMERS_1_TABLE_ALT);
  protected static final String CUSTOMERS_2_TABLE = "customers_second";
  protected static final String CUSTOMERS_2_TABLE_ALT = CUSTOMERS_2_TABLE + TABLE_TEST_SUFFIX;
  protected static final TableIdentifier CUSTOMERS_2_TABLE_IDENTIFIER =
      TableIdentifier.of(TEST_DB, CUSTOMERS_2_TABLE);
  protected static final TableIdentifier CUSTOMERS_2_TABLE_ALT_IDENTIFIER =
      TableIdentifier.of(TEST_DB, CUSTOMERS_2_TABLE_ALT);

  @BeforeEach
  public void beforeEach() {
    List.of(
            CUSTOMERS_1_TABLE_IDENTIFIER, CUSTOMERS_1_TABLE_ALT_IDENTIFIER,
            CUSTOMERS_2_TABLE_IDENTIFIER, CUSTOMERS_2_TABLE_ALT_IDENTIFIER)
        .forEach(table -> catalog().createTable(table, USER_SCHEMA, USER_SPEC));
  }

  protected void runTest(
      boolean useSchema,
      Map<String, Object> extraConfig,
      List<TableIdentifier> tableIdentifiers,
      List<? extends UserEvent> events) {
    KafkaConnectUtils.Config connectorConfig = initConnectorConfig(useSchema, extraConfig);
    events.forEach(event -> send(testTopic(), event, useSchema));

    context().startConnector(connectorConfig);

    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(tableIdentifiers));
  }

  protected KafkaConnectUtils.Config initConnectorConfig(
      boolean useSchema, Map<String, Object> extraConfig) {
    KafkaConnectUtils.Config connectorConfig = createConfig(useSchema);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    extraConfig.forEach(connectorConfig::config);
    return connectorConfig;
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables.evolve-schema-enabled", "false")
        .config("tasks.max", "1");
  }

  @Override
  protected void dropTables() {
    List.of(
            CUSTOMERS_1_TABLE_IDENTIFIER, CUSTOMERS_1_TABLE_ALT_IDENTIFIER,
            CUSTOMERS_2_TABLE_IDENTIFIER, CUSTOMERS_2_TABLE_ALT_IDENTIFIER)
        .forEach(table -> catalog().dropTable(table));
    catalog().dropTable(CUSTOMERS_1_TABLE_IDENTIFIER);
    catalog().dropTable(CUSTOMERS_2_TABLE_IDENTIFIER);
  }
}
