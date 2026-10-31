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
package org.apache.iceberg.connect.v3;

import static java.lang.String.format;
import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.apache.iceberg.connect.service.ConnectorService.V3_AUTO_CREATE_CONNECTOR_CONFIGS;
import static org.apache.iceberg.connect.service.ConnectorService.addConnectorConfigs;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableFiles;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecords;
import static org.apache.iceberg.connect.service.IcebergTableClient.getTablePath;
import static org.apache.iceberg.connect.service.IcebergTableClient.loadCatalogTable;
import static org.apache.iceberg.connect.service.KafkaBaseEventsService.KAFKA_BASE_EVENTS;
import static org.apache.iceberg.connect.service.KafkaBaseEventsService.castKafkaBaseEventsToRecords;
import static org.apache.iceberg.connect.service.RestCatalogSparkClient.runSparkSqlQuery;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestDeletionVectors extends IntegrationTestBaseV3 {
  private static final String ICEBERG_REST_CATALOG = "spark_catalog";

  @ParameterizedTest
  @MethodSource("argsProvider")
  public void testDeletionVector(
      boolean useSchema, String writeDeleteMode, boolean isMergeOnReadEnabled)
      throws InterruptedException {
    Map<String, Object> connectorConfigs =
        Map.of("iceberg.tables.auto-create-props.write.delete.mode", writeDeleteMode);

    runTest(
        useSchema,
        addConnectorConfigs(V3_AUTO_CREATE_CONNECTOR_CONFIGS, connectorConfigs),
        List.of(TABLE_IDENTIFIER_V3),
        KAFKA_BASE_EVENTS);

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER_V3);

    assertThat(extractTableRecords(table))
        .containsExactlyInAnyOrderElementsOf(castKafkaBaseEventsToRecords(table.schema()));

    runSparkSqlQuery(
        format(
            "DELETE FROM %1$s.%2$s WHERE id=2;REFRESH TABLE %1$s.%2$s;",
            ICEBERG_REST_CATALOG, TABLE_IDENTIFIER_V3));

    String tableDataPath = getTablePath(catalog(), TABLE_IDENTIFIER_V3);

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              table.refresh();
              assertThat(extractTableRecords(table).size()).isEqualTo(1);
            });

    assertThat(extractTableFiles(tableDataPath).stream().anyMatch(o -> o.key().endsWith(".puffin")))
        .isEqualTo(isMergeOnReadEnabled);

    List<org.apache.iceberg.data.Record> remainedTableRecords = extractTableRecords(table);
    assertThat(remainedTableRecords.get(0).getField("id")).isEqualTo(1L);
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(true, MERGE_ON_READ.modeName(), true),
        Arguments.of(false, MERGE_ON_READ.modeName(), true),
        Arguments.of(true, COPY_ON_WRITE.modeName(), false),
        Arguments.of(false, COPY_ON_WRITE.modeName(), false));
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER_V3, true);
  }
}
