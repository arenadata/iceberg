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
import static org.apache.iceberg.connect.utils.ConnectorUtils.V3_AUTO_CREATE_CONNECTOR_CONFIGS;
import static org.apache.iceberg.connect.utils.ConnectorUtils.addConnectorConfigs;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.S3_CLIENT;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecords;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTableLocation;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.refreshTableData;
import static org.apache.iceberg.connect.utils.KafkaBaseEventsUtils.KAFKA_BASE_EVENTS;
import static org.apache.iceberg.connect.utils.KafkaBaseEventsUtils.castKafkaBaseEventsToRecords;
import static org.apache.iceberg.connect.utils.RestCatalogSparkUtil.runSparkSqlQuery;
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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

public class TestDeletionVectors extends IntegrationTestBaseV3 {
  private static final String ICEBERG_REST_CATALOG = "spark_catalog";
  private static final String STANDARD_DELETION_MECHANISM = "copy-on-write";
  private static final String DELETION_VECTOR_MECHANISM = "merge-on-read";

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
        List.of(TABLE_IDENTIFIER),
        KAFKA_BASE_EVENTS);

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER);

    assertThat(extractTableRecords(table))
        .containsExactlyInAnyOrderElementsOf(castKafkaBaseEventsToRecords(table.schema()));

    long preDeleteTableSnapshot = refreshTableData(table);

    runSparkSqlQuery(
        format("DELETE FROM %s.%s WHERE id=2", ICEBERG_REST_CATALOG, TABLE_IDENTIFIER));

    String tableDataPath =
        format("%s/data/", loadCatalogTableLocation(loadCatalogTable(catalog(), TABLE_IDENTIFIER)))
            .replaceFirst("/", "");

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .until(() -> refreshTableData(table) != preDeleteTableSnapshot);

    assertThat(
            S3_CLIENT
                .listObjectsV2(
                    ListObjectsV2Request.builder().bucket("bucket").prefix(tableDataPath).build())
                .contents()
                .stream()
                .anyMatch(o -> o.key().endsWith(".puffin")))
        .isEqualTo(isMergeOnReadEnabled);

    List<org.apache.iceberg.data.Record> remainedTableRecords = extractTableRecords(table);
    assertThat(remainedTableRecords).hasSize(1);
    assertThat(remainedTableRecords.get(0).getField("id")).isEqualTo(1L);
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(true, DELETION_VECTOR_MECHANISM, true),
        Arguments.of(false, DELETION_VECTOR_MECHANISM, true),
        Arguments.of(true, STANDARD_DELETION_MECHANISM, false),
        Arguments.of(false, STANDARD_DELETION_MECHANISM, false));
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER, true);
  }
}
