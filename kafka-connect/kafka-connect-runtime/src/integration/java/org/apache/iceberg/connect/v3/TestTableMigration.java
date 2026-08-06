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

import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecords;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecordsAsString;
import static org.apache.iceberg.connect.service.IcebergTableClient.loadCatalogTable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.v3.dto.Info;
import org.apache.iceberg.connect.v3.dto.StructUserEvent;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestTableMigration extends IntegrationTestBaseV3 {

  @ParameterizedTest
  @CsvSource({"true", "false"})
  public void testMigration(boolean useSchema) {
    StructUserEvent<Info> eventOne = new StructUserEvent(1, "Sam", new Info(15));
    runTest(
        useSchema,
        Map.of("iceberg.tables.auto-create-enabled", "true"),
        List.of(TABLE_IDENTIFIER_V3),
        List.of(eventOne));

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER_V3);

    assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(2);

    assertThat(extractTableRecordsAsString(extractTableRecords(table)))
        .containsExactlyInAnyOrderElementsOf(
            List.of(eventOne).stream().map(event -> event.castToString()).toList());

    table.updateProperties().set("format-version", "3").commit();

    StructUserEvent<Info> eventTwo = new StructUserEvent(2, "Susan", new Info(16));

    send(testTopic(), eventTwo, useSchema);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              table.refresh();
              assertThat(extractTableRecords(table).size()).isEqualTo(2);
            });

    assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(3);

    assertThat(extractTableRecordsAsString(extractTableRecords(table)))
        .containsExactlyInAnyOrderElementsOf(
            List.of(eventOne, eventTwo).stream().map(event -> event.castToString()).toList());
  }
}
