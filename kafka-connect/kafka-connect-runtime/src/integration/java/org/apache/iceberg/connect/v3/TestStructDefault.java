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

import static org.apache.iceberg.connect.utils.ConnectorUtils.addConnectorConfigs;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.BASE_V3_TABLE_CONFIG;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecords;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecordsAsString;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.apache.iceberg.connect.v3.dto.EventExtended.INFO_WRITE_DEFAULT;
import static org.apache.iceberg.connect.v3.dto.StructEvent.EVENT_STRUCT_TABLE_SCHEMA;
import static org.apache.iceberg.connect.v3.dto.StructEvent.TEST_STRUCT_SPEC;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.v3.dto.Event;
import org.apache.iceberg.connect.v3.dto.Info;
import org.apache.iceberg.connect.v3.dto.InfoExtended;
import org.apache.iceberg.connect.v3.dto.StructEvent;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestStructDefault extends IntegrationTestBaseV3 {

  private static final List<StructEvent<Info>> KAFKA_STRUCT_EVENTS =
      List.of(new StructEvent(1, "Sam", new Info(15)), new StructEvent(2, "Susan", new Info(14)));

  @ParameterizedTest
  @MethodSource("argsProvider")
  public void testStructDefault(
      boolean useSchema,
      boolean isDefaultsEnabled,
      String status,
      List<? extends Event> kafkaEvents) {
    catalog()
        .createTable(
                TABLE_IDENTIFIER_V3, EVENT_STRUCT_TABLE_SCHEMA, TEST_STRUCT_SPEC, BASE_V3_TABLE_CONFIG);

    runTest(
        useSchema,
        addConnectorConfigs(
            context().connectorCatalogProperties(),
            ImmutableMap.of("iceberg.tables.defaults-enabled", String.valueOf(isDefaultsEnabled))),
        List.of(TABLE_IDENTIFIER_V3),
        kafkaEvents);

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER_V3);

    assertThat(extractTableRecordsAsString(extractTableRecords(table)))
        .hasSameElementsAs(
            castStructEventsToStructEventsExtended(status).stream()
                .map(event -> event.castToString())
                .toList());
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(true, true, INFO_WRITE_DEFAULT, KAFKA_STRUCT_EVENTS),
        Arguments.of(false, true, INFO_WRITE_DEFAULT, KAFKA_STRUCT_EVENTS),
        Arguments.of(true, false, null, KAFKA_STRUCT_EVENTS),
        Arguments.of(false, false, null, KAFKA_STRUCT_EVENTS),
        Arguments.of(true, false, null, castStructEventsToStructEventsExtended(null)),
        Arguments.of(false, false, null, castStructEventsToStructEventsExtended(null)));
  }

  private static List<StructEvent<InfoExtended>> castStructEventsToStructEventsExtended(
      String status) {
    return KAFKA_STRUCT_EVENTS.stream()
        .map(
            testStructEvent ->
                new StructEvent<>(
                    testStructEvent.id(),
                    testStructEvent.username(),
                    new InfoExtended(testStructEvent.info().age(), status)))
        .toList();
  }
}
