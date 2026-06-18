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
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.apache.iceberg.connect.utils.KafkaBaseEventsUtils.KAFKA_BASE_EVENTS;
import static org.apache.iceberg.connect.v3.dto.EventExtended.EVENT_EXTENDED_SPEC;
import static org.apache.iceberg.connect.v3.dto.EventExtended.EVENT_EXTENDED_TABLE_SCHEMA;
import static org.apache.iceberg.connect.v3.dto.EventExtended.INFO_WRITE_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.connect.v3.dto.Event;
import org.apache.iceberg.connect.v3.dto.EventExtended;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBaseDefault extends IntegrationTestBaseV3 {

  @ParameterizedTest
  @MethodSource("argsProvider")
  public void testDefault(
      boolean useSchema, boolean isDefaultsEnabled, String infoValue, List<Event> events) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            EVENT_EXTENDED_TABLE_SCHEMA,
            EVENT_EXTENDED_SPEC,
            BASE_V3_TABLE_CONFIG);

    runTest(
        useSchema,
        addConnectorConfigs(
            context().connectorCatalogProperties(),
            ImmutableMap.of("iceberg.tables.defaults-enabled", String.valueOf(isDefaultsEnabled))),
        List.of(TABLE_IDENTIFIER),
        events);

    assertThat(
            extractTableRecords(loadCatalogTable(catalog(), TABLE_IDENTIFIER)).stream()
                .map(Object::toString)
                .toList())
        .hasSameElementsAs(castEventsToRecords(infoValue).stream().map(Object::toString).toList());
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(true, true, INFO_WRITE_DEFAULT, KAFKA_BASE_EVENTS),
        Arguments.of(false, true, INFO_WRITE_DEFAULT, KAFKA_BASE_EVENTS),
        Arguments.of(true, false, null, KAFKA_BASE_EVENTS),
        Arguments.of(false, false, null, KAFKA_BASE_EVENTS),
        Arguments.of(true, false, null, getEventsExtended()),
        Arguments.of(false, false, null, getEventsExtended()));
  }

  private static List<EventExtended> getEventsExtended() {
    return KAFKA_BASE_EVENTS.stream().map(event -> new EventExtended(event, null)).toList();
  }

  private List<org.apache.iceberg.data.Record> castEventsToRecords(String infoFieldValue) {
    Schema schema = loadCatalogTable(catalog(), TABLE_IDENTIFIER).schema();
    return KAFKA_BASE_EVENTS.stream()
        .map(
            event -> {
              org.apache.iceberg.data.Record record = GenericRecord.create(schema);
              record.setField("id", event.id());
              record.setField("username", event.username());
              record.setField("info", infoFieldValue);
              return record;
            })
        .toList();
  }
}
