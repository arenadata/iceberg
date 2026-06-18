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
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecords;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.v3.dto.StructEvent;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestTableMigration extends IntegrationTestBaseV3 {

  private static final List<StructEvent> KAFKA_EVENTS =
      List.of(new StructEvent(1, "Sam", new StructEvent.Info(15)));

  @ParameterizedTest
  @CsvSource({"true", "false"})
  public void testMigration(boolean useSchema) {

    runTest(
        useSchema,
        addConnectorConfigs(
            context().connectorCatalogProperties(),
            Map.of("iceberg.tables.auto-create-enabled", "true")),
        List.of(TABLE_IDENTIFIER),
        KAFKA_EVENTS);

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER);

    assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(2);

    assertThat(extractTableRecords(table).stream().map(Object::toString).toList())
        .containsExactlyInAnyOrderElementsOf(
            castEventsToRecords(table, KAFKA_EVENTS).stream().map(Object::toString).toList());

    table.updateProperties().set("format-version", "3").commit();

    StructEvent newEvent = new StructEvent(2, "Susan", new StructEvent.Info(16));

    long before = table.currentSnapshot().snapshotId();

    send(testTopic(), newEvent, useSchema);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .until(
            () -> {
              table.refresh();

              return table.currentSnapshot() != null
                  && table.currentSnapshot().snapshotId() != before;
            });

    List<StructEvent> fullEventsList =
        Stream.concat(KAFKA_EVENTS.stream(), Stream.of(newEvent)).toList();

    assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(3);

    assertThat(extractTableRecords(table).stream().map(Object::toString).toList())
        .containsExactlyInAnyOrderElementsOf(
            castEventsToRecords(table, fullEventsList).stream().map(Object::toString).toList());
  }

  private List<org.apache.iceberg.data.Record> castEventsToRecords(
      Table table, List<StructEvent> kafkaEvents) {
    Schema schema = table.schema();
    return kafkaEvents.stream()
        .map(
            event -> {
              org.apache.iceberg.data.Record record = GenericRecord.create(schema);
              record.setField("id", event.id());
              record.setField("username", event.username());
              record.setField("info", setNestedRecord(table, event.info().age()));
              return record;
            })
        .toList();
  }

  private org.apache.iceberg.data.Record setNestedRecord(Table table, int ageValue) {
    Types.StructType nestedRecordSchema = table.schema().findType("info").asStructType();
    org.apache.iceberg.data.Record subRecord = GenericRecord.create(nestedRecordSchema);
    subRecord.setField("age", ageValue);
    return subRecord;
  }
}
