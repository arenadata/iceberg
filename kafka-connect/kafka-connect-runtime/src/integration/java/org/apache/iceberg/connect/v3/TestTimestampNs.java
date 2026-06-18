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

import static org.apache.iceberg.connect.utils.ConnectorUtils.V3_AUTO_CREATE_CONNECTOR_CONFIGS;
import static org.apache.iceberg.connect.utils.ConnectorUtils.addConnectorConfigs;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecords;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.v3.dto.TimestampNsEvent;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTimestampNs extends IntegrationTestBaseV3 {

  private static final List<TimestampNsEvent> KAFKA_TIMESTAMP_NS_EVENTS =
      List.of(
          new TimestampNsEvent(
              1,
              "Sam",
              1714821017000000000L,
              new TimestampNsEvent.TimestampNsFinishTime(1714821017345600000L),
              new TimestampNsEvent.TimestampNsEventTime(1714821017845600000L)),
          new TimestampNsEvent(
              1,
              "Sam",
              1714821017000000000L,
              new TimestampNsEvent.TimestampNsFinishTime(1714821017345600000L),
              new TimestampNsEvent.TimestampNsEventTime(1714821017845600000L)));

  @ParameterizedTest
  @MethodSource("argsProvider")
  public void testTimestampNs(
      Map<String, Object> connectorCustomConfigs,
      Type eventTimeType,
      Type finishTimeType,
      Type checkStatsEventTimeType) {
    runTest(
        true,
        addConnectorConfigs(context().connectorCatalogProperties(), connectorCustomConfigs),
        List.of(TABLE_IDENTIFIER),
        KAFKA_TIMESTAMP_NS_EVENTS);

    Schema expectedSchema =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "username", Types.StringType.get()),
                Types.NestedField.required(3, "event_time", eventTimeType),
                Types.NestedField.required(
                    4,
                    "user_stats",
                    Types.StructType.of(
                        Types.NestedField.required(6, "finish_time", finishTimeType))),
                Types.NestedField.required(
                    5,
                    "check_stats",
                    Types.StructType.of(
                        Types.NestedField.required(7, "event_time", checkStatsEventTimeType)))),
            ImmutableSet.of());

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER);

    assertThat(table.schema().columns()).hasSameElementsAs(expectedSchema.columns());

    List<org.apache.iceberg.data.Record> presetRecords =
        castEventsToRecords(eventTimeType, finishTimeType, checkStatsEventTimeType);

    assertThat(extractTableRecords(table)).containsExactlyInAnyOrderElementsOf(presetRecords);
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(
            V3_AUTO_CREATE_CONNECTOR_CONFIGS,
            Types.TimestampNanoType.withZone(),
            Types.TimestampNanoType.withZone(),
            Types.TimestampNanoType.withZone()),
        Arguments.of(
            addConnectorConfigs(
                V3_AUTO_CREATE_CONNECTOR_CONFIGS,
                Map.of("iceberg.tables.schema-timestamp-ns-fields", "event_time")),
            Types.TimestampNanoType.withoutZone(),
            Types.TimestampNanoType.withZone(),
            Types.TimestampNanoType.withoutZone()),
        Arguments.of(
            addConnectorConfigs(
                V3_AUTO_CREATE_CONNECTOR_CONFIGS,
                Map.of(
                    "iceberg.tables.schema-timestamp-ns-fields",
                    "check_stats.event_time,user_stats.finish_time")),
            Types.TimestampNanoType.withZone(),
            Types.TimestampNanoType.withoutZone(),
            Types.TimestampNanoType.withoutZone()),
        Arguments.of(
            addConnectorConfigs(
                V3_AUTO_CREATE_CONNECTOR_CONFIGS,
                Map.of("iceberg.tables.schema-timestamp-ns-fields", "*")),
            Types.TimestampNanoType.withoutZone(),
            Types.TimestampNanoType.withoutZone(),
            Types.TimestampNanoType.withoutZone()));
  }

  private List<org.apache.iceberg.data.Record> castEventsToRecords(
      Type eventTimeType, Type finishTimeType, Type checkStatsEventTimeType) {
    Schema schema = loadCatalogTable(catalog(), TABLE_IDENTIFIER).schema();
    return KAFKA_TIMESTAMP_NS_EVENTS.stream()
        .map(
            event -> {
              org.apache.iceberg.data.Record record = GenericRecord.create(schema);
              record.setField("id", event.id());
              record.setField("username", event.username());
              setTimeRecordField(eventTimeType, "event_time", record, event.eventTime());
              record.setField(
                  "user_stats",
                  setNestedRecord(
                      finishTimeType, "finish_time", event.userStats().finishTime(), "user_stats"));
              record.setField(
                  "check_stats",
                  setNestedRecord(
                      checkStatsEventTimeType,
                      "event_time",
                      event.checkStats().eventTime(),
                      "check_stats"));
              return record;
            })
        .toList();
  }

  private void setTimeRecordField(
      Type timeFieldType,
      String columnName,
      org.apache.iceberg.data.Record record,
      long eventLongValue) {
    record.setField(
        columnName,
        timeFieldType.equals(Types.TimestampNanoType.withZone())
            ? DateTimeUtil.timestamptzFromMicros(eventLongValue / 1000)
            : LocalDateTime.ofInstant(
                Instant.ofEpochSecond(
                    eventLongValue / 1_000_000_000, eventLongValue % 1_000_000_000),
                ZoneId.of("UTC")));
  }

  private org.apache.iceberg.data.Record setNestedRecord(
      Type timeFieldType, String subFieldName, long eventLongValue, String fieldName) {
    Types.StructType nestedRecordSchema =
        loadCatalogTable(catalog(), TABLE_IDENTIFIER).schema().findType(fieldName).asStructType();
    org.apache.iceberg.data.Record subRecord = GenericRecord.create(nestedRecordSchema);
    setTimeRecordField(timeFieldType, subFieldName, subRecord, eventLongValue);
    return subRecord;
  }
}
