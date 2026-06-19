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
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecordsAsString;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.v3.dto.Event;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestUnknown extends IntegrationTestBaseV3 {
  private static final List<Event> BASE_NULL_EVENTS =
      List.of(new Event(1, null), new Event(2, null));

  @ParameterizedTest
  @MethodSource("argsProvider")
  public void testUnknown(boolean isUnknownSupported, Schema schema) {

    runTest(
        false,
        addConnectorConfigs(
            addConnectorConfigs(
                context().connectorCatalogProperties(), V3_AUTO_CREATE_CONNECTOR_CONFIGS),
            Map.of(
                "iceberg.tables.evolve-unknown-type-enabled", String.valueOf(isUnknownSupported))),
        List.of(TABLE_IDENTIFIER),
        BASE_NULL_EVENTS);

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER);

    assertThat(table.schema().columns()).hasSameElementsAs(schema.columns());

    assertThat(extractTableRecordsAsString(extractTableRecords(table)))
        .containsExactlyInAnyOrderElementsOf(
            BASE_NULL_EVENTS.stream()
                .map(
                    event -> isUnknownSupported ? event.castToString() : String.valueOf(event.id()))
                .toList());
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(
            true,
            new Schema(
                ImmutableList.of(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "username", Types.UnknownType.get())),
                ImmutableSet.of())),
        Arguments.of(
            false,
            new Schema(
                ImmutableList.of(Types.NestedField.optional(1, "id", Types.LongType.get())),
                ImmutableSet.of())));
  }
}
