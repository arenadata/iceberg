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

import static org.apache.iceberg.connect.service.ConnectorService.V3_AUTO_CREATE_CONNECTOR_CONFIGS;
import static org.apache.iceberg.connect.service.ConnectorService.addConnectorConfigs;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecords;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecordsAsString;
import static org.apache.iceberg.connect.service.IcebergTableClient.loadCatalogTable;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.v3.dto.Info;
import org.apache.iceberg.connect.v3.dto.StructUserEvent;
import org.apache.iceberg.connect.v3.dto.UserEvent;
import org.apache.iceberg.connect.v3.dto.UserEventExtended;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVariant extends IntegrationTestBaseV3 {
  private static final List<? extends UserEvent> KAFKA_VARIANT_EVENTS =
      List.of(
          new UserEventExtended(1L, "Sam", "active"),
          new StructUserEvent<>(2, "Susan", new Info(15)));

  @ParameterizedTest
  @MethodSource("argsProvider")
  public void testVariant(boolean useSchema, Schema tableSchema) {
    runTest(
        useSchema,
        addConnectorConfigs(
            addConnectorConfigs(
                context().connectorCatalogProperties(), V3_AUTO_CREATE_CONNECTOR_CONFIGS),
            Map.of("iceberg.tables.schema-variant-fields", "info")),
        List.of(TABLE_IDENTIFIER_V3),
        KAFKA_VARIANT_EVENTS);

    Table table = loadCatalogTable(catalog(), TABLE_IDENTIFIER_V3);

    assertThat(table.schema().columns()).hasSameElementsAs(tableSchema.columns());

    assertThat(extractTableRecordsAsString(extractTableRecords(table)))
        .hasSameElementsAs(
            KAFKA_VARIANT_EVENTS.stream().map(event -> event.castToString()).toList());
  }

  private static Stream<Arguments> argsProvider() {
    return Stream.of(
        Arguments.of(
            true,
            new Schema(
                ImmutableList.of(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "username", Types.StringType.get()),
                    Types.NestedField.optional(3, "info", Types.VariantType.get())))),
        Arguments.of(
            false,
            new Schema(
                ImmutableList.of(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "username", Types.StringType.get()),
                    Types.NestedField.optional(3, "info", Types.VariantType.get())))));
  }
}
