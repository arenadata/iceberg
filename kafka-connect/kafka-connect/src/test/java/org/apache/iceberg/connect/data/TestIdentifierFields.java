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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestIdentifierFields {

  private static final Schema SCHEMA_WITH_IDENTIFIER =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get())),
          ImmutableSet.of(1, 3));

  private static final Schema SCHEMA_WITHOUT_IDENTIFIER =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get())));

  private static final TableReference TABLE_REFERENCE =
      TableReference.of("test_catalog", TableIdentifier.of("test_table"), UUID.randomUUID());

  @Test
  public void testResolveUsesTableIdentifierFieldsWhenIdColumnsNotConfigured() {
    Table table = mockTable(SCHEMA_WITH_IDENTIFIER);
    IcebergSinkConfig config = mockConfig(ImmutableList.of());

    assertThat(IdentifierFields.resolve(table, TABLE_REFERENCE, config))
        .containsExactlyInAnyOrder(1, 3);
  }

  @Test
  public void testResolveReturnsEmptyWhenNeitherSchemaNorConfigDefinesThem() {
    Table table = mockTable(SCHEMA_WITHOUT_IDENTIFIER);
    IcebergSinkConfig config = mockConfig(ImmutableList.of());

    assertThat(IdentifierFields.resolve(table, TABLE_REFERENCE, config)).isEmpty();
  }

  @Test
  public void testResolveIdColumnsOverrideTableIdentifierFields() {
    Table table = mockTable(SCHEMA_WITH_IDENTIFIER);
    IcebergSinkConfig config = mockConfig(ImmutableList.of("data"));

    assertThat(IdentifierFields.resolve(table, TABLE_REFERENCE, config)).containsExactly(2);
  }

  @Test
  public void testResolveIdColumnsOnSchemaWithoutIdentifierFields() {
    Table table = mockTable(SCHEMA_WITHOUT_IDENTIFIER);
    IcebergSinkConfig config = mockConfig(ImmutableList.of("id", "data"));

    assertThat(IdentifierFields.resolve(table, TABLE_REFERENCE, config))
        .containsExactlyInAnyOrder(1, 2);
  }

  @Test
  public void testResolveThrowsWhenIdColumnNotFound() {
    Table table = mockTable(SCHEMA_WITH_IDENTIFIER);
    IcebergSinkConfig config = mockConfig(ImmutableList.of("missing"));

    assertThatThrownBy(() -> IdentifierFields.resolve(table, TABLE_REFERENCE, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ID column not found: missing");
  }

  private Table mockTable(Schema schema) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(schema);
    return table;
  }

  private IcebergSinkConfig mockConfig(List<String> idColumns) {
    TableSinkConfig tableSinkConfig = mock(TableSinkConfig.class);
    when(tableSinkConfig.idColumns()).thenReturn(idColumns);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(tableSinkConfig);
    return config;
  }
}
