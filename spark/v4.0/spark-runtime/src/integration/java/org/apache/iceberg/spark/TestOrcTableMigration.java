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
package org.apache.iceberg.spark;

import static java.lang.String.format;
import static org.apache.iceberg.spark.IcebergCatalogService.BASE_COLUMN_SCHEMA;
import static org.apache.iceberg.spark.IcebergCatalogService.CATALOG_TABLE_NAME;
import static org.apache.iceberg.spark.IcebergCatalogService.RECORDS;
import static org.apache.iceberg.spark.IcebergCatalogService.TABLE_IDENTIFIER;
import static org.apache.iceberg.spark.IcebergCatalogService.TEST_TABLE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestOrcTableMigration extends AbstractTestBase {

  private static final String TIMESTAMP_VAL =
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));

  private static final Column[] COLUMN_SCHEMA_WITH_TIMESTAMP =
      Stream.concat(
              Arrays.stream(BASE_COLUMN_SCHEMA),
              Stream.of(Column.create("create_time", DataTypes.TimestampType, true)))
          .toArray(Column[]::new);

  @Test
  public void testOrcMigrationTableWithTimestampColumn()
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            COLUMN_SCHEMA_WITH_TIMESTAMP,
            new Transform[0],
            Map.of("write.format.default", "orc"));
    insertData(CATALOG_TABLE_NAME, getRecords(true));
    assertRecords(CATALOG_TABLE_NAME, getRecords(false));
    assertThat(extractTableDataFiles(TEST_TABLE).stream().findAny().get()).contains("orc");
    int abc = 4;
  }

  private List<String> getRecords(boolean isInsert) {
    return RECORDS.stream()
        .map(
            row ->
                format(
                    "%s, %s",
                    row, isInsert ? format("TIMESTAMP '%s'", TIMESTAMP_VAL) : TIMESTAMP_VAL))
        .collect(Collectors.toList());
  }
}
