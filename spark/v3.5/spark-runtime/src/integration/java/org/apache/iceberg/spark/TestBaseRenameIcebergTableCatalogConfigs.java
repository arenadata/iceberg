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
import static org.apache.iceberg.spark.IcebergCatalogService.CATALOG_TABLE_NEW_NAME;
import static org.apache.iceberg.spark.IcebergCatalogService.RECORDS;
import static org.apache.iceberg.spark.IcebergCatalogService.TABLE_IDENTIFIER;
import static org.apache.iceberg.spark.IcebergCatalogService.TABLE_IDENTIFIER_NEW;
import static org.apache.iceberg.spark.IcebergCatalogService.TEST_TABLE;
import static org.apache.iceberg.spark.IcebergCatalogService.TEST_TABLE_NEW;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBaseRenameIcebergTableCatalogConfigs extends AbstractTestBase {

  @TestTemplate
  public void testRenameMetadataLocationUpdate(
      boolean isRenameMetadataLocationUpdateEnabled,
      String tableDir,
      List<String> namespaceContents)
      throws TableAlreadyExistsException,
          NoSuchNamespaceException,
          NoSuchTableException,
          IOException {
    initSpark(
        IcebergCatalogType.HIVE,
        Map.of(
            "rename.metadata.location.update",
            String.valueOf(isRenameMetadataLocationUpdateEnabled)));
    catalog().createTable(TABLE_IDENTIFIER, BASE_COLUMN_SCHEMA, new Transform[0], Map.of());
    insertData(CATALOG_TABLE_NAME, RECORDS.subList(0, 2));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER)))
        .isEqualTo(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    AssertionsForInterfaceTypes.assertThat(
            extractFileSystemContents(IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE)));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, CATALOG_TABLE_NEW_NAME));
    insertData(CATALOG_TABLE_NEW_NAME, RECORDS.subList(2, 4));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), tableDir));
    AssertionsForInterfaceTypes.assertThat(
            extractFileSystemContents(IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(namespaceContents);
  }

  @Parameters(
      name = "isRenameMetadataLocationUpdateEnabled={0}, tableDir={1}, namespaceContents={2}")
  private static List<Object[]> renameMetadataLocationUpdateArgsProvider() {
    return Arrays.asList(
        new Object[] {
          true,
          TEST_TABLE_NEW,
          List.of(
              format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE),
              format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW))
        },
        new Object[] {
          false,
          TEST_TABLE,
          List.of(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE))
        });
  }
}
