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
import static org.apache.iceberg.spark.IcebergCatalogProperties.ALT_RECORDS;
import static org.apache.iceberg.spark.IcebergCatalogProperties.BASE_COLUMN_SCHEMA;
import static org.apache.iceberg.spark.IcebergCatalogProperties.CATALOG_TEST_TABLE;
import static org.apache.iceberg.spark.IcebergCatalogProperties.CATALOG_TEST_TABLE_NEW;
import static org.apache.iceberg.spark.IcebergCatalogProperties.RECORDS;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TABLE_IDENTIFIER;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TABLE_IDENTIFIER_NEW;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_DB;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE_NEW;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;

public class TestRenameIcebergTableCatalogConfigs extends IntegrationTestBase {

  @Test
  public void testRenameMetadataLocationUpdateRestNegative()
      throws IOException, TableAlreadyExistsException, NoSuchNamespaceException {
    initSpark(IcebergCatalogType.REST, Map.of("rename.metadata.location.update", "true"));
    sparkCatalog().createTable(TABLE_IDENTIFIER, BASE_COLUMN_SCHEMA, new Transform[0], Map.of());
    List<String> namespaceDirsBeforeAlter =
        List.of(format("%s/%s", IcebergCatalogType.REST.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFileSystemContents(IcebergCatalogType.REST.getNamespacePath(), false))
        .containsExactlyInAnyOrderElementsOf(namespaceDirsBeforeAlter);
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TEST_TABLE, TEST_TABLE_NEW));
    assertThat(sparkCatalog().listTables(new String[] {TEST_DB}))
        .containsExactlyInAnyOrderElementsOf(List.of(TABLE_IDENTIFIER_NEW));
    assertThat(extractFileSystemContents(IcebergCatalogType.REST.getNamespacePath(), false))
        .containsExactlyInAnyOrderElementsOf(namespaceDirsBeforeAlter);
  }

  @Test
  public void testRenameMetadataLocationWithDropBaseDirectoryFeatures()
      throws IOException,
          TableAlreadyExistsException,
          NoSuchNamespaceException,
          NoSuchTableException {
    initSpark(IcebergCatalogType.HIVE, Map.of("rename.metadata.location.update", "true"));
    sparkCatalog().createTable(TABLE_IDENTIFIER, BASE_COLUMN_SCHEMA, new Transform[0], Map.of());
    insertData(CATALOG_TEST_TABLE, RECORDS.subList(0, 2));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TEST_TABLE, CATALOG_TEST_TABLE_NEW));
    insertData(CATALOG_TEST_TABLE_NEW, RECORDS.subList(2, 4));
    sparkCatalog()
        .createTable(
            TABLE_IDENTIFIER,
            BASE_COLUMN_SCHEMA,
            new Transform[0],
            Map.of("drop.base-directory.enabled", "true"));
    insertData(CATALOG_TEST_TABLE, ALT_RECORDS);
    assertThat(extractFileSystemContents(IcebergCatalogType.HIVE.getNamespacePath(), false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE),
                format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW)));
    assertThat(loadCatalogTableLocation(sparkCatalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW));
    assertThat(loadCatalogTableLocation(sparkCatalog().loadTable(TABLE_IDENTIFIER)))
        .isEqualTo(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertRecords(CATALOG_TEST_TABLE_NEW, MAIN_COLUMNS, RECORDS);
    assertRecords(CATALOG_TEST_TABLE, MAIN_COLUMNS, ALT_RECORDS);
    List<String> testTableFiles = extractTableFiles(TEST_TABLE);
    List<String> testTableNewFiles = extractTableFiles(TEST_TABLE_NEW);
    sparkCatalog().purgeTable(TABLE_IDENTIFIER);
    assertThat(sparkCatalog().listTables(new String[] {TEST_DB}))
        .containsExactlyInAnyOrderElementsOf(List.of(TABLE_IDENTIFIER_NEW));
    assertThat(extractFileSystemContents(IcebergCatalogType.HIVE.getNamespacePath(), false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE),
                format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW)));
    assertRecords(CATALOG_TEST_TABLE_NEW, MAIN_COLUMNS, RECORDS);
    List<String> namespaceFilesContents =
        extractFileSystemContents(IcebergCatalogType.HIVE.getNamespacePath(), true);
    assertThat(namespaceFilesContents).doesNotContainAnyElementsOf(testTableFiles);
    assertThat(namespaceFilesContents).containsAnyElementsOf(testTableNewFiles);
  }
}
