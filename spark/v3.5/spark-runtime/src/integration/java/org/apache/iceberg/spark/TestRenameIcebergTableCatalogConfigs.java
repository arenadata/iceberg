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
import static org.apache.iceberg.spark.TestContext.TEST_CATALOG;
import static org.apache.iceberg.spark.TestContext.TEST_DB;
import static org.apache.iceberg.spark.service.IcebergTableClient.loadCatalogTableLocation;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRenameIcebergTableCatalogConfigs extends RenameIcebergTableCatalogTestBase {

  @Test
  public void testRenameMetadataLocationUpdateRestNegative()
      throws IOException, TableAlreadyExistsException, NoSuchNamespaceException {
    initSpark(
        TestContext.IcebergCatalogType.REST, Map.of("rename.metadata.location.update", "true"));
    spark().sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    catalog().createTable(TABLE_IDENTIFIER, BASE_TABLE_SCHEMA, new Transform[0], Map.of());
    List<String> namespaceDirsBeforeAlter =
        List.of(format("%s/%s", TestContext.IcebergCatalogType.REST.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.REST, false))
        .containsExactlyInAnyOrderElementsOf(namespaceDirsBeforeAlter);
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, TEST_TABLE_NEW));
    assertThat(catalog().listTables(new String[] {TEST_DB}))
        .containsExactlyInAnyOrderElementsOf(List.of(TABLE_IDENTIFIER_NEW));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.REST, false))
        .containsExactlyInAnyOrderElementsOf(namespaceDirsBeforeAlter);
  }

  @Test
  public void testRenameMetadataLocationUpdateHadoopNegative()
      throws TableAlreadyExistsException, NoSuchNamespaceException, IOException {
    initSpark(
        TestContext.IcebergCatalogType.HADOOP, Map.of("rename.metadata.location.update", "true"));
    catalog().createTable(TABLE_IDENTIFIER, BASE_TABLE_SCHEMA, new Transform[0], Map.of());
    assertThatThrownBy(
        () ->
            spark()
                .sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, TEST_TABLE_NEW)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot rename Hadoop tables");
  }

  @Test
  public void testRenameMetadataLocationWithDropBaseDirectoryFeatures()
      throws IOException,
      TableAlreadyExistsException,
      NoSuchNamespaceException,
      NoSuchTableException {
    initSpark(
        TestContext.IcebergCatalogType.HIVE, Map.of("rename.metadata.location.update", "true"));
    catalog().createTable(TABLE_IDENTIFIER, BASE_TABLE_SCHEMA, new Transform[0], Map.of());
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, CATALOG_TABLE_NEW_NAME));
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NEW_NAME, String.join(", ", RECORDS.subList(2, 4))));
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            BASE_TABLE_SCHEMA,
            new Transform[0],
            Map.of("drop.base-directory.enabled", "true"));
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(4, 6))));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE),
                format(
                    "%s/%s",
                    TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW)));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertThat(extractTableRecords(CATALOG_TABLE_NEW_NAME))
        .containsExactlyInAnyOrderElementsOf(RECORDS.subList(0, 4));
    assertThat(extractTableRecords(CATALOG_TABLE_NAME))
        .containsExactlyInAnyOrderElementsOf(RECORDS.subList(4, 6));
    List<String> testTableFiles = extractTableFiles(TEST_TABLE);
    List<String> testTableNewFiles = extractTableFiles(TEST_TABLE_NEW);
    catalog().purgeTable(TABLE_IDENTIFIER);
    assertThat(catalog().listTables(new String[] {TEST_DB}))
        .containsExactlyInAnyOrderElementsOf(List.of(TABLE_IDENTIFIER_NEW));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE),
                format(
                    "%s/%s",
                    TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW)));
    assertThat(extractTableRecords(CATALOG_TABLE_NEW_NAME))
        .containsExactlyInAnyOrderElementsOf(RECORDS.subList(0, 4));
    List<String> namespaceFilesContents =
        extractFsContents(TestContext.IcebergCatalogType.HIVE, true);
    assertThat(namespaceFilesContents).doesNotContainAnyElementsOf(testTableFiles);
    assertThat(namespaceFilesContents).containsAnyElementsOf(testTableNewFiles);
  }
}
