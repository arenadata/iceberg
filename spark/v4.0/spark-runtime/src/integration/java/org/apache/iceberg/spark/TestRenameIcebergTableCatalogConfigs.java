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
import static org.apache.iceberg.spark.TestContext.WAREHOUSE_LOCATION;
import static org.apache.iceberg.spark.service.IcebergTableClient.loadCatalogTableLocation;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRenameIcebergTableCatalogConfigs extends AbstractTestBase {

  private static final String TEST_TABLE = "test_table";

  private static final String TEST_TABLE_NEW = "test_table_new";

  private static final Identifier TABLE_IDENTIFIER =
      Identifier.of(new String[] {TEST_DB}, TEST_TABLE);

  private static final Identifier TABLE_IDENTIFIER_NEW =
      Identifier.of(new String[] {TEST_DB}, TEST_TABLE_NEW);

  private static final String CATALOG_TABLE_NAME =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE);

  private static final String CATALOG_TABLE_NEW_NAME =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE_NEW);

  private static final Column[] BASE_TABLE_SCHEMA =
      new Column[] {
        Column.create("id", DataTypes.IntegerType, false),
        Column.create("username", DataTypes.StringType, true)
      };

  private static final List<String> RECORDS =
      List.of("(1, 'Sam')", "(2, 'Bob')", "(3, 'Sue')", "(4, 'Ann')", "(1, 'Tom')", "(2, 'Brian')");

  @ParameterizedTest
  @MethodSource("renameMetadataLocationUpdateArgsProvider")
  public void testRenameMetadataLocationUpdate(
      boolean isRenameMetadataLocationUpdateEnabled,
      String tableDir,
      List<String> namespaceContents)
      throws TableAlreadyExistsException,
          NoSuchNamespaceException,
          NoSuchTableException,
          IOException {
    initSpark(
        TestContext.IcebergCatalogType.HIVE,
        Map.of(
            "rename.metadata.location.update",
            String.valueOf(isRenameMetadataLocationUpdateEnabled)));
    catalog().createTable(TABLE_IDENTIFIER, BASE_TABLE_SCHEMA, new Transform[0], Map.of());
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                format(
                    "%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE)));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, CATALOG_TABLE_NEW_NAME));
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NEW_NAME, String.join(", ", RECORDS.subList(2, 4))));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), tableDir));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(namespaceContents);
  }

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

  @ParameterizedTest
  @MethodSource("renameMetadataLocationUpdateNonDefaultLocationProvider")
  public void testRenameMetadataLocationUpdateNonDefaultLocationNegative(
      Map<String, String> locationConfig)
      throws TableAlreadyExistsException,
          NoSuchNamespaceException,
          IOException,
          NoSuchTableException {
    initSpark(
        TestContext.IcebergCatalogType.HIVE, Map.of("rename.metadata.location.update", "true"));
    spark().sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    catalog().createTable(TABLE_IDENTIFIER, BASE_TABLE_SCHEMA, new Transform[0], locationConfig);
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, TEST_TABLE_NEW));
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NEW_NAME, String.join(", ", RECORDS.subList(2, 4))));
    assertThat(extractTableRecords(CATALOG_TABLE_NEW_NAME))
        .containsExactlyInAnyOrderElementsOf(RECORDS.subList(0, 4));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .doesNotContain(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW));
  }

  @ParameterizedTest
  @MethodSource("dropBaseDirectoryArgsProvider")
  public void testDropBaseDirectoryEnabled(
      boolean isDropBaseDirectoryEnabled, List<String> namespaceContents)
      throws IOException,
          TableAlreadyExistsException,
          NoSuchNamespaceException,
          NoSuchTableException {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            BASE_TABLE_SCHEMA,
            new Transform[0],
            Map.of("drop.base-directory.enabled", String.valueOf(isDropBaseDirectoryEnabled)));
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                format(
                    "%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE)));
    catalog().purgeTable(TABLE_IDENTIFIER);
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .containsExactlyInAnyOrderElementsOf(namespaceContents);
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

  private List<String> extractTableFiles(String tableName) throws NoSuchTableException {
    List<String> tableDataFiles =
        spark()
            .sql(format("SELECT path FROM %s.%s.%s.manifests", TEST_CATALOG, TEST_DB, tableName))
            .collectAsList()
            .stream()
            .map(Row::mkString)
            .collect(Collectors.toList());
    tableDataFiles.addAll(
        spark()
            .sql(format("SELECT file_path FROM %s.%s.%s.files", TEST_CATALOG, TEST_DB, tableName))
            .collectAsList()
            .stream()
            .map(Row::mkString)
            .collect(Collectors.toList()));
    tableDataFiles.addAll(
        ReachableFileUtil.metadataFileLocations(
            ((SparkTable) catalog().loadTable(Identifier.of(new String[] {TEST_DB}, tableName)))
                .table(),
            true));
    return tableDataFiles;
  }

  private List<String> extractFsContents(
      TestContext.IcebergCatalogType catalogType, boolean isRecursive) throws IOException {
    return isRecursive
        ? RemoteIterators.toList((fs().listFiles(catalogType.getNamespacePath(), true))).stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList())
        : Arrays.stream(fs().listStatus(catalogType.getNamespacePath()))
            .collect(Collectors.toList())
            .stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList());
  }

  private List<String> extractTableRecords(String catalogTableName) {
    return spark().sql(format("SELECT * FROM %s", catalogTableName)).collectAsList().stream()
        .map(row -> format("(%s, '%s')", row.get(0), row.get(1)))
        .collect(Collectors.toList());
  }

  private static Stream<Arguments> renameMetadataLocationUpdateArgsProvider() {
    return Stream.of(
        Arguments.of(
            true,
            TEST_TABLE_NEW,
            List.of(
                format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE),
                format(
                    "%s/%s",
                    TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW))),
        Arguments.of(
            false,
            TEST_TABLE,
            List.of(
                format(
                    "%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE))));
  }

  private static Stream<Arguments> dropBaseDirectoryArgsProvider() {
    return Stream.of(
        Arguments.of(true, List.of()),
        Arguments.of(
            false,
            List.of(
                format(
                    "%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE))));
  }

  private static Stream<Arguments> renameMetadataLocationUpdateNonDefaultLocationProvider() {
    return Stream.of(
        Arguments.of(
            Map.of(
                "write.data.path",
                format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")),
            format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")),
        Arguments.of(
            Map.of(
                "write.metadata.path",
                format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "metadata-storage")),
            format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "metadata-storage")),
        Arguments.of(
            Map.of(
                "write.object-storage.enabled",
                "true",
                "write.data.path",
                format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")),
            format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")));
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
    catalog().dropTable(TABLE_IDENTIFIER_NEW);
  }
}
