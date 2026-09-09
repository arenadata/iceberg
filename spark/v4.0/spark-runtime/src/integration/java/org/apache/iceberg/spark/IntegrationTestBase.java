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
import static org.apache.iceberg.spark.IcebergCatalogProperties.AWS_ACCESS_KEY;
import static org.apache.iceberg.spark.IcebergCatalogProperties.AWS_REGION;
import static org.apache.iceberg.spark.IcebergCatalogProperties.AWS_SECRET_KEY;
import static org.apache.iceberg.spark.IcebergCatalogProperties.BASE_CATALOG_CONFIGS;
import static org.apache.iceberg.spark.IcebergCatalogProperties.MINIO_PORT;
import static org.apache.iceberg.spark.IcebergCatalogProperties.SPARK_CATALOG;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_CATALOG;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_DB;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE_NEW;
import static org.apache.iceberg.spark.IcebergCatalogProperties.WAREHOUSE_LOCATION;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class IntegrationTestBase {
  public static final List<String> MAIN_COLUMNS = List.of("id", "username");
  private TableCatalog sparkCatalog;
  private TableCatalog sparkSessionCatalog;
  private SparkSession spark;
  private FileSystem fileSystem;

  @BeforeAll
  public static void baseBeforeAll() {
    TestContext.instance();
  }

  @BeforeEach
  public void baseBefore() throws IOException {
    initSpark(IcebergCatalogType.HIVE, Map.of());
  }

  @AfterEach
  public void baseAfter() throws IOException {
    clearTables();
    Arrays.stream(fileSystem.listStatus(new Path(WAREHOUSE_LOCATION)))
        .forEach(
            fileStatus -> {
              try {
                fileSystem.delete(fileStatus.getPath(), true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    spark.sql(format("DROP NAMESPACE IF EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    spark.sql(format("DROP NAMESPACE IF EXISTS %s.%s", SPARK_CATALOG, TEST_DB));
    try {
      if (sparkCatalog instanceof AutoCloseable) {
        ((AutoCloseable) sparkCatalog).close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void clearTables() {
    sparkCatalog.dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE));
    sparkCatalog.dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE_NEW));
  }

  public void initSpark(IcebergCatalogType catalogType, Map<String, String> customConfigs)
      throws IOException {
    if (this.spark != null) {
      this.spark.close();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[*]")
            .config("spark.driver.host", "localhost")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(
                format("spark.sql.catalog.%s", TEST_CATALOG),
                "org.apache.iceberg.spark.SparkCatalog")
            .config(
                format("spark.sql.catalog.%s", SPARK_CATALOG),
                "org.apache.iceberg.spark.SparkSessionCatalog")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + MINIO_PORT)
            .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION)
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
            .config("spark.hadoop.fs.s3a.path.style.access", "true");
    List.of(SPARK_CATALOG, TEST_CATALOG)
        .forEach(
            configCatalog ->
                Stream.concat(
                        Stream.concat(
                            BASE_CATALOG_CONFIGS.entrySet().stream(),
                            catalogType.getCatalogTypeBaseConfigs().entrySet().stream()),
                        customConfigs.entrySet().stream())
                    .forEach(
                        (entry) ->
                            builder.config(
                                format("spark.sql.catalog.%s.%s", configCatalog, entry.getKey()),
                                entry.getValue())));
    spark = builder.getOrCreate();
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .ignoreExceptions()
        .until(
            () -> {
              ((SupportsNamespaces) spark.sessionState().catalogManager().catalog(SPARK_CATALOG))
                  .listNamespaces();
              return true;
            });
    sparkCatalog = (TableCatalog) spark.sessionState().catalogManager().catalog(TEST_CATALOG);
    sparkSessionCatalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(SPARK_CATALOG);
    spark.sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", SPARK_CATALOG, TEST_DB));
    spark.sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    fileSystem = (new Path(WAREHOUSE_LOCATION)).getFileSystem(spark.sessionState().newHadoopConf());
  }

  public SparkSession spark() {
    return spark;
  }

  public FileSystem fileSystem() {
    return fileSystem;
  }

  public TableCatalog sparkCatalog() {
    return sparkCatalog;
  }

  public TableCatalog sparkSessionCatalog() {
    return sparkSessionCatalog;
  }

  public List<String> extractFileSystemContents(Path path, boolean isRecursive) throws IOException {
    return isRecursive
        ? RemoteIterators.toList((fileSystem().listFiles(path, true))).stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList())
        : Arrays.stream(fileSystem().listStatus(path)).collect(Collectors.toList()).stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList());
  }

  public List<String> extractTableRecords(String catalogTableName, List<String> columns) {
    return spark()
        .sql(format("SELECT %s FROM %s", String.join(", ", columns), catalogTableName))
        .collectAsList()
        .stream()
        .map(row -> row.mkString(", "))
        .collect(Collectors.toList());
  }

  public String loadCatalogTableLocation(Table table) {
    return table.properties().get("location");
  }

  public List<String> extractTableFiles(String tableName) throws NoSuchTableException {
    List<String> tableDataFiles =
        spark()
            .sql(format("SELECT path FROM %s.%s.%s.manifests", TEST_CATALOG, TEST_DB, tableName))
            .collectAsList()
            .stream()
            .map(Row::mkString)
            .collect(Collectors.toList());
    tableDataFiles.addAll(extractTableDataFiles(tableName));
    tableDataFiles.addAll(
        ReachableFileUtil.metadataFileLocations(
            ((SparkTable)
                    sparkCatalog().loadTable(Identifier.of(new String[] {TEST_DB}, tableName)))
                .table(),
            true));
    return tableDataFiles;
  }

  public List<String> extractTableDataFiles(String tableName) {
    return spark()
        .sql(format("SELECT file_path FROM %s.%s.%s.files", TEST_CATALOG, TEST_DB, tableName))
        .collectAsList()
        .stream()
        .map(Row::mkString)
        .collect(Collectors.toList());
  }

  public void insertData(String catalogTableName, List<String> records) {
    spark.sql(
        format(
            "INSERT INTO %s VALUES %s",
            catalogTableName,
            records.stream().map(rec -> "(" + rec + ")").collect(Collectors.joining(", "))));
  }

  public void assertRecords(
      String catalogTableName, List<String> columns, List<String> insertedRecords) {
    assertThat(extractTableRecords(catalogTableName, columns))
        .containsExactlyInAnyOrderElementsOf(
            insertedRecords.stream().map(rec -> rec.replace("'", "")).collect(Collectors.toList()));
  }
}
