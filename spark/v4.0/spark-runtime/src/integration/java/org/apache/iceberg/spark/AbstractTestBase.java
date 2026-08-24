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
import static org.apache.iceberg.spark.TestContext.AWS_ACCESS_KEY;
import static org.apache.iceberg.spark.TestContext.AWS_REGION;
import static org.apache.iceberg.spark.TestContext.AWS_SECRET_KEY;
import static org.apache.iceberg.spark.TestContext.BASE_CATALOG_CONFIGS;
import static org.apache.iceberg.spark.TestContext.MINIO_PORT;
import static org.apache.iceberg.spark.TestContext.TEST_CATALOG;
import static org.apache.iceberg.spark.TestContext.TEST_DB;
import static org.apache.iceberg.spark.TestContext.TEST_TABLE;
import static org.apache.iceberg.spark.TestContext.TEST_TABLE_NEW;
import static org.apache.iceberg.spark.TestContext.WAREHOUSE_LOCATION;
import static org.awaitility.Awaitility.await;

import java.io.File;
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
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class AbstractTestBase {
  private static ComposeContainer container;
  private TableCatalog catalog;
  private SparkSession spark;
  private FileSystem fileSystem;

  @BeforeAll
  public static void baseBeforeAll() {
    getContainer();
  }

  private static synchronized ComposeContainer getContainer() {
    if (container == null) {
      container =
          new ComposeContainer(new File("./docker/docker-compose.yml"))
              .withStartupTimeout(Duration.ofMinutes(2))
              .withTailChildContainers(true)
              .waitingFor(
                  "hive-metastore", Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1));
      container.start();
    }
    return container;
  }

  @BeforeEach
  public void baseBefore() throws IOException {
    initSpark(IcebergCatalogType.HIVE, Map.of());
  }

  @AfterEach
  public void baseAfter() throws IOException {
    catalog.dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE));
    catalog.dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE_NEW));
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

    try {
      if (catalog instanceof AutoCloseable) {
        ((AutoCloseable) catalog).close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:" + MINIO_PORT)
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
            .config("spark.hadoop.fs.s3a.path.style.access", "true");
    Stream.concat(
            Stream.concat(
                BASE_CATALOG_CONFIGS.entrySet().stream(),
                catalogType.getCatalogTypeBaseConfigs().entrySet().stream()),
            customConfigs.entrySet().stream())
        .forEach(
            (entry) ->
                builder.config(
                    format("spark.sql.catalog.%s.%s", TEST_CATALOG, entry.getKey()),
                    entry.getValue()));
    spark = builder.getOrCreate();
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .ignoreExceptions()
        .until(
            () -> {
              ((SupportsNamespaces) spark.sessionState().catalogManager().catalog(TEST_CATALOG))
                  .listNamespaces();
              return true;
            });
    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(TEST_CATALOG);
    spark.sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    fileSystem = (new Path(WAREHOUSE_LOCATION)).getFileSystem(spark.sessionState().newHadoopConf());
  }

  public SparkSession spark() {
    return spark;
  }

  public FileSystem fileSystem() {
    return fileSystem;
  }

  public TableCatalog catalog() {
    return catalog;
  }

  public List<String> extractFileSystemContents(IcebergCatalogType catalogType, boolean isRecursive)
      throws IOException {
    return isRecursive
        ? RemoteIterators.toList((fileSystem().listFiles(catalogType.getNamespacePath(), true)))
            .stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList())
        : Arrays.stream(fileSystem().listStatus(catalogType.getNamespacePath()))
            .collect(Collectors.toList())
            .stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList());
  }

  public List<String> extractTableRecords(String catalogTableName) {
    return spark().sql(format("SELECT * FROM %s", catalogTableName)).collectAsList().stream()
        .map(row -> format("(%s, '%s')", row.get(0), row.get(1)))
        .collect(Collectors.toList());
  }

  public String loadCatalogTableLocation(Table table) {
    return table.properties().get("location");
  }

  protected List<String> extractTableFiles(String tableName) throws NoSuchTableException {
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
}
