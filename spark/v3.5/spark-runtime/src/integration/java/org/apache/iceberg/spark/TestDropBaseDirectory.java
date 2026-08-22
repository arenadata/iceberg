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
import static java.util.Map.entry;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDropBaseDirectory {

  private TableCatalog catalog;
  private SparkSession spark;
  private FileSystem fs;

  private static final String WAREHOUSE_LOCATION = "s3a://warehouse";
  private static final int MINIO_PORT = 9000;
  private static final String AWS_ACCESS_KEY = "minioadmin";
  private static final String AWS_SECRET_KEY = "minioadmin";
  private static final String AWS_REGION = "us-east-1";
  private static final String HIVE_METASTORE_PORT = "9083";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_DB = "test_db";
  private static final String TEST_TABLE = "test_table";
  private static final String CATALOG_TABLE_NAME =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE);
  private static final String TABLE_NAMESPACE_DIR = format("%s/%s.db", WAREHOUSE_LOCATION, TEST_DB);
  private static final List<String> RECORDS =
      List.of("(1, 'Sam')", "(2, 'Bob')", "(3, 'Sue')", "(4, 'Ann')", "(1, 'Tom')", "(2, 'Brian')");

  private static final Map<String, String> BASE_CATALOG_CONFIGS =
      new HashMap<>(
          Map.ofEntries(
              entry("io.manifest.file-io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
              entry("s3.delete.enabled", "true"),
              entry("spark.hadoop.fs.s3.impl.disable.cache", "true"),
              entry("spark.hadoop.fs.s3a.impl.disable.cache", "true"),
              entry(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION),
              entry("hive.metastore.warehouse.dir", WAREHOUSE_LOCATION),
              entry("s3.endpoint", "http://localhost:" + MINIO_PORT),
              entry("s3.access-key-id", AWS_ACCESS_KEY),
              entry("s3.secret-access-key", AWS_SECRET_KEY),
              entry("s3.path-style-access", "true"),
              entry("s3.region", AWS_REGION),
              entry("cache-enabled", "false"),
              entry("s3.impl", "org.apache.iceberg.aws.s3.S3FileIO"),
              entry(
                  "hadoop.fs.s3a.aws.credentials.provider",
                  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
              entry("client.factory", "org.apache.iceberg.aws.DefaultAwsClientFactory")));

  private static final Map<String, String> HIVE_CONFIGS =
      new HashMap<>(
          Map.ofEntries(
              entry("type", "hive"),
              entry(CatalogProperties.URI, "thrift://localhost:" + HIVE_METASTORE_PORT),
              entry("hive.metastore.uris", "thrift://localhost:9083"),
              entry("hive.metastore.schema.verification", "false"),
              entry("hive.metastore.authorization.storage.checks", "false"),
              entry("hive.metastore.client.capability.check", "false"),
              entry("hive.metastore.skip.type.validation", "true")));

  @BeforeAll
  public static void baseBeforeAll() {
    ComposeContainer container =
        new ComposeContainer(new File("./docker/docker-compose.yml"))
            .withStartupTimeout(Duration.ofMinutes(2))
            .withTailChildContainers(true)
            .waitingFor(
                "hive-metastore", Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1));
    container.start();
  }

  @BeforeEach
  public void baseBefore() throws IOException, ParseException {
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
    Map<String, String> allConfigs = new HashMap<>(BASE_CATALOG_CONFIGS);
    allConfigs.putAll(HIVE_CONFIGS);
    for (Map.Entry<String, String> entry : allConfigs.entrySet()) {
      builder.config(
          String.format("spark.sql.catalog.%s.%s", TEST_CATALOG, entry.getKey()), entry.getValue());
    }
    spark = builder.getOrCreate();
    SupportsNamespaces nsCatalog =
        (SupportsNamespaces) Spark3Util.catalogAndIdentifier(spark, TEST_CATALOG).catalog();
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .ignoreExceptions()
        .until(
            () -> {
              nsCatalog.listNamespaces();
              return true;
            });
    catalog = (TableCatalog) Spark3Util.catalogAndIdentifier(spark, TEST_CATALOG).catalog();
    spark.sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    hadoopConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    fs = (new Path(WAREHOUSE_LOCATION)).getFileSystem(hadoopConfig);
  }

  @AfterEach
  public void baseAfter() throws IOException {
    catalog.dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE));
    Arrays.stream(fs.listStatus(new Path(WAREHOUSE_LOCATION)))
        .forEach(
            fileStatus -> {
              try {
                fs.delete(fileStatus.getPath(), true);
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

  @TestTemplate
  public void testDropBaseDirectoryEnabled(
      boolean isDropBaseDirectoryEnabled, List<String> namespaceContents)
      throws IOException,
          TableAlreadyExistsException,
          NoSuchNamespaceException,
          NoSuchTableException {
    Identifier tableIdentifier = Identifier.of(new String[] {TEST_DB}, TEST_TABLE);
    Column[] baseColumnSchema =
        new Column[] {
          Column.create("id", DataTypes.IntegerType, false),
          Column.create("username", DataTypes.StringType, true)
        };
    catalog.createTable(
        tableIdentifier,
        baseColumnSchema,
        new Transform[0],
        Map.of("drop.base-directory.enabled", String.valueOf(isDropBaseDirectoryEnabled)));
    spark.sql(
        format(
            "INSERT INTO %s VALUES %s",
            CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    assertThat(catalog.loadTable(tableIdentifier).properties().get("location"))
        .isEqualTo(format("%s/%s", TABLE_NAMESPACE_DIR, TEST_TABLE));
    catalog.purgeTable(tableIdentifier);
    AssertionsForInterfaceTypes.assertThat(extractFsContents())
        .containsExactlyInAnyOrderElementsOf(namespaceContents);
  }

  private List<String> extractFsContents() throws IOException {
    return Arrays.stream(fs.listStatus(new Path(TABLE_NAMESPACE_DIR)))
        .collect(Collectors.toList())
        .stream()
        .map(fileStatus -> fileStatus.getPath().toString())
        .collect(Collectors.toList());
  }

  @Parameters(name = "isDropBaseDirectoryEnabled={0}, namespaceContents={1}")
  private static List<Object[]> dropBaseDirectoryArgsProvider() {
    return Arrays.asList(
        new Object[] {true, List.of()},
        new Object[] {
          false, List.of(format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, TEST_TABLE))
        });
  }
}
