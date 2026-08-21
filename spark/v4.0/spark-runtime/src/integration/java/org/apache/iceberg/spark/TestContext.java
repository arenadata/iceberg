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
import static org.apache.iceberg.spark.service.IcebergTableClient.AWS_ACCESS_KEY;
import static org.apache.iceberg.spark.service.IcebergTableClient.AWS_REGION;
import static org.apache.iceberg.spark.service.IcebergTableClient.AWS_SECRET_KEY;
import static org.apache.iceberg.spark.service.IcebergTableClient.MINIO_PORT;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class TestContext {

  public static final String HIVE_METASTORE_PORT = "9083";
  public static final String WAREHOUSE_LOCATION = "s3a://warehouse";
  public static final String TEST_DB = "test";
  public static final String TEST_CATALOG = "test_catalog";

  private static final Map<String, String> BASE_CATALOG_CONFIGS =
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
          entry("client.factory", "org.apache.iceberg.aws.DefaultAwsClientFactory"));

  private static volatile TestContext instance;

  private final ComposeContainer container;

  public static synchronized TestContext instance() {
    if (instance == null) {
      instance = new TestContext();
    }
    return instance;
  }

  private TestContext() {
    container =
        new ComposeContainer(new File("./docker/docker-compose.yml"))
            .withStartupTimeout(Duration.ofMinutes(2))
            .withTailChildContainers(true)
            .waitingFor(
                "hive-metastore", Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1));
    container.start();
  }

  protected SparkSession initLocalSparkSession(
      IcebergCatalogType catalogType, Map<String, String> customConfigs) {
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
    fullCatalogConfigs(catalogType, customConfigs)
        .forEach(
            (key, val) ->
                builder.config(format("spark.sql.catalog.%s.%s", TEST_CATALOG, key), val));
    SparkSession sparkSession = builder.getOrCreate();
    checkHiveCatalogAvailability(sparkSession);
    return sparkSession;
  }

  private void checkHiveCatalogAvailability(SparkSession sparkSession) {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .ignoreExceptions()
        .until(
            () -> {
              ((SupportsNamespaces) provideCatalogPlugin(sparkSession)).listNamespaces();
              return true;
            });
  }

  protected TableCatalog provideCatalog(CatalogPlugin catalogPlugin) {
    return (TableCatalog) catalogPlugin;
  }

  protected CatalogPlugin provideCatalogPlugin(SparkSession sparkSession) {
    return sparkSession.sessionState().catalogManager().catalog(TEST_CATALOG);
  }

  protected static Map<String, String> fullCatalogConfigs(
      IcebergCatalogType catalogType, Map<String, String> customConfigs) {
    return Stream.concat(
            catalogType.getCatalogTypeBaseConfigs().entrySet().stream(),
            customConfigs.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected enum IcebergCatalogType {
    HIVE(
        Stream.concat(
                BASE_CATALOG_CONFIGS.entrySet().stream(),
                Map.ofEntries(
                    entry("type", "hive"),
                    entry(CatalogProperties.URI, "thrift://localhost:" + HIVE_METASTORE_PORT),
                    entry("hive.metastore.uris", "thrift://localhost:9083"),
                    entry("hive.metastore.schema.verification", "false"),
                    entry("hive.metastore.authorization.storage.checks", "false"),
                    entry("hive.metastore.client.capability.check", "false"),
                    entry("hive.metastore.skip.type.validation", "true"))
                    .entrySet()
                    .stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        format("%s/%s.db", WAREHOUSE_LOCATION, TEST_DB)),
    HADOOP(
        Stream.concat(
                BASE_CATALOG_CONFIGS.entrySet().stream(),
                Map.of("type", "hadoop").entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        format("%s/%s.db", WAREHOUSE_LOCATION, TEST_DB)),
    REST(
        Stream.concat(
                BASE_CATALOG_CONFIGS.entrySet().stream(),
                Map.of("type", "rest", "uri", "http://localhost:8181").entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        format("%s/%s", WAREHOUSE_LOCATION, TEST_DB));

    private final Map<String, String> catalogTypeBaseConfigs;

    private final String namespaceDir;

    private final Path namespacePath;

    IcebergCatalogType(Map<String, String> catalogTypeBaseConfigs, String namespaceDir) {
      this.catalogTypeBaseConfigs = catalogTypeBaseConfigs;
      this.namespaceDir = namespaceDir;
      this.namespacePath = new Path(namespaceDir);
    }

    public Map<String, String> getCatalogTypeBaseConfigs() {
      return this.catalogTypeBaseConfigs;
    }

    public String getNamespaceDir() {
      return this.namespaceDir;
    }

    public Path getNamespacePath() {
      return this.namespacePath;
    }
  }
}
