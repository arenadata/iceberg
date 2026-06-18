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
package org.apache.iceberg.connect.v3;

import static org.apache.iceberg.connect.utils.ConnectorUtils.AWS_ACCESS_KEY;
import static org.apache.iceberg.connect.utils.ConnectorUtils.AWS_REGION;
import static org.apache.iceberg.connect.utils.ConnectorUtils.AWS_SECRET_KEY;
import static org.apache.iceberg.connect.utils.ConnectorUtils.MINIO_PORT;
import static org.apache.iceberg.connect.utils.ConnectorUtils.V3_AUTO_CREATE_CONNECTOR_CONFIGS;
import static org.apache.iceberg.connect.utils.ConnectorUtils.addConnectorConfigs;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.S3_CLIENT;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.extractTableRecords;
import static org.apache.iceberg.connect.utils.IcebergTableUtils.loadCatalogTable;
import static org.apache.iceberg.connect.utils.KafkaBaseEventsUtils.KAFKA_BASE_EVENTS;
import static org.apache.iceberg.connect.utils.KafkaBaseEventsUtils.castKafkaBaseEventsToRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.KafkaConnectUtils;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;

public class TestEncryption extends IntegrationTestBaseV3 {
  private static final String HIVE_METASTORE_URI = "thrift://localhost:9083";
  private static final String HIVE_WAREHOUSE_LOCATION = "s3://bucket/warehouse";
  private static final String MINIO_CONNECTOR_ENDPOINT = "http://minio:" + MINIO_PORT;
  private static final String TEST_DATABASE = "test_db";
  private static final TableIdentifier HIVE_TABLE_IDENTIFIER =
      TableIdentifier.of(TEST_DATABASE, TEST_TABLE);
  private static final String BUCKET = "warehouse";

  @BeforeEach
  public void beforeEach() {
    S3_CLIENT.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  public void testEncryption(boolean useSchema) {
    runTest(
        useSchema,
        addConnectorConfigs(
            V3_AUTO_CREATE_CONNECTOR_CONFIGS, hiveCatalogConnectorConfigs(testTopic())),
        List.of(HIVE_TABLE_IDENTIFIER),
        KAFKA_BASE_EVENTS);

    Table table = loadCatalogTable(catalog(), HIVE_TABLE_IDENTIFIER);
    Schema schema =
        useSchema
            ? extractRecords(table).stream().findAny().get().struct().asSchema()
            : table.schema();
    assertThat(schema.columns().stream().map(Types.NestedField::name))
        .containsExactlyInAnyOrderElementsOf(List.of("id", "username"));
    List<org.apache.iceberg.data.Record> tableRecords =
        useSchema ? extractRecords(table) : extractTableRecords(table);
    assertThat(tableRecords)
        .containsExactlyInAnyOrderElementsOf(castKafkaBaseEventsToRecords(schema));
  }

  @Test
  public void testEncryptionNegative() throws Exception {
    runTest(
        true,
        hiveCatalogConnectorConfigs(testTopic()),
        List.of(HIVE_TABLE_IDENTIFIER),
        KAFKA_BASE_EVENTS);
    Catalog catalogWithoutEncryptionConfig = new HiveCatalog();
    catalogWithoutEncryptionConfig.initialize("local_hive", hiveCatalogConfigs());

    Table table = loadCatalogTable(catalogWithoutEncryptionConfig, HIVE_TABLE_IDENTIFIER);
    assertThatThrownBy(() -> extractTableRecords(table))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Cant create encryption manager, because key management client is not set");
    ((AutoCloseable) catalogWithoutEncryptionConfig).close();
  }

  @Override
  protected Catalog initCatalog() {
    Catalog catalog = new HiveCatalog();
    catalog.initialize("local_hive", hiveCatalogConfigsWithEncryption());
    return catalog;
  }

  @Override
  protected void createNamespace() {
    ((SupportsNamespaces) catalog()).createNamespace(Namespace.of(TEST_DATABASE));
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", HIVE_TABLE_IDENTIFIER.toString())
        .config("iceberg.tables.evolve-schema-enabled", "true")
        .config("tasks.max", "1");
  }

  private List<org.apache.iceberg.data.Record> extractRecords(Table table) {
    return extractTableRecords(table).stream()
        .map(record -> (org.apache.iceberg.data.Record) record.get(1))
        .toList();
  }

  @Override
  protected void clearNamespace() {
    ((SupportsNamespaces) catalog()).dropNamespace(Namespace.of(TEST_DATABASE));
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(HIVE_TABLE_IDENTIFIER);
    S3_CLIENT.deleteBucket((DeleteBucketRequest.builder().bucket(BUCKET).build()));
  }

  private static Map<String, Object> hiveCatalogConnectorConfigs(String topic) {
    return Map.ofEntries(
        Map.entry("connector.class", "org.apache.iceberg.connect.IcebergSinkConnector"),
        Map.entry("tasks.max", "1"),
        Map.entry("topics", topic),
        Map.entry("iceberg.catalog.type", "hive"),
        Map.entry("iceberg.catalog.uri", "thrift://hive-metastore:9083"),
        Map.entry("iceberg.catalog.warehouse", HIVE_WAREHOUSE_LOCATION),
        Map.entry("iceberg.tables.auto-create-props.write.object-storage.enabled", "true"),
        Map.entry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        Map.entry("s3.endpoint", MINIO_CONNECTOR_ENDPOINT),
        Map.entry("s3.access-key-id", AWS_ACCESS_KEY),
        Map.entry("s3.secret-access-key", AWS_SECRET_KEY),
        Map.entry("s3.path-style-access", "true"),
        Map.entry("s3.region", AWS_REGION),
        Map.entry("iceberg.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
        Map.entry("iceberg.hadoop.fs.s3a.endpoint", MINIO_CONNECTOR_ENDPOINT),
        Map.entry("iceberg.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY),
        Map.entry("iceberg.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY),
        Map.entry("iceberg.hadoop.fs.s3a.path.style.access", "true"),
        Map.entry("iceberg.hadoop.fs.defaultFS", "s3://bucket/"),
        Map.entry(
            "iceberg.catalog.encryption.kms-impl", "com.example.iceberg.kms.LocalAesKmsClient"),
        Map.entry(
            "iceberg.catalog.encryption.kms.master-key",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        Map.entry("iceberg.tables.auto-create-props.encryption.key-id", "test-master-key-1"),
        Map.entry("value.converter", "org.apache.kafka.connect.json.JsonConverter"),
        Map.entry("value.converter.schemas.enable", "false"),
        Map.entry("consumer.override.auto.offset.reset", "earliest"),
        Map.entry("iceberg.control.commit.interval-ms", "30000"));
  }

  private static Map<String, String> hiveCatalogConfigs() {
    return Map.of(
        CatalogProperties.URI,
        HIVE_METASTORE_URI,
        CatalogProperties.FILE_IO_IMPL,
        "org.apache.iceberg.aws.s3.S3FileIO",
        CatalogProperties.WAREHOUSE_LOCATION,
        HIVE_WAREHOUSE_LOCATION,
        "hive.metastore.warehouse.dir",
        HIVE_WAREHOUSE_LOCATION,
        "s3.endpoint",
        "http://localhost:" + MINIO_PORT,
        "s3.access-key-id",
        AWS_ACCESS_KEY,
        "s3.secret-access-key",
        AWS_SECRET_KEY,
        "s3.path-style-access",
        "true",
        "client.region",
        AWS_REGION);
  }

  private static Map<String, String> hiveCatalogConfigsWithEncryption() {
    return Stream.of(
            hiveCatalogConfigs(),
            Map.of(
                "encryption.kms-impl",
                "org.apache.iceberg.connect.utils.encryption.LocalAesKmsClient"))
        .flatMap(m -> m.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2));
  }
}
