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
package org.apache.iceberg.connect.openmetadata;

import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.AbstractTestBase;
import org.apache.iceberg.connect.BaseTestEvent;
import org.apache.iceberg.connect.KafkaConnectUtils;
import org.apache.iceberg.connect.TableUserEvent;
import org.apache.iceberg.connect.service.OpenMetadataIcebergKafkaDeployService;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.openmetadata.client.model.Edge;
import org.openmetadata.client.model.Field;

public class TestOpenMetadata extends AbstractTestBase {

  private static final int OPENMETADATA_PORT = 8585;
  private static final String TEST_TABLE_1 = "test_table_1";
  private static final String TEST_TABLE_2 = "test_table_2";
  private static final TableIdentifier TABLE_IDENTIFIER_1 =
      TableIdentifier.of(TEST_DB, TEST_TABLE_1);
  private static final TableIdentifier TABLE_IDENTIFIER_2 =
      TableIdentifier.of(TEST_DB, TEST_TABLE_2);
  private static final Schema TABLE_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "username", Types.StringType.get()),
              Types.NestedField.required(3, "table", Types.StringType.get())),
          ImmutableSet.of(1));
  private static final List<Field> TOPIC_SCHEMA =
      List.of(
          new Field().name("id").dataType(Field.DataTypeEnum.LONG),
          new Field().name("username").dataType(Field.DataTypeEnum.STRING),
          new Field().name("table").dataType(Field.DataTypeEnum.STRING));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(TABLE_SCHEMA).build();
  private static final List<TableUserEvent> TABLE_EVENTS_PACK_1 =
      List.of(new TableUserEvent(1, "Sam"), new TableUserEvent(2, "Ann"));
  private static final List<TableUserEvent> TABLE_EVENTS_PACK_2 =
      List.of(new TableUserEvent(3, "Tom"), new TableUserEvent(4, "Mary"));
  private static final String ICEBERG_SERVICE_BASE_NAME = "my_iceberg";
  private static final String KAFKA_SERVICE_BASE_NAME = "my_kafka";
  private static final String KAFKA_CONNECT_SERVICE_BASE_NAME = "my_kafka_connect";
  private static final String ICEBERG_DATABASE_NAME = "default";
  private static final String OPENMETADATA_ADDRESS =
      format("http://openmetadata:%s", OPENMETADATA_PORT);
  private OpenMetadataIcebergKafkaDeployService openMetadataService;
  private String suffix;
  private String icebergServiceName;
  private UUID icebergServiceId;
  private UUID icebergIngestionPipelineId;
  private String kafkaServiceName;
  private UUID kafkaServiceId;
  private UUID kafkaIngestionPipelineId;
  private String kafkaConnectServiceName;
  private UUID kafkaConnectServiceId;
  private String kafkaConnectPipelineName;
  private String spareTopic;

  @BeforeEach
  public void beforeEach() throws IOException, URISyntaxException {
    catalog().createTable(TABLE_IDENTIFIER_1, TABLE_SCHEMA, SPEC, Map.of());
    catalog().createTable(TABLE_IDENTIFIER_2, TABLE_SCHEMA, SPEC, Map.of());
    spareTopic = "test-topic-" + UUID.randomUUID();
    createTopic(spareTopic, TEST_TOPIC_PARTITIONS);
    openMetadataService = new OpenMetadataIcebergKafkaDeployService();
    Pair<UUID, UUID> icebergServiceData =
        openMetadataService.deployIcebergService(
            icebergServiceName,
            ICEBERG_DATABASE_NAME,
            TEST_DB,
            List.of(TEST_TABLE_1, TEST_TABLE_2));
    icebergServiceId = icebergServiceData.getLeft();
    icebergIngestionPipelineId = icebergServiceData.getRight();
    Pair<UUID, UUID> kafkaServiceData =
        openMetadataService.deployKafkaService(kafkaServiceName, List.of(testTopic(), spareTopic));
    kafkaServiceId = kafkaServiceData.getLeft();
    kafkaIngestionPipelineId = kafkaServiceData.getRight();
    openMetadataService.updateTopicSchema(kafkaServiceName, testTopic(), TOPIC_SCHEMA);
    openMetadataService.updateTopicSchema(kafkaServiceName, spareTopic, TOPIC_SCHEMA);
    kafkaConnectServiceId =
        openMetadataService.deployKafkaConnectService(
            kafkaConnectServiceName, kafkaServiceName, kafkaConnectPipelineName);
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  public void testOpenMetadataOneToOne(boolean useSchema) {
    runTest(
        useSchema,
        createConfig(useSchema),
        Map.of(),
        List.of(TABLE_IDENTIFIER_1),
        Map.of(testTopic(), getEvents(TABLE_EVENTS_PACK_1, TABLE_IDENTIFIER_1)));
    String icebergFullTestTable1Name = getIcebergTableFullName(TABLE_IDENTIFIER_1);
    List<Edge> icebergTableLineageUpstreamEdges =
        openMetadataService.extractNonEmptyUpstreamEdges("table", icebergFullTestTable1Name, 1);
    assertIcebergKafkaEdge(icebergTableLineageUpstreamEdges.get(0));
    String fullKafkaTopicName = getKafkaTopicFullName(kafkaServiceName, testTopic());
    List<Edge> kafkaTopicLineageDownstreamEdges =
        openMetadataService.extractNonEmptyDownstreamEdges("topic", fullKafkaTopicName, 1);
    assertIcebergKafkaEdge(kafkaTopicLineageDownstreamEdges.get(0));
    assertMessageService();
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  public void testOpenMetadataOneToMany(boolean useSchema) {
    runTest(
        useSchema,
        createConfig(useSchema),
        Map.of(),
        List.of(TABLE_IDENTIFIER_1),
        Map.of(
            testTopic(),
            Stream.concat(
                    getEvents(TABLE_EVENTS_PACK_1, TABLE_IDENTIFIER_1).stream(),
                    getEvents(TABLE_EVENTS_PACK_2, TABLE_IDENTIFIER_2).stream())
                .toList()));
    List<String> icebergFullTablesNames =
        List.of(
            getIcebergTableFullName(TABLE_IDENTIFIER_1),
            getIcebergTableFullName(TABLE_IDENTIFIER_2));
    String fullKafkaTopicName = format("%s.%s", kafkaServiceName, testTopic());
    icebergFullTablesNames.forEach(
        icebergFullTableName -> {
          List<Edge> icebergTableLineageUpstreamEdges =
              openMetadataService.extractNonEmptyUpstreamEdges("table", icebergFullTableName, 1);
          assertIcebergKafkaEdge(icebergTableLineageUpstreamEdges.get(0));
        });

    List<Edge> kafkaTopicLineageDownstreamEdges =
        openMetadataService.extractNonEmptyDownstreamEdges("topic", fullKafkaTopicName, 2);
    kafkaTopicLineageDownstreamEdges.forEach(this::assertIcebergKafkaEdge);
    assertMessageService();
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  public void testOpenMetadataManyToOne(boolean useSchema) {
    runTest(
        useSchema,
        createCustomConfig(useSchema, format("%s,%s", testTopic(), spareTopic)),
        Map.of(),
        List.of(TABLE_IDENTIFIER_1),
        Map.of(
            testTopic(),
            getEvents(TABLE_EVENTS_PACK_1, TABLE_IDENTIFIER_1),
            spareTopic,
            getEvents(TABLE_EVENTS_PACK_2, TABLE_IDENTIFIER_1)));
    String icebergFullTableName = getIcebergTableFullName(TABLE_IDENTIFIER_1);

    List<Edge> icebergTableLineageUpstreamEdges =
        openMetadataService.extractNonEmptyUpstreamEdges("table", icebergFullTableName, 2);
    icebergTableLineageUpstreamEdges.forEach(
        icebergTableLineageUpstreamEdge -> assertIcebergKafkaEdge(icebergTableLineageUpstreamEdge));

    List<String> kafkaTopicsList =
        List.of(
            getKafkaTopicFullName(kafkaServiceName, testTopic()),
            getKafkaTopicFullName(kafkaServiceName, spareTopic));
    kafkaTopicsList.forEach(
        fullKafkaTopicName -> {
          List<Edge> kafkaTopicLineageDownstreamEdges =
              openMetadataService.extractNonEmptyDownstreamEdges("topic", fullKafkaTopicName, 1);
          Edge kafkaTopicLineageDownstreamEdge = kafkaTopicLineageDownstreamEdges.get(0);
          assertIcebergKafkaEdge(kafkaTopicLineageDownstreamEdge);
        });
    assertMessageService();
  }

  protected void runTest(
      boolean useSchema,
      KafkaConnectUtils.Config connectorConfig,
      Map<String, Object> extraConfig,
      List<TableIdentifier> tableIdentifiers,
      Map<String, List<? extends BaseTestEvent>> topicEvents) {
    initConnectorConfig(connectorConfig, extraConfig);
    topicEvents.forEach((topic, events) -> events.forEach(event -> send(topic, event, useSchema)));

    context().startConnector(connectorConfig);

    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(tableIdentifiers));
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createOpenMetadataConfig(createCommonConfig(useSchema));
  }

  private KafkaConnectUtils.Config createCustomConfig(boolean useSchema, String topicName) {
    return createOpenMetadataConfig(createKafkaConnectConfig(useSchema, topicName));
  }

  private KafkaConnectUtils.Config createOpenMetadataConfig(KafkaConnectUtils.Config config) {
    return config
        .config(
            "metadata.reporter.class", "org.apache.kafka.connect.openmetadata.OpenMetadataReporter")
        .config("metadata.reporter.openmetadata.url", OPENMETADATA_ADDRESS)
        .config("metadata.reporter.openmetadata.token", openMetadataService.getToken())
        .config("metadata.reporter.openmetadata.flush.interval.ms", 1000)
        .config("iceberg.metadata.kafka-service-name", kafkaServiceName)
        .config("iceberg.metadata.pipeline-service-name", kafkaConnectServiceName)
        .config("iceberg.metadata.iceberg-service-name", icebergServiceName)
        .config("iceberg.metadata.iceberg-database-name", ICEBERG_DATABASE_NAME)
        .config("iceberg.tables.dynamic-enabled", true)
        .config("iceberg.tables.route-field", "table");
  }

  private List<TableUserEvent> getEvents(
      List<TableUserEvent> tableUserEvents, TableIdentifier tableIdentifier) {
    return tableUserEvents.stream()
        .map(tableUserEvent -> tableUserEvent.setTable(tableIdentifier.toString()))
        .toList();
  }

  private String getIcebergTableFullName(TableIdentifier tableName) {
    return format("%s.%s.%s", icebergServiceName, ICEBERG_DATABASE_NAME, tableName);
  }

  private void assertIcebergKafkaEdge(Edge icebergKafkaEdge) {
    assertThat(icebergKafkaEdge.getLineageDetails().getColumnsLineage().size()).isEqualTo(3);
    Map<String, List<String>> columnLineageMap =
        icebergKafkaEdge.getLineageDetails().getColumnsLineage().stream()
            .map(
                columnLineage ->
                    Map.entry(columnLineage.getToColumn(), columnLineage.getFromColumns()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    String icebergFullTableName =
        extractNameFromColumn(columnLineageMap.keySet().stream().findAny().get());
    assertThat(columnLineageMap.keySet())
        .containsExactlyInAnyOrderElementsOf(
            TABLE_SCHEMA.columns().stream()
                .map(col -> format("%s.%s", icebergFullTableName, col.name()))
                .toList());
    columnLineageMap
        .values()
        .forEach(kafkaColumnList -> assertThat(kafkaColumnList.size()).isEqualTo(1));
    String fullKafkaTopicName =
        extractNameFromColumn(
            columnLineageMap.values().stream().findAny().get().stream().findAny().get());
    columnLineageMap.forEach(
        (icebergCol, kafkaColumns) ->
            assertThat(icebergCol.replace(format("%s.", icebergFullTableName), ""))
                .isEqualTo(kafkaColumns.get(0).replace(format("%s.", fullKafkaTopicName), "")));
    List<Edge> kafkaServiceEdges =
        openMetadataService.extractNonEmptyDownstreamEdges("messagingService", kafkaServiceName, 2);
    List<String> toEntities =
        kafkaServiceEdges.stream().map(edge -> edge.getToEntity().toString()).toList();
    assertThat(toEntities)
        .containsExactlyInAnyOrderElementsOf(
            List.of(icebergServiceId.toString(), kafkaConnectServiceId.toString()));
  }

  private String extractNameFromColumn(String columnName) {
    return columnName.substring(0, columnName.lastIndexOf("."));
  }

  private void assertMessageService() {
    List<Edge> kafkaServiceEdges =
        openMetadataService.extractNonEmptyDownstreamEdges("messagingService", kafkaServiceName, 2);
    List<String> toEntities =
        kafkaServiceEdges.stream().map(edge -> edge.getToEntity().toString()).toList();
    assertThat(toEntities)
        .containsExactlyInAnyOrderElementsOf(
            List.of(icebergServiceId.toString(), kafkaConnectServiceId.toString()));
  }

  private String getKafkaTopicFullName(String kafkaService, String topic) {
    return format("%s.%s", kafkaService, topic);
  }

  private String setServiceName(String baseServiceName) {
    return format("%s_%s", baseServiceName, suffix);
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER_1);
    catalog().dropTable(TABLE_IDENTIFIER_2);
    deleteTopic(spareTopic);
    openMetadataService.clearIcebergService(icebergIngestionPipelineId, icebergServiceId);
    openMetadataService.clearKafkaService(kafkaIngestionPipelineId, kafkaServiceId);
    openMetadataService.clearKafkaConnectService(kafkaConnectServiceId);
  }

  @Override
  protected String initConnectorName() {
    suffix = UUID.randomUUID().toString().substring(0, 4);
    icebergServiceName = setServiceName(ICEBERG_SERVICE_BASE_NAME);
    kafkaServiceName = setServiceName(KAFKA_SERVICE_BASE_NAME);
    kafkaConnectServiceName = setServiceName(KAFKA_CONNECT_SERVICE_BASE_NAME);
    kafkaConnectPipelineName = format("%s_pipeline", kafkaConnectServiceName);
    return kafkaConnectPipelineName;
  }
}
