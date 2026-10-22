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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iceberg.connect.service.ConnectorService.AWS_ACCESS_KEY;
import static org.apache.iceberg.connect.service.ConnectorService.AWS_REGION;
import static org.apache.iceberg.connect.service.ConnectorService.AWS_SECRET_KEY;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.AbstractTestBase;
import org.apache.iceberg.connect.BaseTestEvent;
import org.apache.iceberg.connect.KafkaConnectUtils;
import org.apache.iceberg.connect.TableUserEvent;
import org.apache.iceberg.connect.TestContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.openmetadata.client.api.DatabaseServicesApi;
import org.openmetadata.client.api.DatabasesApi;
import org.openmetadata.client.api.IngestionPipelinesApi;
import org.openmetadata.client.api.LineageApi;
import org.openmetadata.client.api.MessagingServicesApi;
import org.openmetadata.client.api.PipelineServicesApi;
import org.openmetadata.client.api.PipelinesApi;
import org.openmetadata.client.api.TablesApi;
import org.openmetadata.client.api.TopicsApi;
import org.openmetadata.client.api.UsersApi;
import org.openmetadata.client.gateway.OpenMetadata;
import org.openmetadata.client.model.AirflowConfig;
import org.openmetadata.client.model.CreateDatabaseService;
import org.openmetadata.client.model.CreateIngestionPipeline;
import org.openmetadata.client.model.CreateMessagingService;
import org.openmetadata.client.model.CreatePipeline;
import org.openmetadata.client.model.CreatePipelineService;
import org.openmetadata.client.model.CreateTopic;
import org.openmetadata.client.model.Database;
import org.openmetadata.client.model.DatabaseConnection;
import org.openmetadata.client.model.DatabaseList;
import org.openmetadata.client.model.Edge;
import org.openmetadata.client.model.EntityLineage;
import org.openmetadata.client.model.EntityReference;
import org.openmetadata.client.model.Field;
import org.openmetadata.client.model.IngestionPipeline;
import org.openmetadata.client.model.MessageSchema;
import org.openmetadata.client.model.MessagingConnection;
import org.openmetadata.client.model.PipelineConnection;
import org.openmetadata.client.model.PipelineStatus;
import org.openmetadata.client.model.SourceConfig;
import org.openmetadata.client.model.Table;
import org.openmetadata.client.model.Task;
import org.openmetadata.client.model.Topic;
import org.openmetadata.schema.metadataIngestion.DatabaseServiceMetadataPipeline;
import org.openmetadata.schema.metadataIngestion.FilterPattern;
import org.openmetadata.schema.metadataIngestion.Incremental;
import org.openmetadata.schema.metadataIngestion.MessagingServiceMetadataPipeline;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.security.credentials.AWSCredentials;
import org.openmetadata.schema.services.common.SSLCertPaths;
import org.openmetadata.schema.services.connections.database.IcebergConnection;
import org.openmetadata.schema.services.connections.database.iceberg.IcebergCatalog;
import org.openmetadata.schema.services.connections.database.iceberg.IcebergFileSystem;
import org.openmetadata.schema.services.connections.database.iceberg.RestCatalogConnection;
import org.openmetadata.schema.services.connections.messaging.KafkaConnection;
import org.openmetadata.schema.services.connections.messaging.SaslMechanismType;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;
import org.openmetadata.schema.services.connections.pipeline.KafkaConnectConnection;
import org.openmetadata.schema.type.OwnerConfig;

public class TestOpenMetadata extends AbstractTestBase {

  private static final int OPENMETADATA_PORT = 8585;
  private static final HttpClient HTTP = HttpClients.createDefault();
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
  private static final String KAFKA_CONNECT_HOST_PORT = "http://connect:8083";
  private static final String ICEBERG_DATABASE_NAME = "default";
  private static final String OPENMETADATA_ADDRESS =
      format("http://openmetadata:%s", OPENMETADATA_PORT);
  private String suffix;
  private String icebergServiceName;
  private String icebergServiceDatabaseName;
  private String kafkaServiceName;
  private String kafkaConnectServiceName;
  private String kafkaConnectPipelineName;
  private String token;
  private UUID userId;
  private UUID icebergServiceId;
  private UUID icebergIngestionPipelineId;
  private UUID kafkaServiceId;
  private UUID kafkaIngestionPipelineId;
  private UUID kafkaConnectServiceId;
  private OpenMetadata openMetadata;
  private IngestionPipelinesApi pipelineApi;
  private TopicsApi topicsApi;
  private LineageApi lineageApi;
  private DatabasesApi databasesApi;
  private DatabaseServicesApi databaseServicesApi;
  private IngestionPipelinesApi ingestionPipelinesApi;
  private UsersApi usersApi;
  private TablesApi tablesApi;
  private MessagingServicesApi messagingServicesApi;
  private PipelineServicesApi pipelineServicesApi;
  private PipelinesApi pipelinesApi;
  private String spareTopic;

  @BeforeEach
  public void beforeEach() throws IOException, URISyntaxException {
    token = getAuthToken();
    OpenMetadataJWTClientConfig openMetadataJWTClientConfig =
        new OpenMetadataJWTClientConfig().withJwtToken(token);
    openMetadata =
        new OpenMetadata(
            new OpenMetadataConnection()
                .withHostPort(format("http://localhost:%s/api", OPENMETADATA_PORT))
                .withAuthProvider(AuthProvider.OPENMETADATA)
                .withSecurityConfig(openMetadataJWTClientConfig));
    pipelineApi = openMetadata.buildClient(IngestionPipelinesApi.class);
    topicsApi = openMetadata.buildClient(TopicsApi.class);
    lineageApi = openMetadata.buildClient(LineageApi.class);
    databasesApi = openMetadata.buildClient(DatabasesApi.class);
    databaseServicesApi = openMetadata.buildClient(DatabaseServicesApi.class);
    ingestionPipelinesApi = openMetadata.buildClient(IngestionPipelinesApi.class);
    usersApi = openMetadata.buildClient(UsersApi.class);
    tablesApi = openMetadata.buildClient(TablesApi.class);
    messagingServicesApi = openMetadata.buildClient(MessagingServicesApi.class);
    pipelineServicesApi = openMetadata.buildClient(PipelineServicesApi.class);
    pipelinesApi = openMetadata.buildClient(PipelinesApi.class);
    userId = getAdminUserId();

    icebergServiceId = createIcebergService();
    icebergIngestionPipelineId = createIcebergIngestionPipeline();
    deployIngestionPipeline(icebergIngestionPipelineId);
    catalog().createTable(TABLE_IDENTIFIER_1, TABLE_SCHEMA, SPEC, Map.of());
    catalog().createTable(TABLE_IDENTIFIER_2, TABLE_SCHEMA, SPEC, Map.of());
    triggerPipelineRun(icebergIngestionPipelineId);
    assertIcebergServiceDatabases();
    assertIcebergServiceTables();

    kafkaServiceId = createKafkaService();
    kafkaIngestionPipelineId = createKafkaIngestionPipeline();
    deployIngestionPipeline(kafkaIngestionPipelineId);
    spareTopic = "test-topic-" + UUID.randomUUID();
    createTopic(spareTopic, TEST_TOPIC_PARTITIONS);
    triggerPipelineRun(kafkaIngestionPipelineId);
    assertKafkaServiceTopics();
    updateTopicSchema(testTopic());
    updateTopicSchema(spareTopic);

    kafkaConnectServiceId = createKafkaConnectService();
    createKafkaConnectPipeline(kafkaConnectPipelineName);
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
            extractNonEmptyUpstreamEdges("table", icebergFullTestTable1Name);
    assertThat(icebergTableLineageUpstreamEdges.size()).isEqualTo(1);
    assertIcebergKafkaEdge(icebergTableLineageUpstreamEdges.get(0));
    String fullKafkaTopicName = getKafkaTopicFullName(kafkaServiceName, testTopic());
    List<Edge> kafkaTopicLineageDownstreamEdges =
            extractNonEmptyDownstreamEdges("topic", fullKafkaTopicName);
    assertThat(kafkaTopicLineageDownstreamEdges.size()).isEqualTo(1);
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
              extractNonEmptyUpstreamEdges("table", icebergFullTableName);
          assertThat(icebergTableLineageUpstreamEdges.size()).isEqualTo(1);
          assertIcebergKafkaEdge(icebergTableLineageUpstreamEdges.get(0));
        });

    List<Edge> kafkaTopicLineageDownstreamEdges =
        extractNonEmptyDownstreamEdges("topic", fullKafkaTopicName);
    assertThat(kafkaTopicLineageDownstreamEdges.size()).isEqualTo(2);
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
        extractNonEmptyUpstreamEdges("table", icebergFullTableName);
    assertThat(icebergTableLineageUpstreamEdges.size()).isEqualTo(2);
    icebergTableLineageUpstreamEdges.forEach(
        icebergTableLineageUpstreamEdge -> assertIcebergKafkaEdge(icebergTableLineageUpstreamEdge));

    List<String> kafkaTopicsList =
        List.of(
            getKafkaTopicFullName(kafkaServiceName, testTopic()),
            getKafkaTopicFullName(kafkaServiceName, spareTopic));
    kafkaTopicsList.forEach(
        fullKafkaTopicName -> {
          List<Edge> kafkaTopicLineageDownstreamEdges =
              extractNonEmptyDownstreamEdges("topic", fullKafkaTopicName);
          assertThat(kafkaTopicLineageDownstreamEdges.size()).isEqualTo(1);
          Edge kafkaTopicLineageDownstreamEdge = kafkaTopicLineageDownstreamEdges.get(0);
          assertIcebergKafkaEdge(kafkaTopicLineageDownstreamEdge);
        });
    assertMessageService();
  }

  private String getAuthToken() throws IOException {
    HttpPost loginRequest =
        new HttpPost(
            format(Locale.ROOT, "http://localhost:%d/api/v1/users/login", OPENMETADATA_PORT));
    Map<String, String> config =
        Map.of(
            "email",
            "admin@open-metadata.org",
            "password",
            Base64.getEncoder().encodeToString("admin".getBytes(StandardCharsets.UTF_8)));
    String body = TestContext.MAPPER.writeValueAsString(config);
    loginRequest.setHeader("Content-Type", "application/json");
    loginRequest.setEntity(new StringEntity(body));
    return HTTP.execute(
        loginRequest,
        response ->
            TestContext.MAPPER
                .readTree(response.getEntity().getContent())
                .get("accessToken")
                .asText());
  }

  private UUID getAdminUserId() {
    return usersApi.getUserByFQN("admin", "id", "non-deleted").getId();
  }

  private UUID createIcebergService() throws URISyntaxException {
    IcebergFileSystem icebergFileSystem = new IcebergFileSystem();
    icebergFileSystem.withType(
        new AWSCredentials()
            .withAwsAccessKeyId(AWS_ACCESS_KEY)
            .withAwsSecretAccessKey(AWS_SECRET_KEY)
            .withAssumeRoleSessionName(AWS_REGION)
            .withEndPointURL(new URI("http://localhost:9000"))
            .withAwsRegion(AWS_REGION));
    RestCatalogConnection restCatalogConnection =
        new RestCatalogConnection()
            .withFileSystem(icebergFileSystem)
            .withUri(new URI("http://iceberg:8181"))
            .withSsl(new SSLCertPaths())
            .withSigv4(null)
            .withCredential(null);
    IcebergCatalog icebergCatalog =
        new IcebergCatalog()
            .withWarehouseLocation("s3://bucket/warehouse")
            .withName("iceberg")
            .withConnection(restCatalogConnection);
    IcebergConnection icebergConnection =
        new IcebergConnection()
            .withOwnershipProperty("owner")
            .withType(IcebergConnection.IcebergType.ICEBERG)
            .withCatalog(icebergCatalog)
            .withSupportsMetadataExtraction(true)
            .withSchemaFilterPattern(
                new FilterPattern().withIncludes(List.of()).withExcludes(List.of()))
            .withTableFilterPattern(
                new FilterPattern().withIncludes(List.of()).withExcludes(List.of()))
            .withDatabaseFilterPattern(
                new FilterPattern().withIncludes(List.of()).withExcludes(List.of()));
    CreateDatabaseService createDatabaseService =
        new CreateDatabaseService()
            .name(icebergServiceName)
            .serviceType(CreateDatabaseService.ServiceTypeEnum.ICEBERG)
            .connection(new DatabaseConnection().config(icebergConnection))
            .description("")
            .owners(List.of(new EntityReference().id(userId).type("user")));
    return databaseServicesApi.createOrUpdateDatabaseService(createDatabaseService).getId();
  }

  private UUID createIcebergIngestionPipeline() {
    DatabaseServiceMetadataPipeline databaseServiceMetadataPipeline =
        new DatabaseServiceMetadataPipeline()
            .withMarkDeletedStoredProcedures(true)
            .withQueryLogDuration(1)
            .withIncludeTables(true)
            .withQueryParsingTimeoutLimit(300)
            .withType(DatabaseServiceMetadataPipeline.DatabaseMetadataConfigType.DATABASE_METADATA)
            .withIncludeDDL(false)
            .withIncludeTags(true)
            .withMarkDeletedSchemas(false)
            .withIncremental(
                new Incremental().withEnabled(false).withSafetyMarginDays(1).withLookbackDays(7))
            .withThreads(1)
            .withMarkDeletedDatabases(false)
            .withMarkDeletedTables(true)
            .withIncludeOwners(false)
            .withUseFqnForFiltering(false)
            .withIncludeViews(true)
            .withOwnerConfig(new OwnerConfig().withEnableInheritance(true))
            .withIncludeStoredProcedures(true)
            .withOverrideMetadata(false);
    CreateIngestionPipeline ingestionPipeline =
        new CreateIngestionPipeline()
            .raiseOnError(true)
            .owners(List.of(new EntityReference().type("user").id(userId)))
            .name(ingestionPipelineName(icebergServiceName))
            .sourceConfig(new SourceConfig().config(databaseServiceMetadataPipeline))
            .service(new EntityReference().type("databaseService").id(icebergServiceId))
            .loggerLevel(CreateIngestionPipeline.LoggerLevelEnum.INFO)
            .displayName(ingestionPipelineName(icebergServiceName))
            .pipelineType(CreateIngestionPipeline.PipelineTypeEnum.METADATA)
            .airflowConfig(new AirflowConfig().retries(0).startDate(getTime()));
    return ingestionPipelinesApi.createOrUpdateIngestionPipeline(ingestionPipeline).getId();
  }

  private void deployIngestionPipeline(UUID ingestionPipelineId) {
    final AtomicBoolean isDeploySent = new AtomicBoolean(false);
    await()
        .atMost(45, SECONDS)
        .with()
        .pollInterval(2, SECONDS)
        .until(
            () -> {
              if (!isDeploySent.get()) {
                try {
                  pipelineApi.deployIngestion(ingestionPipelineId);
                  isDeploySent.set(true);
                } catch (Exception e) {
                  return false;
                }
              }
              IngestionPipeline ingestionPipeline = getIngestionPipeline(ingestionPipelineId);
              return ingestionPipeline.getDeployed() != null && ingestionPipeline.getDeployed();
            });
  }

  private void triggerPipelineRun(UUID ingestionPipelineId) {
    AtomicBoolean isTriggerSent = new AtomicBoolean(false);
    await()
        .atMost(60, SECONDS)
        .with()
        .pollInterval(2, SECONDS)
        .until(
            () -> {
              if (!isTriggerSent.get()) {
                try {
                  pipelineApi.triggerIngestionPipelineRun(ingestionPipelineId);
                  isTriggerSent.set(true);
                } catch (Exception e) {
                  return false;
                }
              }
              IngestionPipeline pipeline = getIngestionPipeline(ingestionPipelineId);
              if (pipeline != null
                  && pipeline.getPipelineStatuses() != null
                  && pipeline.getPipelineStatuses().getPipelineState() != null) {
                return pipeline
                    .getPipelineStatuses()
                    .getPipelineState()
                    .equals(PipelineStatus.PipelineStateEnum.SUCCESS);
              }
              return false;
            });
  }

  private void assertIcebergServiceDatabases() {
    await()
        .atMost(15, SECONDS)
        .pollInterval(1, SECONDS)
        .until(
            () -> {
              try {
                DatabaseList databaseList =
                    databasesApi.listDatabases(
                        "", icebergServiceName, 10, null, null, "non-deleted");
                return databaseList.getData().stream()
                    .map(Database::getName)
                    .toList()
                    .contains("default");
              } catch (Exception e) {
                return false;
              }
            });
  }

  private void assertIcebergServiceTables() {
    TablesApi.ListTablesQueryParams tablesSearchParam =
            new TablesApi.ListTablesQueryParams()
                    .fields("")
                    .database(icebergServiceDatabaseName)
                    .databaseSchema(format("%s.%s", icebergServiceDatabaseName, TEST_DB));
    List<String> tables =
            tablesApi.listTables(tablesSearchParam).getData().stream().map(Table::getName).toList();
    assertThat(tables).containsAll(List.of(TEST_TABLE_1, TEST_TABLE_2));
  }

  private UUID createKafkaService() {
    KafkaConnection kafkaConnection =
        new KafkaConnection()
            .withType(KafkaConnection.KafkaType.KAFKA)
            .withBootstrapServers("kafka:9092")
            .withSecurityProtocol(KafkaConnection.SecurityProtocol.PLAINTEXT)
            .withSaslMechanism(SaslMechanismType.PLAIN)
            .withSupportsMetadataExtraction(true)
            .withTopicFilterPattern(
                new FilterPattern().withExcludes(List.of()).withIncludes(List.of()));
    MessagingConnection messagingConnection = new MessagingConnection().config(kafkaConnection);
    CreateMessagingService createMessagingService =
        new CreateMessagingService()
            .name(kafkaServiceName)
            .description("")
            .serviceType(CreateMessagingService.ServiceTypeEnum.KAFKA)
            .connection(messagingConnection)
            .owners(List.of(new EntityReference().id(userId).type("user")));
    return messagingServicesApi.createOrUpdateMessagingService(createMessagingService).getId();
  }

  private UUID createKafkaIngestionPipeline() {
    CreateIngestionPipeline createIngestionPipeline =
        new CreateIngestionPipeline()
            .airflowConfig(new AirflowConfig().retries(0).startDate(getTime()))
            .raiseOnError(true)
            .loggerLevel(CreateIngestionPipeline.LoggerLevelEnum.INFO)
            .name(ingestionPipelineName(kafkaServiceName))
            .displayName(ingestionPipelineName(kafkaServiceName))
            .owners(List.of(new EntityReference().type("user").id(userId)))
            .pipelineType(CreateIngestionPipeline.PipelineTypeEnum.METADATA)
            .service(new EntityReference().type("messagingService").id(kafkaServiceId))
            .sourceConfig(
                new SourceConfig()
                    .config(
                        new MessagingServiceMetadataPipeline()
                            .withType(
                                MessagingServiceMetadataPipeline.MessagingMetadataConfigType
                                    .MESSAGING_METADATA)
                            .withGenerateSampleData(false)
                            .withMarkDeletedTopics(true)
                            .withOverrideMetadata(false)));
    return ingestionPipelinesApi.createOrUpdateIngestionPipeline(createIngestionPipeline).getId();
  }

  private void assertKafkaServiceTopics() {
    TopicsApi.ListTopicsQueryParams topicsSearchParam =
        new TopicsApi.ListTopicsQueryParams().service(kafkaServiceName).fields("").limit(10);
    await()
        .atMost(15, SECONDS)
        .pollInterval(1, SECONDS)
        .until(
            () -> {
              try {
                var response = topicsApi.listTopics(topicsSearchParam);
                if (response == null || response.getData() == null) {
                  return false;
                }
                List<String> currentTopics =
                    response.getData().stream().map(Topic::getName).toList();
                return currentTopics.containsAll(List.of(testTopic(), spareTopic));
              } catch (Exception e) {
                return false;
              }
            });
  }

  private void updateTopicSchema(String topicName) {
    MessageSchema messageSchema =
        new MessageSchema()
            .schemaType(MessageSchema.SchemaTypeEnum.JSON)
            .schemaFields(TOPIC_SCHEMA);
    CreateTopic createTopicRequest =
        new CreateTopic()
            .name(topicName)
            .service(kafkaServiceName)
            .partitions(1)
            .messageSchema(messageSchema);
    topicsApi.createOrUpdateTopic(createTopicRequest);
  }

  private UUID createKafkaConnectService() throws URISyntaxException {
    KafkaConnectConnection kafkaConnectConnection =
        new KafkaConnectConnection()
            .withType(KafkaConnectConnection.KafkaConnectType.KAFKA_CONNECT)
            .withHostPort(new URI(KAFKA_CONNECT_HOST_PORT))
            .withVerifySSL(false)
            .withMessagingServiceName(kafkaServiceName)
            .withSupportsMetadataExtraction(true)
            .withPipelineFilterPattern(
                new FilterPattern().withIncludes(List.of(".*")).withExcludes(List.of()));
    PipelineConnection pipelineConnection = new PipelineConnection().config(kafkaConnectConnection);
    CreatePipelineService createPipelineService =
        new CreatePipelineService()
            .name(kafkaConnectServiceName)
            .description("")
            .serviceType(CreatePipelineService.ServiceTypeEnum.KAFKA_CONNECT)
            .connection(pipelineConnection)
            .owners(List.of(new EntityReference().id(userId).type("user")));
    return pipelineServicesApi.createOrUpdatePipelineService(createPipelineService).getId();
  }

  private UUID createKafkaConnectPipeline(String pipelineName) {
    CreatePipeline createPipeline =
        new CreatePipeline()
            .name(pipelineName)
            .displayName(pipelineName)
            .description("")
            .service(kafkaConnectServiceName)
            .tasks(
                List.of(
                    new Task()
                        .name("iceberg_sink_task")
                        .displayName("Iceberg Sink Task")
                        .taskType("Sink")));
    return pipelinesApi.createOrUpdatePipeline(createPipeline).getId();
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
        .config("metadata.reporter.openmetadata.token", token)
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

  private List<Edge> extractNonEmptyUpstreamEdges(String entity, String fqn) {
    AtomicReference<EntityLineage> lineageRef = new AtomicReference<>();
    await()
        .atMost(5, SECONDS)
        .pollInterval(1, SECONDS)
        .until(
            () -> {
              try {
                EntityLineage lineage = lineageApi.getLineageByFQN(entity, fqn, 1, 1);
                if (lineage != null
                    && lineage.getUpstreamEdges() != null
                    && !lineage.getUpstreamEdges().isEmpty()) {
                  lineageRef.set(lineage);
                  return true;
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return false;
            });
    EntityLineage lineage = lineageRef.get();
    return lineage.getUpstreamEdges();
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
        extractNonEmptyDownstreamEdges("messagingService", kafkaServiceName);
    assertThat(kafkaServiceEdges.size()).isEqualTo(2);
    List<String> toEntities =
        kafkaServiceEdges.stream().map(edge -> edge.getToEntity().toString()).toList();
    assertThat(toEntities)
        .containsExactlyInAnyOrderElementsOf(
            List.of(icebergServiceId.toString(), kafkaConnectServiceId.toString()));
  }

  private String extractNameFromColumn(String columnName) {
    return columnName.substring(0, columnName.lastIndexOf("."));
  }

  private List<Edge> extractNonEmptyDownstreamEdges(String entity, String fqn) {
    AtomicReference<EntityLineage> lineageRef = new AtomicReference<>();
    await()
        .atMost(5, SECONDS)
        .pollInterval(1, SECONDS)
        .until(
            () -> {
              try {
                EntityLineage lineage = lineageApi.getLineageByFQN(entity, fqn, 1, 1);
                if (lineage != null
                    && lineage.getDownstreamEdges() != null
                    && !lineage.getDownstreamEdges().isEmpty()) {
                  lineageRef.set(lineage);
                  return true;
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return false;
            });
    EntityLineage lineage = lineageRef.get();
    return lineage.getDownstreamEdges();
  }

  private void assertMessageService() {
    List<Edge> kafkaServiceEdges =
        extractNonEmptyDownstreamEdges("messagingService", kafkaServiceName);
    assertThat(kafkaServiceEdges.size()).isEqualTo(2);
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

  private OffsetDateTime getTime() {
    return OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
  }

  private String ingestionPipelineName(String serviceName) {
    return serviceName + "_ingestion_pipeline";
  }

  private IngestionPipeline getIngestionPipeline(UUID ingestionPipelineId) {
    return pipelineApi.getIngestionPipelineByID(
        ingestionPipelineId, "pipelineStatuses", "non-deleted", "all");
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER_1);
    catalog().dropTable(TABLE_IDENTIFIER_2);
    ingestionPipelinesApi.deleteIngestionPipeline(icebergIngestionPipelineId, true);
    databaseServicesApi.deleteDatabaseService(icebergServiceId, true, true);
    ingestionPipelinesApi.deleteIngestionPipeline(kafkaIngestionPipelineId, true);
    messagingServicesApi.deleteMessagingService(kafkaServiceId, true, true);
    pipelineServicesApi.deletePipelineService(kafkaConnectServiceId, true, true);
  }

  @Override
  protected String initConnectorName() {
    suffix = UUID.randomUUID().toString().substring(0, 4);
    icebergServiceName = setServiceName(ICEBERG_SERVICE_BASE_NAME);
    icebergServiceDatabaseName = format("%s.%s", icebergServiceName, ICEBERG_DATABASE_NAME);
    kafkaServiceName = setServiceName(KAFKA_SERVICE_BASE_NAME);
    kafkaConnectServiceName = setServiceName(KAFKA_CONNECT_SERVICE_BASE_NAME);
    kafkaConnectPipelineName = format("%s_pipeline", kafkaConnectServiceName);
    return kafkaConnectPipelineName;
  }
}
