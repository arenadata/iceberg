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
package org.apache.iceberg.connect.service;

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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.connect.TestContext;
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

public class OpenMetadataIcebergKafkaDeployService {
  public static final int OPENMETADATA_PORT = 8585;
  public static final String KAFKA_CONNECT_HOST_PORT = "http://connect:8083";
  private final String token;
  private final UUID userId;
  private final IngestionPipelinesApi pipelineApi;
  private final TopicsApi topicsApi;
  private final LineageApi lineageApi;
  private final DatabasesApi databasesApi;
  private final DatabaseServicesApi databaseServicesApi;
  private final IngestionPipelinesApi ingestionPipelinesApi;
  private final UsersApi usersApi;
  private final TablesApi tablesApi;
  private final MessagingServicesApi messagingServicesApi;
  private final PipelineServicesApi pipelineServicesApi;
  private final PipelinesApi pipelinesApi;
  private final HttpClient http;

  public OpenMetadataIcebergKafkaDeployService() throws IOException {
    this.http = HttpClients.createDefault();
    this.token = setAdminAuthToken();
    OpenMetadataJWTClientConfig openMetadataJWTClientConfig =
        new OpenMetadataJWTClientConfig().withJwtToken(token);
    OpenMetadata openMetadata =
        new OpenMetadata(
            new OpenMetadataConnection()
                .withHostPort(format("http://localhost:%s/api", OPENMETADATA_PORT))
                .withAuthProvider(AuthProvider.OPENMETADATA)
                .withSecurityConfig(openMetadataJWTClientConfig));
    this.pipelineApi = openMetadata.buildClient(IngestionPipelinesApi.class);
    this.topicsApi = openMetadata.buildClient(TopicsApi.class);
    this.lineageApi = openMetadata.buildClient(LineageApi.class);
    this.databasesApi = openMetadata.buildClient(DatabasesApi.class);
    this.databaseServicesApi = openMetadata.buildClient(DatabaseServicesApi.class);
    this.ingestionPipelinesApi = openMetadata.buildClient(IngestionPipelinesApi.class);
    this.usersApi = openMetadata.buildClient(UsersApi.class);
    this.tablesApi = openMetadata.buildClient(TablesApi.class);
    this.messagingServicesApi = openMetadata.buildClient(MessagingServicesApi.class);
    this.pipelineServicesApi = openMetadata.buildClient(PipelineServicesApi.class);
    this.pipelinesApi = openMetadata.buildClient(PipelinesApi.class);
    this.userId = setAdminUserId();
  }

  public String setAdminAuthToken() throws IOException {
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
    return http.execute(
        loginRequest,
        response ->
            TestContext.MAPPER
                .readTree(response.getEntity().getContent())
                .get("accessToken")
                .asText());
  }

  public UUID setAdminUserId() {
    return usersApi.getUserByFQN("admin", "id", "non-deleted").getId();
  }

  public String getToken() {
    return token;
  }

  public Pair<UUID, UUID> deployIcebergService(
      String icebergServiceName, String databaseName, String schemaName, List<String> icebergTables)
      throws URISyntaxException {
    UUID icebergServiceId = createIcebergService(icebergServiceName);
    UUID icebergIngestionPipelineId =
        createIcebergIngestionPipeline(icebergServiceName, icebergServiceId);
    deployIngestionPipeline(icebergIngestionPipelineId);
    triggerPipelineRun(icebergIngestionPipelineId);
    assertIcebergServiceDatabases(icebergServiceName, databaseName);
    assertIcebergServiceTables(icebergServiceName, databaseName, schemaName, icebergTables);
    return Pair.of(icebergServiceId, icebergIngestionPipelineId);
  }

  public UUID createIcebergService(String icebergServiceName) throws URISyntaxException {
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

  public UUID createIcebergIngestionPipeline(String icebergServiceName, UUID icebergServiceId) {
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

  private OffsetDateTime getTime() {
    return OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
  }

  public void deployIngestionPipeline(UUID ingestionPipelineId) {
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

  public void triggerPipelineRun(UUID ingestionPipelineId) {
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

  public String ingestionPipelineName(String serviceName) {
    return serviceName + "_ingestion_pipeline";
  }

  public IngestionPipeline getIngestionPipeline(UUID ingestionPipelineId) {
    return pipelineApi.getIngestionPipelineByID(
        ingestionPipelineId, "pipelineStatuses", "non-deleted", "all");
  }

  private void assertIcebergServiceDatabases(String icebergServiceName, String databaseName) {
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
                    .contains(databaseName);
              } catch (Exception e) {
                return false;
              }
            });
  }

  public void assertIcebergServiceTables(
      String icebergServiceName,
      String databaseName,
      String schemaName,
      List<String> icebergTables) {
    String icebergServiceDatabaseName = format("%s.%s", icebergServiceName, databaseName);
    TablesApi.ListTablesQueryParams tablesSearchParam =
        new TablesApi.ListTablesQueryParams()
            .fields("")
            .database(icebergServiceDatabaseName)
            .databaseSchema(format("%s.%s", icebergServiceDatabaseName, schemaName));
    List<String> tables =
        tablesApi.listTables(tablesSearchParam).getData().stream().map(Table::getName).toList();
    assertThat(tables).containsAll(icebergTables);
  }

  public Pair<UUID, UUID> deployKafkaService(String kafkaServiceName, List<String> kafkaTopics) {
    UUID kafkaServiceId = createKafkaService(kafkaServiceName);
    UUID kafkaIngestionPipelineId = createKafkaIngestionPipeline(kafkaServiceName, kafkaServiceId);
    deployIngestionPipeline(kafkaIngestionPipelineId);
    triggerPipelineRun(kafkaIngestionPipelineId);
    assertKafkaServiceTopics(kafkaServiceName, kafkaTopics);
    return Pair.of(kafkaServiceId, kafkaIngestionPipelineId);
  }

  public UUID createKafkaService(String kafkaServiceName) {
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

  public UUID createKafkaIngestionPipeline(String kafkaServiceName, UUID kafkaServiceId) {
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

  public void assertKafkaServiceTopics(String kafkaServiceName, List<String> kafkaServiceTopics) {
    TopicsApi.ListTopicsQueryParams topicsSearchParam =
        new TopicsApi.ListTopicsQueryParams().service(kafkaServiceName).fields("").limit(20);
    await()
        .atMost(30, SECONDS)
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
                return currentTopics.containsAll(kafkaServiceTopics);
              } catch (Exception e) {
                return false;
              }
            });
  }

  public void updateTopicSchema(
      String kafkaServiceName, String topicName, List<Field> topicSchema) {
    MessageSchema messageSchema =
        new MessageSchema().schemaType(MessageSchema.SchemaTypeEnum.JSON).schemaFields(topicSchema);
    CreateTopic createTopicRequest =
        new CreateTopic()
            .name(topicName)
            .service(kafkaServiceName)
            .partitions(1)
            .messageSchema(messageSchema);
    topicsApi.createOrUpdateTopic(createTopicRequest);
  }

  public UUID deployKafkaConnectService(
      String kafkaConnectServiceName, String kafkaServiceName, String kafkaConnectPipelineName)
      throws URISyntaxException {
    UUID kafkaConnectServiceId =
        createKafkaConnectService(kafkaConnectServiceName, kafkaServiceName);
    createKafkaConnectPipeline(kafkaConnectServiceName, kafkaConnectPipelineName);
    return kafkaConnectServiceId;
  }

  public UUID createKafkaConnectService(String kafkaConnectServiceName, String kafkaServiceName)
      throws URISyntaxException {
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

  public UUID createKafkaConnectPipeline(String kafkaConnectServiceName, String pipelineName) {
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

  public void clearIcebergService(UUID icebergIngestionPipelineId, UUID icebergServiceId) {
    ingestionPipelinesApi.deleteIngestionPipeline(icebergIngestionPipelineId, true);
    databaseServicesApi.deleteDatabaseService(icebergServiceId, true, true);
  }

  public void clearKafkaService(UUID kafkaIngestionPipelineId, UUID kafkaServiceId) {
    ingestionPipelinesApi.deleteIngestionPipeline(kafkaIngestionPipelineId, true);
    messagingServicesApi.deleteMessagingService(kafkaServiceId, true, true);
  }

  public void clearKafkaConnectService(UUID kafkaConnectServiceId) {
    pipelineServicesApi.deletePipelineService(kafkaConnectServiceId, true, true);
  }

  public List<Edge> extractNonEmptyUpstreamEdges(String entity, String fqn, int edgesCount) {
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
                    && lineage.getUpstreamEdges().size() == edgesCount) {
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

  public List<Edge> extractNonEmptyDownstreamEdges(String entity, String fqn, int edgesCount) {
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
                    && lineage.getDownstreamEdges().size() == edgesCount) {
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
}
