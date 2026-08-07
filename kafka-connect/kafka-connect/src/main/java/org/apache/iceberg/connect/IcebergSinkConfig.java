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
package org.apache.iceberg.connect;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.RecordRoutingStrategy;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkConfig extends AbstractConfig {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkConfig.class.getName());

  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";
  private static final String ROUTE_REGEX = "route-regex";
  private static final String ID_COLUMNS = "id-columns";
  private static final String PARTITION_BY = "partition-by";
  private static final String COMMIT_BRANCH = "commit-branch";

  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final String HADOOP_PROP_PREFIX = "iceberg.hadoop.";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";
  private static final String TABLE_PROP_PREFIX = "iceberg.table.";
  private static final String AUTO_CREATE_PROP_PREFIX = "iceberg.tables.auto-create-props.";
  private static final String WRITE_PROP_PREFIX = "iceberg.tables.write-props.";

  private static final String CATALOG_NAME_PROP = "iceberg.catalog";
  private static final String TABLES_PROP = "iceberg.tables";
  private static final String TABLES_DYNAMIC_PROP = "iceberg.tables.dynamic-enabled";
  private static final String TABLES_ROUTE_FIELD_PROP = "iceberg.tables.route-field";
  private static final String TABLES_TOPIC_TO_TABLE_MAPPING_PROP =
      "iceberg.tables.topic-to-table-mapping";
  private static final String TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP =
      "iceberg.tables.topic-to-table-mapping-file";
  private static final String ROUTING_STRATEGY_PROP = "routing.strategy";
  private static final String TABLES_CDC_FIELD_PROP = "iceberg.tables.cdc-field";
  private static final String TABLES_UPSERT_MODE_ENABLED_PROP =
      "iceberg.tables.upsert-mode-enabled";
  private static final String TABLES_CDC_OPS_INSERT_PROP = "iceberg.tables.cdc.ops.insert";
  private static final String TABLES_CDC_OP_INSERT_DEFAULT = "r,c";
  private static final String TABLES_CDC_OPS_UPDATE_PROP = "iceberg.tables.cdc.ops.update";
  private static final String TABLES_CDC_OP_UPDATE_DEFAULT = "u";
  private static final String TABLES_CDC_OPS_DELETE_PROP = "iceberg.tables.cdc.ops.delete";
  private static final String TABLES_CDC_OP_DELETE_DEFAULT = "d";
  private static final String TABLES_CDC_OPS_IGNORE_PROP = "iceberg.tables.cdc.ops.ignored";
  private static final String TABLES_CDC_OP_IGNORE_DEFAULT = "t,m";
  private static final String TABLES_DEFAULT_COMMIT_BRANCH = "iceberg.tables.default-commit-branch";
  private static final String TABLES_DEFAULT_ID_COLUMNS = "iceberg.tables.default-id-columns";
  private static final String TABLES_DEFAULT_PARTITION_BY = "iceberg.tables.default-partition-by";
  private static final String TABLES_AUTO_CREATE_ENABLED_PROP =
      "iceberg.tables.auto-create-enabled";
  private static final String TABLES_EVOLVE_SCHEMA_ENABLED_PROP =
      "iceberg.tables.evolve-schema-enabled";
  private static final String TABLES_SCHEMA_FORCE_OPTIONAL_PROP =
      "iceberg.tables.schema-force-optional";
  private static final String TABLES_SCHEMA_CASE_INSENSITIVE_PROP =
      "iceberg.tables.schema-case-insensitive";
  private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
  private static final String CONTROL_GROUP_ID_PREFIX_PROP = "iceberg.control.group-id-prefix";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.control.commit.interval-ms";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.control.commit.timeout-ms";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
  private static final String COMMIT_THREADS_PROP = "iceberg.control.commit.threads";
  private static final String CONNECT_GROUP_ID_PROP = "iceberg.connect.group-id";
  private static final String TRANSACTIONAL_PREFIX_PROP =
      "iceberg.coordinator.transactional.prefix";
  private static final String HADOOP_CONF_DIR_PROP = "iceberg.hadoop-conf-dir";

  private static final String HDFS_AUTHENTICATION_KERBEROS_PROP =
      "iceberg.hdfs.authentication.kerberos";
  private static final Boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
  private static final String CONNECT_HDFS_PRINCIPAL_PROP = "iceberg.connect.hdfs.principal";
  private static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
  private static final String CONNECT_HDFS_KEYTAB_PROP = "iceberg.connect.hdfs.keytab";
  private static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_PROP =
      "kerberos.ticket.renew.period.ms";
  private static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;

  private static final String TABLES_SCHEMA_VARIANT_FIELDS_PROP =
      "iceberg.tables.schema-variant-fields";
  private static final String TABLES_SCHEMA_TIMESTAMP_NS_FIELDS_PROP =
      "iceberg.tables.schema-timestamp-ns-fields";
  private static final String TABLES_EVOLVE_UNKNOWN_TYPE_ENABLED_PROP =
      "iceberg.tables.evolve-unknown-type-enabled";
  private static final String TABLES_DEFAULTS_ENABLED_PROP = "iceberg.tables.defaults-enabled";
  private static final String NAME_PROP = "name";
  private static final String TASK_ID = "task.id";
  private static final String BOOTSTRAP_SERVERS_PROP = "bootstrap.servers";

  private static final String DEFAULT_CATALOG_NAME = "iceberg";
  private static final String DEFAULT_CONTROL_TOPIC = "control-iceberg";
  public static final String DEFAULT_CONTROL_GROUP_PREFIX = "cg-control-";

  public static final int SCHEMA_UPDATE_RETRIES = 2; // 3 total attempts
  public static final int CREATE_TABLE_RETRIES = 2; // 3 total attempts

  private static final String COORDINATOR_EXECUTOR_KEEP_ALIVE_TIMEOUT_MS =
      "iceberg.coordinator-executor-keep-alive-timeout-ms";
  private static final int TOPIC_TO_TABLE_MAPPING_FILE_VERSION = 1;
  private static final String TOPIC_TO_TABLE_MAPPING_FILE_VERSION_FIELD = "version";
  private static final String TOPIC_TO_TABLE_MAPPING_FILE_ROUTES_FIELD = "routes";
  private static final String TOPIC_TO_TABLE_MAPPING_SOURCE_NONE = "none";
  private static final String TOPIC_TO_TABLE_MAPPING_SOURCE_INLINE = "inline";
  private static final String TOPIC_TO_TABLE_MAPPING_SOURCE_FILE = "file";
  private static final ObjectMapper TOPIC_TO_TABLE_MAPPING_FILE_MAPPER =
      new ObjectMapper(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  @VisibleForTesting static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

  public static final ConfigDef CONFIG_DEF = newConfigDef();

  public static String version() {
    return IcebergBuild.version();
  }

  private static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        ROUTING_STRATEGY_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Routing strategy. Supported values: dynamic-field, all-tables, regex, topic-to-table");
    configDef.define(
        TABLES_PROP,
        ConfigDef.Type.LIST,
        null,
        Importance.HIGH,
        "Comma-delimited list of destination tables");
    configDef.define(
        TABLES_DYNAMIC_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Enable dynamic routing to tables based on a record value");
    configDef.define(
        TABLES_ROUTE_FIELD_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field for routing records to tables");
    configDef.define(
        TABLES_TOPIC_TO_TABLE_MAPPING_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Static mapping from topic name to table name in format topic1:db.table1,topic2:db.table2");
    configDef.define(
        TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Absolute path to a JSON file with static mapping from topic name to table name");
    configDef.define(
        TABLES_UPSERT_MODE_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to treat all appends as upserts, false otherwise");
    configDef.define(
        TABLES_DEFAULT_COMMIT_BRANCH,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Default branch for commits");
    configDef.define(
        TABLES_DEFAULT_ID_COLUMNS,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Default ID columns for tables, comma-separated");
    configDef.define(
        TABLES_DEFAULT_PARTITION_BY,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Default partition spec to use when creating tables, comma-separated");
    configDef.define(
        TABLES_AUTO_CREATE_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to automatically create destination tables, false otherwise");
    configDef.define(
        TABLES_SCHEMA_FORCE_OPTIONAL_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to set columns as optional during table create and evolution, false to respect schema");
    configDef.define(
        TABLES_SCHEMA_CASE_INSENSITIVE_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to look up table columns by case-insensitive name, false for case-sensitive");
    configDef.define(
        TABLES_EVOLVE_SCHEMA_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to add any missing record fields to the table schema, false otherwise");
    configDef.define(
        CATALOG_NAME_PROP,
        ConfigDef.Type.STRING,
        DEFAULT_CATALOG_NAME,
        Importance.MEDIUM,
        "Iceberg catalog name");
    configDef.define(
        CONTROL_TOPIC_PROP,
        ConfigDef.Type.STRING,
        DEFAULT_CONTROL_TOPIC,
        Importance.MEDIUM,
        "Name of the control topic");
    configDef.define(
        CONTROL_GROUP_ID_PREFIX_PROP,
        ConfigDef.Type.STRING,
        DEFAULT_CONTROL_GROUP_PREFIX,
        Importance.LOW,
        "Prefix of the control consumer group");
    configDef.define(
        CONNECT_GROUP_ID_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.LOW,
        "Name of the Connect consumer group, should not be set under normal conditions");
    configDef.define(
        COMMIT_INTERVAL_MS_PROP,
        ConfigDef.Type.INT,
        COMMIT_INTERVAL_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator interval for performing Iceberg table commits, in millis");
    configDef.define(
        COMMIT_TIMEOUT_MS_PROP,
        ConfigDef.Type.INT,
        COMMIT_TIMEOUT_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator time to wait for worker responses before committing, in millis");
    configDef.define(
        COMMIT_THREADS_PROP,
        ConfigDef.Type.INT,
        Runtime.getRuntime().availableProcessors() * 2,
        Importance.MEDIUM,
        "Coordinator threads to use for table commits, default is (cores * 2)");
    configDef.define(
        TRANSACTIONAL_PREFIX_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.LOW,
        "Optional prefix of the transactional id for the coordinator");
    configDef.define(
        HADOOP_CONF_DIR_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "If specified, Hadoop config files in this directory will be loaded");
    configDef.define(
        COORDINATOR_EXECUTOR_KEEP_ALIVE_TIMEOUT_MS,
        ConfigDef.Type.LONG,
        120000L,
        Importance.LOW,
        "config to control coordinator executor keep alive time");
    defineHdfsKerberosProps(configDef);
    defineV3NewTypesSupportProps(configDef);
    defineCdcProps(configDef);
    return configDef;
  }

  private static void defineHdfsKerberosProps(ConfigDef configDef) {
    configDef.define(
        HDFS_AUTHENTICATION_KERBEROS_PROP,
        ConfigDef.Type.BOOLEAN,
        HDFS_AUTHENTICATION_KERBEROS_DEFAULT,
        Importance.HIGH,
        "Configuration indicating whether HDFS is using Kerberos for authentication");
    configDef.define(
        CONNECT_HDFS_PRINCIPAL_PROP,
        ConfigDef.Type.STRING,
        CONNECT_HDFS_PRINCIPAL_DEFAULT,
        Importance.HIGH,
        "The principal name to load from the keytab for Kerberos authentication");
    configDef.define(
        CONNECT_HDFS_KEYTAB_PROP,
        ConfigDef.Type.STRING,
        CONNECT_HDFS_KEYTAB_DEFAULT,
        Importance.HIGH,
        "The path to the keytab file for the HDFS connector principal. This keytab file should only be readable by the connector user");
    configDef.define(
        KERBEROS_TICKET_RENEW_PERIOD_MS_PROP,
        ConfigDef.Type.LONG,
        KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT,
        Importance.LOW,
        "The period in milliseconds to renew the Kerberos ticket");
  }

  private static void defineV3NewTypesSupportProps(ConfigDef configDef) {
    configDef.define(
        TABLES_SCHEMA_VARIANT_FIELDS_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Comma-separated list of record field paths that must be mapped to Iceberg VARIANT. "
            + "Paths use dot-notation, e.g. 'payload', 'after.details', 'meta.props'.");
    configDef.define(
        TABLES_SCHEMA_TIMESTAMP_NS_FIELDS_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Comma-separated list of record field names or exact field paths that must map Debezium "
            + "NanoTimestamp fields to Iceberg timestamp_ns. Use '*' to match all Debezium "
            + "NanoTimestamp fields, e.g. 'event_time', 'after.event_time', '*'.");
    configDef.define(
        TABLES_EVOLVE_UNKNOWN_TYPE_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to create columns with UNKNOWN type when the field type cannot be inferred "
            + "and later evolve UNKNOWN columns to a specific type when a non-null value arrives.");
    configDef.define(
        TABLES_DEFAULTS_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Enable mapping Kafka Connect schema default values to Iceberg write-default/initial-default");
  }

  private static void defineCdcProps(ConfigDef configDef) {
    configDef.define(
        TABLES_CDC_FIELD_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field that identifies the type of operation (insert, update, or delete)");
    configDef.define(
        TABLES_CDC_OPS_INSERT_PROP,
        ConfigDef.Type.LIST,
        TABLES_CDC_OP_INSERT_DEFAULT,
        Importance.MEDIUM,
        "The comma-separated values of the cdc operation field corresponding to INSERT");
    configDef.define(
        TABLES_CDC_OPS_UPDATE_PROP,
        ConfigDef.Type.LIST,
        TABLES_CDC_OP_UPDATE_DEFAULT,
        Importance.MEDIUM,
        "The comma-separated values of the cdc operation field corresponding to UPDATE");
    configDef.define(
        TABLES_CDC_OPS_DELETE_PROP,
        ConfigDef.Type.LIST,
        TABLES_CDC_OP_DELETE_DEFAULT,
        Importance.MEDIUM,
        "The comma-separated values of the cdc operation field corresponding to DELETE");
    configDef.define(
        TABLES_CDC_OPS_IGNORE_PROP,
        ConfigDef.Type.LIST,
        TABLES_CDC_OP_IGNORE_DEFAULT,
        Importance.MEDIUM,
        "The comma-separated values of the cdc operation field that should be ignored by connector");
  }

  private final Map<String, String> originalProps;
  private final Map<String, String> catalogProps;
  private final Map<String, String> hadoopProps;
  private final Map<String, String> kafkaProps;
  private final Map<String, String> autoCreateProps;
  private final Map<String, String> writeProps;
  private final Map<String, TableSinkConfig> tableConfigMap = Maps.newHashMap();
  private final RecordRoutingStrategy recordRoutingStrategy;
  private final Map<String, String> topicToTableMapping;
  private final String topicToTableMappingSource;
  private final JsonConverter jsonConverter;
  private final Collection<String> schemaVariantFieldPaths;
  private final Collection<String> schemaTimestampNsFieldPaths;

  public IcebergSinkConfig(Map<String, String> originalProps) {
    super(CONFIG_DEF, originalProps);
    this.originalProps = originalProps;

    this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
    this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);

    this.kafkaProps = Maps.newHashMap(loadWorkerProps());
    kafkaProps.putAll(PropertyUtil.propertiesWithPrefix(originalProps, KAFKA_PROP_PREFIX));

    this.autoCreateProps =
        PropertyUtil.propertiesWithPrefix(originalProps, AUTO_CREATE_PROP_PREFIX);
    this.writeProps = PropertyUtil.propertiesWithPrefix(originalProps, WRITE_PROP_PREFIX);

    this.jsonConverter = new JsonConverter();
    jsonConverter.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
            false,
            ConverterConfig.TYPE_CONFIG,
            ConverterType.VALUE.getName()));

    this.recordRoutingStrategy = resolveRoutingStrategy();
    TopicToTableMapping topicToTableMappingConfig = loadTopicToTableMapping();
    this.topicToTableMapping = topicToTableMappingConfig.mapping();
    this.topicToTableMappingSource = topicToTableMappingConfig.source();

    this.schemaVariantFieldPaths =
        parseVariantFieldPaths(getString(TABLES_SCHEMA_VARIANT_FIELDS_PROP));
    this.schemaTimestampNsFieldPaths =
        parseTimestampNsFieldPaths(getString(TABLES_SCHEMA_TIMESTAMP_NS_FIELDS_PROP));
    validate();
  }

  private void validate() {
    checkState(!catalogProps().isEmpty(), "Must specify Iceberg catalog properties");
    switch (recordRoutingStrategy) {
      case DYNAMIC_FIELD:
        checkState(tables() == null, "Cannot specify both static and dynamic table names");
        checkState(
            getTablesRouteField() != null,
            "Must specify a route field if using dynamic table names");
        break;
      case ALL_TABLES:
        checkState(tables() != null && !tables().isEmpty(), "Must specify table name(s)");
        break;
      case REGEX:
        checkState(tables() != null && !tables().isEmpty(), "Must specify table name(s)");
        checkState(
            getTablesRouteField() != null,
            "Must specify a route field if using regex routing strategy");
        break;
      case TOPIC_TO_TABLE:
        checkState(
            !topicToTableMapping.isEmpty(),
            "Must specify either iceberg.tables.topic-to-table-mapping or "
                + "iceberg.tables.topic-to-table-mapping-file for topic-to-table routing strategy");
        break;
      default:
        throw new ConfigException("Unsupported routing strategy: " + recordRoutingStrategy);
    }

    if (recordRoutingStrategy == RecordRoutingStrategy.TOPIC_TO_TABLE) {
      LOG.info(
          "Using routing strategy: {}, topic-to-table mapping source: {}, route count: {}",
          recordRoutingStrategy.value(),
          topicToTableMappingSource,
          topicToTableMapping.size());
    } else {
      LOG.info("Using routing strategy: {}", recordRoutingStrategy.value());
    }
  }

  private void checkState(boolean condition, String msg) {
    if (!condition) {
      throw new ConfigException(msg);
    }
  }

  public String connectorName() {
    return originalProps.get(NAME_PROP);
  }

  public String taskId() {
    return originalProps.get(TASK_ID);
  }

  public String transactionalSuffix() {
    // this is for internal use and is not part of the config definition...
    return originalProps.get(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
  }

  public Map<String, String> catalogProps() {
    return catalogProps;
  }

  public Map<String, String> hadoopProps() {
    return hadoopProps;
  }

  public Map<String, String> kafkaProps() {
    return kafkaProps;
  }

  public Map<String, String> autoCreateProps() {
    return autoCreateProps;
  }

  public Map<String, String> writeProps() {
    return writeProps;
  }

  public String catalogName() {
    return getString(CATALOG_NAME_PROP);
  }

  public List<String> tables() {
    return getList(TABLES_PROP);
  }

  public boolean dynamicTablesEnabled() {
    return getBoolean(TABLES_DYNAMIC_PROP);
  }

  public String tablesRouteField() {
    return getTablesRouteField();
  }

  public RecordRoutingStrategy routingStrategy() {
    return recordRoutingStrategy;
  }

  public Map<String, String> topicToTableMapping() {
    return topicToTableMapping;
  }

  public String tablesDefaultCommitBranch() {
    return getString(TABLES_DEFAULT_COMMIT_BRANCH);
  }

  public String tablesDefaultIdColumns() {
    return getString(TABLES_DEFAULT_ID_COLUMNS);
  }

  public String tablesDefaultPartitionBy() {
    return getString(TABLES_DEFAULT_PARTITION_BY);
  }

  public long keepAliveTimeoutInMs() {
    return getLong(COORDINATOR_EXECUTOR_KEEP_ALIVE_TIMEOUT_MS);
  }

  public TableSinkConfig tableConfig(String tableName) {
    return tableConfigMap.computeIfAbsent(
        tableName,
        notUsed -> {
          Map<String, String> tableConfig =
              PropertyUtil.propertiesWithPrefix(originalProps, TABLE_PROP_PREFIX + tableName + ".");

          String routeRegexStr = tableConfig.get(ROUTE_REGEX);
          Pattern routeRegex = routeRegexStr == null ? null : Pattern.compile(routeRegexStr);

          String idColumnsStr = tableConfig.getOrDefault(ID_COLUMNS, tablesDefaultIdColumns());
          List<String> idColumns = stringToList(idColumnsStr, ",");

          String partitionByStr =
              tableConfig.getOrDefault(PARTITION_BY, tablesDefaultPartitionBy());
          List<String> partitionBy = stringToList(partitionByStr, COMMA_NO_PARENS_REGEX);

          String commitBranch =
              tableConfig.getOrDefault(COMMIT_BRANCH, tablesDefaultCommitBranch());

          return new TableSinkConfig(routeRegex, idColumns, partitionBy, commitBranch);
        });
  }

  public String tablesCdcField() {
    return getString(TABLES_CDC_FIELD_PROP);
  }

  public List<String> tablesCdcOpsInsert() {
    return getList(TABLES_CDC_OPS_INSERT_PROP);
  }

  public List<String> tablesCdcOpsUpdate() {
    return getList(TABLES_CDC_OPS_UPDATE_PROP);
  }

  public List<String> tablesCdcOpsDelete() {
    return getList(TABLES_CDC_OPS_DELETE_PROP);
  }

  public List<String> tablesCdcIgnoredOps() {
    return getList(TABLES_CDC_OPS_IGNORE_PROP);
  }

  @VisibleForTesting
  static List<String> stringToList(String value, String regex) {
    if (value == null || value.isEmpty()) {
      return ImmutableList.of();
    }

    return Arrays.stream(value.split(regex)).map(String::trim).collect(Collectors.toList());
  }

  private RecordRoutingStrategy resolveRoutingStrategy() {
    String strategyValue = getString(ROUTING_STRATEGY_PROP);
    try {
      RecordRoutingStrategy routingStrategy = RecordRoutingStrategy.fromConfig(strategyValue);
      if (routingStrategy != null) {
        return routingStrategy;
      }

      if (dynamicTablesEnabled()) {
        return RecordRoutingStrategy.DYNAMIC_FIELD;
      }

      if (getTablesRouteField() != null) {
        return RecordRoutingStrategy.REGEX;
      }

      return RecordRoutingStrategy.ALL_TABLES;
    } catch (IllegalArgumentException e) {
      throw new ConfigException(ROUTING_STRATEGY_PROP, strategyValue, e.getMessage());
    }
  }

  private String getTablesRouteField() {
    String routeField = getString(TABLES_ROUTE_FIELD_PROP);
    return routeField == null || routeField.isBlank() ? null : routeField.trim();
  }

  private TopicToTableMapping loadTopicToTableMapping() {
    String mappingValue = getString(TABLES_TOPIC_TO_TABLE_MAPPING_PROP);
    String mappingFile = getString(TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP);

    boolean hasInlineMapping = hasText(mappingValue);
    boolean hasMappingFile = hasText(mappingFile);
    checkState(
        !(hasInlineMapping && hasMappingFile),
        "Cannot specify both "
            + TABLES_TOPIC_TO_TABLE_MAPPING_PROP
            + " and "
            + TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP);

    if (hasMappingFile) {
      return new TopicToTableMapping(
          parseTopicToTableMappingFile(mappingFile.trim()), TOPIC_TO_TABLE_MAPPING_SOURCE_FILE);
    }

    if (hasInlineMapping) {
      return new TopicToTableMapping(
          parseInlineTopicToTableMapping(mappingValue), TOPIC_TO_TABLE_MAPPING_SOURCE_INLINE);
    }

    return new TopicToTableMapping(ImmutableMap.of(), TOPIC_TO_TABLE_MAPPING_SOURCE_NONE);
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }

  private Map<String, String> parseInlineTopicToTableMapping(String mappingValue) {
    Map<String, String> result = new LinkedHashMap<>();
    for (String pair : Splitter.on(',').trimResults().split(mappingValue)) {
      checkState(!pair.isEmpty(), "Empty mapping pair is not allowed");

      String[] split = pair.split(":", 2);
      checkState(split.length == 2, "Invalid mapping pair: " + pair + ". Expected topic:table");

      String topic = split[0].trim();
      String table = split[1].trim();
      checkState(
          !topic.isEmpty() && !table.isEmpty(),
          "Topic and table must be non-empty in pair: " + pair);

      checkState(!result.containsKey(topic), "Duplicate topic mapping: " + topic);
      checkTableName(
          table, TABLES_TOPIC_TO_TABLE_MAPPING_PROP, "<redacted>", "mapping pair: " + pair);

      result.put(topic, table);
    }

    return ImmutableMap.copyOf(result);
  }

  private Map<String, String> parseTopicToTableMappingFile(String mappingFile) {
    Path mappingPath = Paths.get(mappingFile);
    checkMappingFile(mappingPath.isAbsolute(), mappingFile, "Mapping file path must be absolute");
    checkMappingFile(Files.exists(mappingPath), mappingFile, "Mapping file does not exist");
    checkMappingFile(Files.isRegularFile(mappingPath), mappingFile, "Mapping file must be a file");
    checkMappingFile(Files.isReadable(mappingPath), mappingFile, "Mapping file is not readable");

    JsonNode root;
    try (BufferedReader reader = Files.newBufferedReader(mappingPath, StandardCharsets.UTF_8)) {
      root = TOPIC_TO_TABLE_MAPPING_FILE_MAPPER.readTree(reader);
    } catch (IOException e) {
      throw new ConfigException(
          TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP,
          mappingFile,
          "Cannot read or parse JSON mapping file: " + e.getMessage());
    }

    checkMappingFile(
        root != null && root.isObject(), mappingFile, "Mapping file root must be an object");

    JsonNode version = root.get(TOPIC_TO_TABLE_MAPPING_FILE_VERSION_FIELD);
    checkMappingFile(
        version != null && version.isIntegralNumber(),
        mappingFile,
        "Mapping file must contain integer version: " + TOPIC_TO_TABLE_MAPPING_FILE_VERSION);
    checkMappingFile(
        version.intValue() == TOPIC_TO_TABLE_MAPPING_FILE_VERSION,
        mappingFile,
        "Unsupported mapping file version: "
            + version.asText()
            + ", expected: "
            + TOPIC_TO_TABLE_MAPPING_FILE_VERSION);

    JsonNode routes = root.get(TOPIC_TO_TABLE_MAPPING_FILE_ROUTES_FIELD);
    checkMappingFile(
        routes != null && routes.isObject(), mappingFile, "Mapping file routes must be an object");

    Map<String, String> result = new LinkedHashMap<>();
    Iterator<Entry<String, JsonNode>> routeEntries = routes.fields();
    while (routeEntries.hasNext()) {
      Entry<String, JsonNode> route = routeEntries.next();
      String topic = route.getKey();
      JsonNode tableNode = route.getValue();

      checkMappingFile(!topic.isBlank(), mappingFile, "Route topic must be non-empty");
      checkMappingFile(
          topic.equals(topic.trim()),
          mappingFile,
          "Route topic must not have leading or trailing whitespace: " + topic);
      checkMappingFile(
          tableNode != null && tableNode.isTextual(),
          mappingFile,
          "Route table for topic " + topic + " must be a string");

      String table = tableNode.textValue();
      checkMappingFile(
          !table.isBlank(), mappingFile, "Route table for topic " + topic + " must be non-empty");
      checkMappingFile(
          table.equals(table.trim()),
          mappingFile,
          "Route table for topic " + topic + " must not have leading or trailing whitespace");

      checkTableName(
          table, TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP, mappingFile, "topic: " + topic);
      result.put(topic, table);
    }

    checkMappingFile(!result.isEmpty(), mappingFile, "Mapping file routes must not be empty");
    return ImmutableMap.copyOf(result);
  }

  private void checkMappingFile(boolean condition, String mappingFile, String message) {
    if (!condition) {
      throw new ConfigException(TABLES_TOPIC_TO_TABLE_MAPPING_FILE_PROP, mappingFile, message);
    }
  }

  private void checkTableName(String table, String property, String value, String context) {
    try {
      TableIdentifier.parse(table);
    } catch (RuntimeException e) {
      throw new ConfigException(property, value, "Invalid table identifier for " + context);
    }
  }

  private static class TopicToTableMapping {
    private final Map<String, String> mapping;
    private final String source;

    TopicToTableMapping(Map<String, String> mapping, String source) {
      this.mapping = mapping;
      this.source = source;
    }

    Map<String, String> mapping() {
      return mapping;
    }

    String source() {
      return source;
    }
  }

  public String controlTopic() {
    return getString(CONTROL_TOPIC_PROP);
  }

  public String controlGroupIdPrefix() {
    return getString(CONTROL_GROUP_ID_PREFIX_PROP);
  }

  public String connectGroupId() {
    String result = getString(CONNECT_GROUP_ID_PROP);
    if (result != null) {
      return result;
    }

    String connectorName = connectorName();
    Preconditions.checkNotNull(connectorName, "Connector name cannot be null");
    return "connect-" + connectorName;
  }

  public boolean kerberosAuthentication() {
    return getBoolean(HDFS_AUTHENTICATION_KERBEROS_PROP);
  }

  public String connectHdfsPrincipal() {
    return getString(CONNECT_HDFS_PRINCIPAL_PROP);
  }

  public String connectHdfsKeytab() {
    return getString(CONNECT_HDFS_KEYTAB_PROP);
  }

  public long kerberosTicketRenewPeriodMs() {
    return getLong(KERBEROS_TICKET_RENEW_PERIOD_MS_PROP);
  }

  public int commitIntervalMs() {
    return getInt(COMMIT_INTERVAL_MS_PROP);
  }

  public int commitTimeoutMs() {
    return getInt(COMMIT_TIMEOUT_MS_PROP);
  }

  public boolean isUpsertMode() {
    return getBoolean(TABLES_UPSERT_MODE_ENABLED_PROP);
  }

  public int commitThreads() {
    return getInt(COMMIT_THREADS_PROP);
  }

  public String transactionalPrefix() {
    String result = getString(TRANSACTIONAL_PREFIX_PROP);
    if (result != null) {
      return result;
    }

    return "";
  }

  public String hadoopConfDir() {
    return getString(HADOOP_CONF_DIR_PROP);
  }

  public boolean autoCreateEnabled() {
    return getBoolean(TABLES_AUTO_CREATE_ENABLED_PROP);
  }

  public boolean evolveSchemaEnabled() {
    return getBoolean(TABLES_EVOLVE_SCHEMA_ENABLED_PROP);
  }

  public boolean schemaForceOptional() {
    return getBoolean(TABLES_SCHEMA_FORCE_OPTIONAL_PROP);
  }

  public boolean schemaCaseInsensitive() {
    return getBoolean(TABLES_SCHEMA_CASE_INSENSITIVE_PROP);
  }

  public JsonConverter jsonConverter() {
    return jsonConverter;
  }

  @VisibleForTesting
  static boolean checkClassName(String className) {
    return (className.matches(".*\\.ConnectDistributed.*")
        || className.matches(".*\\.ConnectStandalone.*"));
  }

  /**
   * This method attempts to load the Kafka Connect worker properties, which are not exposed to
   * connectors. It does this by parsing the Java command used to launch the worker, extracting the
   * name of the properties file, and then loading the file. <br>
   * The sink uses these properties, if available, when initializing its internal Kafka clients. By
   * doing this, Kafka-related properties only need to be set in the worker properties and do not
   * need to be duplicated in the sink config. <br>
   * If the worker properties cannot be loaded, then Kafka-related properties must be set via the
   * `iceberg.kafka.*` sink configs.
   *
   * @return The Kafka Connect worker properties
   */
  private Map<String, String> loadWorkerProps() {
    String javaCmd = System.getProperty("sun.java.command");
    if (javaCmd != null && !javaCmd.isEmpty()) {
      List<String> args = Splitter.on(' ').splitToList(javaCmd);
      if (args.size() > 1 && checkClassName(args.get(0))) {
        Properties result = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(args.get(1)))) {
          result.load(in);
          // sanity check that this is the config we want
          if (result.containsKey(BOOTSTRAP_SERVERS_PROP)) {
            return Maps.fromProperties(result);
          }
        } catch (Exception e) {
          // NO-OP
        }
      }
    }
    LOG.info(
        "Worker properties not loaded, using only {}* properties for Kafka clients",
        KAFKA_PROP_PREFIX);
    return ImmutableMap.of();
  }

  private static Collection<String> parseVariantFieldPaths(String value) {
    return parseFieldPaths(value, TABLES_SCHEMA_VARIANT_FIELDS_PROP, false);
  }

  private static Collection<String> parseTimestampNsFieldPaths(String value) {
    return parseFieldPaths(value, TABLES_SCHEMA_TIMESTAMP_NS_FIELDS_PROP, true);
  }

  private static Collection<String> parseFieldPaths(
      String value, String propName, boolean allowWildcard) {
    if (value == null || value.trim().isEmpty()) {
      return ImmutableList.of();
    }
    // split by comma, trim, drop empties, keep order, drop duplicates
    List<String> raw = stringToList(value, ",");
    Set<String> result = Sets.newLinkedHashSet();
    for (String path : raw) {
      if (path == null) {
        continue;
      }
      String trimmedPath = path.trim();
      if (trimmedPath.isEmpty()) {
        continue;
      }
      validateFieldPath(propName, trimmedPath, allowWildcard);
      result.add(trimmedPath);
    }
    return ImmutableList.copyOf(result);
  }

  private static void validateFieldPath(String propName, String path, boolean allowWildcard) {
    if (path.startsWith(".") || path.endsWith(".") || path.contains("..")) {
      throw new ConfigException(
          propName,
          path,
          "Invalid field path. Use dot-notation without empty segments, e.g. 'a.b.c'");
    }
    Iterable<String> parts = Splitter.on('.').split(path);
    for (String part : parts) {
      if (part.isEmpty()) {
        throw new ConfigException(
            propName, path, "Invalid field path. Empty segment is not allowed.");
      }
      if (part.contains("*")) {
        if (allowWildcard && "*".equals(path)) {
          continue;
        }
        throw new ConfigException(
            propName,
            path,
            "Invalid field path segment '"
                + part
                + "'. Wildcard '*' is only allowed as the entire value.");
      }
      if (!part.matches("[A-Za-z0-9_\\-]+")) {
        throw new ConfigException(
            propName, path, "Invalid field path segment '" + part + "'. Allowed: [A-Za-z0-9_-]");
      }
    }
  }

  public Collection<String> schemaVariantFieldPaths() {
    return schemaVariantFieldPaths;
  }

  public Collection<String> schemaTimestampNsFieldPaths() {
    return schemaTimestampNsFieldPaths;
  }

  public boolean evolveUnknownTypeEnabled() {
    return getBoolean(TABLES_EVOLVE_UNKNOWN_TYPE_ENABLED_PROP);
  }

  public boolean tableDefaultsEnabled() {
    return getBoolean(TABLES_DEFAULTS_ENABLED_PROP);
  }
}
