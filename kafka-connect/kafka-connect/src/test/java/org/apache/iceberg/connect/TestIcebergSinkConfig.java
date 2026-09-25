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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

public class TestIcebergSinkConfig {

  @Test
  public void testGetVersion() {
    String version = IcebergSinkConfig.version();
    assertThat(version).isNotNull();
  }

  @Test
  public void testInvalid() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics", "source-topic",
            "iceberg.catalog.type", "rest",
            "iceberg.tables", "db.landing",
            "iceberg.tables.dynamic-enabled", "true");
    assertThatThrownBy(() -> new IcebergSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Cannot specify both static and dynamic table names");
  }

  @Test
  public void testGetDefault() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "source-topic",
            "iceberg.tables", "db.landing");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.commitIntervalMs()).isEqualTo(300_000);
    assertThat(config.schemaTimestampNsFieldPaths()).isEmpty();
  }

  @Test
  public void testMetadataPipelineFqnQuotesDottedSegments() {
    Map<String, String> props = Maps.newHashMap(propsWith("name", "orders.connector"));
    props.put("iceberg.metadata.pipeline-service-name", "kafka.connect");

    assertThat(new IcebergSinkConfig(props).metadataPipelineFqn())
        .isEqualTo("\"kafka.connect\".\"orders.connector\"");
  }

  @Test
  public void testSchemaTimestampNsFieldPaths() {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            propsWith(
                "iceberg.tables.schema-timestamp-ns-fields",
                " event_time, after.event_time,*,payload.event_time,event_time "));

    assertThat(config.schemaTimestampNsFieldPaths())
        .containsExactly("event_time", "after.event_time", "*", "payload.event_time");
  }

  @Test
  public void testSchemaTimestampNsFieldPathsRejectInvalidPatterns() {
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    propsWith("iceberg.tables.schema-timestamp-ns-fields", "payload.e*")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.schema-timestamp-ns-fields");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    propsWith("iceberg.tables.schema-timestamp-ns-fields", "payload.*")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.schema-timestamp-ns-fields");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    propsWith("iceberg.tables.schema-timestamp-ns-fields", "*.event_time")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.schema-timestamp-ns-fields");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    propsWith("iceberg.tables.schema-timestamp-ns-fields", ".event_time")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.schema-timestamp-ns-fields");
  }

  @Test
  public void testSchemaVariantFieldPathsRejectWildcard() {
    assertThatThrownBy(
            () -> new IcebergSinkConfig(propsWith("iceberg.tables.schema-variant-fields", "*")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.schema-variant-fields");
  }

  @Test
  public void testTopicToTableInlineMapping() {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            topicToTableProps(
                "iceberg.tables.topic-to-table-mapping",
                "sales-topic:prod.sales,logs-topic:prod.logs"));

    assertThat(config.topicToTableMapping())
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of("sales-topic", "prod.sales", "logs-topic", "prod.logs"));
  }

  @Test
  public void testTopicToTableFileMapping(@TempDir Path tempDir) throws IOException {
    Path mappingFile =
        writeMappingFile(
            tempDir,
            "{\"version\":1,\"routes\":{\"sales-topic\":\"prod.sales\",\"logs-topic\":\"prod.logs\"}}");

    IcebergSinkConfig config =
        new IcebergSinkConfig(
            topicToTableProps(
                "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString()));

    assertThat(config.topicToTableMapping())
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of("sales-topic", "prod.sales", "logs-topic", "prod.logs"));
  }

  @Test
  public void testTopicToTableMappingCannotUseInlineAndFile(@TempDir Path tempDir)
      throws IOException {
    Path mappingFile =
        writeMappingFile(tempDir, "{\"version\":1,\"routes\":{\"sales-topic\":\"prod.sales\"}}");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping",
                        "sales-topic:prod.sales",
                        "iceberg.tables.topic-to-table-mapping-file",
                        mappingFile.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Cannot specify both iceberg.tables.topic-to-table-mapping")
        .hasMessageContaining("iceberg.tables.topic-to-table-mapping-file");
  }

  @Test
  public void testTopicToTableRequiresMappingSource() {
    assertThatThrownBy(() -> new IcebergSinkConfig(topicToTableProps()))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Must specify either iceberg.tables.topic-to-table-mapping")
        .hasMessageContaining("iceberg.tables.topic-to-table-mapping-file");
  }

  @Test
  public void testTopicToTableFileMappingRejectsMissingFile(@TempDir Path tempDir) {
    Path mappingFile = tempDir.resolve("missing-routes.json");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Mapping file does not exist");
  }

  @Test
  public void testTopicToTableFileMappingRejectsRelativePath() {
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps("iceberg.tables.topic-to-table-mapping-file", "routes.json")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Mapping file path must be absolute");
  }

  @Test
  public void testTopicToTableFileMappingRejectsDirectory(@TempDir Path tempDir) {
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping-file", tempDir.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Mapping file must be a file");
  }

  @Test
  public void testTopicToTableFileMappingRejectsUnreadableFile(@TempDir Path tempDir)
      throws IOException {
    Path mappingFile =
        writeMappingFile(tempDir, "{\"version\":1,\"routes\":{\"sales-topic\":\"prod.sales\"}}");
    boolean permissionsChanged = mappingFile.toFile().setReadable(false, false);

    try {
      assumeTrue(permissionsChanged && !Files.isReadable(mappingFile));

      assertThatThrownBy(
              () ->
                  new IcebergSinkConfig(
                      topicToTableProps(
                          "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString())))
          .isInstanceOf(ConfigException.class)
          .hasMessageContaining("Mapping file is not readable");
    } finally {
      mappingFile.toFile().setReadable(true, false);
    }
  }

  @Test
  public void testTopicToTableFileMappingRejectsMalformedJson(@TempDir Path tempDir)
      throws IOException {
    Path mappingFile = writeMappingFile(tempDir, "{");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Cannot read or parse JSON mapping file");
  }

  @Test
  public void testTopicToTableFileMappingRejectsTrailingTokens(@TempDir Path tempDir)
      throws IOException {
    Path mappingFile =
        writeMappingFile(
            tempDir,
            "{\"version\":1,\"routes\":{\"sales-topic\":\"prod.sales\"}}"
                + "{\"version\":1,\"routes\":{\"logs-topic\":\"prod.logs\"}}");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Cannot read or parse JSON mapping file");
  }

  @Test
  public void testTopicToTableFileMappingRejectsInvalidStructure(@TempDir Path tempDir)
      throws IOException {
    assertInvalidMappingFile(tempDir, "[]", "Mapping file root must be an object");
    assertInvalidMappingFile(
        tempDir,
        "{\"routes\":{\"sales-topic\":\"prod.sales\"}}",
        "Mapping file must contain integer version: 1");
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":2,\"routes\":{\"sales-topic\":\"prod.sales\"}}",
        "Unsupported mapping file version: 2, expected: 1");
    assertInvalidMappingFile(tempDir, "{\"version\":1}", "Mapping file routes must be an object");
    assertInvalidMappingFile(
        tempDir, "{\"version\":1,\"routes\":[]}", "Mapping file routes must be an object");
    assertInvalidMappingFile(
        tempDir, "{\"version\":1,\"routes\":{}}", "Mapping file routes must not be empty");
  }

  @Test
  public void testTopicToTableFileMappingRejectsInvalidRoutes(@TempDir Path tempDir)
      throws IOException {
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":1,\"routes\":{\"sales-topic\":1}}",
        "Route table for topic sales-topic must be a string");
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":1,\"routes\":{\"\":\"prod.sales\"}}",
        "Route topic must be non-empty");
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":1,\"routes\":{\" sales-topic\":\"prod.sales\"}}",
        "Route topic must not have leading or trailing whitespace:  sales-topic");
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":1,\"routes\":{\"sales-topic\":\"\"}}",
        "Route table for topic sales-topic must be non-empty");
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":1,\"routes\":{\"sales-topic\":\" prod.sales\"}}",
        "Route table for topic sales-topic must not have leading or trailing whitespace");
    assertInvalidMappingFile(
        tempDir,
        "{\"version\":1,\"routes\":{\"sales-topic\":\"prod.\"}}",
        "Invalid table identifier for topic: sales-topic");
  }

  @Test
  public void testTopicToTableFileMappingRejectsDuplicateTopics(@TempDir Path tempDir)
      throws IOException {
    Path mappingFile =
        writeMappingFile(
            tempDir,
            "{\"version\":1,\"routes\":{\"sales-topic\":\"prod.sales\",\"sales-topic\":\"prod.logs\"}}");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Cannot read or parse JSON mapping file")
        .hasMessageContaining("Duplicate field 'sales-topic'");
  }

  @Test
  public void testStringToList() {
    List<String> result = IcebergSinkConfig.stringToList(null, ",");
    assertThat(result).isEmpty();

    result = IcebergSinkConfig.stringToList("", ",");
    assertThat(result).isEmpty();

    result = IcebergSinkConfig.stringToList("one ", ",");
    assertThat(result).contains("one");

    result = IcebergSinkConfig.stringToList("one, two", ",");
    assertThat(result).contains("one", "two");

    result = IcebergSinkConfig.stringToList("bucket(id, 4)", ",");
    assertThat(result).contains("bucket(id", "4)");

    result =
        IcebergSinkConfig.stringToList("bucket(id, 4)", IcebergSinkConfig.COMMA_NO_PARENS_REGEX);
    assertThat(result).contains("bucket(id, 4)");

    result =
        IcebergSinkConfig.stringToList(
            "bucket(id, 4), type", IcebergSinkConfig.COMMA_NO_PARENS_REGEX);
    assertThat(result).contains("bucket(id, 4)", "type");
  }

  @Test
  public void testStringWithParensToList() {}

  @Test
  public void testCheckClassName() {
    Boolean result =
        IcebergSinkConfig.checkClassName("org.apache.kafka.connect.cli.ConnectDistributed");
    assertThat(result).isTrue();

    result = IcebergSinkConfig.checkClassName("org.apache.kafka.connect.cli.ConnectStandalone");
    assertThat(result).isTrue();

    result = IcebergSinkConfig.checkClassName("some.other.package.ConnectDistributed");
    assertThat(result).isTrue();

    result = IcebergSinkConfig.checkClassName("some.other.package.ConnectStandalone");
    assertThat(result).isTrue();

    result = IcebergSinkConfig.checkClassName("some.package.ConnectDistributedWrapper");
    assertThat(result).isTrue();

    result = IcebergSinkConfig.checkClassName("org.apache.kafka.clients.producer.KafkaProducer");
    assertThat(result).isFalse();
  }

  private static void assertInvalidMappingFile(
      Path tempDir, String mappingJson, String expectedMessage) throws IOException {
    Path mappingFile = writeMappingFile(tempDir, mappingJson);

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    topicToTableProps(
                        "iceberg.tables.topic-to-table-mapping-file", mappingFile.toString())))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(expectedMessage);
  }

  private static Path writeMappingFile(Path tempDir, String mappingJson) throws IOException {
    Path mappingFile = Files.createTempFile(tempDir, "topic-table-routes", ".json");
    Files.writeString(mappingFile, mappingJson);
    return mappingFile;
  }

  private static Map<String, String> topicToTableProps(String... keyValues) {
    Map<String, String> props = Maps.newHashMap();
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", "source-topic");
    props.put("routing.strategy", "topic-to-table");

    for (int index = 0; index < keyValues.length; index += 2) {
      props.put(keyValues[index], keyValues[index + 1]);
    }

    return props;
  }

  private static Map<String, String> propsWith(String key, String value) {
    return ImmutableMap.<String, String>builder()
        .put("iceberg.catalog.type", "rest")
        .put("topics", "source-topic")
        .put("iceberg.tables", "db.landing")
        .put(key, value)
        .build();
  }

  private static Map<String, String> baseProps(String... extra) {
    Map<String, String> props = Maps.newHashMap();
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", "source-topic");
    props.put("iceberg.tables", "db.landing");
    for (int i = 0; i < extra.length; i += 2) {
      props.put(extra[i], extra[i + 1]);
    }
    return props;
  }

  /**
   * Copy-on-write props plus the change stream it requires: without a CDC field or upsert mode the
   * configuration is refused outright, so every copy-on-write case has to carry one.
   */
  private Map<String, String> copyOnWriteProps(String... extra) {
    Map<String, String> props =
        baseProps(
            "iceberg.tables.row-level-mode", "copy-on-write",
            "iceberg.tables.upsert-mode-enabled", "true");
    for (int i = 0; i < extra.length; i += 2) {
      props.put(extra[i], extra[i + 1]);
    }
    return props;
  }

  @Test
  public void testRowLevelModeDefaultsToMergeOnRead() {
    IcebergSinkConfig config = new IcebergSinkConfig(baseProps());
    assertThat(config.rowLevelMode()).isEqualTo(RowLevelOperationMode.MERGE_ON_READ);
    assertThat(config.isCopyOnWriteMode()).isFalse();
  }

  @Test
  public void testRowLevelModeCopyOnWrite() {
    IcebergSinkConfig config = new IcebergSinkConfig(copyOnWriteProps());
    assertThat(config.rowLevelMode()).isEqualTo(RowLevelOperationMode.COPY_ON_WRITE);
    assertThat(config.isCopyOnWriteMode()).isTrue();
  }

  @Test
  public void testRowLevelModeUnknownValueFailsRatherThanFallingBack() {
    // rejected while the configuration is validated, not silently treated as merge-on-read
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(baseProps("iceberg.tables.row-level-mode", "merge-on-write")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("merge-on-write")
        .hasMessageContaining("iceberg.tables.row-level-mode");
  }

  @Test
  public void testStagingOrphanTtlWithinTheCommitIntervalPlusTimeoutIsRejected() {
    // a staged change file is referenced by nothing between the moment a task writes it and the
    // moment the coordinator's buffer learns of it (up to one commit interval plus the time the
    // coordinator waits for worker responses). Sweeping inside that window deletes change data that
    // has not been applied yet. A file closed at the start of a 5 min cycle and answered 25 s into
    // the timeout is 5 min 25 s old before it is referenced, so a 5 min 10 s TTL is not enough
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.control.commit.interval-ms", "300000",
                        "iceberg.control.commit.timeout-ms", "30000",
                        "iceberg.tables.copy-on-write.staging-orphan-ttl-ms", "310000")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.copy-on-write.staging-orphan-ttl-ms")
        .hasMessageContaining("iceberg.control.commit.interval-ms")
        .hasMessageContaining("iceberg.control.commit.timeout-ms");

    // the bound itself is refused, one past it is accepted
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.control.commit.interval-ms", "300000",
                        "iceberg.control.commit.timeout-ms", "30000",
                        "iceberg.tables.copy-on-write.staging-orphan-ttl-ms", "330000")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.control.commit.timeout-ms");
    assertThat(
            new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.control.commit.interval-ms", "300000",
                        "iceberg.control.commit.timeout-ms", "30000",
                        "iceberg.tables.copy-on-write.staging-orphan-ttl-ms", "330001"))
                .copyOnWriteStagingOrphanTtlMs())
        .isEqualTo(330_001L);

    // both settings are ints: their sum must not wrap around into a negative bound
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.control.commit.interval-ms", String.valueOf(Integer.MAX_VALUE),
                        "iceberg.control.commit.timeout-ms", String.valueOf(Integer.MAX_VALUE),
                        "iceberg.tables.copy-on-write.staging-orphan-ttl-ms",
                            String.valueOf(2L * Integer.MAX_VALUE))))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.copy-on-write.staging-orphan-ttl-ms");

    // merge-on-read does not sweep staging and is not checked
    assertThat(
            new IcebergSinkConfig(
                    baseProps(
                        "iceberg.control.commit.interval-ms", "300000",
                        "iceberg.tables.copy-on-write.staging-orphan-ttl-ms", "1"))
                .isCopyOnWriteMode())
        .isFalse();

    // the defaults leave two orders of magnitude of headroom
    IcebergSinkConfig defaults = new IcebergSinkConfig(copyOnWriteProps());
    assertThat(defaults.copyOnWriteStagingOrphanTtlMs())
        .isGreaterThan((long) defaults.commitIntervalMs() + defaults.commitTimeoutMs());
  }

  @Test
  public void testCopyOnWriteWithoutAChangeStreamIsRejected() {
    // without a CDC field or upsert mode every record is a plain append, the writer reports
    // DataWritten, and the copy-on-write committer does not consume it: the records would be
    // acknowledged and never land. Refused rather than accepted and silently dropped
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(baseProps("iceberg.tables.row-level-mode", "copy-on-write")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.cdc-field")
        .hasMessageContaining("iceberg.tables.upsert-mode-enabled");

    // either one is enough
    assertThat(
            new IcebergSinkConfig(
                    baseProps(
                        "iceberg.tables.row-level-mode", "copy-on-write",
                        "iceberg.tables.cdc-field", "op"))
                .isCopyOnWriteMode())
        .isTrue();
    assertThat(new IcebergSinkConfig(copyOnWriteProps()).isCopyOnWriteMode()).isTrue();

    // merge-on-read is untouched: a plain append connector is its normal shape
    assertThat(new IcebergSinkConfig(baseProps()).isCopyOnWriteMode()).isFalse();
  }

  @Test
  public void testCopyOnWriteWithAnEmptyCdcFieldIsRejected() {
    // an empty cdc-field (a templated value left blank) names no field, so there is still no change
    // stream: refused exactly like an unset one
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    baseProps(
                        "iceberg.tables.row-level-mode", "copy-on-write",
                        "iceberg.tables.cdc-field", "")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.cdc-field")
        .hasMessageContaining("iceberg.tables.upsert-mode-enabled");

    // ConfigDef trims string values, so a blank one is the same as an empty one
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    baseProps(
                        "iceberg.tables.row-level-mode", "copy-on-write",
                        "iceberg.tables.cdc-field", "  ")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.cdc-field");

    // upsert mode alone is still enough
    assertThat(
            new IcebergSinkConfig(copyOnWriteProps("iceberg.tables.cdc-field", ""))
                .isCopyOnWriteMode())
        .isTrue();

    // merge-on-read is untouched
    assertThat(new IcebergSinkConfig(baseProps("iceberg.tables.cdc-field", "")).tablesCdcField())
        .isEmpty();
  }

  @Test
  public void testCopyOnWriteRejectsDuplicateTableNames() {
    // a name listed twice makes the router hand the same record to one writer twice, so one
    // (_topic, _partition, _offset) triple lands in the change-set twice; the normalizer happens to
    // fold identical copies back into one, but that is a coincidence of the copies being equal,
    // not a check, and the misconfiguration passes silently
    assertThatThrownBy(() -> new IcebergSinkConfig(copyOnWriteProps("iceberg.tables", "db.t,db.t")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables")
        .hasMessageContaining("db.t");

    // merge-on-read is untouched: duplicates there are pre-existing upstream behavior
    assertThat(new IcebergSinkConfig(baseProps("iceberg.tables", "db.t,db.t")).tables())
        .containsExactly("db.t", "db.t");
  }

  @Test
  public void testCopyOnWriteQuotasMustBePositive() {
    // a zero record quota admits no key, so every slice comes back empty and truncated and the
    // drain never advances; the other quotas are equally meaningless at zero
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps("iceberg.tables.copy-on-write.max-slice-keys", "0")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.copy-on-write.max-slice-keys");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps("iceberg.tables.copy-on-write.max-rewrite-bytes", "0")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.copy-on-write.max-rewrite-bytes");

    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.tables.copy-on-write.rewrite-response-chunk-files", "0")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.copy-on-write.rewrite-response-chunk-files");

    // retries are the one that may legitimately be zero: no retry at all is a choice
    assertThat(
            new IcebergSinkConfig(
                    copyOnWriteProps("iceberg.tables.copy-on-write.commit-retries", "0"))
                .copyOnWriteCommitRetries())
        .isZero();
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps("iceberg.tables.copy-on-write.commit-retries", "-1")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("iceberg.tables.copy-on-write.commit-retries");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "iceberg.tables.copy-on-write.pruning.max-in-cardinality",
        "iceberg.tables.copy-on-write.rewrite-threads",
        "iceberg.tables.copy-on-write.rewrite-timeout-ms",
        "iceberg.tables.copy-on-write.staging-orphan-cleanup-interval-ms",
        "iceberg.tables.copy-on-write.staging-orphan-ttl-ms"
      })
  public void testCopyOnWriteTuningSettingsMustBePositive(String property) {
    // zero does not switch any of these off: a zero rewrite timeout cancels every slice on the next
    // tick and zero rewrite threads leave the task nothing to rewrite with, so the drain never
    // advances; refused by ConfigDef rather than accepted
    assertThatThrownBy(() -> new IcebergSinkConfig(copyOnWriteProps(property, "0")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(property);

    // the bound is ConfigDef's, so it holds in merge-on-read too; there it is the only check, while
    // copy-on-write would also refuse a zero orphan TTL through its startup validation
    assertThatThrownBy(() -> new IcebergSinkConfig(baseProps(property, "0")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(property);
  }

  @ParameterizedTest
  @CsvSource({
    "iceberg.tables.copy-on-write.max-slice-keys, "
        + "iceberg.tables.copy-on-write.max-change-set-records",
    "iceberg.tables.copy-on-write.staging-orphan-cleanup-interval-ms, "
        + "iceberg.tables.copy-on-write.staging-sweep-interval-ms"
  })
  public void testCopyOnWriteRenamedPropertiesAreReadOnlyUnderTheirNewNames(
      String property, String formerName) {
    // the feature was never released, so the former name is not an alias: it is an unknown
    // property ConfigDef ignores, and its value, however invalid, takes no effect
    assertThatThrownBy(() -> new IcebergSinkConfig(copyOnWriteProps(property, "0")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(property);

    assertThatCode(() -> new IcebergSinkConfig(copyOnWriteProps(formerName, "0")))
        .doesNotThrowAnyException();
  }

  @Test
  public void testCopyOnWriteRejectsConflictingAutoCreateProps() {
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.tables.auto-create-props.write.update.mode", "merge-on-read")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("write.update.mode");
  }

  @Test
  public void testCopyOnWriteAcceptsMatchingAutoCreateProps() {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            copyOnWriteProps(
                "iceberg.tables.auto-create-props.write.delete.mode", "copy-on-write"));
    assertThat(config.isCopyOnWriteMode()).isTrue();
  }

  @Test
  public void testMergeOnReadIgnoresAutoCreateProps() {
    // the validation is copy-on-write only, so existing installations are untouched
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            baseProps("iceberg.tables.auto-create-props.write.update.mode", "merge-on-read"));
    assertThat(config.isCopyOnWriteMode()).isFalse();
  }

  @Test
  public void testCopyOnWriteRejectsConflictingCatalogTableDefaultProps() {
    // table-default.* is applied by the catalog underneath auto-create-props, so a mismatch here
    // is invisible to a check that only reads auto-create-props: the table gets created with it,
    // then fails the per-table check on the very next write
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.catalog.table-default.write.delete.mode", "merge-on-read")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("write.delete.mode");
  }

  @Test
  public void testCopyOnWriteRejectsConflictingCatalogTableOverrideProps() {
    // table-override.* is applied by the catalog on top of auto-create-props, so it wins even
    // when auto-create-props explicitly declares copy-on-write
    assertThatThrownBy(
            () ->
                new IcebergSinkConfig(
                    copyOnWriteProps(
                        "iceberg.tables.auto-create-props.write.merge.mode", "copy-on-write",
                        "iceberg.catalog.table-override.write.merge.mode", "merge-on-read")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("write.merge.mode");
  }

  @Test
  public void testCopyOnWriteTableOverridePropsWinsOverConflictingTableDefault() {
    // only the final resolved value matters, not each source individually: table-override
    // shadows a conflicting table-default, so the combination is accepted
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            copyOnWriteProps(
                "iceberg.catalog.table-default.write.update.mode", "merge-on-read",
                "iceberg.catalog.table-override.write.update.mode", "copy-on-write"));
    assertThat(config.isCopyOnWriteMode()).isTrue();
  }

  @Test
  public void testCopyOnWriteStagingLocationWithFileSchemeWarnsOnce() {
    // a file: staging location is only reachable from the node that wrote it; on a multi-node
    // Connect cluster the coordinator (or a worker on another node) cannot see it, so the table
    // stalls, and a lost node loses staged changes whose offsets have already been released. The
    // warning comes from creating the config (that is task startup) and construction still
    // succeeds: the scheme is a warning, not a refusal
    Logger log = mock(Logger.class);
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            copyOnWriteProps(
                "iceberg.tables.copy-on-write.staging-location", "file:///data/staging"),
            log);
    assertThat(config.copyOnWriteStagingLocation()).isEqualTo("file:///data/staging");
    verify(log)
        .warn(
            anyString(),
            eq("iceberg.tables.copy-on-write.staging-location"),
            eq("file:///data/staging"));
    verifyNoMoreInteractions(log);
  }

  @Test
  public void testCopyOnWriteStagingLocationWithRemoteSchemeDoesNotWarn() {
    Logger log = mock(Logger.class);
    new IcebergSinkConfig(
        copyOnWriteProps("iceberg.tables.copy-on-write.staging-location", "s3://bucket/staging"),
        log);
    verifyNoInteractions(log);
  }

  @Test
  public void testMergeOnReadDoesNotWarnOnFileStagingLocation() {
    // merge-on-read never reads the staging location, so a file: scheme there is not a hazard
    Logger log = mock(Logger.class);
    new IcebergSinkConfig(
        baseProps("iceberg.tables.copy-on-write.staging-location", "file:///data/staging"), log);
    verifyNoInteractions(log);
  }

  @Test
  public void testCopyOnWriteDefaults() {
    IcebergSinkConfig config = new IcebergSinkConfig(copyOnWriteProps());
    assertThat(config.copyOnWriteMaxSliceKeys()).isEqualTo(200_000L);
    assertThat(config.copyOnWriteMaxRewriteBytes()).isEqualTo(10L * 1024 * 1024 * 1024);
    assertThat(config.copyOnWritePruningMaxInCardinality()).isEqualTo(200);
    assertThat(config.copyOnWriteCommitRetries()).isEqualTo(2);
    assertThat(config.copyOnWriteRewriteTimeoutMs()).isEqualTo(120_000L);
    assertThat(config.copyOnWriteRewriteResponseChunkFiles()).isEqualTo(500);
    assertThat(config.controlMessageMaxBytes()).isEqualTo((int) (1024 * 1024 * 0.8));
    assertThat(config.copyOnWriteStagingOrphanCleanupIntervalMs()).isEqualTo(3_600_000L);
    assertThat(config.copyOnWriteStagingOrphanTtlMs()).isEqualTo(86_400_000L);
    assertThat(config.copyOnWriteRewriteThreads())
        .isEqualTo(Math.min(2, Runtime.getRuntime().availableProcessors()));
    assertThat(config.copyOnWriteStagingLocation()).isNull();
  }

  @Test
  public void testRewriteThreadsDescriptionSaysItIsPerTask() {
    // the default is sized for a shared node: an operator who raises it must know every task on the
    // node gets its own pools, and all assignments of a task share them
    String description = documentation("iceberg.tables.copy-on-write.rewrite-threads");
    assertThat(description).contains("per task").contains("multiply");
  }

  @Test
  public void testRewriteTimeoutDescriptionDoesNotTieItToTheCommitInterval() {
    // the description is what an operator sees in Connect's REST validation and UI: sized below the
    // commit interval, a timeout shorter than a slice rewrite cancels and retries the slice forever
    String description = documentation("iceberg.tables.copy-on-write.rewrite-timeout-ms");
    assertThat(description).doesNotContain("commit.interval-ms").contains("well above");
    // the task reads it too: an assignment that waited in its queue longer is not started
    assertThat(description).contains("queue");
  }

  @Test
  public void testCommitRetriesDescriptionCoversFailedRewrites() {
    // without concurrent writers the setting still retries a slice a task failed to rewrite;
    // described as conflict-only, an operator sets it to 0 and every such failure costs a cycle
    String description = documentation("iceberg.tables.copy-on-write.commit-retries");
    assertThat(description).contains("concurrent writer").contains("fail").contains("next commit");
  }

  private static String documentation(String property) {
    ConfigDef.ConfigKey key = IcebergSinkConfig.CONFIG_DEF.configKeys().get(property);
    assertThat(key).as("config key %s", property).isNotNull();
    return key.documentation;
  }
}
