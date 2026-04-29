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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    Map<String, String> props = new HashMap<>();
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", "source-topic");
    props.put("routing.strategy", "topic-to-table");

    for (int index = 0; index < keyValues.length; index += 2) {
      props.put(keyValues[index], keyValues[index + 1]);
    }

    return props;
  }
}
