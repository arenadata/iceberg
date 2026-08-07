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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationTopicToTable extends IntegrationTestBase {

  private static final String TEST_TABLE1 = "topic_tbl1";
  private static final String TEST_TABLE2 = "topic_tbl2";
  private static final TableIdentifier TABLE_IDENTIFIER1 = TableIdentifier.of(TEST_DB, TEST_TABLE1);
  private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of(TEST_DB, TEST_TABLE2);
  private static final Path CONNECT_INSTALL_HOST_DIR = Paths.get("build", "install");
  private static final String CONNECT_INSTALL_CONTAINER_DIR = "/test/kafka-connect";

  private String secondTopic;
  private String unmappedTopic;
  private Path topicToTableMappingHostFile;
  private String topicToTableMappingContainerFile;

  @BeforeEach
  public void before() throws IOException {
    secondTopic = testTopic() + "-second";
    unmappedTopic = testTopic() + "-unmapped";
    String mappingFileName = "topic-table-routes-" + UUID.randomUUID() + ".json";
    Files.createDirectories(CONNECT_INSTALL_HOST_DIR);
    topicToTableMappingHostFile = CONNECT_INSTALL_HOST_DIR.resolve(mappingFileName);
    topicToTableMappingContainerFile = CONNECT_INSTALL_CONTAINER_DIR + "/" + mappingFileName;
    Files.writeString(
        topicToTableMappingHostFile,
        String.format(
            "{\"version\":1,\"routes\":{\"%s\":\"%s.%s\",\"%s\":\"%s.%s\"}}",
            testTopic(), TEST_DB, TEST_TABLE1, secondTopic, TEST_DB, TEST_TABLE2),
        StandardCharsets.UTF_8);
    createTopic(secondTopic, TEST_TOPIC_PARTITIONS);
    createTopic(unmappedTopic, TEST_TOPIC_PARTITIONS);
  }

  @AfterEach
  public void after() throws IOException {
    if (topicToTableMappingHostFile != null) {
      Files.deleteIfExists(topicToTableMappingHostFile);
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testIcebergSink(String branch) {
    catalog().createTable(TABLE_IDENTIFIER1, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);
    catalog().createTable(TABLE_IDENTIFIER2, TestEvent.TEST_SCHEMA);

    boolean useSchema = branch == null;
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER1, TABLE_IDENTIFIER2));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER1, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER1, branch);

    files = dataFiles(TABLE_IDENTIFIER2, branch);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).recordCount()).isEqualTo(1);
    assertSnapshotProps(TABLE_IDENTIFIER2, branch);
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("topics", String.format("%s,%s,%s", testTopic(), secondTopic, unmappedTopic))
        .config("routing.strategy", "topic-to-table")
        .config("iceberg.tables.topic-to-table-mapping-file", topicToTableMappingContainerFile);
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    TestEvent event1 = new TestEvent(1, "type1", Instant.now(), "test1");
    TestEvent event2 = new TestEvent(2, "type2", Instant.now(), "test2");
    TestEvent event3 = new TestEvent(3, "type3", Instant.now(), "ignored");

    send(testTopic(), event1, useSchema);
    send(secondTopic, event2, useSchema);
    send(unmappedTopic, event3, useSchema);
  }

  @Override
  protected void dropTables() {
    deleteTopic(secondTopic);
    deleteTopic(unmappedTopic);
    catalog().dropTable(TABLE_IDENTIFIER1);
    catalog().dropTable(TABLE_IDENTIFIER2);
  }
}
