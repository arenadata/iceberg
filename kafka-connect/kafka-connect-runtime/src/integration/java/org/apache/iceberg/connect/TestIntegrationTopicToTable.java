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

import java.time.Instant;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationTopicToTable extends IntegrationTestBase {

  private static final String TEST_TABLE1 = "topic_tbl1";
  private static final String TEST_TABLE2 = "topic_tbl2";
  private static final TableIdentifier TABLE_IDENTIFIER1 = TableIdentifier.of(TEST_DB, TEST_TABLE1);
  private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of(TEST_DB, TEST_TABLE2);

  private String secondTopic;
  private String unmappedTopic;

  @BeforeEach
  public void before() {
    secondTopic = testTopic() + "-second";
    unmappedTopic = testTopic() + "-unmapped";
    createTopic(secondTopic, TEST_TOPIC_PARTITIONS);
    createTopic(unmappedTopic, TEST_TOPIC_PARTITIONS);
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
        .config(
            "iceberg.tables.topic-to-table-mapping",
            String.format(
                "%s:%s.%s,%s:%s.%s",
                testTopic(), TEST_DB, TEST_TABLE1, secondTopic, TEST_DB, TEST_TABLE2));
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
  void dropTables() {
    deleteTopic(secondTopic);
    deleteTopic(unmappedTopic);
    catalog().dropTable(TABLE_IDENTIFIER1);
    catalog().dropTable(TABLE_IDENTIFIER2);
  }
}
