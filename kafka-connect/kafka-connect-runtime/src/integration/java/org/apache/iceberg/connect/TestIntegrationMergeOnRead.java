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
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationMergeOnRead extends IntegrationTestBaseRowLevel {

  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);
  private static final Instant NOW = Instant.ofEpochMilli(System.currentTimeMillis());
  private static final boolean USE_SCHEMA = true;

  @ParameterizedTest
  @MethodSource("useSchemaAndBranch")
  public void testMergeOnReadIsDefault(boolean useSchema, String branch) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(useSchema, null, branch, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    CdcTestEvent third = CdcTestEvent.insert(3, "type3", NOW, "third");
    List<CdcTestEvent> inserts = List.of(first, second, third);
    sendAndAwait(TABLE_IDENTIFIER, branch, useSchema, inserts, inserts);

    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(
        TABLE_IDENTIFIER,
        branch,
        useSchema,
        List.of(updated, CdcTestEvent.delete(second)),
        List.of(updated, third));

    assertThat(deleteFiles(TABLE_IDENTIFIER, branch)).isNotEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {COPY_ON_WRITE, MERGE_ON_READ})
  public void testSameResultAsCopyOnWrite(String rowLevelMode) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(USE_SCHEMA, rowLevelMode, null, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    CdcTestEvent third = CdcTestEvent.insert(3, "type3", NOW, "third");
    CdcTestEvent fourth = CdcTestEvent.insert(4, "type4", NOW, "fourth");
    List<CdcTestEvent> inserts = List.of(first, second, third, fourth);
    sendAndAwait(TABLE_IDENTIFIER, null, USE_SCHEMA, inserts, inserts);

    // one expected result for both modes: update, delete, insert and update of the new key
    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    CdcTestEvent fifth = CdcTestEvent.insert(5, "type5", NOW, "fifth");
    CdcTestEvent fifthUpdated = CdcTestEvent.update(5, "type5", NOW, "fifth updated");
    sendAndAwait(
        TABLE_IDENTIFIER,
        null,
        USE_SCHEMA,
        List.of(updated, CdcTestEvent.delete(second), fifth, fifthUpdated),
        List.of(updated, third, fourth, fifthUpdated));
  }

  @Test
  public void testSwitchToCopyOnWrite() {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(USE_SCHEMA, MERGE_ON_READ, null, ImmutableMap.of()));

    // ids 1 and 2 in one partition, so one task writes them to one data file: the copy-on-write
    // rewrite of id 1 reads the file that also holds the deleted id 2. Id 3 goes to the other
    // partition: in merge-on-read a task without records does not answer the coordinator, which
    // then commits only on commit.timeout-ms
    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    CdcTestEvent third = CdcTestEvent.insert(3, "type3", NOW, "third");
    sendToPartition(first, 0);
    sendToPartition(second, 0);
    sendToPartition(third, 1);
    flush();
    List<CdcTestEvent> inserts = List.of(first, second, third);
    awaitRows(TABLE_IDENTIFIER, null, inserts);

    sendAndAwait(
        TABLE_IDENTIFIER,
        null,
        USE_SCHEMA,
        List.of(CdcTestEvent.delete(second)),
        List.of(first, third));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    long switchSnapshotId = table.currentSnapshot().snapshotId();

    context().stopConnector(connectorName());
    context().startConnector(rowLevelConfig(USE_SCHEMA, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(TABLE_IDENTIFIER, null, USE_SCHEMA, List.of(updated), List.of(updated, third));

    assertNoConnectorDeleteFiles(table, switchSnapshotId);
  }

  @Test
  public void testSwitchBackToMergeOnRead() {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(USE_SCHEMA, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    List<CdcTestEvent> inserts = List.of(first, second);
    sendAndAwait(TABLE_IDENTIFIER, null, USE_SCHEMA, inserts, inserts);

    // a one-key change set is one slice under the default max-slice-keys: its snapshot ends the
    // drain
    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(TABLE_IDENTIFIER, null, USE_SCHEMA, List.of(updated), List.of(updated, second));

    context().stopConnector(connectorName());
    context().startConnector(rowLevelConfig(USE_SCHEMA, MERGE_ON_READ, null, ImmutableMap.of()));

    // an update in each partition: in merge-on-read a task without records does not answer the
    // coordinator, which then commits only on commit.timeout-ms
    CdcTestEvent updatedAgain = CdcTestEvent.update(1, "type1", NOW, "updated again");
    CdcTestEvent secondUpdated = CdcTestEvent.update(2, "type2", NOW, "second updated");
    sendToPartition(updatedAgain, 0);
    sendToPartition(secondUpdated, 1);
    flush();
    awaitRows(TABLE_IDENTIFIER, null, List.of(updatedAgain, secondUpdated));

    assertThat(deleteFiles(TABLE_IDENTIFIER, null)).isNotEmpty();
  }

  private void sendToPartition(CdcTestEvent event, int partition) {
    producer()
        .send(
            new ProducerRecord<>(
                testTopic(), partition, Long.toString(event.id()), event.serialize(USE_SCHEMA)));
  }

  private static Stream<Arguments> useSchemaAndBranch() {
    return Stream.of(true, false)
        .flatMap(
            useSchema ->
                Stream.of(null, "test_branch").map(branch -> Arguments.of(useSchema, branch)));
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("routing.strategy", "all-tables")
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE));
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
