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

import static org.apache.iceberg.connect.service.RestCatalogSparkClient.runSparkSqlQuery;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIntegrationCopyOnWrite extends IntegrationTestBaseRowLevel {

  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);
  private static final Instant NOW = Instant.ofEpochMilli(System.currentTimeMillis());
  private static final Instant THREE_DAYS_AGO = NOW.minus(Duration.ofDays(3));
  private static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";
  // marks the result rows among the log lines of the spark-sql output
  private static final String SPARK_ROW_PREFIX = "row:";

  @ParameterizedTest
  @MethodSource("useSchemaBranchAndPartitioning")
  public void testInsertUpdateDelete(boolean useSchema, String branch, boolean partitioned) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            TestEvent.TEST_SCHEMA,
            partitioned ? TestEvent.TEST_SPEC : PartitionSpec.unpartitioned());
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, branch, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", THREE_DAYS_AGO, "second");
    CdcTestEvent third = CdcTestEvent.insert(3, "type3", NOW, "third");
    List<CdcTestEvent> inserts = List.of(first, second, third);
    sendAndAwait(TABLE_IDENTIFIER, branch, useSchema, inserts, inserts);

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    long insertSnapshotId = snapshot(table, branch).snapshotId();
    // a file's lower bound on id is a value it holds, so every file planned here holds id 1 or 2
    Set<String> filesWithChangedRows =
        dataFileLocations(table, insertSnapshotId, Expressions.in("id", 1L, 2L));

    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(
        TABLE_IDENTIFIER,
        branch,
        useSchema,
        List.of(updated, CdcTestEvent.delete(second)),
        List.of(updated, third));

    table.refresh();
    assertNoConnectorDeleteFiles(table, insertSnapshotId);
    assertThat(filesWithChangedRows)
        .isNotEmpty()
        .doesNotContainAnyElementsOf(
            dataFileLocations(
                table, snapshot(table, branch).snapshotId(), Expressions.alwaysTrue()));
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
    if (branch != null) {
      assertThat(table.currentSnapshot()).as("main").isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFirstBatchOnEmptyTable(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent inserted = CdcTestEvent.insert(1, "type1", NOW, "inserted");
    CdcTestEvent updated = CdcTestEvent.update(2, "type2", NOW, "updated");
    CdcTestEvent deleted = CdcTestEvent.insert(3, "type3", NOW, "deleted");
    sendAndAwait(
        TABLE_IDENTIFIER,
        null,
        useSchema,
        List.of(inserted, updated, CdcTestEvent.delete(deleted)),
        List.of(inserted, updated));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertThat(table.snapshots())
        .isNotEmpty()
        .allSatisfy(snapshot -> assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCompositeKey(boolean useSchema) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            new Schema(TestEvent.TEST_SCHEMA.columns(), ImmutableSet.of(1, 2)),
            PartitionSpec.unpartitioned());
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent firstA = CdcTestEvent.insert(1, "a", NOW, "1a");
    CdcTestEvent firstB = CdcTestEvent.insert(1, "b", NOW, "1b");
    CdcTestEvent secondA = CdcTestEvent.insert(2, "a", NOW, "2a");
    CdcTestEvent secondB = CdcTestEvent.insert(2, "b", NOW, "2b");
    List<CdcTestEvent> inserts = List.of(firstA, firstB, secondA, secondB);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    CdcTestEvent updated = CdcTestEvent.update(1, "a", NOW, "updated");
    sendAndAwait(
        TABLE_IDENTIFIER,
        null,
        useSchema,
        List.of(updated, CdcTestEvent.delete(secondB)),
        List.of(updated, firstB, secondA));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPartitionedRewriteScope(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    // three days, the ids of each day disjoint from the others, so an id filter plans the data
    // files of exactly the days holding those ids
    Instant yesterday = NOW.minus(Duration.ofDays(1));
    Instant twoDaysAgo = NOW.minus(Duration.ofDays(2));
    CdcTestEvent changed = CdcTestEvent.insert(1, "type1", NOW, "changed");
    CdcTestEvent deleted = CdcTestEvent.insert(2, "type2", NOW, "deleted");
    List<CdcTestEvent> untouched =
        List.of(
            CdcTestEvent.insert(3, "type3", yesterday, "third"),
            CdcTestEvent.insert(4, "type4", yesterday, "fourth"),
            CdcTestEvent.insert(5, "type5", twoDaysAgo, "fifth"),
            CdcTestEvent.insert(6, "type6", twoDaysAgo, "sixth"));
    List<CdcTestEvent> inserts = Lists.newArrayList(changed, deleted);
    inserts.addAll(untouched);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    Expression untouchedDays = Expressions.in("id", 3L, 4L, 5L, 6L);
    Set<String> untouchedFiles =
        dataFileLocations(table, table.currentSnapshot().snapshotId(), untouchedDays);

    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    List<CdcTestEvent> expected = Lists.newArrayList(updated);
    expected.addAll(untouched);
    sendAndAwait(
        TABLE_IDENTIFIER,
        null,
        useSchema,
        List.of(updated, CdcTestEvent.delete(deleted)),
        expected);

    table.refresh();
    assertThat(dataFileLocations(table, table.currentSnapshot().snapshotId(), untouchedDays))
        .isNotEmpty()
        .isEqualTo(untouchedFiles);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testKeyOnlyDeletePartitioned(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", THREE_DAYS_AGO, "second");
    List<CdcTestEvent> inserts = List.of(first, second);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    // merge-on-read routes a key-only delete to the null partition, where it never applies
    sendAndAwait(
        TABLE_IDENTIFIER, null, useSchema, List.of(CdcTestEvent.deleteKey(2)), List.of(first));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testUpdateMovesPartition(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA, TestEvent.TEST_SPEC);
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent moved = CdcTestEvent.insert(1, "type1", THREE_DAYS_AGO, "moved");
    CdcTestEvent other = CdcTestEvent.insert(2, "type2", NOW, "other");
    List<CdcTestEvent> inserts = List.of(moved, other);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    // the rows hold one version of id 1, the new one
    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, List.of(updated), List.of(updated, other));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertThat(dataFileDays(table, Expressions.equal("id", 1L)))
        .containsExactly((int) ChronoUnit.DAYS.between(Instant.EPOCH, NOW));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInsertOfExistingKey(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    List<CdcTestEvent> inserts = List.of(first, second);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    CdcTestEvent reinserted = CdcTestEvent.insert(1, "type1", NOW, "reinserted");
    sendAndAwait(
        TABLE_IDENTIFIER, null, useSchema, List.of(reinserted), List.of(reinserted, second));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testChangeSetSlicing(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context()
        .startConnector(
            rowLevelConfig(
                useSchema,
                COPY_ON_WRITE,
                null,
                ImmutableMap.of("iceberg.tables.copy-on-write.max-slice-keys", 2)));

    List<CdcTestEvent> inserts =
        LongStream.rangeClosed(1, 10)
            .mapToObj(id -> CdcTestEvent.insert(id, "type" + id, NOW, "inserted"))
            .toList();
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    long insertSnapshotId = table.currentSnapshot().snapshotId();

    List<CdcTestEvent> updates =
        LongStream.rangeClosed(1, 10)
            .mapToObj(id -> CdcTestEvent.update(id, "type" + id, NOW, "updated"))
            .toList();
    sendAsOneBatch(updates, useSchema);
    awaitRows(TABLE_IDENTIFIER, null, updates);

    table.refresh();
    List<Snapshot> drain =
        Lists.reverse(
            Lists.newArrayList(
                SnapshotUtil.ancestorsBetween(
                    table, table.currentSnapshot().snapshotId(), insertSnapshotId)));
    assertThat(drain).hasSizeGreaterThanOrEqualTo(2);
    assertThat(drain.subList(0, drain.size() - 1))
        .allSatisfy(snapshot -> assertThat(snapshot.summary()).containsKey(CHANGE_SET_ID_PROP));
    assertThat(drain.get(drain.size() - 1).summary()).doesNotContainKey(CHANGE_SET_ID_PROP);
    assertNoConnectorDeleteFiles(table, insertSnapshotId);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRestartKeepsExactlyOnce(boolean useSchema) {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    KafkaConnectUtils.Config connectorConfig =
        rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of());
    context().startConnector(connectorConfig);

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    List<CdcTestEvent> inserts = List.of(first, second);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    long committedSnapshotId = table.currentSnapshot().snapshotId();

    context().stopConnector(connectorName());
    context().startConnector(connectorConfig);

    // the restarted connector runs a commit cycle every second
    Awaitility.await()
        .during(Duration.ofSeconds(15))
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              table.refresh();
              assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committedSnapshotId);
            });

    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, List.of(updated), List.of(updated, second));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSparkReadsCopyOnWriteTable(boolean useSchema) throws InterruptedException {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);
    context().startConnector(rowLevelConfig(useSchema, COPY_ON_WRITE, null, ImmutableMap.of()));

    CdcTestEvent first = CdcTestEvent.insert(1, "type1", NOW, "first");
    CdcTestEvent second = CdcTestEvent.insert(2, "type2", NOW, "second");
    CdcTestEvent third = CdcTestEvent.insert(3, "type3", NOW, "third");
    List<CdcTestEvent> inserts = List.of(first, second, third);
    sendAndAwait(TABLE_IDENTIFIER, null, useSchema, inserts, inserts);

    CdcTestEvent updated = CdcTestEvent.update(1, "type1", NOW, "updated");
    sendAndAwait(
        TABLE_IDENTIFIER,
        null,
        useSchema,
        List.of(updated, CdcTestEvent.delete(second)),
        List.of(updated, third));

    String output =
        runSparkSqlQuery(
            String.format(
                "SELECT concat('%s', id, '|', type, '|', unix_millis(ts), '|', payload)"
                    + " FROM spark_catalog.%s",
                SPARK_ROW_PREFIX, TABLE_IDENTIFIER));
    assertThat(output.lines().filter(line -> line.startsWith(SPARK_ROW_PREFIX)))
        .containsExactlyInAnyOrder(
            sparkRow(1, "type1", NOW, "updated"), sparkRow(3, "type3", NOW, "third"));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      assertThat(tasks).isNotEmpty().allSatisfy(task -> assertThat(task.deletes()).isEmpty());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Sends the events to one partition without waiting for each, so one poll of one task and one
   * commit cycle take them all.
   */
  private void sendAsOneBatch(List<CdcTestEvent> events, boolean useSchema) {
    events.forEach(
        event ->
            producer()
                .send(
                    new ProducerRecord<>(
                        testTopic(), 0, Long.toString(event.id()), event.serialize(useSchema))));
    flush();
  }

  private static String sparkRow(long id, String type, Instant ts, String payload) {
    return SPARK_ROW_PREFIX + id + "|" + type + "|" + ts.toEpochMilli() + "|" + payload;
  }

  /** Partition days of the live data files that may hold rows matching the filter. */
  private static Set<Integer> dataFileDays(Table table, Expression filter) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
      Set<Integer> days = Sets.newHashSet();
      tasks.forEach(task -> days.add(task.file().partition().get(0, Integer.class)));
      return days;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Stream<Arguments> useSchemaBranchAndPartitioning() {
    return Stream.of(true, false)
        .flatMap(
            useSchema ->
                Stream.of(null, "test_branch")
                    .flatMap(
                        branch ->
                            Stream.of(false, true)
                                .map(partitioned -> Arguments.of(useSchema, branch, partitioned))));
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
