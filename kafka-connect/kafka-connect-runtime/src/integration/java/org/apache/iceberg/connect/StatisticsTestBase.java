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

import static org.apache.iceberg.connect.StatisticsItSupport.awaitLanded;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitStatistics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.StatisticsItSupport.Kind;
import org.apache.iceberg.connect.StatisticsItSupport.Row;
import org.apache.iceberg.connect.StatisticsItSupport.Target;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;

/**
 * The stand side of the integration tests of the statistics: one table of its own for each test
 * (the lines of the connector log and the staging directory name only that test), the connector
 * with the statistics on, JSON records of {@link Row}, and the waiting for a snapshot with its
 * statistics.
 */
abstract class StatisticsTestBase extends AbstractTestBase {

  static final Duration LANDING_TIMEOUT = Duration.ofSeconds(90);
  // the staging orphan TTL has to exceed commit.interval-ms plus commit.timeout-ms; the common
  // Integer.MAX_VALUE fails that check
  static final int COMMIT_TIMEOUT_MS = 30_000;

  private TableIdentifier tableId;
  private Instant testStart;

  protected abstract String tablePrefix();

  @BeforeEach
  public void initTable() {
    this.tableId =
        TableIdentifier.of(
            TEST_DB, tablePrefix() + "_" + UUID.randomUUID().toString().substring(0, 8));
    // Docker takes the moment of a log in whole seconds
    this.testStart = Instant.now().minusSeconds(2);
  }

  protected TableIdentifier tableId() {
    return tableId;
  }

  protected Instant testStart() {
    return testStart;
  }

  protected Target target() {
    return new Target(tableId, connectorName());
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("routing.strategy", "all-tables")
        .config("iceberg.tables", tableId.toString())
        .config("iceberg.control.commit.timeout-ms", COMMIT_TIMEOUT_MS)
        .config("iceberg.tables.statistics.enabled", true);
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(tableId);
  }

  /** Starts the connector of the test; the extra settings win over the common ones. */
  protected void startConnector(Map<String, Object> extraConfig) {
    KafkaConnectUtils.Config config = createConfig(false);
    context().connectorCatalogProperties().forEach(config::config);
    extraConfig.forEach(config::config);
    context().startConnector(config);
  }

  protected void awaitStats(Table table, long snapshotId, Kind... kinds) {
    awaitStatistics(table, target(), snapshotId, kinds);
  }

  protected void sendRows(List<Row> rows) {
    rows.forEach(
        row ->
            producer()
                .send(
                    new ProducerRecord<>(
                        testTopic(),
                        (int) (row.id() % TEST_TOPIC_PARTITIONS),
                        Long.toString(row.id()),
                        row.serialize(false))));
    flush();
  }

  /**
   * Sends the rows to one partition without waiting for each, so that one poll of one task and one
   * commit cycle take them all.
   */
  protected void sendRowsToOnePartition(List<Row> rows) {
    rows.forEach(
        row ->
            producer()
                .send(
                    new ProducerRecord<>(
                        testTopic(), 0, Long.toString(row.id()), row.serialize(false))));
    flush();
  }

  /** Sends the rows and waits until the table holds exactly the count; returns the head. */
  protected long appendCycle(Table table, List<Row> rows, long expectedRows) {
    sendRows(rows);
    return awaitLanded(
        table,
        current -> rowCount(current) == expectedRows,
        "the table holds " + expectedRows + " rows",
        LANDING_TIMEOUT);
  }

  static List<Record> readRecords(Table table) {
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      return Lists.newArrayList(records);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static long rowCount(Table table) {
    return readRecords(table).size();
  }
}
