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

import static org.apache.iceberg.connect.StatisticsItSupport.Kind;
import static org.apache.iceberg.connect.StatisticsItSupport.Row;
import static org.apache.iceberg.connect.StatisticsItSupport.SentValues;
import static org.apache.iceberg.connect.StatisticsItSupport.Target;
import static org.apache.iceberg.connect.StatisticsItSupport.assertFinalFile;
import static org.apache.iceberg.connect.StatisticsItSupport.assertNdvWithinBounds;
import static org.apache.iceberg.connect.StatisticsItSupport.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsItSupport.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.connect.StatisticsItSupport.assertWrittenByConnector;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitConnectLog;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitLanded;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitNoStagedPuffins;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitStatistics;
import static org.apache.iceberg.connect.StatisticsItSupport.connectLog;
import static org.apache.iceberg.connect.StatisticsItSupport.partitionStatisticsFile;
import static org.apache.iceberg.connect.StatisticsItSupport.readPartitionStats;
import static org.apache.iceberg.connect.StatisticsItSupport.sparkPartitionStats;
import static org.apache.iceberg.connect.StatisticsItSupport.stagedPuffins;
import static org.apache.iceberg.connect.StatisticsItSupport.stagingDirectory;
import static org.apache.iceberg.connect.StatisticsItSupport.stateCleared;
import static org.apache.iceberg.connect.StatisticsItSupport.stateRaised;
import static org.apache.iceberg.connect.StatisticsItSupport.statisticsCommitted;
import static org.apache.iceberg.connect.StatisticsItSupport.statisticsFile;
import static org.apache.iceberg.connect.StatisticsItSupport.statisticsWarnings;
import static org.apache.iceberg.connect.StatisticsItSupport.warningsOf;
import static org.apache.iceberg.connect.service.RestCatalogSparkClient.runSparkSqlQuery;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests of the NDV and partition statistics of the sink in merge-on-read (append and
 * CDC): NDV on every commit, partition statistics, the catch-up after a stop, schema evolution, the
 * initial state, the retention of statistics records, the staging cleanup and upserts with deletes.
 *
 * <p>Every test has its own table, so the lines of the connector log and the staging directory name
 * only that test.
 */
public class TestIntegrationStatistics extends AbstractTestBase {

  private static final Duration LANDING_TIMEOUT = Duration.ofSeconds(90);
  // the staging orphan TTL has to exceed commit.interval-ms plus commit.timeout-ms; the common
  // Integer.MAX_VALUE fails that check
  private static final int COMMIT_TIMEOUT_MS = 30_000;
  private static final int ROWS_PER_CYCLE = 12;
  // distinct values of the column of namedRows in one portion of ROWS_PER_CYCLE rows
  private static final int NAMED_VALUES = 5;

  private static final List<String> ALL_TYPES_COLUMNS =
      ImmutableList.of(
          "c_int", "c_long", "c_string", "c_date", "c_decimal", "c_ts", "c_uuid", "c_bool");
  private static final Schema ALL_TYPES =
      new Schema(
          Types.NestedField.optional(1, "c_int", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c_long", Types.LongType.get()),
          Types.NestedField.optional(3, "c_string", Types.StringType.get()),
          Types.NestedField.optional(4, "c_date", Types.DateType.get()),
          Types.NestedField.optional(5, "c_decimal", Types.DecimalType.of(10, 2)),
          Types.NestedField.optional(6, "c_ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(7, "c_uuid", Types.UUIDType.get()),
          Types.NestedField.optional(8, "c_bool", Types.BooleanType.get()));

  private static final List<String> SIMPLE_COLUMNS = ImmutableList.of("id", "name", "amount");
  private static final Schema SIMPLE =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(3, "amount", Types.IntegerType.get())),
          ImmutableSet.of(1));

  private static final Schema PARTITIONED =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "region", Types.StringType.get()),
          Types.NestedField.optional(3, "name", Types.StringType.get()));
  private static final List<String> PARTITIONED_COLUMNS = ImmutableList.of("id", "region", "name");
  private static final List<String> REGIONS = ImmutableList.of("eu", "us", "ap");

  private TableIdentifier tableId;
  private Instant testStart;

  @BeforeEach
  public void initTable() {
    this.tableId =
        TableIdentifier.of(TEST_DB, "stats_" + UUID.randomUUID().toString().substring(0, 8));
    // Docker takes the moment of a log in whole seconds
    this.testStart = Instant.now().minusSeconds(2);
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

  @Test
  public void testNdvOnEveryCommitForAllColumnTypes() throws InterruptedException {
    Table table = catalog().createTable(tableId, ALL_TYPES);
    startConnector(ImmutableMap.of());

    SentValues sent = new SentValues();
    for (int cycle = 0; cycle < 3; cycle++) {
      List<Row> rows = allTypesRows(cycle * ROWS_PER_CYCLE, ROWS_PER_CYCLE);
      rows.forEach(row -> sent.add(row.fields()));
      long snapshotId = appendCycle(table, rows, (long) ROWS_PER_CYCLE * (cycle + 1));

      awaitStats(table, snapshotId, Kind.NDV);
      StatisticsFile file = assertFinalFile(table, snapshotId, ALL_TYPES_COLUMNS);
      assertStatisticsFilesExist(table);
      assertStatisticsOnOwnSnapshot(table);
      Map<String, Long> live = assertNdvWithinBounds(table, tableId, snapshotId, file, sent);
      // append only: the live rows hold every value sent, so the lower and upper bounds are equal
      ALL_TYPES_COLUMNS.forEach(
          column ->
              assertThat(live.get(column))
                  .as("distinct values of %s in the live rows", column)
                  .isEqualTo(sent.distinct(column)));
    }

    // a cycle may commit in two snapshots (one for each task), so there can be more than three
    assertThat(table.statisticsFiles()).hasSizeGreaterThanOrEqualTo(3);
  }

  @Test
  public void testPartitionStatisticsMatchPartitionsOfSpark() throws InterruptedException {
    PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED).identity("region").build();
    Table table = catalog().createTable(tableId, PARTITIONED, spec);
    startConnector(ImmutableMap.of("iceberg.tables.statistics.partition-stats-enabled", true));

    for (int cycle = 0; cycle < 2; cycle++) {
      long snapshotId =
          appendCycle(
              table,
              regionRows(cycle * ROWS_PER_CYCLE, ROWS_PER_CYCLE),
              (long) ROWS_PER_CYCLE * (cycle + 1));

      awaitStats(table, snapshotId, Kind.NDV, Kind.PARTITIONS);
      assertThat(statisticsFile(table, snapshotId)).isNotNull();
      assertThat(partitionStatisticsFile(table, snapshotId)).isNotNull();
      assertFinalFile(table, snapshotId, PARTITIONED_COLUMNS);
      assertStatisticsFilesExist(table);
      assertStatisticsOnOwnSnapshot(table);

      Map<String, String> connector = readPartitionStats(table, snapshotId);
      assertThat(connector).as("partitions of the statistics").hasSameSizeAs(REGIONS);
      assertThat(connector).isEqualTo(sparkPartitionStats(tableId, snapshotId, "region"));
    }
  }

  @Test
  public void testPartitionStatisticsOnUnpartitionedTable() throws InterruptedException {
    Table table = catalog().createTable(tableId, SIMPLE);
    startConnector(ImmutableMap.of("iceberg.tables.statistics.partition-stats-enabled", true));

    long snapshotId = appendCycle(table, simpleRows(0, ROWS_PER_CYCLE), ROWS_PER_CYCLE);
    // only the NDV is written; waiting for the partition statistics would never end
    awaitStats(table, snapshotId, Kind.NDV);
    assertFinalFile(table, snapshotId, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);

    // a cycle later the step would have written the partition statistics
    long nextSnapshotId =
        appendCycle(table, simpleRows(ROWS_PER_CYCLE, ROWS_PER_CYCLE), 2L * ROWS_PER_CYCLE);
    awaitStats(table, nextSnapshotId, Kind.NDV);
    assertThat(table.partitionStatisticsFiles()).isEmpty();
    // the step logs its last line after the record is committed
    awaitConnectLog(
        testStart,
        statisticsCommitted(tableId, nextSnapshotId),
        "the step of the second snapshot is over");
    List<String> log = connectLog(testStart);
    assertThat(warningsOf(log, tableId)).isEmpty();
    // the loggers of the statistics are told by the end of a line, which the log pattern of the
    // worker image sets: a line the step surely wrote has to be told as theirs
    assertThat(log)
        .filteredOn(statisticsCommitted(tableId, snapshotId))
        .as("the line of the step of the first snapshot")
        .isNotEmpty()
        .allMatch(StatisticsItSupport::writtenByStatisticsLogger);
    // a warning of the statistics need not name the table; the lines that name another table come
    // from the end of the previous test, which the log since the start of this one still holds
    assertThat(statisticsWarnings(log, tableId)).isEmpty();
  }

  @Test
  public void testStatisticsCatchUpAfterConnectorWasStopped() throws InterruptedException {
    Table table = catalog().createTable(tableId, SIMPLE);
    startConnector(ImmutableMap.of());

    SentValues sent = new SentValues();
    for (int cycle = 0; cycle < 2; cycle++) {
      List<Row> rows = simpleRows(cycle * ROWS_PER_CYCLE, ROWS_PER_CYCLE);
      rows.forEach(row -> sent.add(row.fields()));
      long snapshotId = appendCycle(table, rows, (long) ROWS_PER_CYCLE * (cycle + 1));
      awaitStats(table, snapshotId, Kind.NDV);
    }

    // the head of the table gets a snapshot without statistics while nobody writes them
    context().stopConnector(connectorName());
    sparkInsert(ImmutableList.of(simpleRow(1000, "spark-a", 1), simpleRow(1001, "spark-b", 2)))
        .forEach(row -> sent.add(row.fields()));
    table.refresh();
    long head = table.currentSnapshot().snapshotId();
    assertThat(statisticsFile(table, head)).as("record of the snapshot of Spark").isNull();

    // restarted with no records to write: the lag of the table at start is the only reason
    // for a step
    startConnector(ImmutableMap.of());

    awaitStats(table, head, Kind.NDV);
    assertThat(table.currentSnapshot().snapshotId())
        .as("the connector commits no data")
        .isEqualTo(head);
    StatisticsFile file = assertFinalFile(table, head, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    // the gap of the snapshot of Spark is within the limit: the record covers its rows too
    Map<String, Long> live = assertNdvWithinBounds(table, tableId, head, file, sent);
    SIMPLE_COLUMNS.forEach(column -> assertThat(live.get(column)).isEqualTo(sent.distinct(column)));
  }

  @Test
  public void testNewFieldOfRecordsGetsItsBlob() throws InterruptedException {
    Table table = catalog().createTable(tableId, simple("name"));
    startConnector(ImmutableMap.of("iceberg.tables.evolve-schema-enabled", true));

    SentValues sent = new SentValues();
    List<Row> first = namedRows("name", "name-", 0, ROWS_PER_CYCLE);
    first.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, first, ROWS_PER_CYCLE);
    awaitStats(table, firstSnapshotId, Kind.NDV);
    assertFinalFile(table, firstSnapshotId, ImmutableList.of("id", "name"));

    // the records carry a field the table does not have; the connector adds the column
    List<Row> second = Lists.newArrayList();
    for (int i = ROWS_PER_CYCLE; i < 2 * ROWS_PER_CYCLE; i++) {
      Map<String, Object> fields = Maps.newLinkedHashMap();
      fields.put("id", (long) i);
      fields.put("name", "name-" + i % 5);
      fields.put("extra", "extra-" + i % 4);
      second.add(new Row(i, fields));
    }

    second.forEach(row -> sent.add(row.fields()));
    long secondSnapshotId = appendCycle(table, second, 2L * ROWS_PER_CYCLE);
    awaitStats(table, secondSnapshotId, Kind.NDV);

    StatisticsFile file =
        assertFinalFile(table, secondSnapshotId, ImmutableList.of("id", "name", "extra"));
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertNdvWithinBounds(table, tableId, secondSnapshotId, file, sent);
    assertThat(
            StatisticsItSupport.ndv(
                StatisticsItSupport.blobOf(file, schemaOf(table, secondSnapshotId), "extra")))
        .as("the blob of the new column")
        .isEqualTo(4L);
  }

  @Test
  public void testDroppedColumnHasNoBlobOnNextSnapshot() throws InterruptedException {
    Table table = catalog().createTable(tableId, simple("name", "note"));
    startConnector(ImmutableMap.of("iceberg.tables.evolve-schema-enabled", true));

    SentValues sent = new SentValues();
    List<Row> first = Lists.newArrayList();
    for (int i = 0; i < ROWS_PER_CYCLE; i++) {
      Map<String, Object> fields = Maps.newLinkedHashMap();
      fields.put("id", (long) i);
      fields.put("name", "name-" + i % 6);
      fields.put("note", "note-" + i);
      first.add(new Row(i, fields));
    }

    first.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, first, ROWS_PER_CYCLE);
    awaitStats(table, firstSnapshotId, Kind.NDV);
    assertFinalFile(table, firstSnapshotId, ImmutableList.of("id", "name", "note"));

    runSparkSqlQuery(String.format("ALTER TABLE spark_catalog.%s DROP COLUMN note", tableId));

    // the records no longer carry the field: with the schema evolution on, it would come back
    List<Row> second = namedRows("name", "name-", ROWS_PER_CYCLE, ROWS_PER_CYCLE);
    second.forEach(row -> sent.add(row.fields()));
    long secondSnapshotId = appendCycle(table, second, 2L * ROWS_PER_CYCLE);
    awaitStats(table, secondSnapshotId, Kind.NDV);

    StatisticsFile file = assertFinalFile(table, secondSnapshotId, ImmutableList.of("id", "name"));
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(schemaOf(table, secondSnapshotId).findField("note")).isNull();
    assertNdvWithinBounds(table, tableId, secondSnapshotId, file, sent);
  }

  @Test
  public void testRenamedColumnKeepsItsBlob() throws InterruptedException {
    Table table = catalog().createTable(tableId, simple("name"));
    startConnector(ImmutableMap.of("iceberg.tables.evolve-schema-enabled", true));

    SentValues sent = new SentValues();
    List<Row> first = namedRows("name", "name-", 0, ROWS_PER_CYCLE);
    first.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, first, ROWS_PER_CYCLE);
    awaitStats(table, firstSnapshotId, Kind.NDV);
    StatisticsFile firstFile =
        assertFinalFile(table, firstSnapshotId, ImmutableList.of("id", "name"));
    int fieldId = schemaOf(table, firstSnapshotId).findField("name").fieldId();

    runSparkSqlQuery(
        String.format("ALTER TABLE spark_catalog.%s RENAME COLUMN name TO title", tableId));
    sent.rename("name", "title");

    // the records name the column by its new name; the old one would add a column
    List<Row> second = namedRows("title", "title-", ROWS_PER_CYCLE, ROWS_PER_CYCLE);
    second.forEach(row -> sent.add(row.fields()));
    long secondSnapshotId = appendCycle(table, second, 2L * ROWS_PER_CYCLE);
    awaitStats(table, secondSnapshotId, Kind.NDV);

    StatisticsFile file = assertFinalFile(table, secondSnapshotId, ImmutableList.of("id", "title"));
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(schemaOf(table, secondSnapshotId).findField("title").fieldId())
        .as("field id of the renamed column")
        .isEqualTo(fieldId);
    assertThat(StatisticsItSupport.blobOf(file, schemaOf(table, secondSnapshotId), "title"))
        .isNotNull();
    assertThat(StatisticsItSupport.blobOf(firstFile, schemaOf(table, firstSnapshotId), "name"))
        .isNotNull();
    // the blob goes on from that of the old name: the two portions share no value, so a blob built
    // from the second one alone would count only NAMED_VALUES
    assertNdvWithinBounds(table, tableId, secondSnapshotId, file, sent);
    assertThat(
            StatisticsItSupport.ndv(
                StatisticsItSupport.blobOf(file, schemaOf(table, secondSnapshotId), "title")))
        .as("the blob of the renamed column goes on from that of the old name")
        .isEqualTo(2L * NAMED_VALUES);
  }

  @Test
  public void testEmptyTableStartsItsStatisticsFromScratch() throws InterruptedException {
    Table table = catalog().createTable(tableId, SIMPLE);
    startConnector(ImmutableMap.of());

    SentValues sent = new SentValues();
    List<Row> rows = simpleRows(0, ROWS_PER_CYCLE);
    rows.forEach(row -> sent.add(row.fields()));
    long snapshotId = appendCycle(table, rows, ROWS_PER_CYCLE);

    awaitStats(table, snapshotId, Kind.NDV);
    StatisticsFile file = assertFinalFile(table, snapshotId, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    // nobody ran ANALYZE: every record is the connector's (a cycle may commit in two snapshots)
    assertWrittenByConnector(table);
    assertNdvWithinBounds(table, tableId, snapshotId, file, sent);
    assertThat(connectLog(testStart).stream().anyMatch(stateRaised("missingBaseline", tableId)))
        .as("the table with no data has a baseline to start from")
        .isFalse();
  }

  @Test
  public void testNonEmptyTableWithoutStatisticsWaitsForAnalyze() throws InterruptedException {
    Table table = catalog().createTable(tableId, SIMPLE);
    SentValues sent = new SentValues();
    sparkInsert(ImmutableList.of(simpleRow(1000, "spark-a", 1), simpleRow(1001, "spark-b", 2)))
        .forEach(row -> sent.add(row.fields()));

    // a short interval, so that the table with no news is checked again soon
    startConnector(ImmutableMap.of("iceberg.tables.statistics.health-recheck-interval-ms", 3000));
    List<Row> first = simpleRows(0, ROWS_PER_CYCLE);
    first.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, first, ROWS_PER_CYCLE + 2);

    // the history holds data written without statistics: there is nothing to continue
    awaitConnectLog(
        testStart, stateRaised("missingBaseline", tableId), "the state missingBaseline is raised");
    awaitNoStagedPuffins(table, target());
    table.refresh();
    assertThat(statisticsFile(table, firstSnapshotId)).as("no baseline, no record").isNull();
    assertThat(table.statisticsFiles()).isEmpty();

    // ANALYZE gives the baseline
    runSparkSqlQuery(
        String.format("CALL spark_catalog.system.compute_table_stats(table => '%s')", tableId));
    table.refresh();
    assertThat(statisticsFile(table, firstSnapshotId)).as("record of ANALYZE").isNotNull();
    // no data is sent: the check of the quiet table comes with the recheck interval (3 s) and a
    // cycle (1 s), so half a minute is more than enough
    awaitConnectLog(
        testStart,
        stateCleared("missingBaseline", tableId),
        "the state missingBaseline is cleared after ANALYZE",
        Duration.ofSeconds(30));

    // the next commit goes on from the statistics of ANALYZE
    List<Row> second = simpleRows(ROWS_PER_CYCLE, ROWS_PER_CYCLE);
    second.forEach(row -> sent.add(row.fields()));
    long secondSnapshotId = appendCycle(table, second, 2L * ROWS_PER_CYCLE + 2);

    awaitStats(table, secondSnapshotId, Kind.NDV);
    StatisticsFile file = assertFinalFile(table, secondSnapshotId, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    Map<String, Long> live = assertNdvWithinBounds(table, tableId, secondSnapshotId, file, sent);
    SIMPLE_COLUMNS.forEach(column -> assertThat(live.get(column)).isEqualTo(sent.distinct(column)));
  }

  @Test
  public void testLatestTenRecordsOfEachKindAreKept() {
    PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED).identity("region").build();
    Table table = catalog().createTable(tableId, PARTITIONED, spec);
    startConnector(ImmutableMap.of("iceberg.tables.statistics.partition-stats-enabled", true));

    Set<Long> ndvSnapshots = Sets.newHashSet();
    Set<Long> partitionSnapshots = Sets.newHashSet();
    Set<String> paths = Sets.newHashSet();
    int cycles = 15;
    for (int cycle = 0; cycle < cycles; cycle++) {
      long snapshotId = appendCycle(table, regionRows(cycle * 4, 4), 4L * (cycle + 1));
      awaitStats(table, snapshotId, Kind.NDV, Kind.PARTITIONS);

      // the records seen before the step of a later cycle took the oldest away
      table
          .statisticsFiles()
          .forEach(
              file -> {
                ndvSnapshots.add(file.snapshotId());
                paths.add(file.path());
              });
      table
          .partitionStatisticsFiles()
          .forEach(
              file -> {
                partitionSnapshots.add(file.snapshotId());
                paths.add(file.path());
              });
    }

    table.refresh();
    List<Long> keptNdv = table.statisticsFiles().stream().map(StatisticsFile::snapshotId).toList();
    List<Long> keptPartitions =
        table.partitionStatisticsFiles().stream().map(PartitionStatisticsFile::snapshotId).toList();
    // the snapshots with no record (the jobs of two cycles merged) are not counted
    assertThat(keptNdv)
        .hasSize(10)
        .containsExactlyInAnyOrderElementsOf(newest(table, ndvSnapshots, 10));
    assertThat(keptPartitions)
        .hasSize(10)
        .containsExactlyInAnyOrderElementsOf(newest(table, partitionSnapshots, 10));
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);

    // the files of the removed records are deleted
    Set<String> kept = Sets.newHashSet();
    table.statisticsFiles().forEach(file -> kept.add(file.path()));
    table.partitionStatisticsFiles().forEach(file -> kept.add(file.path()));
    Set<String> removed = Sets.difference(paths, kept);
    assertThat(removed).isNotEmpty();
    Awaitility.await()
        .atMost(StatisticsItSupport.AWAIT_TIMEOUT)
        .pollInterval(StatisticsItSupport.POLL_INTERVAL)
        .untilAsserted(
            () ->
                assertThat(removed)
                    .allSatisfy(
                        path ->
                            assertThat(table.io().newInputFile(path).exists())
                                .as("file of a removed record %s", path)
                                .isFalse()));
  }

  @Test
  public void testStagingHoldsNoPuffinAfterCycleAndOldOneIsCleanedUp() throws IOException {
    Table table = catalog().createTable(tableId, SIMPLE);
    // the TTL and the interval are cut down, the TTL still exceeds the interval and the timeout
    startConnector(
        ImmutableMap.of(
            "iceberg.control.commit.timeout-ms", 10_000,
            "iceberg.tables.staging-orphan-ttl-ms", 20_000,
            "iceberg.tables.staging-orphan-cleanup-interval-ms", 2_000));

    long snapshotId = appendCycle(table, simpleRows(0, ROWS_PER_CYCLE), ROWS_PER_CYCLE);
    awaitStats(table, snapshotId, Kind.NDV);
    assertThat(stagedPuffins(table, target())).as("Puffin files of the cycle").isEmpty();

    // an orphan of a task that died between writing the file and sending the event
    String orphan =
        stagingDirectory(table, target()) + "/stats/orphan-" + UUID.randomUUID() + ".puffin";
    OutputFile file = table.io().newOutputFile(orphan);
    try (OutputStream out = file.create()) {
      out.write("not a Puffin file".getBytes(StandardCharsets.UTF_8));
    }

    assertThat(stagedPuffins(table, target())).containsExactly(orphan);
    // younger than the TTL: the cleanups of the next seconds leave it
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(6))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(stagedPuffins(table, target())).containsExactly(orphan));
    // older than the TTL: the cleanup deletes it
    awaitNoStagedPuffins(table, target());

    // the cleanup leaves the statistics of the table alone
    assertStatisticsFilesExist(table);
    assertThat(statisticsFile(table, snapshotId)).isNotNull();
  }

  @Test
  public void testUpsertAndDeleteKeepNdvBetweenLiveAndSent() throws InterruptedException {
    Table table = catalog().createTable(tableId, SIMPLE);
    startConnector(ImmutableMap.of("iceberg.tables.cdc-field", "op"));

    SentValues sent = new SentValues();
    List<Row> inserts = Lists.newArrayList();
    for (int i = 1; i <= 6; i++) {
      inserts.add(cdcRow("c", i, "a" + i, i));
    }

    inserts.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId =
        cdcCycle(
            table,
            inserts,
            ImmutableMap.of(1L, "a1", 2L, "a2", 3L, "a3", 4L, "a4", 5L, "a5", 6L, "a6"));
    awaitStats(table, firstSnapshotId, Kind.NDV);
    StatisticsFile firstFile = assertFinalFile(table, firstSnapshotId, SIMPLE_COLUMNS);
    assertNdvWithinBounds(table, tableId, firstSnapshotId, firstFile, sent);

    // three keys get new versions, two keys go: their old values are in no live row any more
    List<Row> changes = Lists.newArrayList();
    for (int i = 1; i <= 3; i++) {
      changes.add(cdcRow("u", i, "b" + i, 100 + i));
    }

    changes.forEach(row -> sent.add(row.fields()));
    changes.add(cdcRow("d", 4, "a4", 4));
    changes.add(cdcRow("d", 5, "a5", 5));
    long secondSnapshotId =
        cdcCycle(table, changes, ImmutableMap.of(1L, "b1", 2L, "b2", 3L, "b3", 6L, "a6"));
    awaitStats(table, secondSnapshotId, Kind.NDV);

    StatisticsFile file = assertFinalFile(table, secondSnapshotId, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    Map<String, Long> live = assertNdvWithinBounds(table, tableId, secondSnapshotId, file, sent);
    // L < U, so that the check tells an NDV too low from one above everything sent
    assertThat(live.get("id")).isEqualTo(4L);
    assertThat(sent.distinct("id")).isEqualTo(6L);
    assertThat(live.get("name")).isEqualTo(4L);
    assertThat(sent.distinct("name")).isEqualTo(9L);
    assertThat(live.get("amount")).isEqualTo(4L);
    assertThat(sent.distinct("amount")).isEqualTo(9L);
  }

  private Target target() {
    return new Target(tableId, connectorName());
  }

  private void startConnector(Map<String, Object> extraConfig) {
    KafkaConnectUtils.Config config = createConfig(false);
    context().connectorCatalogProperties().forEach(config::config);
    extraConfig.forEach(config::config);
    context().startConnector(config);
  }

  private void awaitStats(Table table, long snapshotId, Kind... kinds) {
    awaitStatistics(table, target(), snapshotId, kinds);
  }

  private void sendRows(List<Row> rows) {
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

  private long appendCycle(Table table, List<Row> rows, long expectedRows) {
    sendRows(rows);
    return awaitLanded(
        table,
        current -> rowCount(current) == expectedRows,
        "the table holds " + expectedRows + " rows",
        LANDING_TIMEOUT);
  }

  /** Sends the change events and waits until the live rows are the expected names by id. */
  private long cdcCycle(Table table, List<Row> events, Map<Long, String> expected) {
    sendRows(events);
    Predicate<Table> landed =
        current -> {
          Map<Long, String> names = Maps.newHashMap();
          readRecords(current)
              .forEach(
                  record ->
                      names.put((Long) record.getField("id"), (String) record.getField("name")));
          return names.equals(expected);
        };
    return awaitLanded(table, landed, "the live rows are " + expected, LANDING_TIMEOUT);
  }

  private static List<Record> readRecords(Table table) {
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      return Lists.newArrayList(records);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static long rowCount(Table table) {
    return readRecords(table).size();
  }

  /** Inserts the rows through Spark: a snapshot the connector did not write. */
  private List<Row> sparkInsert(List<Row> rows) throws InterruptedException {
    StringBuilder values = new StringBuilder();
    for (Row row : rows) {
      values
          .append(values.length() == 0 ? "" : ", ")
          .append(
              String.format(
                  "(%s, '%s', %s)",
                  row.fields().get("id"), row.fields().get("name"), row.fields().get("amount")));
    }

    runSparkSqlQuery(String.format("INSERT INTO spark_catalog.%s VALUES %s", tableId, values));
    return rows;
  }

  private static List<Long> newest(Table table, Set<Long> snapshotIds, int count) {
    List<Long> sorted =
        snapshotIds.stream()
            .sorted(Comparator.comparingLong(id -> table.snapshot(id).sequenceNumber()))
            .toList();
    return sorted.subList(sorted.size() - count, sorted.size());
  }

  private static Schema schemaOf(Table table, long snapshotId) {
    return table.schemas().get(table.snapshot(snapshotId).schemaId());
  }

  /** A table of the id (the key) and the given optional string columns. */
  private static Schema simple(String... columns) {
    List<Types.NestedField> fields = Lists.newArrayList();
    fields.add(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (int i = 0; i < columns.length; i++) {
      fields.add(Types.NestedField.optional(2 + i, columns[i], Types.StringType.get()));
    }

    return new Schema(fields, ImmutableSet.of(1));
  }

  private static Row simpleRow(long id, String name, int amount) {
    Map<String, Object> fields = Maps.newLinkedHashMap();
    fields.put("id", id);
    fields.put("name", name);
    fields.put("amount", amount);
    return new Row(id, fields);
  }

  /** Rows of {@link #SIMPLE}: values repeat, so that the distinct count is below the row count. */
  private static List<Row> simpleRows(int from, int count) {
    List<Row> rows = Lists.newArrayList();
    for (int i = from; i < from + count; i++) {
      rows.add(simpleRow(i, "name-" + i % 7, i % 5));
    }

    return rows;
  }

  /**
   * Rows of the id and one string column of the given name; the column holds {@link #NAMED_VALUES}
   * distinct values {@code <valuePrefix>0, <valuePrefix>1, …}, so portions with different prefixes
   * share none.
   */
  private static List<Row> namedRows(String column, String valuePrefix, int from, int count) {
    List<Row> rows = Lists.newArrayList();
    for (int i = from; i < from + count; i++) {
      Map<String, Object> fields = Maps.newLinkedHashMap();
      fields.put("id", (long) i);
      fields.put(column, valuePrefix + i % NAMED_VALUES);
      rows.add(new Row(i, fields));
    }

    return rows;
  }

  private static List<Row> regionRows(int from, int count) {
    List<Row> rows = Lists.newArrayList();
    for (int i = from; i < from + count; i++) {
      Map<String, Object> fields = Maps.newLinkedHashMap();
      fields.put("id", (long) i);
      fields.put("region", REGIONS.get(i % REGIONS.size()));
      fields.put("name", "name-" + i % 4);
      rows.add(new Row(i, fields));
    }

    return rows;
  }

  private static Row cdcRow(String op, long id, String name, int amount) {
    Map<String, Object> fields = Maps.newLinkedHashMap();
    fields.put("id", id);
    fields.put("name", name);
    fields.put("amount", amount);
    fields.put("op", op);
    return new Row(id, fields);
  }

  /** A row of every column type; a value is null in every ninth row of a column. */
  private static List<Row> allTypesRows(int from, int count) {
    List<Row> rows = Lists.newArrayList();
    for (int seq = from; seq < from + count; seq++) {
      Map<String, Object> fields = Maps.newLinkedHashMap();
      fields.put("c_int", valueOrNull(seq, 0, seq % 13));
      fields.put("c_long", valueOrNull(seq, 1, 1_000_000_000_000L + seq));
      fields.put("c_string", valueOrNull(seq, 2, "value-" + seq % 21));
      fields.put("c_date", valueOrNull(seq, 3, LocalDate.of(2024, 1, 1).plusDays(seq % 40)));
      fields.put("c_decimal", valueOrNull(seq, 4, BigDecimal.valueOf(seq % 30 * 100 + 25, 2)));
      fields.put(
          "c_ts",
          valueOrNull(seq, 5, LocalDateTime.of(2024, 1, 1, 0, 0).plusSeconds(seq % 50 * 3600L)));
      fields.put(
          "c_uuid",
          valueOrNull(
              seq,
              6,
              UUID.nameUUIDFromBytes(("uuid-" + seq % 17).getBytes(StandardCharsets.UTF_8))));
      fields.put("c_bool", valueOrNull(seq, 7, seq % 2 == 0));
      rows.add(new Row(seq, fields));
    }

    return rows;
  }

  private static Object valueOrNull(int seq, int column, Object value) {
    return (seq + column) % 9 == 0 ? null : value;
  }
}
