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

import static org.apache.iceberg.connect.StatisticsItSupport.METRICS;
import static org.apache.iceberg.connect.StatisticsItSupport.METRIC_GAP_OVER_LIMIT;
import static org.apache.iceberg.connect.StatisticsItSupport.METRIC_MISSING_BASELINE;
import static org.apache.iceberg.connect.StatisticsItSupport.METRIC_STALE;
import static org.apache.iceberg.connect.StatisticsItSupport.RECOMPUTE_SNAPSHOT_ID_PROP;
import static org.apache.iceberg.connect.StatisticsItSupport.assertFinalFile;
import static org.apache.iceberg.connect.StatisticsItSupport.assertNdvWithinBounds;
import static org.apache.iceberg.connect.StatisticsItSupport.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsItSupport.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitConnectLog;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitMetric;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitNoMetrics;
import static org.apache.iceberg.connect.StatisticsItSupport.awaitNoStagedPuffins;
import static org.apache.iceberg.connect.StatisticsItSupport.blobOf;
import static org.apache.iceberg.connect.StatisticsItSupport.metricsOf;
import static org.apache.iceberg.connect.StatisticsItSupport.ndv;
import static org.apache.iceberg.connect.StatisticsItSupport.ndvBounds;
import static org.apache.iceberg.connect.StatisticsItSupport.sparkAttributeStats;
import static org.apache.iceberg.connect.StatisticsItSupport.sparkComputeTableStats;
import static org.apache.iceberg.connect.StatisticsItSupport.sparkDistinctCounts;
import static org.apache.iceberg.connect.StatisticsItSupport.sparkInsert;
import static org.apache.iceberg.connect.StatisticsItSupport.stateCleared;
import static org.apache.iceberg.connect.StatisticsItSupport.stateRaised;
import static org.apache.iceberg.connect.StatisticsItSupport.statisticsFile;
import static org.apache.iceberg.connect.StatisticsItSupport.trinoDistinctCounts;
import static org.apache.iceberg.connect.StatisticsItSupport.trinoInsert;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.StatisticsItSupport.Kind;
import org.apache.iceberg.connect.StatisticsItSupport.MetricRegistration;
import org.apache.iceberg.connect.StatisticsItSupport.Row;
import org.apache.iceberg.connect.StatisticsItSupport.SentValues;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Integration tests of the statistics of the sink against the other engines of the stand and the
 * metrics of the worker: compatibility with Spark and Trino, and the metrics in JMX.
 *
 * <p>The engines read the records the connector writes (Spark {@code attributeStats}, Trino {@code
 * SHOW STATS}), write their own that the connector goes on from ({@code compute_table_stats},
 * {@code ANALYZE}), and Trino merges its sketches into those of the connector on {@code INSERT}.
 */
public class TestIntegrationStatisticsEngines extends StatisticsTestBase {

  private static final int ROWS_PER_CYCLE = 12;
  // the quiet table is checked again every health-recheck-interval-ms (3 s in the tests) and when a
  // cycle of the coordinator comes by: the timeout leaves room for a few of each
  private static final Duration RECHECK_TIMEOUT = Duration.ofSeconds(30);

  private static final List<String> SET_COLUMNS =
      ImmutableList.of(
          "c_int", "c_long", "c_string", "c_date", "c_decimal", "c_ts", "c_uuid", "c_bool");
  // a column the set does not name: no blob, so no distinct count in the engines
  private static final String SKIPPED_COLUMN = "c_skipped";
  private static final Schema ENGINES =
      new Schema(
          Types.NestedField.optional(1, "c_int", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c_long", Types.LongType.get()),
          Types.NestedField.optional(3, "c_string", Types.StringType.get()),
          Types.NestedField.optional(4, "c_date", Types.DateType.get()),
          Types.NestedField.optional(5, "c_decimal", Types.DecimalType.of(10, 2)),
          Types.NestedField.optional(6, "c_ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(7, "c_uuid", Types.UUIDType.get()),
          Types.NestedField.optional(8, "c_bool", Types.BooleanType.get()),
          Types.NestedField.optional(9, SKIPPED_COLUMN, Types.StringType.get()));

  private static final List<String> SIMPLE_COLUMNS = ImmutableList.of("id", "name", "amount");
  private static final Schema SIMPLE =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(3, "amount", Types.IntegerType.get())),
          ImmutableSet.of(1));

  private interface Analyze {
    void run(TableIdentifier identifier) throws InterruptedException;
  }

  @Override
  protected String tablePrefix() {
    return "engines";
  }

  @Test
  public void testSparkSeesNdvOfConnectorForColumnsOfSet() throws InterruptedException {
    Table table = catalog().createTable(tableId(), ENGINES);
    startConnector(setOfColumns());

    long snapshotId = appendCycle(table, engineRows(0, 2 * ROWS_PER_CYCLE), 2L * ROWS_PER_CYCLE);
    awaitStats(table, snapshotId, Kind.NDV);
    StatisticsFile file = assertFinalFile(table, snapshotId, SET_COLUMNS);

    // Spark with CBO on takes the record of the snapshot it scans
    Map<String, String> spark = sparkAttributeStats(tableId());
    Schema schema = schemaOf(table, snapshotId);
    for (String column : SET_COLUMNS) {
      assertThat(spark)
          .as("distinct counts of Spark, column %s", column)
          .containsEntry(column, String.valueOf(ndv(blobOf(file, schema, column))));
    }

    assertThat(spark.getOrDefault(SKIPPED_COLUMN, ""))
        .as("distinct count of Spark for the column the set does not name")
        .isEmpty();
  }

  @Test
  public void testSparkComputeTableStatsIsContinuedByConnector() throws InterruptedException {
    continuesAfterAnalyze(StatisticsItSupport::sparkComputeTableStats);
  }

  @Test
  public void testNdvOfConnectorEqualsFileOfSpark() throws InterruptedException {
    Table table = catalog().createTable(tableId(), ENGINES);
    startConnector(setOfColumns());

    // append only, every value is live: the lower and upper bounds are equal for the connector and
    // for Spark
    long snapshotId = appendCycle(table, engineRows(0, 2 * ROWS_PER_CYCLE), 2L * ROWS_PER_CYCLE);
    awaitStats(table, snapshotId, Kind.NDV);
    StatisticsFile connectorFile = assertFinalFile(table, snapshotId, SET_COLUMNS);
    Schema schema = schemaOf(table, snapshotId);
    Map<String, Long> connector = Maps.newLinkedHashMap();
    for (String column : SET_COLUMNS) {
      connector.put(column, ndv(blobOf(connectorFile, schema, column)));
    }

    // Spark replaces the record of the snapshot with its own file
    sparkComputeTableStats(tableId());
    table.refresh();
    StatisticsFile sparkFile = statisticsFile(table, snapshotId);
    assertThat(sparkFile).isNotNull();
    assertThat(sparkFile.path()).as("the file of Spark").isNotEqualTo(connectorFile.path());
    for (String column : SET_COLUMNS) {
      BlobMetadata blob = blobOf(sparkFile, schema, column);
      assertThat(blob).as("blob of Spark for column %s", column).isNotNull();
      assertThat(blob.properties())
          .as("Spark writes no property of the connector")
          .doesNotContainKey(RECOMPUTE_SNAPSHOT_ID_PROP);
      // the sketch of a column is exact for the small tables of the tests, so the engines agree to
      // the digit. Spark hashes a uuid as a string, the connector as 16 bytes: the two sketches
      // of the uuid column are not compatible, the equality is checked for the other columns
      if (!"c_uuid".equals(column)) {
        assertThat(ndv(blob))
            .as("ndv of column %s, the file of Spark against that of the connector", column)
            .isEqualTo(connector.get(column));
      }
    }
  }

  @Test
  public void testTrinoShowStatsEqualsNdvOfConnector() throws InterruptedException {
    Table table = catalog().createTable(tableId(), ENGINES);
    startConnector(setOfColumns());

    long snapshotId = appendCycle(table, engineRows(0, 2 * ROWS_PER_CYCLE), 2L * ROWS_PER_CYCLE);
    awaitStats(table, snapshotId, Kind.NDV);
    StatisticsFile file = assertFinalFile(table, snapshotId, SET_COLUMNS);

    Map<String, Long> trino = trinoDistinctCounts(tableId());
    Schema schema = schemaOf(table, snapshotId);
    for (String column : SET_COLUMNS) {
      assertThat(trino)
          .as("distinct_values_count of Trino, column %s", column)
          .containsEntry(column, ndv(blobOf(file, schema, column)));
    }

    assertThat(trino)
        .as("distinct_values_count of Trino for the column the set does not name")
        .doesNotContainKey(SKIPPED_COLUMN);
  }

  @Test
  public void testTrinoAnalyzeIsContinuedByConnector() throws InterruptedException {
    continuesAfterAnalyze(StatisticsItSupport::trinoAnalyze);
  }

  @Test
  public void testTrinoInsertMergesItsSketchIntoThatOfConnectorWithUuid()
      throws InterruptedException {
    Table table = catalog().createTable(tableId(), ENGINES);
    startConnector(setOfColumns());

    SentValues sent = new SentValues();
    List<Row> rows = engineRows(0, 2 * ROWS_PER_CYCLE);
    rows.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, rows, 2L * ROWS_PER_CYCLE);
    awaitStats(table, firstSnapshotId, Kind.NDV);
    assertFinalFile(table, firstSnapshotId, SET_COLUMNS);

    // Trino brings a row of values the connector sent (the uuid included) and a row of new ones:
    // a sketch merged by the same hashing counts the first once and the second as new
    List<Map<String, Object>> inserted =
        ImmutableList.of(engineFields(4), engineFieldsOfTrinoOnlyRow());
    inserted.forEach(sent::add);
    trinoInsert(tableId(), inserted);

    table.refresh();
    long secondSnapshotId = table.currentSnapshot().snapshotId();
    assertThat(secondSnapshotId).as("the snapshot of Trino").isNotEqualTo(firstSnapshotId);
    StatisticsFile trinoFile = statisticsFile(table, secondSnapshotId);
    assertThat(trinoFile).as("record of the snapshot of Trino").isNotNull();
    assertThat(trinoFile.blobMetadata())
        .as("Trino writes no property of the connector")
        .allSatisfy(
            blob -> assertThat(blob.properties()).doesNotContainKey(RECOMPUTE_SNAPSHOT_ID_PROP));

    // append only, no deleted value: the lower and upper bounds are equal, the merge keeps to them
    Snapshot snapshot = table.snapshot(secondSnapshotId);
    Map<String, Long> live = sparkDistinctCounts(tableId(), secondSnapshotId, SET_COLUMNS);
    Map<String, Long> trino = trinoDistinctCounts(tableId());
    Schema schema = schemaOf(table, secondSnapshotId);
    for (String column : SET_COLUMNS) {
      long[] bounds =
          ndvBounds(
              live.get(column),
              sent.distinct(column),
              StatisticsItSupport.liveRows(table, snapshot),
              StatisticsItSupport.DEFAULT_LG_K);
      assertThat(trino.get(column))
          .as("distinct_values_count of Trino after its INSERT, column %s", column)
          .isBetween(bounds[0], bounds[1])
          .isEqualTo(ndv(blobOf(trinoFile, schema, column)));
    }

    assertThat(trino.get("c_uuid"))
        .as("the uuid column: the sketch of Trino is merged into that of the connector")
        .isEqualTo(sent.distinct("c_uuid"))
        .isEqualTo(live.get("c_uuid"));
  }

  @Test
  public void testMetricsAreInJmxOfWorkerWithTagsOfConnectorAndTask() {
    catalog().createTable(tableId(), SIMPLE);
    startConnector(ImmutableMap.of());

    // the leader of the control topic registers the metrics when it starts the coordinator
    awaitMetric(connectorName(), METRIC_STALE, 0);
    List<MetricRegistration> registrations = metricsOf(connectorName());
    assertThat(registrations).hasSize(1);
    MetricRegistration metrics = registrations.get(0);
    assertThat(metrics.name().getDomain()).isEqualTo("kafka.connect");
    assertThat(metrics.name().getKeyProperty("type"))
        .as("group of the metrics")
        .isEqualTo("plugins");
    assertThat(metrics.connector()).isEqualTo(connectorName());
    // tasks.max = 2
    assertThat(metrics.task()).matches("[01]");
    assertThat(metrics.values().keySet()).containsExactlyInAnyOrderElementsOf(METRICS);
    assertThat(metrics.values().values()).as("no state is raised").containsOnly(0.0);

    // the metrics go with the coordinator
    context().stopConnector(connectorName());
    awaitNoMetrics(connectorName());
  }

  @Test
  public void testForeignAppendOverGapLimitRaisesGapOverLimit() throws InterruptedException {
    Table table = catalog().createTable(tableId(), SIMPLE);
    // no data file of a foreign snapshot fits within the gap limit
    startConnector(ImmutableMap.of("iceberg.tables.statistics.gap-max-files", 0));

    SentValues sent = new SentValues();
    List<Row> first = simpleRows(0, ROWS_PER_CYCLE);
    first.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, first, ROWS_PER_CYCLE);
    awaitStats(table, firstSnapshotId, Kind.NDV);
    awaitMetric(connectorName(), METRIC_GAP_OVER_LIMIT, 0);

    // a snapshot of another writer comes between the record and the next commit of the connector
    List<Row> foreign =
        ImmutableList.of(simpleRow(1000, "spark-a", 1), simpleRow(1001, "spark-b", 2));
    sparkInsert(tableId(), foreign);
    foreign.forEach(row -> sent.add(row.fields()));

    List<Row> second = simpleRows(ROWS_PER_CYCLE, ROWS_PER_CYCLE);
    second.forEach(row -> sent.add(row.fields()));
    appendCycle(table, second, 2L * ROWS_PER_CYCLE + 2);

    awaitMetric(connectorName(), METRIC_GAP_OVER_LIMIT, 1);
    awaitConnectLog(
        testStart(),
        stateRaised("gapOverLimit", tableId()),
        "the state gapOverLimit is raised for the foreign append");

    // the statistics of an engine close the gap: the next step goes on from them and clears it
    sparkComputeTableStats(tableId());
    List<Row> third = simpleRows(2 * ROWS_PER_CYCLE, ROWS_PER_CYCLE);
    third.forEach(row -> sent.add(row.fields()));
    long thirdSnapshotId = appendCycle(table, third, 3L * ROWS_PER_CYCLE + 2);
    awaitStats(table, thirdSnapshotId, Kind.NDV);

    awaitMetric(connectorName(), METRIC_GAP_OVER_LIMIT, 0);
    awaitConnectLog(
        testStart(),
        stateCleared("gapOverLimit", tableId()),
        "the state gapOverLimit is cleared by a step that writes the NDV");
    StatisticsFile file = assertFinalFile(table, thirdSnapshotId, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertNdvWithinBounds(table, tableId(), thirdSnapshotId, file, sent);
  }

  /**
   * A table with data no statistics cover: the connector has nothing to go on from, raises {@code
   * missingBaseline} and writes no record. After the ANALYZE of an engine the metric falls to 0
   * with no new data, and the next commit of the connector goes on from the record of the engine.
   */
  private void continuesAfterAnalyze(Analyze analyze) throws InterruptedException {
    Table table = catalog().createTable(tableId(), SIMPLE);
    SentValues sent = new SentValues();
    List<Row> foreign =
        ImmutableList.of(simpleRow(1000, "spark-a", 1), simpleRow(1001, "spark-b", 2));
    sparkInsert(tableId(), foreign);
    foreign.forEach(row -> sent.add(row.fields()));

    // a short interval, so that the table with no news is checked again soon
    startConnector(ImmutableMap.of("iceberg.tables.statistics.health-recheck-interval-ms", 3000));
    List<Row> first = simpleRows(0, ROWS_PER_CYCLE);
    first.forEach(row -> sent.add(row.fields()));
    long firstSnapshotId = appendCycle(table, first, ROWS_PER_CYCLE + 2);

    // the history holds data written without statistics: there is nothing to go on from
    awaitMetric(connectorName(), METRIC_MISSING_BASELINE, 1);
    awaitConnectLog(
        testStart(),
        stateRaised("missingBaseline", tableId()),
        "the state missingBaseline is raised");
    awaitNoStagedPuffins(table, target());
    table.refresh();
    assertThat(table.statisticsFiles()).as("no baseline, no record").isEmpty();

    analyze.run(tableId());
    table.refresh();
    StatisticsFile base = statisticsFile(table, firstSnapshotId);
    assertThat(base).as("record of the engine").isNotNull();
    assertThat(base.blobMetadata().get(0).properties())
        .as("the engine writes no property of the connector")
        .doesNotContainKey(RECOMPUTE_SNAPSHOT_ID_PROP);
    // no data is sent: the check of the quiet table comes with the interval (3 s) and a cycle
    awaitMetric(connectorName(), METRIC_MISSING_BASELINE, 0, RECHECK_TIMEOUT);
    awaitConnectLog(
        testStart(),
        stateCleared("missingBaseline", tableId()),
        "the state missingBaseline is cleared after ANALYZE",
        RECHECK_TIMEOUT);

    // the next commit goes on from the record of the engine
    List<Row> second = simpleRows(ROWS_PER_CYCLE, ROWS_PER_CYCLE);
    second.forEach(row -> sent.add(row.fields()));
    long secondSnapshotId = appendCycle(table, second, 2L * ROWS_PER_CYCLE + 2);

    awaitStats(table, secondSnapshotId, Kind.NDV);
    StatisticsFile file = assertFinalFile(table, secondSnapshotId, SIMPLE_COLUMNS);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    // the record of the engine is the last full recount: the blob goes on from it
    for (BlobMetadata blob : file.blobMetadata()) {
      assertThat(blob.properties())
          .as("blob of the connector over the record of the engine")
          .containsEntry(RECOMPUTE_SNAPSHOT_ID_PROP, String.valueOf(firstSnapshotId));
    }

    Map<String, Long> live = assertNdvWithinBounds(table, tableId(), secondSnapshotId, file, sent);
    SIMPLE_COLUMNS.forEach(column -> assertThat(live.get(column)).isEqualTo(sent.distinct(column)));
    awaitMetric(connectorName(), METRIC_MISSING_BASELINE, 0);
  }

  /** The settings of the connector: the set of the columns of the table is written out. */
  private Map<String, Object> setOfColumns() {
    return ImmutableMap.of(
        "iceberg.table." + tableId() + ".statistics.columns", String.join(",", SET_COLUMNS));
  }

  private static Schema schemaOf(Table table, long snapshotId) {
    return table.schemas().get(table.snapshot(snapshotId).schemaId());
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

  private static List<Row> engineRows(int from, int count) {
    List<Row> rows = Lists.newArrayList();
    for (int seq = from; seq < from + count; seq++) {
      rows.add(new Row(seq, engineFields(seq)));
    }

    return rows;
  }

  /** A row of every column type; a value is null in every ninth row of a column. */
  private static Map<String, Object> engineFields(int seq) {
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
            seq, 6, UUID.nameUUIDFromBytes(("uuid-" + seq % 17).getBytes(StandardCharsets.UTF_8))));
    fields.put("c_bool", valueOrNull(seq, 7, seq % 2 == 0));
    fields.put(SKIPPED_COLUMN, "skipped-" + seq);
    return fields;
  }

  /** A row with a value no row of {@link #engineFields} has in any column but the boolean one. */
  private static Map<String, Object> engineFieldsOfTrinoOnlyRow() {
    Map<String, Object> fields = Maps.newLinkedHashMap();
    fields.put("c_int", 999);
    fields.put("c_long", 9_999_999_999_999L);
    fields.put("c_string", "trino-only");
    fields.put("c_date", LocalDate.of(2030, 1, 1));
    fields.put("c_decimal", new BigDecimal("99999.99"));
    fields.put("c_ts", LocalDateTime.of(2030, 1, 1, 0, 0));
    fields.put("c_uuid", UUID.nameUUIDFromBytes("trino-only".getBytes(StandardCharsets.UTF_8)));
    fields.put("c_bool", true);
    fields.put(SKIPPED_COLUMN, "skipped-trino");
    return fields;
  }

  private static Object valueOrNull(int seq, int column, Object value) {
    return (seq + column) % 9 == 0 ? null : value;
  }
}
