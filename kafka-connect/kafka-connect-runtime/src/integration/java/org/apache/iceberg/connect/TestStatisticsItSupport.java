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

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.StatisticsItSupport.Row;
import org.apache.iceberg.connect.StatisticsItSupport.SentValues;
import org.apache.iceberg.connect.StatisticsItSupport.Target;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

/** The pure logic of {@link StatisticsItSupport}: no stand needed. */
public class TestStatisticsItSupport {

  private static final TableIdentifier TABLE = TableIdentifier.of("test", "stats_1");

  @Test
  public void testBoundsAreExactBelowSketchSize() {
    // U below K = 4096: L <= ndv <= min(U, live)
    assertThat(StatisticsItSupport.ndvBounds(4, 9, 100L, 12)).containsExactly(4, 9);
    assertThat(StatisticsItSupport.ndvBounds(4, 9, 6L, 12)).containsExactly(4, 6);
    assertThat(StatisticsItSupport.ndvBounds(4, 9, null, 12)).containsExactly(4, 9);
    assertThat(StatisticsItSupport.ndvBounds(7, 7, 7L, 12)).containsExactly(7, 7);
  }

  @Test
  public void testBoundsAllowErrorOfSketchFromSketchSize() {
    // lgK = 4 (K = 16): U = 16 is not below K, the error is 3/sqrt(16) = 75%
    long[] bounds = StatisticsItSupport.ndvBounds(10, 16, null, 4);
    assertThat(bounds).containsExactly(2, 28);
    // the live rows cap the upper bound in either mode
    assertThat(StatisticsItSupport.ndvBounds(10, 16, 20L, 4)).containsExactly(2, 20);
  }

  @Test
  public void testCountsOfSparkAreReadFromRowWithMarkOnly() {
    String output =
        "26/09/30 12:00:00 INFO SparkContext: Running Spark version 4.0.4\n"
            + "L|id=4|name=9|amount=0\n"
            + "Time taken: 2.5 seconds, Fetched 1 row(s)\n";
    assertThat(StatisticsItSupport.parseSparkCounts(output))
        .containsExactly(Map.entry("id", 4L), Map.entry("name", 9L), Map.entry("amount", 0L));
  }

  @Test
  public void testOutputWithoutRowOfCountsIsRefused() {
    assertThatThrownBy(() -> StatisticsItSupport.parseSparkCounts("Time taken: 1 seconds\n"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("the result row of the Spark query");
  }

  @Test
  public void testSentValuesCountDistinctNotNull() {
    SentValues sent = new SentValues();
    sent.add(Map.of("a", 1, "b", "x"));
    sent.add(Map.of("a", 1, "b", "y"));
    sent.add("a", null);
    sent.add("a", 2);

    assertThat(sent.distinct("a")).isEqualTo(2L);
    assertThat(sent.distinct("b")).isEqualTo(2L);
    assertThat(sent.distinct("unknown")).isEqualTo(0L);
  }

  @Test
  public void testDecimalsOfDifferentScaleAreOneValue() {
    SentValues sent = new SentValues();
    sent.add("d", new BigDecimal("12.50"));
    sent.add("d", new BigDecimal("12.5"));
    sent.add("d", new BigDecimal("100.00"));
    sent.add("d", new BigDecimal("1E+2"));

    assertThat(sent.distinct("d")).isEqualTo(2L);
  }

  @Test
  public void testRenameMovesValuesToNewName() {
    SentValues sent = new SentValues();
    sent.add("name", "a");
    sent.add("name", "b");
    sent.rename("name", "title");
    sent.add("title", "b");
    sent.add("title", "c");

    assertThat(sent.distinct("name")).isEqualTo(0L);
    assertThat(sent.distinct("title")).isEqualTo(3L);
  }

  @Test
  public void testRowIsJsonObjectOfValuesConverterReads() {
    Row row =
        new Row(
            1,
            ImmutableMap.<String, Object>builder()
                .put("i", 5)
                .put("l", 7L)
                .put("s", "text")
                .put("d", LocalDate.of(2024, 1, 2))
                .put("n", new BigDecimal("12.50"))
                .put("t", LocalDateTime.of(2024, 1, 2, 3, 4, 5))
                .put("u", UUID.fromString("00000000-0000-0000-0000-00000000002a"))
                .put("b", true)
                .build());

    assertThat(row.serialize(false))
        .isEqualTo(
            "{\"i\":5,\"l\":7,\"s\":\"text\",\"d\":\"2024-01-02\",\"n\":\"12.50\","
                + "\"t\":1704164645000,\"u\":\"00000000-0000-0000-0000-00000000002a\","
                + "\"b\":true}");
  }

  @Test
  public void testStagingDirectoryEscapesDotsSlashesAndPercents() {
    String location = "s3://bucket/warehouse/test/stats_1";

    assertThat(StatisticsItSupport.stagingDirectory(location, new Target(TABLE, "c1")))
        .isEqualTo("s3://bucket/warehouse/test/stats_1/kc-staging/test.stats_1/connect-c1");
    assertThat(
            StatisticsItSupport.stagingDirectory(
                location, new Target(TableIdentifier.of("a.b", "c/d"), "x%y")))
        .isEqualTo("s3://bucket/warehouse/test/stats_1/kc-staging/a%2Eb.c%2Fd/connect-x%25y");
  }

  @Test
  public void testDistinctCountsOfSparkAreReadAfterPromptOfRepl() {
    String output =
        "scala> val stats: Statistics = Statistics(sizeInBytes=84.0 B, rowCount=3)\n"
            + "\n"
            + "scala> NDV|name|2\n"
            + "NDV|id|3\n"
            + "NDV|note|\n"
            + "\n"
            + "scala> \n";

    // a column Spark knows with no distinct count is an empty string, not a missing key
    assertThat(StatisticsItSupport.parseAttributeStats(output))
        .containsExactly(Map.entry("name", "2"), Map.entry("id", "3"), Map.entry("note", ""));
    assertThat(StatisticsItSupport.parseAttributeStats("scala> 1 + 1\nres0: Int = 2\n")).isEmpty();
  }

  @Test
  public void testShowStatsOfTrinoGivesDistinctCountOfColumnsWithOne() {
    // column_name, data_size, distinct_values_count, nulls_fraction, row_count, low, high
    List<List<String>> rows =
        List.of(
            List.of("id", "", "3.0", "0.0", "", "1", "3"),
            List.of("name", "324.0", "2.0", "0.0", "", "", ""),
            List.of("note", "120.0", "", "0.0", "", "", ""),
            List.of("", "", "", "", "3.0"));

    assertThat(StatisticsItSupport.parseShowStats(rows))
        .containsExactly(Map.entry("id", 3L), Map.entry("name", 2L));
  }

  @Test
  public void testInsertOfTrinoCarriesTypedLiteralsOfEveryColumnType() {
    Map<String, Object> first = Maps.newLinkedHashMap();
    first.put("c_int", 5);
    first.put("c_long", 1_000_000_000_005L);
    first.put("c_string", "it's");
    first.put("c_date", LocalDate.of(2024, 1, 2));
    first.put("c_decimal", new BigDecimal("1.25"));
    first.put("c_ts", LocalDateTime.of(2024, 1, 2, 3, 4, 5));
    first.put("c_uuid", UUID.fromString("00000000-0000-0000-0000-00000000002a"));
    first.put("c_bool", true);
    Map<String, Object> second = Maps.newLinkedHashMap();
    second.put("c_int", null);
    second.put("c_long", -1L);
    second.put("c_string", "x");
    second.put("c_date", null);
    second.put("c_decimal", new BigDecimal("1E+2"));
    second.put("c_ts", null);
    second.put("c_uuid", null);
    second.put("c_bool", false);

    assertThat(StatisticsItSupport.trinoInsertStatement(TABLE, List.of(first, second)))
        .isEqualTo(
            "INSERT INTO test.stats_1 (c_int, c_long, c_string, c_date, c_decimal, c_ts, c_uuid,"
                + " c_bool) VALUES (5, 1000000000005, 'it''s', DATE '2024-01-02', DECIMAL '1.25',"
                + " TIMESTAMP '2024-01-02 03:04:05', UUID '00000000-0000-0000-0000-00000000002a',"
                + " true), (NULL, -1, 'x', NULL, DECIMAL '100', NULL, NULL, false)");
  }

  @Test
  public void testInsertOfSparkNamesColumnsAndEscapesStrings() {
    Map<String, Object> first = Maps.newLinkedHashMap();
    first.put("id", 1L);
    first.put("name", "it's a\\b");
    first.put("amount", 5);
    // the map of the second row is built in another order: the values follow the columns
    Map<String, Object> second = Maps.newLinkedHashMap();
    second.put("amount", null);
    second.put("name", "x");
    second.put("id", 2L);

    assertThat(
            StatisticsItSupport.sparkInsertStatement(
                TABLE, List.of(new Row(1, first), new Row(2, second))))
        .isEqualTo(
            "INSERT INTO spark_catalog.test.stats_1 (id, name, amount) VALUES"
                + " (1, 'it\\'s a\\\\b', 5), (2, 'x', NULL)");
  }

  @Test
  public void testLogLineOfStateAtSnapshot() {
    List<String> log =
        List.of(
            "[2026-09-30 12:00:00,000] WARN Statistics state 'stale' of table test.stats_1"
                + " raised at snapshot 55: columns name=0.23 (StatisticsHealth)",
            "[2026-09-30 12:00:01,000] WARN Statistics state 'stale' of table test.stats_1"
                + " raised at snapshot 5: columns name=0.23 (StatisticsHealth)");

    assertThat(log.stream().filter(StatisticsItSupport.stateRaised("stale", TABLE, 5)))
        .as("snapshot 5 is not a prefix of snapshot 55")
        .containsExactly(log.get(1));
    assertThat(log.stream().anyMatch(StatisticsItSupport.stateRaised("stale", TABLE, 555)))
        .isFalse();
  }

  @Test
  public void testLogLinesOfStates() {
    List<String> log =
        List.of(
            "[2026-09-30 12:00:00,000] WARN Statistics state 'missingBaseline' of table"
                + " test.stats_1 raised at snapshot 5: no baseline (StatisticsHealth)",
            "[2026-09-30 12:00:09,000] INFO Statistics state 'missingBaseline' of table"
                + " test.stats_1 cleared at snapshot 5 (StatisticsHealth)",
            "[2026-09-30 12:00:10,000] WARN Something else of test.stats_2 (Coordinator)");

    assertThat(log.stream().filter(StatisticsItSupport.stateRaised("missingBaseline", TABLE)))
        .containsExactly(log.get(0));
    assertThat(log.stream().filter(StatisticsItSupport.stateCleared("missingBaseline", TABLE)))
        .containsExactly(log.get(1));
    assertThat(log.stream().anyMatch(StatisticsItSupport.stateRaised("stale", TABLE))).isFalse();
    assertThat(StatisticsItSupport.warningsOf(log, TABLE)).containsExactly(log.get(0));
  }

  @Test
  public void testWarningsOfStatisticsLoggersNeedNotNameTable() {
    List<String> log =
        List.of(
            "[2026-09-30 12:00:00,000] WARN Cannot write partition statistics (StatisticsStep)",
            "[2026-09-30 12:00:01,000] ERROR Sketch failed (ColumnSketchCollector)",
            "[2026-09-30 12:00:02,000] INFO Committed statistics (StatisticsStep)",
            "[2026-09-30 12:00:03,000] WARN Something else (Coordinator)",
            "[2026-09-30 12:00:04,000] WARN No partition spec"
                + " (org.apache.iceberg.connect.data.StatisticsStep:123)",
            "[2026-09-30 12:00:05,000] ERROR Sketch failed"
                + " (org.apache.iceberg.connect.data.ColumnSketchCollector)",
            "[2026-09-30 12:00:06,000] WARN Something else"
                + " (org.apache.iceberg.connect.channel.Coordinator:88)");

    assertThat(StatisticsItSupport.statisticsWarnings(log))
        .containsExactly(log.get(0), log.get(1), log.get(4), log.get(5));
  }

  @Test
  public void testWarningsOfStatisticsLoggersThatNameAnotherTableAreLeftOut() {
    List<String> log =
        List.of(
            "[2026-09-30 12:00:00,000] WARN Cannot write partition statistics of table test.stats_1"
                + " (org.apache.iceberg.connect.data.StatisticsStep:123)",
            "[2026-09-30 12:00:01,000] WARN Cannot write partition statistics of table test.stats_2"
                + " (org.apache.iceberg.connect.data.StatisticsStep:123)",
            "[2026-09-30 12:00:02,000] WARN Statistics state 'stale' of table test.stats_10 raised"
                + " (org.apache.iceberg.connect.data.StatisticsHealth:77)",
            "[2026-09-30 12:00:03,000] ERROR Sketch failed (ColumnSketchCollector)",
            "[2026-09-30 12:00:04,000] WARN Steps of test.stats_1 and test.stats_2 stopped"
                + " (StatisticsStep)",
            "[2026-09-30 12:00:05,000] WARN Something else of table test.stats_1 (Coordinator)");

    assertThat(StatisticsItSupport.statisticsWarnings(log, TABLE))
        .as("a line naming no table or only this one is kept; stats_10 is another table")
        .containsExactly(log.get(0), log.get(3));
  }

  @Test
  public void testLineOfStepTellsHowManyFilesItReadToRecount() {
    String recount =
        "[2026-09-30 12:00:00,000] INFO Committed statistics of table test.stats_1 at snapshot 5:"
            + " 3 NDV blobs in s3://b/metadata/stats-5-x.puffin, partition statistics none;"
            + " 2 files, 1024 bytes read to recount (StatisticsStep)";
    String merged = recount.replace("; 2 files, 1024 bytes", "; 0 files, 0 bytes");
    String other = recount.replace("at snapshot 5:", "at snapshot 55:");

    assertThat(StatisticsItSupport.filesRead(recount)).isEqualTo(2L);
    assertThat(StatisticsItSupport.filesRead(merged)).isZero();
    assertThat(StatisticsItSupport.recounted(TABLE, 5).test(recount)).isTrue();
    assertThat(StatisticsItSupport.recounted(TABLE, 5).test(merged))
        .as("no data file was read: the sketches were merged")
        .isFalse();
    assertThat(StatisticsItSupport.recounted(TABLE, 5).test(other))
        .as("snapshot 5 is not a prefix of snapshot 55")
        .isFalse();
    assertThat(StatisticsItSupport.statisticsCommitted(TABLE, 5).test(merged)).isTrue();
  }

  @Test
  public void testSketchFilesOfManifestAreLocationsOfItsEntries() throws Exception {
    JsonNode manifest =
        TestContext.MAPPER.readTree(
            "{\"changeSetId\":\"c1\",\"sketchFiles\":[{\"location\":\"s3://b/stats/a.puffin\","
                + "\"fileSizeBytes\":10,\"footerSizeBytes\":4,\"stagedFiles\":[\"s3://b/f1\"]},"
                + "{\"location\":\"s3://b/stats/b.puffin\",\"fileSizeBytes\":11,"
                + "\"footerSizeBytes\":4,\"stagedFiles\":[]}]}");

    assertThat(StatisticsItSupport.sketchFilesOf(manifest))
        .containsExactly("s3://b/stats/a.puffin", "s3://b/stats/b.puffin");
    assertThat(StatisticsItSupport.sketchFilesOf(TestContext.MAPPER.readTree("{}"))).isEmpty();
  }
}
