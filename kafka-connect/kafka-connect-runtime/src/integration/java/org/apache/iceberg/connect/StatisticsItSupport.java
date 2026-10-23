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

import static org.apache.iceberg.connect.service.DockerClient.DOCKER_CLIENT;
import static org.apache.iceberg.connect.service.DockerClient.getContainer;
import static org.apache.iceberg.connect.service.RestCatalogSparkClient.runSparkShell;
import static org.apache.iceberg.connect.service.RestCatalogSparkClient.runSparkSqlQuery;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.service.TrinoClient;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.puffin.StandardPuffinProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.awaitility.Awaitility;

/**
 * Checks shared by the integration tests of the statistics: waiting for the statistics step, the
 * invariants of the files it leaves in the table metadata and in the staging directory, the bounds
 * of the NDV, and a reader of the connector log.
 *
 * <p>The statistics step is asynchronous (merge-on-read: a queue on the transition pool), so a
 * series of cycles sends the next portion of records only after {@link #awaitStatistics} of the
 * previous snapshot: otherwise the jobs merge and the snapshot gets no record.
 */
public final class StatisticsItSupport {

  /** The kinds of the statistics records a step writes. */
  public enum Kind {
    NDV,
    PARTITIONS
  }

  public static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  public static final String RECOMPUTE_SNAPSHOT_ID_PROP = "kafka.connect.recompute-snapshot-id";
  public static final String SKETCHED_ROWS_PROP = "kafka.connect.sketched-rows";
  public static final String NDV_PROP = "ndv";

  /** The default of {@code iceberg.tables.statistics.lg-nominal-entries}. */
  public static final int DEFAULT_LG_K = 12;

  /** The metrics of the sink. */
  public static final String METRIC_GAP_OVER_LIMIT = "statistics-tables-gap-over-limit";

  public static final String METRIC_MISSING_BASELINE = "statistics-tables-missing-baseline";
  public static final String METRIC_STALE = "statistics-tables-stale";
  public static final List<String> METRICS =
      List.of(METRIC_GAP_OVER_LIMIT, METRIC_MISSING_BASELINE, METRIC_STALE);

  static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(120);
  static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

  private static final String STAGING_DIR = "kc-staging";
  private static final String STATS_DIR = "stats";
  private static final String CONNECT_IMAGE = "kafka-connect";
  private static final String CONNECT_SERVICE = "connect";
  private static final String SPARK_ROW_PREFIX = "L|";
  private static final String SPARK_PARTITION_PREFIX = "P|";
  // the logger closes a line: "... (StatisticsStep)"; the log pattern of the worker image prints it
  // with the package and the line: "... (org.apache.iceberg.connect.data.StatisticsStep:245)"
  private static final Pattern STATISTICS_LOGGER =
      Pattern.compile(
          "\\((?:[\\w$]+\\.)*(?:(?:Statistics|Ndv|PartitionStats|HealthTransition)\\w*|\\w*Sketch\\w*)"
              + "(?::\\d+)?\\)\\s*$");
  private static final Pattern SPARK_COUNT = Pattern.compile("\\|(\\w+)=(\\d+)");
  // the REPL of spark-shell prints its prompt before the first line of the output of a statement
  private static final Pattern SPARK_ATTRIBUTE_NDV = Pattern.compile("NDV\\|(\\w+)\\|(\\d*)");
  // the JMX port of the worker is published by the compose file of the stand
  private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:9102/jmxrmi";
  private static final String METRICS_OBJECT_NAME = "kafka.connect:type=plugins,*";
  // the count of the statistics step at the end of its line, see StatisticsStep.logOutcome
  private static final Pattern FILES_READ = Pattern.compile("; (\\d+) files, \\d+ bytes read");
  private static final DateTimeFormatter TRINO_TIMESTAMP =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private StatisticsItSupport() {}

  /** The table of a test and the connector that writes it. */
  public static final class Target {
    private final TableIdentifier identifier;
    private final String connectorName;

    public Target(TableIdentifier identifier, String connectorName) {
      this.identifier = identifier;
      this.connectorName = connectorName;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String connectorName() {
      return connectorName;
    }
  }

  /**
   * Waits until the snapshot has the records of all the given kinds, then until the connector's
   * {@code <stagingDirectory>/stats/} of the table holds no Puffin file: the step deletes the
   * intermediate files of a cycle after its commit. The test must not send records while it waits.
   */
  public static void awaitStatistics(Table table, Target target, long snapshotId, Kind... kinds) {
    List<Kind> expected = Arrays.asList(kinds);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              table.refresh();
              if (expected.contains(Kind.NDV)) {
                assertThat(statisticsFile(table, snapshotId))
                    .as("NDV record of snapshot %s", snapshotId)
                    .isNotNull();
              }
              if (expected.contains(Kind.PARTITIONS)) {
                assertThat(partitionStatisticsFile(table, snapshotId))
                    .as("partition statistics record of snapshot %s", snapshotId)
                    .isNotNull();
              }
            });
    awaitNoStagedPuffins(table, target);
  }

  public static void awaitNoStagedPuffins(Table table, Target target) {
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () ->
                assertThat(stagedPuffins(table, target))
                    .as("intermediate Puffin files under %s", statsDirectory(table, target))
                    .isEmpty());
  }

  /** Waits until the table satisfies the test; returns the head snapshot id. */
  public static long awaitLanded(
      Table table, Predicate<Table> landed, String description, Duration timeout) {
    Awaitility.await()
        .atMost(timeout)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              table.refresh();
              assertThat(landed.test(table)).as(description).isTrue();
            });
    return table.currentSnapshot().snapshotId();
  }

  /** The NDV record of the snapshot, or null. */
  public static StatisticsFile statisticsFile(Table table, long snapshotId) {
    return table.statisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  /** The partition statistics record of the snapshot, or null. */
  public static PartitionStatisticsFile partitionStatisticsFile(Table table, long snapshotId) {
    return table.partitionStatisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  public static void assertStatisticsFilesExist(Table table) {
    table.refresh();
    for (StatisticsFile file : table.statisticsFiles()) {
      assertThat(table.io().newInputFile(file.path()).exists())
          .as("statistics file %s of snapshot %s", file.path(), file.snapshotId())
          .isTrue();
    }

    for (PartitionStatisticsFile file : table.partitionStatisticsFiles()) {
      assertThat(table.io().newInputFile(file.path()).exists())
          .as("partition statistics file %s of snapshot %s", file.path(), file.snapshotId())
          .isTrue();
    }
  }

  /**
   * The snapshot id of every blob of a record, in the table metadata and in the footer of its file,
   * is the snapshot id of the record, and a snapshot has at most one NDV record and one partition
   * statistics record.
   */
  public static void assertStatisticsOnOwnSnapshot(Table table) {
    table.refresh();
    for (StatisticsFile file : table.statisticsFiles()) {
      for (BlobMetadata blob : file.blobMetadata()) {
        assertThat(blob.sourceSnapshotId())
            .as("blob of %s in the metadata", file.path())
            .isEqualTo(file.snapshotId());
      }

      for (org.apache.iceberg.puffin.BlobMetadata blob : readFooter(table, file).blobs()) {
        assertThat(blob.snapshotId())
            .as("blob of %s in the file", file.path())
            .isEqualTo(file.snapshotId());
      }
    }

    assertThat(table.statisticsFiles().stream().map(StatisticsFile::snapshotId))
        .as("snapshots of the NDV records")
        .doesNotHaveDuplicates();
    assertThat(table.partitionStatisticsFiles().stream().map(PartitionStatisticsFile::snapshotId))
        .as("snapshots of the partition statistics records")
        .doesNotHaveDuplicates();
  }

  public static FileMetadata readFooter(Table table, StatisticsFile file) {
    try (PuffinReader reader =
        Puffin.read(table.io().newInputFile(file.path()))
            .withFileSize(file.fileSizeInBytes())
            .withFooterSize(file.fileFooterSizeInBytes())
            .build()) {
      return reader.fileMetadata();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The final file of a snapshot S written by the connector: the path {@code
   * metadata/stats-<S>-<commitId>.puffin}, one theta blob for each column of the set with {@code
   * fields} = [field id], the properties of the blob, and the footer of the file equal to the
   * metadata. Returns the record.
   *
   * @param columns the names of the columns of the set, in the schema of the snapshot
   */
  public static StatisticsFile assertFinalFile(
      Table table, long snapshotId, Collection<String> columns) {
    table.refresh();
    Snapshot snapshot = table.snapshot(snapshotId);
    StatisticsFile file = statisticsFile(table, snapshotId);
    assertThat(file).as("NDV record of snapshot %s", snapshotId).isNotNull();

    // the commit id in the name is that of the cycle; a step for a snapshot of another writer (the
    // lag of the table) has a commit id of its own
    String commitId = snapshot.summary().get(COMMIT_ID_PROP);
    String commitIdPattern = commitId != null ? Pattern.quote(commitId) : "[0-9a-f-]{36}";
    assertThat(file.path())
        .matches(".*/metadata/stats-" + snapshotId + "-" + commitIdPattern + "\\.puffin");

    Schema schema = table.schemas().get(snapshot.schemaId());
    List<Integer> expectedIds = Lists.newArrayList();
    for (String column : columns) {
      assertThat(schema.findField(column)).as("column %s in the schema", column).isNotNull();
      expectedIds.add(schema.findField(column).fieldId());
    }

    List<Integer> blobIds = Lists.newArrayList();
    for (BlobMetadata blob : file.blobMetadata()) {
      assertThat(blob.type()).isEqualTo(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);
      assertThat(blob.fields()).hasSize(1);
      assertThat(blob.sourceSnapshotId()).isEqualTo(snapshotId);
      assertThat(blob.sourceSnapshotSequenceNumber()).isEqualTo(snapshot.sequenceNumber());
      assertThat(blob.properties())
          .containsKeys(NDV_PROP, RECOMPUTE_SNAPSHOT_ID_PROP, SKETCHED_ROWS_PROP);
      assertThat(Long.parseLong(blob.properties().get(NDV_PROP))).isNotNegative();
      assertThat(Long.parseLong(blob.properties().get(RECOMPUTE_SNAPSHOT_ID_PROP))).isPositive();
      assertThat(Long.parseLong(blob.properties().get(SKETCHED_ROWS_PROP))).isNotNegative();
      blobIds.add(blob.fields().get(0));
    }

    assertThat(blobIds).as("field ids of the blobs of %s", file.path()).doesNotHaveDuplicates();
    assertThat(blobIds)
        .as("field ids of the blobs of %s for columns %s", file.path(), columns)
        .containsExactlyInAnyOrderElementsOf(expectedIds);

    FileMetadata footer = readFooter(table, file);
    assertThat(footer.properties().get(StandardPuffinProperties.CREATED_BY_PROPERTY))
        .startsWith("iceberg-kafka-connect ");
    assertThat(footer.blobs()).hasSameSizeAs(file.blobMetadata());
    for (org.apache.iceberg.puffin.BlobMetadata blob : footer.blobs()) {
      assertThat(blob.type()).isEqualTo(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);
      assertThat(blob.snapshotId()).isEqualTo(snapshotId);
      assertThat(blob.inputFields()).hasSize(1);
      assertThat(blobIds).contains(blob.inputFields().get(0));
    }

    return file;
  }

  /**
   * Every blob of every record of the table has the properties of the connector: no engine (Spark
   * {@code compute_table_stats}, Trino {@code ANALYZE}) wrote a record.
   */
  public static void assertWrittenByConnector(Table table) {
    table.refresh();
    assertThat(table.statisticsFiles()).isNotEmpty();
    for (StatisticsFile file : table.statisticsFiles()) {
      for (BlobMetadata blob : file.blobMetadata()) {
        assertThat(blob.properties())
            .as("blob of %s", file.path())
            .containsKeys(RECOMPUTE_SNAPSHOT_ID_PROP, SKETCHED_ROWS_PROP);
      }
    }
  }

  /** The blob of the record for the column, or null. */
  public static BlobMetadata blobOf(StatisticsFile file, Schema schema, String column) {
    int fieldId = schema.findField(column).fieldId();
    return file.blobMetadata().stream()
        .filter(blob -> blob.fields().equals(List.of(fieldId)))
        .findFirst()
        .orElse(null);
  }

  public static long ndv(BlobMetadata blob) {
    return Long.parseLong(blob.properties().get(NDV_PROP));
  }

  /** The partition statistics of the record: partition value of the first field → "rows|files". */
  public static Map<String, String> readPartitionStats(Table table, long snapshotId) {
    PartitionStatisticsFile file = partitionStatisticsFile(table, snapshotId);
    assertThat(file).as("partition statistics record of snapshot %s", snapshotId).isNotNull();
    Schema schema =
        PartitionStatsHandler.schema(
            Partitioning.partitionType(table), TableUtil.formatVersion(table));
    Map<String, String> partitions = Maps.newTreeMap();
    try (CloseableIterable<PartitionStats> stats =
        PartitionStatsHandler.readPartitionStatsFile(
            schema, table.io().newInputFile(file.path()))) {
      for (PartitionStats partition : stats) {
        partitions.put(
            String.valueOf(partition.partition().get(0, Object.class)),
            partition.dataRecordCount() + "|" + partition.dataFileCount());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return partitions;
  }

  /**
   * The staging directory of the connector for the table, {@code <staging>/<tableDir>/<groupDir>}
   * of {@code StagedChangeFileWriter.stagingDirectory}: the default staging location {@code <table
   * location>/kc-staging}, the table part is the namespace levels and the table name joined with a
   * dot, the group part is {@code connect-<connector>}. In every part a dot, a slash and a percent
   * sign are escaped as {@code %XX}.
   */
  public static String stagingDirectory(Table table, Target target) {
    return stagingDirectory(table.location(), target);
  }

  static String stagingDirectory(String tableLocation, Target target) {
    StringBuilder directory =
        new StringBuilder(tableLocation).append('/').append(STAGING_DIR).append('/');
    for (String level : target.identifier().namespace().levels()) {
      appendEscaped(directory, level).append('.');
    }

    appendEscaped(directory, target.identifier().name()).append('/');
    return appendEscaped(directory, "connect-" + target.connectorName()).toString();
  }

  private static StringBuilder appendEscaped(StringBuilder directory, String part) {
    for (char ch : part.toCharArray()) {
      if (ch == '.') {
        directory.append("%2E");
      } else if (ch == '/') {
        directory.append("%2F");
      } else if (ch == '%') {
        directory.append("%25");
      } else {
        directory.append(ch);
      }
    }

    return directory;
  }

  public static String defaultStagingLocation(Table table) {
    return table.location() + "/" + STAGING_DIR + "/";
  }

  public static List<String> filesUnder(Table table, String prefix) {
    SupportsPrefixOperations io = (SupportsPrefixOperations) table.io();
    List<String> files = Lists.newArrayList();
    for (FileInfo info : io.listPrefix(prefix)) {
      files.add(info.location());
    }

    return files;
  }

  public static String statsDirectory(Table table, Target target) {
    return stagingDirectory(table, target) + "/" + STATS_DIR + "/";
  }

  public static List<String> stagedPuffins(Table table, Target target) {
    return filesUnder(table, statsDirectory(table, target));
  }

  /**
   * The manifest of a change set of copy-on-write, {@code
   * <stagingDirectory>/_changesets/<changeSetId>.json}, see {@code ChangeSetManifest.location}.
   */
  public static String changeSetManifest(Table table, Target target, String changeSetId) {
    return stagingDirectory(table, target) + "/_changesets/" + changeSetId + ".json";
  }

  /**
   * The intermediate Puffin files that the manifest of the change set names in {@code sketchFiles}:
   * one for each frozen answer of a worker that came with a Puffin file.
   */
  public static List<String> changeSetSketchFiles(Table table, Target target, String changeSetId) {
    String location = changeSetManifest(table, target, changeSetId);
    try (InputStream in = table.io().newInputFile(location).newStream()) {
      return sketchFilesOf(TestContext.MAPPER.readTree(in));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static List<String> sketchFilesOf(JsonNode manifest) {
    List<String> locations = Lists.newArrayList();
    JsonNode entries = manifest.get("sketchFiles");
    if (entries != null) {
      entries.forEach(entry -> locations.add(entry.get("location").asText()));
    }

    return locations;
  }

  /**
   * Writes a file of a few bytes under {@code <stagingDirectory>/stats/} of the connector that no
   * answer, step or change set names: an orphan Puffin file that a cleanup of the directory deletes
   * once it is older than {@code staging-orphan-ttl-ms}. Returns its location.
   */
  public static String plantOrphanPuffin(Table table, Target target) {
    String location = statsDirectory(table, target) + "orphan-" + UUID.randomUUID() + ".puffin";
    try (PositionOutputStream out = table.io().newOutputFile(location).create()) {
      out.write("orphan".getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return location;
  }

  /**
   * The values a test has sent into the table, per column: the upper bound U of the NDV. Only the
   * values of the rows a writer puts into data files count: the inserted and the updated ones,
   * every version of a row, also the versions later overwritten or deleted.
   */
  public static final class SentValues {
    private final Map<String, Set<Object>> values = Maps.newHashMap();

    /** Adds the values of a row; {@code null} is not a value. */
    public SentValues add(Map<String, Object> row) {
      row.forEach(this::add);
      return this;
    }

    public SentValues add(String column, Object value) {
      if (value != null) {
        values.computeIfAbsent(column, name -> Sets.newHashSet()).add(normalized(value));
      }

      return this;
    }

    public SentValues addAll(Collection<? extends Map<String, Object>> rows) {
      rows.forEach(this::add);
      return this;
    }

    /** After a rename of the column the values are those of the new name. */
    public SentValues rename(String from, String to) {
      Set<Object> renamed = values.remove(from);
      if (renamed != null) {
        values.computeIfAbsent(to, name -> Sets.newHashSet()).addAll(renamed);
      }

      return this;
    }

    public long distinct(String column) {
      return values.getOrDefault(column, Set.of()).size();
    }

    private static Object normalized(Object value) {
      // the table stores the decimal with the scale of the column, not that of the record
      return value instanceof BigDecimal ? ((BigDecimal) value).stripTrailingZeros() : value;
    }
  }

  /**
   * L: the numbers of distinct values of the columns in the live rows of the snapshot, by Spark.
   */
  public static Map<String, Long> sparkDistinctCounts(
      TableIdentifier identifier, long snapshotId, Collection<String> columns)
      throws InterruptedException {
    StringBuilder select = new StringBuilder("concat('L'");
    for (String column : columns) {
      select.append(", '|").append(column).append("=', count(DISTINCT ").append(column).append(")");
    }

    select.append(")");
    String output =
        runSparkSqlQuery(
            String.format(
                "SELECT %s FROM spark_catalog.%s VERSION AS OF %s",
                select, identifier, snapshotId));
    return parseSparkCounts(output);
  }

  static Map<String, Long> parseSparkCounts(String output) {
    List<String> lines = output.lines().filter(line -> line.startsWith(SPARK_ROW_PREFIX)).toList();
    assertThat(lines).as("the result row of the Spark query in: %s", output).hasSize(1);
    Map<String, Long> counts = Maps.newLinkedHashMap();
    Matcher matcher = SPARK_COUNT.matcher(lines.get(0));
    while (matcher.find()) {
      counts.put(matcher.group(1), Long.parseLong(matcher.group(2)));
    }

    return counts;
  }

  /**
   * The live rows of a snapshot S, live(S): {@code total-records} of the summary before v3, {@code
   * total-records − total-position-deletes} from v3, or null if the summary lacks a field.
   */
  public static Long liveRows(Table table, Snapshot snapshot) {
    String records = snapshot.summary().get("total-records");
    if (records == null || TableUtil.formatVersion(table) < 3) {
      return records == null ? null : Long.parseLong(records);
    }

    String deletes = snapshot.summary().get("total-position-deletes");
    if (deletes == null) {
      return null;
    }

    return Long.parseLong(records) - Long.parseLong(deletes);
  }

  /**
   * The bounds of the NDV of a column, the number of the distinct values in the live rows L and the
   * number in everything sent U: with U below K = 2^lgK the sketch is exact and L ≤ ndv ≤ min(U,
   * live); otherwise the error of the sketch is 3/√K of the value.
   *
   * @param live live(S), or null if it is unknown
   * @return {lower, upper}
   */
  public static long[] ndvBounds(long lower, long upper, Long live, int lgK) {
    long nominalEntries = 1L << lgK;
    long low;
    long high;
    if (upper < nominalEntries) {
      low = lower;
      high = upper;
    } else {
      double error = 3.0 / Math.sqrt(nominalEntries);
      low = (long) Math.floor((1 - error) * lower);
      high = (long) Math.ceil((1 + error) * upper);
    }

    return new long[] {low, live == null ? high : Math.min(high, live)};
  }

  /**
   * For every blob of the record of the snapshot, its {@code ndv} is within its bounds. L is
   * counted by Spark on the snapshot, U comes from the values the test sent.
   *
   * @return L, the numbers of distinct values in the live rows, by column
   */
  public static Map<String, Long> assertNdvWithinBounds(
      Table table,
      TableIdentifier identifier,
      long snapshotId,
      StatisticsFile file,
      SentValues sent)
      throws InterruptedException {
    Snapshot snapshot = table.snapshot(snapshotId);
    Schema schema = table.schemas().get(snapshot.schemaId());
    List<String> columns = Lists.newArrayList();
    for (BlobMetadata blob : file.blobMetadata()) {
      columns.add(schema.findColumnName(blob.fields().get(0)));
    }

    Map<String, Long> lower = sparkDistinctCounts(identifier, snapshotId, columns);
    Long live = liveRows(table, snapshot);
    for (BlobMetadata blob : file.blobMetadata()) {
      String column = schema.findColumnName(blob.fields().get(0));
      long lowerCount = lower.get(column);
      long upperCount = sent.distinct(column);
      long[] bounds = ndvBounds(lowerCount, upperCount, live, DEFAULT_LG_K);
      assertThat(ndv(blob))
          .as(
              "ndv of column %s at snapshot %s (L=%s, U=%s, live=%s)",
              column, snapshotId, lowerCount, upperCount, live)
          .isBetween(bounds[0], bounds[1]);
    }

    return lower;
  }

  /**
   * The partitions of the table by Spark at the snapshot ({@code SELECT * FROM t.partitions VERSION
   * AS OF snapshotId}) in the form of {@link #readPartitionStats}: value of the partition field →
   * "rows|files".
   *
   * @param partitionField the name of the (first) partition field of the table
   */
  public static Map<String, String> sparkPartitionStats(
      TableIdentifier identifier, long snapshotId, String partitionField)
      throws InterruptedException {
    String output =
        runSparkSqlQuery(
            String.format(
                "SELECT concat('%s', partition.%s, '|', record_count, '|', file_count)"
                    + " FROM spark_catalog.%s.partitions VERSION AS OF %s",
                SPARK_PARTITION_PREFIX, partitionField, identifier, snapshotId));
    Map<String, String> partitions = Maps.newTreeMap();
    output
        .lines()
        .filter(line -> line.startsWith(SPARK_PARTITION_PREFIX))
        .map(line -> line.substring(SPARK_PARTITION_PREFIX.length()))
        .forEach(
            line -> {
              int split = line.indexOf('|');
              partitions.put(line.substring(0, split), line.substring(split + 1));
            });
    return partitions;
  }

  /** A record of the topic: a JSON object without the envelope of the schema. */
  public static final class Row extends BaseTestEvent {
    private final Map<String, Object> fields;

    /**
     * @param key the key of the Kafka record; the records of one key must go to one partition
     */
    public Row(long key, Map<String, Object> fields) {
      super(key);
      this.fields = fields;
    }

    public Map<String, Object> fields() {
      return fields;
    }

    @Override
    protected String serialize(boolean useSchema) {
      Map<String, Object> json = Maps.newLinkedHashMap();
      fields.forEach((name, value) -> json.put(name, jsonValue(value)));
      try {
        return TestContext.MAPPER.writeValueAsString(json);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    /** The JSON form of the value the record converter of the connector reads back. */
    static Object jsonValue(Object value) {
      if (value instanceof LocalDate || value instanceof UUID) {
        return value.toString();
      } else if (value instanceof BigDecimal) {
        return ((BigDecimal) value).toPlainString();
      } else if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
      }

      return value;
    }
  }

  /**
   * What Spark (CBO on) knows of the columns of the table: {@code distinctCount} of {@code
   * attributeStats} of the optimized plan of {@code spark.table(t)}, by column name; an empty
   * string for a column Spark has statistics for but no distinct count.
   */
  public static Map<String, String> sparkAttributeStats(TableIdentifier identifier)
      throws InterruptedException {
    String output =
        runSparkShell(
            "val stats = spark.table(\"spark_catalog."
                + identifier
                + "\").queryExecution.optimizedPlan.stats\n"
                + "stats.attributeStats.foreach { case (a, s) =>"
                + " println(\"NDV|\" + a.name + \"|\" + s.distinctCount.getOrElse(\"\")) }");
    return parseAttributeStats(output);
  }

  static Map<String, String> parseAttributeStats(String output) {
    Map<String, String> stats = Maps.newLinkedHashMap();
    Matcher matcher = SPARK_ATTRIBUTE_NDV.matcher(output);
    while (matcher.find()) {
      stats.put(matcher.group(1), matcher.group(2));
    }

    return stats;
  }

  /** Spark {@code compute_table_stats} of the table: the record of the head, all columns. */
  public static void sparkComputeTableStats(TableIdentifier identifier)
      throws InterruptedException {
    runSparkSqlQuery(
        String.format("CALL spark_catalog.system.compute_table_stats(table => '%s')", identifier));
  }

  /** Inserts the rows through Spark (one snapshot of a writer that is not the connector). */
  public static void sparkInsert(TableIdentifier identifier, Collection<Row> rows)
      throws InterruptedException {
    runSparkSqlQuery(sparkInsertStatement(identifier, rows));
  }

  /** The columns come from the first row, in its order; every row names the same ones. */
  static String sparkInsertStatement(TableIdentifier identifier, Collection<Row> rows) {
    List<String> columns = Lists.newArrayList(rows.iterator().next().fields().keySet());
    List<String> tuples = Lists.newArrayList();
    for (Row row : rows) {
      List<String> literals = Lists.newArrayList();
      for (String column : columns) {
        literals.add(sparkLiteral(row.fields().get(column)));
      }

      tuples.add("(" + String.join(", ", literals) + ")");
    }

    return String.format(
        "INSERT INTO spark_catalog.%s (%s) VALUES %s",
        identifier, String.join(", ", columns), String.join(", ", tuples));
  }

  /**
   * The literal of a value of the test rows in Spark SQL: a string escapes the quote and the
   * backslash with a backslash (a doubled quote would be two literals that Spark joins into one).
   */
  static String sparkLiteral(Object value) {
    if (value == null) {
      return "NULL";
    } else if (value instanceof String) {
      return "'" + ((String) value).replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    return value.toString();
  }

  /** Trino {@code ANALYZE} of the table: the record of the head, all columns. */
  public static void trinoAnalyze(TableIdentifier identifier) throws InterruptedException {
    TrinoClient.query("ANALYZE " + identifier);
  }

  /**
   * {@code SHOW STATS} of Trino: {@code distinct_values_count} by column name; a column with no
   * count (Trino shows nothing without an NDV blob) is absent.
   */
  public static Map<String, Long> trinoDistinctCounts(TableIdentifier identifier)
      throws InterruptedException {
    return parseShowStats(TrinoClient.query("SHOW STATS FOR " + identifier));
  }

  /** The rows of SHOW STATS: column_name, data_size, distinct_values_count, nulls_fraction, … */
  static Map<String, Long> parseShowStats(List<List<String>> rows) {
    Map<String, Long> counts = Maps.newLinkedHashMap();
    for (List<String> row : rows) {
      assertThat(row).as("a row of SHOW STATS").hasSizeGreaterThanOrEqualTo(3);
      // the last row, with no column name, holds the row count of the table
      if (!row.get(0).isEmpty() && !row.get(2).isEmpty()) {
        counts.put(row.get(0), Math.round(Double.parseDouble(row.get(2))));
      }
    }

    return counts;
  }

  /** Inserts the rows through Trino, which merges its sketches into the previous ones. */
  public static void trinoInsert(TableIdentifier identifier, List<Map<String, Object>> rows)
      throws InterruptedException {
    TrinoClient.query(trinoInsertStatement(identifier, rows));
  }

  static String trinoInsertStatement(TableIdentifier identifier, List<Map<String, Object>> rows) {
    List<String> columns = Lists.newArrayList(rows.get(0).keySet());
    StringBuilder statement =
        new StringBuilder("INSERT INTO ")
            .append(identifier)
            .append(" (")
            .append(String.join(", ", columns))
            .append(") VALUES ");
    for (int i = 0; i < rows.size(); i++) {
      List<String> literals = Lists.newArrayList();
      for (String column : columns) {
        literals.add(trinoLiteral(rows.get(i).get(column)));
      }

      statement
          .append(i == 0 ? "" : ", ")
          .append("(")
          .append(String.join(", ", literals))
          .append(")");
    }

    return statement.toString();
  }

  static String trinoLiteral(Object value) {
    if (value == null) {
      return "NULL";
    } else if (value instanceof String) {
      return "'" + ((String) value).replace("'", "''") + "'";
    } else if (value instanceof LocalDate) {
      return "DATE '" + value + "'";
    } else if (value instanceof BigDecimal) {
      return "DECIMAL '" + ((BigDecimal) value).toPlainString() + "'";
    } else if (value instanceof LocalDateTime) {
      return "TIMESTAMP '" + TRINO_TIMESTAMP.format((LocalDateTime) value) + "'";
    } else if (value instanceof UUID) {
      return "UUID '" + value + "'";
    }

    return value.toString();
  }

  /**
   * The metrics one task of a connector registers: the leader of the control topic registers them
   * when it starts the coordinator and drops them when it stops it, so a connector has one
   * registration at a time, and none between a stop of the coordinator and the next start.
   */
  public static final class MetricRegistration {
    private final ObjectName name;
    private final Map<String, Double> values;

    MetricRegistration(ObjectName name, Map<String, Double> values) {
      this.name = name;
      this.values = values;
    }

    public ObjectName name() {
      return name;
    }

    public String connector() {
      return name.getKeyProperty("connector");
    }

    public String task() {
      return name.getKeyProperty("task");
    }

    public Map<String, Double> values() {
      return values;
    }
  }

  /** The registrations of the metrics of the connector in the JMX of the worker. */
  public static List<MetricRegistration> metricsOf(String connector) {
    try (JMXConnector jmx = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL))) {
      MBeanServerConnection server = jmx.getMBeanServerConnection();
      List<MetricRegistration> registrations = Lists.newArrayList();
      for (ObjectName name : server.queryNames(new ObjectName(METRICS_OBJECT_NAME), null)) {
        if (!connector.equals(name.getKeyProperty("connector"))) {
          continue;
        }

        Map<String, Double> values = Maps.newTreeMap();
        for (MBeanAttributeInfo attribute : server.getMBeanInfo(name).getAttributes()) {
          if (METRICS.contains(attribute.getName())) {
            values.put(
                attribute.getName(),
                ((Number) server.getAttribute(name, attribute.getName())).doubleValue());
          }
        }

        if (!values.isEmpty()) {
          registrations.add(new MetricRegistration(name, values));
        }
      }

      return registrations;
    } catch (Exception e) {
      throw new IllegalStateException("Cannot read the JMX of the worker at " + JMX_URL, e);
    }
  }

  /** Waits until the connector has exactly one registration and the metric has the value. */
  public static void awaitMetric(String connector, String metric, double expected) {
    awaitMetric(connector, metric, expected, AWAIT_TIMEOUT);
  }

  public static void awaitMetric(
      String connector, String metric, double expected, Duration timeout) {
    Awaitility.await()
        .atMost(timeout)
        .pollInterval(POLL_INTERVAL)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              List<MetricRegistration> registrations = metricsOf(connector);
              assertThat(registrations)
                  .as("registrations of the metrics of connector %s", connector)
                  .hasSize(1);
              assertThat(registrations.get(0).values())
                  .as("metrics of connector %s, task %s", connector, registrations.get(0).task())
                  .containsEntry(metric, expected);
            });
  }

  /** Waits until the coordinator of the connector is stopped: its metrics are dropped. */
  public static void awaitNoMetrics(String connector) {
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .ignoreExceptions()
        .untilAsserted(
            () ->
                assertThat(metricsOf(connector))
                    .as("registrations of the metrics of connector %s", connector)
                    .isEmpty());
  }

  /** The log of the Connect container since the moment, one entry per line. */
  public static List<String> connectLog(Instant since) throws InterruptedException {
    String containerId = getContainer(CONNECT_IMAGE, CONNECT_SERVICE).getId();
    StringBuilder log = new StringBuilder();
    DOCKER_CLIENT
        .logContainerCmd(containerId)
        .withStdOut(true)
        .withStdErr(true)
        .withSince((int) since.getEpochSecond())
        .exec(
            new ResultCallback.Adapter<Frame>() {
              @Override
              public void onNext(Frame frame) {
                log.append(new String(frame.getPayload(), StandardCharsets.UTF_8));
              }
            })
        .awaitCompletion();
    return log.toString().lines().toList();
  }

  /** Waits until a line of the log of the Connect container since the moment satisfies the test. */
  public static void awaitConnectLog(Instant since, Predicate<String> line, String description) {
    awaitConnectLog(since, line, description, AWAIT_TIMEOUT);
  }

  public static void awaitConnectLog(
      Instant since, Predicate<String> line, String description, Duration timeout) {
    Awaitility.await()
        .atMost(timeout)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () ->
                assertThat(connectLog(since).stream().anyMatch(line))
                    .as("a line of the Connect log: %s", description)
                    .isTrue());
  }

  /** The line of {@code StatisticsHealth} that raises a state of the table (WARN). */
  public static Predicate<String> stateRaised(String state, TableIdentifier identifier) {
    String message = "Statistics state '" + state + "' of table " + identifier + " raised";
    return line -> line.contains("WARN") && line.contains(message);
  }

  /** The line of {@code StatisticsHealth} that raises a state of the table at the snapshot. */
  public static Predicate<String> stateRaised(
      String state, TableIdentifier identifier, long snapshotId) {
    String message =
        "Statistics state '" + state + "' of table " + identifier + " raised at snapshot ";
    return line -> line.contains("WARN") && line.contains(message + snapshotId + ":");
  }

  /** The line of {@code StatisticsHealth} that clears a state of the table (INFO). */
  public static Predicate<String> stateCleared(String state, TableIdentifier identifier) {
    String message = "Statistics state '" + state + "' of table " + identifier + " cleared";
    return line -> line.contains("INFO") && line.contains(message);
  }

  /** The line of the statistics step that committed the record of the snapshot (INFO). */
  public static Predicate<String> statisticsCommitted(TableIdentifier identifier, long snapshotId) {
    String message =
        "Committed statistics of table " + identifier + " at snapshot " + snapshotId + ":";
    return line -> line.contains("INFO") && line.contains(message);
  }

  /**
   * The line of the step that committed the record of the snapshot after it read at least one data
   * file to recount: the answers without a Puffin file are counted from their staged files.
   */
  public static Predicate<String> recounted(TableIdentifier identifier, long snapshotId) {
    return statisticsCommitted(identifier, snapshotId).and(line -> filesRead(line) > 0);
  }

  /** The count of files that a line of {@link #statisticsCommitted} says were read; 0 if none. */
  static long filesRead(String line) {
    Matcher matcher = FILES_READ.matcher(line);
    return matcher.find() ? Long.parseLong(matcher.group(1)) : 0L;
  }

  /** The WARN and ERROR entries of the log that name the table. */
  public static List<String> warningsOf(List<String> log, TableIdentifier identifier) {
    return log.stream()
        .filter(StatisticsItSupport::isWarning)
        .filter(line -> line.contains(identifier.toString()))
        .toList();
  }

  /**
   * The WARN and ERROR entries of the log written by the loggers of the statistics (the class name
   * closes the line: {@code (StatisticsStep)}, with the package and the line number or without
   * them), whether or not they name a table.
   */
  public static List<String> statisticsWarnings(List<String> log) {
    return log.stream()
        .filter(StatisticsItSupport::isWarning)
        .filter(StatisticsItSupport::writtenByStatisticsLogger)
        .toList();
  }

  /**
   * The entries of {@link #statisticsWarnings(List)} that name no table of the namespace of {@code
   * identifier} other than it, as {@code <namespace>.<name>}. The log since the start of a test
   * also holds the end of the previous test, and its tables are of the same namespace. An entry
   * that names no table is kept.
   */
  public static List<String> statisticsWarnings(List<String> log, TableIdentifier identifier) {
    Pattern tables =
        Pattern.compile("\\b" + Pattern.quote(identifier.namespace().toString()) + "\\.(\\w+)");
    return statisticsWarnings(log).stream()
        .filter(line -> namesNoOtherTable(line, tables, identifier.name()))
        .toList();
  }

  private static boolean namesNoOtherTable(String line, Pattern tables, String name) {
    Matcher matcher = tables.matcher(line);
    while (matcher.find()) {
      if (!matcher.group(1).equals(name)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Whether the line is closed by a logger of the statistics, the way {@link
   * #statisticsWarnings(List)} tells them. A line that the statistics surely logged has to pass it:
   * if the log pattern ended the lines otherwise, no line would pass, and a check that no warning
   * of the statistics is logged would hold whatever is logged.
   */
  public static boolean writtenByStatisticsLogger(String line) {
    return STATISTICS_LOGGER.matcher(line).find();
  }

  private static boolean isWarning(String line) {
    return line.contains("WARN") || line.contains("ERROR");
  }
}
