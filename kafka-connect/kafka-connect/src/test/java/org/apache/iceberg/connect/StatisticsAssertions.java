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
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardPuffinProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Checks of the statistics a test ends with: the files of the records exist, each record describes
 * its own snapshot, and no intermediate Puffin file is left behind.
 */
public final class StatisticsAssertions {
  private static final String CONNECTOR = "iceberg-kafka-connect";
  private static final String NDV_PROPERTY = "ndv";
  private static final String STATS_DIR = "/stats/";
  private static final String PUFFIN_SUFFIX = ".puffin";
  private static final long AWAIT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final long POLL_MS = 10L;
  private static final Pattern FINAL_FILE = Pattern.compile("/stats-\\d+-[^/]*\\.puffin$");

  private StatisticsAssertions() {}

  public static void awaitStatistics(Table table, long snapshotId, String stagingDirectory) {
    awaitStatistics(table, snapshotId, false, stagingDirectory);
  }

  /**
   * Waits for the asynchronous statistics step of S: until S has a record of every kind enabled
   * (NDV, and partition statistics if {@code partitionStatistics}), then until no intermediate
   * Puffin file is left under {@code <stagingDirectory>/stats/}, which the step deletes after its
   * commit. A test with a series of cycles sends the next one only after this, or the tasks of the
   * queue merge and S gets no record.
   */
  public static void awaitStatistics(
      Table table, long snapshotId, boolean partitionStatistics, String stagingDirectory) {
    await(
        String.format(Locale.ROOT, "the statistics records of snapshot %d", snapshotId),
        () -> {
          table.refresh();
          boolean ndv =
              table.statisticsFiles().stream().anyMatch(file -> file.snapshotId() == snapshotId);
          boolean partitions =
              table.partitionStatisticsFiles().stream()
                  .anyMatch(file -> file.snapshotId() == snapshotId);
          return ndv && (partitions || !partitionStatistics);
        });

    String statsDirectory =
        (stagingDirectory.endsWith("/") ? stagingDirectory : stagingDirectory + "/")
            + ColumnSketchCollector.STATS_DIR;
    await(
        "no intermediate Puffin files under " + statsDirectory,
        () -> filesUnder(statsDirectory).stream().noneMatch(path -> path.endsWith(PUFFIN_SUFFIX)));
  }

  /** Polls {@code condition} until it holds, failing the test after {@link #AWAIT_MS}. */
  public static void await(String what, BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AWAIT_MS);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }

      try {
        Thread.sleep(POLL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted waiting for " + what, e);
      }
    }
  }

  public static void assertStatisticsFilesExist(Table table) {
    table.refresh();
    for (StatisticsFile file : table.statisticsFiles()) {
      assertThat(table.io().newInputFile(file.path()).exists())
          .as(
              "the file of the NDV record of snapshot %s exists: %s",
              file.snapshotId(), file.path())
          .isTrue();
    }

    for (PartitionStatisticsFile file : table.partitionStatisticsFiles()) {
      assertThat(table.io().newInputFile(file.path()).exists())
          .as(
              "the file of the partition statistics record of snapshot %s exists: %s",
              file.snapshotId(), file.path())
          .isTrue();
    }
  }

  /**
   * Every NDV record the connector wrote stands on the snapshot it describes: the name of the file,
   * the snapshot id of each blob in the record and in the file, and the sequence number of each
   * blob in the file are those of the snapshot of the record, and the {@code ndv} of each blob is
   * at most {@code live(S)}. A record whose file is missing is skipped only when the connector
   * could not have written it: one with the name the connector gives its files fails.
   */
  public static void assertStatisticsOnOwnSnapshot(Table table) {
    table.refresh();
    for (StatisticsFile file : table.statisticsFiles()) {
      if (!fileExists(table, file)) {
        // the file of a record somebody else made may be anywhere, and its existence is
        // assertStatisticsFilesExist's to check
        assertThat(hasConnectorsName(table, file))
            .as(
                "the record of snapshot %s names a file of the connector that is gone: %s",
                file.snapshotId(), file.path())
            .isFalse();
        continue;
      }

      FileMetadata footer = footer(table, file);
      String createdBy = footer.properties().get(StandardPuffinProperties.CREATED_BY_PROPERTY);
      if (createdBy == null || !createdBy.startsWith(CONNECTOR)) {
        continue;
      }

      long snapshotId = file.snapshotId();
      assertThat(file.path())
          .as("the name of the connector's NDV file of snapshot %s", snapshotId)
          .contains(String.format(Locale.ROOT, "/stats-%d-", snapshotId));
      for (BlobMetadata blob : file.blobMetadata()) {
        assertThat(blob.sourceSnapshotId())
            .as("the snapshot of blob %s in the record of snapshot %s", blob.fields(), snapshotId)
            .isEqualTo(snapshotId);
      }

      Snapshot snapshot = table.snapshot(snapshotId);
      assertThat(snapshot).as("snapshot %s of an NDV record", snapshotId).isNotNull();
      Long live = live(table, snapshot);
      for (org.apache.iceberg.puffin.BlobMetadata blob : footer.blobs()) {
        assertThat(blob.snapshotId())
            .as(
                "the snapshot of blob %s in the file of snapshot %s",
                blob.inputFields(), snapshotId)
            .isEqualTo(snapshotId);
        assertThat(blob.sequenceNumber())
            .as(
                "the sequence number of blob %s in the file of snapshot %s",
                blob.inputFields(), snapshotId)
            .isEqualTo(snapshot.sequenceNumber());
        String ndv = blob.properties().get(NDV_PROPERTY);
        if (live != null && ndv != null) {
          assertThat(Long.parseLong(ndv))
              .as(
                  "the ndv of blob %s in the file of snapshot %s is at most live(S)",
                  blob.inputFields(), snapshotId)
              .isLessThanOrEqualTo(live);
        }
      }
    }
  }

  /**
   * {@code live(S)}: total records; from v3 less total position deletes, not below zero; or null
   * without them. Before v3 dangling position deletes may count rows that are gone.
   */
  private static Long live(Table table, Snapshot snapshot) {
    String records = snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP);
    if (records == null || TableUtil.formatVersion(table) < 3) {
      return records == null ? null : Long.parseLong(records);
    }

    String positionDeletes = snapshot.summary().get(SnapshotSummary.TOTAL_POS_DELETES_PROP);
    if (positionDeletes == null) {
      return null;
    }

    return Math.max(0L, Long.parseLong(records) - Long.parseLong(positionDeletes));
  }

  /**
   * Whether the record names its file as the connector does: {@code stats-<snapshotId>-*.puffin} in
   * the metadata directory of the table.
   */
  private static boolean hasConnectorsName(Table table, StatisticsFile file) {
    String prefix = "stats-" + file.snapshotId() + "-";
    String location =
        table instanceof HasTableOperations
            ? ((HasTableOperations) table).operations().metadataFileLocation(prefix)
            : table.location() + "/metadata/" + prefix;
    return file.path().startsWith(location) && file.path().endsWith(PUFFIN_SUFFIX);
  }

  /**
   * The intermediate Puffin files under the staging directory ({@code <dir>/stats/*.puffin} at any
   * depth) are only the ones still in flight.
   */
  public static void assertNoStrayPuffins(String stagingDir, Collection<String> inFlight) {
    Set<String> expected =
        inFlight.stream().map(StatisticsAssertions::normalize).collect(Collectors.toSet());
    List<String> puffins =
        filesUnder(stagingDir).stream()
            .filter(path -> path.contains(STATS_DIR) && path.endsWith(PUFFIN_SUFFIX))
            .collect(Collectors.toList());
    assertThat(puffins)
        .as("the intermediate Puffin files under %s are only those in flight", stagingDir)
        .allMatch(expected::contains);
  }

  public static void assertNoFilesUnder(String dir) {
    assertThat(filesUnder(dir)).as("the files under %s", dir).isEmpty();
  }

  /**
   * The control of {@link #assertNoFilesUnder}: something under {@code dir}, so that an emptiness
   * check of a location is known to see the files of the {@code FileIO} the test writes to.
   */
  public static void assertSomeFilesUnder(String dir) {
    assertThat(filesUnder(dir)).as("the files under %s", dir).isNotEmpty();
  }

  /**
   * Every snapshot of the table has at most one NDV record, and every {@code stats-<S>-*.puffin}
   * file of the connector in the metadata directory of the table is the file of a record: none is
   * left without one by a step that lost to another of the same S.
   */
  public static void assertOneRecordPerSnapshotAndNoStrayFinalFiles(Table table) {
    table.refresh();
    Map<Long, List<String>> recordsBySnapshot = Maps.newHashMap();
    for (StatisticsFile file : table.statisticsFiles()) {
      recordsBySnapshot
          .computeIfAbsent(file.snapshotId(), id -> Lists.newArrayList())
          .add(normalize(file.path()));
    }

    recordsBySnapshot.forEach(
        (snapshotId, paths) ->
            assertThat(paths).as("the NDV records of snapshot %s", snapshotId).hasSize(1));

    Set<String> recorded =
        recordsBySnapshot.values().stream().flatMap(List::stream).collect(Collectors.toSet());
    String metadata = table.location() + "/metadata/";
    List<String> finals =
        filesUnder(metadata).stream()
            .map(StatisticsAssertions::normalize)
            .filter(path -> FINAL_FILE.matcher(path).find())
            .collect(Collectors.toList());
    assertThat(finals)
        .as("the stats-<S>-* files under %s are those of the records", metadata)
        .allMatch(recorded::contains);
  }

  private static String normalize(String location) {
    return location.startsWith("file:") ? URI.create(location).getPath() : location;
  }

  private static boolean fileExists(Table table, StatisticsFile file) {
    try {
      return table.io().newInputFile(file.path()).exists();
    } catch (NotFoundException e) {
      return false;
    }
  }

  private static FileMetadata footer(Table table, StatisticsFile file) {
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

  /** {@code dir} is an in-memory location of {@link InMemoryFileIO} or a local one. */
  private static List<String> filesUnder(String dir) {
    String prefix = dir.endsWith("/") ? dir : dir + "/";
    if (dir.startsWith("file:") || dir.startsWith("/")) {
      Path root = Paths.get(normalize(dir));
      if (!Files.exists(root)) {
        return Lists.newArrayList();
      }

      try (Stream<Path> files = Files.walk(root)) {
        return files
            .filter(Files::isRegularFile)
            .map(path -> path.toAbsolutePath().toString())
            .collect(Collectors.toList());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return inMemoryFiles().keySet().stream()
        .filter(path -> path.startsWith(prefix))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, byte[]> inMemoryFiles() {
    try {
      Field files = InMemoryFileIO.class.getDeclaredField("IN_MEMORY_FILES");
      files.setAccessible(true);
      return (Map<String, byte[]>) files.get(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot list the in-memory files", e);
    }
  }
}
