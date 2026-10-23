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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;

/**
 * The partition statistics of the statistics step: whether snapshot S needs them, and their file
 * computed and written. Nothing is committed here.
 */
final class PartitionStatisticsWriter {
  // the output file of the format probe: the probe only creates a writer builder
  private static final OutputFile PROBE_FILE =
      new OutputFile() {
        @Override
        public PositionOutputStream create() {
          throw new UnsupportedOperationException("Cannot create the probe file");
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
          throw new UnsupportedOperationException("Cannot create the probe file");
        }

        @Override
        public String location() {
          return "partition-statistics-format-probe";
        }

        @Override
        public InputFile toInputFile() {
          throw new UnsupportedOperationException("Cannot read the probe file");
        }
      };

  private final Logger log;
  // tables warned about a format without a writer of partition statistics; a supported format
  // takes the table out, so that the warning repeats if the format goes back
  private final Set<TableIdentifier> unsupportedFormatWarned = Sets.newConcurrentHashSet();

  PartitionStatisticsWriter(Logger log) {
    this.log = log;
  }

  /** What the computation of the partition statistics of S came to. */
  static final class Computed {
    private final PartitionStatisticsFile file;
    private final Throwable failure;

    private Computed(PartitionStatisticsFile file, Throwable failure) {
      this.file = file;
      this.failure = failure;
    }

    /** The new file, or {@code null}: the statistics are empty, registered already, or failed. */
    PartitionStatisticsFile file() {
      return file;
    }

    Throwable failure() {
      return failure;
    }
  }

  boolean needed(
      TableIdentifier identifier, Table table, long snapshotId, TableSinkConfig tableConfig) {
    return tableConfig.partitionStatsEnabled()
        && Partitioning.isPartitioned(table)
        && !StatisticsRecords.hasPartitionStatistics(table, snapshotId)
        && formatSupported(identifier, table);
  }

  /**
   * Whether the partition statistics can be written in {@code write.format.default}: a format
   * without a writer would fail every step after a full scan of the manifests.
   */
  private boolean formatSupported(TableIdentifier identifier, Table table) {
    String format =
        table
            .properties()
            .getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    if (hasInternalWriter(format)) {
      unsupportedFormatWarned.remove(identifier);
      return true;
    }

    if (unsupportedFormatWarned.add(identifier)) {
      log.warn(
          "Not writing the partition statistics of table {}: format {} "
              + "(write.format.default) is not supported for them",
          identifier,
          format);
    } else {
      log.debug(
          "Not writing the partition statistics of table {}: format {} is not supported",
          identifier,
          format);
    }

    return false;
  }

  /**
   * Computes and writes the partition statistics of S. Any exception, a {@link LinkageError} too
   * (say, a codec missing from the plugin), is returned as the failure; once stopped, it is thrown
   * instead. A file whose record is already there is somebody else's and is not returned.
   */
  Computed compute(
      TableIdentifier identifier, Table table, long snapshotId, BooleanSupplier stopped) {
    boolean full = withoutAncestorFile(table, snapshotId);
    if (full) {
      log.info(
          "Computing the partition statistics of table {} at snapshot {} in full: "
              + "no ancestor has partition statistics",
          identifier,
          snapshotId);
    }

    long start = System.nanoTime();
    PartitionStatisticsFile file;
    try {
      file = PartitionStatsHandler.computeAndWriteStatsFile(table, snapshotId);
    } catch (IOException | RuntimeException | LinkageError e) {
      if (stopped.getAsBoolean()) {
        Throwables.throwIfUnchecked(e);
        throw new UncheckedIOException((IOException) e);
      }

      return new Computed(null, e);
    }

    if (full) {
      log.info(
          "Computed the partition statistics of table {} at snapshot {} in full in {} ms",
          identifier,
          snapshotId,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    if (file == null) {
      log.debug(
          "No partition statistics of table {} at snapshot {}: they are empty",
          identifier,
          snapshotId);
      return new Computed(null, null);
    }

    if (isRegistered(table, file)) {
      log.debug(
          "Partition statistics of table {} at snapshot {} are already registered: {}",
          identifier,
          snapshotId,
          file.path());
      return new Computed(null, null);
    }

    return new Computed(file, null);
  }

  static boolean isRegistered(Table table, PartitionStatisticsFile file) {
    return table.partitionStatisticsFiles().stream()
        .anyMatch(
            registered ->
                registered.snapshotId() == file.snapshotId()
                    && registered.path().equals(file.path()));
  }

  /** Whether {@link InternalData} has a writer for the format; the builder writes nothing. */
  private static boolean hasInternalWriter(String format) {
    try {
      InternalData.write(FileFormat.fromString(format), PROBE_FILE);
      return true;
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      return false;
    }
  }

  /** Whether no ancestor of S has partition statistics: then they are computed in full. */
  private static boolean withoutAncestorFile(Table table, long snapshotId) {
    Set<Long> withFile = Sets.newHashSet();
    table.partitionStatisticsFiles().forEach(file -> withFile.add(file.snapshotId()));
    if (withFile.isEmpty()) {
      return true;
    }

    for (Snapshot snapshot : SnapshotUtil.ancestorsOf(snapshotId, table::snapshot)) {
      if (withFile.contains(snapshot.snapshotId())) {
        return false;
      }
    }

    return true;
  }
}
