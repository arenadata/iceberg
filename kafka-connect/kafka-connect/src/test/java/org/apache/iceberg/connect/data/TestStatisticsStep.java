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

import static org.apache.iceberg.connect.StatisticsAssertions.assertNoStrayPuffins;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.EndlessAvroFile;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.NdvBounds;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The statistics step of snapshot S from start to end: records that are already there, the NDV, the
 * partition statistics, the re-check, the transaction with the last N records, the files of the
 * step, the intermediate Puffin files, the outcome and the health transition, and the failures
 * around them.
 */
public class TestStatisticsStep {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final int LG_K = 12;
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()),
              optional(2, "data", Types.StringType.get()),
              optional(3, "score", Types.IntegerType.get())),
          Set.of(1));
  private static final Flags ALL_UP = new Flags(true, true, true);

  private InMemoryCatalog catalog;
  private String staging;
  private StepFileIO io;
  private ControlledOperations ops;
  // the table as other writers see it: data commits and records of other writers go through it
  private Table data;
  // the table of the step: its operations and FileIO are the test's
  private Table table;
  private StatisticsHealth health;
  private final List<String> logs = new CopyOnWriteArrayList<>();
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private StatisticsStep step;
  private IcebergSinkConfig config;
  private TableSinkConfig tableConfig;
  private int fileCount = 0;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(
        "mem",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION, "mem://warehouse/" + UUID.randomUUID()));
    catalog.createNamespace(NAMESPACE);
    staging = "mem://staging/" + UUID.randomUUID();
    io = new StepFileIO();
    health = new StatisticsHealth(0L);
    step = new StatisticsStep(NdvTestFiles.recordingLogger(logs));
    config = config(10, 1_000);
    tableConfig = tableConfig(true, null);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void ownSnapshotGetsBothKindsInOneTransactionAndPuffinsAreDeleted() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    long snapshotId = answer.snapshotId();
    UUID commitId = UUID.randomUUID();

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(result.failure()).isNull();
    assertThat(ops.commits()).as("one transaction for both kinds").isEqualTo(1);
    StatisticsFile ndv = ndvRecord(snapshotId);
    assertThat(ndv.path()).isEqualTo(NdvComputation.fileLocation(table, snapshotId, commitId));
    assertThat(ndv.blobMetadata()).hasSize(3);
    assertThat(partitionRecord(snapshotId)).isNotNull();
    NdvBounds.of(data, snapshotId, "id")
        .written(LongStream.range(0, 10).boxed().collect(Collectors.toList()))
        .checkRecorded(LG_K);
    assertThat(logs)
        .anyMatch(
            line ->
                line.startsWith("INFO Committed statistics of table " + TABLE)
                    && line.contains("3 NDV blobs in " + ndv.path()));
    assertThat(health.count(flags -> true)).as("the NDV transition applied").isEqualTo(1);
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertNoStrayPuffins(staging, List.of());
  }

  @Test
  public void stoppedBeforeStepDoesNothing() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    stop.set(true);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(result.failure()).isNull();
    assertThat(ops.refreshes()).isZero();
    assertThat(ops.commits()).isZero();
    assertThat(io.created()).isEmpty();
    assertPuffinsKept(answer);
  }

  @Test
  public void snapshotExpiredBeforeStepIsGoneAndPuffinsAreDeleted() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    StatisticsAnswer next = commitOwn(rows("a", 10, 20));
    data.expireSnapshots().expireSnapshotId(answer.snapshotId()).cleanExpiredFiles(false).commit();

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.SNAPSHOT_GONE);
    assertThat(ops.commits()).isZero();
    assertThat(io.created()).isEmpty();
    assertThat(io.fileExists(answer.sketchFile().location())).isFalse();
    assertNoStrayPuffins(staging, List.of(next.sketchFile().location()));
  }

  @Test
  public void snapshotExpiredDuringStepIsGoneAndNothingIsCommittedForIt() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    StatisticsAnswer next = commitOwn(rows("a", 10, 20));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    AtomicBoolean expired = new AtomicBoolean(false);
    // the re-check before the commit is the first refresh after the NDV file is written
    ops.beforeRefresh(
        () -> {
          if (!io.created("/stats-").isEmpty() && expired.compareAndSet(false, true)) {
            data.expireSnapshots()
                .expireSnapshotId(answer.snapshotId())
                .cleanExpiredFiles(false)
                .commit();
          }
        });

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.SNAPSHOT_GONE);
    assertThat(expired).as("S expired by the re-check").isTrue();
    assertThat(ops.commits()).as("no record of an expired snapshot").isZero();
    assertThat(io.fileExists(path)).as("the NDV file of the step is deleted").isFalse();
    assertThat(health.version(TABLE)).as("no health transition").isZero();
    assertNoStrayPuffins(staging, List.of(next.sketchFile().location()));
  }

  @Test
  public void healthRemovedDuringStepIsNotRestoredByItsTransition() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    health.update(TABLE, flags -> ALL_UP);
    AtomicBoolean removed = new AtomicBoolean(false);
    // the table is found gone by another path (a re-check) while the step runs
    ops.beforeRefresh(
        () -> {
          if (!io.created("/stats-").isEmpty() && removed.compareAndSet(false, true)) {
            health.remove(TABLE);
          }
        });

    Result result = step.run(own(answer).build());

    assertThat(health.count(flags -> true)).as("the record removed meanwhile").isZero();
    assertThat(removed).as("removed by the re-check").isTrue();
    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(answer.snapshotId())).isNotNull();
  }

  @Test
  public void healthChangedDuringStepTakesItsTransition() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    health.update(TABLE, flags -> ALL_UP);
    AtomicBoolean evaluated = new AtomicBoolean(false);
    // an evaluation by the head (or a concurrent step) changes the record while the step runs
    ops.beforeRefresh(
        () -> {
          if (!io.created("/stats-").isEmpty() && evaluated.compareAndSet(false, true)) {
            health.update(TABLE, flags -> new Flags(false, true, false));
          }
        });

    Result result = step.run(own(answer).build());

    assertThat(health.count(Flags::any)).as("the step ended last puts the flags down").isZero();
    assertThat(evaluated).as("changed by the evaluation").isTrue();
    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(health.count(flags -> true)).as("the record is kept").isEqualTo(1);
  }

  @Test
  public void droppedTableIsGonePuffinsAreDeletedAndItsHealthRemoved() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    health.update(TABLE, flags -> ALL_UP);
    ops.failRefresh(() -> new NoSuchTableException("Table does not exist: %s", TABLE));

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.TABLE_GONE);
    assertThat(result.failure()).isNull();
    assertThat(health.version(TABLE)).as("the health record is removed").isZero();
    assertPuffinsDeleted(answer);
  }

  @Test
  public void stepOfLagOnTableWithoutHeadIsNotNeeded() {
    createTable(false);

    Result result = step.run(input().build());

    assertThat(result.outcome()).isEqualTo(Outcome.NOT_NEEDED);
    assertThat(ops.refreshes()).as("the head is looked up after a refresh").isPositive();
    assertThat(ops.commits()).isZero();
  }

  @Test
  public void stepOfLagOnIntermediateSliceIsDeferred() {
    createTable(false);
    commitOwn(rows("a", 0, 10));
    data.newAppend()
        .appendFile(writeDataFile(rows("a", 10, 20)))
        .set(NdvChain.CHANGE_SET_ID_PROP, "change-set-1")
        .commit();

    Result result = step.run(input().build());

    assertThat(result.outcome()).isEqualTo(Outcome.DEFERRED);
    assertThat(ops.commits()).isZero();
    assertThat(io.created()).isEmpty();
    assertThat(health.version(TABLE)).isZero();
  }

  @Test
  public void unexpectedExceptionFailsStepWithoutThrowing() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    IllegalStateException unavailable = new IllegalStateException("catalog unavailable");
    ops.failRefresh(() -> unavailable);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isSameAs(unavailable);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void exceptionOnceStoppedIsStopAndKeepsPuffins() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    ops.failRefresh(
        () -> {
          stop.set(true);
          return new UncheckedIOException(new InterruptedIOException("request interrupted"));
        });

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(result.failure()).isNull();
    assertPuffinsKept(answer);
  }

  @Test
  public void ndvRecordOfAnotherWriterSkipsNdvAndPartitionStatisticsAreWritten() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    StatisticsFile trino = foreignNdv(answer.snapshotId());

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(answer.snapshotId()).path()).isEqualTo(trino.path());
    assertThat(io.created("/stats-")).as("no NDV file is written").isEmpty();
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertThat(health.version(TABLE)).as("the NDV was not calculated").isZero();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void recordsOfBothKindsAlreadyThereAreNotNeeded() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    StatisticsFile trino = foreignNdv(answer.snapshotId());
    PartitionStatisticsFile spark = foreignPartitions(answer.snapshotId());

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.NOT_NEEDED);
    assertThat(ops.commits()).isZero();
    assertThat(io.created()).isEmpty();
    assertThat(ndvRecord(answer.snapshotId()).path()).isEqualTo(trino.path());
    assertThat(partitionRecord(answer.snapshotId()).path()).isEqualTo(spark.path());
    assertPuffinsDeleted(answer);
  }

  @Test
  public void notNeededAfterMissingBaselineLeavesFlagToRecheck() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    foreignNdv(answer.snapshotId());
    health.update(TABLE, flags -> new Flags(false, true, false));

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.NOT_NEEDED);
    assertThat(health.count(Flags::missingBaseline))
        .as("only the recheck of the head clears the flag")
        .isEqualTo(1);
    assertThat(logs).noneMatch(line -> line.contains("'missingBaseline'"));
  }

  @Test
  public void partitionStatisticsWithoutAncestorFileAreFullRecomputeLoggedBeforeAndAfter() {
    createTable(true);
    StatisticsAnswer first = commitOwn(rows("a", 0, 10));

    assertThat(step.run(own(first).build()).outcome()).isEqualTo(Outcome.WRITTEN);

    assertThat(partitionRecord(first.snapshotId())).isNotNull();
    assertThat(logs)
        .anyMatch(
            line ->
                line.equals(
                    "INFO Computing the partition statistics of table "
                        + TABLE
                        + " at snapshot "
                        + first.snapshotId()
                        + " in full: no ancestor has partition statistics"));
    assertThat(logs)
        .anyMatch(
            line ->
                line.startsWith(
                        "INFO Computed the partition statistics of table "
                            + TABLE
                            + " at snapshot "
                            + first.snapshotId()
                            + " in full in ")
                    && line.endsWith(" ms"));

    logs.clear();
    StatisticsAnswer second = commitOwn(rows("b", 10, 20));
    assertThat(step.run(own(second).build()).outcome()).isEqualTo(Outcome.WRITTEN);

    assertThat(partitionRecord(second.snapshotId())).isNotNull();
    assertThat(logs)
        .as("the parent has a file: an increment, not a full recompute")
        .noneMatch(line -> line.contains("in full"));
    assertStatisticsFilesExist(data);
  }

  @Test
  public void partitionStatisticsOfOrcTableAreNotNeededWithOneWarning() {
    createTable(true);
    StatisticsAnswer first = commitOwn(rows("a", 0, 10));
    data.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "orc").commit();

    Result result = step.run(own(first).build());

    assertThat(result.outcome())
        .as("a format without a writer of partition statistics is not a failure")
        .isEqualTo(Outcome.WRITTEN);
    assertThat(result.failure()).isNull();
    assertThat(ndvRecord(first.snapshotId())).isNotNull();
    assertThat(partitionRecord(first.snapshotId())).isNull();
    assertThat(logs).as("no full recompute").noneMatch(line -> line.contains("in full"));

    StatisticsAnswer second = commitOwn(rows("b", 10, 20));
    assertThat(step.run(own(second).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(step.run(input().build()).outcome())
        .as("a lag step at a head with the NDV")
        .isEqualTo(Outcome.NOT_NEEDED);

    assertThat(partitionRecord(second.snapshotId())).isNull();
    assertThat(io.created(data.location() + "/metadata/partition-stats-")).isEmpty();
    assertThat(logs.stream().filter(line -> line.contains("Not writing the partition statistics")))
        .as("one warning per table, then DEBUG")
        .containsExactly(
            "WARN Not writing the partition statistics of table "
                + TABLE
                + ": format orc (write.format.default) is not supported for them",
            "DEBUG Not writing the partition statistics of table "
                + TABLE
                + ": format orc is not supported",
            "DEBUG Not writing the partition statistics of table "
                + TABLE
                + ": format orc is not supported");
    assertStatisticsFilesExist(data);
  }

  @Test
  public void partitionStatisticsAreWrittenAgainOnceFormatIsSupported() {
    createTable(true);
    StatisticsAnswer first = commitOwn(rows("a", 0, 10));
    data.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "orc").commit();
    assertThat(step.run(own(first).build()).outcome()).isEqualTo(Outcome.WRITTEN);

    data.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "avro").commit();
    StatisticsAnswer second = commitOwn(rows("b", 10, 20));
    assertThat(step.run(own(second).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(partitionRecord(second.snapshotId())).isNotNull();

    data.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, "orc").commit();
    StatisticsAnswer third = commitOwn(rows("c", 20, 30));
    assertThat(step.run(own(third).build()).outcome()).isEqualTo(Outcome.WRITTEN);

    assertThat(partitionRecord(third.snapshotId())).isNull();
    assertThat(logs.stream().filter(line -> line.startsWith("WARN Not writing the partition")))
        .as("the warning again after a supported format")
        .hasSize(2);
  }

  @Test
  public void rejectedRegistrationDeletesNewFilesOfBothKinds() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    ops.failCommits(() -> new ForbiddenException("Not allowed to update table %s", TABLE));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(ForbiddenException.class);
    String ndvPath = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.deleted()).contains(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isFalse();
    assertThat(io.fileExists(partitionFiles.get(0))).isFalse();
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(data.partitionStatisticsFiles()).isEmpty();
    assertThat(health.version(TABLE)).as("no health transition on a failed commit").isZero();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void emptyPartitionStatisticsWriteNdvWithoutPartitionRecord() {
    createTable(true);
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    tableConfig = tableConfig(false, null);
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    data.refresh();
    assertThat(data.partitionStatisticsFiles()).isEmpty();

    // every row deleted; the next commit drops the manifest of the deleted entries: S has no
    // manifests, the partition statistics are empty ("empty branch case" of the core)
    data.refresh();
    data.newDelete().deleteFile(base.dataFiles().get(0)).commit();
    data.newAppend().commit();
    long snapshotId = data.currentSnapshot().snapshotId();
    tableConfig = tableConfig(true, null);

    Result result = step.run(input().build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(snapshotId)).isNotNull();
    assertThat(partitionRecord(snapshotId)).isNull();
    assertThat(io.created("/partition-stats-")).isEmpty();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
  }

  @Test
  public void partitionRecordPickedUpByRefreshOfNdvIsReturnedAndNotDeleted() {
    createTable(true);
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    String basePath = ndvRecord(base.snapshotId()).path();
    StatisticsAnswer answer = commitOwn(rows("b", 10, 20));
    // the file of the base record is not found: NdvBaseline refreshes the table to see whether
    // the record still points to it; a record of Spark for S lands between the check for records
    // and the partition statistics
    io.failReads(basePath, () -> new NotFoundException("File does not exist: %s", basePath));
    int baseReads = io.opened(basePath);
    List<PartitionStatisticsFile> spark = Lists.newArrayList();
    ops.beforeRefresh(
        () -> {
          if (io.opened(basePath) > baseReads && spark.isEmpty()) {
            spark.add(foreignPartitions(answer.snapshotId()));
          }
        });
    int commits = ops.commits();
    List<String> partitionFiles = List.copyOf(io.created("/partition-stats-"));

    Result result = step.run(own(answer).build());

    assertThat(spark).as("registered on the refresh of the NDV").hasSize(1);
    String foreign = spark.get(0).path();
    assertThat(result.outcome())
        .as("the NDV has no baseline, the partition statistics are not needed")
        .isEqualTo(Outcome.FINAL);
    assertThat(result.failure()).isNull();
    assertThat(ops.commits()).isEqualTo(commits);
    assertThat(io.created("/partition-stats-"))
        .as("the core returned the registered record, no new file")
        .isEqualTo(partitionFiles);
    assertThat(io.deleted()).doesNotContain(foreign);
    assertThat(io.fileExists(foreign)).as("the file of the foreign record is kept").isTrue();
    assertThat(partitionRecord(answer.snapshotId()).path()).isEqualTo(foreign);
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(health.count(Flags::missingBaseline)).isEqualTo(1);
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void emptySetOfColumnsWritesPartitionStatisticsWithoutNdv() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    health.update(TABLE, flags -> ALL_UP);
    tableConfig = tableConfig(true, ImmutableList.of());

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(io.created("/stats-")).isEmpty();
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertThat(health.count(Flags::any)).as("the flags are put down").isZero();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void emptySetOfColumnsOnUnpartitionedTableIsNotNeeded() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    tableConfig = tableConfig(true, ImmutableList.of());

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.NOT_NEEDED);
    assertThat(ops.commits()).isZero();
    assertThat(io.created()).isEmpty();
    assertPuffinsDeleted(answer);
  }

  @Test
  public void ndvOverLimitIsFinalRaisesFlagAndCommitsNothing() {
    createTable(false);
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    data.newAppend().appendFile(writeDataFile(rows("a", 10, 20))).commit();
    long snapshotId = data.currentSnapshot().snapshotId();
    config = config(10, 0);
    int commits = ops.commits();

    Result result = step.run(input().build());

    assertThat(result.outcome()).isEqualTo(Outcome.FINAL);
    assertThat(ops.commits()).isEqualTo(commits);
    assertThat(ndvRecord(snapshotId)).isNull();
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertThat(logs)
        .anyMatch(
            line ->
                line.startsWith("WARN Statistics state 'gapOverLimit' of table " + TABLE)
                    && line.contains("at snapshot " + snapshotId));
  }

  @Test
  public void ndvOverLimitDeletesPuffinsOfTaskAtOnce() {
    createTable(false);
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    data.newAppend().appendFile(writeDataFile(rows("a", 10, 20))).commit();
    StatisticsAnswer answer = commitOwn(rows("a", 20, 30));
    config = config(10, 0);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FINAL);
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void ndvOverLimitWithPartitionStatisticsWrittenIsWrittenAndRaisesFlag() {
    createTable(true);
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    data.newAppend().appendFile(writeDataFile(rows("a", 10, 20))).commit();
    long snapshotId = data.currentSnapshot().snapshotId();
    config = config(10, 0);

    Result result = step.run(input().build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(snapshotId)).isNull();
    assertThat(partitionRecord(snapshotId)).isNotNull();
    assertThat(ndvRecord(base.snapshotId())).as("NDV records are not removed").isNotNull();
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertThat(logs).anyMatch(line -> line.startsWith("WARN Statistics state 'gapOverLimit'"));
  }

  @Test
  public void columnAddedToSetWithoutBaseBlobIsMissingBaselineAndOthersAreWritten() {
    createTable(false);
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    tableConfig = tableConfig(true, ImmutableList.of("id", "data"));
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(base.snapshotId()).blobMetadata()).hasSize(2);

    StatisticsAnswer answer = commitOwn(rows("a", 10, 20));
    tableConfig = tableConfig(true, ImmutableList.of("id", "data", "score"));
    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(answer.snapshotId()).blobMetadata())
        .extracting(blob -> blob.fields().get(0))
        .containsExactly(1, 2);
    assertThat(health.count(Flags::missingBaseline)).isEqualTo(1);
    assertStatisticsOnOwnSnapshot(data);
  }

  @Test
  public void answerPuffinOfSmallerSetIsRecountedFromDataFiles() {
    createTable(false);
    tableConfig = tableConfig(true, ImmutableList.of("id", "data"));
    Record[] rows = rows("a", 0, 10);
    DataFile file = writeDataFile(rows);
    SketchFile sketch = puffin(ImmutableList.of("id"), rows);
    data.newAppend().appendFile(file).commit();
    long snapshotId = data.currentSnapshot().snapshotId();
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), sketch);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(snapshotId).blobMetadata()).hasSize(2);
    NdvBounds.of(data, snapshotId, "data").written("a").checkRecorded(LG_K);
    assertThat(logs)
        .anyMatch(line -> line.startsWith("INFO Committed") && line.contains("; 1 files, "));
    assertPuffinsDeleted(answer);
  }

  @Test
  public void existingFinalFileFailsNdvKeepsFileAndCommitsPartitions() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    byte[] planted = "planted".getBytes(StandardCharsets.UTF_8);
    io.addFile(path, planted);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(AlreadyExistsException.class);
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertThat(io.deleted()).doesNotContain(path);
    assertThat(read(path)).as("the planted file is intact").isEqualTo(planted);
    assertThat(health.version(TABLE)).as("the NDV is not committed").isZero();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void finalFileFailingAfterCreateIsDeletedAndPartitionsAreCommitted() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    io.failClose(path::equals);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(UncheckedIOException.class);
    assertThat(io.created()).contains(path);
    assertThat(io.deleted()).contains(path);
    assertThat(io.fileExists(path)).as("the partial file is deleted").isFalse();
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertStatisticsFilesExist(data);
  }

  @Test
  public void finalFileWhoseCreateFailsForAnyReasonIsNotDeleted() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    byte[] planted = "planted".getBytes(StandardCharsets.UTF_8);
    io.addFile(path, planted);
    // create() fails before it can tell that the file exists (a HEAD that fails), not with
    // AlreadyExists: the file is still not the step's
    io.failCreate(path::equals);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).hasStackTraceContaining("cannot create");
    assertThat(result.failure()).isNotInstanceOf(AlreadyExistsException.class);
    assertThat(io.deleted()).doesNotContain(path);
    assertThat(read(path)).as("the planted file is intact").isEqualTo(planted);
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void ndvErrorCommitsPartitionStatisticsAndFails() {
    createTable(true);
    StatisticsAnswer answer = uncoveredAnswer(rows("a", 0, 10));
    io.failReads(answer.dataFiles().get(0).location(), () -> new IOException("read failure"));
    health.update(TABLE, flags -> ALL_UP);
    long version = health.version(TABLE);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isNotNull();
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(io.created("/stats-")).isEmpty();
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertThat(health.version(TABLE)).as("an NDV error leaves the health").isEqualTo(version);
    assertStatisticsFilesExist(data);

    // the next step writes only the missing kind and keeps the record of the other
    String partitionPath = partitionRecord(answer.snapshotId()).path();
    io.healReads(answer.dataFiles().get(0).location());

    Result next = step.run(own(answer).build());

    assertThat(next.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(answer.snapshotId())).isNotNull();
    assertThat(partitionRecord(answer.snapshotId()).path()).isEqualTo(partitionPath);
    assertThat(io.created("/partition-stats-")).as("no second partition file").hasSize(1);
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
  }

  @Test
  public void partitionStatisticsErrorCommitsNdvAndFails() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    io.failCreate(path -> path.contains("/partition-stats-"));
    health.update(TABLE, flags -> ALL_UP);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).hasStackTraceContaining("cannot create");
    assertThat(ndvRecord(answer.snapshotId())).isNotNull();
    assertThat(partitionRecord(answer.snapshotId())).isNull();
    assertThat(health.count(Flags::any)).as("the NDV written puts the flags down").isZero();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertPuffinsDeleted(answer);

    // the next step writes only the missing kind and keeps the record of the other
    String ndvPath = ndvRecord(answer.snapshotId()).path();
    int ndvFiles = io.created("/stats-").size();
    io.failCreate(path -> false);

    Result next = step.run(own(answer).build());

    assertThat(next.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(partitionRecord(answer.snapshotId())).isNotNull();
    assertThat(ndvRecord(answer.snapshotId()).path()).isEqualTo(ndvPath);
    assertThat(io.created("/stats-")).as("no second NDV file").hasSize(ndvFiles);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void refreshFailureBeforeTransactionDeletesFilesOfStep() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    IllegalStateException unavailable = new IllegalStateException("catalog unavailable");
    io.afterClose(path::equals, () -> ops.failRefresh(() -> unavailable));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isSameAs(unavailable);
    assertThat(ops.commits()).isZero();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.fileExists(path)).isFalse();
    assertThat(io.fileExists(partitionFiles.get(0))).isFalse();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void linkageErrorFailsStepAndDeletesItsFiles() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    NoClassDefFoundError missing = new NoClassDefFoundError("datasketches");
    io.afterClose(
        path::equals,
        () ->
            ops.beforeRefresh(
                () -> {
                  throw missing;
                }));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isSameAs(missing);
    assertThat(ops.commits()).isZero();
    assertThat(io.fileExists(path)).isFalse();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.fileExists(partitionFiles.get(0))).isFalse();
    assertPuffinsDeleted(answer);
  }

  @Test
  public void linkageErrorWritingFinalFileFailsNdvAndPartitionsAreCommitted() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    // the file is created and written, then a codec class turns out to be missing
    NoClassDefFoundError missing = new NoClassDefFoundError("a codec of the final file");
    io.afterClose(
        path::equals,
        () -> {
          throw missing;
        });

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isSameAs(missing);
    assertThat(io.created()).contains(path);
    assertThat(io.fileExists(path)).as("the file of the step is deleted").isFalse();
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(partitionRecord(answer.snapshotId()))
        .as("the error of one kind does not stop the other")
        .isNotNull();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void linkageErrorWritingPartitionStatisticsFailsThemAndNdvIsCommitted() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    NoClassDefFoundError missing = new NoClassDefFoundError("a codec of the partition statistics");
    io.afterClose(
        path -> path.contains("/partition-stats-"),
        () -> {
          throw missing;
        });

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isSameAs(missing);
    assertThat(io.created("/partition-stats-")).hasSize(1);
    assertThat(partitionRecord(answer.snapshotId())).isNull();
    assertThat(ndvRecord(answer.snapshotId()))
        .as("the error of one kind does not stop the other")
        .isNotNull();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void ndvErrorAndPartitionRecordAppearingBeforeRecheckFailWithoutTransaction() {
    createTable(true);
    StatisticsAnswer answer = uncoveredAnswer(rows("a", 0, 10));
    io.failReads(answer.dataFiles().get(0).location(), () -> new IOException("read failure"));
    AtomicInteger foreign = new AtomicInteger();
    io.afterClose(
        path -> path.contains("/partition-stats-"),
        () -> {
          foreignPartitions(answer.snapshotId());
          foreign.incrementAndGet();
        });

    Result result = step.run(own(answer).build());

    assertThat(foreign).hasValue(1);
    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(ops.commits()).isZero();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.fileExists(partitionFiles.get(0))).as("our file is deleted").isFalse();
    assertThat(partitionRecord(answer.snapshotId()).path()).isNotEqualTo(partitionFiles.get(0));
    assertStatisticsFilesExist(data);
  }

  @Test
  public void ndvRecordAppearingBeforeRecheckIsKeptAndOurFileDeleted() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    List<StatisticsFile> trino = Lists.newArrayList();
    io.afterClose(path::equals, () -> trino.add(foreignNdv(answer.snapshotId())));
    health.update(TABLE, flags -> ALL_UP);
    long version = health.version(TABLE);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.NOT_NEEDED);
    assertThat(ops.commits()).isZero();
    assertThat(trino).hasSize(1);
    assertThat(ndvRecord(answer.snapshotId()).path()).isEqualTo(trino.get(0).path());
    assertThat(io.fileExists(path)).as("our file is deleted").isFalse();
    assertThat(health.version(TABLE)).as("as for records already there").isEqualTo(version);
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void partitionRecordAppearingBeforeRecheckLeavesNdvToCommit() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    List<PartitionStatisticsFile> spark = Lists.newArrayList();
    io.afterClose(path::equals, () -> spark.add(foreignPartitions(answer.snapshotId())));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(answer.snapshotId()).path()).isEqualTo(path);
    assertThat(spark).hasSize(1);
    assertThat(partitionRecord(answer.snapshotId()).path()).isEqualTo(spark.get(0).path());
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.fileExists(partitionFiles.get(0))).as("our file is deleted").isFalse();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
  }

  @Test
  public void stopDuringReadOfGapFileIsStoppedAndChangesNothing() {
    createTable(false);
    EndlessAvroFile gap = baseAndEndlessGap();
    StatisticsAnswer answer = commitOwn(rows("a", 2_000, 2_010));
    long version = health.version(TABLE);
    int commits = ops.commits();
    int deleted = io.deleted().size();
    gap.arm().whenServed(15_000L, () -> stop.set(true));

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(result.failure()).isNull();
    assertThat(gap.rowsServed())
        .as("the read stops at the check of the rows after the flag")
        .isBetween(15_000L, 30_000L);
    assertStoppedWithoutChange(answer, version, commits, deleted);
  }

  @Test
  public void interruptDuringReadOfGapFileWithoutFlagIsStopped() {
    createTable(false);
    EndlessAvroFile gap = baseAndEndlessGap();
    StatisticsAnswer answer = commitOwn(rows("a", 2_000, 2_010));
    long version = health.version(TABLE);
    int commits = ops.commits();
    int deleted = io.deleted().size();
    // the read of an interrupted thread throws, as a client of an object store does
    gap.armInterruptible().whenServed(15_000L, () -> Thread.currentThread().interrupt());

    Result result;
    try {
      result = step.run(own(answer).build());
    } finally {
      assertThat(Thread.interrupted()).as("the interrupt is left in place").isTrue();
    }

    assertThat(stop.get()).as("the flag stays down").isFalse();
    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(result.failure()).isNull();
    assertThat(gap.rowsServed()).isBetween(15_000L, 30_000L);
    assertStoppedWithoutChange(answer, version, commits, deleted);
  }

  /**
   * An own snapshot with its record, then an append of another writer whose file does not end once
   * armed: the gap of the next step.
   */
  private EndlessAvroFile baseAndEndlessGap() {
    StatisticsAnswer base = commitOwn(rows("a", 0, 10));
    assertThat(step.run(own(base).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    EndlessAvroFile gap =
        EndlessAvroFile.write(data, SCHEMA, Arrays.asList(rows("b", 1_000, 2_000)));
    io.serve(gap);
    data.refresh();
    data.newAppend().appendFile(gap.dataFile()).commit();
    logs.clear();
    return gap;
  }

  /** A stopped step: no commit, no delete, no file of the step, the health and Puffins as were. */
  private void assertStoppedWithoutChange(
      StatisticsAnswer answer, long version, int commits, int deleted) {
    assertThat(ops.commits()).isEqualTo(commits);
    assertThat(io.deleted()).as("nothing is deleted after the stop").hasSize(deleted);
    assertThat(ndvRecord(answer.snapshotId())).isNull();
    assertThat(io.created("/stats-" + answer.snapshotId() + "-")).isEmpty();
    assertThat(health.version(TABLE)).as("no health transition").isEqualTo(version);
    assertThat(logs).noneMatch(line -> line.startsWith("WARN"));
    assertPuffinsKept(answer);
  }

  @Test
  public void stopBetweenFinalFileAndRecheckKeepsFilesAndPuffins() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    io.afterClose(path::equals, () -> stop.set(true));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(result.failure()).isNull();
    assertThat(ops.commits()).isZero();
    assertThat(io.deleted()).isEmpty();
    assertThat(io.fileExists(path)).isTrue();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.fileExists(partitionFiles.get(0))).isTrue();
    assertThat(health.version(TABLE)).isZero();
    assertPuffinsKept(answer);
  }

  @Test
  public void stopRightBeforeCommitTransactionKeepsFiles() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    // the refresh of the re-check is the first one after the final file
    ops.afterRefresh(
        () -> {
          if (io.fileExists(path)) {
            stop.set(true);
          }
        });

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(ops.commits()).isZero();
    assertThat(io.deleted()).isEmpty();
    assertThat(io.fileExists(path)).isTrue();
    assertThat(io.created("/partition-stats-")).hasSize(1).allMatch(io::fileExists);
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertPuffinsKept(answer);
  }

  @Test
  public void stopRightAfterCommitTransactionIsWrittenAndKeepsFilesOfRemovedRecords() {
    createTable(false);
    List<StatisticsFile> old = Lists.newArrayList();
    for (int round = 0; round < 3; round += 1) {
      StatisticsAnswer answer = commitOwn(rows("a", round * 10, round * 10 + 10));
      assertThat(step.run(own(answer).build()).outcome()).isEqualTo(Outcome.WRITTEN);
      old.add(ndvRecord(answer.snapshotId()));
    }

    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    config = config(1, 1_000);
    int commits = ops.commits();
    int deleted = io.deleted().size();
    BooleanSupplier stoppedAfterCommit = () -> ops.commits() > commits;

    Result result = step.run(own(answer).stopped(stoppedAfterCommit).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(result.failure()).isNull();
    data.refresh();
    assertThat(data.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactly(answer.snapshotId());
    assertThat(old).allMatch(record -> io.fileExists(record.path()));
    assertThat(io.deleted()).as("nothing is deleted after the stop").hasSize(deleted);
    assertPuffinsKept(answer);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void stopBetweenDeletionsOfRemovedRecordsKeepsRestOfFiles() {
    createTable(false);
    List<StatisticsFile> old = Lists.newArrayList();
    for (int round = 0; round < 3; round += 1) {
      StatisticsAnswer answer = commitOwn(rows("a", round * 10, round * 10 + 10));
      assertThat(step.run(own(answer).build()).outcome()).isEqualTo(Outcome.WRITTEN);
      old.add(ndvRecord(answer.snapshotId()));
    }

    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    config = config(1, 1_000);
    io.afterDelete(path -> path.contains("/stats-"), () -> stop.set(true));

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ndvRecord(answer.snapshotId())).isNotNull();
    assertThat(old.stream().filter(record -> io.fileExists(record.path())))
        .as("only the first file is deleted before the stop")
        .hasSize(2);
    assertPuffinsKept(answer);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void recordsBeyondLastNAreRemovedAndTheirFilesDeleted() {
    createTable(false);
    config = config(2, 1_000);
    List<StatisticsFile> records = Lists.newArrayList();
    for (int round = 0; round < 3; round += 1) {
      StatisticsAnswer answer = commitOwn(rows("a", round * 10, round * 10 + 10));
      assertThat(step.run(own(answer).build()).outcome()).isEqualTo(Outcome.WRITTEN);
      records.add(ndvRecord(answer.snapshotId()));
    }

    data.refresh();
    assertThat(data.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(records.get(1).snapshotId(), records.get(2).snapshotId());
    assertThat(io.deleted()).contains(records.get(0).path());
    assertThat(io.fileExists(records.get(0).path())).isFalse();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
  }

  @Test
  public void recordsOfKindNotInTransactionAreNotRemoved() {
    createTable(true);
    List<Long> snapshots = Lists.newArrayList();
    for (int round = 0; round < 2; round += 1) {
      StatisticsAnswer answer = commitOwn(rows("a", round * 10, round * 10 + 10));
      assertThat(step.run(own(answer).build()).outcome()).isEqualTo(Outcome.WRITTEN);
      snapshots.add(answer.snapshotId());
    }

    List<String> oldPartitionFiles =
        data.partitionStatisticsFiles().stream()
            .map(PartitionStatisticsFile::path)
            .collect(Collectors.toList());
    StatisticsAnswer answer = commitOwn(rows("a", 20, 30));
    foreignNdv(answer.snapshotId());
    config = config(1, 1_000);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    data.refresh();
    assertThat(data.partitionStatisticsFiles())
        .extracting(PartitionStatisticsFile::snapshotId)
        .containsExactly(answer.snapshotId());
    assertThat(oldPartitionFiles).noneMatch(io::fileExists);
    data.refresh();
    assertThat(data.statisticsFiles())
        .as("the NDV is not written by the step: its records stay")
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(snapshots.get(0), snapshots.get(1), answer.snapshotId());
    assertStatisticsFilesExist(data);
  }

  @Test
  public void unknownOutcomeOfAppliedCommitIsWritten() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    ops.unknownOutcome(true);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(result.failure()).isNull();
    assertThat(ndvRecord(answer.snapshotId()).path()).isEqualTo(path);
    assertThat(io.fileExists(path)).isTrue();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void unknownOutcomeOfCommitNotAppliedKeepsFilesAndFails() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    ops.unknownOutcome(false);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(CommitStateUnknownException.class);
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(data.partitionStatisticsFiles()).isEmpty();
    assertThat(io.deleted()).doesNotContain(path);
    assertThat(io.deleted()).doesNotContainAnyElementsOf(io.created("/partition-stats-"));
    assertThat(io.fileExists(path)).isTrue();
    assertThat(io.created("/partition-stats-")).hasSize(1).allMatch(io::fileExists);
    assertThat(health.version(TABLE)).isZero();
    assertPuffinsDeleted(answer);
  }

  @Test
  public void networkFailureAfterAppliedCommitIsConfirmedAndKeepsFiles() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    long snapshotId = answer.snapshotId();
    UUID commitId = UUID.randomUUID();
    String ndvPath = NdvComputation.fileLocation(table, snapshotId, commitId);
    // the REST catalog applied the update, the connection is reset before the response
    ops.applyAndFailCommits(TestStatisticsStep::connectionReset);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(result.failure()).isNull();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(ndvRecord(snapshotId).path()).isEqualTo(ndvPath);
    assertThat(partitionRecord(snapshotId).path()).isEqualTo(partitionFiles.get(0));
    assertThat(io.deleted()).doesNotContain(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isTrue();
    assertThat(io.fileExists(partitionFiles.get(0))).isTrue();
    assertThat(health.count(flags -> true)).as("the NDV transition applied").isEqualTo(1);
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void networkFailureOfCommitNotAppliedKeepsFilesAndFails() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String ndvPath = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    ops.failCommits(TestStatisticsStep::connectionReset);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(RESTException.class);
    assertThat(ops.commits()).as("no retry of a failure that is not a conflict").isEqualTo(1);
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(data.partitionStatisticsFiles()).isEmpty();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.deleted()).doesNotContain(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isTrue();
    assertThat(io.fileExists(partitionFiles.get(0))).isTrue();
    assertThat(health.version(TABLE)).as("no health transition on a failed commit").isZero();
    assertPuffinsDeleted(answer);
  }

  /**
   * Three NDV records of older snapshots: the step of the next snapshot with N = 1 removes them.
   */
  private List<StatisticsFile> oldNdvRecords() {
    config = config(10, 1_000);
    List<StatisticsFile> records = Lists.newArrayList();
    for (int round = 0; round < 3; round += 1) {
      StatisticsAnswer answer = commitOwn(rows("a", round * 10, round * 10 + 10));
      assertThat(step.run(own(answer).build()).outcome()).isEqualTo(Outcome.WRITTEN);
      records.add(ndvRecord(answer.snapshotId()));
    }

    assertThat(records).hasSize(3).allMatch(record -> io.fileExists(record.path()));
    config = config(1, 1_000);
    return records;
  }

  @Test
  public void unknownOutcomeNotAppliedKeepsRecordsAndFilesRemovedByN() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.unknownOutcome(false);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(CommitStateUnknownException.class);
    data.refresh();
    assertThat(data.statisticsFiles())
        .extracting(StatisticsFile::path)
        .containsExactlyInAnyOrderElementsOf(paths(old));
    assertThat(io.deleted()).doesNotContainAnyElementsOf(paths(old));
    assertThat(paths(old)).allMatch(io::fileExists);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void unknownOutcomeAppliedDeletesFilesRemovedByNAfterRefresh() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.unknownOutcome(true);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    data.refresh();
    assertThat(data.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactly(answer.snapshotId());
    assertThat(io.deleted()).containsAll(paths(old));
    assertThat(paths(old)).noneMatch(io::fileExists);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void unknownOutcomeWhoseRefreshFailsDeletesNoFile() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.unknownOutcome(true);
    IllegalStateException unavailable = new IllegalStateException("catalog unavailable");
    ops.beforeFirstCommit(() -> ops.failRefresh(() -> unavailable));

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(CommitStateUnknownException.class);
    assertThat(result.failure().getSuppressed()).contains(unavailable);
    assertThat(io.deleted())
        .as("neither the files removed by N nor the NDV file of the step")
        .doesNotContainAnyElementsOf(paths(old))
        .doesNotContainAnyElementsOf(io.created("/stats-"));
    assertThat(paths(old)).allMatch(io::fileExists);
    assertThat(io.created("/stats-")).allMatch(io::fileExists);
  }

  @Test
  public void networkFailureNotAppliedKeepsRecordsAndFilesRemovedByN() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.failCommits(TestStatisticsStep::connectionReset);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(RESTException.class);
    data.refresh();
    assertThat(data.statisticsFiles())
        .extracting(StatisticsFile::path)
        .containsExactlyInAnyOrderElementsOf(paths(old));
    assertThat(io.deleted()).doesNotContainAnyElementsOf(paths(old));
    assertThat(paths(old)).allMatch(io::fileExists);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void networkFailureAppliedDeletesFilesRemovedByN() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.applyAndFailCommits(TestStatisticsStep::connectionReset);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    data.refresh();
    assertThat(data.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactly(answer.snapshotId());
    assertThat(io.deleted()).containsAll(paths(old));
    assertThat(paths(old)).noneMatch(io::fileExists);
    assertStatisticsFilesExist(data);
  }

  @Test
  public void networkFailureWhoseRefreshFailsDeletesNoFile() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.applyAndFailCommits(TestStatisticsStep::connectionReset);
    IllegalStateException unavailable = new IllegalStateException("catalog unavailable");
    ops.beforeFirstCommit(() -> ops.failRefresh(() -> unavailable));

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(RESTException.class);
    assertThat(io.deleted())
        .as("neither the files removed by N nor the NDV file of the step")
        .doesNotContainAnyElementsOf(paths(old))
        .doesNotContainAnyElementsOf(io.created("/stats-"));
    assertThat(paths(old)).allMatch(io::fileExists);
  }

  @Test
  public void unknownOutcomeWhoseRefreshIsCutShortByStopKeepsFilesAndStops() {
    // the commit ends unknown before the stop, and the refresh that looks for the outcome is cut
    // short by it: a stop, not a failure that puts the table into the lag
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String path = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    ops.unknownOutcome(false);
    ops.beforeFirstCommit(
        () ->
            ops.failRefresh(
                () -> {
                  stop.set(true);
                  return new IllegalStateException("interrupted");
                }));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(io.deleted()).as("the commit may have been applied").doesNotContain(path);
    assertThat(io.fileExists(path)).isTrue();
    assertThat(health.version(TABLE)).isZero();
  }

  @Test
  public void refreshCutShortByStopWhileLookingForFilesRemovedByNIsNoWarn() {
    createTable(false);
    List<StatisticsFile> old = oldNdvRecords();
    StatisticsAnswer answer = commitOwn(rows("a", 30, 40));
    ops.afterCommit(
        () ->
            ops.failRefresh(
                () -> {
                  stop.set(true);
                  return new IllegalStateException("interrupted");
                }));
    logs.clear();

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(logs).noneMatch(line -> line.startsWith("WARN"));
    assertThat(paths(old)).as("nothing is deleted once stopped").allMatch(io::fileExists);
  }

  @Test
  public void refreshOfGoneTableCutShortByStopKeepsHealthAndFiles() {
    // as in the queue of merge-on-read: a stop first, then a table gone
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    health.update(TABLE, flags -> ALL_UP);
    long version = health.version(TABLE);
    ops.failRefresh(
        () -> {
          stop.set(true);
          return new NoSuchTableException("Table does not exist: %s", TABLE);
        });

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(version).isNotZero();
    assertThat(health.version(TABLE)).as("the health record stays").isEqualTo(version);
    assertPuffinsKept(answer);
  }

  private static List<String> paths(List<StatisticsFile> records) {
    return records.stream().map(StatisticsFile::path).collect(Collectors.toList());
  }

  @Test
  public void conflictOnEveryRetryOfTransactionDeletesFilesOfStep() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String ndvPath = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    data.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "2")
        .set(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "1")
        .set(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "1")
        .commit();
    ops.failCommits(() -> new CommitFailedException("Table %s changed", TABLE));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(CommitFailedException.class);
    assertThat(ops.commits()).as("the first commit and two retries").isEqualTo(3);
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.deleted()).contains(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isFalse();
    assertThat(io.fileExists(partitionFiles.get(0))).isFalse();
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(data.partitionStatisticsFiles()).isEmpty();
    assertThat(health.version(TABLE)).as("no health transition on a failed commit").isZero();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void anyFailureWithoutStrictCleanupDeletesFilesOfStep() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String ndvPath = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    ops.requireStrictCleanup(false);
    ops.failCommits(() -> new RuntimeException("commit failed"));

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isExactlyInstanceOf(RuntimeException.class);
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.deleted()).contains(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isFalse();
    assertThat(io.fileExists(partitionFiles.get(0))).isFalse();
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(data.partitionStatisticsFiles()).isEmpty();
    assertThat(health.version(TABLE)).as("no health transition on a failed commit").isZero();
    assertStatisticsFilesExist(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void unknownOutcomeOfAppliedCommitWithoutStrictCleanupIsConfirmed() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    long snapshotId = answer.snapshotId();
    UUID commitId = UUID.randomUUID();
    String ndvPath = NdvComputation.fileLocation(table, snapshotId, commitId);
    ops.requireStrictCleanup(false);
    ops.unknownOutcome(true);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(result.failure()).isNull();
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(ndvRecord(snapshotId).path()).isEqualTo(ndvPath);
    assertThat(partitionRecord(snapshotId).path()).isEqualTo(partitionFiles.get(0));
    assertThat(io.deleted()).doesNotContain(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isTrue();
    assertThat(io.fileExists(partitionFiles.get(0))).isTrue();
    assertThat(health.count(flags -> true)).as("the NDV transition applied").isEqualTo(1);
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void unknownOutcomeOfCommitNotAppliedWithoutStrictCleanupKeepsFiles() {
    createTable(true);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    UUID commitId = UUID.randomUUID();
    String ndvPath = NdvComputation.fileLocation(table, answer.snapshotId(), commitId);
    ops.requireStrictCleanup(false);
    ops.unknownOutcome(false);

    Result result = step.run(own(answer).commitId(commitId).build());

    assertThat(result.outcome()).isEqualTo(Outcome.FAILED);
    assertThat(result.failure()).isInstanceOf(CommitStateUnknownException.class);
    List<String> partitionFiles = io.created("/partition-stats-");
    assertThat(partitionFiles).hasSize(1);
    assertThat(io.deleted()).doesNotContain(ndvPath, partitionFiles.get(0));
    assertThat(io.fileExists(ndvPath)).isTrue();
    assertThat(io.fileExists(partitionFiles.get(0))).isTrue();
    data.refresh();
    assertThat(data.statisticsFiles()).isEmpty();
    assertThat(data.partitionStatisticsFiles()).isEmpty();
    assertThat(health.version(TABLE)).as("no health transition on a failed commit").isZero();
    assertPuffinsDeleted(answer);
  }

  @Test
  public void concurrentDataCommitIsRetriedByTransaction() {
    createTable(false);
    StatisticsAnswer answer = commitOwn(rows("a", 0, 10));
    ops.beforeFirstCommit(
        () -> data.newAppend().appendFile(writeDataFile(rows("a", 10, 20))).commit());

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(ops.commits()).as("a conflict and a retry").isEqualTo(2);
    data.refresh();
    assertThat(data.currentSnapshot().snapshotId()).isNotEqualTo(answer.snapshotId());
    assertThat(ndvRecord(answer.snapshotId())).isNotNull();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
  }

  @Test
  public void failureKeepsPuffinsOnlyWhenAskedTo() {
    createTable(false);
    Record[] rows = rows("a", 0, 10);
    DataFile file = writeDataFile(rows);
    // the Puffin covers only id: data and score are read from the data file, which fails
    SketchFile sketch = puffin(ImmutableList.of("id"), rows);
    data.newAppend().appendFile(file).commit();
    long snapshotId = data.currentSnapshot().snapshotId();
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(snapshotId, List.of(file), sketch);
    io.failReads(file.location(), () -> new IOException("read failure"));

    Result kept = step.run(own(answer).keepSketchesOnFailure(true).build());

    assertThat(kept.outcome()).isEqualTo(Outcome.FAILED);
    assertPuffinsKept(answer);

    Result deleted = step.run(own(answer).build());

    assertThat(deleted.outcome()).isEqualTo(Outcome.FAILED);
    assertPuffinsDeleted(answer);
  }

  @Test
  public void sharedPuffinOfTwoAnswersIsDeletedOnce() {
    createTable(false);
    Record[] rows = rows("a", 0, 10);
    DataFile first = writeDataFile(rows);
    DataFile second = writeDataFile(rows("a", 10, 20));
    SketchFile sketch = puffin(null, rows);
    data.newAppend().appendFile(first).appendFile(second).commit();
    long snapshotId = data.currentSnapshot().snapshotId();
    List<StatisticsAnswer> answers =
        List.of(
            StatisticsAnswer.ofDataFiles(snapshotId, List.of(first), sketch),
            StatisticsAnswer.ofDataFiles(snapshotId, List.of(second), sketch));

    Result result =
        step.run(
            input()
                .snapshotId(snapshotId)
                .ownSnapshotIds(Set.of(snapshotId))
                .answers(answers)
                .build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(io.deleted().stream().filter(sketch.location()::equals)).hasSize(1);
    assertThat(io.fileExists(sketch.location())).isFalse();
    assertNoStrayPuffins(staging, List.of());
  }

  @Test
  public void branchRolledBackToParentOfSLeavesRecordOnSAndHeadRecordAlone() {
    createTable(false);
    StatisticsAnswer parent = commitOwn(rows("a", 0, 10));
    assertThat(step.run(own(parent).build()).outcome()).isEqualTo(Outcome.WRITTEN);
    StatisticsFile headRecord = ndvRecord(parent.snapshotId());
    StatisticsAnswer answer = commitOwn(rows("a", 10, 20));
    data.manageSnapshots().rollbackTo(parent.snapshotId()).commit();
    config = config(1, 1_000);

    Result result = step.run(own(answer).build());

    assertThat(result.outcome()).isEqualTo(Outcome.WRITTEN);
    assertThat(data.currentSnapshot().snapshotId()).isEqualTo(parent.snapshotId());
    assertThat(ndvRecord(answer.snapshotId())).isNotNull();
    assertThat(ndvRecord(parent.snapshotId()).path()).isEqualTo(headRecord.path());
    assertThat(io.fileExists(headRecord.path())).isTrue();
    assertStatisticsFilesExist(data);
    assertStatisticsOnOwnSnapshot(data);
  }

  private void createTable(boolean partitioned) {
    PartitionSpec spec =
        partitioned
            ? PartitionSpec.builderFor(SCHEMA).identity("data").build()
            : PartitionSpec.unpartitioned();
    this.data = catalog.createTable(TABLE, SCHEMA, spec);
    this.ops =
        new ControlledOperations(((HasTableOperations) catalog.loadTable(TABLE)).operations(), io);
    this.table = new BaseTable(ops, "mem." + TABLE);
  }

  /** The input of a step: S not set (a lag step), nothing own, the settings of the test. */
  private StatisticsStepInput.Builder input() {
    return StatisticsStepInput.builder()
        .table(table)
        .identifier(TABLE)
        .commitId(UUID.randomUUID())
        .stopped(stop::get)
        .config(config)
        .tableConfig(tableConfig)
        .health(health);
  }

  private StatisticsStepInput.Builder own(StatisticsAnswer answer) {
    return input()
        .snapshotId(answer.snapshotId())
        .ownSnapshotIds(Set.of(answer.snapshotId()))
        .answers(List.of(answer));
  }

  private static IcebergSinkConfig config(int retainSnapshots, int gapMaxFiles) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.statisticsLgNominalEntries()).thenReturn(LG_K);
    when(config.statisticsGapMaxFiles()).thenReturn(gapMaxFiles);
    when(config.statisticsGapMaxBytes()).thenReturn(Long.MAX_VALUE);
    when(config.statisticsStaleDeletedRatio()).thenReturn(0.2);
    when(config.statisticsRetainSnapshots()).thenReturn(retainSnapshots);
    when(config.schemaCaseInsensitive()).thenReturn(false);
    return config;
  }

  /** Statistics on; {@code null} columns mean the default set. */
  private static TableSinkConfig tableConfig(boolean partitionStats, List<String> columns) {
    return new TableSinkConfig(
        null, ImmutableList.of(), ImmutableList.of(), null, true, partitionStats, columns);
  }

  /** An own commit of the rows: its answer carries the data file and the writer's Puffin. */
  private StatisticsAnswer commitOwn(Record... rows) {
    DataFile file = writeDataFile(rows);
    SketchFile sketch = puffin(null, rows);
    data.refresh();
    data.newAppend().appendFile(file).commit();
    return StatisticsAnswer.ofDataFiles(data.currentSnapshot().snapshotId(), List.of(file), sketch);
  }

  /** An own commit whose answer has no Puffin: every column is read from the data file. */
  private StatisticsAnswer uncoveredAnswer(Record... rows) {
    DataFile file = writeDataFile(rows);
    data.refresh();
    data.newAppend().appendFile(file).commit();
    return StatisticsAnswer.ofDataFiles(data.currentSnapshot().snapshotId(), List.of(file), null);
  }

  /** The intermediate Puffin file of a writer of the rows under the staging directory. */
  private SketchFile puffin(List<String> columns, Record... rows) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(TABLE, data, staging, "0", LG_K, columns, false, SCHEMA);
    for (Record row : rows) {
      sketches.update(row);
    }

    SketchFile file = sketches.write(UUID.randomUUID());
    assertThat(file).as("the writer's Puffin").isNotNull();
    return file;
  }

  /** A record of Trino at the snapshot, with a real Puffin file without blobs. */
  private StatisticsFile foreignNdv(long snapshotId) {
    String path = data.location() + "/metadata/trino-" + UUID.randomUUID() + ".stats";
    try (PuffinWriter writer =
        Puffin.write(data.io().newOutputFile(path)).createdBy("Trino").build()) {
      writer.finish();
      StatisticsFile file =
          new GenericStatisticsFile(
              snapshotId, path, writer.fileSize(), writer.footerSize(), ImmutableList.of());
      data.refresh();
      data.updateStatistics().setStatistics(file).commit();
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private PartitionStatisticsFile foreignPartitions(long snapshotId) {
    String path = data.location() + "/metadata/spark-partitions-" + UUID.randomUUID() + ".parquet";
    io.addFile(path, new byte[] {1});
    PartitionStatisticsFile file =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path(path)
            .fileSizeInBytes(1L)
            .build();
    data.refresh();
    data.updatePartitionStatistics().setPartitionStatistics(file).commit();
    return file;
  }

  private StatisticsFile ndvRecord(long snapshotId) {
    data.refresh();
    return data.statisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  private PartitionStatisticsFile partitionRecord(long snapshotId) {
    data.refresh();
    return data.partitionStatisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  private void assertPuffinsDeleted(StatisticsAnswer answer) {
    assertThat(io.fileExists(answer.sketchFile().location()))
        .as("the Puffin of the answer is deleted")
        .isFalse();
    assertNoStrayPuffins(staging, List.of());
  }

  private void assertPuffinsKept(StatisticsAnswer answer) {
    assertThat(io.fileExists(answer.sketchFile().location()))
        .as("the Puffin of the answer is kept")
        .isTrue();
    assertNoStrayPuffins(staging, List.of(answer.sketchFile().location()));
  }

  private byte[] read(String path) {
    InputFile file = io.newInputFile(path);
    try (InputStream stream = file.newStream()) {
      return ByteStreams.toByteArray(stream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Rows of one partition: ids from {@code from} (inclusive) to {@code to} (exclusive). */
  private static Record[] rows(String partition, long from, long to) {
    return LongStream.range(from, to)
        .mapToObj(
            id -> {
              Record record = GenericRecord.create(SCHEMA);
              record.set(0, id);
              record.set(1, partition);
              record.set(2, (int) (id % 3));
              return record;
            })
        .toArray(Record[]::new);
  }

  private DataFile writeDataFile(Record... rows) {
    fileCount += 1;
    PartitionSpec spec = data.spec();
    PartitionKey partition = null;
    if (spec.isPartitioned()) {
      partition = new PartitionKey(spec, data.schema());
      partition.partition(new InternalRecordWrapper(data.schema().asStruct()).wrap(rows[0]));
    }

    OutputFileFactory files = OutputFileFactory.builderFor(data, 1, fileCount).build();
    EncryptedOutputFile output =
        partition == null ? files.newOutputFile() : files.newOutputFile(spec, partition);
    try {
      DataWriter<Record> writer =
          new GenericFileWriterFactory.Builder(data)
              .dataSchema(data.schema())
              .build()
              .newDataWriter(output, spec, partition);
      try {
        for (Record row : rows) {
          writer.write(row);
        }
      } finally {
        writer.close();
      }

      return writer.toDataFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** What the REST client throws when the connection breaks after the request is sent. */
  private static RESTException connectionReset() {
    return new RESTException(
        new SocketException("Connection reset"), "Failed to update table %s", TABLE);
  }

  /**
   * The operations of the step's table: they count refreshes and commits and can fail them the way
   * a catalog does.
   */
  private static final class ControlledOperations implements TableOperations {
    private final TableOperations delegate;
    private final FileIO io;
    private final AtomicInteger refreshes = new AtomicInteger();
    private final AtomicInteger commits = new AtomicInteger();
    private volatile Supplier<RuntimeException> refreshFailure = null;
    private volatile Runnable beforeRefresh = null;
    private volatile Runnable afterRefresh = null;
    private volatile Supplier<RuntimeException> commitFailure = null;
    private volatile boolean appliedBeforeFailure = false;
    private volatile boolean strictCleanup = true;
    private volatile Runnable beforeFirstCommit = null;
    private volatile Runnable afterCommit = null;

    private ControlledOperations(TableOperations delegate, FileIO io) {
      this.delegate = delegate;
      this.io = io;
    }

    void failRefresh(Supplier<RuntimeException> failure) {
      this.refreshFailure = failure;
    }

    /** Runs before every refresh loads the metadata: the refresh sees the action's commits. */
    void beforeRefresh(Runnable action) {
      this.beforeRefresh = action;
    }

    void afterRefresh(Runnable action) {
      this.afterRefresh = action;
    }

    /** Every commit fails without applying the metadata. */
    void failCommits(Supplier<RuntimeException> failure) {
      this.appliedBeforeFailure = false;
      this.commitFailure = failure;
    }

    /** Every commit applies the metadata and then fails, as when the response is lost. */
    void applyAndFailCommits(Supplier<RuntimeException> failure) {
      this.appliedBeforeFailure = true;
      this.commitFailure = failure;
    }

    /** Every commit ends with {@code CommitStateUnknownException}, applied or not. */
    void unknownOutcome(boolean applied) {
      Supplier<RuntimeException> failure =
          () -> new CommitStateUnknownException(new IllegalStateException("response lost"));
      if (applied) {
        applyAndFailCommits(failure);
      } else {
        failCommits(failure);
      }
    }

    void requireStrictCleanup(boolean strict) {
      this.strictCleanup = strict;
    }

    /** Runs after every commit that went through. */
    void afterCommit(Runnable action) {
      this.afterCommit = action;
    }

    void beforeFirstCommit(Runnable action) {
      this.beforeFirstCommit = action;
    }

    int refreshes() {
      return refreshes.get();
    }

    int commits() {
      return commits.get();
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      refreshes.incrementAndGet();
      Supplier<RuntimeException> failure = refreshFailure;
      if (failure != null) {
        throw failure.get();
      }

      Runnable before = beforeRefresh;
      if (before != null) {
        before.run();
      }

      TableMetadata metadata = delegate.refresh();
      Runnable action = afterRefresh;
      if (action != null) {
        action.run();
      }

      return metadata;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      commits.incrementAndGet();
      Runnable action = beforeFirstCommit;
      if (action != null) {
        this.beforeFirstCommit = null;
        action.run();
      }

      Supplier<RuntimeException> failure = commitFailure;
      if (failure != null) {
        if (appliedBeforeFailure) {
          delegate.commit(base, metadata);
        }

        throw failure.get();
      }

      delegate.commit(base, metadata);
      Runnable after = afterCommit;
      if (after != null) {
        after.run();
      }
    }

    @Override
    public boolean requireStrictCleanup() {
      return strictCleanup;
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }
  }

  /**
   * The {@code FileIO} of the step's table: remembers the files created and deleted, can fail the
   * creation or the close of a file and can act once a file is closed.
   */
  private static final class StepFileIO extends ReadFileIO {
    private final List<String> created = new CopyOnWriteArrayList<>();
    private final List<String> deleted = new CopyOnWriteArrayList<>();
    private final List<Map.Entry<Predicate<String>, Runnable>> afterClose =
        new CopyOnWriteArrayList<>();
    private final List<Map.Entry<Predicate<String>, Runnable>> afterDelete =
        new CopyOnWriteArrayList<>();
    private volatile Predicate<String> failCreate = path -> false;
    private volatile Predicate<String> failClose = path -> false;

    void failCreate(Predicate<String> paths) {
      this.failCreate = paths;
    }

    /** The file is written in full, then its close throws. */
    void failClose(Predicate<String> paths) {
      this.failClose = paths;
    }

    /** Runs the action once, after the first matching file is closed. */
    void afterClose(Predicate<String> paths, Runnable action) {
      afterClose.add(Map.entry(paths, action));
    }

    /** Runs the action once, after the first matching file is deleted. */
    void afterDelete(Predicate<String> paths, Runnable action) {
      afterDelete.add(Map.entry(paths, action));
    }

    List<String> created() {
      return created;
    }

    List<String> created(String part) {
      return created.stream().filter(path -> path.contains(part)).collect(Collectors.toList());
    }

    List<String> deleted() {
      return deleted;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return new StepOutputFile(super.newOutputFile(path), path);
    }

    @Override
    public void deleteFile(String path) {
      deleted.add(path);
      super.deleteFile(path);
      runOnce(afterDelete, path);
    }

    private void closed(String path) throws IOException {
      runOnce(afterClose, path);

      if (failClose.test(path)) {
        throw new IOException("cannot close " + path);
      }
    }

    private static void runOnce(List<Map.Entry<Predicate<String>, Runnable>> actions, String path) {
      for (Map.Entry<Predicate<String>, Runnable> entry : actions) {
        if (entry.getKey().test(path) && actions.remove(entry)) {
          entry.getValue().run();
        }
      }
    }

    private final class StepOutputFile implements OutputFile {
      private final OutputFile delegate;
      private final String path;

      private StepOutputFile(OutputFile delegate, String path) {
        this.delegate = delegate;
        this.path = path;
      }

      @Override
      public PositionOutputStream create() {
        return track(delegate::create);
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return track(delegate::createOrOverwrite);
      }

      private PositionOutputStream track(Supplier<PositionOutputStream> open) {
        if (failCreate.test(path)) {
          throw new UncheckedIOException(new IOException("cannot create " + path));
        }

        PositionOutputStream stream = open.get();
        created.add(path);
        return new PositionOutputStream() {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void write(int value) throws IOException {
            stream.write(value);
          }

          @Override
          public void write(byte[] bytes, int off, int len) throws IOException {
            stream.write(bytes, off, len);
          }

          @Override
          public void flush() throws IOException {
            stream.flush();
          }

          @Override
          public void close() throws IOException {
            stream.close();
            closed(path);
          }
        };
      }

      @Override
      public String location() {
        return delegate.location();
      }

      @Override
      public InputFile toInputFile() {
        return delegate.toInputFile();
      }
    }
  }
}
