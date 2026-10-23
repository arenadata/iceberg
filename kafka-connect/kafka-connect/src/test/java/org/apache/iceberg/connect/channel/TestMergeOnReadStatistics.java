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
package org.apache.iceberg.connect.channel;

import static org.apache.iceberg.connect.StatisticsAssertions.assertNoStrayPuffins;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.connect.StatisticsAssertions.await;
import static org.apache.iceberg.connect.StatisticsAssertions.awaitStatistics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.EndlessAvroFile;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.NdvBounds;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.WarningCapture;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.connect.data.NdvChain;
import org.apache.iceberg.connect.data.StatisticsAnswer;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Merge-on-read with statistics through the coordinator. The test plays the workers: it writes real
 * data files and the intermediate Puffin files of their writers and sends their {@code
 * DataWritten}.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class TestMergeOnReadStatistics extends ChannelTestBase {
  private static final TableIdentifier OTHER_TABLE = TableIdentifier.of(NAMESPACE, "tbl2");
  private static final long AWAIT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final Schema SCORED =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "date", Types.StringType.get()),
          Types.NestedField.optional(4, "score", Types.IntegerType.get()));

  private final String staging = "mem://staging/" + UUID.randomUUID();
  private final ControlledStep step = new ControlledStep();
  private final AtomicReference<StatisticsHealth> coordinatorHealth = new AtomicReference<>();
  private StatisticsLag lag = new StatisticsLag();
  private final Map<UUID, OffsetDateTime> validThrough = Maps.newConcurrentMap();
  private final AtomicReference<Runnable> afterDataCommit = new AtomicReference<>();
  private final AtomicReference<RuntimeException> unknownDataCommit = new AtomicReference<>();
  private HookedCatalog hookedCatalog;
  private MetadataEvents metadataEvents;
  private Coordinator coordinator;
  private long ctlOffset = 1L;
  private int fileCount = 0;
  private final Map<String, AtomicInteger> reads = Maps.newConcurrentMap();
  private volatile boolean countReads = false;
  // served without an end through the tables of the coordinator, once armed
  private volatile EndlessAvroFile endless = null;

  @BeforeEach
  public void hookCatalog() {
    hookedCatalog = new HookedCatalog(catalog);
    metadataEvents = mock(MetadataEvents.class);
  }

  @AfterEach
  public void stopCoordinator() {
    step.releaseAll();
    if (coordinator != null) {
      coordinator.terminate();
    }

    assertStatisticsOnOwnSnapshot(table);
    assertThat(step.overlaps()).as("two steps of one table at once").isEmpty();
  }

  @Test
  public void everyCommitGetsNdvOfItsOwnSnapshot() {
    enableStatistics(TABLE_IDENTIFIER);
    startCoordinator(coordinatorWithStep(hookedCatalog, metadataEvents, step));

    List<Long> written = Lists.newArrayList();
    for (int cycle = 0; cycle < 3; cycle++) {
      List<Record> rows = rows(cycle * 10L, cycle * 10L + 10);
      UUID commitId = appendCycle(rows);
      long snapshotId = ownSnapshotId(commitId);
      written.addAll(ids(rows));

      assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId())
          .as("CommitToTable of cycle %s", cycle)
          .isEqualTo(snapshotId);
      assertThat(table.snapshot(snapshotId).summary())
          .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true");
      assertThat(commitComplete(commitId).validThroughTs()).isEqualTo(validThrough.get(commitId));

      // the next cycle only after this one's record, or the tasks of the queue merge
      awaitStatistics(table, snapshotId, staging);
      NdvBounds.of(table, snapshotId, "id").written(written).checkRecorded(STATISTICS_LG_K);
      NdvBounds.of(table, snapshotId, "data")
          .written(dataValues(0, cycle * 10L + 10))
          .checkRecorded(STATISTICS_LG_K);
    }

    assertThat(table.statisticsFiles()).hasSize(3);
    assertThat(step.inputs()).hasSize(3);
    StatisticsStepInput last = step.inputs().get(2);
    assertThat(last.ownSnapshotIds()).containsExactly(last.snapshotId());
    assertThat(last.answers()).hasSize(1);
    assertThat(last.answers().get(0).snapshotId()).isEqualTo(last.snapshotId());
    assertThat(last.answers().get(0).sketchFile()).isNotNull();
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void foreignAppendRightAfterDataCommitLeavesRecordOnOwnSnapshot() {
    enableStatistics(TABLE_IDENTIFIER);
    DataFile foreign = writeDataFile(SCHEMA, rows(100, 110));
    afterDataCommit.set(() -> foreignAppend(foreign));
    startCoordinator();

    UUID commitId = appendCycle(rows(0, 10));

    long snapshotId = ownSnapshotId(commitId);
    table.refresh();
    Snapshot head = table.currentSnapshot();
    assertThat(head.snapshotId()).as("the foreign append is the head").isNotEqualTo(snapshotId);
    assertThat(head.parentId()).isEqualTo(snapshotId);
    assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId()).isEqualTo(snapshotId);

    awaitStatistics(table, snapshotId, staging);
    assertThat(record(head.snapshotId())).as("no record on the foreign head").isNull();
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(snapshotId);
    NdvBounds.of(table, snapshotId, "id").written(range(0, 10)).checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void foreignAppendInGapIsReadIntoNextSnapshot() {
    enableStatistics(TABLE_IDENTIFIER);
    startCoordinator();
    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    awaitStatistics(table, first, staging);

    foreignAppend(writeDataFile(SCHEMA, rows(100, 110)));
    long second = ownSnapshotId(appendCycle(rows(10, 20)));
    awaitStatistics(table, second, staging);

    List<Long> written = range(0, 20);
    written.addAll(range(100, 110));
    NdvBounds.of(table, second, "id").written(written).checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void commitBranchOtherThanMainHasItsStatisticsAndItsLagStepOnHeadOfBranch() {
    enableStatisticsOnBranch(TABLE_IDENTIFIER, "b");
    startCoordinator();

    // the first commit creates the branch, there is no head of it to start from
    UUID firstCommit = appendCycle(rows(0, 10));
    long first = ownSnapshotId(firstCommit);
    assertThat(commitToTable(firstCommit, TABLE_IDENTIFIER).snapshotId()).isEqualTo(first);
    assertThat(branchHead("b")).as("the commit is on the branch").isEqualTo(first);
    assertThat(table.snapshot(first).summary())
        .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true");
    awaitStatistics(table, first, staging);

    // somebody else appends to the branch, then the own commit goes on top of it
    foreignAppendToBranch("b", writeDataFile(SCHEMA, rows(100, 110)));
    UUID secondCommit = appendCycle(rows(10, 20));
    long second = ownSnapshotId(secondCommit);
    assertThat(commitToTable(secondCommit, TABLE_IDENTIFIER).snapshotId()).isEqualTo(second);
    assertThat(branchHead("b")).isEqualTo(second);
    awaitStatistics(table, second, staging);
    List<Long> written = range(0, 20);
    written.addAll(range(100, 110));
    NdvBounds.of(table, second, "id").written(written).checkRecorded(STATISTICS_LG_K);

    // the head of the branch is a foreign snapshot: the lag step writes it
    foreignAppendToBranch("b", writeDataFile(SCHEMA, rows(200, 210)));
    long head = branchHead("b");
    lag.enter(TABLE_IDENTIFIER);
    emptyCyclesUntil(
        "the lag step writes the head of the branch",
        () -> record(head) != null && !lag.contains(TABLE_IDENTIFIER));

    written.addAll(range(200, 210));
    NdvBounds.of(table, head, "id").written(written).checkRecorded(STATISTICS_LG_K);
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(first, second, null);
    table.refresh();
    assertThat(table.currentSnapshot()).as("main has no snapshot").isNull();
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void severalTasksAndSchemaChangeInOneCycleAreAllInNdv() {
    enableStatistics(TABLE_IDENTIFIER);
    startCoordinator();
    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    awaitStatistics(table, first, staging);

    // task-0 writes with the old schema; then a column is added and task-1 writes with it
    UUID commitId = startCommit();
    send(written(commitId, TABLE_IDENTIFIER, SCHEMA, rows(10, 20), "task-0", true));
    table.updateSchema().addColumn("score", Types.IntegerType.get()).commit();
    table.refresh();
    assertThat(table.schema().sameSchema(SCORED)).isTrue();
    List<Record> scored = scoredRows(20, 30);
    send(written(commitId, TABLE_IDENTIFIER, table.schema(), scored, "task-1", true));
    completeCommit(commitId);

    long second = ownSnapshotId(commitId);
    awaitStatistics(table, second, staging);
    assertThat(step.inputs().get(1).answers()).hasSize(2);
    NdvBounds.of(table, second, "id").written(range(0, 30)).checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, second, "score")
        .written(scored.stream().map(row -> row.getField("score")).collect(Collectors.toList()))
        .checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void deletedPuffinIsReadFromDataFilesOfItsAnswer() {
    enableStatistics(TABLE_IDENTIFIER);
    startCoordinator();

    UUID commitId = startCommit();
    DataWritten answer = written(commitId, TABLE_IDENTIFIER, SCHEMA, rows(0, 10), "task-0", true);
    table.io().deleteFile(answer.sketchFile().location());
    send(answer);
    completeCommit(commitId);

    long snapshotId = ownSnapshotId(commitId);
    awaitStatistics(table, snapshotId, staging);
    NdvBounds.of(table, snapshotId, "id").written(range(0, 10)).checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, snapshotId, "data")
        .written(dataValues(0, 10))
        .checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void sharedPuffinCutByPartialCommitOverestimatesFirstSnapshotAndIsReadAfterward() {
    enableStatistics(TABLE_IDENTIFIER);
    countReads = true;
    startCoordinator();

    UUID firstCommit = startCommit();
    List<Record> firstRows = taggedRows(0, 10, "r1-", 2);
    List<Record> secondRows = taggedRows(10, 20, "r2-", 3);
    List<DataWritten> results = sharedPuffinResults(firstCommit, firstRows, secondRows);
    SketchFile puffin = results.get(0).sketchFile();
    DataFile firstFile = results.get(0).dataFiles().get(0);
    DataFile secondFile = results.get(1).dataFiles().get(0);
    // the poll of the coordinator cuts the transaction of the task: R2 misses the first commit
    send(results.get(0));
    completeCommit(firstCommit);
    long first = ownSnapshotId(firstCommit);
    awaitStatistics(table, first, staging);

    NdvBounds firstData = NdvBounds.of(table, first, "data").written(values(firstRows, "data"));
    assertThat(firstData.recordedNdv())
        .as("the step of S1 unions all of P, the values of R2 too")
        .isEqualTo(5);
    firstData.overestimate(values(secondRows, "data").toArray()).checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, first, "id")
        .written(range(0, 10))
        .overestimate(range(10, 20).toArray())
        .checkRecorded(STATISTICS_LG_K);
    assertThat(inMemoryFiles().contains(puffin.location()))
        .as("the step of S1 deletes P")
        .isFalse();
    assertThat(reads(puffin.location())).as("P read by the step of S1").isEqualTo(1);
    assertThat(reads(firstFile.location())).as("R1 is covered by P").isZero();

    // the late R2 is taken by the next cycle, under the commit id of its writer
    UUID secondCommit = startCommit();
    send(results.get(1));
    completeCommit(secondCommit);
    long second = ownSnapshotId(secondCommit);
    awaitStatistics(table, second, staging);

    StatisticsStepInput secondInput = step.inputs().get(1);
    assertThat(secondInput.snapshotId()).isEqualTo(second);
    assertThat(secondInput.answers()).hasSize(1);
    assertThat(secondInput.answers().get(0).sketchFile().location())
        .as("R2 still refers to the deleted P")
        .isEqualTo(puffin.location());
    assertThat(reads(secondFile.location()))
        .as("the data file of R2 is read by the step of S2")
        .isEqualTo(1);
    assertThat(reads(firstFile.location())).isZero();
    NdvBounds.of(table, second, "data")
        .written(values(firstRows, "data"))
        .written(values(secondRows, "data"))
        .checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, second, "id").written(range(0, 20)).checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void sharedPuffinCutByPartialCommitIsReadOnceByMergedTask() throws InterruptedException {
    enableStatistics(TABLE_IDENTIFIER);
    countReads = true;
    Held held = step.holdNextStep();
    startCoordinator();

    List<Record> baseRows = taggedRows(100, 110, "b-", 1);
    UUID baseCommit = appendCycle(baseRows);
    held.awaitStarted();

    UUID firstCommit = startCommit();
    List<Record> firstRows = taggedRows(0, 10, "r1-", 2);
    List<Record> secondRows = taggedRows(10, 20, "r2-", 3);
    List<DataWritten> results = sharedPuffinResults(firstCommit, firstRows, secondRows);
    SketchFile puffin = results.get(0).sketchFile();
    send(results.get(0));
    completeCommit(firstCommit);
    UUID secondCommit = startCommit();
    send(results.get(1));
    completeCommit(secondCommit);
    long first = ownSnapshotId(firstCommit);
    long second = ownSnapshotId(secondCommit);
    assertThat(step.inputs()).as("only the held step has started").hasSize(1);

    held.release();
    awaitStatistics(table, second, staging);

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(ownSnapshotId(baseCommit), second);
    StatisticsStepInput merged = step.inputs().get(1);
    assertThat(merged.ownSnapshotIds()).containsExactlyInAnyOrder(first, second);
    assertThat(merged.answers())
        .extracting(answer -> answer.sketchFile().location())
        .containsExactly(puffin.location(), puffin.location());
    assertThat(record(first)).as("only S of the newer task gets a record").isNull();
    assertThat(reads(puffin.location())).as("P read once by the merged task").isEqualTo(1);
    for (DataWritten result : results) {
      assertThat(reads(result.dataFiles().get(0).location()))
          .as("the data files of the answers are covered by P")
          .isZero();
    }

    assertThat(inMemoryFiles().contains(puffin.location())).isFalse();
    NdvBounds.of(table, second, "data")
        .base(values(baseRows, "data"))
        .written(values(firstRows, "data"))
        .written(values(secondRows, "data"))
        .checkRecorded(STATISTICS_LG_K);
    List<Long> ids = range(0, 20);
    ids.addAll(range(100, 110));
    NdvBounds.of(table, second, "id").written(ids).checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  /**
   * The column goes from int to long: the writer's sketch of it is invalid and its Puffin has no
   * blob of it, though both results refer to that Puffin.
   */
  @Test
  public void columnWidenedInMiddleOfWritersCycleIsReadFromDataFilesOfBothResults() {
    enableStatistics(TABLE_IDENTIFIER);
    countReads = true;
    startCoordinator();
    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    awaitStatistics(table, first, staging);
    table.updateSchema().addColumn("score", Types.IntegerType.get()).commit();
    table.refresh();
    Schema intSchema = table.schema();
    assertThat(intSchema.sameSchema(SCORED)).isTrue();

    UUID commitId = startCommit();
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER, table, staging, "task-0", STATISTICS_LG_K, null, false, intSchema);
    List<Record> ints = scoredRows(10, 20);
    ints.forEach(sketches::update);
    DataFile intFile = writeDataFile(intSchema, ints);
    table.updateSchema().updateColumn("score", Types.LongType.get()).commit();
    table.refresh();
    Schema longSchema = table.schema();
    sketches.schemaChanged(longSchema);
    List<Record> longs = widenedRows(longSchema, 20, 30);
    longs.forEach(sketches::update);
    DataFile longFile = writeDataFile(longSchema, longs);
    SketchFile puffin = sketches.write(commitId);
    assertThat(puffin).as("the writer's Puffin").isNotNull();
    send(dataWritten(commitId, ImmutableList.of(intFile), ImmutableList.of(), puffin));
    send(dataWritten(commitId, ImmutableList.of(longFile), ImmutableList.of(), puffin));
    completeCommit(commitId);

    long second = ownSnapshotId(commitId);
    awaitStatistics(table, second, staging);
    assertThat(step.inputs().get(1).answers()).hasSize(2);
    assertThat(reads(intFile.location())).as("the data file of the int rows").isPositive();
    assertThat(reads(longFile.location())).as("the data file of the long rows").isPositive();
    List<Object> scores = Lists.newArrayList();
    for (Object score : values(ints, "score")) {
      scores.add(score == null ? null : ((Integer) score).longValue());
    }
    scores.addAll(values(longs, "score"));
    NdvBounds score = NdvBounds.of(table, second, "score").written(scores);
    assertThat(score.lower()).as("the bounds have values to tell").isGreaterThan(5);
    score.checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, second, "id").written(range(0, 30)).checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  /** The Puffin is overwritten with garbage, not deleted, before the answer is sent. */
  @Test
  public void corruptPuffinIsReadFromDataFilesOfItsAnswer() throws IOException {
    enableStatistics(TABLE_IDENTIFIER);
    countReads = true;
    startCoordinator();

    UUID commitId = startCommit();
    DataWritten answer = written(commitId, TABLE_IDENTIFIER, SCHEMA, rows(0, 10), "task-0", true);
    // as long as the file was, so that the reader fails on the content and not on the length
    byte[] garbage = new byte[(int) answer.sketchFile().fileSizeBytes()];
    Arrays.fill(garbage, (byte) 3);
    try (OutputStream out =
        table.io().newOutputFile(answer.sketchFile().location()).createOrOverwrite()) {
      out.write(garbage);
    }

    send(answer);
    completeCommit(commitId);

    long snapshotId = ownSnapshotId(commitId);
    awaitStatistics(table, snapshotId, staging);
    assertThat(reads(answer.dataFiles().get(0).location()))
        .as("the data file of the answer")
        .isEqualTo(1);
    NdvBounds.of(table, snapshotId, "id").written(range(0, 10)).checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, snapshotId, "data")
        .written(dataValues(0, 10))
        .checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  /**
   * S1 is rolled back before the step and S2 takes its place, so the tasks of the two snapshots
   * merge. S1 is not an ancestor of S2: none of its answers is united, and its data file is not
   * read.
   */
  @Test
  public void mergedTaskDropsAnswersOfRolledBackSnapshot() throws InterruptedException {
    enableStatistics(TABLE_IDENTIFIER);
    countReads = true;
    Held held = step.holdNextStep();
    startCoordinator();

    List<Record> baseRows = taggedRows(100, 110, "b-", 1);
    UUID baseCommit = appendCycle(baseRows);
    held.awaitStarted();
    long base = ownSnapshotId(baseCommit);

    UUID firstCommit = startCommit();
    List<Record> firstRows = taggedRows(0, 10, "r1-", 2);
    DataWritten firstAnswer =
        written(firstCommit, TABLE_IDENTIFIER, SCHEMA, firstRows, "task-0", true);
    send(firstAnswer);
    completeCommit(firstCommit);
    long first = ownSnapshotId(firstCommit);
    assertThat(ownSnapshotParentOf(first)).isEqualTo(base);
    table.refresh();
    table.manageSnapshots().rollbackTo(base).commit();

    UUID secondCommit = startCommit();
    List<Record> secondRows = taggedRows(10, 20, "r2-", 3);
    send(written(secondCommit, TABLE_IDENTIFIER, SCHEMA, secondRows, "task-0", true));
    completeCommit(secondCommit);
    long second = ownSnapshotId(secondCommit);
    assertThat(ownSnapshotParentOf(second)).as("S2 is on the branch again").isEqualTo(base);
    assertThat(step.inputs()).as("only the held step has started").hasSize(1);

    held.release();
    awaitStatistics(table, second, staging);

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(base, second);
    assertThat(step.inputs().get(1).ownSnapshotIds()).containsExactlyInAnyOrder(first, second);
    assertThat(record(first)).as("S1 is off the branch, no record").isNull();
    assertThat(reads(firstAnswer.dataFiles().get(0).location())).as("the data file of S1").isZero();
    NdvBounds.of(table, second, "data")
        .base(values(baseRows, "data"))
        .written(values(secondRows, "data"))
        .checkRecorded(STATISTICS_LG_K);
    List<Long> ids = range(10, 20);
    ids.addAll(range(100, 110));
    NdvBounds.of(table, second, "id").written(ids).checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void dataWrittenWithoutSketchFileIsReadFromItsDataFiles() {
    enableStatistics(TABLE_IDENTIFIER);
    startCoordinator();

    UUID commitId = startCommit();
    send(written(commitId, TABLE_IDENTIFIER, SCHEMA, rows(0, 10), "task-0", true));
    send(written(commitId, TABLE_IDENTIFIER, SCHEMA, rows(10, 20), "task-1", false));
    completeCommit(commitId);

    long snapshotId = ownSnapshotId(commitId);
    awaitStatistics(table, snapshotId, staging);
    assertThat(step.inputs().get(0).answers())
        .extracting(answer -> answer.sketchFile() == null)
        .containsExactlyInAnyOrder(false, true);
    NdvBounds.of(table, snapshotId, "id").written(range(0, 20)).checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, snapshotId, "data")
        .written(dataValues(0, 20))
        .checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void answersAreResponsesDataCommitTookAndRowDeltaIsMarked() {
    enableStatistics(TABLE_IDENTIFIER);
    step.behave(input -> new Result(Outcome.NOT_NEEDED, null));
    // responses up to control topic offset 3 were applied by an earlier commit of the connector
    table.newAppend().set(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}").commit();
    startCoordinator();

    UUID commitId = startCommit();
    DataFile applied = fakeDataFile(5L);
    send(dataWritten(commitId, ImmutableList.of(applied), ImmutableList.of(), fakeSketch()));
    send(dataWritten(commitId, ImmutableList.of(applied), ImmutableList.of(), fakeSketch()));
    DataFile taken = fakeDataFile(5L);
    SketchFile takenSketch = fakeSketch();
    send(dataWritten(commitId, ImmutableList.of(taken), ImmutableList.of(), takenSketch));
    // no rows, a file the first response already brought, delete files only: not answers
    send(dataWritten(commitId, ImmutableList.of(fakeDataFile(0L)), ImmutableList.of(), null));
    send(dataWritten(commitId, ImmutableList.of(taken), ImmutableList.of(), fakeSketch()));
    send(dataWritten(commitId, ImmutableList.of(), ImmutableList.of(fakeDeleteFile()), null));
    completeCommit(commitId);

    long snapshotId = ownSnapshotId(commitId);
    assertThat(table.snapshot(snapshotId).summary())
        .as("the row delta of the cycle")
        .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true")
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":" + ctlOffset + "}");
    await("the step of the cycle", () -> step.inputs().size() == 1);
    StatisticsStepInput input = step.inputs().get(0);
    assertThat(input.snapshotId()).isEqualTo(snapshotId);
    assertThat(input.commitId()).isEqualTo(commitId);
    assertThat(input.ownSnapshotIds()).containsExactly(snapshotId);
    assertThat(input.answers()).hasSize(1);
    StatisticsAnswer answer = input.answers().get(0);
    assertThat(answer.snapshotId()).isEqualTo(snapshotId);
    assertThat(answer.dataFiles()).extracting(DataFile::location).containsExactly(taken.location());
    assertThat(answer.sketchFile().location()).isEqualTo(takenSketch.location());
    assertThat(lag.contains(TABLE_IDENTIFIER)).isFalse();
  }

  @Test
  public void withStatisticsOffForeignAppendIsSnapshotOfCommitToTableAsBefore() {
    DataFile foreign = writeDataFile(SCHEMA, rows(100, 101));
    afterDataCommit.set(() -> foreignAppend(foreign));
    startCoordinator(coordinatorWithStep(hookedCatalog, metadataEvents, step));

    UUID first = appendCycleWithoutPuffin(rows(0, 10));
    UUID second = appendCycleWithoutPuffin(rows(10, 20));

    table.refresh();
    long firstOwn = ownSnapshotId(first);
    long secondOwn = ownSnapshotId(second);
    Snapshot foreignSnapshot = table.snapshot(ownSnapshotParentOf(secondOwn));
    assertThat(foreignSnapshot.parentId()).isEqualTo(firstOwn);
    assertThat(commitToTable(first, TABLE_IDENTIFIER).snapshotId())
        .as("the head of the branch, as the released version")
        .isEqualTo(foreignSnapshot.snapshotId());
    assertThat(commitToTable(second, TABLE_IDENTIFIER).snapshotId()).isEqualTo(secondOwn);
    for (Snapshot snapshot : table.snapshots()) {
      assertThat(snapshot.summary()).doesNotContainKey(NdvChain.STATISTICS_ENABLED_PROP);
      assertThat(noStatisticsFileOf(snapshot.snapshotId())).isTrue();
    }

    assertThat(table.statisticsFiles()).isEmpty();
    assertThat(table.partitionStatisticsFiles()).isEmpty();
    assertThat(step.inputs()).isEmpty();
    assertThat(rowCount(table)).isEqualTo(21);
    assertNoStrayPuffins(staging + "/" + ColumnSketchCollector.STATS_DIR, ImmutableList.of());
  }

  @Test
  public void statisticsOffForOneOfTwoTablesLeaveThatTableAsBefore() {
    enableStatistics(TABLE_IDENTIFIER);
    Table other = catalog.createTable(OTHER_TABLE, SCHEMA);
    startCoordinator(coordinatorWithStep(hookedCatalog, metadataEvents, step));

    UUID commitId = startCommit();
    send(written(commitId, TABLE_IDENTIFIER, SCHEMA, rows(0, 10), "task-0", true));
    send(written(commitId, OTHER_TABLE, SCHEMA, rows(0, 10), "task-0", false));
    completeCommit(commitId);

    long snapshotId = ownSnapshotId(commitId);
    awaitStatistics(table, snapshotId, staging);
    other.refresh();
    Snapshot otherSnapshot = other.currentSnapshot();
    assertThat(otherSnapshot.summary()).doesNotContainKey(NdvChain.STATISTICS_ENABLED_PROP);
    assertThat(commitToTable(commitId, OTHER_TABLE).snapshotId())
        .isEqualTo(otherSnapshot.snapshotId());
    assertThat(other.statisticsFiles()).isEmpty();
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::identifier)
        .containsExactly(TABLE_IDENTIFIER);
    assertThat(table.snapshot(snapshotId).summary())
        .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true");
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void stepThatAlwaysFailsChangesNothingOfDataCommits() {
    enableStatistics(TABLE_IDENTIFIER);
    step.behave(
        input -> {
          throw new IllegalStateException("the statistics step fails");
        });
    startCoordinator();

    for (int cycle = 0; cycle < 3; cycle++) {
      UUID commitId = appendCycle(rows(cycle * 10L, cycle * 10L + 10));
      assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId())
          .isEqualTo(ownSnapshotId(commitId));
      assertThat(commitComplete(commitId).validThroughTs())
          .as("valid-through of cycle %s", cycle)
          .isNotNull()
          .isEqualTo(validThrough.get(commitId));
      assertThat(committedControlTopicOffset()).isEqualTo(new OffsetAndMetadata(ctlOffset));
    }

    await("the failed steps put the table into the lag", () -> lag.contains(TABLE_IDENTIFIER));
    await(
        "the Puffin files of the failed steps are deleted",
        () -> noPuffinsUnder(staging + "/" + ColumnSketchCollector.STATS_DIR));
    assertThat(rowCount(table)).isEqualTo(30);
    table.refresh();
    assertThat(table.statisticsFiles()).isEmpty();
  }

  @Test
  public void stepHeldLongerThanCycleHoldsUpNoCycleAndTasksSetMeanwhileMerge()
      throws InterruptedException {
    enableStatistics(TABLE_IDENTIFIER);
    Held held = step.holdNextStep();
    startCoordinator();

    UUID first = appendCycle(rows(0, 10));
    held.awaitStarted();
    UUID second = appendCycle(rows(10, 20));
    UUID third = appendCycle(rows(20, 30));

    for (UUID commitId : ImmutableList.of(second, third)) {
      assertThat(commitComplete(commitId).validThroughTs())
          .as("the cycle does not wait for the step")
          .isNotNull()
          .isEqualTo(validThrough.get(commitId));
    }

    assertThat(step.inputs()).as("only the held step has started").hasSize(1);
    assertThat(rowCount(table)).isEqualTo(30);

    held.release();
    long firstSnapshot = ownSnapshotId(first);
    long secondSnapshot = ownSnapshotId(second);
    long thirdSnapshot = ownSnapshotId(third);
    awaitStatistics(table, thirdSnapshot, staging);

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(firstSnapshot, thirdSnapshot);
    StatisticsStepInput merged = step.inputs().get(1);
    assertThat(merged.commitId()).isEqualTo(third);
    assertThat(merged.ownSnapshotIds()).containsExactlyInAnyOrder(secondSnapshot, thirdSnapshot);
    assertThat(merged.answers())
        .extracting(StatisticsAnswer::snapshotId)
        .containsExactly(secondSnapshot, thirdSnapshot);
    assertThat(record(firstSnapshot)).isNotNull();
    assertThat(record(secondSnapshot)).as("only S of the newer task gets a record").isNull();
    NdvBounds.of(table, thirdSnapshot, "id").written(range(0, 30)).checkRecorded(STATISTICS_LG_K);
    NdvBounds.of(table, thirdSnapshot, "data")
        .written(dataValues(0, 30))
        .checkRecorded(STATISTICS_LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void failingSearchOfOwnSnapshotLeavesCommitToHeadAndTableInLag() {
    enableStatistics(TABLE_IDENTIFIER);
    DataFile foreign = writeDataFile(SCHEMA, rows(100, 101));
    afterDataCommit.set(() -> foreignAppend(foreign));
    hookedCatalog.wrap(
        loaded -> {
          Table spied = spy(loaded);
          doThrow(new IllegalStateException("snapshot lookup fails"))
              .when(spied)
              .snapshot(anyLong());
          return spied;
        });
    startCoordinator();

    UUID commitId = appendCycle(rows(0, 10));

    table.refresh();
    assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId())
        .as("S was not found: the head")
        .isEqualTo(table.currentSnapshot().snapshotId())
        .isNotEqualTo(ownSnapshotId(commitId));
    assertThat(commitComplete(commitId).validThroughTs()).isEqualTo(validThrough.get(commitId));
    verify(metadataEvents).lineageCommit(any(), eq(TABLE_IDENTIFIER), any());
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();

    // the next commit's step removes the lag by the token its task took when it was set. Its
    // NDV is not written: the foreign append behind the unrecorded S has no statistics marker, so
    // it leaves no baseline
    hookedCatalog.wrap(UnaryOperator.identity());
    long next = ownSnapshotId(appendCycle(rows(10, 20)));
    await("the step removes the lag", () -> !lag.contains(TABLE_IDENTIFIER));
    assertThat(step.inputs()).extracting(StatisticsStepInput::snapshotId).containsExactly(next);
  }

  @Test
  public void ownSnapshotNoLongerOnBranchLeavesCommitToHeadAndTableInLag() {
    enableStatistics(TABLE_IDENTIFIER);
    AtomicReference<TableCommitter> committer = new AtomicReference<>();
    startCoordinator(
        started -> {
          committer.set(started);
          return started;
        });
    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    await("the step of the first commit", () -> record(first) != null);
    int stepsBefore = step.inputs().size();

    // the branch goes back to the snapshot before the commit: no snapshot with the commit id is
    // left in the chain above it, and nothing throws
    afterDataCommit.set(() -> table.manageSnapshots().rollbackTo(first).commit());
    UUID commitId;
    try (WarningCapture warnings = WarningCapture.capture()) {
      commitId = appendCycle(rows(10, 20));
      String notOnTheBranch =
          "Snapshot of commit "
              + commitId
              + " to table "
              + TABLE_IDENTIFIER
              + " is not on the branch";
      assertThat(warnings.lines())
          .as("the operator is told that the commit gets no statistics step")
          .anyMatch(line -> line.contains(notOnTheBranch));
    }

    // the steps run on the transition pool: a task set by mistake shows in the inputs only once
    // its step has run, so wait until the table has no step running or waiting
    MergeOnReadTableCommitter merged = (MergeOnReadTableCommitter) committer.get();
    await("no statistics step of the table", () -> !merged.hasStatisticsStep(TABLE_IDENTIFIER));
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(first);
    assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId())
        .as("S is not on the branch: the head")
        .isEqualTo(first)
        .isNotEqualTo(ownSnapshotId(commitId));
    assertThat(commitComplete(commitId).validThroughTs()).isEqualTo(validThrough.get(commitId));
    verify(metadataEvents, times(2)).lineageCommit(any(), eq(TABLE_IDENTIFIER), any());
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(step.inputs()).as("no statistics step of the commit").hasSize(stepsBefore);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void failureToSetTaskLeavesCommitToOwnSnapshotAndTableInLag() {
    enableStatistics(TABLE_IDENTIFIER);
    hookedCatalog.wrap(
        loaded -> {
          Table spied = spy(loaded);
          // fails once the data commit is in: setting the task keeps the table's uuid for the
          // staging cleanup
          doAnswer(
                  invocation -> {
                    if (spied.currentSnapshot() != null) {
                      throw new IllegalStateException("no uuid");
                    }

                    return invocation.callRealMethod();
                  })
              .when(spied)
              .uuid();
          return spied;
        });
    startCoordinator();

    UUID commitId = appendCycle(rows(0, 10));

    assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId())
        .isEqualTo(ownSnapshotId(commitId));
    assertThat(commitComplete(commitId).validThroughTs()).isEqualTo(validThrough.get(commitId));
    verify(metadataEvents).lineageCommit(any(), eq(TABLE_IDENTIFIER), any());
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(committedControlTopicOffset()).isEqualTo(new OffsetAndMetadata(ctlOffset));
    assertThat(rowCount(table)).isEqualTo(10);
    assertThat(step.inputs()).isEmpty();
  }

  @Test
  public void unconfirmedDataCommitPutsTableIntoLag() {
    enableStatistics(TABLE_IDENTIFIER);
    unknownDataCommit.set(new CommitStateUnknownException(new IllegalStateException("lost")));
    startCoordinator();

    UUID commitId = appendCycle(rows(0, 10));

    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(commitComplete(commitId).validThroughTs())
        .as("the table did not commit, as before")
        .isNull();
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .doesNotContain(PayloadType.COMMIT_TO_TABLE);
  }

  @Test
  public void tailPutsTableIntoLagOfCopyOnWriteAndNeverRemovesIt() throws Exception {
    enableStatistics(TABLE_IDENTIFIER);
    StatisticsLag copyOnWriteLag = new StatisticsLag();
    copyOnWriteLag.enter(TABLE_IDENTIFIER);
    long generation = copyOnWriteLag.token(TABLE_IDENTIFIER);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      MergeOnReadTableCommitter tail =
          new MergeOnReadTableCommitter(
              catalog,
              config,
              MetadataEvents.NOOP,
              event -> {},
              pool,
              new StatisticsHealth(0L),
              step,
              copyOnWriteLag,
              MergeOnReadTableCommitter.Mode.TAIL);

      UUID commitId = UUID.randomUUID();
      CommitResult result = tail.commit(tailRequest(commitId, rows(0, 10), 1L));
      assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
      long snapshotId = ownSnapshotId(commitId);
      assertThat(table.snapshot(snapshotId).summary())
          .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true");
      awaitStatistics(table, snapshotId, staging);
      // the task has ended once the pool of one thread runs what was set after it
      pool.submit(() -> {}).get(AWAIT_MS, TimeUnit.MILLISECONDS);
      assertThat(copyOnWriteLag.token(TABLE_IDENTIFIER))
          .as("a written step of the tail does not remove the lag of copy-on-write")
          .isEqualTo(generation);
      assertThat(cleanupTablesOfQueue(tail))
          .as("the tail keeps no table: it has no staging cleanup of merge-on-read")
          .isEmpty();

      step.behave(
          input -> {
            throw new IllegalStateException("the statistics step fails");
          });
      tail.commit(tailRequest(UUID.randomUUID(), rows(10, 20), 2L));
      await(
          "the failed step of the tail enters the lag of copy-on-write",
          () -> copyOnWriteLag.token(TABLE_IDENTIFIER) > generation);
      tail.stop();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void stepFailingOnceIsFollowedByLagStepThatReadsFailedStartFromEmptyBaseline() {
    enableStatistics(TABLE_IDENTIFIER);
    step.failOnce();
    startCoordinator();

    long snapshotId = ownSnapshotId(appendCycle(rows(0, 10)));
    await("the failed step puts the table into the lag", () -> lag.contains(TABLE_IDENTIFIER));

    // no more data: the lag check sets a lag step on the head
    emptyCyclesUntil(
        "the lag step writes the head",
        () -> record(snapshotId) != null && !lag.contains(TABLE_IDENTIFIER));

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(snapshotId, null);
    // the start that failed has the statistics marker: it is read from its data files, from
    // an empty baseline
    NdvBounds.of(table, snapshotId, "id").written(range(0, 10)).checkRecorded(STATISTICS_LG_K);
    assertThat(health().count(Flags::missingBaseline)).isZero();
    assertThat(rowCount(table)).isEqualTo(10);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void failedStartAfterForeignTruncateIsReadFromEmptyBaselineToo() {
    enableStatistics(TABLE_IDENTIFIER);
    foreignAppend(writeDataFile(SCHEMA, rows(100, 110)));
    catalog
        .loadTable(TABLE_IDENTIFIER)
        .newDelete()
        .deleteFromRowFilter(Expressions.alwaysTrue())
        .commit();
    step.failOnce();
    startCoordinator();

    long snapshotId = ownSnapshotId(appendCycle(rows(0, 10)));
    await("the failed step puts the table into the lag", () -> lag.contains(TABLE_IDENTIFIER));
    emptyCyclesUntil(
        "the lag step writes the head",
        () -> record(snapshotId) != null && !lag.contains(TABLE_IDENTIFIER));

    // the truncate is the empty point: the rows it removed are not in the gap
    NdvBounds.of(table, snapshotId, "id").written(range(0, 10)).checkRecorded(STATISTICS_LG_K);
    assertThat(health().count(Flags::missingBaseline)).isZero();
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void statisticsTurnedOnForTableConnectorWroteWithoutThemRaiseMissingBaseline() {
    startCoordinator();
    long written = ownSnapshotId(appendCycleWithoutPuffin(rows(0, 10)));
    assertThat(table.snapshot(written).summary())
        .doesNotContainKey(NdvChain.STATISTICS_ENABLED_PROP);

    // a new configuration restarts the task
    enableStatistics(TABLE_IDENTIFIER);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    restartCoordinator();
    assertThat(lag.contains(TABLE_IDENTIFIER)).as("in the lag at startup").isTrue();

    emptyCyclesUntil("the lag step finds no baseline", () -> !lag.contains(TABLE_IDENTIFIER));

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly((Long) null);
    assertThat(record(written)).as("no NDV without a baseline").isNull();
    assertThat(health().count(Flags::missingBaseline)).isEqualTo(1);
  }

  @Test
  public void unconfirmedDataCommitThatWasAppliedGetsStatisticsOfHeadByLagStep() {
    enableStatistics(TABLE_IDENTIFIER);
    unknownDataCommit.set(new CommitStateUnknownException(new IllegalStateException("lost")));
    startCoordinator();

    UUID commitId = appendCycle(rows(0, 10));
    long snapshotId = ownSnapshotId(commitId);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(commitComplete(commitId).validThroughTs()).isNull();

    // the responses stay buffered, and the next cycle finds them below the committed offsets:
    // nothing to commit, and the table of the lag gets a lag step as if it had no responses
    emptyCyclesUntil(
        "the lag step writes the head",
        () -> record(snapshotId) != null && !lag.contains(TABLE_IDENTIFIER));

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly((Long) null);
    NdvBounds.of(table, snapshotId, "id").written(range(0, 10)).checkRecorded(STATISTICS_LG_K);
    assertThat(rowCount(table)).isEqualTo(10);
    assertThat(committedControlTopicOffset()).isEqualTo(new OffsetAndMetadata(ctlOffset));
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void taskOfPreviousCycleWrittenAfterNewLagLeavesLag() throws InterruptedException {
    enableStatistics(TABLE_IDENTIFIER);
    Held held = step.holdNextStep();
    startCoordinator();

    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    held.awaitStarted();
    // the data commit of the next cycle is applied but not confirmed while the task of the first
    // one runs: the table enters the lag for the newer snapshot
    unknownDataCommit.set(new CommitStateUnknownException(new IllegalStateException("lost")));
    long second = ownSnapshotId(appendCycle(rows(10, 20)));
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();

    held.release();
    await("the held task writes its own snapshot", () -> record(first) != null);

    // its token predates the entry, so the lag stays: the buffered responses of the second
    // cycle are below the committed offsets now, nothing to commit, and a lag step on the head
    emptyCyclesUntil(
        "the lag step writes the newer snapshot",
        () -> record(second) != null && !lag.contains(TABLE_IDENTIFIER));

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(first, null);
    NdvBounds.of(table, second, "id").written(range(0, 20)).checkRecorded(STATISTICS_LG_K);
    assertThat(rowCount(table)).isEqualTo(20);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void foreignRecordOnHeadBeforeLagStepRemovesLagAsNotNeeded() {
    enableStatistics(TABLE_IDENTIFIER);
    step.failOnce();
    startCoordinator();
    long snapshotId = ownSnapshotId(appendCycle(rows(0, 10)));
    await("the failed step puts the table into the lag", () -> lag.contains(TABLE_IDENTIFIER));

    Held held = step.holdNextStep();
    emptyCyclesUntil("the lag step starts", held::hasStarted);
    // ANALYZE writes the head while the lag step waits
    String foreign = foreignRecord(snapshotId);
    held.release();

    await("the lag step removes the lag", () -> !lag.contains(TABLE_IDENTIFIER));
    assertThat(step.results()).extracting(Result::outcome).containsExactly(Outcome.NOT_NEEDED);
    assertThat(record(snapshotId).path()).as("the foreign record stays").isEqualTo(foreign);
  }

  @Test
  public void stopWithWaitingTaskAndRestartWriteHeadOfTableOfIcebergTables()
      throws InterruptedException {
    stopWithWaitingTaskAndRestart(ImmutableMap.of("iceberg.tables", TABLE_IDENTIFIER.toString()));
  }

  @Test
  public void stopWithWaitingTaskAndRestartWriteHeadOfTableMappedToTopic()
      throws InterruptedException {
    stopWithWaitingTaskAndRestart(
        ImmutableMap.of(
            "routing.strategy",
            "topic-to-table",
            "iceberg.tables.topic-to-table-mapping",
            SRC_TOPIC_NAME + ":" + TABLE_IDENTIFIER));
  }

  /**
   * {@code stop()} drops the waiting task and stops the running one; after the restart the table is
   * in the lag as a static table of {@code routing}, and a lag step writes its head.
   */
  private void stopWithWaitingTaskAndRestart(Map<String, String> routing)
      throws InterruptedException {
    Map<String, String> props = Maps.newHashMap(routing);
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", SRC_TOPIC_NAME);
    when(config.configuredTables()).thenReturn(new IcebergSinkConfig(props).configuredTables());
    enableStatistics(TABLE_IDENTIFIER);
    Held held = step.holdNextStep();
    startCoordinator();

    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    held.awaitStarted();
    long second = ownSnapshotId(appendCycle(rows(10, 20)));
    restartCoordinator();

    assertThat(record(first)).isNull();
    assertThat(record(second)).isNull();
    assertThat(lag.contains(TABLE_IDENTIFIER)).as("in the lag at startup").isTrue();
    emptyCyclesUntil(
        "the lag step writes the head",
        () -> record(second) != null && !lag.contains(TABLE_IDENTIFIER));

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(first, null);
    NdvBounds.of(table, second, "id").written(range(0, 20)).checkRecorded(STATISTICS_LG_K);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void withTwoCommitThreadsTablesOfLagCatchUpThoughOneOfThemAlwaysFails() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier third = TableIdentifier.of(NAMESPACE, "tbl3");
    catalog.createTable(OTHER_TABLE, SCHEMA);
    catalog.createTable(third, SCHEMA);
    List<TableIdentifier> tables = ImmutableList.of(TABLE_IDENTIFIER, OTHER_TABLE, third);
    enableStatistics(tables.toArray(new TableIdentifier[0]));
    step.behave(
        input -> {
          throw new IllegalStateException("the statistics step fails");
        });
    startCoordinator();

    UUID commitId = startCommit();
    for (TableIdentifier tableIdentifier : tables) {
      send(written(commitId, tableIdentifier, SCHEMA, rows(0, 10), "task-0", true));
    }

    completeCommit(commitId);
    await("the failed steps put the tables into the lag", () -> lag.tables().size() == 3);

    // from now on only the step of the first table fails; lag steps get at most half of the two
    // commit threads: one at a time
    step.behave(
        input -> {
          if (input.identifier().equals(TABLE_IDENTIFIER)) {
            throw new IllegalStateException("the statistics step fails");
          }

          return null;
        });
    emptyCyclesUntil(
        "the two other tables catch up",
        () -> recordOfHead(OTHER_TABLE) != null && recordOfHead(third) != null);

    assertThat(lag.tables()).containsExactly(TABLE_IDENTIFIER);
    assertThat(recordOfHead(TABLE_IDENTIFIER)).isNull();
  }

  @Test
  public void tablesOfLagThatCannotBeLoadedOrCarryChangeSetHoldUpNoCycle() {
    catalog.createTable(OTHER_TABLE, SCHEMA);
    enableStatistics(TABLE_IDENTIFIER, OTHER_TABLE);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER, OTHER_TABLE));
    // the last snapshot of this connector carries a change set a drain left unfinished
    table
        .newAppend()
        .appendFile(writeDataFile(SCHEMA, rows(0, 10)))
        .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":0}")
        .set("kafka.connect.copy-on-write.change-set-id", UUID.randomUUID().toString())
        .commit();
    hookedCatalog.failLoading(OTHER_TABLE, new IllegalStateException("the catalog fails"));
    startCoordinator();

    for (int cycle = 0; cycle < 3; cycle++) {
      UUID commitId = emptyCycle();
      assertThat(commitComplete(commitId).validThroughTs())
          .as("valid-through of cycle %s", cycle)
          .isNotNull()
          .isEqualTo(validThrough.get(commitId));
    }

    assertThat(lag.tables()).containsExactlyInAnyOrder(TABLE_IDENTIFIER, OTHER_TABLE);
    assertThat(hookedCatalog.loads(OTHER_TABLE)).as("tried in every cycle").isEqualTo(3);
    assertThat(step.inputs()).isEmpty();
  }

  @Test
  public void staticTableInLagAndItsResponsesInOneCycleGetOneCommitAndOneTask() {
    enableStatistics(TABLE_IDENTIFIER);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    // a history without the connector's statistics: the step finds no baseline and raises a flag
    foreignAppend(writeDataFile(SCHEMA, rows(100, 110)));
    AtomicInteger commits = new AtomicInteger();
    startCoordinator(
        committer ->
            new DelegatingCommitter(committer) {
              @Override
              public CommitResult commit(TableCommitRequest request) {
                commits.incrementAndGet();
                return super.commit(request);
              }
            });
    assertThat(lag.contains(TABLE_IDENTIFIER)).as("in the lag at startup").isTrue();

    UUID commitId = appendCycle(rows(0, 10));
    long snapshotId = ownSnapshotId(commitId);
    await("the task removes the lag of the startup", () -> !lag.contains(TABLE_IDENTIFIER));

    assertThat(commits).as("one commit() of the table in the cycle").hasValue(1);
    assertThat(commitToTable(commitId, TABLE_IDENTIFIER).snapshotId()).isEqualTo(snapshotId);
    assertThat(commitComplete(commitId).validThroughTs()).isEqualTo(validThrough.get(commitId));
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(snapshotId);
    assertThat(health().count(Flags::missingBaseline))
        .as("the metric counts the table once")
        .isEqualTo(1);
  }

  @Test
  public void tableRecreatedWhileTaskOfOldOneWaitsKeepsOneRecordOfHealth()
      throws InterruptedException {
    enableStatistics(TABLE_IDENTIFIER);
    startCoordinator();
    long old = ownSnapshotId(appendCycle(rows(0, 10)));
    awaitStatistics(table, old, staging);
    assertThat(health().count(flags -> true)).isEqualTo(1);

    Held held = step.holdNextStep();
    appendCycle(rows(10, 20));
    held.awaitStarted();
    appendCycle(rows(20, 30));

    // the table is re-created under its name while a task of the old one waits, and another
    // writer fills the new one
    catalog.dropTable(TABLE_IDENTIFIER, false);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    foreignAppend(writeDataFile(SCHEMA, rows(100, 110)));
    held.release();

    // the snapshots of the old tasks are gone with the old table: the lag is for the new one
    emptyCyclesUntil(
        "the lag step on the head of the new table",
        () -> step.inputs().size() == 4 && !lag.contains(TABLE_IDENTIFIER));

    assertThat(step.inputs().get(3).snapshotId()).as("a lag step").isNull();
    assertThat(health().count(flags -> true)).as("one record of health").isEqualTo(1);
    assertThat(health().count(Flags::missingBaseline))
        .as("the history of the new table has no baseline")
        .isEqualTo(1);
  }

  @Test
  public void lagStepEndingBetweenPendingTablesAndCommitLeavesTableUnloaded() {
    enableStatistics(TABLE_IDENTIFIER);
    step.failOnce();
    AtomicReference<Runnable> afterPendingTables = new AtomicReference<>();
    startCoordinator(
        committer ->
            new DelegatingCommitter(committer) {
              @Override
              public Set<TableReference> pendingTables() {
                Set<TableReference> pending = super.pendingTables();
                Runnable hook = afterPendingTables.getAndSet(null);
                if (hook != null) {
                  hook.run();
                }

                return pending;
              }
            });
    long snapshotId = ownSnapshotId(appendCycle(rows(0, 10)));
    await("the failed step puts the table into the lag", () -> lag.contains(TABLE_IDENTIFIER));
    Held held = step.holdNextStep();
    emptyCyclesUntil("the lag step starts", held::hasStarted);

    // the next cycle lists the table of the lag; its lag step ends before commit()
    AtomicInteger loadsBeforeCommit = new AtomicInteger();
    afterPendingTables.set(
        () -> {
          held.release();
          await("the lag step removes the lag", () -> !lag.contains(TABLE_IDENTIFIER));
          loadsBeforeCommit.set(hookedCatalog.loads(TABLE_IDENTIFIER));
        });
    UUID commitId = emptyCycle();

    assertThat(afterPendingTables.get()).as("the hook ran").isNull();
    assertThat(hookedCatalog.loads(TABLE_IDENTIFIER))
        .as("commit() answers without loading the table")
        .isEqualTo(loadsBeforeCommit.get());
    assertThat(commitComplete(commitId).validThroughTs())
        .isNotNull()
        .isEqualTo(validThrough.get(commitId));
    assertThat(record(snapshotId)).isNotNull();
  }

  /** How the coordinator stops the step that reads the gap. */
  enum Stop {
    /** {@code Coordinator.terminate()}: the flag of the committer, then the pools interrupted. */
    TERMINATE,
    /** The flag of the committer alone. */
    COMMITTER_FLAG,
    /** The thread of the pool interrupted alone, and the read of the file throws on it. */
    INTERRUPT
  }

  /**
   * The real step of S reads an endless file of another writer in the gap, and the coordinator
   * stops it within the timeout of terminate(): the step is STOPPED and changes nothing.
   */
  @ParameterizedTest
  @EnumSource(Stop.class)
  public void stopDuringReadOfGapStopsStepAndChangesNothing(Stop stop) {
    enableStatistics(TABLE_IDENTIFIER);
    AtomicReference<TableCommitter> committer = new AtomicReference<>();
    startCoordinator(
        started -> {
          committer.set(started);
          return started;
        });
    long first = ownSnapshotId(appendCycle(rows(0, 10)));
    awaitStatistics(table, first, staging);
    EndlessAvroFile gap = EndlessAvroFile.write(table, SCHEMA, rows(1_000, 2_000));
    foreignAppend(gap.dataFile());
    endless = stop == Stop.INTERRUPT ? gap.armInterruptible() : gap.arm();
    long generation = lag.token(TABLE_IDENTIFIER);
    long version = health().version(TABLE_IDENTIFIER);
    int results = step.results().size();
    AtomicReference<DataWritten> answer = new AtomicReference<>();

    long second;
    try (WarningCapture warnings = WarningCapture.capture()) {
      UUID commitId =
          cycle(
              id -> {
                answer.set(written(id, TABLE_IDENTIFIER, SCHEMA, rows(10, 20), "task-0", true));
                return answer.get();
              });
      second = ownSnapshotId(commitId);
      gap.awaitServed(15_000L);
      stop(stop, committer.get(), gap);

      await("the step of S is over", () -> step.results().size() > results);
      assertThat(warnings.lines()).as("no WARN").isEmpty();
    }

    assertThat(step.inputs().get(step.inputs().size() - 1).snapshotId()).isEqualTo(second);
    assertThat(step.results().get(results).outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(lag.contains(TABLE_IDENTIFIER)).as("S is not in the lag").isFalse();
    assertThat(lag.token(TABLE_IDENTIFIER)).isEqualTo(generation);
    assertThat(health().version(TABLE_IDENTIFIER)).as("no health transition").isEqualTo(version);
    assertThat(record(second)).isNull();
    assertThat(noStatisticsFileOf(second)).as("no NDV file of S").isTrue();
    assertThat(inMemoryFiles())
        .as("the Puffin file of the answer is kept")
        .contains(answer.get().sketchFile().location());
  }

  private void stop(Stop stop, TableCommitter committer, EndlessAvroFile gap) {
    switch (stop) {
      case TERMINATE:
        coordinator.terminationTimeout(Duration.ofSeconds(2));
        // throws once the step outlives the timeout
        coordinator.terminate();
        coordinator = null;
        break;
      case COMMITTER_FLAG:
        committer.stop();
        break;
      default:
        gap.reader().interrupt();
        break;
    }
  }

  private void startCoordinator() {
    startCoordinator(UnaryOperator.identity());
  }

  /** A coordinator with the lag of the test; {@code wrap} may watch its committer. */
  private void startCoordinator(UnaryOperator<TableCommitter> wrap) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    StatisticsLag committerLag = lag;
    startCoordinator(
        new Coordinator(
            config,
            ImmutableList.of(),
            clientFactory,
            mock(SinkTaskContext.class),
            step,
            (send, sendInOneTransaction, transitionPool, health, stepOf) -> {
              coordinatorHealth.set(health);
              return wrap.apply(
                  new MergeOnReadTableCommitter(
                      hookedCatalog,
                      config,
                      metadataEvents,
                      send,
                      transitionPool,
                      health,
                      stepOf,
                      committerLag,
                      MergeOnReadTableCommitter.Mode.STANDALONE));
            }));
  }

  /** A restart of the task: a new coordinator on new clients, with a lag of its own. */
  private void restartCoordinator() {
    coordinator.terminate();
    producer = new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    when(clientFactory.createProducer(any())).thenReturn(producer);
    when(clientFactory.createConsumer(any())).thenReturn(consumer);
    lag = new StatisticsLag();
    startCoordinator();
  }

  private StatisticsHealth health() {
    return coordinatorHealth.get();
  }

  private void startCoordinator(Coordinator newCoordinator) {
    this.coordinator = newCoordinator;
    coordinator.start();
    initConsumer();
  }

  private UUID startCommit() {
    coordinator.process();
    Event event = AvroUtil.decode(producer.history().get(producer.history().size() - 1).value());
    assertThat(event.type()).isEqualTo(PayloadType.START_COMMIT);
    return ((StartCommit) event.payload()).commitId();
  }

  private void send(DataWritten payload) {
    Event event = new Event(CONNECT_CONSUMER_GROUP_ID, payload);
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "key", AvroUtil.encode(event)));
  }

  private void completeCommit(UUID commitId) {
    OffsetDateTime ts = EventTestUtil.now();
    validThrough.put(commitId, ts);
    Event event =
        new Event(
            CONNECT_CONSUMER_GROUP_ID,
            new DataComplete(
                commitId, ImmutableList.of(new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 1L, ts))));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "key", AvroUtil.encode(event)));
    coordinator.process();
  }

  private UUID appendCycle(List<Record> rows) {
    return cycle(commitId -> written(commitId, TABLE_IDENTIFIER, SCHEMA, rows, "task-0", true));
  }

  private UUID appendCycleWithoutPuffin(List<Record> rows) {
    return cycle(commitId -> written(commitId, TABLE_IDENTIFIER, SCHEMA, rows, "task-0", false));
  }

  private UUID cycle(Function<UUID, DataWritten> response) {
    UUID commitId = startCommit();
    send(response.apply(commitId));
    completeCommit(commitId);
    return commitId;
  }

  private UUID emptyCycle() {
    UUID commitId = startCommit();
    completeCommit(commitId);
    return commitId;
  }

  /**
   * Runs cycles in which no writer answers until {@code done}; each of them claims its
   * valid-through, whatever the statistics do.
   */
  private void emptyCyclesUntil(String what, BooleanSupplier done) {
    for (int cycle = 0; cycle < 20; cycle++) {
      UUID commitId = emptyCycle();
      assertThat(commitComplete(commitId).validThroughTs())
          .as("valid-through of cycle %s without responses", cycle)
          .isNotNull()
          .isEqualTo(validThrough.get(commitId));
      if (holdsWithin(TimeUnit.SECONDS.toMillis(1), done)) {
        return;
      }
    }

    throw new AssertionError("Not after 20 cycles: " + what);
  }

  private static boolean holdsWithin(long timeoutMs, BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        return false;
      }

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    return true;
  }

  private TableCommitRequest tailRequest(UUID commitId, List<Record> rows, long offset) {
    DataWritten payload = written(commitId, TABLE_IDENTIFIER, SCHEMA, rows, "task-0", true);
    Envelope envelope = new Envelope(new Event(CONNECT_CONSUMER_GROUP_ID, payload), 0, offset);
    return new TableCommitRequest(
        payload.tableReference(),
        ImmutableList.of(envelope),
        ImmutableMap.of(0, offset + 1),
        commitId,
        null);
  }

  private CommitToTable commitToTable(UUID commitId, TableIdentifier tableIdentifier) {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()))
        .filter(event -> event.type() == PayloadType.COMMIT_TO_TABLE)
        .map(event -> (CommitToTable) event.payload())
        .filter(payload -> payload.commitId().equals(commitId))
        .filter(payload -> payload.tableReference().identifier().equals(tableIdentifier))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No CommitToTable of " + tableIdentifier));
  }

  private CommitComplete commitComplete(UUID commitId) {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()))
        .filter(event -> event.type() == PayloadType.COMMIT_COMPLETE)
        .map(event -> (CommitComplete) event.payload())
        .filter(payload -> payload.commitId().equals(commitId))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No CommitComplete of " + commitId));
  }

  private OffsetAndMetadata committedControlTopicOffset() {
    TopicPartition partition = new TopicPartition(CTL_TOPIC_NAME, 0);
    return consumer.committed(Set.of(partition)).get(partition);
  }

  private long ownSnapshotId(UUID commitId) {
    table.refresh();
    for (Snapshot snapshot : table.snapshots()) {
      if (commitId.toString().equals(snapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP))) {
        return snapshot.snapshotId();
      }
    }

    throw new AssertionError("No snapshot of commit " + commitId);
  }

  private long ownSnapshotParentOf(long snapshotId) {
    return table.snapshot(snapshotId).parentId();
  }

  private StatisticsFile record(long snapshotId) {
    table.refresh();
    return table.statisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  /** The NDV record of the head of the table, or {@code null}. */
  private StatisticsFile recordOfHead(TableIdentifier tableIdentifier) {
    Table loaded = catalog.loadTable(tableIdentifier);
    long headId = loaded.currentSnapshot().snapshotId();
    return loaded.statisticsFiles().stream()
        .filter(file -> file.snapshotId() == headId)
        .findFirst()
        .orElse(null);
  }

  /**
   * Registers a foreign NDV record of every column of the snapshot, as ANALYZE does, whose file
   * does not exist: a step that finds it has nothing to read.
   *
   * @return its path
   */
  private String foreignRecord(long snapshotId) {
    Table target = catalog.loadTable(TABLE_IDENTIFIER);
    Snapshot snapshot = target.snapshot(snapshotId);
    List<BlobMetadata> blobs = Lists.newArrayList();
    for (Types.NestedField column : SCHEMA.columns()) {
      blobs.add(
          new GenericBlobMetadata(
              StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
              snapshotId,
              snapshot.sequenceNumber(),
              ImmutableList.of(column.fieldId()),
              ImmutableMap.of("ndv", "1")));
    }

    String path = "mem://nowhere/stats-" + snapshotId + "-" + UUID.randomUUID() + ".puffin";
    target
        .updateStatistics()
        .setStatistics(new GenericStatisticsFile(snapshotId, path, 100L, 10L, blobs))
        .commit();
    return path;
  }

  /** No NDV file of the snapshot in metadata, registered or not. */
  private boolean noStatisticsFileOf(long snapshotId) {
    String prefix = table.location() + "/metadata/stats-" + snapshotId + "-";
    return inMemoryFiles().stream().noneMatch(path -> path.startsWith(prefix));
  }

  private boolean noPuffinsUnder(String directory) {
    try {
      assertNoStrayPuffins(directory, ImmutableList.of());
      return true;
    } catch (AssertionError e) {
      return false;
    }
  }

  private static Set<TableIdentifier> cleanupTablesOfQueue(MergeOnReadTableCommitter committer) {
    try {
      Field steps = MergeOnReadTableCommitter.class.getDeclaredField("statisticsSteps");
      steps.setAccessible(true);
      return ((StatisticsStepQueue) steps.get(committer)).cleanupTables().keySet();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<String> inMemoryFiles() {
    try {
      Field files = InMemoryFileIO.class.getDeclaredField("IN_MEMORY_FILES");
      files.setAccessible(true);
      return ImmutableList.copyOf(((Map<String, ?>) files.get(null)).keySet());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private void foreignAppend(DataFile file) {
    catalog.loadTable(TABLE_IDENTIFIER).newAppend().appendFile(file).commit();
  }

  private void foreignAppendToBranch(String branch, DataFile file) {
    catalog.loadTable(TABLE_IDENTIFIER).newAppend().appendFile(file).toBranch(branch).commit();
  }

  private long branchHead(String branch) {
    table.refresh();
    return table.snapshot(branch).snapshotId();
  }

  private void enableStatisticsOnBranch(TableIdentifier tableIdentifier, String branch) {
    enableStatistics(tableIdentifier);
    when(config.tableConfig(tableIdentifier.toString()))
        .thenReturn(
            new TableSinkConfig(
                null, ImmutableList.of(), ImmutableList.of(), branch, true, false, null));
  }

  private static long rowCount(Table readTable) {
    readTable.refresh();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(readTable).build()) {
      long count = 0;
      for (Record ignored : rows) {
        count += 1;
      }

      return count;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * A real data file of the rows and, if {@code withPuffin}, the writer's intermediate Puffin;
   * without it, a {@code DataWritten} of the released version.
   */
  private DataWritten written(
      UUID commitId,
      TableIdentifier tableIdentifier,
      Schema schema,
      List<Record> rows,
      String taskId,
      boolean withPuffin) {
    Table target = catalog.loadTable(tableIdentifier);
    DataFile file = writeDataFile(target, schema, rows);
    TableReference reference = TableReference.of(catalog.name(), tableIdentifier, target.uuid());
    if (!withPuffin) {
      return new DataWritten(
          StructType.of(),
          commitId,
          reference,
          ImmutableList.of(file),
          ImmutableList.of(),
          ImmutableList.of(SRC_TOPIC_NAME));
    }

    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            tableIdentifier, target, staging, taskId, STATISTICS_LG_K, null, false, schema);
    rows.forEach(sketches::update);
    SketchFile sketch = sketches.write(commitId);
    assertThat(sketch).as("the writer's Puffin").isNotNull();
    return new DataWritten(
        StructType.of(),
        commitId,
        reference,
        ImmutableList.of(file),
        ImmutableList.of(),
        ImmutableList.of(SRC_TOPIC_NAME),
        sketch);
  }

  /**
   * The two results of one writer in one cycle (a schema change between them): each its own data
   * file, both under the one Puffin of the writer over all the rows (IcebergWriter.complete).
   */
  private List<DataWritten> sharedPuffinResults(
      UUID commitId, List<Record> firstRows, List<Record> secondRows) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER, table, staging, "task-0", STATISTICS_LG_K, null, false, SCHEMA);
    firstRows.forEach(sketches::update);
    secondRows.forEach(sketches::update);
    SketchFile sketch = sketches.write(commitId);
    assertThat(sketch).as("the writer's Puffin").isNotNull();
    return ImmutableList.of(
        dataWritten(
            commitId,
            ImmutableList.of(writeDataFile(SCHEMA, firstRows)),
            ImmutableList.of(),
            sketch),
        dataWritten(
            commitId,
            ImmutableList.of(writeDataFile(SCHEMA, secondRows)),
            ImmutableList.of(),
            sketch));
  }

  private DataWritten dataWritten(
      UUID commitId, List<DataFile> dataFiles, List<DeleteFile> deleteFiles, SketchFile sketch) {
    return new DataWritten(
        StructType.of(),
        commitId,
        TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid()),
        dataFiles,
        deleteFiles,
        ImmutableList.of(SRC_TOPIC_NAME),
        sketch);
  }

  private DataFile writeDataFile(Schema schema, List<Record> rows) {
    return writeDataFile(table, schema, rows);
  }

  private DataFile writeDataFile(Table target, Schema schema, List<Record> rows) {
    fileCount += 1;
    target.refresh();
    OutputFileFactory files = OutputFileFactory.builderFor(target, 1, fileCount).build();
    EncryptedOutputFile output = files.newOutputFile();
    try {
      DataWriter<Record> writer =
          new GenericFileWriterFactory.Builder(target)
              .dataSchema(schema)
              .build()
              .newDataWriter(output, PartitionSpec.unpartitioned(), null);
      try {
        rows.forEach(writer::write);
      } finally {
        writer.close();
      }

      return writer.toDataFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private DataFile fakeDataFile(long recordCount) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(recordCount)
        .build();
  }

  private DeleteFile fakeDeleteFile() {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath(table.location() + "/data/" + UUID.randomUUID() + "-deletes.parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(1L)
        .build();
  }

  private SketchFile fakeSketch() {
    return new SketchFile(staging + "/stats/" + UUID.randomUUID() + ".puffin", 10L, 5L);
  }

  private static List<Record> rows(long from, long to) {
    List<Record> rows = Lists.newArrayList();
    for (long id = from; id < to; id++) {
      Record row = GenericRecord.create(SCHEMA);
      row.setField("id", id);
      row.setField("data", data(id));
      row.setField("date", "2026-09-" + (10 + id % 5));
      rows.add(row);
    }

    return rows;
  }

  /** Rows of the schema with {@code score}: half of them without it. */
  private static List<Record> scoredRows(long from, long to) {
    List<Record> rows = Lists.newArrayList();
    for (long id = from; id < to; id++) {
      Record row = GenericRecord.create(SCORED);
      row.setField("id", id);
      row.setField("data", data(id));
      row.setField("date", "2026-09-" + (10 + id % 5));
      row.setField("score", id % 2 == 0 ? (int) (id * 3) : null);
      rows.add(row);
    }

    return rows;
  }

  /** Rows of the schema with {@code score} of long, half of them without it, past the int range. */
  private static List<Record> widenedRows(Schema schema, long from, long to) {
    List<Record> rows = Lists.newArrayList();
    for (long id = from; id < to; id++) {
      Record row = GenericRecord.create(schema);
      row.setField("id", id);
      row.setField("data", data(id));
      row.setField("date", "2026-09-" + (10 + id % 5));
      row.setField("score", id % 2 == 0 ? id * 3 + 10_000_000_000L : null);
      rows.add(row);
    }

    return rows;
  }

  /** Rows whose {@code data} is one of {@code distinct} values starting with {@code prefix}. */
  private static List<Record> taggedRows(long from, long to, String prefix, int distinct) {
    List<Record> rows = rows(from, to);
    rows.forEach(row -> row.setField("data", prefix + ((Long) row.getField("id") % distinct)));
    return rows;
  }

  private static List<Object> values(List<Record> rows, String column) {
    return rows.stream().map(row -> row.getField(column)).collect(Collectors.toList());
  }

  private static String data(long id) {
    return "d" + (id % 7);
  }

  private static List<Object> dataValues(long from, long to) {
    return LongStream.range(from, to)
        .mapToObj(TestMergeOnReadStatistics::data)
        .collect(Collectors.toList());
  }

  private static List<Long> range(long from, long to) {
    return LongStream.range(from, to).boxed().collect(Collectors.toList());
  }

  private int reads(String location) {
    AtomicInteger count = reads.get(location);
    return count == null ? 0 : count.get();
  }

  private static List<Long> ids(List<Record> rows) {
    return rows.stream().map(row -> (Long) row.getField("id")).collect(Collectors.toList());
  }

  /** A step held on a latch: it has started, and it goes on once released. */
  private static final class Held {
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    void awaitStarted() throws InterruptedException {
      assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).as("the step started").isTrue();
    }

    boolean hasStarted() {
      return started.getCount() == 0;
    }

    void release() {
      release.countDown();
    }
  }

  /**
   * The statistics step of the tests: the real one unless {@link #behave} says otherwise. It can
   * hold the next step on a latch and records two steps of one table at once.
   */
  private static final class ControlledStep extends StatisticsStep {
    private final List<StatisticsStepInput> inputs = new CopyOnWriteArrayList<>();
    private final List<String> overlaps = new CopyOnWriteArrayList<>();
    private final Map<TableIdentifier, AtomicInteger> running = Maps.newConcurrentMap();
    private final List<Held> helds = new CopyOnWriteArrayList<>();
    private final AtomicReference<Held> holdNext = new AtomicReference<>();
    private final List<Result> results = new CopyOnWriteArrayList<>();
    private volatile Function<StatisticsStepInput, Result> behavior = null;

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      AtomicInteger count = running.computeIfAbsent(input.identifier(), id -> new AtomicInteger());
      if (count.incrementAndGet() > 1) {
        overlaps.add(input.identifier() + " at " + input.snapshotId());
      }

      try {
        Held held = holdNext.getAndSet(null);
        if (held != null) {
          held.started.countDown();
          if (!held.release.await(AWAIT_MS, TimeUnit.MILLISECONDS)) {
            overlaps.add("the step of " + input.snapshotId() + " was never released");
          }
        }

        Function<StatisticsStepInput, Result> scripted = behavior;
        Result scriptedResult = scripted == null ? null : scripted.apply(input);
        // a behavior that answers null runs the real step
        Result result = scriptedResult == null ? super.run(input) : scriptedResult;
        results.add(result);
        return result;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return new Result(Outcome.STOPPED, null);
      } finally {
        count.decrementAndGet();
      }
    }

    Held holdNextStep() {
      Held held = new Held();
      helds.add(held);
      holdNext.set(held);
      return held;
    }

    void releaseAll() {
      helds.forEach(Held::release);
    }

    void behave(Function<StatisticsStepInput, Result> newBehavior) {
      this.behavior = newBehavior;
    }

    /** The next step throws; the steps after it are real. */
    void failOnce() {
      behave(
          input -> {
            behave(null);
            throw new IllegalStateException("the statistics step fails once");
          });
    }

    /** The results the steps returned, in the order they ended; a step that threw has none. */
    List<Result> results() {
      return results;
    }

    List<StatisticsStepInput> inputs() {
      return inputs;
    }

    List<String> overlaps() {
      return overlaps;
    }
  }

  /** Passes every call to the committer of the coordinator; a test overrides what it watches. */
  private static class DelegatingCommitter implements TableCommitter {
    private final TableCommitter delegate;

    private DelegatingCommitter(TableCommitter delegate) {
      this.delegate = delegate;
    }

    @Override
    public CommitResult commit(TableCommitRequest request) {
      return delegate.commit(request);
    }

    @Override
    public Set<TableReference> pendingTables() {
      return delegate.pendingTables();
    }

    @Override
    public void stop() {
      delegate.stop();
    }
  }

  /**
   * The catalog of the coordinator: its tables commit through {@link HookedOperations}, a test can
   * wrap them (a spy that fails a call), fail their loads and count them.
   */
  private final class HookedCatalog implements Catalog {
    private final Catalog delegate;
    private final Map<TableIdentifier, AtomicInteger> loads = Maps.newConcurrentMap();
    private final Map<TableIdentifier, RuntimeException> loadFailures = Maps.newConcurrentMap();
    private volatile UnaryOperator<Table> wrapper = UnaryOperator.identity();

    private HookedCatalog(Catalog delegate) {
      this.delegate = delegate;
    }

    void wrap(UnaryOperator<Table> newWrapper) {
      this.wrapper = newWrapper;
    }

    void failLoading(TableIdentifier identifier, RuntimeException failure) {
      loadFailures.put(identifier, failure);
    }

    /** The loads of the table so far, by the committer and by its steps. */
    int loads(TableIdentifier identifier) {
      AtomicInteger count = loads.get(identifier);
      return count == null ? 0 : count.get();
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return delegate.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return delegate.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      delegate.renameTable(from, to);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      loads.computeIfAbsent(identifier, id -> new AtomicInteger()).incrementAndGet();
      RuntimeException failure = loadFailures.get(identifier);
      if (failure != null) {
        throw failure;
      }

      Table loaded = delegate.loadTable(identifier);
      TableOperations operations = ((HasTableOperations) loaded).operations();
      return wrapper.apply(new BaseTable(new HookedOperations(operations), loaded.name()));
    }
  }

  /**
   * Operations that run the test's hook right after a data commit of the connector (another writer
   * appending before the committer reads the head) or leave its outcome unknown.
   */
  private final class HookedOperations implements TableOperations {
    private final TableOperations delegate;

    private HookedOperations(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
      if (!isDataCommit(base, metadata)) {
        return;
      }

      RuntimeException unknown = unknownDataCommit.getAndSet(null);
      if (unknown != null) {
        throw unknown;
      }

      Runnable hook = afterDataCommit.getAndSet(null);
      if (hook != null) {
        hook.run();
      }
    }

    private boolean isDataCommit(TableMetadata base, TableMetadata metadata) {
      Snapshot committed = metadata.currentSnapshot();
      Snapshot before = base == null ? null : base.currentSnapshot();
      return committed != null
          && committed.summary().containsKey(COMMIT_ID_SNAPSHOT_PROP)
          && (before == null || before.snapshotId() != committed.snapshotId());
    }

    @Override
    public FileIO io() {
      return countReads || endless != null ? new CountingFileIO(delegate.io()) : delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
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
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }
  }

  /**
   * Counts the input files opened by path, for the tests that set {@code countReads}, and serves
   * the {@code endless} file.
   */
  private final class CountingFileIO implements FileIO {
    private final FileIO delegate;

    private CountingFileIO(FileIO delegate) {
      this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
      count(path);
      return served(path, delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      count(path);
      return served(path, delegate.newInputFile(path, length));
    }

    private InputFile served(String path, InputFile file) {
      EndlessAvroFile endlessFile = endless;
      return endlessFile == null ? file : endlessFile.serve(path, file);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    private void count(String path) {
      if (!countReads) {
        return;
      }

      reads.computeIfAbsent(path, location -> new AtomicInteger()).incrementAndGet();
    }
  }
}
