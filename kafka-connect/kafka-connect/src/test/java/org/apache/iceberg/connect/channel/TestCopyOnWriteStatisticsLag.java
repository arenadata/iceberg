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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.StatisticsAssertions;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
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
 * The statistics lag of copy-on-write, with the committer driven cycle by cycle as {@code
 * Coordinator.doCommit} drives it. A lag step held in {@link HeldPool} runs when, and on the
 * thread, the test says.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class TestCopyOnWriteStatisticsLag {
  private static final String CTL_TOPIC = "ctl-topic";
  private static final String GROUP_ID = "cg-connect";
  private static final String SRC_TOPIC = "src-topic";
  private static final long AWAIT_SECONDS = 10L;
  private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);
  private static final Map<String, List<TopicPartitionRef>> ACTIVE_TASKS =
      ImmutableMap.of("task-0", ImmutableList.of());

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);
  private static final Schema CHANGE =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "op", Types.IntegerType.get()));

  private InMemoryCatalog catalog;
  private CountingCatalog countingCatalog;
  private Table table;
  private IcebergSinkConfig config;
  private final StatisticsHealth health = new StatisticsHealth(HOUR_MS);
  private final ScriptedStep step = new ScriptedStep();
  private final HeldPool pool = new HeldPool();
  private CopyOnWriteRewriteDriver driver;
  private CoordinatorRun run;
  private volatile Runnable afterPendingTables = () -> {};

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
    countingCatalog = new CountingCatalog(catalog);

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn(GROUP_ID);
    when(config.commitThreads()).thenReturn(2);
    when(config.tableConfig(any())).thenReturn(statistics(true));
    when(config.stagingLocation()).thenReturn(null);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1_000_000L);
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(Long.MAX_VALUE);
    when(config.copyOnWritePruningMaxInCardinality()).thenReturn(1000);
    when(config.copyOnWriteCommitRetries()).thenReturn(2);
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(600_000L);
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
    when(config.controlMessageMaxBytes()).thenReturn(1024 * 1024);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(Long.MAX_VALUE);
    when(config.stagingOrphanTtlMs()).thenReturn(86_400_000L);
    when(config.statisticsInUse()).thenReturn(true);
    when(config.statisticsLgNominalEntries()).thenReturn(12);
    when(config.statisticsGapMaxFiles()).thenReturn(1_000);
    when(config.statisticsGapMaxBytes()).thenReturn(Long.MAX_VALUE);
    when(config.statisticsRetainSnapshots()).thenReturn(10);
    when(config.statisticsStaleDeletedRatio()).thenReturn(0.2);
    when(config.statisticsHealthRecheckIntervalMs()).thenReturn(HOUR_MS);
    when(config.configuredTables()).thenReturn(ImmutableSet.of());
  }

  @AfterEach
  public void after() throws IOException {
    if (run != null) {
      run.terminate();
    }

    if (driver != null) {
      driver.stop();
    }

    catalog.close();
    assertThat(step.overlaps.overlaps()).as("two steps of one (table, S) at once").isEmpty();
  }

  @Test
  public void tableOfIcebergTablesIsInLagAtStartupAndGetsItsStepOnHead() {
    lagAtStartup(
        ImmutableMap.of("iceberg.tables", TABLE_IDENTIFIER + "," + NAMESPACE + ".tbl_off"));
  }

  @Test
  public void tableMappedToTopicIsInLagAtStartupAndGetsItsStepOnHead() {
    lagAtStartup(
        ImmutableMap.of(
            "routing.strategy",
            "topic-to-table",
            "iceberg.tables.topic-to-table-mapping",
            SRC_TOPIC + ":" + TABLE_IDENTIFIER + ",src-off:" + NAMESPACE + ".tbl_off"));
  }

  /**
   * The static tables with statistics enter the lag at startup, not verified. The first cycle reads
   * the pointer of the table and runs the quick check on the table it loaded for it; the second
   * gives the table its lag step on the head.
   */
  private void lagAtStartup(Map<String, String> routing) {
    appendRows(row(1L, "a"));
    TableIdentifier off = TableIdentifier.of(NAMESPACE, "tbl_off");
    catalog.createTable(off, SCHEMA, PartitionSpec.unpartitioned());
    when(config.tableConfig(off.toString())).thenReturn(statistics(false));
    Map<String, String> props = Maps.newHashMap(routing);
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", SRC_TOPIC + ",src-off");
    when(config.configuredTables()).thenReturn(new IcebergSinkConfig(props).configuredTables());
    newDriver();

    assertThat(lag().tables()).as("the static tables with statistics").containsExactly(tbl());
    assertThat(lag().isVerified(tbl())).isFalse();

    Map<TableReference, CommitResult> first = cycle();
    assertThat(first.keySet()).containsExactlyInAnyOrder(withoutUuid(tbl()), withoutUuid(off));
    assertCommitted(first);
    assertThat(countingCatalog.loads(tbl()))
        .as("loaded once: for the pointer, then for the quick check")
        .isEqualTo(1);
    assertThat(lag().isVerified(tbl())).isTrue();
    assertThat(step.inputs).as("not admitted before it is verified").isEmpty();

    Map<TableReference, CommitResult> second = cycle();
    assertThat(second.keySet()).containsExactly(withoutUuid(tbl()));
    assertCommitted(second);
    assertThat(step.inputs)
        .singleElement()
        .satisfies(
            input -> {
              assertThat(input.snapshotId()).as("the head").isNull();
              assertThat(input.identifier()).isEqualTo(tbl());
              assertThat(input.ownSnapshotIds()).isEmpty();
              assertThat(input.answers()).isEmpty();
              assertThat(input.table().uuid()).isEqualTo(table.uuid());
            });
    assertThat(lag().tables()).isEmpty();
    assertThat(states().keySet())
        .as("one state per table: the lag step's is under the reference with the UUID")
        .containsExactly(withUuid(tbl()));
    assertThat(cycle()).isEmpty();
  }

  @Test
  public void withStatisticsOffThereIsNoLagAtStartup() {
    when(config.statisticsInUse()).thenReturn(false);
    when(config.tableConfig(any())).thenReturn(statistics(false));
    when(config.configuredTables()).thenReturn(ImmutableSet.of(tbl()));
    newDriver();

    assertThat(lag().tables()).isEmpty();
    assertCommitted(cycle());
    assertThat(cycle()).isEmpty();
    assertThat(step.inputs).isEmpty();
  }

  @Test
  public void lagStepHoldingLockLongerThanCycleLeavesItsQuietTableCommitted() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    HeldLagSteps held = holdLagSteps();
    pool.hold();

    Map<TableReference, CommitResult> first = cycle();
    assertThat(first.keySet()).containsExactly(withoutUuid(tbl()));
    assertCommitted(first);
    assertThat(pool.held()).as("the lag step, on the transition pool").isEqualTo(1);
    assertThat(states().keySet())
        .as("one state per table: under the reference with the UUID, none without")
        .containsExactly(withUuid(tbl()));
    Thread running = pool.startHeld();
    held.awaitStarted();

    int loads = countingCatalog.loads(tbl());
    for (int cycle = 0; cycle < 2; cycle++) {
      Set<TableReference> pending = committer().pendingTables();
      assertThat(pending)
          .as("the lock of the lag step keeps the state out of the cycle")
          .containsExactly(withoutUuid(tbl()));
      assertCommitted(cycleWith(pending));
    }

    assertThat(countingCatalog.loads(tbl()))
        .as("its step in flight, the table is not admitted: answered without a load")
        .isEqualTo(loads);
    assertThat(pool.held()).as("no second lag step").isZero();

    held.release();
    join(running);
    assertThat(step.lagSteps()).hasSize(1);
    assertThat(lag().tables()).isEmpty();
    assertThat(committer().pendingTables()).isEmpty();

    // the lock held by a transition, the flag down: listed and PENDING, as without statistics
    TableDrainState state = states().get(withUuid(tbl()));
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch unlock = new CountDownLatch(1);
    Thread transition =
        new Thread(
            () -> {
              state.lock.lock();
              try {
                locked.countDown();
                await(unlock, "the end of the transition");
              } finally {
                state.lock.unlock();
              }
            });
    transition.start();
    await(locked, "the transition holds the lock");
    Set<TableReference> pending = committer().pendingTables();
    assertThat(pending).containsExactly(withUuid(tbl()));
    assertThat(cycleWith(pending).get(withUuid(tbl())).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    unlock.countDown();
    join(transition);
  }

  @Test
  public void responsesArrivingWhileLagStepHoldsLockWaitAndDrainAfterIt() {
    appendRows(row(1L, "a"), row(2L, "b"));
    newDriver();
    lag().enter(tbl());
    HeldLagSteps held = holdLagSteps();
    pool.hold();
    assertCommitted(cycle());
    Thread running = pool.startHeld();
    held.awaitStarted();

    TableReference written = withUuid(tbl());
    TableCommitRequest responses =
        requestOf(written, rowChangesAt(written, 10L, stagedFiles(written, update(1L, "a1"))));
    Map<TableReference, CommitResult> during = cycle(responses);
    assertThat(during.keySet())
        .as("the reference of the buffer drops the one of the lag")
        .containsExactly(written);
    assertThat(during.get(written).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(during.get(written).consumed()).isEmpty();

    held.release();
    join(running);
    pool.runInline();
    assertThat(driver.commit(responses)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll()).containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b"));
    assertThat(lag().tables()).isEmpty();
  }

  /** The table stays in the lag with a new generation, and the {@code Error} goes on. */
  @Test
  public void lagStepThatThrowsErrorKeepsTableInLagAndFreesSlotAndLock() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    OutOfMemoryError error = new OutOfMemoryError("a substitute that throws");
    step.behavior =
        input -> {
          throw error;
        };
    pool.hold();
    assertCommitted(cycle());
    assertThat(pool.held()).isEqualTo(1);

    assertThatThrownBy(pool::runHeld).isSameAs(error);

    assertThat(step.lagSteps()).hasSize(1);
    assertThat(lag().contains(tbl())).isTrue();
    assertThat(lag().token(tbl())).isGreaterThan(generation);
    assertThat(lagStepLimiter().inFlight(tbl())).as("the slot is free").isFalse();
    TableDrainState state = states().get(withUuid(tbl()));
    assertThat(state.lock.isLocked()).as("the lock is free").isFalse();
    assertThat(state.lagStepHoldsLock).isFalse();
  }

  enum Found {
    UNFINISHED,
    STOPPED
  }

  @ParameterizedTest
  @EnumSource(Found.class)
  public void lagStepFindingTableUnfinishedOrStoppedTakesNoStep(Found found) {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    pool.hold();
    assertCommitted(cycle());
    assertThat(pool.held()).isEqualTo(1);

    TableDrainState state = states().get(withUuid(tbl()));
    state.lock.lock();
    try {
      if (found == Found.UNFINISHED) {
        state.unfinishedDrain = true;
      } else {
        state.stoppedReason = "stopped by the test";
      }
    } finally {
      state.lock.unlock();
    }

    assertThat(lagStepLimiter().inFlight(tbl())).as("the step holds the slot").isTrue();

    pool.runHeld();
    assertThat(step.inputs).isEmpty();
    assertThat(lag().token(tbl())).isEqualTo(generation);
    assertThat(lagStepLimiter().inFlight(tbl())).as("the slot is free").isFalse();
    assertThat(lagStepLimiter().freeSlots()).as("one slot for commitThreads = 2").isEqualTo(1);
    assertThat(committer().pendingTables())
        .as("the state is listed as it is without statistics")
        .containsExactly(withUuid(tbl()), withoutUuid(tbl()));
  }

  /**
   * One slot, three tables in the lag, and the step of one always fails: in a finite number of
   * cycles the other two get their steps and leave the lag, and the slot is free.
   */
  @Test
  public void tableWhoseLagStepAlwaysFailsDoesNotKeepOnlySlotFromOthers() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier failing = tableWithHead("failing");
    TableIdentifier first = tableWithHead("first");
    TableIdentifier second = tableWithHead("second");
    newDriver();
    lag().enter(failing);
    lag().enter(first);
    lag().enter(second);
    step.behavior =
        input ->
            input.identifier().equals(failing)
                ? new Result(Outcome.FAILED, new IllegalStateException("the step always fails"))
                : new Result(Outcome.WRITTEN, null);

    for (int cycles = 0; cycles < 10 && lag().tables().size() > 1; cycles++) {
      assertCommitted(cycle());
    }

    assertThat(lag().tables()).containsExactly(failing);
    assertThat(step.lagSteps())
        .extracting(StatisticsStepInput::identifier)
        .contains(failing, first, second);
    assertThat(lagStepLimiter().freeSlots()).as("the slot is free").isEqualTo(1);
    assertThat(lagStepLimiter().inFlight(failing)).isFalse();
  }

  @Test
  public void lagStepFindingDrainStartedSinceTakesNoStep() {
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    pool.hold();
    assertCommitted(cycle());
    Runnable lagStep = pool.take();

    pool.runInline();
    TableReference written = withUuid(tbl());
    driver.handOutOneSlice(
        requestOf(
            written,
            rowChangesAt(written, 10L, stagedFiles(written, update(1L, "a1"), update(2L, "b2")))));
    lagStep.run();

    assertThat(step.inputs).isEmpty();
    assertThat(lag().token(tbl())).isEqualTo(generation);
    assertThat(committer().pendingTables()).contains(written);
  }

  /**
   * A table of the lag whose drain is active or unfinished is in the cycle under the reference of
   * its state, which drops the one of the lag; it answers as without statistics.
   */
  @Test
  public void tableOfLagWithDrainActiveOrUnfinishedAnswersFromCommitTable() {
    appendRows(row(1L, "a"), row(2L, "b"));
    newDriver();
    TableReference written = withUuid(tbl());
    List<?> slice =
        driver.handOutOneSlice(
            requestOf(
                written,
                rowChangesAt(
                    written, 10L, stagedFiles(written, update(1L, "a1"), update(2L, "b2")))));
    assertThat(slice).isNotEmpty();
    lag().enter(tbl());

    Set<TableReference> pending = committer().pendingTables();
    assertThat(pending).containsExactlyInAnyOrder(written, withoutUuid(tbl()));
    Map<TableReference, CommitResult> active = cycleWith(pending);
    assertThat(active.keySet()).containsExactly(written);
    assertThat(active.get(written).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);

    // unfinished: a drain left without a change set in flight
    TableDrainState state = states().get(written);
    state.lock.lock();
    try {
      state.reset();
      state.unfinishedDrain = true;
    } finally {
      state.lock.unlock();
    }

    pending = committer().pendingTables();
    assertThat(pending).containsExactlyInAnyOrder(written, withoutUuid(tbl()));
    Map<TableReference, CommitResult> unfinished = cycleWith(pending);
    assertThat(unfinished.keySet()).containsExactly(written);
    assertThat(unfinished.get(written).outcome())
        .as("nothing to drain, as without statistics")
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(step.lagSteps()).isEmpty();
    assertThat(pool.held()).isZero();
  }

  @Test
  public void lagRemovedBetweenPendingTablesAndCommitAnswersWithoutLoadOrState() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());

    Set<TableReference> pending = committer().pendingTables();
    assertThat(pending).containsExactly(withoutUuid(tbl()));
    lag().removeIfToken(tbl(), lag().token(tbl()));
    assertCommitted(cycleWith(pending));

    assertThat(countingCatalog.loads(tbl())).isZero();
    assertThat(states()).isEmpty();
  }

  @Test
  public void lagStepEndingBetweenPendingTablesAndCommitLeavesNoStateWithoutUuid() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    HeldLagSteps held = holdLagSteps();
    pool.hold();
    assertCommitted(cycle());
    Thread running = pool.startHeld();
    held.awaitStarted();

    Set<TableReference> pending = committer().pendingTables();
    assertThat(pending).containsExactly(withoutUuid(tbl()));
    held.release();
    join(running);
    assertThat(lag().contains(tbl())).as("the lag step removed the lag").isFalse();

    int loads = countingCatalog.loads(tbl());
    assertCommitted(cycleWith(pending));
    assertThat(countingCatalog.loads(tbl())).isEqualTo(loads);
    assertThat(states().keySet()).containsExactly(withUuid(tbl()));
  }

  /**
   * Through the {@link Coordinator}: the lag step holds the lock across cycles, the quiet table of
   * the lag commits, and {@code CommitComplete} claims the timestamp.
   */
  @Test
  public void cycleDuringLagStepClaimsValidThroughThroughCoordinator() {
    appendRows(row(1L, "a"));
    HeldLagSteps held = holdLagSteps();
    pool.hold();
    run = new CoordinatorRun();
    lag().enter(tbl());

    CommitComplete first = run.cycle();
    assertThat(first.validThroughTs()).as("the step is only handed to the pool").isEqualTo(run.ts);
    assertThat(pool.held()).isEqualTo(1);
    Thread running = pool.startHeld();
    held.awaitStarted();

    int loads = countingCatalog.loads(tbl());
    for (int cycle = 0; cycle < 2; cycle++) {
      CommitComplete during = run.cycle();
      assertThat(during.validThroughTs())
          .as("the lock of the step keeps the state out of the cycle: the table is committed")
          .isEqualTo(run.ts);
    }

    assertThat(countingCatalog.loads(tbl())).as("not admitted while in flight").isEqualTo(loads);
    assertThat(pool.held()).as("no second lag step").isZero();

    held.release();
    join(running);
    assertThat(step.lagSteps()).hasSize(1);
    assertThat(run.cycle().validThroughTs()).isEqualTo(run.ts);
  }

  /**
   * Through the {@link Coordinator}: the lag step ends between {@code pendingTables()} and {@code
   * commit()} of the cycle; the reference without a UUID it listed is answered without a load or a
   * state, and the cycle claims the timestamp.
   */
  @Test
  public void lagStepEndingInsideCycleClaimsValidThroughThroughCoordinator() {
    appendRows(row(1L, "a"));
    HeldLagSteps held = holdLagSteps();
    pool.hold();
    run = new CoordinatorRun();
    lag().enter(tbl());
    assertThat(run.cycle().validThroughTs()).isEqualTo(run.ts);
    Thread running = pool.startHeld();
    held.awaitStarted();

    afterPendingTables =
        () -> {
          held.release();
          join(running);
        };
    int loads = countingCatalog.loads(tbl());
    CommitComplete ending = run.cycle();

    assertThat(lag().contains(tbl())).as("the lag step removed the lag").isFalse();
    assertThat(ending.validThroughTs()).isEqualTo(run.ts);
    assertThat(countingCatalog.loads(tbl())).isEqualTo(loads);
    assertThat(states().keySet()).containsExactly(withUuid(tbl()));
  }

  @Test
  public void referenceWithoutUuidOutOfLagAndOfUnresumedTablesLeavesStates() {
    appendRows(row(1L, "a"));
    newDriver();

    CommitResult result = committer().commit(withoutResponses(withoutUuid(tbl())));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(result.consumed()).isEmpty();
    assertThat(countingCatalog.loads(tbl())).isZero();
    assertThat(states()).isEmpty();
  }

  /**
   * A table whose {@code uuid()} is {@code null} has its state and its lag under one reference;
   * once it has a state, the request of the lag goes to {@code commitTable}.
   */
  @Test
  public void tableWithoutUuidWithStateAnswersFromCommitTable() {
    appendRows(row(1L, "a"), row(2L, "b"));
    newDriver();
    // what a worker names a table without a UUID by
    TableReference unnamed = withoutUuid(tbl());
    List<RewriteAssigned> slice =
        driver.handOutOneSlice(
            requestOf(unnamed, rowChangesAt(unnamed, 10L, stagedFiles(unnamed, update(1L, "a1")))));
    lag().enter(tbl());

    Set<TableReference> pending = committer().pendingTables();
    assertThat(pending).containsExactly(unnamed);
    Map<TableReference, CommitResult> active = cycleWith(pending);
    assertThat(active.get(unnamed).outcome())
        .as("the drain is active")
        .isEqualTo(TableCommitter.Outcome.PENDING);

    driver.answerAssignments(slice);
    assertThat(readAll()).containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b"));
    step.inputs.clear();
    lag().enter(tbl());
    pending = committer().pendingTables();
    assertThat(pending).as("the table of the lag").containsExactly(unnamed);
    Map<TableReference, CommitResult> idle = cycleWith(pending);
    assertThat(idle.get(unnamed).outcome())
        .as("nothing to drain, as without statistics")
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(step.inputs).as("no lag step").isEmpty();
    assertThat(pool.held()).isZero();
    assertThat(lag().contains(tbl())).isTrue();
  }

  /** Through {@code commit()} such a request never reaches it. */
  @Test
  public void commitTableRefusesRequestWithoutResponsesUnderReferenceWithoutState() {
    appendRows(row(1L, "a"));
    newDriver();

    assertThatThrownBy(() -> commitTable(withoutResponses(withUuid(tbl()))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no responses and no copy-on-write state");
    assertThat(states()).isEmpty();
  }

  /**
   * A static table in the lag at startup whose head is an intermediate slice resumes its drain
   * through {@code resumeAfterRestart}, which makes the state {@code commitTable} requires.
   */
  @Test
  public void restartResumesDrainOfStaticTableInLagFromItsPointer() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(tbl()));
    newDriver();
    TableReference written = withUuid(tbl());
    driver.commitOneSlice(
        requestOf(
            written,
            rowChangesAt(
                written,
                10L,
                stagedFiles(written, update(1L, "a1"), update(2L, "b2"), update(3L, "c3")))));

    // a new coordinator: nothing is buffered
    newDriver();
    assertThat(lag().contains(tbl())).as("in the lag at startup").isTrue();
    assertThat(committer().pendingTables()).containsExactly(withoutUuid(tbl()));

    // the first cycle has no tasks: the drain waits for the next one
    for (TableReference visited : committer().pendingTables()) {
      assertThat(
              driver.commitWithNoTasks(
                  new TableCommitRequest(
                      visited,
                      ImmutableList.of(),
                      ImmutableMap.of(),
                      UUID.randomUUID(),
                      OffsetDateTime.now())))
          .isEqualTo(TableCommitter.Outcome.PENDING);
    }
    assertThat(unresumedTables()).containsExactly(tbl());
    assertThat(states().keySet()).containsExactly(written);

    for (TableReference visited : committer().pendingTables()) {
      assertThat(driver.commit(withoutResponses(visited)))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
    }
    assertThat(readAll())
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"));
    assertThat(unresumedTables()).isEmpty();
    assertThat(step.lagSteps()).as("the lag check took no part").isEmpty();
    assertThat(step.inputs).as("the step of the final snapshot").hasSize(1);
    assertThat(lag().tables()).isEmpty();
    assertThat(committer().pendingTables()).isEmpty();
  }

  @Test
  public void tableRecreatedWhileItsLagStepWaitsForPermitGetsItsStepOnNewHeadNextCycle()
      throws InterruptedException {
    // three slots, two permits
    when(config.commitThreads()).thenReturn(6);
    TableIdentifier first = tableWithHead("first");
    TableIdentifier second = tableWithHead("second");
    appendRows(row(1L, "a"));
    UUID recreatedFrom = table.uuid();
    newDriver();
    lag().enter(first);
    lag().enter(second);
    lag().enter(tbl());
    CountDownLatch bothRun = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    step.behavior =
        input -> {
          if (!input.identifier().equals(tbl())) {
            bothRun.countDown();
            await(release, "the release of the permits");
          }

          return new Result(Outcome.WRITTEN, null);
        };
    pool.hold();
    assertCommitted(cycle());
    assertThat(pool.held()).isEqualTo(3);

    List<Thread> threads = Lists.newArrayList(pool.startHeld(), pool.startHeld());
    await(bothRun, "two lag steps hold the permits");
    Thread waiting = pool.startHeld();
    threads.add(waiting);
    awaitWaiting(waiting);
    catalog.dropTable(tbl(), false);
    table = catalog.createTable(tbl(), SCHEMA, PartitionSpec.unpartitioned());
    appendRows(row(2L, "b"));
    release.countDown();
    threads.forEach(TestCopyOnWriteStatisticsLag::join);

    assertThat(step.inputs)
        .extracting(StatisticsStepInput::identifier)
        .containsExactlyInAnyOrder(first, second);
    assertThat(states().keySet())
        .as("no state under the reference of the table recreated")
        .doesNotContain(TableReference.of(catalog.name(), tbl(), recreatedFrom));
    assertThat(lag().contains(tbl())).isTrue();

    pool.runInline();
    assertCommitted(cycle());
    assertThat(step.inputs).hasSize(3);
    StatisticsStepInput onTheNewHead = step.inputs.get(2);
    assertThat(onTheNewHead.identifier()).isEqualTo(tbl());
    assertThat(onTheNewHead.table().uuid()).isEqualTo(table.uuid());
    assertThat(states().keySet()).contains(withUuid(tbl()));
    assertThat(lag().tables()).isEmpty();
  }

  /**
   * The state a lag step holds is no longer the one registered: while the step waits on the pool, a
   * cycle with responses finds the table gone, and {@code startDrain} takes the state out; the
   * responses of the next cycle make a new one under the same reference and start a drain on it.
   * The step must not run on the old state: its lock is not the one of the new state.
   */
  @Test
  public void lagStepOnStateNoLongerRegisteredTakesNoStepAndLeavesLag() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    pool.hold();
    assertCommitted(cycle());
    assertThat(pool.held()).isEqualTo(1);
    TableReference reference = withUuid(tbl());
    TableDrainState old = states().get(reference);
    assertThat(old).isNotNull();
    long generation = lag().token(tbl());

    countingCatalog.failLoading(tbl(), new NoSuchTableException("the table is gone for a cycle"));
    TableCommitRequest whileGone =
        requestOf(
            reference, rowChangesAt(reference, 10L, stagedFiles(reference, update(1L, "a1"))));
    assertThat(cycle(whileGone).get(reference).outcome())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(states()).as("startDrain took the state out").doesNotContainKey(reference);
    countingCatalog.loadNormally(tbl());
    TableCommitRequest responses =
        requestOf(
            reference, rowChangesAt(reference, 20L, stagedFiles(reference, update(1L, "a2"))));
    assertThat(cycle(responses).get(reference).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    TableDrainState replacement = states().get(reference);
    assertThat(replacement).as("the responses made a new state").isNotNull().isNotSameAs(old);

    pool.runHeld();

    assertThat(step.inputs).as("no step on the state that is not registered").isEmpty();
    assertThat(lag().contains(tbl())).as("the lag is as it was").isTrue();
    assertThat(lag().token(tbl())).isEqualTo(generation);
    assertThat(states()).containsEntry(reference, replacement);
    assertThat(old.lagStepHoldsLock).isFalse();
    assertThat(old.lock.isLocked()).isFalse();
  }

  /**
   * A lag step removes the state of a table created again between what {@code commit()} reads of
   * the states and what {@code commitTable} makes of it: the request without responses, listed by
   * {@code pendingTables()} while the step held the lock without the flag, goes to the lag check
   * instead of failing the table.
   */
  @Test
  public void stateRemovedByLagStepWhileRequestWithoutResponsesIsMadeGoesToLagCheck() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    pool.hold();
    assertCommitted(cycle());
    TableReference reference = withUuid(tbl());
    assertThat(states()).containsKey(reference);

    // only the committer's map is replaced: the staging cleanup source keeps the map it was given,
    // which loses the state too, but sees nothing the committer adds to the copy afterwards
    RemovedAfterRead racing = new RemovedAfterRead(states());
    setField(field(committer(), "states"), "states", racing);
    racing.arm(reference);

    CommitResult result = committer().commit(withoutResponses(reference));

    assertThat(racing.removed()).as("the state was removed after it was read").isTrue();
    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(result.consumed()).isEmpty();
    pool.runHeld();
  }

  @Test
  public void tableNamedTwiceGetsOneLagStepAndItsSecondNameIsStopped() {
    // two slots
    when(config.commitThreads()).thenReturn(4);
    appendRows(row(1L, "a"));
    // a case-insensitive catalog: iceberg.tables=db.tbl,DB.TBL names one table twice
    TableIdentifier otherCase = TableIdentifier.of(Namespace.of("DB"), "TBL");
    countingCatalog.alias(otherCase, tbl());
    when(config.configuredTables()).thenReturn(ImmutableSet.of(tbl(), otherCase));
    newDriver();
    assertThat(lag().tables()).containsExactlyInAnyOrder(tbl(), otherCase);

    assertCommitted(cycle());
    assertCommitted(cycle());

    assertThat(step.lagSteps()).as("one lag step for the one table").hasSize(1);
    List<TableDrainState> stopped =
        states().values().stream().filter(TableDrainState::stopped).collect(Collectors.toList());
    assertThat(stopped)
        .singleElement()
        .satisfies(
            state ->
                assertThat(state.stoppedReason)
                    .as("the message of commitTable")
                    .contains("under two names"));
    TableIdentifier stoppedName = stopped.get(0).tableReference.identifier();
    assertThat(step.lagSteps().get(0).identifier()).isNotEqualTo(stoppedName);
    assertThat(lag().tables()).containsExactly(stoppedName);
  }

  @Test
  public void lagStepWaitsForStepOfTailAndFindsRecordNextCycle() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    step.behavior =
        input ->
            new Result(input.snapshotId() == null ? Outcome.NOT_NEEDED : Outcome.WRITTEN, null);
    TableReference written = withUuid(tbl());
    pool.hold();

    // merge-on-read responses buffered before the switch to copy-on-write: the tail
    TableCommitRequest tail = requestOf(written, dataWrittenAt(written, 5L, row(2L, "b")));
    Map<TableReference, CommitResult> first = cycle(tail);
    assertThat(first.keySet()).containsExactly(written);
    assertThat(first.get(written).outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    Runnable stepOfTheTail = pool.take();
    long tailSnapshotId = catalog.loadTable(tbl()).currentSnapshot().snapshotId();

    Map<TableReference, CommitResult> second = cycle();
    assertThat(second.keySet()).containsExactly(withoutUuid(tbl()));
    assertCommitted(second);
    pool.take().run();
    assertThat(step.inputs).as("no lag step while the step of the tail waits").isEmpty();
    assertThat(lag().contains(tbl())).isTrue();

    stepOfTheTail.run();
    pool.runInline();
    assertCommitted(cycle());
    assertThat(step.inputs)
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(tailSnapshotId, (Long) null);
    assertThat(lag().tables()).isEmpty();
  }

  @Test
  public void staticTableInLagAndItsResponsesInOneCycleGetOneCommitAndOneStep() {
    appendRows(row(1L, "a"));
    when(config.configuredTables()).thenReturn(ImmutableSet.of(tbl()));
    newDriver();
    assertThat(lag().contains(tbl())).isTrue();
    TableReference written = withUuid(tbl());
    TableCommitRequest responses =
        requestOf(written, rowChangesAt(written, 10L, stagedFiles(written, update(1L, "a1"))));

    Set<TableReference> pending = committer().pendingTables();
    assertThat(pending).containsExactly(withoutUuid(tbl()));
    assertThat(cycleTables(ImmutableSet.of(written), pending)).containsExactly(written);
    assertThat(driver.commit(responses)).isEqualTo(TableCommitter.Outcome.COMMITTED);

    Snapshot finalSnapshot = catalog.loadTable(tbl()).currentSnapshot();
    assertThat(step.inputs)
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(finalSnapshot.snapshotId());
    assertThat(lag().tables()).isEmpty();
    assertThat(committer().pendingTables()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Outcome.class)
  public void lagStepActsOnItsOutcome(Outcome outcome) {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    step.behavior =
        input ->
            new Result(
                outcome,
                outcome == Outcome.FAILED ? new IllegalStateException("the step fails") : null);

    assertCommitted(cycle());

    assertThat(step.lagSteps()).hasSize(1);
    switch (outcome) {
      case WRITTEN:
      case NOT_NEEDED:
      case FINAL:
      case TABLE_GONE:
        assertThat(lag().contains(tbl())).isFalse();
        break;
      case FAILED:
      case SNAPSHOT_GONE:
        assertThat(lag().token(tbl())).as("entered again").isGreaterThan(generation);
        break;
      default:
        assertThat(lag().token(tbl())).isEqualTo(generation);
        break;
    }

    if (outcome == Outcome.FAILED) {
      assertThat(lag().recordFailure(tbl()))
          .as("the failure is counted: the next one is not the first in a row")
          .isFalse();
    }
  }

  @Test
  public void tableDroppedBeforeItsLagStepLeavesLagAndItsHealth() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    health.update(tbl(), flags -> new Flags(false, true, false));
    pool.hold();
    assertCommitted(cycle());

    catalog.dropTable(tbl(), false);
    pool.runHeld();

    assertThat(step.inputs).isEmpty();
    assertThat(lag().contains(tbl())).isFalse();
    assertThat(health.version(tbl())).as("no record of health").isZero();
  }

  @Test
  public void tableGoneWhileCommitterStopsKeepsLagAndItsHealth() {
    // the same event, a table not found with the stop flag up, is a stop here as it is in the
    // queue of merge-on-read: the health and the lag stay
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    health.update(tbl(), flags -> new Flags(false, true, false));
    pool.hold();
    assertCommitted(cycle());
    long version = health.version(tbl());

    countingCatalog.failLoading(tbl(), new NoSuchTableException("Table does not exist: %s", tbl()));
    // the flag rises while the table is being loaded
    countingCatalog.beforeLoading(driver::stop);
    pool.runHeld();

    assertThat(step.inputs).isEmpty();
    assertThat(version).isNotZero();
    assertThat(health.version(tbl())).as("the record of health stays").isEqualTo(version);
    assertThat(lag().token(tbl())).as("the lag stays as it was").isEqualTo(generation);
  }

  @Test
  public void lagStepWaitingForLockOfTableTakesNoStepOnceCommitterStopsAndReadsNothing()
      throws InterruptedException {
    // lock() is not interruptible: the step that got the lock after the stop reads neither the
    // catalog nor the storage
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    pool.hold();
    assertCommitted(cycle());
    TableDrainState state = states().get(withUuid(tbl()));
    int loads = countingCatalog.loads(tbl());

    state.lock.lock();
    Thread waiting;
    try {
      waiting = pool.startHeld();
      StatisticsAssertions.await("the step waits for the lock", state.lock::hasQueuedThreads);
      driver.stop();
    } finally {
      state.lock.unlock();
    }

    waiting.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));

    assertThat(waiting.isAlive()).isFalse();
    assertThat(countingCatalog.loads(tbl())).as("no read after the stop").isEqualTo(loads);
    assertThat(step.inputs).isEmpty();
    assertThat(lag().token(tbl())).isEqualTo(generation);
    assertThat(lagStepLimiter().inFlight(tbl())).as("the slot is free").isFalse();
  }

  @Test
  public void tableThatFailsToLoadForItsLagStepStaysInLag() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    long generation = lag().token(tbl());
    pool.hold();
    assertCommitted(cycle());

    countingCatalog.failLoading(tbl(), new IllegalStateException("the catalog fails"));
    pool.runHeld();

    assertThat(step.inputs).isEmpty();
    assertThat(lag().token(tbl())).as("entered again, as FAILED").isGreaterThan(generation);
    assertThat(lag().recordFailure(tbl())).isFalse();
  }

  @Test
  public void tableThatFailsToLoadInLagCheckStaysInLagAndCommitted() {
    appendRows(row(1L, "a"));
    newDriver();
    // the first visit of the table (resumeAfterRestart) is over
    assertCommitted(cycle());
    lag().enter(tbl());
    countingCatalog.failLoading(tbl(), new IllegalStateException("the catalog fails"));
    int loads = countingCatalog.loads(tbl());

    assertCommittedWithLagOf(cycle());

    assertThat(countingCatalog.loads(tbl())).as("the branch tried to load it").isGreaterThan(loads);
    assertThat(step.inputs).as("no step without the table").isEmpty();
    assertThat(lag().contains(tbl())).as("the lag stays").isTrue();
    assertThat(committer().pendingTables()).as("the table is still listed").isNotEmpty();
  }

  @Test
  public void lagStepThatThrowsEveryTimeLeavesEveryCycleCommitted() {
    appendRows(row(1L, "a"));
    newDriver();
    lag().enter(tbl());
    step.behavior =
        input -> {
          throw new IllegalStateException("the step throws");
        };

    for (int i = 0; i < 3; i++) {
      assertCommittedWithLagOf(cycle());
    }

    assertThat(step.lagSteps()).as("the step was set").isNotEmpty();
    assertThat(lag().contains(tbl())).as("the lag stays").isTrue();
  }

  private void newDriver() {
    if (driver != null) {
      driver.stop();
    }

    driver =
        new CopyOnWriteRewriteDriver(
            countingCatalog, config, MetadataEvents.NOOP, event -> {}, 1, health, step, pool);
  }

  private CopyOnWriteTableCommitter committer() {
    return driver.committer();
  }

  /**
   * One cycle as {@code Coordinator.doCommit} runs it: {@code pendingTables()}, the tables of the
   * cycle, and {@code commit()} of each with its request of {@code buffered}, or one without
   * responses.
   */
  private Map<TableReference, CommitResult> cycle(TableCommitRequest... buffered) {
    return cycleWith(committer().pendingTables(), buffered);
  }

  private Map<TableReference, CommitResult> cycleWith(
      Set<TableReference> pending, TableCommitRequest... buffered) {
    Map<TableReference, TableCommitRequest> requests = Maps.newLinkedHashMap();
    for (TableCommitRequest request : buffered) {
      requests.put(request.tableReference(), request);
    }

    Map<TableReference, CommitResult> results = Maps.newLinkedHashMap();
    for (TableReference reference : cycleTables(requests.keySet(), pending)) {
      TableCommitRequest request =
          requests.containsKey(reference) ? requests.get(reference) : withoutResponses(reference);
      results.put(reference, committer().commit(request));
    }

    return results;
  }

  /** Every table of the cycle answers {@code COMMITTED} with nothing consumed. */
  private static void assertCommitted(Map<TableReference, CommitResult> results) {
    assertThat(results)
        .allSatisfy(
            (reference, result) -> {
              assertThat(result.outcome())
                  .as("the answer for %s", reference)
                  .isEqualTo(TableCommitter.Outcome.COMMITTED);
              assertThat(result.consumed()).isEmpty();
            });
  }

  /**
   * As {@link #assertCommitted}, and the cycle visits the table of the test under the reference of
   * the lag: an empty cycle would pass the other check vacuously.
   */
  private void assertCommittedWithLagOf(Map<TableReference, CommitResult> results) {
    assertThat(results).as("the cycle visits the table in the lag").containsKey(withoutUuid(tbl()));
    assertCommitted(results);
  }

  // the members below are reached by reflection, so the tests also compile against a committer
  // and a coordinator without them
  @SuppressWarnings("unchecked")
  private static Set<TableReference> cycleTables(
      Set<TableReference> buffered, Set<TableReference> pending) {
    try {
      Method method = Coordinator.class.getDeclaredMethod("cycleTables", Set.class, Set.class);
      method.setAccessible(true);
      return (Set<TableReference>) method.invoke(null, buffered, pending);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private CommitResult commitTable(TableCommitRequest request) {
    try {
      Method method =
          CopyOnWriteTableCommitter.class.getDeclaredMethod(
              "commitTable", TableCommitRequest.class);
      method.setAccessible(true);
      return (CommitResult) method.invoke(committer(), request);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }

      throw new IllegalStateException(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private StatisticsLag lag() {
    return (StatisticsLag) field(committer(), "statisticsLag");
  }

  @SuppressWarnings("unchecked")
  private Map<TableReference, TableDrainState> states() {
    return (Map<TableReference, TableDrainState>) field(field(committer(), "states"), "states");
  }

  private StatisticsLagStepLimiter lagStepLimiter() {
    return (StatisticsLagStepLimiter) field(field(committer(), "lagStepRunner"), "lagStepLimiter");
  }

  @SuppressWarnings("unchecked")
  private Set<TableIdentifier> unresumedTables() {
    return (Set<TableIdentifier>) field(field(committer(), "states"), "unresumedTables");
  }

  private static Object field(Object target, String name) {
    try {
      Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void setField(Object target, String name, Object value) {
    try {
      Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private HeldLagSteps holdLagSteps() {
    HeldLagSteps held = new HeldLagSteps();
    step.behavior =
        input -> {
          if (input.snapshotId() == null) {
            held.started.countDown();
            await(held.release, "the release of the lag step");
          }

          return new Result(Outcome.WRITTEN, null);
        };
    return held;
  }

  /** A lag step that waits in its step until released. */
  private static final class HeldLagSteps {
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    void awaitStarted() {
      await(started, "the lag step runs");
    }

    void release() {
      release.countDown();
    }
  }

  private static TableSinkConfig statistics(boolean enabled) {
    return new TableSinkConfig(
        null, ImmutableList.of(), ImmutableList.of(), null, enabled, false, null);
  }

  private static TableIdentifier tbl() {
    return TABLE_IDENTIFIER;
  }

  private TableReference withoutUuid(TableIdentifier id) {
    return TableReference.of(catalog.name(), id);
  }

  private TableReference withUuid(TableIdentifier id) {
    return TableReference.of(catalog.name(), id, catalog.loadTable(id).uuid());
  }

  private static TableCommitRequest withoutResponses(TableReference reference) {
    return new TableCommitRequest(
        reference,
        ImmutableList.of(),
        ImmutableMap.of(),
        UUID.randomUUID(),
        OffsetDateTime.now(),
        ACTIVE_TASKS);
  }

  private static TableCommitRequest requestOf(TableReference reference, Envelope envelope) {
    return new TableCommitRequest(
        reference,
        ImmutableList.of(envelope),
        ImmutableMap.of(0, envelope.offset() + 1),
        UUID.randomUUID(),
        OffsetDateTime.now(),
        ACTIVE_TASKS);
  }

  private static Envelope rowChangesAt(
      TableReference reference, long offset, List<StagedChangeFile> files) {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), reference, "task-0", List.of(SRC_TOPIC), files, null);
    return new Envelope(new Event(GROUP_ID, payload), 0, offset);
  }

  private Envelope dataWrittenAt(TableReference reference, long offset, Record... rows) {
    DataWritten payload =
        new DataWritten(
            table.spec().partitionType(),
            UUID.randomUUID(),
            reference,
            writeRows(rows),
            List.of(),
            List.of(SRC_TOPIC),
            null);
    return new Envelope(new Event(GROUP_ID, payload), 0, offset);
  }

  private List<StagedChangeFile> stagedFiles(TableReference reference, Record... changes) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, reference, ID_FIELDS, null, GROUP_ID, "task-0");
    long offset = 0;
    for (Record change : changes) {
      int op = (int) change.getField("op");
      writer.write(writer.stagedRow(tableRow(change), op, SRC_TOPIC, 0, offset++));
    }

    return writer.complete();
  }

  private static Record tableRow(Record change) {
    GenericRecord tableRow = GenericRecord.create(SCHEMA);
    tableRow.setField("id", change.getField("id"));
    tableRow.setField("data", change.getField("data"));
    return tableRow;
  }

  private static Record update(long id, String data) {
    GenericRecord change = GenericRecord.create(CHANGE);
    change.setField("id", id);
    change.setField("data", data);
    change.setField("op", StagedChangeSchema.OP_UPDATE);
    return change;
  }

  private TableIdentifier tableWithHead(String name) {
    TableIdentifier id = TableIdentifier.of(NAMESPACE, name);
    Table created = catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
    AppendFiles append = created.newAppend();
    writeRows(created, row(1L, name)).forEach(append::appendFile);
    append.commit();
    return id;
  }

  private void appendRows(Record... rows) {
    table.refresh();
    AppendFiles append = table.newAppend();
    writeRows(rows).forEach(append::appendFile);
    append.commit();
  }

  private List<DataFile> writeRows(Record... rows) {
    return writeRows(table, rows);
  }

  private static List<DataFile> writeRows(Table target, Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            new GenericFileWriterFactory.Builder(target).dataSchema(target.schema()).build(),
            OutputFileFactory.builderFor(target, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            target.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(target.spec(), target.schema());
        key.partition(row);
        writer.write(row, target.spec(), key);
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return writer.result().dataFiles();
  }

  private static Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private List<org.assertj.core.groups.Tuple> readAll() {
    table.refresh();
    List<org.assertj.core.groups.Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(tuple(record.get(0, Object.class), record.get(1, Object.class)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return rows;
  }

  private static void await(CountDownLatch latch, String what) {
    try {
      assertThat(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS)).as(what).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(what, e);
    }
  }

  /** Waits until {@code thread} waits with a timeout: the lag step for a permit. */
  private static void awaitWaiting(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
    while (thread.getState() != Thread.State.TIMED_WAITING) {
      assertThat(System.nanoTime()).as("the lag step waits for a permit").isLessThan(deadline);
      Thread.sleep(5);
    }
  }

  private static void join(Thread thread) {
    try {
      thread.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }

    assertThat(thread.isAlive()).as("%s is over", thread.getName()).isFalse();
  }

  /**
   * A {@link Coordinator} over the committer of the test, one cycle of the control topic at a time:
   * {@code StartCommit}, the {@code DataComplete} of the one task, the commit, and its {@code
   * CommitComplete}.
   */
  private final class CoordinatorRun {
    private final MockProducer<String, byte[]> producer =
        new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
    private final MockConsumer<String, byte[]> consumer =
        new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final Coordinator coordinator;
    private int cycles = 0;
    private long offset = 0L;
    private OffsetDateTime ts;

    CoordinatorRun() {
      when(config.commitIntervalMs()).thenReturn(0);
      when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
      when(config.commitThreads()).thenReturn(1);
      when(config.isCopyOnWriteMode()).thenReturn(true);
      when(config.rowLevelMode()).thenReturn(RowLevelOperationMode.COPY_ON_WRITE);
      producer.initTransactions();
      KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);
      when(clientFactory.createProducer(any())).thenReturn(producer);
      when(clientFactory.createConsumer(any())).thenReturn(consumer);
      when(clientFactory.createAdmin()).thenReturn(mock(Admin.class));
      coordinator =
          new Coordinator(
              config,
              ImmutableList.of(),
              clientFactory,
              mock(SinkTaskContext.class),
              step,
              (send, sendInOneTransaction, transitionPool, statisticsHealth, stepOf) -> {
                newDriver();
                return new AfterPendingTables(committer());
              });
      TopicPartition controlPartition = new TopicPartition(CTL_TOPIC, 0);
      coordinator.start();
      consumer.rebalance(ImmutableList.of(controlPartition));
      consumer.updateBeginningOffsets(ImmutableMap.of(controlPartition, 0L));
    }

    /** One commit of the coordinator; {@link #ts} is the timestamp its task reported. */
    CommitComplete cycle() {
      coordinator.process();
      List<StartCommit> started = controlEventsOf(StartCommit.class);
      assertThat(started).as("StartCommit #%s", cycles).hasSizeGreaterThan(cycles);
      UUID commitId = started.get(cycles++).commitId();
      ts = EventTestUtil.now();
      DataComplete complete =
          new DataComplete(
              commitId, ImmutableList.of(new TopicPartitionOffset(SRC_TOPIC, 0, 1L, ts)), "task-0");
      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC, 0, offset++, "key", AvroUtil.encode(new Event(GROUP_ID, complete))));
      coordinator.process();

      List<CommitComplete> completed =
          controlEventsOf(CommitComplete.class).stream()
              .filter(event -> event.commitId().equals(commitId))
              .collect(Collectors.toList());
      assertThat(completed).as("CommitComplete of %s", commitId).hasSize(1);
      return completed.get(0);
    }

    void terminate() {
      coordinator.terminate();
    }

    private <T extends Payload> List<T> controlEventsOf(Class<T> type) {
      return producer.history().stream()
          .map(record -> AvroUtil.decode(record.value()).payload())
          .filter(type::isInstance)
          .map(type::cast)
          .collect(Collectors.toList());
    }
  }

  /** The committer of the test with a hook right after {@code pendingTables()}. */
  private final class AfterPendingTables implements TableCommitter {
    private final TableCommitter delegate;

    private AfterPendingTables(TableCommitter delegate) {
      this.delegate = delegate;
    }

    @Override
    public CommitResult commit(TableCommitRequest request) {
      return delegate.commit(request);
    }

    @Override
    public boolean receive(Envelope envelope) {
      return delegate.receive(envelope);
    }

    @Override
    public void process(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {
      delegate.process(nowMs, bufferedResponses);
    }

    @Override
    public StagingCleanupSource stagingCleanupSource() {
      return delegate.stagingCleanupSource();
    }

    @Override
    public Set<TableReference> pendingTables() {
      Set<TableReference> pending = delegate.pendingTables();
      afterPendingTables.run();
      return pending;
    }

    @Override
    public Collection<Envelope> replayAnchors() {
      return delegate.replayAnchors();
    }

    @Override
    public void stop() {
      delegate.stop();
    }
  }

  /**
   * A copy of the states, one of which is taken out right after the first read of it, from the copy
   * and from the map copied alike.
   */
  private static final class RemovedAfterRead
      extends ConcurrentHashMap<TableReference, TableDrainState> {
    private final Map<TableReference, TableDrainState> original;
    private volatile TableReference armed;
    private volatile boolean removed;

    RemovedAfterRead(Map<TableReference, TableDrainState> states) {
      this.original = states;
      putAll(states);
    }

    void arm(TableReference reference) {
      this.armed = reference;
    }

    boolean removed() {
      return removed;
    }

    @Override
    public TableDrainState get(Object key) {
      return takeOut(key, super.get(key));
    }

    @Override
    public boolean containsKey(Object key) {
      return takeOut(key, super.get(key)) != null;
    }

    private TableDrainState takeOut(Object key, TableDrainState found) {
      if (found != null && key.equals(armed)) {
        armed = null;
        original.remove(key, found);
        removed = remove(key, found);
      }

      return found;
    }
  }

  /** The step of the tests: records its inputs and answers what {@link #behavior} says. */
  private static final class ScriptedStep extends StatisticsStep {
    private final List<StatisticsStepInput> inputs = Lists.newCopyOnWriteArrayList();
    private final StepOverlaps overlaps = new StepOverlaps();
    private volatile Function<StatisticsStepInput, Result> behavior =
        input -> new Result(Outcome.WRITTEN, null);

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      return overlaps.track(input, () -> behavior.apply(input));
    }

    /** The inputs of the lag steps: S is the head. */
    List<StatisticsStepInput> lagSteps() {
      return inputs.stream()
          .filter(input -> input.snapshotId() == null)
          .collect(Collectors.toList());
    }
  }

  /**
   * The catalog of the committer: counts the loads of each table, can fail them, and can answer to
   * a second name of a table, as a case-insensitive catalog does.
   */
  private static final class CountingCatalog implements Catalog {
    private final Catalog delegate;
    private final Map<TableIdentifier, AtomicInteger> loads = Maps.newConcurrentMap();
    private final Map<TableIdentifier, RuntimeException> failures = Maps.newConcurrentMap();
    private final Map<TableIdentifier, TableIdentifier> aliases = Maps.newConcurrentMap();
    private volatile Runnable beforeLoading = () -> {};

    private CountingCatalog(Catalog delegate) {
      this.delegate = delegate;
    }

    int loads(TableIdentifier identifier) {
      AtomicInteger count = loads.get(identifier);
      return count == null ? 0 : count.get();
    }

    /** Runs at the start of every load, before it fails or answers. */
    void beforeLoading(Runnable action) {
      this.beforeLoading = action;
    }

    void failLoading(TableIdentifier identifier, RuntimeException failure) {
      failures.put(identifier, failure);
    }

    void loadNormally(TableIdentifier identifier) {
      failures.remove(identifier);
    }

    void alias(TableIdentifier alias, TableIdentifier identifier) {
      aliases.put(alias, identifier);
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
      beforeLoading.run();
      RuntimeException failure = failures.get(identifier);
      if (failure != null) {
        throw failure;
      }

      return delegate.loadTable(aliases.getOrDefault(identifier, identifier));
    }
  }
}
