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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.ref.WeakReference;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.channel.StatisticsStepQueue.CleanupTable;
import org.apache.iceberg.connect.channel.StatisticsStepQueue.OutcomeAction;
import org.apache.iceberg.connect.data.StatisticsAnswer;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/** The merge-on-read queue of statistics steps, over a substituted step. */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class TestStatisticsStepQueue {
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  private static final TableIdentifier OTHER = TableIdentifier.of("db", "other");
  private static final TableReference REFERENCE = TableReference.of("mem", TABLE);
  private static final long AWAIT_MS = 10_000L;

  private final List<String> logs = new CopyOnWriteArrayList<>();
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final ManualPool manualPool = new ManualPool();
  private final String staging = "mem://staging/" + UUID.randomUUID() + "/stats/";
  private ExecutorService threadPool;
  private InMemoryFileIO io;
  private Catalog catalog;
  private Table loadedTable;
  private IcebergSinkConfig config;
  private TableSinkConfig tableConfig;
  private StatisticsHealth health;
  private StatisticsLag lag;
  private ScriptedStep step;
  private int puffinCount = 0;

  @BeforeEach
  public void before() {
    threadPool = Executors.newCachedThreadPool();
    io = new InMemoryFileIO();
    catalog = mock(Catalog.class);
    loadedTable = mock(Table.class);
    when(catalog.loadTable(any(TableIdentifier.class))).thenReturn(loadedTable);
    config = mock(IcebergSinkConfig.class);
    tableConfig = mock(TableSinkConfig.class);
    when(config.tableConfig(anyString())).thenReturn(tableConfig);
    health = new StatisticsHealth(0L);
    lag = new StatisticsLag();
    step = new ScriptedStep();
  }

  @AfterEach
  public void after() throws InterruptedException {
    step.releaseAll();
    threadPool.shutdownNow();
    assertThat(threadPool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(step.overlaps()).as("two steps of one table at once").isEmpty();
  }

  @Test
  public void taskGoesToPoolAtOnceWhenNoneRuns() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsStepTask task = task(1L, 0L, puffin(), puffin());

    queue.enqueue(task);

    assertThat(manualPool.size()).isEqualTo(1);
    assertThat(queue.hasTask(TABLE)).isTrue();
    assertThat(step.inputs()).as("the step runs on the pool, not in enqueue").isEmpty();

    manualPool.runNext();

    assertThat(step.inputs()).hasSize(1);
    StatisticsStepInput input = step.inputs().get(0);
    assertThat(input.identifier()).isEqualTo(TABLE);
    assertThat(input.snapshotId()).isEqualTo(1L);
    assertThat(input.ownSnapshotIds()).containsExactly(1L);
    assertThat(input.answers()).isEqualTo(task.answers());
    assertThat(input.commitId()).isEqualTo(task.commitId());
    assertThat(input.ownChangeSetIds()).isEmpty();
    assertThat(input.keepSketchesOnFailure()).isFalse();
    assertThat(input.config()).isSameAs(config);
    assertThat(input.tableConfig()).isSameAs(tableConfig);
    assertThat(input.health()).isSameAs(health);
    assertThat(input.stopped().getAsBoolean()).isFalse();
    assertThat(queue.hasTask(TABLE)).isFalse();
    assertThat(manualPool.size()).isZero();
  }

  @Test
  public void taskSetWhileOneRunsWaitsAndGoesToPoolWhenItEnds() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsStepTask first = task(1L, 0L, puffin());
    StatisticsStepTask second = task(2L, 0L, puffin());

    queue.enqueue(first);
    queue.enqueue(second);

    assertThat(manualPool.size()).as("the second waits in the queue, not on the pool").isEqualTo(1);

    manualPool.runNext();

    assertThat(step.snapshotIds()).containsExactly(1L);
    assertThat(manualPool.size()).as("the waiting task went to the pool").isEqualTo(1);
    assertThat(queue.hasTask(TABLE)).isTrue();

    manualPool.runNext();

    assertThat(step.snapshotIds()).containsExactly(1L, 2L);
    assertThat(step.inputs().get(1).answers()).isEqualTo(second.answers());
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void threeCommitsWhileFirstRunsMergeIntoOneStepOnThird() {
    StatisticsStepQueue queue = queue(manualPool);
    lag.enter(TABLE);
    long oldToken = lag.token(TABLE);
    lag.enter(TABLE);
    long newToken = lag.token(TABLE);
    StatisticsStepTask first = task(1L, 0L, puffin());
    StatisticsStepTask second = task(2L, oldToken, puffin());
    StatisticsStepTask third = task(3L, newToken, puffin());

    queue.enqueue(first);
    queue.enqueue(second);
    queue.enqueue(third);

    assertThat(manualPool.size()).isEqualTo(1);

    manualPool.runAll();

    assertThat(step.snapshotIds())
        .as("one step on the third snapshot; the second gets none")
        .containsExactly(1L, 3L);
    StatisticsStepInput running = step.inputs().get(0);
    assertThat(running.ownSnapshotIds()).as("a running task is not merged").containsExactly(1L);
    assertThat(running.answers()).isEqualTo(first.answers());
    StatisticsStepInput merged = step.inputs().get(1);
    assertThat(merged.commitId()).isEqualTo(third.commitId());
    assertThat(merged.ownSnapshotIds()).containsExactlyInAnyOrder(2L, 3L);
    assertThat(merged.answers())
        .containsExactlyElementsOf(
            ImmutableList.<StatisticsAnswer>builder()
                .addAll(second.answers())
                .addAll(third.answers())
                .build());
    assertThat(lag.contains(TABLE))
        .as("the merged task removes the lag by the token of the newer task")
        .isFalse();
  }

  @Test
  public void mergeTakesTokenOfNewerTaskEvenIfItIsOlder() {
    StatisticsStepQueue queue = queue(manualPool);
    lag.enter(TABLE);
    long token = lag.token(TABLE);
    queue.enqueue(task(1L, 0L));
    queue.enqueue(task(2L, token));
    queue.enqueue(task(3L, 0L));

    manualPool.runAll();

    assertThat(step.snapshotIds()).containsExactly(1L, 3L);
    assertThat(lag.contains(TABLE))
        .as("the token of the waiting task is replaced by the newer one's")
        .isTrue();
  }

  @Test
  public void mergedWithTakesSnapshotCommitIdAndTokenOfNewerAndUnitesRest() {
    Table otherTable = commitTable(new InMemoryFileIO());
    StatisticsStepTask older = task(2L, 5L, puffin());
    StatisticsStepTask newer =
        new StatisticsStepTask(
            TableReference.of("mem", TABLE, UUID.randomUUID()),
            otherTable,
            3L,
            UUID.randomUUID(),
            ImmutableSet.of(3L, 4L),
            7L,
            answers(3L, puffin()));

    StatisticsStepTask merged = older.mergedWith(newer);

    assertThat(merged.snapshotId()).isEqualTo(3L);
    assertThat(merged.commitId()).isEqualTo(newer.commitId());
    assertThat(merged.lagToken()).isEqualTo(7L);
    assertThat(merged.tableReference()).isSameAs(newer.tableReference());
    assertThat(merged.table()).isSameAs(otherTable);
    assertThat(merged.ownSnapshotIds()).containsExactlyInAnyOrder(2L, 3L, 4L);
    assertThat(merged.answers()).containsExactly(older.answers().get(0), newer.answers().get(0));
  }

  @Test
  public void answersWithSharedPuffinInMergedTasksAreAllKept() {
    // answers of different commits share one Puffin file: the waiting task of commit N merges with
    // that of commit N+1
    StatisticsStepQueue queue = queue(manualPool);
    String shared = puffin();
    queue.enqueue(task(1L, 0L, puffin()));
    StatisticsStepTask second = task(2L, 0L, shared);
    StatisticsStepTask third = task(3L, 0L, shared);
    queue.enqueue(second);
    queue.enqueue(third);

    assertThat(queue.inFlightSketchLocations(TABLE)).contains(shared);

    manualPool.runAll();

    StatisticsStepInput merged = step.inputs().get(1);
    assertThat(merged.answers()).hasSize(2);
    assertThat(merged.answers())
        .allSatisfy(answer -> assertThat(answer.sketchFile().location()).isEqualTo(shared));
    assertThat(merged.ownSnapshotIds()).containsExactlyInAnyOrder(2L, 3L);
  }

  @Test
  public void tablesHaveTheirOwnQueues() {
    StatisticsStepQueue queue = queue(manualPool);

    queue.enqueue(task(1L, 0L));
    queue.enqueue(task(OTHER, 2L, 0L));

    assertThat(manualPool.size()).as("both run at once").isEqualTo(2);

    manualPool.runAll();

    assertThat(step.snapshotIds()).containsExactlyInAnyOrder(1L, 2L);
  }

  @Test
  public void queueIsKeyedByIdentifierNotByReference() {
    StatisticsStepQueue queue = queue(manualPool);

    queue.enqueue(task(1L, 0L));
    queue.enqueue(
        new StatisticsStepTask(
            TableReference.of("mem", TABLE, UUID.randomUUID()),
            commitTable(io),
            2L,
            UUID.randomUUID(),
            ImmutableSet.of(2L),
            0L,
            ImmutableList.of()));

    assertThat(manualPool.size()).as("a reference with a uuid is the same table").isEqualTo(1);

    manualPool.runAll();

    assertThat(step.snapshotIds()).containsExactly(1L, 2L);
  }

  @Test
  public void dataTaskDoesNotWaitForLagStepWaitingForPermit() throws Exception {
    StatisticsStepQueue queue = queue(threadPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    takeAllPermits(limiter);
    lag.enter(TABLE);
    long token = lag.token(TABLE);
    CountDownLatch release = step.latch(1L);

    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag)).isTrue();
    assertThat(limiter.inFlight(TABLE)).isTrue();
    queue.enqueue(task(1L, token, puffin()));

    step.awaitStarted(1L);
    assertThat(step.snapshotIds()).as("the data task runs at once").containsExactly(1L);

    limiter.release();
    limiter.release();
    await(() -> !limiter.inFlight(TABLE), "the lag step leaves its slot");
    await(() -> limiter.availablePermits() == 2, "the lag step gives its permit back");
    assertThat(queue.inFlightSketchLocations(TABLE)).as("the data task still runs").isNotEmpty();

    release.countDown();
    await(() -> !queue.hasTask(TABLE), "the data task ends");

    assertThat(step.snapshotIds())
        .as("no lag step: the table had a task when it got the permit")
        .containsExactly(1L);
    assertThat(lag.contains(TABLE)).as("the data task removes the lag").isFalse();
  }

  @Test
  public void lagStepTakesEmptyQueueAndDataTaskSetAfterItWaits() throws Exception {
    StatisticsStepQueue queue = queue(threadPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    lag.enter(TABLE);
    CountDownLatch release = step.latch(null);

    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag)).isTrue();
    step.awaitStarted(null);
    String puffin = puffin();
    queue.enqueue(task(1L, 0L, puffin));

    assertThat(queue.inFlightSketchLocations(TABLE)).containsExactly(puffin);
    assertThat(step.inputs()).as("the data task waits behind the lag step").hasSize(1);

    release.countDown();
    await(() -> !queue.hasTask(TABLE), "both tasks end");

    assertThat(step.snapshotIds()).containsExactly(null, 1L);
    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(lag.contains(TABLE)).isFalse();
  }

  @Test
  public void lagStepIsNotSetWhileTableHasTask() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    int freeSlots = limiter.freeSlots();

    queue.enqueue(task(1L, 0L));
    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag))
        .as("a running task")
        .isFalse();
    queue.enqueue(task(2L, 0L));
    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag))
        .as("a running and a waiting task")
        .isFalse();

    assertThat(limiter.freeSlots()).isEqualTo(freeSlots);
    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(manualPool.size()).isEqualTo(1);

    assertThat(
            queue.enqueueLagStep(TableReference.of("mem", OTHER), UUID.randomUUID(), limiter, lag))
        .isTrue();
    assertThat(
            queue.enqueueLagStep(TableReference.of("mem", OTHER), UUID.randomUUID(), limiter, lag))
        .as("a lag step of the table that has not taken the queue yet")
        .isFalse();
    assertThat(limiter.freeSlots()).isEqualTo(freeSlots - 1);
    assertThat(manualPool.size()).isEqualTo(2);

    manualPool.runAll();

    assertThat(step.snapshotIds()).containsExactlyInAnyOrder(1L, 2L, null);
    assertThat(limiter.freeSlots()).isEqualTo(freeSlots);
  }

  @Test
  public void lagStepTakesTokenAfterPermit() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    lag.enter(TABLE);
    UUID commitId = UUID.randomUUID();

    assertThat(queue.enqueueLagStep(REFERENCE, commitId, limiter, lag)).isTrue();
    // the table enters the lag again before the step runs: a token taken when the step was set
    // would not remove this entry
    lag.enter(TABLE);
    manualPool.runNext();

    assertThat(step.inputs()).hasSize(1);
    StatisticsStepInput input = step.inputs().get(0);
    assertThat(input.snapshotId()).as("S is the head: the step finds it").isNull();
    assertThat(input.ownSnapshotIds()).isEmpty();
    assertThat(input.answers()).isEmpty();
    assertThat(input.commitId()).isEqualTo(commitId);
    assertThat(input.table()).isSameAs(loadedTable);
    assertThat(lag.contains(TABLE)).isFalse();
    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void lagStepGivesPermitAndSlotBackWhenItFails() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    lag.enter(TABLE);
    IllegalStateException failure = new IllegalStateException("step failed");
    step.behave(input -> new Result(Outcome.FAILED, failure));

    queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag);
    manualPool.runNext();

    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(lag.contains(TABLE)).isTrue();
    assertThat(logs)
        .as("a lag step has no snapshot of its own")
        .anyMatch(
            line -> line.startsWith("WARN Statistics step of table db.tbl at the head failed"))
        .noneMatch(line -> line.contains("snapshot null"));

    step.behave(
        input -> {
          throw new IllegalStateException("a substitute that throws");
        });
    queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag);
    manualPool.runNext();

    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(queue.hasTask(TABLE)).isFalse();
    assertThat(logs).anyMatch(line -> line.startsWith("DEBUG Statistics step of table db.tbl"));
  }

  @Test
  public void taskLoadsItsOwnTableRightBeforeStep() {
    StatisticsStepQueue queue = queue(manualPool);
    queue.rememberTable(TABLE, mock(Table.class));

    queue.enqueue(task(1L, 0L));

    verify(catalog, never()).loadTable(any(TableIdentifier.class));

    manualPool.runNext();

    verify(catalog, times(1)).loadTable(TABLE);
    assertThat(step.inputs().get(0).table()).isSameAs(loadedTable);
  }

  @Test
  public void tableGoneWhenTaskLoadsItIsTableGoneAndPuffinsAreDeleted() {
    StatisticsStepQueue queue = queue(manualPool);
    when(catalog.loadTable(TABLE)).thenThrow(new NoSuchTableException("Table dropped: %s", TABLE));
    health.update(TABLE, flags -> new Flags(true, false, false));
    queue.rememberTable(TABLE, mock(Table.class));
    lag.enter(TABLE);
    String first = puffin();
    String second = puffin();

    queue.enqueue(task(1L, lag.token(TABLE), first, second));
    manualPool.runNext();

    assertThat(step.inputs()).as("the step does not run").isEmpty();
    assertThat(io.fileExists(first)).isFalse();
    assertThat(io.fileExists(second)).isFalse();
    assertThat(health.version(TABLE)).as("the health record is removed").isZero();
    assertThat(lag.contains(TABLE)).isFalse();
    assertThat(queue.cleanupTables()).doesNotContainKey(TABLE);
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void stoppedWhileTaskLoadsTableIsStoppedAndPuffinsStay() {
    List<Result> results = Lists.newArrayList();
    StatisticsStepQueue queue = queue(manualPool, recording(results));
    when(catalog.loadTable(TABLE))
        .thenAnswer(
            invocation -> {
              stop.set(true);
              throw new IllegalStateException("the catalog client is closed");
            });
    health.update(TABLE, flags -> new Flags(true, false, false));
    lag.enter(TABLE);
    long version = health.version(TABLE);
    String puffin = puffin();

    queue.enqueue(task(1L, lag.token(TABLE), puffin));
    manualPool.runNext();

    assertThat(results).extracting(Result::outcome).containsExactly(Outcome.STOPPED);
    assertThat(step.inputs()).isEmpty();
    assertThat(io.fileExists(puffin)).as("Puffin files are kept on stop").isTrue();
    assertThat(health.version(TABLE)).isEqualTo(version);
    assertThat(lag.contains(TABLE)).isTrue();
  }

  @Test
  public void otherFailureToLoadTableIsFailureAndPuffinsAreDeleted() {
    List<Result> results = Lists.newArrayList();
    StatisticsStepQueue queue = queue(manualPool, recording(results));
    IllegalStateException failure = new IllegalStateException("catalog unavailable");
    when(catalog.loadTable(TABLE)).thenThrow(failure);
    health.update(TABLE, flags -> new Flags(true, false, false));
    long version = health.version(TABLE);
    String puffin = puffin();

    queue.enqueue(task(1L, 0L, puffin));
    manualPool.runNext();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).outcome()).isEqualTo(Outcome.FAILED);
    assertThat(results.get(0).failure()).isSameAs(failure);
    assertThat(step.inputs()).isEmpty();
    assertThat(io.fileExists(puffin)).isFalse();
    assertThat(health.version(TABLE)).isEqualTo(version);
    assertThat(lag.contains(TABLE)).isTrue();
    assertThat(logs)
        .anyMatch(
            line ->
                line.startsWith("WARN Statistics step of table db.tbl at snapshot 1 failed")
                    && line.contains("statistics lag"));
  }

  @Test
  public void taskHoldsTableOfItsCommitUntilItEnds() {
    // a catalog that vends credentials per table closes the table's FileIO once its operations are
    // collected, and the task deletes its Puffin files through that FileIO when its own load fails
    StatisticsStepQueue queue = queue(manualPool);
    when(catalog.loadTable(TABLE)).thenThrow(new IllegalStateException("catalog unavailable"));
    String puffin = puffin();
    WeakReference<Table> table = enqueuedWithItsTable(queue, puffin);

    assertThat(collectedWithin(table, 2_000L))
        .as("the waiting task holds the table of its commit")
        .isFalse();

    manualPool.runNext();

    assertThat(io.fileExists(puffin)).as("deleted through the FileIO of that table").isFalse();
    assertThat(collectedWithin(table, AWAIT_MS)).as("let go once the task ends").isTrue();
  }

  @Test
  public void inFlightSketchLocationsDoNotWaitForRunningStep() throws Exception {
    StatisticsStepQueue queue = queue(threadPool);
    CountDownLatch release = step.latch(1L);
    String first = puffin();
    String second = puffin();
    String third = puffin();
    String fourth = puffin();

    queue.enqueue(task(1L, 0L, first, second));
    step.awaitStarted(1L);

    assertThat(inFlightWithin(queue, TABLE)).containsExactlyInAnyOrder(first, second);

    queue.enqueue(task(2L, 0L, third));
    queue.enqueue(task(3L, 0L, fourth));

    assertThat(inFlightWithin(queue, TABLE))
        .as("the running task and the merged waiting one")
        .containsExactlyInAnyOrder(first, second, third, fourth);
    assertThat(inFlightWithin(queue, OTHER)).isEmpty();

    release.countDown();
    await(() -> !queue.hasTask(TABLE), "the tasks end");

    assertThat(queue.inFlightSketchLocations(TABLE)).isEmpty();
    assertThat(step.snapshotIds()).containsExactly(1L, 3L);
  }

  @Test
  public void stopDropsWaitingTaskAndRunningOneSeesFlag() throws Exception {
    StatisticsStepQueue queue = queue(threadPool);
    CountDownLatch release = step.latch(1L);
    queue.enqueue(task(1L, 0L, puffin()));
    step.awaitStarted(1L);
    String waiting = puffin();
    queue.enqueue(task(2L, 0L, waiting));

    queue.stop();

    BooleanSupplier stopped = step.inputs().get(0).stopped();
    assertThat(stopped.getAsBoolean())
        .as("the running step sees the stop of the queue, not only the committer's flag")
        .isTrue();
    assertThat(queue.inFlightSketchLocations(TABLE)).doesNotContain(waiting);

    release.countDown();
    await(() -> !queue.hasTask(TABLE), "the running task ends");

    assertThat(step.snapshotIds()).as("the waiting task never runs").containsExactly(1L);
    assertThat(io.fileExists(waiting)).as("left to the staging cleanup").isTrue();
  }

  @Test
  public void poolThreadInterruptedWhileTaskLoadsTableIsStoppedAndPuffinsStay() {
    List<Result> results = Lists.newArrayList();
    StatisticsStepQueue queue = queue(manualPool, recording(results));
    when(catalog.loadTable(TABLE))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("the catalog call is interrupted");
            });
    String puffin = puffin();

    queue.enqueue(task(1L, 0L, puffin));
    try {
      manualPool.runNext();
    } finally {
      // the manual pool runs on the test thread: do not leak the flag
      Thread.interrupted();
    }

    assertThat(results).extracting(Result::outcome).containsExactly(Outcome.STOPPED);
    assertThat(step.inputs()).isEmpty();
    assertThat(io.fileExists(puffin)).as("Puffin files are kept on stop").isTrue();
  }

  @Test
  public void poolThreadInterruptedBeforeTaskIsStoppedWithoutLoadingTable() {
    List<Result> results = Lists.newArrayList();
    StatisticsStepQueue queue = queue(manualPool, recording(results));
    String puffin = puffin();

    queue.enqueue(task(1L, 0L, puffin));
    Thread.currentThread().interrupt();
    try {
      manualPool.runNext();
    } finally {
      Thread.interrupted();
    }

    assertThat(results).extracting(Result::outcome).containsExactly(Outcome.STOPPED);
    verify(catalog, never()).loadTable(any(TableIdentifier.class));
    assertThat(step.inputs()).isEmpty();
    assertThat(io.fileExists(puffin)).as("Puffin files are kept on stop").isTrue();
  }

  @Test
  public void stopBetweenDeletionsOfSketchesKeepsNextOnes() {
    StatisticsStepQueue queue = queue(manualPool);
    when(catalog.loadTable(TABLE)).thenThrow(new IllegalStateException("catalog unavailable"));
    String first = puffin();
    String second = puffin();
    String third = puffin();
    FileIO stoppingIo =
        new InMemoryFileIO() {
          @Override
          public void deleteFile(String location) {
            io.deleteFile(location);
            stop.set(true);
          }
        };
    StatisticsStepTask task =
        new StatisticsStepTask(
            REFERENCE,
            commitTable(stoppingIo),
            1L,
            UUID.randomUUID(),
            ImmutableSet.of(1L),
            0L,
            answers(1L, first, second, third));

    queue.enqueue(task);
    manualPool.runNext();

    assertThat(io.fileExists(first)).as("deleted before the stop").isFalse();
    assertThat(io.fileExists(second)).as("the stop comes between the deletions").isTrue();
    assertThat(io.fileExists(third)).isTrue();
  }

  @Test
  public void stopWithLagStepThatHasNotStartedFreesItsSlot() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    int freeSlots = limiter.freeSlots();
    lag.enter(TABLE);
    queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag);

    assertThat(limiter.freeSlots()).isEqualTo(freeSlots - 1);

    queue.stop();

    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(limiter.freeSlots()).isEqualTo(freeSlots);
    assertThat(queue.hasTask(TABLE)).isFalse();

    manualPool.runAll();

    assertThat(step.inputs()).isEmpty();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(limiter.freeSlots()).isEqualTo(freeSlots);
    assertThat(lag.contains(TABLE)).isTrue();
  }

  @Test
  public void stopWithLagStepWaitingForPermitFreesItsSlot() throws Exception {
    StatisticsStepQueue queue = queue(threadPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(2);
    takeAllPermits(limiter);
    lag.enter(TABLE);
    queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag);

    assertThat(limiter.freeSlots()).isZero();

    queue.stop();

    assertThat(limiter.inFlight(TABLE)).isFalse();
    assertThat(limiter.freeSlots()).isEqualTo(1);

    limiter.release();
    limiter.release();
    threadPool.shutdown();
    assertThat(threadPool.awaitTermination(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    assertThat(step.inputs()).isEmpty();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(limiter.freeSlots()).isEqualTo(1);
  }

  @Test
  public void nothingIsSetAfterStop() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    int freeSlots = limiter.freeSlots();

    queue.stop();
    queue.enqueue(task(1L, 0L));

    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag)).isFalse();
    assertThat(manualPool.size()).isZero();
    assertThat(queue.hasTask(TABLE)).isFalse();
    assertThat(limiter.freeSlots()).isEqualTo(freeSlots);
  }

  @Test
  public void committerStopFlagStopsTaskBeforeItLoadsTable() {
    List<Result> results = Lists.newArrayList();
    StatisticsStepQueue queue = queue(manualPool, recording(results));
    String puffin = puffin();
    queue.enqueue(task(1L, 0L, puffin));
    queue.enqueue(task(2L, 0L));

    stop.set(true);
    manualPool.runAll();

    assertThat(results).extracting(Result::outcome).containsExactly(Outcome.STOPPED);
    verify(catalog, never()).loadTable(any(TableIdentifier.class));
    assertThat(step.inputs()).isEmpty();
    assertThat(io.fileExists(puffin)).isTrue();
    assertThat(queue.hasTask(TABLE)).as("the waiting task is dropped").isFalse();
  }

  @Test
  public void standaloneOutcomeRemovesLagByTokenOfTask() {
    StatisticsStepQueue queue = queue(manualPool);
    OutcomeAction action = StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs));

    for (Outcome outcome : List.of(Outcome.WRITTEN, Outcome.NOT_NEEDED, Outcome.FINAL)) {
      lag.enter(TABLE);
      long token = lag.token(TABLE);
      action.apply(queue, task(1L, token), new Result(outcome, null));
      assertThat(lag.contains(TABLE)).as("removed on %s", outcome).isFalse();

      lag.enter(TABLE);
      long oldToken = lag.token(TABLE);
      lag.enter(TABLE);
      action.apply(queue, task(1L, oldToken), new Result(outcome, null));
      assertThat(lag.contains(TABLE))
          .as("a newer entry survives the task of an earlier cycle on %s", outcome)
          .isTrue();
      lag.removeIfToken(TABLE, lag.token(TABLE));
    }
  }

  @Test
  public void standaloneOutcomeEntersLagOnFailureAndWarnsOnlyOnFirstInRow() {
    StatisticsStepQueue queue = queue(manualPool);
    OutcomeAction action = StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs));
    IllegalStateException failure = new IllegalStateException("kind failed");

    action.apply(queue, task(1L, 0L), new Result(Outcome.FAILED, failure));

    assertThat(lag.contains(TABLE)).isTrue();
    assertThat(lag.isVerified(TABLE)).isTrue();
    assertThat(logs).hasSize(1);
    assertThat(logs.get(0)).startsWith("WARN Statistics step of table db.tbl at snapshot 1 failed");
    long token = lag.token(TABLE);

    action.apply(queue, task(2L, 0L), new Result(Outcome.FAILED, failure));

    assertThat(lag.token(TABLE)).as("a new generation").isGreaterThan(token);
    assertThat(logs).hasSize(2);
    assertThat(logs.get(1)).startsWith("DEBUG Statistics step of table db.tbl at snapshot 2");
  }

  @Test
  public void standaloneOutcomeEntersLagWhenSnapshotIsGone() {
    StatisticsStepQueue queue = queue(manualPool);
    OutcomeAction action = StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs));

    action.apply(queue, task(1L, 0L), new Result(Outcome.SNAPSHOT_GONE, null));

    assertThat(lag.contains(TABLE)).isTrue();
    assertThat(lag.isVerified(TABLE)).isTrue();
    assertThat(logs).isEmpty();
  }

  @Test
  public void standaloneOutcomeOnTableGoneRemovesLagAndForgetsTable() {
    StatisticsStepQueue queue = queue(manualPool);
    OutcomeAction action = StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs));
    queue.rememberTable(TABLE, mock(Table.class));
    queue.rememberTable(OTHER, mock(Table.class));
    lag.enter(TABLE);

    action.apply(queue, task(1L, lag.token(TABLE)), new Result(Outcome.TABLE_GONE, null));

    assertThat(lag.contains(TABLE)).isFalse();
    assertThat(queue.cleanupTables()).containsOnlyKeys(OTHER);
  }

  @Test
  public void deferredAndStoppedChangeNothing() {
    StatisticsStepQueue queue = queue(manualPool);
    OutcomeAction standalone = StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs));
    OutcomeAction tail = StatisticsStepQueue.tailOutcome(lag, recordingLogger(logs));
    lag.enter(TABLE);
    long token = lag.token(TABLE);
    queue.rememberTable(TABLE, mock(Table.class));

    for (Outcome outcome : List.of(Outcome.DEFERRED, Outcome.STOPPED)) {
      standalone.apply(queue, task(1L, token), new Result(outcome, null));
      tail.apply(queue, task(1L, token), new Result(outcome, null));
    }

    assertThat(lag.token(TABLE)).isEqualTo(token);
    assertThat(queue.cleanupTables()).containsOnlyKeys(TABLE);
    assertThat(logs).isEmpty();
  }

  @Test
  public void tailOutcomeOnlyEntersCopyOnWriteLag() {
    StatisticsStepQueue queue = queue(manualPool);
    StatisticsLag copyOnWriteLag = new StatisticsLag();
    OutcomeAction action = StatisticsStepQueue.tailOutcome(copyOnWriteLag, recordingLogger(logs));
    queue.rememberTable(TABLE, mock(Table.class));
    copyOnWriteLag.enter(TABLE);
    long token = copyOnWriteLag.token(TABLE);

    for (Outcome outcome :
        List.of(Outcome.WRITTEN, Outcome.NOT_NEEDED, Outcome.FINAL, Outcome.TABLE_GONE)) {
      action.apply(queue, task(1L, token), new Result(outcome, null));
    }

    assertThat(copyOnWriteLag.token(TABLE)).as("the tail never removes the lag").isEqualTo(token);
    assertThat(queue.cleanupTables()).containsOnlyKeys(TABLE);

    action.apply(
        queue, task(OTHER, 2L, 0L), new Result(Outcome.FAILED, new IllegalStateException("tail")));
    action.apply(queue, task(OTHER, 3L, 0L), new Result(Outcome.SNAPSHOT_GONE, null));

    assertThat(copyOnWriteLag.contains(OTHER)).isTrue();
    assertThat(logs).hasSize(1);
    assertThat(logs.get(0))
        .startsWith("WARN Statistics step of table db.other at snapshot 2 failed");
  }

  @Test
  public void queueAppliesOutcomeBeforeTaskLeavesIt() {
    AtomicReference<Boolean> hadTask = new AtomicReference<>();
    AtomicReference<Set<String>> inFlight = new AtomicReference<>();
    AtomicReference<StatisticsStepQueue> queueRef = new AtomicReference<>();
    StatisticsStepQueue queue =
        queue(
            manualPool,
            (owner, task, result) -> {
              hadTask.set(owner.hasTask(TABLE));
              inFlight.set(owner.inFlightSketchLocations(TABLE));
              assertThat(owner).isSameAs(queueRef.get());
            });
    queueRef.set(queue);
    String puffin = puffin();

    queue.enqueue(task(1L, 0L, puffin));
    manualPool.runNext();

    assertThat(hadTask.get()).isTrue();
    assertThat(inFlight.get()).containsExactly(puffin);
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void cleanupTablesKeepLatestTableOfEachIdentifier() {
    StatisticsStepQueue queue = queue(manualPool);
    Table first = commitTable(io);
    Table second = commitTable(io);
    Table other = commitTable(io);

    queue.rememberTable(TABLE, first);
    queue.rememberTable(TABLE, second);
    queue.rememberTable(OTHER, other);
    Map<TableIdentifier, CleanupTable> tables = queue.cleanupTables();

    assertThat(tables).containsOnlyKeys(TABLE, OTHER);
    assertThat(tables.get(TABLE).uuid()).isEqualTo(second.uuid());
    assertThat(tables.get(TABLE).location()).isEqualTo(second.location());
    assertThat(tables.get(TABLE).name()).isEqualTo(second.name());
    assertThat(tables.get(OTHER).uuid()).isEqualTo(other.uuid());

    queue.forgetTable(TABLE);

    assertThat(tables).as("a snapshot").containsOnlyKeys(TABLE, OTHER);
    assertThat(queue.cleanupTables()).containsOnlyKeys(OTHER);
    assertThat(queue.cleanupTable(OTHER).uuid()).isEqualTo(other.uuid());
  }

  @Test
  public void cleanupTablesDoNotHoldTableOfCommit() {
    // dynamic routing to thousands of tables would keep the metadata and the FileIO of each
    StatisticsStepQueue queue = queue(manualPool);
    WeakReference<Table> table = rememberedTable(queue);

    assertThat(collectedWithin(table, AWAIT_MS))
        .as("the cleanup set does not hold the table of commit()")
        .isTrue();
    assertThat(queue.cleanupTables()).containsOnlyKeys(TABLE);
  }

  @Test
  public void cleanupTablesAreSafeToChangeFromManyThreads() throws Exception {
    StatisticsStepQueue queue = queue(manualPool);
    List<CompletableFuture<Void>> writers = Lists.newArrayList();
    for (int writer = 0; writer < 8; writer++) {
      int base = writer * 100;
      writers.add(
          CompletableFuture.runAsync(
              () -> {
                for (int index = 0; index < 100; index++) {
                  TableIdentifier id = TableIdentifier.of("db", "t" + (base + index));
                  queue.rememberTable(id, mock(Table.class));
                  queue.cleanupTables();
                  if (index % 2 == 0) {
                    queue.forgetTable(id);
                  }
                }
              },
              threadPool));
    }

    CompletableFuture.allOf(writers.toArray(new CompletableFuture<?>[0]))
        .get(AWAIT_MS, TimeUnit.MILLISECONDS);

    assertThat(queue.cleanupTables()).hasSize(400);
  }

  @Test
  public void stepThatThrowsIsFailureAndWaitingTaskStillRuns() {
    List<Result> results = Lists.newArrayList();
    StatisticsStepQueue queue = queue(manualPool, recording(results));
    IllegalStateException failure = new IllegalStateException("a substitute that throws");
    step.behave(
        input -> {
          if (input.snapshotId() == 1L) {
            throw failure;
          }

          return new Result(Outcome.WRITTEN, null);
        });
    String puffin = puffin();
    queue.enqueue(task(1L, 0L, puffin));
    queue.enqueue(task(2L, 0L));

    manualPool.runAll();

    assertThat(results)
        .extracting(Result::outcome)
        .containsExactly(Outcome.FAILED, Outcome.WRITTEN);
    assertThat(results.get(0).failure()).isSameAs(failure);
    assertThat(io.fileExists(puffin)).isFalse();
    assertThat(step.snapshotIds()).containsExactly(1L, 2L);
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void stepThatThrowsErrorPutsTableIntoLagAndErrorGoesOn() {
    StatisticsStepQueue queue = queue(manualPool);
    OutOfMemoryError error = new OutOfMemoryError("a substitute that throws");
    step.behave(
        input -> {
          throw error;
        });
    queue.enqueue(task(1L, 0L));

    assertThatThrownBy(manualPool::runAll).isSameAs(error);

    assertThat(lag.contains(TABLE)).isTrue();
    assertThat(logs).hasSize(1);
    assertThat(logs.get(0)).startsWith("WARN Statistics step of table db.tbl at snapshot 1 failed");
    assertThat(queue.hasTask(TABLE)).as("the queue is free").isFalse();
  }

  @Test
  public void stepOfLagThatThrowsErrorKeepsLagAndFreesSlot() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(2);
    StatisticsStepQueue queue = queue(manualPool);
    OutOfMemoryError error = new OutOfMemoryError("a substitute that throws");
    step.behave(
        input -> {
          throw error;
        });
    lag.enter(TABLE);
    long token = lag.token(TABLE);
    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag)).isTrue();

    assertThatThrownBy(manualPool::runAll).isSameAs(error);

    assertThat(lag.contains(TABLE)).isTrue();
    assertThat(lag.token(TABLE)).as("a new generation").isGreaterThan(token);
    assertThat(queue.hasTask(TABLE)).as("the queue is free").isFalse();
    assertThat(limiter.inFlight(TABLE)).as("the slot is free").isFalse();
    assertThat(limiter.availablePermits()).as("the permit is given back").isEqualTo(2);
  }

  @Test
  public void tailOutcomeEntersCopyOnWriteLagWhenStepThrowsError() {
    StatisticsLag copyOnWriteLag = new StatisticsLag();
    StatisticsStepQueue queue =
        queue(manualPool, StatisticsStepQueue.tailOutcome(copyOnWriteLag, recordingLogger(logs)));
    OutOfMemoryError error = new OutOfMemoryError("a substitute that throws");
    step.behave(
        input -> {
          throw error;
        });
    queue.enqueue(task(1L, 0L));

    assertThatThrownBy(manualPool::runAll).isSameAs(error);

    assertThat(copyOnWriteLag.contains(TABLE)).isTrue();
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void actionThatThrowsDoesNotStallQueue() {
    StatisticsStepQueue queue =
        queue(
            manualPool,
            (owner, task, result) -> {
              throw new IllegalStateException("action failed");
            });
    queue.enqueue(task(1L, 0L));
    queue.enqueue(task(2L, 0L));

    manualPool.runAll();

    assertThat(step.snapshotIds()).containsExactly(1L, 2L);
    assertThat(queue.hasTask(TABLE)).isFalse();
  }

  @Test
  public void rejectedPoolDoesNotStallTableOrKeepSlot() {
    StatisticsStepQueue queue =
        queue(
            command -> {
              throw new RejectedExecutionException("pool is shut down");
            });
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    int freeSlots = limiter.freeSlots();

    queue.enqueue(task(1L, 0L));

    assertThat(queue.hasTask(TABLE)).isFalse();
    assertThat(queue.enqueueLagStep(REFERENCE, UUID.randomUUID(), limiter, lag)).isFalse();
    assertThat(queue.hasTask(TABLE)).isFalse();
    assertThat(limiter.freeSlots()).isEqualTo(freeSlots);
  }

  private StatisticsStepQueue queue(Executor pool) {
    return queue(pool, StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs)));
  }

  private StatisticsStepQueue queue(Executor pool, OutcomeAction action) {
    return new StatisticsStepQueue(pool, catalog, config, step, health, stop::get, action);
  }

  private OutcomeAction recording(List<Result> results) {
    OutcomeAction standalone = StatisticsStepQueue.standaloneOutcome(lag, recordingLogger(logs));
    return (owner, task, result) -> {
      results.add(result);
      standalone.apply(owner, task, result);
    };
  }

  private StatisticsStepTask task(long snapshotId, long token, String... puffins) {
    return task(TABLE, snapshotId, token, puffins);
  }

  private StatisticsStepTask task(
      TableIdentifier id, long snapshotId, long token, String... puffins) {
    return task(id, commitTable(io), snapshotId, token, puffins);
  }

  private static StatisticsStepTask task(
      TableIdentifier id, Table table, long snapshotId, long token, String... puffins) {
    return new StatisticsStepTask(
        TableReference.of("mem", id),
        table,
        snapshotId,
        UUID.randomUUID(),
        ImmutableSet.of(snapshotId),
        token,
        answers(snapshotId, puffins));
  }

  private WeakReference<Table> enqueuedWithItsTable(StatisticsStepQueue queue, String puffin) {
    Table table = commitTable(io);
    queue.enqueue(task(TABLE, table, 1L, 0L, puffin));
    return new WeakReference<>(table);
  }

  private WeakReference<Table> rememberedTable(StatisticsStepQueue queue) {
    Table table = commitTable(io);
    queue.rememberTable(TABLE, table);
    return new WeakReference<>(table);
  }

  /**
   * The table of {@code commit()} with {@code tableIo}: a plain proxy rather than a mock, which
   * Mockito keeps reachable after a call, so that a test can see whether the queue still holds it.
   */
  private static Table commitTable(FileIO tableIo) {
    UUID uuid = UUID.randomUUID();
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "io":
                  return tableIo;
                case "uuid":
                  return uuid;
                case "location":
                  return "mem://warehouse/db/tbl-" + uuid;
                case "name":
                  return "mem.db.tbl";
                case "hashCode":
                  return System.identityHashCode(proxy);
                case "equals":
                  return proxy == args[0];
                case "toString":
                  return "commit table " + uuid;
                default:
                  throw new UnsupportedOperationException(method.getName());
              }
            });
  }

  private static boolean collectedWithin(WeakReference<?> reference, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (reference.get() != null && System.currentTimeMillis() < deadline) {
      System.gc();
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while waiting for a collection", e);
      }
    }

    return reference.get() == null;
  }

  private static List<StatisticsAnswer> answers(long snapshotId, String... puffins) {
    List<StatisticsAnswer> answers = Lists.newArrayList();
    for (String puffin : puffins) {
      answers.add(
          StatisticsAnswer.ofDataFiles(
              snapshotId, ImmutableList.of(), new SketchFile(puffin, 16L, 8L)));
    }

    return answers;
  }

  private String puffin() {
    puffinCount += 1;
    String location = staging + "sketch-" + puffinCount + ".puffin";
    io.addFile(location, "puffin".getBytes(StandardCharsets.UTF_8));
    return location;
  }

  private static void takeAllPermits(StatisticsLagStepLimiter limiter) {
    assertThat(limiter.acquire(() -> false)).isTrue();
    assertThat(limiter.acquire(() -> false)).isTrue();
  }

  private Set<String> inFlightWithin(StatisticsStepQueue queue, TableIdentifier id)
      throws Exception {
    return CompletableFuture.supplyAsync(() -> queue.inFlightSketchLocations(id))
        .get(2, TimeUnit.SECONDS);
  }

  private static void await(BooleanSupplier condition, String what) {
    long deadline = System.currentTimeMillis() + AWAIT_MS;
    while (!condition.getAsBoolean()) {
      assertThat(System.currentTimeMillis()).as("waiting until %s", what).isLessThan(deadline);
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while waiting until " + what, e);
      }
    }
  }

  private static Logger recordingLogger(List<String> records) {
    return (Logger)
        Proxy.newProxyInstance(
            Logger.class.getClassLoader(),
            new Class<?>[] {Logger.class},
            (proxy, method, args) -> {
              String name = method.getName();
              if (args != null
                  && args.length > 0
                  && args[0] instanceof String
                  && ImmutableSet.of("warn", "debug", "info", "error", "trace").contains(name)) {
                Object[] formatArgs;
                if (args.length == 2 && args[1] instanceof Object[]) {
                  formatArgs = (Object[]) args[1];
                } else {
                  formatArgs = Arrays.copyOfRange(args, 1, args.length);
                }

                records.add(
                    name.toUpperCase(Locale.ROOT)
                        + " "
                        + MessageFormatter.arrayFormat((String) args[0], formatArgs).getMessage());
              }

              return null;
            });
  }

  /**
   * The substituted step: records its inputs, returns what {@link #behave} says ({@code WRITTEN} by
   * default), waits on the latch of its own snapshot S and records two steps of one table running
   * at once.
   */
  private static class ScriptedStep extends StatisticsStep {
    // a lag step has no S: its latches are under this key
    private static final long HEAD = Long.MIN_VALUE;

    private final List<StatisticsStepInput> inputs = new CopyOnWriteArrayList<>();
    private final List<String> overlaps = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<TableIdentifier, AtomicInteger> running = Maps.newConcurrentMap();
    private final Map<Long, CountDownLatch> releases = Maps.newConcurrentMap();
    private final Map<Long, CountDownLatch> starts = Maps.newConcurrentMap();
    private volatile Function<StatisticsStepInput, Result> behavior =
        input -> new Result(Outcome.WRITTEN, null);

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      AtomicInteger count = running.computeIfAbsent(input.identifier(), id -> new AtomicInteger());
      if (count.incrementAndGet() > 1) {
        overlaps.add(input.identifier() + " at " + input.snapshotId());
      }

      try {
        long key = input.snapshotId() == null ? HEAD : input.snapshotId();
        CountDownLatch started = starts.get(key);
        if (started != null) {
          started.countDown();
        }

        CountDownLatch release = releases.get(key);
        if (release != null && !release.await(AWAIT_MS, TimeUnit.MILLISECONDS)) {
          overlaps.add("the latch of " + input.snapshotId() + " was never released");
        }

        return behavior.apply(input);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return new Result(Outcome.STOPPED, null);
      } finally {
        count.decrementAndGet();
      }
    }

    /**
     * Makes the step of S (or the lag step for {@code null}) wait until the returned latch opens.
     */
    CountDownLatch latch(Long snapshotId) {
      long key = snapshotId == null ? HEAD : snapshotId;
      starts.put(key, new CountDownLatch(1));
      CountDownLatch release = new CountDownLatch(1);
      releases.put(key, release);
      return release;
    }

    void awaitStarted(Long snapshotId) throws InterruptedException {
      CountDownLatch started = starts.get(snapshotId == null ? HEAD : snapshotId);
      assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS))
          .as("the step of %s started", snapshotId)
          .isTrue();
    }

    void releaseAll() {
      releases.values().forEach(CountDownLatch::countDown);
    }

    void behave(Function<StatisticsStepInput, Result> newBehavior) {
      this.behavior = newBehavior;
    }

    List<StatisticsStepInput> inputs() {
      return inputs;
    }

    List<Long> snapshotIds() {
      List<Long> ids = Lists.newArrayList();
      inputs.forEach(input -> ids.add(input.snapshotId()));
      return ids;
    }

    List<String> overlaps() {
      return overlaps;
    }
  }
}
