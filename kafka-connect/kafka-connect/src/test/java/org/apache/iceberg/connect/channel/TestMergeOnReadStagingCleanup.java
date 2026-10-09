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
import static org.apache.iceberg.connect.StatisticsAssertions.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.WarningCapture;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.connect.data.StatisticsAnswer;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The staging cleanup of merge-on-read with statistics: its source offers the tables of the queue
 * of statistics steps, keeps the Puffin files of their running and waiting tasks, and cleans up
 * nothing but {@code <stagingDirectory>/stats/}.
 *
 * <p>Passes run with a TTL of 0 over a listing of the in-memory files of the JVM ({@link
 * InMemoryStagingCleanup}), so any window in which nothing reaches a live Puffin becomes a delete
 * at once. Every pass also finds an orphan Puffin, whose delete shows that the pass ran.
 */
public class TestMergeOnReadStagingCleanup extends ChannelTestBase {
  private static final long AWAIT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final long COMMITTED_OFFSET = 10L;

  private final String stagingLocation = "mem://staging-" + UUID.randomUUID();
  private final HoldingStep step = new HoldingStep();
  private final ExecutorService pool = Executors.newCachedThreadPool();
  private final List<MergeOnReadTableCommitter> committers = Lists.newArrayList();
  private final InMemoryFileIO files = new InMemoryFileIO();
  private long offset = 0L;
  private int fileCount = 0;

  @BeforeEach
  public void stagingWithStatistics() {
    enableStatistics(TABLE_IDENTIFIER);
    when(config.stagingLocation()).thenReturn(stagingLocation);
    when(config.stagingOrphanTtlMs()).thenReturn(0L);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(0L);
  }

  @AfterEach
  public void stopCommitters() throws InterruptedException {
    step.releaseAll();
    committers.forEach(MergeOnReadTableCommitter::stop);
    pool.shutdownNow();
    assertThat(pool.awaitTermination(AWAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
  }

  @Test
  public void puffinOfStepHeldAfterItsTaskIsSetOutlivesPassAndIsReadWhenReleased()
      throws InterruptedException {
    MergeOnReadTableCommitter committer = committer();
    Cleanup cleanup = cleanup(committer);
    Held held = step.holdNext();
    DataWritten answer = response(rows(0, 10), "task-0");
    commit(committer, answer);
    held.awaitStarted();
    // a step without the Puffin would have to recount the data file, and fail
    table.io().deleteFile(answer.dataFiles().get(0).location());
    String orphan = orphanPuffin();

    cleanup.pass();

    assertThat(exists(orphan)).as("the pass ran and deleted the orphan").isFalse();
    assertThat(exists(answer.sketchFile().location()))
        .as("the Puffin of the running task")
        .isTrue();

    held.release();
    awaitStepsOver(committer, 1);
    assertThat(step.results()).extracting(Result::outcome).containsExactly(Outcome.WRITTEN);
    assertThat(recordOf(step.inputs().get(0).snapshotId()))
        .as("the NDV of the own snapshot, read from the Puffin")
        .isNotNull();
    assertThat(exists(answer.sketchFile().location())).as("deleted by the step").isFalse();
  }

  @Test
  public void waitingTaskMergedWithNewOneKeepsPuffinsOfBothThroughPass()
      throws InterruptedException {
    MergeOnReadTableCommitter committer = committer();
    Cleanup cleanup = cleanup(committer);
    Held held = step.holdNext();
    DataWritten running = response(rows(0, 10), "task-0");
    commit(committer, running);
    held.awaitStarted();
    DataWritten waiting = response(rows(10, 20), "task-0");
    commit(committer, waiting);
    DataWritten merged = response(rows(20, 30), "task-0");
    commit(committer, merged);
    // a table has at most one waiting task: the two waiting tasks are one now
    assertThat(committer.inFlightSketchLocations(TABLE_IDENTIFIER))
        .containsExactlyInAnyOrder(
            running.sketchFile().location(),
            waiting.sketchFile().location(),
            merged.sketchFile().location());
    table.io().deleteFile(waiting.dataFiles().get(0).location());
    table.io().deleteFile(merged.dataFiles().get(0).location());
    String orphan = orphanPuffin();

    cleanup.pass();

    assertThat(exists(orphan)).as("the pass ran and deleted the orphan").isFalse();
    assertThat(exists(running.sketchFile().location())).as("of the running task").isTrue();
    assertThat(exists(waiting.sketchFile().location())).as("of the waiting task").isTrue();
    assertThat(exists(merged.sketchFile().location())).as("of the task merged into it").isTrue();

    held.release();
    awaitStepsOver(committer, 2);
    assertThat(step.results())
        .extracting(Result::outcome)
        .containsExactly(Outcome.WRITTEN, Outcome.WRITTEN);
    StatisticsStepInput mergedInput = step.inputs().get(1);
    assertThat(mergedInput.answers())
        .extracting(StatisticsAnswer::sketchFile)
        .extracting(SketchFile::location)
        .containsExactlyInAnyOrder(waiting.sketchFile().location(), merged.sketchFile().location());
    assertThat(recordOf(mergedInput.snapshotId()))
        .as("the NDV of the merged task, read from both Puffin files")
        .isNotNull();
    assertNoStrayPuffins(statsDirectory(), ImmutableList.of());
  }

  @Test
  public void stepThatDeletesItsPuffinWhilePassWaitsOnPoolCancelsNothing()
      throws InterruptedException {
    MergeOnReadTableCommitter committer = committer();
    List<Runnable> waitingOnThePool = new CopyOnWriteArrayList<>();
    Cleanup cleanup = new Cleanup(committer.stagingCleanupSource(), waitingOnThePool::add);
    Held held = step.holdNext();
    DataWritten answer = response(rows(0, 10), "task-0");
    commit(committer, answer);
    held.awaitStarted();
    String orphan = orphanPuffin();

    try (WarningCapture warnings = WarningCapture.capture()) {
      // the pass takes the Puffin of the running task as live
      cleanup.pass();
      assertThat(waitingOnThePool).as("the pass waits on the pool").hasSize(1);
      held.release();
      awaitStepsOver(committer, 1);
      assertThat(exists(answer.sketchFile().location())).as("deleted by the step").isFalse();
      waitingOnThePool.forEach(Runnable::run);

      assertThat(exists(orphan))
          .as("a live file that is gone by the listing does not cancel the pass")
          .isFalse();
      assertThat(cleanup.scheduler.hasWarnedOfCleanupFailure(reference()))
          .as("no WARN of a failed read of the persisted state")
          .isFalse();
      assertThat(warnings.lines()).as("no WARN of a cancelled or a failed pass").isEmpty();
    }
  }

  @Test
  public void mergeOnReadCleansUpOnlyIntermediatePuffinFilesOfItsStagingDirectory() {
    MergeOnReadTableCommitter committer = committer();
    Cleanup cleanup = cleanup(committer);
    commit(committer, response(rows(0, 10), "task-0"));
    awaitStepsOver(committer, 1);
    // copy-on-write files of a change set left unfinished by a switch to merge-on-read, as old as
    // the orphan: only the pointer in the summary, which merge-on-read does not read, reaches them
    String manifest =
        write(
            ChangeSetManifest.location(
                stagingLocation, reference(), CONNECT_CONSUMER_GROUP_ID, UUID.randomUUID()));
    String staged = write(stagingDirectory() + "/task-0/" + UUID.randomUUID() + "/0.parquet");
    String normalized =
        write(stagingDirectory() + "/_normalized/" + UUID.randomUUID() + ".parquet");
    String orphan = orphanPuffin();

    cleanup.pass();

    assertThat(exists(orphan)).as("the Puffin nothing reaches").isFalse();
    assertThat(exists(manifest)).as("the manifest").isTrue();
    assertThat(exists(staged)).as("the staged file").isTrue();
    assertThat(exists(normalized)).as("the normalized file").isTrue();
    CleanupTarget target = committer.stagingCleanupSource().snapshot(reference());
    assertThat(target.cleanupPrefix()).isEqualTo(statsDirectory() + "/");
    assertThat(target.branch()).isNull();
    assertThat(target.tableUuid()).isEqualTo(table.uuid());
  }

  @Test
  public void onlyStandaloneCommitterWithStatisticsInUseHasSourceOfCleanup() {
    assertThat(committer().stagingCleanupSource()).as("statistics in use").isNotNull();
    assertThat(committer().stagingCleanupSource().tables())
        .as("no table before its first task")
        .isEmpty();
    assertThat(committer(MergeOnReadTableCommitter.Mode.TAIL).stagingCleanupSource())
        .as("the tail of copy-on-write: its coordinator asks copy-on-write")
        .isNull();

    when(config.statisticsInUse()).thenReturn(false);
    assertThat(committer().stagingCleanupSource()).as("statistics off").isNull();
  }

  @Test
  public void coordinatorOfMergeOnReadHasStagingCleanupOnlyWithStatisticsInUse() {
    assertThat(coordinatorHasStagingCleanup()).as("statistics in use").isTrue();

    when(config.statisticsInUse()).thenReturn(false);
    assertThat(coordinatorHasStagingCleanup())
        .as("statistics off: nothing is written to staging")
        .isFalse();
  }

  @Test
  public void afterFailuresOfMergeOnReadPassLeavesNoStrayPuffin() throws InterruptedException {
    MergeOnReadTableCommitter before = committer();
    // written, and the task failed before it sent the response
    String unsent = response(rows(0, 10), "task-1").sketchFile().location();
    // committed; then the same response comes again, with a Puffin of its own, and is filtered
    DataWritten committed = response(rows(10, 20), "task-0");
    long committedOffset = commit(before, committed);
    awaitStepsOver(before, 1);
    DataWritten again = response(rows(10, 20), "task-0");
    assertThat(before.commit(request(committedOffset, again)).consumed())
        .as("filtered as committed")
        .hasSize(1);
    // stopped with a running task and a waiting one
    Held held = step.holdNext();
    DataWritten running = response(rows(20, 30), "task-0");
    commit(before, running);
    held.awaitStarted();
    DataWritten waiting = response(rows(30, 40), "task-0");
    commit(before, waiting);
    before.stop();
    held.release();
    await("the running step sees the stop", () -> !before.hasStatisticsStep(TABLE_IDENTIFIER));
    assertThat(
            Arrays.asList(
                unsent,
                again.sketchFile().location(),
                running.sketchFile().location(),
                waiting.sketchFile().location()))
        .as("left behind")
        .allMatch(this::exists);

    // a restart: the table joins the cleanup with its first task
    MergeOnReadTableCommitter after = committer();
    Cleanup cleanup = cleanup(after);
    cleanup.pass();
    assertThat(exists(unsent)).as("no table to clean up before the first task").isTrue();
    DataWritten next = response(rows(40, 50), "task-0");
    commit(after, next);
    awaitStepsOver(after, 3);

    cleanup.pass();

    assertNoStrayPuffins(statsDirectory(), after.inFlightSketchLocations(TABLE_IDENTIFIER));
  }

  @Test
  public void puffinSharedByResponsesInTwoTasksIsDeletedByFirstStepAndLeavesCleanupNothing() {
    // responses sharing a Puffin in different commits: one writer, a schema change, and the
    // responses of the cycle cut by a partial commit
    MergeOnReadTableCommitter committer = committer();
    Cleanup cleanup = cleanup(committer);
    List<Record> first = rows(0, 10);
    List<Record> second = rows(10, 20);
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER,
            table,
            stagingDirectory(),
            "task-0",
            STATISTICS_LG_K,
            null,
            false,
            SCHEMA);
    first.forEach(sketches::update);
    second.forEach(sketches::update);
    UUID commitId = UUID.randomUUID();
    SketchFile shared = sketches.write(commitId);

    commit(committer, dataWritten(commitId, writeDataFile(first), shared));
    awaitStepsOver(committer, 1);
    assertThat(exists(shared.location())).as("deleted by the first step").isFalse();
    commit(committer, dataWritten(commitId, writeDataFile(second), shared));
    awaitStepsOver(committer, 2);
    assertThat(step.results())
        .extracting(Result::outcome)
        .as("the second recounts its data file")
        .containsExactly(Outcome.WRITTEN, Outcome.WRITTEN);
    assertNoStrayPuffins(statsDirectory(), ImmutableList.of());
    String orphan = orphanPuffin();
    Set<String> before = filesUnder(stagingDirectory());

    cleanup.pass();

    assertThat(filesUnder(stagingDirectory()))
        .as("the pass deletes the orphan and nothing else")
        .isEqualTo(Sets.difference(before, Sets.newHashSet(orphan)));
  }

  @Test
  public void tableRejectedForUnfinishedChangeSetJoinsCleanupOfItsStatsDirectory() {
    MergeOnReadTableCommitter committer = committer();
    Cleanup cleanup = cleanup(committer);
    UUID changeSetId = UUID.randomUUID();
    connectorAppend(changeSetId);
    String manifest =
        write(
            ChangeSetManifest.location(
                stagingLocation, reference(), CONNECT_CONSUMER_GROUP_ID, changeSetId));
    String staged = write(stagingDirectory() + "/task-0/" + UUID.randomUUID() + "/0.parquet");
    DataWritten answer = response(rows(0, 10), "task-0");
    TableCommitRequest request = request(COMMITTED_OFFSET, answer);

    CommitResult result = committer.commit(request);

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(result.consumed()).as("the responses stay buffered").isEmpty();
    assertThat(committer.stagingCleanupSource().tables())
        .as("joined the cleanup before the refusal")
        .containsExactly(reference());
    String orphan = orphanPuffin();

    cleanup.pass(request.envelopes());

    assertThat(exists(orphan)).as("the Puffin nothing reaches").isFalse();
    assertThat(exists(answer.sketchFile().location())).as("of the buffered response").isTrue();
    assertThat(exists(manifest)).as("the manifest of the change set").isTrue();
    assertThat(exists(staged)).as("the staged file of the change set").isTrue();
  }

  @Test
  public void tableRejectedForRowChangesWrittenAboveCommittedOffsetsJoinsCleanup() {
    MergeOnReadTableCommitter committer = committer();
    Cleanup cleanup = cleanup(committer);
    connectorAppend(null);
    String staged = write(stagingDirectory() + "/task-0/" + UUID.randomUUID() + "/0.parquet");
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER,
            table,
            stagingDirectory(),
            "task-0",
            STATISTICS_LG_K,
            null,
            false,
            SCHEMA);
    rows(0, 10).forEach(sketches::update);
    UUID commitId = UUID.randomUUID();
    SketchFile sketch = sketches.write(commitId);
    RowChangesWritten changes =
        new RowChangesWritten(
            commitId,
            reference(),
            "task-0",
            ImmutableList.of(SRC_TOPIC_NAME),
            ImmutableList.of(
                new StagedChangeFile(staged, 10L, 10L, 0, 1, ImmutableMap.of(), ImmutableMap.of())),
            sketch);
    List<Envelope> buffered =
        ImmutableList.of(
            new Envelope(new Event(CONNECT_CONSUMER_GROUP_ID, changes), 0, COMMITTED_OFFSET));

    CommitResult result =
        committer.commit(
            new TableCommitRequest(
                reference(), buffered, ImmutableMap.of(0, COMMITTED_OFFSET + 1), commitId, null));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(committer.stagingCleanupSource().tables())
        .as("joined the cleanup before the refusal")
        .containsExactly(reference());
    String orphan = orphanPuffin();

    cleanup.pass(buffered);

    assertThat(exists(orphan)).as("the Puffin nothing reaches").isFalse();
    assertThat(exists(sketch.location())).as("of the buffered response").isTrue();
    assertThat(exists(staged)).as("the staged file").isTrue();
  }

  @Test
  public void tableGoneWhenRequestWithResponsesLoadsItLeavesCleanup() {
    MergeOnReadTableCommitter committer = committer();
    commit(committer, response(rows(0, 10), "task-0"));
    awaitStepsOver(committer, 1);
    assertThat(committer.stagingCleanupSource().tables()).containsExactly(reference());
    // written before the drop: the writer's Puffin needs the table
    DataWritten late = response(rows(10, 20), "task-0");
    assertThat(catalog.dropTable(TABLE_IDENTIFIER, false)).isTrue();

    commit(committer, late);

    assertThat(committer.stagingCleanupSource().tables())
        .as("no stale Table to clean up")
        .isEmpty();
  }

  @Test
  public void rejectedTableDoesNotJoinCleanupWithStatisticsOff() {
    when(config.statisticsInUse()).thenReturn(false);
    MergeOnReadTableCommitter committer = committer();
    connectorAppend(UUID.randomUUID());

    CommitResult result =
        committer.commit(request(COMMITTED_OFFSET, response(rows(0, 10), "task-0")));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(committer.stagingCleanupSource()).as("no cleanup without statistics").isNull();
    // a source is stateless over the committer's queue: the one it would give shows the queue
    when(config.statisticsInUse()).thenReturn(true);
    assertThat(committer.stagingCleanupSource().tables())
        .as("the queue remembered nothing")
        .isEmpty();
  }

  @Test
  public void rejectedTableWithStatisticsOffForItDoesNotJoinCleanup() {
    when(config.tableConfig(TABLE_IDENTIFIER.toString())).thenReturn(mock(TableSinkConfig.class));
    MergeOnReadTableCommitter committer = committer();
    connectorAppend(UUID.randomUUID());

    CommitResult result =
        committer.commit(request(COMMITTED_OFFSET, response(rows(0, 10), "task-0")));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(committer.stagingCleanupSource().tables())
        .as("no Puffin of its own to clean up")
        .isEmpty();
  }

  private MergeOnReadTableCommitter committer() {
    return committer(MergeOnReadTableCommitter.Mode.STANDALONE);
  }

  private MergeOnReadTableCommitter committer(MergeOnReadTableCommitter.Mode mode) {
    MergeOnReadTableCommitter committer =
        new MergeOnReadTableCommitter(
            catalog,
            config,
            MetadataEvents.NOOP,
            event -> {},
            pool,
            new StatisticsHealth(TimeUnit.HOURS.toMillis(1)),
            step,
            new StatisticsLag(),
            mode);
    committers.add(committer);
    return committer;
  }

  /** The cleanup a coordinator runs over the committer, passes inline; none without a source. */
  private Cleanup cleanup(MergeOnReadTableCommitter committer) {
    return new Cleanup(committer.stagingCleanupSource(), Runnable::run);
  }

  /**
   * The staging cleanup as the coordinator builds it. {@code pass()} runs with an empty commit
   * buffer: the responses of a request leave it once {@code commit()} returns.
   */
  private final class Cleanup {
    private final StagingCleanupScheduler scheduler;

    private Cleanup(StagingCleanupSource source, Executor passes) {
      this.scheduler =
          source == null
              ? null
              : new StagingCleanupScheduler(
                  config, passes, InMemoryStagingCleanup.listing(source), () -> true, () -> false);
    }

    void pass() {
      pass(ImmutableList.of());
    }

    void pass(Collection<Envelope> buffered) {
      if (scheduler != null) {
        scheduler.maybeClean(System.currentTimeMillis(), () -> buffered);
      }
    }
  }

  private boolean coordinatorHasStagingCleanup() {
    AtomicReference<TableCommitter> created = new AtomicReference<>();
    Coordinator coordinator =
        new Coordinator(
            config,
            ImmutableList.of(),
            clientFactory,
            mock(SinkTaskContext.class),
            step,
            (send, sendInOneTransaction, transitionPool, health, stepOf) -> {
              created.set(
                  new MergeOnReadTableCommitter(
                      catalog,
                      config,
                      MetadataEvents.NOOP,
                      send,
                      transitionPool,
                      health,
                      stepOf,
                      new StatisticsLag(),
                      MergeOnReadTableCommitter.Mode.STANDALONE));
              return created.get();
            });
    try {
      return coordinator.hasStagingCleanup();
    } finally {
      coordinator.terminate();
    }
  }

  /** Commits the responses in one request, and returns the control topic offset of the last. */
  private long commit(MergeOnReadTableCommitter committer, DataWritten... responses) {
    long last = offset + responses.length - 1;
    CommitResult result = committer.commit(request(offset, responses));
    offset = last + 1;
    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(result.consumed()).hasSize(responses.length);
    return last;
  }

  private TableCommitRequest request(long firstOffset, DataWritten... responses) {
    List<Envelope> envelopes = Lists.newArrayList();
    long next = firstOffset;
    for (DataWritten response : responses) {
      envelopes.add(new Envelope(new Event(CONNECT_CONSUMER_GROUP_ID, response), 0, next));
      next += 1;
    }

    return new TableCommitRequest(
        reference(), envelopes, ImmutableMap.of(0, next), responses[0].commitId(), null);
  }

  /** Waits until {@code count} steps have ended and the committer's queue is empty. */
  private void awaitStepsOver(MergeOnReadTableCommitter committer, int count) {
    await(
        count + " steps over",
        () -> step.ended() >= count && !committer.hasStatisticsStep(TABLE_IDENTIFIER));
  }

  private StatisticsFile recordOf(long snapshotId) {
    table.refresh();
    return table.statisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  private TableReference reference() {
    return TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());
  }

  private String stagingDirectory() {
    return StagedChangeFileWriter.stagingDirectory(
        stagingLocation, reference(), CONNECT_CONSUMER_GROUP_ID);
  }

  private String statsDirectory() {
    return stagingDirectory() + "/" + ColumnSketchCollector.STATS_DIR;
  }

  private String orphanPuffin() {
    return write(
        statsDirectory() + "/" + UUID.randomUUID() + "-task-9-" + UUID.randomUUID() + ".puffin");
  }

  private String write(String location) {
    try (OutputStream out = files.newOutputFile(location).createOrOverwrite()) {
      out.write(location.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return location;
  }

  /**
   * A data commit of this connector, with its committed offset {@link #COMMITTED_OFFSET} and, if
   * given, the pointer of a change set left unfinished by a switch from copy-on-write.
   */
  private void connectorAppend(UUID changeSetId) {
    AppendFiles append =
        table
            .newAppend()
            .appendFile(writeDataFile(rows(100, 110)))
            .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":" + COMMITTED_OFFSET + "}");
    if (changeSetId != null) {
      append.set(ChangeSetStore.CHANGE_SET_ID_PROP, changeSetId.toString());
    }

    append.commit();
  }

  private boolean exists(String location) {
    return files.fileExists(location);
  }

  private static Set<String> filesUnder(String directory) {
    return StreamSupport.stream(
            new InMemoryStagingCleanup.ListingInMemoryFileIO()
                .listPrefix(directory + "/")
                .spliterator(),
            false)
        .map(FileInfo::location)
        .collect(Collectors.toSet());
  }

  /** A writer's response: the rows in a data file, and their Puffin under the staging directory. */
  private DataWritten response(List<Record> rows, String taskId) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER,
            table,
            stagingDirectory(),
            taskId,
            STATISTICS_LG_K,
            null,
            false,
            SCHEMA);
    rows.forEach(sketches::update);
    UUID commitId = UUID.randomUUID();
    SketchFile sketch = sketches.write(commitId);
    assertThat(sketch).as("the writer's Puffin").isNotNull();
    return dataWritten(commitId, writeDataFile(rows), sketch);
  }

  private DataWritten dataWritten(UUID commitId, DataFile file, SketchFile sketch) {
    return new DataWritten(
        StructType.of(),
        commitId,
        reference(),
        ImmutableList.of(file),
        ImmutableList.of(),
        ImmutableList.of(SRC_TOPIC_NAME),
        sketch);
  }

  private DataFile writeDataFile(List<Record> rows) {
    fileCount += 1;
    table.refresh();
    OutputFileFactory outputs = OutputFileFactory.builderFor(table, 1, fileCount).build();
    EncryptedOutputFile output = outputs.newOutputFile();
    try {
      DataWriter<Record> writer =
          new GenericFileWriterFactory.Builder(table)
              .dataSchema(SCHEMA)
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

  private static List<Record> rows(long from, long to) {
    List<Record> rows = Lists.newArrayList();
    for (long id = from; id < to; id++) {
      Record row = GenericRecord.create(SCHEMA);
      row.setField("id", id);
      row.setField("data", "data-" + id);
      row.setField("date", "2026-09-" + (10 + id % 5));
      rows.add(row);
    }

    return rows;
  }

  private static final class Held {
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    void awaitStarted() throws InterruptedException {
      assertThat(started.await(AWAIT_MS, TimeUnit.MILLISECONDS)).as("the step started").isTrue();
    }

    void release() {
      release.countDown();
    }
  }

  /** The real statistics step, which can hold the next step on a latch and records what it did. */
  private static final class HoldingStep extends StatisticsStep {
    private final List<StatisticsStepInput> inputs = new CopyOnWriteArrayList<>();
    private final List<Result> results = new CopyOnWriteArrayList<>();
    private final List<Held> helds = new CopyOnWriteArrayList<>();
    private final AtomicReference<Held> holdNext = new AtomicReference<>();

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      Held held = holdNext.getAndSet(null);
      if (held != null) {
        held.started.countDown();
        try {
          held.release.await(AWAIT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      Result result = super.run(input);
      results.add(result);
      return result;
    }

    Held holdNext() {
      Held held = new Held();
      helds.add(held);
      holdNext.set(held);
      return held;
    }

    void releaseAll() {
      helds.forEach(Held::release);
    }

    int ended() {
      return results.size();
    }

    List<StatisticsStepInput> inputs() {
      return inputs;
    }

    List<Result> results() {
      return results;
    }
  }
}
