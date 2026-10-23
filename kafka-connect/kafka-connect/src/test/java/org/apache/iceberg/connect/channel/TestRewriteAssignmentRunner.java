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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.AffectedFilePlanner;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetNormalizer;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.PlanResult;
import org.apache.iceberg.connect.data.copyonwrite.RewriteAssigner;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Who owns the replacement files a worker writes, and what happens to them when nobody will commit
 * them.
 *
 * <p>Up to the first chunk they are the worker's: a rewrite the coordinator gave up on while it
 * ran, or one that failed before it answered, leaves files the coordinator has no way of reaching:
 * it throws a stale answer away without reading it, and never sees one that was not sent. So the
 * worker deletes them.
 *
 * <p>From the first chunk on they are not: a send that throws may still have delivered the chunk
 * ({@code commitTransaction} raises a timeout on a transaction that committed), so the coordinator
 * may hold the answer and commit the slice. It deletes what it collected if it cancels the slice
 * instead, and {@code remove_orphan_files} covers the rest.
 *
 * <p>And how many of them a task may be writing at once: {@code rewrite-threads} is a limit on the
 * task, shared by every assignment it runs.
 */
public class TestRewriteAssignmentRunner {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);

  private static final String ONCE_EACH =
      "the table after the slice: every row once, whichever task wrote it";

  /** The rows of the table once the slice of {@link #rewriteByThreeTasks} is committed. */
  private static final List<String> REWRITTEN =
      List.of("1:a2", "3:c2", "4:d", "5:e2", "6:f", "7:g", "8:h");

  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private RewriteAssignmentRunner assignmentRunner;

  /**
   * The drain the assignments of a test belong to, unless it names another one: slices of one drain
   * carry one commit id, and it is only a different one that says the coordinator has restarted.
   */
  private UUID drainCommitId;

  @BeforeEach
  public void before() {
    drainCommitId = UUID.randomUUID();
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
    appendRows(table, row(1L, "a"), row(2L, "b"));

    config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("cg-connect");
    when(config.taskId()).thenReturn("task-0");
    // one thread, which is the setting the livelock needs: a superseded rewrite that runs anyway
    // holds up the very slice that replaced it
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
    // longer than any of these rewrites: the queue check is only under test where it is set short
    when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(TimeUnit.MINUTES.toMillis(1));
    when(config.controlMessageMaxBytes()).thenReturn(1024 * 1024);
  }

  @AfterEach
  public void after() throws IOException {
    if (assignmentRunner != null) {
      assignmentRunner.stop();
    }
    catalog.close();
  }

  @Test
  public void testAnAssignmentAlreadySupersededIsNeverStarted() {
    UUID changeSetId = UUID.randomUUID();
    RewriteAssigned live = assignment(changeSetId, 1);
    RewriteAssigned superseded = assignment(changeSetId, 0);
    RewriteAssigned next = assignment(changeSetId, 2);

    AtomicInteger loads = new AtomicInteger();
    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch gate = new CountDownLatch(1);
    Catalog counting = mock(Catalog.class);
    when(counting.loadTable(any()))
        .thenAnswer(
            invocation -> {
              loads.incrementAndGet();
              running.countDown();
              // holds the first rewrite inside the pool's only thread, so the assignments below
              // are queued behind a rewrite that is genuinely in flight
              gate.await(10, TimeUnit.SECONDS);
              return table;
            });

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            counting,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(live, live.assignments().get(0));
    await(running);
    // the coordinator timed slice 1 out and handed out 2; the redelivery of slice 0 arriving
    // afterwards must not un-supersede anything either
    assignmentRunner.submit(superseded, superseded.assignments().get(0));
    assignmentRunner.submit(next, next.assignments().get(0));
    gate.countDown();

    await(answered);
    // nothing else is in flight: the pool has one thread and slice 2 was the last thing on it

    // slice 1 was already running and stops at its first check without an answer: slice 2 is the
    // live one, and the coordinator would throw an answer to slice 1 away unread. Slice 0 never
    // even loaded the table
    assertThat(loads.get()).as("the superseded slice never loaded the table").isEqualTo(2);
    assertThat(answers).extracting(RewriteComplete::sliceSeq).containsExactly(2);
  }

  @Test
  public void testASupersededMarkNeverMovesBackwards() {
    UUID changeSetId = UUID.randomUUID();
    RewriteAssigned first = assignment(changeSetId, 0);
    RewriteAssigned second = assignment(changeSetId, 1);
    RewriteAssigned other = assignment(UUID.randomUUID(), 0);

    assignmentRunner = new RewriteAssignmentRunner(mock(Catalog.class), config, event -> {});

    assignmentRunner.remember(second);
    assignmentRunner.remember(first);

    assertThat(assignmentRunner.superseded(first)).isTrue();
    assertThat(assignmentRunner.superseded(second)).isFalse();

    // a new change set supersedes the old one whole, whatever the slice numbers say
    assignmentRunner.remember(other);
    assertThat(assignmentRunner.superseded(second)).isTrue();
    assertThat(assignmentRunner.superseded(other)).isFalse();
  }

  /**
   * A drain that is resumed numbers its slices from zero again, so the slice number alone does not
   * tell the attempt of the coordinator that died from the attempt of the one that took over. The
   * commit id does, and an assignment carrying another one is the newer of the two: it can only
   * have been handed out by a drain that started after this task heard of this one.
   */
  @Test
  public void testAnAssignmentOfAResumedDrainSupersedesTheSameSliceOfThePreviousOne() {
    UUID changeSetId = UUID.randomUUID();
    RewriteAssigned byPrevious = assignment(changeSetId, 0, UUID.randomUUID());
    RewriteAssigned byResumed = assignment(changeSetId, 0, UUID.randomUUID());

    assignmentRunner = new RewriteAssignmentRunner(mock(Catalog.class), config, event -> {});

    assignmentRunner.remember(byPrevious);
    assignmentRunner.remember(byResumed);

    assertThat(assignmentRunner.superseded(byPrevious))
        .as("slice 0 of the drain that was cut short")
        .isTrue();
    assertThat(assignmentRunner.superseded(byResumed))
        .as("slice 0 of the drain that resumed it")
        .isFalse();
  }

  /**
   * The same as {@link #testAnAssignmentOfAResumedDrainSupersedesTheSameSliceOfThePreviousOne}, but
   * at slice numbers a plain numeric comparison would get backwards: a resumed drain always
   * supersedes the one it replaced, however far that one's slice count had climbed, because slice
   * numbers start from zero again in every drain and say nothing across them.
   */
  @Test
  public void testAResumedDrainSupersedesAFarAdvancedPreviousOneEvenAtItsOwnSliceZero() {
    UUID changeSetId = UUID.randomUUID();
    RewriteAssigned byPrevious = assignment(changeSetId, 3, UUID.randomUUID());
    RewriteAssigned byResumed = assignment(changeSetId, 0, UUID.randomUUID());

    assignmentRunner = new RewriteAssignmentRunner(mock(Catalog.class), config, event -> {});

    assignmentRunner.remember(byPrevious);
    assignmentRunner.remember(byResumed);

    assertThat(assignmentRunner.superseded(byResumed))
        .as("slice 0 of the resumed drain, even though the previous one reached slice 3")
        .isFalse();
  }

  /**
   * The same, at the rewrite rather than at the mark: the attempt of the coordinator that died is
   * dropped as soon as the resumed drain's assignment arrives, and only the live attempt answers:
   * an answer to the dead one would be counted as an answer to the live slice, whose keys it does
   * not own.
   */
  @Test
  public void testARewriteSupersededByAResumedDrainAnswersNothing() {
    UUID changeSetId = UUID.randomUUID();
    RewriteAssigned byPrevious = assignment(changeSetId, 0, UUID.randomUUID());
    RewriteAssigned byResumed = assignment(changeSetId, 0, UUID.randomUUID());

    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch gate = new CountDownLatch(1);
    Catalog held = mock(Catalog.class);
    when(held.loadTable(any()))
        .thenAnswer(
            invocation -> {
              running.countDown();
              // holds the attempt of the dead coordinator inside the pool's only thread, so the
              // assignment of the resumed drain arrives while it is genuinely in flight
              gate.await(10, TimeUnit.SECONDS);
              return table;
            });

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            held,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(byPrevious, byPrevious.assignments().get(0));
    await(running);
    assignmentRunner.submit(byResumed, byResumed.assignments().get(0));
    gate.countDown();

    await(answered);
    // one thread, and the resumed drain's assignment was the last thing on it: nothing else is in
    // flight by now
    assertThat(answers)
        .as("only the drain that is running is answered")
        .extracting(RewriteComplete::commitId)
        .containsExactly(byResumed.commitId());
  }

  @Test
  public void testARewriteSupersededMidFileGoesNoFurther() {
    // a second file the slice touches: a superseded rewrite that kept going would have more to do
    appendRows(table, row(3L, "c"), row(4L, "d"));
    UUID changeSetId = UUID.randomUUID();
    List<StagedChangeFile> changes =
        staged(table, TABLE_REFERENCE, StagedChangeSchema.OP_UPDATE, row(1L, "a2"), row(3L, "c2"));
    RewriteAssigned superseded =
        assignment(table, TABLE_REFERENCE, changes, changeSetId, 0, drainCommitId);
    RewriteAssigned replacement =
        assignment(table, TABLE_REFERENCE, changes, changeSetId, 1, drainCommitId);

    List<String> steps = Lists.newCopyOnWriteArrayList();
    DataFileTrail trail = new DataFileTrail(table.io(), table.location() + "/data/", steps);
    Table trailed = probedBy(table, trail);
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER))
        .thenAnswer(
            invocation -> {
              steps.add("load");
              return trailed;
            });

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            loading,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(superseded, superseded.assignments().get(0));
    trail.awaitFirstRead();
    // the coordinator gave up on slice 0 and handed out slice 1 while slice 0 is inside a file
    assignmentRunner.submit(replacement, replacement.assignments().get(0));
    trail.release();

    await(answered);

    List<String> done = ImmutableList.copyOf(steps);
    // slice 1 loads the table only once slice 0 has let go of the pool's only thread
    List<String> bySuperseded = done.subList(1, done.lastIndexOf("load"));
    assertThat(bySuperseded)
        .as("what slice 0 did once slice 1 arrived: nothing past the read it was already in")
        .containsOnly(bySuperseded.get(0));
    assertThat(answers).extracting(RewriteComplete::sliceSeq).containsExactly(1);
  }

  @Test
  public void testAStopMidRewriteGoesNoFurtherAnswersNothingAndLeavesNoFileBehind() {
    // two files: the rewrite is held reading the second, with the first one's remainder written
    appendRows(table, row(3L, "c"), row(4L, "d"));
    List<StagedChangeFile> changes =
        staged(table, TABLE_REFERENCE, StagedChangeSchema.OP_UPDATE, row(1L, "a2"), row(3L, "c2"));
    RewriteAssigned assigned = assignment(table, TABLE_REFERENCE, changes, UUID.randomUUID(), 0);

    // and an assignment for another table queued behind it on the pool's only thread
    TableIdentifier otherIdentifier = TableIdentifier.of(NAMESPACE, "other");
    TableReference otherReference = TableReference.of("catalog", otherIdentifier);
    Table other = catalog.createTable(otherIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    appendRows(other, row(1L, "a"));
    RewriteAssigned queued =
        assignment(
            other,
            otherReference,
            staged(other, otherReference, StagedChangeSchema.OP_DELETE, row(1L, null)),
            UUID.randomUUID(),
            0);

    List<String> steps = Lists.newCopyOnWriteArrayList();
    DataFileTrail trail = new DataFileTrail(table.io(), table.location() + "/data/", steps, 2);
    Table trailed = probedBy(table, trail);
    AtomicInteger otherLoads = new AtomicInteger();
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER)).thenReturn(trailed);
    when(loading.loadTable(otherIdentifier))
        .thenAnswer(
            invocation -> {
              otherLoads.incrementAndGet();
              return other;
            });

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    assignmentRunner =
        new RewriteAssignmentRunner(
            loading, config, event -> answers.add((RewriteComplete) event.payload()));

    assignmentRunner.submit(assigned, assigned.assignments().get(0));
    assignmentRunner.submit(queued, queued.assignments().get(0));
    trail.awaitFirstRead();
    List<String> beforeStop = ImmutableList.copyOf(steps);
    // a rebalance revokes this task's partitions while the rewrite is inside the second file
    Thread stopping = stopInBackground(assignmentRunner);
    trail.release();
    awaitStopped(stopping);

    // FAILED would spend a commit retry and hand the slice out again at once, to the active tasks
    // that still include this task: silence lets the coordinator time it out and replan without it
    assertThat(answers).as("what a stopping task answered").isEmpty();
    assertThat(steps.subList(beforeStop.size(), steps.size()))
        .as("what the rewrite did once the task began to stop")
        .isEmpty();
    assertThat(otherLoads.get()).as("the queued assignment never loaded its table").isZero();

    List<String> written =
        beforeStop.stream()
            .filter(step -> step.startsWith("write "))
            .map(step -> step.substring("write ".length()))
            .collect(Collectors.toList());
    assertThat(written).as("the rewrite had written a replacement file").isNotEmpty();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    written.forEach(location -> assertThat(io.fileExists(location)).as(location).isFalse());
  }

  @Test
  public void testAStopMidAnswerLetsTheAnswerFinish() {
    // one file per chunk, two replacement files: the remainder of the two files, and the keys
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(1);
    appendRows(table, row(3L, "c"), row(4L, "d"));
    List<StagedChangeFile> changes =
        staged(table, TABLE_REFERENCE, StagedChangeSchema.OP_UPDATE, row(1L, "a2"), row(3L, "c2"));
    RewriteAssigned assigned = assignment(table, TABLE_REFERENCE, changes, UUID.randomUUID(), 0);

    AtomicReference<Thread> stopping = new AtomicReference<>();
    CountDownLatch stopStarted = new CountDownLatch(1);
    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    assignmentRunner =
        new RewriteAssignmentRunner(
            catalog,
            config,
            event -> {
              RewriteComplete payload = (RewriteComplete) event.payload();
              if (payload.status() == RewriteComplete.STATUS_OK && payload.chunkIndex() == 0) {
                // the task begins to stop while the first chunk is on its way
                stopping.set(stopInBackground(assignmentRunner));
                stopStarted.countDown();
              }
              if (payload.status() == RewriteComplete.STATUS_OK
                  && Thread.currentThread().isInterrupted()) {
                // what a producer does to a thread interrupted inside a transaction
                throw new InterruptException("interrupted while sending the answer");
              }
              answers.add(payload);
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));
    await(stopStarted);
    awaitStopped(stopping.get());

    // the coordinator may already hold the first chunk: the rest of the answer completes the slice
    assertThat(answers)
        .extracting(
            RewriteComplete::status, RewriteComplete::chunkIndex, RewriteComplete::chunkCount)
        .containsExactly(
            tuple(RewriteComplete.STATUS_OK, 0, 2), tuple(RewriteComplete.STATUS_OK, 1, 2));
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    answers.stream()
        .flatMap(answer -> answer.dataFiles().stream())
        .forEach(file -> assertThat(io.fileExists(file.location())).as(file.location()).isTrue());
  }

  @Test
  public void testAFailureWhileTheTaskStopsIsNotReported() {
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);

    AtomicReference<Thread> stopping = new AtomicReference<>();
    CountDownLatch stopStarted = new CountDownLatch(1);
    Catalog unavailable = mock(Catalog.class);
    when(unavailable.loadTable(any()))
        .thenAnswer(
            invocation -> {
              // the catalog fails as the task begins to stop, say because its own client closes
              stopping.set(stopInBackground(assignmentRunner));
              stopStarted.countDown();
              throw new IllegalStateException("catalog unavailable");
            });

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    assignmentRunner =
        new RewriteAssignmentRunner(
            unavailable, config, event -> answers.add((RewriteComplete) event.payload()));

    assignmentRunner.submit(assigned, assigned.assignments().get(0));
    await(stopStarted);
    awaitStopped(stopping.get());

    assertThat(answers).as("what a stopping task answered").isEmpty();
  }

  @Test
  public void testAMissingDataFileIsReportedAsAPermanentFailure() {
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);
    // gone from storage while the table still lists it: unless the coordinator hears that a retry
    // is pointless, it hands the same file out again every cycle
    for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      table.io().deleteFile(file.location());
    }

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            catalog,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(answered);

    assertThat(answers).hasSize(1);
    assertThat(answers.get(0).failureKind()).isEqualTo(RewriteComplete.FAILURE_PERMANENT);
    assertThat(answers.get(0).status()).isEqualTo(RewriteComplete.STATUS_FAILED);
  }

  /**
   * A chunk that has gone to the producer may already be at the coordinator, whatever the producer
   * then tells this task: {@code commitTransaction} raises a timeout or an interrupt on a
   * transaction that committed, and Kafka allows nothing but a retry of that very commit to find
   * out which it was. So from the first chunk on the files are the coordinator's to account for: it
   * either commits the slice, or deletes what it collected when it cancels it.
   */
  @Test
  public void testAnAnswerWhoseLastChunkFailedKeepsEveryFileItNamed() {
    // one file per chunk, two replacement files: the coordinator holds chunk 0 of 2 before the
    // send of chunk 1 fails
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(1);
    appendRows(table, row(3L, "c"), row(4L, "d"));
    List<StagedChangeFile> changes =
        staged(table, TABLE_REFERENCE, StagedChangeSchema.OP_UPDATE, row(1L, "a2"), row(3L, "c2"));
    RewriteAssigned assigned = assignment(table, TABLE_REFERENCE, changes, UUID.randomUUID(), 3);

    List<String> sent = Lists.newCopyOnWriteArrayList();
    List<Integer> failureKinds = Lists.newCopyOnWriteArrayList();
    CountDownLatch failed = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            catalog,
            config,
            event -> {
              RewriteComplete payload = (RewriteComplete) event.payload();
              if (payload.status() == RewriteComplete.STATUS_OK) {
                payload.dataFiles().forEach(file -> sent.add(file.location()));
                if (payload.chunkIndex() == payload.chunkCount() - 1) {
                  // the transaction of the last chunk committed and the producer did not hear so:
                  // the coordinator now holds the whole answer and will commit the slice
                  throw new TimeoutException("timeout expired while awaiting commitTransaction");
                }
                return;
              }
              failureKinds.add(payload.failureKind());
              failed.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(failed);

    assertThat(sent).as("both chunks of the answer went to the producer").hasSize(2);
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    sent.forEach(
        location ->
            assertThat(io.fileExists(location))
                .as("replacement file of an answer the coordinator may hold: %s", location)
                .isTrue());

    // FAILED is still sent: the send may equally have failed for good, and the coordinator
    // ignores a status that follows an answer it already counted as whole
    assertThat(failureKinds).containsExactly(RewriteComplete.FAILURE_RETRYABLE);
  }

  /**
   * The same where the answer is one chunk and the send of that very first chunk throws: a send
   * that threw is not a send that did not happen, and this task cannot tell the two apart. The
   * files stay, and the coordinator either commits them or deletes what it collected.
   */
  @Test
  public void testAnAnswerThatFailedOnItsOnlyChunkStillLeavesItsFilesAlone() {
    List<String> written = Lists.newCopyOnWriteArrayList();
    List<Integer> failureKinds = Lists.newCopyOnWriteArrayList();
    CountDownLatch failed = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            catalog,
            config,
            event -> {
              RewriteComplete payload = (RewriteComplete) event.payload();
              if (payload.status() == RewriteComplete.STATUS_OK) {
                // the producer raises on the only chunk of the answer, having sent it or not
                payload.dataFiles().forEach(file -> written.add(file.location()));
                throw new IllegalStateException("producer failed");
              }
              failureKinds.add(payload.failureKind());
              failed.countDown();
            });

    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);
    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(failed);

    // a producer that failed once may well not fail again
    assertThat(failureKinds).containsExactly(RewriteComplete.FAILURE_RETRYABLE);

    assertThat(written).as("the rewrite did produce replacement files").isNotEmpty();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    written.forEach(
        location ->
            assertThat(io.fileExists(location))
                .as("replacement file of a chunk that went to the producer: %s", location)
                .isTrue());
  }

  @Test
  public void testAnOversizedDescriptorFailsTheSliceBeforeAnyChunkIsSent() {
    // no descriptor of this table fits a message, and one descriptor cannot be split: the answer
    // is unsendable, and the rewrite of the next cycle would measure the very same bytes
    when(config.controlMessageMaxBytes()).thenReturn(64);
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);

    List<String> steps = Lists.newCopyOnWriteArrayList();
    // 0: nothing is held, the trail only records what the rewrite wrote
    DataFileTrail trail = new DataFileTrail(table.io(), table.location() + "/data/", steps, 0);
    Table trailed = probedBy(table, trail);
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER)).thenReturn(trailed);

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            loading,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(answered);

    assertThat(answers).hasSize(1);
    assertThat(answers.get(0).status()).isEqualTo(RewriteComplete.STATUS_FAILED);
    assertThat(answers.get(0).failureKind()).isEqualTo(RewriteComplete.FAILURE_PERMANENT);

    List<String> written = writtenFiles(steps);
    assertThat(written).as("the rewrite did produce replacement files").isNotEmpty();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    written.forEach(location -> assertThat(io.fileExists(location)).as(location).isFalse());
  }

  @Test
  public void testAChunkTheProducerRefusesForItsSizeIsAPermanentFailure() {
    List<Integer> failureKinds = Lists.newCopyOnWriteArrayList();
    CountDownLatch failed = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            catalog,
            config,
            event -> {
              RewriteComplete payload = (RewriteComplete) event.payload();
              if (payload.status() == RewriteComplete.STATUS_OK) {
                // a transactional producer records a rejected record and raises it, wrapped, out
                // of commitTransaction
                throw new KafkaException(
                    "Cannot execute transactional method because we are in an error state",
                    new RecordTooLargeException(
                        "The message is 2000000 bytes when serialized which is larger than "
                            + "1048576"));
              }
              failureKinds.add(payload.failureKind());
              failed.countDown();
            });

    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);
    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(failed);

    // the message is too large for the topic, and every retry sends the same bytes
    assertThat(failureKinds).containsExactly(RewriteComplete.FAILURE_PERMANENT);
  }

  @Test
  public void testAnErrorInTheRewriteIsAnsweredRatherThanLeftToTimeOut() {
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);
    Table unreadable =
        probedBy(table, new UnloadableCodec(table.io(), table.location() + "/data/"));
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER)).thenReturn(unreadable);

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            loading,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(answered);

    assertThat(answers).hasSize(1);
    assertThat(answers.get(0).status()).isEqualTo(RewriteComplete.STATUS_FAILED);
    // a class that would not initialize once will not initialize in this JVM at all: retrying it
    // every cycle only holds the drain, and the operator has to fix the host either way
    assertThat(answers.get(0).failureKind()).isEqualTo(RewriteComplete.FAILURE_PERMANENT);
  }

  @Test
  public void testAFailureThatCannotBeSentStaysInsideTheRewriteAssignmentRunner() {
    // the rewrite fails (its data file is gone) and the producer is down, so even FAILED cannot be
    // sent: a Runnable that throws prints to System.err past SLF4J and takes its thread with it
    for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      table.io().deleteFile(file.location());
    }
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);

    Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
    List<Throwable> escaped = Lists.newCopyOnWriteArrayList();
    CountDownLatch uncaught = new CountDownLatch(1);
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, error) -> {
          escaped.add(error);
          uncaught.countDown();
        });
    try {
      CountDownLatch attempted = new CountDownLatch(1);
      assignmentRunner =
          new RewriteAssignmentRunner(
              catalog,
              config,
              event -> {
                attempted.countDown();
                throw new IllegalStateException("producer failed");
              });

      assignmentRunner.submit(assigned, assigned.assignments().get(0));
      await(attempted);

      assertThat(awaitBriefly(uncaught))
          .as("what escaped the rewrite thread: " + escaped)
          .isFalse();
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(previous);
    }
  }

  @Test
  public void testAnAssignmentThatWaitedOutTheTimeoutInTheQueueIsNeverStarted() {
    // short enough for the test to wait it out, and far longer than an assignment that starts at
    // once takes to get going
    long timeoutMs = 500;
    when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(timeoutMs);

    // the assignment left waiting is for a second table: another one for this table would arrive
    // as the newer slice and be dropped as superseded, which is not what is under test here
    TableIdentifier queuedIdentifier = TableIdentifier.of(NAMESPACE, "queued");
    TableReference queuedReference = TableReference.of("catalog", queuedIdentifier);
    Table queuedTable =
        catalog.createTable(queuedIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    appendRows(queuedTable, row(1L, "a"), row(2L, "b"));

    RewriteAssigned running = assignment(UUID.randomUUID(), 0);
    RewriteAssigned queued = assignmentFor(queuedTable, queuedReference);

    DataFileTrail trail =
        new DataFileTrail(table.io(), table.location() + "/data/", Lists.newCopyOnWriteArrayList());
    Table trailed = probedBy(table, trail);
    CountDownLatch queuedLoaded = new CountDownLatch(1);
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER)).thenReturn(trailed);
    when(loading.loadTable(queuedIdentifier))
        .thenAnswer(
            invocation -> {
              queuedLoaded.countDown();
              return queuedTable;
            });

    BlockingQueue<RewriteComplete> answers = new LinkedBlockingQueue<>();
    assignmentRunner =
        new RewriteAssignmentRunner(
            loading, config, event -> answers.add((RewriteComplete) event.payload()));

    assignmentRunner.submit(running, running.assignments().get(0));
    trail.awaitFirstRead();
    // one rewrite thread, so this one waits behind the rewrite already inside a file. By the time
    // it is let through, the coordinator has timed its slice out and handed out a replacement
    assignmentRunner.submit(queued, queued.assignments().get(0));
    waitOut(3 * timeoutMs);
    trail.release();

    assertThat(answered(answers).tableReference().identifier())
        .as("the slice that ran answers")
        .isEqualTo(TABLE_IDENTIFIER);
    assertThat(awaitBriefly(queuedLoaded))
        .as("the assignment that waited out rewrite-timeout-ms does not even load its table")
        .isFalse();
    assertThat(answers).as("and answers nothing: the coordinator is not waiting for it").isEmpty();

    // and the next assignment for that table, which has waited for nothing, runs as usual: what is
    // refused is the wait, not the table it waited behind
    RewriteAssigned fresh = assignmentFor(queuedTable, queuedReference);
    assignmentRunner.submit(fresh, fresh.assignments().get(0));

    RewriteComplete answer = answered(answers);
    assertThat(answer.tableReference().identifier()).isEqualTo(queuedIdentifier);
    assertThat(answer.status()).isEqualTo(RewriteComplete.STATUS_OK);
  }

  @Test
  public void testRewriteThreadsBoundTheWholeTaskRatherThanEachAssignment() {
    int rewriteThreads = 2;
    when(config.copyOnWriteRewriteThreads()).thenReturn(rewriteThreads);

    // two tables drained at once: this one plans two files and splits into two chunks, the fixture
    // table plans one file and has a key to write back. Neither assignment needs more than two
    // threads on its own, and together they must not get more than two either
    TableIdentifier wideIdentifier = TableIdentifier.of(NAMESPACE, "wide");
    TableReference wideReference = TableReference.of("catalog", wideIdentifier);
    Table wide = catalog.createTable(wideIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    appendRows(wide, row(1L, "a"), row(2L, "b"));
    appendRows(wide, row(3L, "c"), row(4L, "d"));
    RewriteAssigned wideAssigned =
        assignment(
            wide,
            wideReference,
            staged(wide, wideReference, StagedChangeSchema.OP_DELETE, row(1L, null), row(3L, null)),
            UUID.randomUUID(),
            0);
    RewriteAssigned narrowAssigned = assignment(UUID.randomUUID(), 0);

    DataFileProbe probe =
        new DataFileProbe(
            table.io(), rewriteThreads, wide.location() + "/data/", table.location() + "/data/");
    Table probedWide = probedBy(wide, probe);
    Table probedNarrow = probedBy(table, probe);
    Catalog probed = mock(Catalog.class);
    when(probed.loadTable(wideIdentifier)).thenReturn(probedWide);
    when(probed.loadTable(TABLE_IDENTIFIER)).thenReturn(probedNarrow);

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(2);
    assignmentRunner =
        new RewriteAssignmentRunner(
            probed,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(wideAssigned, wideAssigned.assignments().get(0));
    assignmentRunner.submit(narrowAssigned, narrowAssigned.assignments().get(0));

    // both answer: an assignment waiting on its own chunks holds none of the task's threads, so two
    // of them waiting at once do not starve each other
    await(answered);
    assertThat(answers)
        .extracting(RewriteComplete::status)
        .containsExactly(RewriteComplete.STATUS_OK, RewriteComplete.STATUS_OK);
    assertThat(answers)
        .extracting(answer -> answer.tableReference().identifier())
        .containsExactlyInAnyOrder(wideIdentifier, TABLE_IDENTIFIER);

    assertThat(probe.mostAtOnce())
        .as("threads holding a data file open at once, across both assignments of the task")
        .isLessThanOrEqualTo(rewriteThreads);
    assertThat(probe.mostAtOnce())
        .as("threads holding a data file open at once: the limit is still used in full")
        .isEqualTo(rewriteThreads);
  }

  @Test
  public void testEveryChunkOfAnAnswerNumbersItselfAndNamesTheSliceItAnswers() {
    // one file per chunk, two replacement files: the remainder of the two files, and the keys
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(1);
    appendRows(table, row(3L, "c"), row(4L, "d"));
    List<StagedChangeFile> changes =
        staged(table, TABLE_REFERENCE, StagedChangeSchema.OP_UPDATE, row(1L, "a2"), row(3L, "c2"));
    UUID changeSetId = UUID.randomUUID();
    RewriteAssigned assigned = assignment(table, TABLE_REFERENCE, changes, changeSetId, 3);

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(2);
    assignmentRunner =
        new RewriteAssignmentRunner(
            catalog,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(answered);

    // 0..n-1 under one count: a coordinator holding {0, 1} of 2 knows the answer is whole, while
    // one holding {0} of 2 knows a message is still out. Every chunk claiming to be 1 of 1 would
    // have the slice committed on the first one, without the files of the rest
    assertThat(answers)
        .extracting(
            RewriteComplete::status, RewriteComplete::chunkIndex, RewriteComplete::chunkCount)
        .containsExactly(
            tuple(RewriteComplete.STATUS_OK, 0, 2), tuple(RewriteComplete.STATUS_OK, 1, 2));

    // and each of them names the attempt it answers: the coordinator counts chunks per task of the
    // current attempt, and a chunk that named another slice would be dropped as stale
    assertThat(answers)
        .extracting(
            RewriteComplete::commitId,
            RewriteComplete::changeSetId,
            RewriteComplete::sliceSeq,
            RewriteComplete::taskId,
            RewriteComplete::tableReference)
        .containsOnly(tuple(assigned.commitId(), changeSetId, 3, "task-0", TABLE_REFERENCE));

    assertThat(
            answers.stream()
                .flatMap(answer -> answer.dataFiles().stream())
                .map(DataFile::location)
                .collect(Collectors.toList()))
        .as("the chunks carry the whole answer, each file once")
        .doesNotHaveDuplicates()
        .hasSize(2);
  }

  @Test
  public void testThreeTasksWriteEveryKeyOfTheSliceExactlyOnce() {
    List<RewriteComplete> answers = rewriteByThreeTasks(catalog);

    // every task writes its block of the slice's keys, whether or not it was given files: with
    // each worker taking itself for the only owner, every changed key would come back three times
    assertThat(answers)
        .as("each task answers its own slice, whole, in one chunk")
        .extracting(RewriteComplete::taskId, RewriteComplete::status, RewriteComplete::chunkCount)
        .containsExactlyInAnyOrder(
            tuple("task-0", RewriteComplete.STATUS_OK, 1),
            tuple("task-1", RewriteComplete.STATUS_OK, 1),
            tuple("task-2", RewriteComplete.STATUS_OK, 1));
    assertThat(answers)
        .filteredOn(answer -> !answer.taskId().equals("task-0"))
        .as("a task without files still writes the keys of its block")
        .allSatisfy(answer -> assertThat(answer.dataFiles()).isNotEmpty());
    assertThat(committed(answers)).as(ONCE_EACH).containsExactlyInAnyOrderElementsOf(REWRITTEN);
  }

  @Test
  public void testTasksSplitTheSliceByTheIdentifierFieldsOfTheAssignment() {
    // this worker's handle is at a schema without the identifier fields the coordinator resolved
    // for the change set: the slice's keys, and so which of them are this task's, are the
    // coordinator's
    Schema unkeyed = new Schema(SCHEMA.columns());
    Table unkeyedHandle = spy(table);
    doReturn(unkeyed).when(unkeyedHandle).schema();
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER)).thenReturn(unkeyedHandle);

    List<RewriteComplete> answers = rewriteByThreeTasks(loading);

    assertThat(answers)
        .extracting(RewriteComplete::status)
        .containsOnly(RewriteComplete.STATUS_OK)
        .hasSize(3);
    assertThat(committed(answers)).as(ONCE_EACH).containsExactlyInAnyOrderElementsOf(REWRITTEN);
  }

  /**
   * One slice of six keys (updates of 1, 3 and 5, a delete of 2, inserts of 7 and 8) rewritten by
   * three tasks, owners 0, 1 and 2 of 3, each on a {@link RewriteAssignmentRunner} of its own. All
   * files of the plan go to {@code task-0}; the other two get none, and only their block of the
   * keys.
   */
  private List<RewriteComplete> rewriteByThreeTasks(Catalog workerCatalog) {
    appendRows(table, row(3L, "c"), row(4L, "d"), row(5L, "e"), row(6L, "f"));
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, TABLE_REFERENCE, ID_FIELDS, null, "cg-connect", "task-0");
    writer.write(writer.stagedRow(row(1L, "a2"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, 0));
    writer.write(writer.stagedRow(row(2L, "b"), StagedChangeSchema.OP_DELETE, "src-topic", 0, 1));
    writer.write(writer.stagedRow(row(3L, "c2"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, 2));
    writer.write(writer.stagedRow(row(5L, "e2"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, 3));
    writer.write(writer.stagedRow(row(7L, "g"), StagedChangeSchema.OP_INSERT, "src-topic", 0, 4));
    writer.write(writer.stagedRow(row(8L, "h"), StagedChangeSchema.OP_INSERT, "src-topic", 0, 5));
    RewriteAssigned planned =
        assignment(table, TABLE_REFERENCE, writer.complete(), UUID.randomUUID(), 0);
    Assignment allFiles = planned.assignments().get(0);
    assertThat(allFiles.files()).as("the plan has files to rewrite").isNotEmpty();

    int owners = 3;
    BlockingQueue<RewriteComplete> answers = new LinkedBlockingQueue<>();
    List<RewriteAssignmentRunner> tasks = Lists.newArrayList();
    try {
      for (int owner = 0; owner < owners; owner++) {
        String taskId = "task-" + owner;
        IcebergSinkConfig taskConfig = mock(IcebergSinkConfig.class);
        when(taskConfig.connectGroupId()).thenReturn("cg-connect");
        when(taskConfig.taskId()).thenReturn(taskId);
        when(taskConfig.copyOnWriteRewriteThreads()).thenReturn(1);
        when(taskConfig.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
        when(taskConfig.copyOnWriteRewriteTimeoutMs()).thenReturn(TimeUnit.MINUTES.toMillis(1));
        when(taskConfig.controlMessageMaxBytes()).thenReturn(1024 * 1024);

        Assignment mine =
            owner == 0
                ? new Assignment(taskId, ImmutableList.of(), allFiles.files())
                : new Assignment(taskId, ImmutableList.of(), ImmutableList.of());
        RewriteAssigned assigned =
            new RewriteAssigned(
                table.spec().partitionType(),
                planned.commitId(),
                planned.tableReference(),
                planned.changeSetId(),
                planned.sliceSeq(),
                planned.baseSnapshotId(),
                planned.normalizedRef(),
                ImmutableList.of(mine),
                owner,
                owners,
                planned.identifierFieldIds());

        RewriteAssignmentRunner task =
            new RewriteAssignmentRunner(
                workerCatalog, taskConfig, event -> answers.add((RewriteComplete) event.payload()));
        tasks.add(task);
        task.submit(assigned, mine);
      }

      List<RewriteComplete> received = Lists.newArrayList();
      for (int owner = 0; owner < owners; owner++) {
        received.add(answered(answers));
      }
      return received;
    } finally {
      tasks.forEach(RewriteAssignmentRunner::stop);
    }
  }

  /** The table's rows as {@code id:data} once the answers replace every file of the plan. */
  private List<String> committed(List<RewriteComplete> answers) {
    OverwriteFiles overwrite = table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue());
    answers.forEach(answer -> answer.dataFiles().forEach(overwrite::addFile));
    overwrite.commit();
    table.refresh();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      return Lists.newArrayList(rows).stream()
          .map(read -> read.getField("id") + ":" + read.getField("data"))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testAReplacementFileUnderAnotherSpecFailsTheSliceInsteadOfBeingSent() {
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 0);
    // this worker's handle is at a spec the coordinator's plan was not made against: rows carried
    // over keep the spec of the file they came from, and one response can only carry one partition
    // type. Encoding them under the current one would write one partition column's value under
    // another's name
    PartitionSpec evolved = PartitionSpec.builderFor(SCHEMA).withSpecId(1).build();
    List<String> steps = Lists.newCopyOnWriteArrayList();
    // 0: nothing is held, the trail only records what the rewrite wrote
    DataFileTrail trail = new DataFileTrail(table.io(), table.location() + "/data/", steps, 0);
    Table evolvedHandle = spy(table);
    doReturn(trail).when(evolvedHandle).io();
    doReturn(evolved).when(evolvedHandle).spec();
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(TABLE_IDENTIFIER)).thenReturn(evolvedHandle);

    List<RewriteComplete> answers = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            loading,
            config,
            event -> {
              answers.add((RewriteComplete) event.payload());
              answered.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(answered);

    assertThat(answers)
        .extracting(RewriteComplete::status)
        .as("the slice is failed rather than answered with files of two specs")
        .containsExactly(RewriteComplete.STATUS_FAILED);
    assertThat(answers.get(0).dataFiles()).isEmpty();

    List<String> written = writtenFiles(steps);
    assertThat(written).as("the rewrite did produce replacement files").isNotEmpty();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    written.forEach(location -> assertThat(io.fileExists(location)).as(location).isFalse());
  }

  @Test
  public void testATableThatCannotBeLoadedIsAnsweredRatherThanLeftToTimeOut() {
    RewriteAssigned assigned = assignment(UUID.randomUUID(), 2);
    Catalog unavailable = mock(Catalog.class);
    // the catalog is out of reach for a moment: the next cycle may well load the table
    when(unavailable.loadTable(any())).thenThrow(new IllegalStateException("catalog unavailable"));

    List<Event> events = Lists.newCopyOnWriteArrayList();
    CountDownLatch answered = new CountDownLatch(1);
    assignmentRunner =
        new RewriteAssignmentRunner(
            unavailable,
            config,
            event -> {
              events.add(event);
              answered.countDown();
            });

    assignmentRunner.submit(assigned, assigned.assignments().get(0));

    await(answered);

    assertThat(events).hasSize(1);
    RewriteComplete answer = (RewriteComplete) events.get(0).payload();
    assertThat(answer.status()).isEqualTo(RewriteComplete.STATUS_FAILED);
    assertThat(answer.failureKind()).isEqualTo(RewriteComplete.FAILURE_RETRYABLE);
    assertThat(answer.dataFiles()).isEmpty();
    assertThat(answer.chunkIndex()).isZero();
    assertThat(answer.chunkCount()).isEqualTo(1);
    assertThat(answer.sliceSeq()).isEqualTo(2);
    assertThat(answer.taskId()).isEqualTo("task-0");

    // there was no table to take a partition type from, and the event still has to build and
    // encode: an exception here leaves the failure unreported and the coordinator waiting out the
    // whole timeout for a slice that is already lost
    RewriteComplete decoded =
        (RewriteComplete) AvroUtil.decode(AvroUtil.encode(events.get(0))).payload();
    assertThat(decoded.status()).isEqualTo(RewriteComplete.STATUS_FAILED);
    assertThat(decoded.sliceSeq()).isEqualTo(2);
    assertThat(decoded.dataFiles()).isEmpty();
  }

  // -- fixtures ---------------------------------------------------------------------------------

  /** A real assignment: real staged change, real normalized file, real plan of the real table. */
  private RewriteAssigned assignment(UUID changeSetId, int sliceSeq) {
    return assignment(changeSetId, sliceSeq, drainCommitId);
  }

  /** The same slice of the same change set, handed out by the drain {@code commitId} names. */
  private RewriteAssigned assignment(UUID changeSetId, int sliceSeq, UUID commitId) {
    return assignment(table, TABLE_REFERENCE, update(1L, "a2"), changeSetId, sliceSeq, commitId);
  }

  static RewriteAssigned assignment(
      Table target,
      TableReference reference,
      List<StagedChangeFile> staged,
      UUID changeSetId,
      int sliceSeq) {
    return assignment(target, reference, staged, changeSetId, sliceSeq, UUID.randomUUID());
  }

  static RewriteAssigned assignment(
      Table target,
      TableReference reference,
      List<StagedChangeFile> staged,
      UUID changeSetId,
      int sliceSeq,
      UUID commitId) {
    long baseSnapshotId = target.currentSnapshot().snapshotId();
    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(target, ID_FIELDS, staged, null, Long.MAX_VALUE);
    StagedChangeFile normalizedRef =
        ChangeSetNormalizer.writeNormalized(
            target,
            ID_FIELDS,
            slice,
            target.location()
                + "/_staging/normalized-"
                + commitId
                + "-"
                + changeSetId
                + "-"
                + sliceSeq
                + ".avro");

    PlanResult plan = AffectedFilePlanner.plan(target, baseSnapshotId, slice, 1000, Long.MAX_VALUE);
    List<Assignment> assignments =
        RewriteAssigner.assign(
            plan.fileScanTasks(),
            ImmutableMap.of("task-0", ImmutableList.<TopicPartitionRef>of()),
            target.spec().partitionType());

    return new RewriteAssigned(
        target.spec().partitionType(),
        commitId,
        reference,
        changeSetId,
        sliceSeq,
        baseSnapshotId,
        normalizedRef,
        ImmutableList.of(assignments.get(0)),
        0,
        1,
        ImmutableList.copyOf(ID_FIELDS));
  }

  /** A fresh assignment of slice 0 of a new change set of another table. */
  private static RewriteAssigned assignmentFor(Table target, TableReference reference) {
    return assignment(
        target,
        reference,
        staged(target, reference, StagedChangeSchema.OP_UPDATE, row(1L, "a2")),
        UUID.randomUUID(),
        0);
  }

  /** The next answer sent, waiting for it to arrive. */
  private static RewriteComplete answered(BlockingQueue<RewriteComplete> answers) {
    try {
      RewriteComplete answer = answers.poll(30, TimeUnit.SECONDS);
      assertThat(answer).as("an answer was sent").isNotNull();
      return answer;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** Lets the clock run: the queue is timed against it, not against a step of the test. */
  private static void waitOut(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static List<String> writtenFiles(List<String> steps) {
    return steps.stream()
        .filter(step -> step.startsWith("write "))
        .map(step -> step.substring("write ".length()))
        .collect(Collectors.toList());
  }

  /** True if the latch counted down within a moment: for asserting that it does not. */
  private static boolean awaitBriefly(CountDownLatch latch) {
    try {
      return latch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /**
   * Runs {@code stop()} on a thread of its own, returning once it waits for the rewrites to finish:
   * by then it has done whatever it does to the rewrites in flight.
   */
  private static Thread stopInBackground(RewriteAssignmentRunner target) {
    Thread stopping = new Thread(target::stop, "stopping-task");
    stopping.start();
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (stopping.getState() != Thread.State.TIMED_WAITING) {
      assertThat(stopping.isAlive() || stopping.getState() == Thread.State.NEW)
          .as("stop() waits for the rewrites in flight")
          .isTrue();
      assertThat(System.nanoTime()).as("stop() is waiting by now").isLessThan(deadlineNanos);
      Thread.onSpinWait();
    }
    return stopping;
  }

  private static void awaitStopped(Thread stopping) {
    try {
      stopping.join(TimeUnit.SECONDS.toMillis(30));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
    assertThat(stopping.isAlive()).as("stop() returned").isFalse();
  }

  private List<StagedChangeFile> update(long id, String data) {
    return staged(table, TABLE_REFERENCE, StagedChangeSchema.OP_UPDATE, row(id, data));
  }

  static List<StagedChangeFile> staged(
      Table target, TableReference reference, int op, Record... tableRows) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(target, reference, ID_FIELDS, null, "cg-connect", "task-0");
    long offset = 0L;
    for (Record tableRow : tableRows) {
      writer.write(writer.stagedRow(tableRow, op, "src-topic", 0, offset++));
    }
    return writer.complete();
  }

  /** The same table, reading and writing through the probe. */
  static Table probedBy(Table target, FileIO probe) {
    Table probed = spy(target);
    doReturn(probe).when(probed).io();
    return probed;
  }

  /**
   * Records, in order, what happens to the data files under one location: {@code read <path>} as a
   * file is opened for reading, {@code write <path>} as one is created.
   *
   * <p>The first read is held until {@link #release()}, so a test can act while a rewrite is inside
   * a file for real.
   */
  static final class DataFileTrail implements FileIO {
    private final FileIO delegate;
    private final String dataLocation;
    private final List<String> steps;
    private final int heldRead;
    private final AtomicInteger reads = new AtomicInteger();
    private final CountDownLatch held = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);

    DataFileTrail(FileIO delegate, String dataLocation, List<String> steps) {
      this(delegate, dataLocation, steps, 1);
    }

    /** Holds the {@code heldRead}-th read of a data file instead of the first. */
    DataFileTrail(FileIO delegate, String dataLocation, List<String> steps, int heldRead) {
      this.delegate = delegate;
      this.dataLocation = dataLocation;
      this.steps = steps;
      this.heldRead = heldRead;
    }

    /** Returns once a data file is being opened for reading; that read waits for release. */
    void awaitFirstRead() {
      await(held);
    }

    void release() {
      released.countDown();
    }

    @Override
    public InputFile newInputFile(String path) {
      read(path);
      return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      read(path);
      return delegate.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      if (path.startsWith(dataLocation)) {
        steps.add("write " + path);
      }
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

    private void read(String path) {
      if (!path.startsWith(dataLocation)) {
        return;
      }
      steps.add("read " + path);
      if (reads.incrementAndGet() != heldRead) {
        return;
      }
      held.countDown();
      try {
        released.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * A table whose data files cannot be read at all: what a compression codec whose native library
   * will not load on this host looks like from inside a rewrite.
   */
  private static final class UnloadableCodec implements FileIO {
    private final FileIO delegate;
    private final String dataLocation;

    private UnloadableCodec(FileIO delegate, String dataLocation) {
      this.delegate = delegate;
      this.dataLocation = dataLocation;
    }

    @Override
    public InputFile newInputFile(String path) {
      refuse(path);
      return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      refuse(path);
      return delegate.newInputFile(path, length);
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

    private void refuse(String path) {
      if (path.startsWith(dataLocation)) {
        throw new NoClassDefFoundError("Could not initialize class org.xerial.snappy.Snappy");
      }
    }
  }

  /**
   * Counts the threads that hold a data file of the probed tables open, for reading or writing.
   *
   * <p>A thread that starts to hold one is held back until more than {@code limit} threads do, or
   * for a second: work that is able to overlap then does overlap, whatever the scheduling.
   */
  private static final class DataFileProbe implements FileIO {
    private static final long HOLD_MS = 1_000;

    private final FileIO delegate;
    private final int limit;
    private final List<String> dataLocations;
    private final Map<Thread, Integer> openStreams = Maps.newHashMap();
    private int mostAtOnce = 0;

    private DataFileProbe(FileIO delegate, int limit, String... dataLocations) {
      this.delegate = delegate;
      this.limit = limit;
      this.dataLocations = ImmutableList.copyOf(dataLocations);
    }

    synchronized int mostAtOnce() {
      return mostAtOnce;
    }

    @Override
    public InputFile newInputFile(String path) {
      return probed(path, delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return probed(path, delegate.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      OutputFile file = delegate.newOutputFile(path);
      return isData(path) ? new ProbedOutputFile(file) : file;
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    private InputFile probed(String path, InputFile file) {
      return isData(path) ? new ProbedInputFile(file) : file;
    }

    private boolean isData(String path) {
      return dataLocations.stream().anyMatch(path::startsWith);
    }

    private synchronized Thread opened() {
      Thread owner = Thread.currentThread();
      if (openStreams.merge(owner, 1, Integer::sum) > 1) {
        return owner;
      }

      mostAtOnce = Math.max(mostAtOnce, openStreams.size());
      notifyAll();
      long deadline = System.currentTimeMillis() + HOLD_MS;
      try {
        for (long left = HOLD_MS;
            openStreams.size() <= limit && left > 0;
            left = deadline - System.currentTimeMillis()) {
          wait(left);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return owner;
    }

    private synchronized void closed(Thread owner) {
      openStreams.computeIfPresent(owner, (thread, open) -> open > 1 ? open - 1 : null);
    }

    private final class ProbedInputFile implements InputFile {
      private final InputFile file;

      private ProbedInputFile(InputFile file) {
        this.file = file;
      }

      @Override
      public long getLength() {
        return file.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        SeekableInputStream stream = file.newStream();
        Thread owner = opened();
        AtomicBoolean released = new AtomicBoolean();
        return new SeekableInputStream() {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }

          @Override
          public int read() throws IOException {
            return stream.read();
          }

          @Override
          public int read(byte[] bytes, int off, int len) throws IOException {
            return stream.read(bytes, off, len);
          }

          @Override
          public void close() throws IOException {
            try {
              stream.close();
            } finally {
              if (released.compareAndSet(false, true)) {
                closed(owner);
              }
            }
          }
        };
      }

      @Override
      public String location() {
        return file.location();
      }

      @Override
      public boolean exists() {
        return file.exists();
      }
    }

    private final class ProbedOutputFile implements OutputFile {
      private final OutputFile file;

      private ProbedOutputFile(OutputFile file) {
        this.file = file;
      }

      @Override
      public PositionOutputStream create() {
        return probed(file.create());
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return probed(file.createOrOverwrite());
      }

      private PositionOutputStream probed(PositionOutputStream stream) {
        Thread owner = opened();
        AtomicBoolean released = new AtomicBoolean();
        return new PositionOutputStream() {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public long storedLength() throws IOException {
            return stream.storedLength();
          }

          @Override
          public void write(int b) throws IOException {
            stream.write(b);
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
            try {
              stream.close();
            } finally {
              if (released.compareAndSet(false, true)) {
                closed(owner);
              }
            }
          }
        };
      }

      @Override
      public String location() {
        return file.location();
      }

      @Override
      public InputFile toInputFile() {
        return file.toInputFile();
      }
    }
  }

  static void appendRows(Table target, Record... rows) {
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
    AppendFiles append = target.newAppend();
    writer.result().dataFiles().forEach(append::appendFile);
    append.commit();
    target.refresh();
  }

  static Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }
}
