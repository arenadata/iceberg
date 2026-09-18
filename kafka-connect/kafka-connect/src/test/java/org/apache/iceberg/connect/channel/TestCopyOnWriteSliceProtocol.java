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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.AssignedFileScanTask;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetNormalizer;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.CopyOnWriteRewriter;
import org.apache.iceberg.connect.data.copyonwrite.PlanSummary;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The slice protocol itself, driven one event at a time: what the coordinator does when a worker
 * fails, goes silent, answers late, or when a concurrent writer invalidates the answer.
 *
 * <p>Where {@link TestCopyOnWriteTableCommitter} asks what ends up in the table, this asks how it
 * got there. Holding the answers back is what makes a conflicting writer and an exhausted retry
 * count testable at all: the conflict is committed in the window between handing a slice out and
 * answering it.
 */
public class TestCopyOnWriteSliceProtocol {

  private static final String CTL_TOPIC = "ctl-topic";
  private static final String GROUP_ID = "cg-connect";
  private static final long TIMEOUT_MS = 60_000L;
  private static final int MESSAGE_LIMIT = 1024 * 1024;

  /**
   * Small enough to cut this schema's assignments in two, large enough for one descriptor.
   *
   * <p>A message of this event is around 10.5 KB of Avro schema before a single file descriptor,
   * and a descriptor of this two-column table is around 160 bytes on top: an assignment of ten
   * files is past this, one of five is not. The numbers move with the schema of the event, not with
   * the test: a change that puts one descriptor past this limit stops the table instead, which is
   * what the last assertions here would report.
   */
  private static final int SMALL_MESSAGE_LIMIT = 11_500;

  private static final String COPY_ON_WRITE_CHANGE_SET_ID_PROP =
      "kafka.connect.copy-on-write.change-set-id";
  private static final String OFFSETS_PROP = "kafka.connect.offsets." + CTL_TOPIC + "." + GROUP_ID;

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);

  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private CopyOnWriteTableCommitter committer;
  private List<RewriteAssigned> assigned;
  private List<Event> otherEvents;

  /** Every batch the committer sent, as it sent it: one batch is one producer transaction. */
  private List<List<Event>> rounds;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn(GROUP_ID);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.copyOnWriteStagingLocation()).thenReturn(null);
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1_000_000L);
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(Long.MAX_VALUE);
    when(config.copyOnWritePruningMaxInCardinality()).thenReturn(1000);
    when(config.copyOnWriteCommitRetries()).thenReturn(2);
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(TIMEOUT_MS);
    when(config.copyOnWriteStagingSweepIntervalMs()).thenReturn(Long.MAX_VALUE);
    when(config.copyOnWriteStagingOrphanTtlMs()).thenReturn(86_400_000L);
    when(config.controlMessageMaxBytes()).thenReturn(MESSAGE_LIMIT);

    assigned = Lists.newArrayList();
    otherEvents = Lists.newArrayList();
    rounds = Lists.newArrayList();
    committer =
        new CopyOnWriteTableCommitter(
            catalog,
            config,
            MetadataEvents.NOOP,
            events -> {
              rounds.add(ImmutableList.copyOf(events));
              for (Event event : events) {
                if (event.payload().type() == PayloadType.REWRITE_ASSIGNED) {
                  assigned.add((RewriteAssigned) roundTrip(event).payload());
                } else {
                  otherEvents.add(event);
                }
              }
            },
            Runnable::run,
            () -> true);
  }

  /**
   * The same committer, but with its transitions queued instead of run, so a test can look at what
   * {@code commit()} itself did before anything was handed to the pool.
   */
  private CopyOnWriteTableCommitter deferredCommitter(Queue<Runnable> transitions) {
    return new CopyOnWriteTableCommitter(
        catalog,
        config,
        MetadataEvents.NOOP,
        events -> {
          for (Event event : events) {
            if (event.payload().type() == PayloadType.REWRITE_ASSIGNED) {
              assigned.add((RewriteAssigned) roundTrip(event).payload());
            } else {
              otherEvents.add(event);
            }
          }
        },
        transitions::add,
        () -> true);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testTheFirstSliceIsPlannedOffTheCallingThread() {
    // doCommit() joins the commit pool from the coordinator thread, so whatever commit() does
    // synchronously is coordinator latency, and normalizing a change set and planning its files
    // is minutes on a large one. commit() may decide; the planning belongs to the pool
    appendRows(row(1L, "a"));
    Queue<Runnable> transitions = Lists.newLinkedList();
    CopyOnWriteTableCommitter deferred = deferredCommitter(transitions);

    CommitResult result = deferred.commit(request(stagedFiles(update(1L, "a2"))));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(assigned).as("nothing planned on the calling thread").isEmpty();
    assertThat(transitions).as("the slice was handed to the pool").hasSize(1);

    // and the change set is frozen and persisted by then, so the drain survives losing this pool
    assertThat(transitions.remove()).isNotNull();
    transitions.clear();
  }

  @Test
  public void testCommitDoesNotWaitOnATableWhoseTransitionIsRunning() {
    appendRows(row(1L, "a"));
    Queue<Runnable> transitions = Lists.newLinkedList();
    CopyOnWriteTableCommitter deferred = deferredCommitter(transitions);
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));

    deferred.commit(request);
    // the queued transition has not run, so the drain is mid-start; a second cycle must come back
    // with PENDING and its envelopes intact rather than block behind it
    CommitResult second = deferred.commit(request);

    assertThat(second.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(second.consumed()).isEmpty();
    assertThat(assigned).isEmpty();
    transitions.clear();
  }

  @Test
  public void testTheCoordinatorThreadNeverWaitsOnATableWhoseTransitionHoldsItsLock()
      throws Exception {
    // a transition holds the table's lock for as long as it plans a slice or retries an Iceberg
    // commit, which is minutes. Here it is a real pool thread held inside the send of its slice:
    // run inline, a transition would share the caller's reentrant lock, and a lock() on the
    // coordinator's side would pass unnoticed. Against a held lock it blocks, and trips the timeout
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    CountDownLatch handedOut = new CountDownLatch(1);
    CountDownLatch sendMayFinish = new CountDownLatch(1);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    CopyOnWriteTableCommitter busy =
        new CopyOnWriteTableCommitter(
            catalog,
            config,
            MetadataEvents.NOOP,
            events -> {
              for (Event event : events) {
                if (event.payload().type() == PayloadType.REWRITE_ASSIGNED) {
                  assigned.add((RewriteAssigned) roundTrip(event).payload());
                }
              }
              handedOut.countDown();
              try {
                // bounded, so that a mutation that deadlocks fails the test rather than hanging it
                sendMayFinish.await(30, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            pool,
            () -> true);
    ExecutorService coordinatorThread = Executors.newSingleThreadExecutor();
    long snapshotsBefore = countSnapshots();

    try {
      try {
        busy.commit(request);
        assertThat(handedOut.await(30, TimeUnit.SECONDS)).as("the slice is being sent").isTrue();
        RewriteAssigned slice = takeSlice();
        long overdue = System.currentTimeMillis() + TIMEOUT_MS + 1;

        CommitResult cycle = promptly(coordinatorThread, "commit()", () -> busy.commit(request));
        assertThat(cycle.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
        assertThat(cycle.consumed()).isEmpty();

        Set<TableReference> pending =
            promptly(coordinatorThread, "pendingTables()", busy::pendingTables);
        assertThat(pending).as("a table mid-transition is not idle").contains(TABLE_REFERENCE);

        // overdue, but its state is not the coordinator's to look at while the transition holds it
        promptly(
            coordinatorThread,
            "process()",
            () -> {
              busy.process(overdue, ImmutableList::of);
              return null;
            });

        for (Assignment assignment : slice.assignments()) {
          Envelope answer =
              envelope(answer(slice, assignment.taskId(), rewriteFor(slice, assignment)));
          promptly(coordinatorThread, "receive()", () -> busy.receive(answer));
        }
      } finally {
        sendMayFinish.countDown();
      }

      // the answers queued behind the transition are applied as it lets go of the lock, and the
      // slice they complete commits on the pool. Each empty task waits for what was queued before
      // it, so the pool is idle at every check
      TableCommitter.Outcome outcome = TableCommitter.Outcome.PENDING;
      for (int round = 0; round < 10 && outcome != TableCommitter.Outcome.COMMITTED; round++) {
        pool.submit(() -> {}).get(30, TimeUnit.SECONDS);
        outcome = busy.commit(request).outcome();
      }

      assertThat(outcome).as("the drain ends").isEqualTo(TableCommitter.Outcome.COMMITTED);
      assertThat(countSnapshots())
          .as("the slice was not cancelled by the overdue tick")
          .isEqualTo(snapshotsBefore + 1);
      assertThat(readAll()).containsExactly("1=a2");
      assertThat(assigned).as("handed out once").isEmpty();
    } finally {
      pool.shutdownNow();
      coordinatorThread.shutdownNow();
    }
  }

  @Test
  public void testCommitReturnsPendingAndHandsTheSliceOut() {
    appendRows(row(1L, "a"));

    CommitResult result = committer.commit(request(stagedFiles(update(1L, "a2"))));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    // nothing is spent until the change set is persisted, so the coordinator keeps holding the
    // offset
    assertThat(result.consumed()).isEmpty();
    // one message per task, not one message with every assignment: the descriptors of a task's
    // files are the whole size of this payload, so framing it per task is what keeps a wide plan
    // inside the producer's limit
    assertThat(assigned).hasSize(2);
    assertThat(assigned).allSatisfy(event -> assertThat(event.assignments()).hasSize(1));
    assertThat(assigned).allSatisfy(event -> assertThat(event.ownerCount()).isEqualTo(2));
    assertThat(assigned).extracting(RewriteAssigned::ownerIndex).containsExactlyInAnyOrder(0, 1);
    assertThat(sliceCount()).isEqualTo(1);
    assertThat(assigned.get(0).sliceSeq()).isEqualTo(0);
    assertThat(table.currentSnapshot().summary())
        .doesNotContainKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
  }

  @Test
  public void testAStoppedCoordinatorDoesNotCommitASliceAnsweredAfterTheStop() {
    // a leader change: this coordinator is told to stop and another may already have handed the
    // table a slice. An answer arriving now must not start a commit, a send or a delete
    appendRows(row(1L, "a"));
    committer.commit(request(stagedFiles(update(1L, "a2"))));
    RewriteAssigned slice = takeSlice();
    long snapshotsBefore = countSnapshots();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    List<String> written = Lists.newArrayList();

    committer.stop();
    for (Assignment assignment : slice.assignments()) {
      List<DataFile> files = rewriteFor(slice, assignment);
      files.forEach(file -> written.add(file.location()));
      committer.receive(envelope(answer(slice, assignment.taskId(), files)));
    }

    assertThat(countSnapshots()).as("no commit after the stop").isEqualTo(snapshotsBefore);
    assertThat(otherEvents).as("nothing sent after the stop").isEmpty();
    assertThat(assigned).as("nothing handed out after the stop").isEmpty();
    assertThat(written).isNotEmpty();
    assertThat(written)
        .as("no file deleted after the stop")
        .allSatisfy(location -> assertThat(io.fileExists(location)).isTrue());
    assertThat(io.fileExists(slice.normalizedRef().location())).isTrue();
    assertThat(io.fileExists(manifestLocation(slice))).isTrue();
  }

  @Test
  public void testAStoppedCoordinatorNeitherDeletesNorHandsOutAgainASliceThatFailsAfterTheStop() {
    appendRows(row(1L, "a"));
    committer.commit(request(stagedFiles(update(1L, "a2"))));
    RewriteAssigned slice = takeSlice();
    assertThat(slice.assignments()).hasSize(2);
    Assignment answered = slice.assignments().get(0);
    List<DataFile> files = rewriteFor(slice, answered);
    committer.receive(envelope(answer(slice, answered.taskId(), files)));
    InMemoryFileIO io = (InMemoryFileIO) table.io();

    committer.stop();
    // a failure with retries left would delete the answered files and hand the slice out at once
    committer.receive(envelope(failure(slice, slice.assignments().get(1).taskId())));

    assertThat(assigned).as("nothing handed out after the stop").isEmpty();
    assertThat(files)
        .as("no file deleted after the stop")
        .allSatisfy(file -> assertThat(io.fileExists(file.location())).isTrue());
    assertThat(io.fileExists(slice.normalizedRef().location())).isTrue();
  }

  @Test
  public void testAnAssignmentTooLargeForOneMessageGoesOutAsChunksOfOneRound() {
    // the plan of a slice is not the coordinator's to shrink: one key with no bounds to prune by
    // plans the whole table, so an assignment that outgrows the producer's limit is cut into
    // messages instead. Sent one message per task, an oversized round throws on the send, fails
    // the drain and comes back the same size every cycle, with the table stopped in all but name
    when(config.controlMessageMaxBytes()).thenReturn(SMALL_MESSAGE_LIMIT);
    for (long id = 1; id <= 20; id++) {
      appendRows(row(id, "a"));
    }

    committer.commit(
        request(
            stagedFiles(
                updates(
                    1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L,
                    19L, 20L))));

    List<Event> round = assignmentRound();
    assertThat(round)
        .as("every message of the round in one transaction, sizes %s", encodedSizes(round))
        .hasSizeGreaterThan(2);
    assertThat(encodedSizes(round))
        .as("no message past the limit")
        .allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(SMALL_MESSAGE_LIMIT));

    for (String taskId : ImmutableList.of("task-0", "task-1")) {
      List<RewriteAssigned> chunks = chunksOf(taskId);
      assertThat(chunks).as("chunks of %s", taskId).hasSizeGreaterThan(1);
      assertThat(chunks).extracting(RewriteAssigned::chunkCount).containsOnly(chunks.size());
      assertThat(chunks)
          .extracting(RewriteAssigned::chunkIndex)
          .containsExactlyInAnyOrderElementsOf(
              IntStream.range(0, chunks.size()).boxed().collect(Collectors.toList()));
      // what tells the worker which keys it owns and where the slice is repeats in every chunk
      assertThat(chunks).extracting(RewriteAssigned::ownerCount).containsOnly(2);
      assertThat(chunks).extracting(RewriteAssigned::ownerIndex).hasSize(chunks.size());
      assertThat(chunks)
          .extracting(event -> event.ownerIndex())
          .containsOnly(chunks.get(0).ownerIndex());
      assertThat(chunks)
          .extracting(event -> event.normalizedRef().location())
          .containsOnly(chunks.get(0).normalizedRef().location());
      assertThat(chunks)
          .extracting(RewriteAssigned::identifierFieldIds)
          .containsOnly(chunks.get(0).identifierFieldIds());
    }

    // and nothing of the plan is lost between the chunks: the drain goes through and the rows of
    // every planned file come out rewritten
    RewriteAssigned slice = takeSlice();
    assertThat(slice.assignments().stream().mapToInt(assignment -> assignment.files().size()).sum())
        .as("every planned file assigned exactly once")
        .isEqualTo(20);
    answerAll(slice);
    assertThat(readAll())
        .containsExactlyInAnyOrder(
            "1=v2", "2=v2", "3=v2", "4=v2", "5=v2", "6=v2", "7=v2", "8=v2", "9=v2", "10=v2",
            "11=v2", "12=v2", "13=v2", "14=v2", "15=v2", "16=v2", "17=v2", "18=v2", "19=v2",
            "20=v2");
  }

  @Test
  public void testAnAssignmentChunkThatCannotBeSplitStopsTheTable() {
    // a single data file descriptor past the limit cannot be cut in two, and the next cycle plans
    // the same file into a descriptor of the same size: the table is stopped with the remedy
    // instead of failing the same drain every commit interval while it holds the control offsets
    when(config.controlMessageMaxBytes()).thenReturn(64);
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));

    committer.commit(request);

    assertThat(assigned).as("nothing that large was handed out").isEmpty();
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    assertThat(assigned).as("nor when the timeout passes").isEmpty();
    // still visited, so that CommitComplete carries no valid-through while the table is stopped
    assertThat(committer.pendingTables()).containsExactly(TABLE_REFERENCE);
    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(assigned).as("nor on the next cycle").isEmpty();
    assertThat(readAll()).containsExactly("1=a");
  }

  @Test
  public void testEnvelopesAreConsumedOnlyOnceTheChangeSetIsApplied() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));

    assertThat(committer.commit(request).consumed()).isEmpty();
    answerAll(takeSlice());

    CommitResult next = committer.commit(request);
    assertThat(next.consumed()).containsExactlyElementsOf(request.envelopes());
    assertThat(next.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testThePlanSummaryShowsTheBytesThatChangeAndTheChangeSetSoFar() {
    // every key lives in the same data file and each slice takes one key, so every slice rewrites
    // that file again for one row's worth of change. The operator sees "400 GB for 12 MB" only if
    // the summary logged before each RewriteAssigned carries the bytes that change, the ratio of
    // the rewrite to them, and what the change set's committed slices have rewritten so far
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, TABLE_REFERENCE, ID_FIELDS, null, GROUP_ID, "task-0");
    for (long id = 1; id <= 3; id++) {
      writer.write(
          writer.stagedRow(
              (GenericRecord) row(id, "v2"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, id));
    }

    committer.commit(request(writer.complete()));

    long replacedByCommittedSlices = 0;
    for (int seq = 0; seq < 3; seq++) {
      RewriteAssigned slice = takeSlice();
      assertThat(slice.sliceSeq()).isEqualTo(seq);
      PlanSummary summary = committer.planSummary(TABLE_REFERENCE);
      long changeBytes = slice.normalizedRef().fileSizeBytes();
      assertThat(summary.totalRewriteBytes()).isPositive();
      assertThat(summary.toString())
          .as("slice %d", seq)
          .contains("changeBytes=" + changeBytes)
          .contains(
              String.format(
                  Locale.ROOT,
                  "rewriteAmplification=%.2f",
                  (double) summary.totalRewriteBytes() / changeBytes))
          .contains("changeSetBytesReplaced=" + replacedByCommittedSlices)
          .contains("changeSetSlicesCommitted=" + seq);
      replacedByCommittedSlices += summary.totalRewriteBytes();
      answerAll(slice);
    }

    assertThat(assigned).isEmpty();
    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2", "3=v2");
  }

  @Test
  public void testFilesWithoutBoundsAreWarnedAboutOncePerChangeSet() {
    // write.metadata.metrics.* left at counts for this file: no bounds on the identifier column,
    // so the plan cannot prune it and rewrites it on every slice that touches its key range. The
    // operator only sees this in a WARN: the byte-quota summary is an INFO line
    appendRows(row(1L, "a"));
    DataFile noBounds =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(table.location() + "/data/no-bounds.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(50L)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(noBounds).commit();
    table.refresh();

    committer.commit(request(update(1L, "a2")));

    assertThat(committer.filesWithoutBoundsWarnedFor(TABLE_REFERENCE)).isNotNull();
  }

  @Test
  public void testAnIntermediateCommitWarnsAboutSlicingWithProgressAndTheThreeLevers() {
    // every slice that does not exhaust the change set is itself evidence of write
    // amplification; an operator filtering on WARN must see it as it happens, with the three
    // levers that cut the slicing rather than work around it, not infer it from an INFO line
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    committer.commit(request(updates(1L, 2L, 3L)));

    answerAll(takeSlice());
    assertThat(committer.sliceProgressWarning(TABLE_REFERENCE))
        .as("progress after the first of three slices")
        .contains("1 slice(s) committed so far")
        .contains("iceberg.control.commit.interval-ms")
        .contains("partition or sort the table by the key")
        .contains("merge-on-read");
    assertThat(committer.sliceProgressWarningsLogged(TABLE_REFERENCE))
        .as("warning logged once for the first intermediate commit")
        .isEqualTo(1);

    answerAll(takeSlice());
    assertThat(committer.sliceProgressWarning(TABLE_REFERENCE))
        .as("progress after the second of three slices")
        .contains("2 slice(s) committed so far");
    assertThat(committer.sliceProgressWarningsLogged(TABLE_REFERENCE))
        .as("warning logged again for the second intermediate commit")
        .isEqualTo(2);

    answerAll(takeSlice());
    assertThat(assigned).isEmpty();
    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2", "3=v2");
    assertThat(committer.sliceProgressWarningsLogged(TABLE_REFERENCE))
        .as("the slice that exhausts the change set is not an intermediate commit")
        .isEqualTo(2);
  }

  @Test
  public void testWorkerFailureCancelsTheSliceImmediately() {
    appendRows(row(1L, "a"));
    committer.commit(request(stagedFiles(update(1L, "a2"))));
    RewriteAssigned first = takeSlice();

    // one task reports a failed rewrite: the slice goes, whole, without waiting out the timeout
    committer.receive(envelope(failure(first, first.assignments().get(0).taskId())));

    assertThat(sliceCount()).as("slice re-assigned at once").isEqualTo(1);
    assertThat(assigned.get(0).sliceSeq()).isEqualTo(first.sliceSeq() + 1);
    assertThat(assigned.get(0).changeSetId()).isEqualTo(first.changeSetId());

    // the re-assigned slice completes normally
    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testAPermanentWorkerFailureStopsTheTable() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();

    // the worker says the same rewrite would fail again: handing the slice out once more would
    // only repeat the failure, every cycle, with the control topic offsets held all the while
    committer.receive(
        envelope(
            failure(
                first, first.assignments().get(0).taskId(), RewriteComplete.FAILURE_PERMANENT)));

    assertThat(assigned).as("the slice is not handed out again").isEmpty();
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    assertThat(assigned).as("nor when its timeout passes").isEmpty();
    // still visited, so that CommitComplete carries no valid-through while the table is stopped
    assertThat(committer.pendingTables()).containsExactly(TABLE_REFERENCE);

    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(assigned).as("nor on the next cycle").isEmpty();
    assertThat(readAll()).containsExactly("1=a");
  }

  @Test
  public void testSilentWorkerCancelsTheSliceAndTheNextCycleHandsItToItsTasks() {
    // a task that went silent has most likely failed or moved in a rebalance, and the active
    // tasks the slice went out on still include it: handed out again at once, the slice would
    // only wait out the timeout again. The drain is kept, and the next cycle's active tasks get
    // the slice
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"));
    TableCommitRequest request = request(updates(1L, 2L));
    committer.commit(request);
    answerAll(takeSlice());
    RewriteAssigned second = takeSlice();
    long snapshotsBefore = countSnapshots();

    committer.process(System.currentTimeMillis(), ImmutableList::of);
    assertThat(assigned).as("not yet due").isEmpty();

    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    assertThat(assigned).as("not handed out again to the active tasks it timed out on").isEmpty();
    assertThat(committer.pendingTables()).as("the drain is kept").contains(TABLE_REFERENCE);

    CommitResult next = committer.commit(withTasks(request, "task-1", "task-2"));
    assertThat(next.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(sliceCount()).isEqualTo(1);
    RewriteAssigned retried = takeSlice();
    assertThat(retried.changeSetId()).isEqualTo(second.changeSetId());
    assertThat(retried.sliceSeq()).isEqualTo(second.sliceSeq() + 1);
    assertThat(retried.assignments())
        .extracting(Assignment::taskId)
        .containsExactlyInAnyOrder("task-1", "task-2");

    // from the cursor: the slice that committed before the timeout is not applied again
    answerAll(retried);
    assertThat(countSnapshots()).isEqualTo(snapshotsBefore + 1);
    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2");
  }

  @Test
  public void testAChunkErrorHandsTheSliceOutOnTheNextCycle() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    committer.commit(request);
    RewriteAssigned first = takeSlice();

    // chunk 2 of an announced 2: a protocol error, retried the way a timeout is. The constructor
    // refuses such a chunk, so it is set the way decoding one off the wire sets it
    RewriteComplete outOfRange =
        (RewriteComplete)
            chunk(first, first.assignments().get(0).taskId(), ImmutableList.of(), 1, 2).payload();
    outOfRange.put(outOfRange.getSchema().getField("chunk_index").pos(), 2);
    committer.receive(envelope(new Event(GROUP_ID, outOfRange)));
    assertThat(assigned).as("not handed out again at once").isEmpty();

    committer.commit(request);
    assertThat(sliceCount()).isEqualTo(1);
    RewriteAssigned retried = takeSlice();
    assertThat(retried.sliceSeq()).isEqualTo(first.sliceSeq() + 1);
    answerAll(retried);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testATimedOutSliceDeletesItsFilesOnThePoolRatherThanTheCoordinatorThread() {
    // process() runs on the coordinator thread, and a cancelled slice can carry hundreds of files.
    // Deleting them there, on a slow object store, is what misses max.poll.interval.ms
    appendRows(row(1L, "a"));
    Queue<Runnable> transitions = Lists.newLinkedList();
    CopyOnWriteTableCommitter deferred = deferredCommitter(transitions);
    deferred.commit(request(update(1L, "a2")));
    transitions.remove().run();
    RewriteAssigned first = takeSlice();
    String normalized = first.normalizedRef().location();
    List<DataFile> written = Lists.newArrayList();
    first.assignments().forEach(assignment -> written.addAll(rewriteFor(first, assignment)));
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(written).as("the attempt wrote replacement files").isNotEmpty();

    // the coordinator holds these files, and the task still owes a second chunk
    deferred.receive(envelope(chunk(first, first.assignments().get(0).taskId(), written, 0, 2)));
    deferred.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);

    assertThat(written)
        .as("still in place when process() returns")
        .allSatisfy(file -> assertThat(io.fileExists(file.location())).isTrue());
    assertThat(io.fileExists(normalized)).as("normalized file when process() returns").isTrue();

    while (!transitions.isEmpty()) {
      transitions.remove().run();
    }

    assertThat(written)
        .as("deleted by the pool")
        .allSatisfy(file -> assertThat(io.fileExists(file.location())).isFalse());
    assertThat(io.fileExists(normalized)).as("normalized file, deleted by the pool").isFalse();
    assertThat(assigned).as("handed out again only by the next cycle").isEmpty();
  }

  @Test
  public void testALostChunkNeverCompletesTheSlice() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    long snapshotsBefore = countSnapshots();

    // every other task answers in full, so the only thing the slice is missing is one chunk
    Assignment mine = first.assignments().get(0);
    answerAllExcept(first, mine.taskId());

    // this task announces three chunks and only two arrive. Each chunk is its own producer
    // transaction, so one can be lost while the rest commit; trusting a flag on the final chunk
    // would complete the slice here, short of whatever the missing one carried
    committer.receive(envelope(chunk(first, mine.taskId(), rewriteFor(first, mine), 0, 3)));
    committer.receive(envelope(chunk(first, mine.taskId(), ImmutableList.of(), 1, 3)));

    assertThat(countSnapshots())
        .as("nothing committed on a short answer")
        .isEqualTo(snapshotsBefore);
    assertThat(assigned).as("the task has not answered, so the slice still waits").isEmpty();

    // it degrades to the timeout, which cancels the slice; the next cycle hands it out again
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    committer.commit(request);
    assertThat(sliceCount()).isEqualTo(1);
    assertThat(assigned.get(0).sliceSeq()).isEqualTo(first.sliceSeq() + 1);

    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  /**
   * The case a bare count cannot see: one chunk arrives twice and another never arrives, which adds
   * up to the announced total. Counting messages completes the slice here and commits it with the
   * duplicate's files twice over and the lost one's not at all; counting which chunks arrived does
   * not.
   */
  @Test
  public void testADuplicateChunkDoesNotStandInForALostOne() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    long snapshotsBefore = countSnapshots();

    Assignment mine = first.assignments().get(0);
    answerAllExcept(first, mine.taskId());

    // chunk 0 twice, chunk 2 never: three messages for three announced chunks
    committer.receive(envelope(chunk(first, mine.taskId(), rewriteFor(first, mine), 0, 3)));
    committer.receive(envelope(chunk(first, mine.taskId(), ImmutableList.of(), 1, 3)));
    committer.receive(envelope(chunk(first, mine.taskId(), rewriteFor(first, mine), 0, 3)));

    assertThat(countSnapshots())
        .as("three messages, but only two distinct chunks: nothing may commit")
        .isEqualTo(snapshotsBefore);
    assertThat(assigned).as("the task has not answered, so the slice still waits").isEmpty();

    // and it degrades to the timeout, like any other short answer
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    committer.commit(request);
    assertThat(sliceCount()).isEqualTo(1);

    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testChunksSeenDoNotSurviveACancelledSlice() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    String taskId = first.assignments().get(0).taskId();

    // two of three chunks arrive, then the slice times out and the next cycle replaces it
    committer.receive(envelope(chunk(first, taskId, ImmutableList.of(), 0, 3)));
    committer.receive(envelope(chunk(first, taskId, ImmutableList.of(), 1, 3)));
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    committer.commit(request);
    RewriteAssigned second = takeSlice();

    Assignment mine = second.assignments().get(0);
    answerAllExcept(second, mine.taskId());

    // had the chunks carried over, chunks 0 and 1 would already be marked seen and this one would
    // complete the replacement slice with two thirds of its files missing
    committer.receive(envelope(chunk(second, mine.taskId(), rewriteFor(second, mine), 2, 3)));
    assertThat(assigned).as("still waiting for the other two chunks").isEmpty();

    committer.receive(envelope(chunk(second, mine.taskId(), ImmutableList.of(), 0, 3)));
    committer.receive(envelope(chunk(second, mine.taskId(), ImmutableList.of(), 1, 3)));
    assertThat(readAll()).containsExactly("1=a2");
  }

  /**
   * The case a count taken off the chunk in hand cannot see: two attempts of one task reach the
   * slice, one having split its answer in two chunks and the other in three. Their indexes differ,
   * so as many chunks as the last one announced have arrived, and the slice commits two chunks'
   * files out of five. A chunk disagreeing with the count its task first announced is a protocol
   * error, and the slice goes whole.
   */
  @Test
  public void testAChunkCountThatDisagreesWithTheFirstOneCancelsTheSlice() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    long snapshotsBefore = countSnapshots();

    Assignment mine = first.assignments().get(0);
    answerAllExcept(first, mine.taskId());

    // the second attempt's chunk 2 of 3, then the first attempt's chunk 1 of 2
    committer.receive(envelope(chunk(first, mine.taskId(), ImmutableList.of(), 2, 3)));
    committer.receive(envelope(chunk(first, mine.taskId(), rewriteFor(first, mine), 1, 2)));

    assertThat(countSnapshots())
        .as("a slice whole chunks short of an answer may not commit")
        .isEqualTo(snapshotsBefore);
    assertThat(assigned).as("a protocol error is retried the way a timeout is").isEmpty();

    committer.commit(request);
    assertThat(sliceCount()).isEqualTo(1);
    RewriteAssigned retried = takeSlice();
    assertThat(retried.sliceSeq()).isEqualTo(first.sliceSeq() + 1);

    answerAll(retried);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testAnAnswerFromATaskOutsideTheActiveTasksIsIgnored() {
    appendRows(row(1L, "a"));
    committer.commit(request(stagedFiles(update(1L, "a2"))));
    RewriteAssigned first = takeSlice();
    long snapshotsBefore = countSnapshots();

    // a task the slice was never handed to answers in full, its files rewritten from the same
    // assignment: collecting them would commit these rows twice over
    List<DataFile> outside = Lists.newArrayList();
    first.assignments().forEach(assignment -> outside.addAll(rewriteFor(first, assignment)));
    assertThat(outside).as("the straggler rewrote the whole slice").isNotEmpty();
    committer.receive(envelope(answer(first, "task-9", outside)));

    assertThat(countSnapshots())
        .as("no commit off an answer from outside the active tasks")
        .isEqualTo(snapshotsBefore);
    assertThat(assigned).as("the slice still waits for its own tasks").isEmpty();

    answerAll(first);
    assertThat(readAll()).containsExactly("1=a2");
    assertThat(committedLocations())
        .as("none of the straggler's files are in the table")
        .doesNotContainAnyElementsOf(
            outside.stream().map(DataFile::location).collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testAFailureAfterOneOfTheTasksChunksStillCancelsTheSliceAtOnce() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    String taskId = first.assignments().get(0).taskId();

    // one of two chunks arrived, and then the task failed. The status decides before the chunk
    // accounting does: a FAILED carries chunk 0 of 1, which the accounting would drop as a repeat
    // of an index already seen, leaving the slice to wait out the whole timeout
    committer.receive(envelope(chunk(first, taskId, ImmutableList.of(), 0, 2)));
    committer.receive(envelope(failure(first, taskId)));

    assertThat(sliceCount()).as("slice re-assigned at once").isEqualTo(1);
    assertThat(assigned.get(0).sliceSeq()).isEqualTo(first.sliceSeq() + 1);

    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testAFailureAndARepeatedChunkAfterAFullAnswerLeaveTheCommitAlone() {
    appendRows(row(1L, "a"));
    Queue<Runnable> transitions = Lists.newLinkedList();
    CopyOnWriteTableCommitter deferred = deferredCommitter(transitions);
    deferred.commit(request(update(1L, "a2")));
    transitions.remove().run();
    RewriteAssigned first = takeSlice();
    long snapshotsBefore = countSnapshots();

    // every task answers: the slice is complete and its commit is queued, not yet run
    for (Assignment assignment : first.assignments()) {
      deferred.receive(envelope(answer(first, assignment.taskId(), rewriteFor(first, assignment))));
    }
    assertThat(countSnapshots()).as("the commit is queued, not run").isEqualTo(snapshotsBefore);

    // the worker's last chunk reached the control topic and the send threw afterwards, so the
    // worker reports FAILED and repeats the chunk. The answer is already complete: acting on
    // either would throw away a slice that is on its way to the table
    Assignment mine = first.assignments().get(0);
    deferred.receive(envelope(failure(first, mine.taskId())));
    deferred.receive(envelope(answer(first, mine.taskId(), rewriteFor(first, mine))));

    while (!transitions.isEmpty()) {
      transitions.remove().run();
    }

    assertThat(countSnapshots()).as("the slice committed, once").isEqualTo(snapshotsBefore + 1);
    assertThat(readAll()).containsExactly("1=a2");
    assertThat(assigned).as("not handed out again").isEmpty();
  }

  @Test
  public void testLateAnswerToACancelledSliceIsIgnored() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    long snapshotsBefore = countSnapshots();
    // the worker did its work before the coordinator gave up on it: cancelling deletes the
    // slice's normalized file, so a straggler could not have read it afterwards either
    List<DataFile> lateFiles = rewriteFor(first);

    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    committer.commit(request);
    RewriteAssigned second = takeSlice();

    // the original slice finally answers: its sequence number is stale, so it is dropped
    for (Assignment assignment : first.assignments()) {
      committer.receive(
          envelope(
              answer(
                  first,
                  assignment.taskId(),
                  assignment.taskId().equals(first.assignments().get(0).taskId())
                      ? lateFiles
                      : ImmutableList.of())));
    }

    assertThat(countSnapshots()).as("no commit from a cancelled slice").isEqualTo(snapshotsBefore);
    assertThat(assigned).isEmpty();

    answerAll(second);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testConcurrentWriterOnTheSameKeysForcesAReplan() {
    appendRows(row(1L, "a"));
    committer.commit(request(stagedFiles(update(1L, "a2"))));
    RewriteAssigned first = takeSlice();

    List<DataFile> written = rewriteFor(first);
    // a competing writer lands the same key while the rewrite was in flight
    appendRows(row(1L, "concurrent"));

    committer.receive(envelope(answer(first, first.assignments().get(0).taskId(), written)));
    committer.receive(
        envelope(answer(first, first.assignments().get(1).taskId(), ImmutableList.of())));

    assertThat(sliceCount()).as("conflict replanned against the new snapshot").isEqualTo(1);
    assertThat(assigned.get(0).baseSnapshotId()).isEqualTo(table.currentSnapshot().snapshotId());

    answerAll(takeSlice());
    // the retry planned against the snapshot the competitor left, so its row is the one updated
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testAConcurrentAppendToAnotherPartitionCommitsWithoutAReplan() {
    // the validation is narrowed to files that could hold the slice's keys. Unnarrowed, any file
    // appended while a slice is rewritten conflicts (another partition, a one-off INSERT, a
    // second connector) and a steady stream of them fails every attempt of every slice
    catalog.dropTable(TABLE_IDENTIFIER);
    table =
        catalog.createTable(
            TABLE_IDENTIFIER, SCHEMA, PartitionSpec.builderFor(SCHEMA).truncate("id", 10).build());
    appendRows(row(1L, "a"));
    committer.commit(request(update(1L, "a2")));
    RewriteAssigned first = takeSlice();
    List<List<DataFile>> written = Lists.newArrayList();
    first.assignments().forEach(assignment -> written.add(rewriteFor(first, assignment)));

    // another writer appends to a partition holding none of the slice's keys while the rewrite ran
    appendRows(row(15L, "other"));
    long competitor = table.currentSnapshot().snapshotId();
    for (int i = 0; i < written.size(); i++) {
      committer.receive(
          envelope(answer(first, first.assignments().get(i).taskId(), written.get(i))));
    }

    assertThat(readAll()).containsExactlyInAnyOrder("1=a2", "15=other");
    assertThat(assigned).as("committed on the first attempt, nothing replanned").isEmpty();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(competitor);
  }

  @Test
  public void testARowDeletedFromARewrittenFileWhileTheSliceRanForcesAReplan() {
    // a merge-on-read DELETE lands a position delete on a file the slice is rewriting. The
    // replacement was read without it: committed, it would take the file's place and leave the
    // delete naming a file no longer in the table, so the deleted row would be back
    appendRows(row(1L, "a"), row(2L, "b"));
    DataFile base = Iterables.getOnlyElement(table.currentSnapshot().addedDataFiles(table.io()));
    committer.commit(request(update(1L, "a2")));
    RewriteAssigned first = takeSlice();
    List<List<DataFile>> written = Lists.newArrayList();
    first.assignments().forEach(assignment -> written.add(rewriteFor(first, assignment)));

    deletePosition(base, 1L);
    long competitor = table.currentSnapshot().snapshotId();
    for (int i = 0; i < written.size(); i++) {
      committer.receive(
          envelope(answer(first, first.assignments().get(i).taskId(), written.get(i))));
    }
    for (int attempt = 0; attempt < 3 && !assigned.isEmpty(); attempt++) {
      answerAll(takeSlice());
    }

    assertThat(readAll()).containsExactly("1=a2");
    assertThat(table.currentSnapshot().parentId())
        .as("replanned against the snapshot with the delete")
        .isEqualTo(competitor);
  }

  @Test
  public void testAFirstSnapshotLandingUnderAnUnbasedSliceForcesAReplan() {
    // an empty table: the slice is planned against no snapshot at all and rewritten append-only
    committer.commit(request(updateThenDelete(1L, "a2", 2L)));
    RewriteAssigned first = takeSlice();
    List<List<DataFile>> written = Lists.newArrayList();
    first.assignments().forEach(assignment -> written.add(rewriteFor(first, assignment)));

    // another writer creates the table's first snapshot, with both keys, while the rewrite ran
    appendRows(row(1L, "concurrent"), row(2L, "b"));
    long competitor = table.currentSnapshot().snapshotId();
    for (int i = 0; i < written.size(); i++) {
      committer.receive(
          envelope(answer(first, first.assignments().get(i).taskId(), written.get(i))));
    }
    for (int attempt = 0; attempt < 3 && !assigned.isEmpty(); attempt++) {
      answerAll(takeSlice());
    }

    // appended on top of the competitor, the slice would leave key 1 twice and key 2 not deleted
    assertThat(readAll()).containsExactly("1=a2");
    assertThat(table.currentSnapshot().parentId())
        .as("replanned against the snapshot the competitor left")
        .isEqualTo(competitor);
  }

  @Test
  public void testMainAdvancingUnderTheFirstSliceOfANewBranchForcesAReplan() {
    appendRows(row(1L, "a"));
    String branch = "new-branch";
    TableSinkConfig branchConfig = mock(TableSinkConfig.class);
    when(branchConfig.commitBranch()).thenReturn(branch);
    when(config.tableConfig(any())).thenReturn(branchConfig);

    // the branch does not exist, so the slice is planned against the head of main it will fork from
    committer.commit(request(update(1L, "a2")));
    RewriteAssigned first = takeSlice();
    assertThat(first.baseSnapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    List<List<DataFile>> written = Lists.newArrayList();
    first.assignments().forEach(assignment -> written.add(rewriteFor(first, assignment)));

    // main moves on with the same key while the rewrite ran: the branch now forks from that head
    appendRows(row(1L, "concurrent"));
    long mainHead = table.currentSnapshot().snapshotId();
    for (int i = 0; i < written.size(); i++) {
      committer.receive(
          envelope(answer(first, first.assignments().get(i).taskId(), written.get(i))));
    }
    for (int attempt = 0; attempt < 3 && !assigned.isEmpty(); attempt++) {
      answerAll(takeSlice());
    }

    assertThat(readAllOnRef(branch)).containsExactly("1=a2");
    assertThat(table.snapshot(branch).parentId())
        .as("replanned against the head main moved to")
        .isEqualTo(mainHead);
    assertThat(readAll()).containsExactlyInAnyOrder("1=a", "1=concurrent");
  }

  @Test
  public void testExhaustedRetriesHandTheSliceOutAgainOnTheNextCycle() {
    // a table under regular compaction conflicts now and then. Giving the drain up for it would
    // cost a cycle answering FAILED and another to start over, with the manifest deleted and the
    // change set frozen again in between
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);

    // conflict on every attempt: two retries configured, so three attempts in all
    RewriteAssigned attempt = takeSlice();
    answerAllOverAConflict(attempt, "concurrent-0");
    for (int n = 1; n < 3; n++) {
      assertThat(sliceCount()).as("attempt %d handed out", n).isEqualTo(1);
      attempt = takeSlice();
      answerAllOverAConflict(attempt, "concurrent-" + n);
    }
    assertThat(assigned).as("the series is over").isEmpty();

    CommitResult next = committer.commit(request);
    assertThat(sliceCount()).as("handed out again by the next commit itself").isEqualTo(1);
    assertThat(next.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    RewriteAssigned retried = takeSlice();
    assertThat(retried.changeSetId()).isEqualTo(attempt.changeSetId());
    assertThat(retried.sliceSeq()).isEqualTo(attempt.sliceSeq() + 1);
    assertThat(((InMemoryFileIO) table.io()).fileExists(manifestLocation(retried))).isTrue();

    answerAll(retried);
    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testFailuresPastTheRetriesKeepTheDrainForTheNextCycle() {
    // the slice failed, not the drain: dropping the drain would delete the manifest just frozen,
    // freeze the same envelopes again and start over from slice 0
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    committer.commit(request);
    RewriteAssigned attempt = takeSlice();
    String manifest = manifestLocation(attempt);

    // two retries configured: two failures are handed out again at once, the third ends the series
    for (int n = 0; n < 3; n++) {
      committer.receive(envelope(failure(attempt, attempt.assignments().get(0).taskId())));
      if (n < 2) {
        assertThat(sliceCount()).as("failure %d handed out again at once", n).isEqualTo(1);
        attempt = takeSlice();
      }
    }

    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(io.fileExists(manifest)).as("the change set still draining").isTrue();
    assertThat(assigned).as("the series is over").isEmpty();
    assertThat(committer.pendingTables()).contains(TABLE_REFERENCE);

    CommitResult next = committer.commit(request);
    assertThat(next.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(sliceCount()).isEqualTo(1);
    RewriteAssigned retried = takeSlice();
    assertThat(retried.changeSetId()).as("not frozen again").isEqualTo(attempt.changeSetId());
    assertThat(retried.sliceSeq()).isEqualTo(attempt.sliceSeq() + 1);

    // a new cycle starts a new series, with the retries back in full
    committer.receive(envelope(failure(retried, retried.assignments().get(0).taskId())));
    assertThat(sliceCount()).as("handed out again at once in the new series").isEqualTo(1);
    answerAll(takeSlice());
    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testFailuresAndConflictsDrawOnOneRetryQuota() {
    // every immediate retry is a full round of rewriting by every task, whatever sent the slice
    // back. A quota of commit-retries for each would let a slice that fails and conflicts in turn
    // go round far more often than configured
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    committer.commit(request);

    RewriteAssigned failed = takeSlice();
    committer.receive(envelope(failure(failed, failed.assignments().get(0).taskId())));
    assertThat(sliceCount()).as("first retry, after a failure").isEqualTo(1);

    answerAllOverAConflict(takeSlice(), "concurrent");
    assertThat(sliceCount()).as("second retry, after a conflict").isEqualTo(1);

    RewriteAssigned failedAgain = takeSlice();
    committer.receive(envelope(failure(failedAgain, failedAgain.assignments().get(0).taskId())));
    assertThat(assigned).as("both retries spent: the slice waits for the next cycle").isEmpty();

    committer.commit(request);
    assertThat(sliceCount()).isEqualTo(1);
    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testASliceOverTheByteQuotaThatTimesOutIsWarnedAboutAsNotFittingTheTimeout() {
    // a slice over the byte quota has no upper bound on its bytes, and a timeout hands out the
    // same slice with the same plan every cycle: the operator must be told that the lever is
    // rewrite-timeout-ms, not the quota, and not that the tasks went silent
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"));
    committer.commit(request(updates(1L, 2L)));
    takeSlice();
    long planBytes = committer.planSummary(TABLE_REFERENCE).totalRewriteBytes();

    assertThat(committer.overByteQuotaWarning(TABLE_REFERENCE))
        .as("the over-quota warning names the timeout the slice has to fit in")
        .contains(planBytes + " bytes")
        .contains("iceberg.tables.copy-on-write.rewrite-timeout-ms")
        .contains(TIMEOUT_MS + " ms");

    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);

    assertThat(committer.cancellationWarning(TABLE_REFERENCE))
        .as("the timeout is put down to the slice not fitting, not to silent tasks")
        .startsWith("Cancelling slice")
        .contains(planBytes + " bytes")
        .contains("iceberg.tables.copy-on-write.rewrite-timeout-ms")
        .doesNotContain("no complete answer");
  }

  @Test
  public void testASliceWithinTheByteQuotaThatTimesOutIsPutDownToTheSilentTasks() {
    appendRows(row(1L, "a"));
    committer.commit(request(update(1L, "a2")));
    takeSlice();

    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);

    assertThat(committer.overByteQuotaWarning(TABLE_REFERENCE)).isNull();
    assertThat(committer.cancellationWarning(TABLE_REFERENCE))
        .startsWith("Cancelling slice")
        .contains("no complete answer within " + TIMEOUT_MS + " ms");
  }

  @Test
  public void testCancellationsAreCountedAcrossCyclesUntilTheSliceCommits() {
    // the sequence number moves on with every attempt and every slice alike, so it cannot tell an
    // operator one slice failing over and over from slices failing one after another
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    committer.commit(request);

    RewriteAssigned first = takeSlice();
    committer.receive(envelope(failure(first, first.assignments().get(0).taskId())));
    assertThat(committer.cancelledInARow(TABLE_REFERENCE)).as("a failure").isEqualTo(1);

    takeSlice();
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    assertThat(committer.cancelledInARow(TABLE_REFERENCE)).as("then a timeout").isEqualTo(2);

    committer.commit(request);
    RewriteAssigned nextCycle = takeSlice();
    committer.receive(envelope(failure(nextCycle, nextCycle.assignments().get(0).taskId())));
    assertThat(committer.cancelledInARow(TABLE_REFERENCE))
        .as("counted on into the next cycle")
        .isEqualTo(3);

    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
    assertThat(committer.cancelledInARow(TABLE_REFERENCE)).as("the slice committed").isZero();
  }

  @Test
  public void testASliceThatFailedToStartIsStartedAgainByTheNextCommit() {
    // the failure happened on the pool, outside any commit(), and no CommitResult can carry it.
    // The next commit() starts the drain again at once and answers by what that does
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    failNextSliceStart();
    committer.commit(request);
    assertThat(assigned).as("the first slice failed to start").isEmpty();

    CommitResult next = committer.commit(request);
    assertThat(sliceCount()).as("started by the next commit itself").isEqualTo(1);
    assertThat(next.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);

    answerAll(takeSlice());
    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testEverySliceOfADrainGetsItsOwnRetries() {
    // commit-retries is a quota per slice. Counted across the drain, a drain of fifty slices would
    // fail its cycle on the third conflict, however far apart the three were
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    TableCommitRequest request = request(updates(1L, 2L, 3L));
    committer.commit(request);

    // two retries configured, and each of the three slices conflicts once
    for (long id = 1; id <= 3; id++) {
      assertThat(sliceCount()).as("slice of key %d handed out", id).isEqualTo(1);
      RewriteAssigned attempt = takeSlice();
      List<List<DataFile>> written = Lists.newArrayList();
      attempt.assignments().forEach(assignment -> written.add(rewriteFor(attempt, assignment)));
      appendRows(row(id, "concurrent"));
      for (int i = 0; i < written.size(); i++) {
        committer.receive(
            envelope(answer(attempt, attempt.assignments().get(i).taskId(), written.get(i))));
      }

      assertThat(sliceCount())
          .as("slice of key %d replanned after its one conflict", id)
          .isEqualTo(1);
      answerAll(takeSlice());
    }

    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2", "3=v2");
    assertThat(assigned).isEmpty();
    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
  }

  @Test
  public void testAConflictDeletesTheFilesOfItsAttemptAndKeepsTheChangeSet() {
    // the replacement files were written against a base that is gone, so nothing will ever commit
    // them: left behind, they wait in the table's data directory for remove_orphan_files, and the
    // attempt's normalized file waits in staging for the sweep's TTL
    appendRows(row(1L, "a"));
    List<StagedChangeFile> staged = update(1L, "a2");
    committer.commit(request(staged));
    RewriteAssigned first = takeSlice();
    String normalized = first.normalizedRef().location();
    List<List<DataFile>> written = Lists.newArrayList();
    first.assignments().forEach(assignment -> written.add(rewriteFor(first, assignment)));
    List<DataFile> attemptFiles = Lists.newArrayList(Iterables.concat(written));
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(attemptFiles).as("the attempt wrote replacement files").isNotEmpty();
    assertThat(io.fileExists(normalized)).isTrue();

    appendRows(row(1L, "concurrent"));
    for (int i = 0; i < written.size(); i++) {
      committer.receive(
          envelope(answer(first, first.assignments().get(i).taskId(), written.get(i))));
    }

    assertThat(attemptFiles)
        .as("replacement files of the attempt that conflicted")
        .allSatisfy(file -> assertThat(io.fileExists(file.location())).isFalse());
    assertThat(io.fileExists(normalized))
        .as("normalized file of the attempt that conflicted")
        .isFalse();
    // the retry normalizes the slice again from the staged files, so they and the manifest stay
    assertThat(staged).allSatisfy(file -> assertThat(io.fileExists(file.location())).isTrue());
    assertThat(io.fileExists(manifestLocation(first))).isTrue();
    assertThat(sliceCount()).as("conflict replanned").isEqualTo(1);

    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testACommittedSliceTakesItsNormalizedFileAndTheLastSliceTheChangeSet() {
    // a slice's normalized file is done with once the slice commits. The staged files and the
    // manifest are what the next slice, or a restarted coordinator, drains the rest from
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"));
    List<StagedChangeFile> staged = updates(1L, 2L);
    committer.commit(request(staged));
    InMemoryFileIO io = (InMemoryFileIO) table.io();

    RewriteAssigned firstSlice = takeSlice();
    String firstNormalized = firstSlice.normalizedRef().location();
    assertThat(io.fileExists(firstNormalized)).isTrue();
    answerAll(firstSlice);

    assertThat(io.fileExists(firstNormalized))
        .as("normalized file of the slice that committed")
        .isFalse();
    assertThat(staged).allSatisfy(file -> assertThat(io.fileExists(file.location())).isTrue());
    assertThat(io.fileExists(manifestLocation(firstSlice))).as("manifest mid-drain").isTrue();

    RewriteAssigned lastSlice = takeSlice();
    String lastNormalized = lastSlice.normalizedRef().location();
    assertThat(lastNormalized).isNotEqualTo(firstNormalized);
    assertThat(io.fileExists(lastNormalized)).isTrue();
    answerAll(lastSlice);

    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2");
    assertThat(io.fileExists(lastNormalized))
        .as("normalized file of the slice that drained the change set")
        .isFalse();
    assertThat(staged).allSatisfy(file -> assertThat(io.fileExists(file.location())).isFalse());
    assertThat(io.fileExists(manifestLocation(lastSlice))).isFalse();
  }

  @Test
  public void testAChangeSetThatNeverCommittedTakesItsManifestWithIt() {
    // a drain that fails without ever committing a slice leaves its envelopes in the coordinator's
    // buffer, so the next cycle freezes them again under a new id. The manifest of the attempt that
    // failed is then named by nothing at all (no snapshot, no buffer) and a table failing every
    // cycle would leave one behind per cycle until the orphan TTL collected them
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    committer.commit(request);

    RewriteAssigned first = takeSlice();
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            first.changeSetId());
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(io.fileExists(manifestLocation)).as("frozen and durable").isTrue();

    // a worker fails the slice, and handing it out again fails to start: the drain comes off
    failNextSliceStart();
    committer.receive(envelope(failure(first, first.assignments().get(0).taskId())));

    assertThat(assigned).as("the drain came off").isEmpty();
    assertThat(io.fileExists(manifestLocation))
        .as("nothing points at it any more, so it is not left behind")
        .isFalse();
    // and the change set is genuinely re-frozen from the same envelopes, not lost
    committer.commit(request);
    RewriteAssigned refrozen = takeSlice();
    assertThat(refrozen.changeSetId()).isNotEqualTo(first.changeSetId());
    answerAll(refrozen);
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testADroppedChangeSetKeepsItsManifestUntilTheOneThatAdoptedItsFilesCommits() {
    // a change set frozen under identifier fields the table no longer has, left mid-drain: the
    // latest snapshot of this connector still points at it
    appendRows(row(1L, "a"), row(2L, "b"));
    ChangeSetManifest dropped =
        ChangeSetManifest.freeze(
            table, Set.of(), update(1L, "a2"), Set.of("src-topic"), ImmutableMap.of(0, 1L), null);
    String droppedLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            dropped.changeSetId());
    dropped.write(table.io(), droppedLocation);
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, dropped.changeSetId().toString())
        .commit();

    // its files are frozen into a new change set, whose first slice then never commits
    TableCommitRequest request = request(update(2L, "b2"));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    failNextSliceStart();
    committer.receive(envelope(failure(first, first.assignments().get(0).taskId())));
    assertThat(assigned).as("the drain came off").isEmpty();

    // nothing moved the pointer: the next cycle, or a coordinator restarted here, reads that
    // manifest again. Deleted with the drop, it would fail every cycle from now on
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(io.fileExists(droppedLocation))
        .as("the manifest the snapshot summary still points at")
        .isTrue();

    committer.commit(request);
    answerAll(takeSlice());
    assertThat(readAll()).containsExactlyInAnyOrder("1=a2", "2=b2");
    assertThat(io.fileExists(droppedLocation))
        .as("deleted once a commit moved the pointer off it")
        .isFalse();
  }

  @Test
  public void testATableWhosePointerNamesADroppedChangeSetIsOfferedACommitWithAnEmptyBuffer() {
    // a change set frozen under identifier fields the table no longer has, left mid-drain: its
    // envelopes are spent, and nothing is ever written to the table again
    appendRows(row(1L, "a"));
    ChangeSetManifest dropped =
        ChangeSetManifest.freeze(
            table, Set.of(), update(1L, "a2"), Set.of("src-topic"), ImmutableMap.of(0, 1L), null);
    dropped.write(
        table.io(),
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            dropped.changeSetId()));
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, dropped.changeSetId().toString())
        .commit();

    // a cycle with nothing buffered adopts its files into a new change set, whose first slice
    // then never commits
    TableCommitRequest nothingBuffered = withoutEnvelopes(request(List.of()));
    committer.commit(nothingBuffered);
    RewriteAssigned first = takeSlice();
    assertThat(first.changeSetId()).isNotEqualTo(dropped.changeSetId());
    failNextSliceStart();
    committer.receive(envelope(failure(first, first.assignments().get(0).taskId())));
    assertThat(assigned).as("the drain came off").isEmpty();

    // the pointer still names the dropped change set, and no response in the buffer brings the
    // table back: pendingTables() has to
    assertThat(committer.pendingTables())
        .as("the tail of the dropped change set is still outside the table")
        .contains(TABLE_REFERENCE);

    committer.commit(nothingBuffered);
    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
    assertThat(committer.pendingTables()).doesNotContain(TABLE_REFERENCE);
  }

  @Test
  public void testNoActiveTasksDefersInsteadOfDroppingTheResponses() {
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(stagedFiles(update(1L, "a2")));
    TableCommitRequest noTasks =
        new TableCommitRequest(
            request.tableReference(),
            request.envelopes(),
            request.controlTopicOffsets(),
            request.commitId(),
            request.validThroughTs(),
            ImmutableMap.of());

    CommitResult result = committer.commit(noTasks);

    // nobody could have rewritten anything, so the responses must stay buffered: reporting them
    // consumed would drop the rows outright
    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(result.consumed()).isEmpty();
    assertThat(assigned).isEmpty();

    // and the next cycle, with tasks present, applies them
    committer.commit(request);
    answerAll(takeSlice());
    assertThat(readAll()).containsExactly("1=a2");
  }

  @Test
  public void testACycleWithNoTasksKeepsTheDrainOnItsLastActiveTasks() {
    // a partial cycle in the middle of a rebalance can close before any task reports in. The drain
    // is already out with the active tasks of the last cycle; forgetting them would leave the next
    // slice nobody to go to
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"));
    TableCommitRequest request = request(updates(1L, 2L));
    committer.commit(request);
    RewriteAssigned first = takeSlice();

    CommitResult partial = committer.commit(withTasks(request));
    assertThat(partial.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(assigned).isEmpty();

    answerAll(first);
    assertThat(sliceCount()).as("the next slice handed out").isEqualTo(1);
    RewriteAssigned second = takeSlice();
    assertThat(second.changeSetId()).isEqualTo(first.changeSetId());
    assertThat(second.assignments())
        .as("to the last cycle's active tasks")
        .extracting(Assignment::taskId)
        .containsExactlyInAnyOrder("task-0", "task-1");
    assertThat(second.ownerCount()).isEqualTo(first.ownerCount());

    answerAll(second);
    assertThat(committer.commit(request).outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2");
  }

  @Test
  public void testActiveTasksThatShrankMidDrainGetTheNextSlice() {
    // the active tasks are refreshed on every cycle of a drain, not pinned when it starts: a task
    // gone in a rebalance would otherwise be handed every slice left and time each of them out
    when(config.copyOnWriteMaxChangeSetRecords()).thenReturn(1L);
    appendRows(row(1L, "a"), row(2L, "b"));
    TableCommitRequest request = request(updates(1L, 2L));
    committer.commit(request);
    RewriteAssigned first = takeSlice();

    committer.commit(withTasks(request, "task-1"));
    assertThat(assigned).as("the slice out stays with the tasks it went to").isEmpty();

    answerAll(first);
    assertThat(sliceCount()).isEqualTo(1);
    RewriteAssigned second = takeSlice();
    assertThat(second.assignments())
        .as("to this cycle's active tasks")
        .extracting(Assignment::taskId)
        .containsExactly("task-1");
    assertThat(second.ownerCount()).isEqualTo(1);

    answerAll(second);
    assertThat(readAll()).containsExactlyInAnyOrder("1=v2", "2=v2");
  }

  @Test
  public void testASliceWaitingForACycleWithNoTasksGoesToTheLastActiveTasks() {
    // a slice that timed out waits for the next cycle. If that cycle has no active tasks, the
    // drain keeps the active tasks from its last cycle rather than waiting for one that does
    appendRows(row(1L, "a"));
    TableCommitRequest request = request(update(1L, "a2"));
    committer.commit(request);
    RewriteAssigned first = takeSlice();
    committer.process(System.currentTimeMillis() + TIMEOUT_MS + 1, ImmutableList::of);
    assertThat(assigned).isEmpty();

    CommitResult next = committer.commit(withTasks(request));
    assertThat(next.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(sliceCount()).as("handed out by the cycle with no tasks").isEqualTo(1);
    RewriteAssigned retried = takeSlice();
    assertThat(retried.sliceSeq()).isEqualTo(first.sliceSeq() + 1);
    assertThat(retried.assignments())
        .extracting(Assignment::taskId)
        .containsExactlyInAnyOrder("task-0", "task-1");

    answerAll(retried);
    assertThat(readAll()).containsExactly("1=a2");
  }

  // -- protocol helpers -------------------------------------------------------------------------

  /**
   * One slice's messages, merged back into a single event.
   *
   * <p>The coordinator sends one {@code RewriteAssigned} per task, so a slice arrives as a run of
   * messages sharing a change set and a sequence number. Merging them is a convenience for the
   * assertions below, which are about the protocol's decisions rather than its framing; {@link
   * #sliceCount()} is what checks the framing itself.
   */
  private RewriteAssigned takeSlice() {
    RewriteAssigned head = assigned.remove(0);
    List<Assignment> all = Lists.newArrayList(head.assignments());
    while (!assigned.isEmpty()
        && assigned.get(0).changeSetId().equals(head.changeSetId())
        && assigned.get(0).sliceSeq() == head.sliceSeq()) {
      all.addAll(assigned.remove(0).assignments());
    }
    all = mergedChunks(all);
    return new RewriteAssigned(
        table.spec().partitionType(),
        head.commitId(),
        head.tableReference(),
        head.changeSetId(),
        head.sliceSeq(),
        head.baseSnapshotId(),
        head.normalizedRef(),
        all,
        head.ownerIndex(),
        head.ownerCount(),
        head.identifierFieldIds());
  }

  /**
   * One assignment per task, as the workers assemble it: an assignment sent as several chunks
   * arrives as one record per chunk, each with part of the files.
   */
  private static List<Assignment> mergedChunks(List<Assignment> assignments) {
    Map<String, List<FileScanTaskDescriptor>> files = Maps.newLinkedHashMap();
    Map<String, Assignment> first = Maps.newLinkedHashMap();
    for (Assignment assignment : assignments) {
      first.putIfAbsent(assignment.taskId(), assignment);
      files
          .computeIfAbsent(assignment.taskId(), taskId -> Lists.newArrayList())
          .addAll(assignment.files());
    }
    return first.entrySet().stream()
        .map(
            entry ->
                new Assignment(
                    entry.getKey(),
                    entry.getValue().expectedPartitions(),
                    files.get(entry.getKey())))
        .collect(Collectors.toList());
  }

  /** The messages of the one round of assignments the committer has sent. */
  private List<Event> assignmentRound() {
    List<List<Event>> assignmentRounds =
        rounds.stream()
            .filter(
                round ->
                    round.stream()
                        .anyMatch(event -> event.payload().type() == PayloadType.REWRITE_ASSIGNED))
            .collect(Collectors.toList());
    assertThat(assignmentRounds).as("rounds of assignments sent").hasSize(1);
    return assignmentRounds.get(0);
  }

  private static List<Integer> encodedSizes(List<Event> round) {
    return round.stream().map(event -> AvroUtil.encode(event).length).collect(Collectors.toList());
  }

  /** The messages of one task's assignment, in the order they were sent. */
  private List<RewriteAssigned> chunksOf(String taskId) {
    return assigned.stream()
        .filter(
            event ->
                event.assignments().stream()
                    .anyMatch(assignment -> assignment.taskId().equals(taskId)))
        .collect(Collectors.toList());
  }

  /** How many distinct slices are waiting, regardless of how many messages each was sent as. */
  private long sliceCount() {
    return assigned.stream()
        .map(event -> event.changeSetId() + "#" + event.sliceSeq())
        .distinct()
        .count();
  }

  /** Answers every assignment of a slice, rewriting for real, as the workers would. */
  private void answerAllExcept(RewriteAssigned event, String taskId) {
    for (Assignment assignment : event.assignments()) {
      if (!assignment.taskId().equals(taskId)) {
        committer.receive(
            envelope(answer(event, assignment.taskId(), rewriteFor(event, assignment))));
      }
    }
  }

  private void answerAll(RewriteAssigned event) {
    for (Assignment assignment : event.assignments()) {
      List<DataFile> written = rewriteFor(event, assignment);
      committer.receive(envelope(answer(event, assignment.taskId(), written)));
    }
  }

  /**
   * Answers a slice of key 1 in full after a competing writer landed a row with that key, so its
   * commit conflicts.
   */
  private void answerAllOverAConflict(RewriteAssigned event, String concurrentData) {
    List<DataFile> written = rewriteFor(event);
    appendRows(row(1L, concurrentData));
    for (Assignment assignment : event.assignments()) {
      committer.receive(
          envelope(
              answer(
                  event,
                  assignment.taskId(),
                  assignment.taskId().equals(event.assignments().get(0).taskId())
                      ? written
                      : ImmutableList.of())));
    }
  }

  /** Has the next slice fail to start, once: a transient failure inside a transition. */
  private void failNextSliceStart() {
    long maxRecords = config.copyOnWriteMaxChangeSetRecords();
    when(config.copyOnWriteMaxChangeSetRecords())
        .thenThrow(new UncheckedIOException(new IOException("storage unavailable")))
        .thenReturn(maxRecords);
  }

  /** {@code request} as a cycle whose active tasks are exactly {@code taskIds}. */
  private static TableCommitRequest withTasks(TableCommitRequest request, String... taskIds) {
    ImmutableMap.Builder<String, List<TopicPartitionRef>> tasks = ImmutableMap.builder();
    for (String taskId : taskIds) {
      tasks.put(taskId, ImmutableList.of());
    }
    return new TableCommitRequest(
        request.tableReference(),
        request.envelopes(),
        request.controlTopicOffsets(),
        request.commitId(),
        request.validThroughTs(),
        tasks.build());
  }

  /** {@code request} as a cycle with nothing of the table in the commit buffer. */
  private static TableCommitRequest withoutEnvelopes(TableCommitRequest request) {
    return new TableCommitRequest(
        request.tableReference(),
        ImmutableList.of(),
        request.controlTopicOffsets(),
        request.commitId(),
        request.validThroughTs(),
        request.activeTasks());
  }

  /**
   * {@code body} called from a thread standing in for the coordinator's: a call that waits fails
   * the test instead of hanging it.
   */
  private static <T> T promptly(ExecutorService thread, String call, Callable<T> body)
      throws Exception {
    Future<T> result = thread.submit(body);
    assertThat(result).as("%s returns without waiting", call).succeedsWithin(Duration.ofSeconds(5));
    return result.get();
  }

  /** The whole slice rewritten by its first owner, for tests that only need one answer's files. */
  private List<DataFile> rewriteFor(RewriteAssigned event) {
    return rewriteFor(event, event.assignments().get(0));
  }

  private List<DataFile> rewriteFor(RewriteAssigned event, Assignment assignment) {
    Table target = catalog.loadTable(event.tableReference().identifier());
    // from the message, exactly as RewriteAssignmentRunner does: the coordinator's identifier
    // fields decide key order, and a worker resolving its own could partition the slice
    // differently
    Set<Integer> identifierFieldIds = Sets.newHashSet(event.identifierFieldIds());
    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(
            target,
            identifierFieldIds,
            ImmutableList.of(event.normalizedRef()),
            null,
            Long.MAX_VALUE);
    List<FileScanTask> files = AssignedFileScanTask.from(target, assignment.files());
    List<String> owners =
        event.assignments().stream()
            .map(Assignment::taskId)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    return new CopyOnWriteRewriter(target, 1)
        .rewrite(
            event.baseSnapshotId(),
            files,
            slice,
            owners.indexOf(assignment.taskId()),
            owners.size());
  }

  private Event chunk(
      RewriteAssigned event, String taskId, List<DataFile> files, int chunkIndex, int chunkCount) {
    return new Event(
        GROUP_ID,
        new RewriteComplete(
            table.spec().partitionType(),
            event.commitId(),
            event.tableReference(),
            event.changeSetId(),
            event.sliceSeq(),
            taskId,
            RewriteComplete.STATUS_OK,
            files,
            chunkIndex,
            chunkCount));
  }

  private Event answer(RewriteAssigned event, String taskId, List<DataFile> files) {
    return new Event(
        GROUP_ID,
        new RewriteComplete(
            table.spec().partitionType(),
            event.commitId(),
            event.tableReference(),
            event.changeSetId(),
            event.sliceSeq(),
            taskId,
            RewriteComplete.STATUS_OK,
            files,
            0,
            1));
  }

  private Event failure(RewriteAssigned event, String taskId) {
    return failure(event, taskId, RewriteComplete.FAILURE_RETRYABLE);
  }

  private Event failure(RewriteAssigned event, String taskId, int failureKind) {
    return new Event(
        GROUP_ID,
        new RewriteComplete(
            table.spec().partitionType(),
            event.commitId(),
            event.tableReference(),
            event.changeSetId(),
            event.sliceSeq(),
            taskId,
            RewriteComplete.STATUS_FAILED,
            ImmutableList.of(),
            0,
            1,
            failureKind));
  }

  private Envelope envelope(Event event) {
    return new Envelope(roundTrip(event), 0, 0);
  }

  private static Event roundTrip(Event event) {
    return AvroUtil.decode(AvroUtil.encode(event));
  }

  // -- fixtures ---------------------------------------------------------------------------------

  private TableCommitRequest request(List<StagedChangeFile> files) {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), TABLE_REFERENCE, "task-0", List.of("src-topic"), files);
    Envelope envelope = new Envelope(new Event(GROUP_ID, payload), 0, 5L);
    return new TableCommitRequest(
        TABLE_REFERENCE,
        List.of(envelope),
        ImmutableMap.of(0, 6L),
        UUID.randomUUID(),
        OffsetDateTime.now(),
        ImmutableMap.of(
            "task-0", ImmutableList.<TopicPartitionRef>of(),
            "task-1", ImmutableList.<TopicPartitionRef>of()));
  }

  private List<StagedChangeFile> stagedFiles(long id, int op, String data) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, TABLE_REFERENCE, ID_FIELDS, null, GROUP_ID, "task-0");
    GenericRecord tableRow = GenericRecord.create(SCHEMA);
    tableRow.setField("id", id);
    tableRow.setField("data", data);
    writer.write(writer.stagedRow(tableRow, op, "src-topic", 0, 0L));
    return writer.complete();
  }

  private List<StagedChangeFile> update(long id, String data) {
    return stagedFiles(id, StagedChangeSchema.OP_UPDATE, data);
  }

  /** One staged file: {@code updatedId} updated, then {@code deletedId} deleted. */
  private List<StagedChangeFile> updateThenDelete(long updatedId, String data, long deletedId) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, TABLE_REFERENCE, ID_FIELDS, null, GROUP_ID, "task-0");
    writer.write(
        writer.stagedRow(row(updatedId, data), StagedChangeSchema.OP_UPDATE, "src-topic", 0, 0L));
    writer.write(
        writer.stagedRow(row(deletedId, null), StagedChangeSchema.OP_DELETE, "src-topic", 0, 1L));
    return writer.complete();
  }

  /** One staged file updating each of {@code ids} to {@code v2}. */
  private List<StagedChangeFile> updates(long... ids) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, TABLE_REFERENCE, ID_FIELDS, null, GROUP_ID, "task-0");
    for (long id : ids) {
      writer.write(
          writer.stagedRow(row(id, "v2"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, id));
    }
    return writer.complete();
  }

  private String manifestLocation(RewriteAssigned slice) {
    return ChangeSetManifest.location(
        StagedChangeFileWriter.stagingLocation(table, null),
        TABLE_REFERENCE,
        GROUP_ID,
        slice.changeSetId());
  }

  private List<StagedChangeFile> stagedFiles(List<StagedChangeFile> files) {
    return files;
  }

  private void appendRows(Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).build(),
            OutputFileFactory.builderFor(table, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            table.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(table.spec(), table.schema());
        key.partition(row);
        writer.write(row, table.spec(), key);
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    AppendFiles append = table.newAppend();
    writer.result().dataFiles().forEach(append::appendFile);
    append.commit();
    table.refresh();
  }

  /** A merge-on-read delete of one row: a position delete file naming {@code file}, committed. */
  private void deletePosition(DataFile file, long position) {
    PositionDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .build()
            .newPositionDeleteWriter(
                OutputFileFactory.builderFor(table, 1, 1).build().newOutputFile(),
                table.spec(),
                null);
    try (PositionDeleteWriter<Record> open = writer) {
      open.write(PositionDelete.<Record>create().set(file.location(), position, null));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    table.refresh();
  }

  private Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private List<String> readAll() {
    table.refresh();
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(record.get(0, Object.class) + "=" + record.get(1, Object.class));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private List<String> readAllOnRef(String ref) {
    table.refresh();
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).useSnapshot(table.snapshot(ref).snapshotId()).build()) {
      for (Record record : records) {
        rows.add(record.get(0, Object.class) + "=" + record.get(1, Object.class));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  /** The data files the table's current snapshot is made of. */
  private List<String> committedLocations() {
    table.refresh();
    List<String> locations = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        locations.add(task.file().location());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return locations;
  }

  private long countSnapshots() {
    table.refresh();
    return Lists.newArrayList(table.snapshots()).size();
  }
}
