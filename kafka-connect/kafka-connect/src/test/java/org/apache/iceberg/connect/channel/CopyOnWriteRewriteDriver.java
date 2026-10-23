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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.data.copyonwrite.AssignedFileScanTask;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetNormalizer;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.CopyOnWriteRewriter;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Plays the coordinator and every worker around a real {@link CopyOnWriteTableCommitter}, so a test
 * can ask for a commit and get back the outcome the whole protocol arrived at.
 *
 * <p>The committer is a state machine: {@code commit()} starts a slice and returns {@code PENDING},
 * and the slice only finishes when the workers' answers come back. In production that loop is
 * {@code Coordinator.process()}; here it is {@link #commit}, which runs the assignments through the
 * real {@link CopyOnWriteRewriter}, feeds the answers back through {@link
 * CopyOnWriteTableCommitter#receive} and repeats until the drain ends. Answers make the round trip
 * through {@link AvroUtil}, so what the committer sees is what the control topic would have
 * delivered, not the objects it sent.
 *
 * <p>This is what lets the same scenarios run against any number of tasks: with {@code taskCount >
 * 1} the plan is split and the slice's keys are owned by different workers, exactly as in a real
 * multi-task rewrite.
 */
class CopyOnWriteRewriteDriver {

  private static final int MAX_SLICES = 1000;

  private final CopyOnWriteTableCommitter committer;
  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final List<RewriteAssigned> inbox = Lists.newArrayList();
  private final List<RewriteAssigned> handedOut = Lists.newArrayList();
  private final Map<String, List<TopicPartitionRef>> activeTasks;

  CopyOnWriteRewriteDriver(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<Event> eventSink,
      int taskCount) {
    this.catalog = catalog;
    this.config = config;
    this.activeTasks = Maps.newLinkedHashMap();
    for (int i = 0; i < taskCount; i++) {
      activeTasks.put("task-" + i, ImmutableList.of());
    }
    this.committer =
        new CopyOnWriteTableCommitter(
            catalog,
            config,
            metadataEvents,
            events -> {
              for (Event event : events) {
                if (event.payload().type() == PayloadType.REWRITE_ASSIGNED) {
                  RewriteAssigned assigned = (RewriteAssigned) roundTrip(event).payload();
                  handedOut.add(assigned);
                  inbox.add(assigned);
                } else {
                  eventSink.accept(event);
                }
              }
            },
            // inline: the driver is single threaded, and a transition submitted from receive() has
            // to have run by the time receive() returns for the loop below to see its effect
            Runnable::run,
            () -> true);
  }

  /**
   * Commits, answers exactly one slice, and stops: the coordinator dying right after a slice
   * landed. Whatever the committer handed out next is left unanswered, so a fresh driver over the
   * same catalog resumes from the persisted state alone.
   */
  void commitOneSlice(TableCommitRequest request) {
    committer.commit(withActiveTasks(request));
    if (inbox.isEmpty()) {
      return;
    }

    // a slice arrives as one message per task; answering the last of them may hand out the next
    // slice, which this method deliberately leaves unanswered
    List<RewriteAssigned> slice = Lists.newArrayList(inbox);
    inbox.clear();
    slice.forEach(this::answer);
  }

  /**
   * Commits and hands out whatever slice that started, leaving it unanswered: the coordinator that
   * dies between handing a slice out and hearing back, with its workers still rewriting. The
   * assignments come back so that another driver (the coordinator that took over) can be given the
   * answer to them.
   */
  List<RewriteAssigned> handOutOneSlice(TableCommitRequest request) {
    committer.commit(withActiveTasks(request));
    List<RewriteAssigned> slice = ImmutableList.copyOf(inbox);
    inbox.clear();
    return slice;
  }

  /** Answers assignments to this committer, whichever coordinator handed them out. */
  void answerAssignments(List<RewriteAssigned> slice) {
    slice.forEach(this::answer);
  }

  Set<TableReference> pendingTables() {
    return committer.pendingTables();
  }

  Collection<Envelope> replayAnchors() {
    return committer.replayAnchors();
  }

  /** The coordinator stopping, as {@code Coordinator.terminate()} tells its committer. */
  void stop() {
    committer.stop();
  }

  /** Every assignment handed out so far, answered or not, in the order it went out. */
  List<RewriteAssigned> handedOut() {
    return ImmutableList.copyOf(handedOut);
  }

  /** Commits once in a cycle no task reported in to, as a partial cycle after a restart may be. */
  TableCommitter.Outcome commitWithNoTasks(TableCommitRequest request) {
    return committer.commit(request).outcome();
  }

  /**
   * Commits once, with this driver's active tasks, as a single coordinator cycle would: no looping
   * to a terminal outcome. Answers whatever slice this call handed out, as {@link #commitOneSlice}
   * does, so a slice that lands within this same call is reflected in the table; the outcome
   * returned is the one this call's {@code commit()} itself reported, before any such answer.
   */
  TableCommitter.Outcome commitOnce(TableCommitRequest request) {
    TableCommitter.Outcome outcome = committer.commit(withActiveTasks(request)).outcome();
    if (!inbox.isEmpty()) {
      List<RewriteAssigned> slice = Lists.newArrayList(inbox);
      inbox.clear();
      slice.forEach(this::answer);
    }
    return outcome;
  }

  private TableCommitRequest withActiveTasks(TableCommitRequest request) {
    return new TableCommitRequest(
        request.tableReference(),
        request.envelopes(),
        request.controlTopicOffsets(),
        request.commitId(),
        request.validThroughTs(),
        activeTasks);
  }

  /** Commits and drives the protocol to a terminal outcome, as several coordinator cycles would. */
  TableCommitter.Outcome commit(TableCommitRequest request) {
    TableCommitRequest withActiveTasks = withActiveTasks(request);

    for (int cycle = 0; cycle < MAX_SLICES; cycle++) {
      CommitResult result = committer.commit(withActiveTasks);
      runAssignments();
      if (result.outcome() != TableCommitter.Outcome.PENDING) {
        return result.outcome();
      }
    }
    throw new IllegalStateException("Drain did not terminate");
  }

  /** Runs everything the committer handed out, and everything that handing it back produces. */
  private void runAssignments() {
    for (int message = 0; message < MAX_SLICES && !inbox.isEmpty(); message++) {
      answer(inbox.remove(0));
      committer.process(System.currentTimeMillis(), ImmutableList::of);
    }
  }

  /** One worker's share of one slice: rewrite it for real, answer over the wire. */
  private void answer(RewriteAssigned assigned) {
    Assignment assignment = assigned.assignments().get(0);
    Table table = catalog.loadTable(assigned.tableReference().identifier());
    // from the message, exactly as RewriteAssignmentRunner does
    Set<Integer> identifierFieldIds = Sets.newHashSet(assigned.identifierFieldIds());

    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(
            table,
            identifierFieldIds,
            ImmutableList.of(assigned.normalizedRef()),
            null,
            Long.MAX_VALUE);

    List<FileScanTask> myFiles = AssignedFileScanTask.from(table, assignment.files());

    List<DataFile> written =
        new CopyOnWriteRewriter(table, 1)
            .rewrite(
                assigned.baseSnapshotId(),
                myFiles,
                slice,
                assigned.ownerIndex(),
                assigned.ownerCount());

    Event answer =
        new Event(
            config.connectGroupId(),
            new RewriteComplete(
                table.spec().partitionType(),
                assigned.commitId(),
                assigned.tableReference(),
                assigned.changeSetId(),
                assigned.sliceSeq(),
                assignment.taskId(),
                RewriteComplete.STATUS_OK,
                written,
                0,
                1));
    committer.receive(new Envelope(roundTrip(answer), 0, 0));
  }

  /** Through the wire format and back: the committer must cope with what Avro gives it. */
  private static Event roundTrip(Event event) {
    return AvroUtil.decode(AvroUtil.encode(event));
  }
}
