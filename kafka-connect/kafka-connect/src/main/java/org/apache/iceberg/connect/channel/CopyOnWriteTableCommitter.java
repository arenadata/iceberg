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
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.data.RecordRoutingStrategy;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetNormalizer;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.data.copyonwrite.PlanSummary;
import org.apache.iceberg.connect.data.copyonwrite.RewriteAssigner;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives the copy-on-write commit of every table: freezes {@link RowChangesWritten} responses into
 * a change-set manifest, then drains it one normalized slice at a time: plan the affected files,
 * hand them to the workers, commit what comes back through {@code OverwriteFiles} (or {@code
 * AppendFiles} for an empty table or a not-yet-created branch), until the change set is exhausted.
 *
 * <p>A state machine, not a blocking call: {@code commit()} is made from {@code doCommit()}, which
 * keeps {@code Coordinator.process()} from consuming the answers, so it starts a slice and returns
 * {@link Outcome#PENDING}; {@link #receive} collects the answers, and {@link #process} times a
 * stuck slice out. Per-table state lives in {@link TableDrainState}, under its lock: {@code
 * commit()} runs on the commit pool and takes it, {@code receive()} and {@code process()} run on
 * the coordinator thread and only {@code tryLock}. A transition can hold the lock for minutes
 * (planning a slice, an Iceberg commit retrying) and a coordinator thread waiting on it would miss
 * {@code max.poll.interval.ms} and leave the group; an answer arriving meanwhile is queued for the
 * next tick.
 *
 * <p>The persisted side of a change set is {@link ChangeSetStore}, the Iceberg commit of a slice
 * {@link SliceCommitter}, what the coordinator's staging cleanup is to leave alone {@link
 * CopyOnWriteStagingCleanupSource}. {@link #discardSliceFiles} stays private to the state machine:
 * it is where a replacement file changes owner, and no collaborator may reach a data file.
 *
 * <p>With the statistics of a table on, the final snapshot F of a drain of change set X gets a
 * statistics step. An F whose step did not finish is pending: X stays, and the recount of F runs
 * the step again from the manifest of X. Both are {@link CopyOnWriteFinalStatistics}'; when they
 * run, under the lock of the table, is the state machine's.
 */
class CopyOnWriteTableCommitter implements TableCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteTableCommitter.class);

  private static final long NO_BASE_SNAPSHOT = -1L;
  private static final String CHANGESETS_NORMALIZED_AVRO_FILE_NAME_PATH_FORMAT =
      "%s/_changesets/normalized-%s-%d.avro";

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final MetadataEvents metadataEvents;
  private final Consumer<List<Event>> eventSender;
  private final Executor exec;
  private final ChangeSetStore changeSets;
  private final SliceCommitter sliceCommitter;
  private final SlicePlanner planner;
  private final CopyOnWriteStagingCleanupSource stagingCleanupSource;
  private final MergeOnReadTailHandoff mergeOnReadTail;
  private final TableDrainRegistry states;
  // the statistics lag of copy-on-write: entered at startup, by the step of the final snapshot
  // of a drain, by a final slice with no snapshot to step on, and by the merge-on-read tail
  private final StatisticsLag statisticsLag = new StatisticsLag();
  private final StatisticsHealth health;
  private final CopyOnWriteFinalStatistics finalStatistics;
  private final CopyOnWriteLagStepRunner lagStepRunner;
  private volatile boolean stopped;

  CopyOnWriteTableCommitter(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<List<Event>> eventSender,
      Executor exec) {
    this(
        catalog,
        config,
        metadataEvents,
        eventSender,
        exec,
        StatisticsHealth.NOOP,
        new StatisticsStep());
  }

  /**
   * @param exec the transition pool
   */
  CopyOnWriteTableCommitter(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<List<Event>> eventSender,
      Executor exec,
      StatisticsHealth health,
      StatisticsStep step) {
    this(
        catalog,
        config,
        metadataEvents,
        eventSender,
        exec,
        health,
        step,
        LoggerFactory.getLogger(CopyOnWriteFinalStatistics.class));
  }

  @VisibleForTesting
  CopyOnWriteTableCommitter(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<List<Event>> eventSender,
      Executor exec,
      StatisticsHealth health,
      StatisticsStep step,
      Logger finalStatisticsLog) {
    this.catalog = catalog;
    this.config = config;
    this.metadataEvents = metadataEvents;
    this.eventSender = eventSender;
    this.exec = exec;
    this.health = health;
    Set<TableIdentifier> configuredTables = config.configuredTables();
    this.states = new TableDrainRegistry(configuredTables);
    this.changeSets = new ChangeSetStore(config);
    this.sliceCommitter = new SliceCommitter(config);
    this.planner = new SlicePlanner(config);
    this.finalStatistics =
        new CopyOnWriteFinalStatistics(
            config, changeSets, statisticsLag, health, step, () -> stopped, finalStatisticsLog);
    this.mergeOnReadTail =
        new MergeOnReadTailHandoff(
            new MergeOnReadTableCommitter(
                catalog,
                config,
                metadataEvents,
                event -> eventSender.accept(ImmutableList.of(event)),
                exec,
                health,
                step,
                statisticsLag,
                MergeOnReadTableCommitter.Mode.TAIL));
    this.lagStepRunner =
        new CopyOnWriteLagStepRunner(
            catalog,
            config,
            exec,
            states,
            statisticsLag,
            health,
            finalStatistics,
            mergeOnReadTail::hasStep,
            () -> stopped);
    this.stagingCleanupSource =
        new CopyOnWriteStagingCleanupSource(
            config,
            changeSets,
            states.view(),
            catalog::loadTable,
            mergeOnReadTail::inFlightSketchLocations);
    if (config.statisticsInUse()) {
      // the static tables with statistics start in the lag, not verified until the quick check
      statisticsLag.enterAtStartup(
          configuredTables.stream()
              .filter(id -> config.tableConfig(id.toString()).statisticsEnabled())
              .collect(Collectors.toList()));
    }
  }

  private <T> T underLock(TableReference tableReference, Function<TableDrainState, T> read) {
    TableDrainState state = states.get(tableReference);
    state.lock.lock();
    try {
      return read.apply(state);
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void stop() {
    stopped = true;
    mergeOnReadTail.stop();
    LOG.info(
        "Coordinator is stopping: copy-on-write transitions in flight start no commit, delete or "
            + "send from here on");
  }

  @Override
  public CommitResult commit(TableCommitRequest request) {
    // read once: a lag step on the pool may remove the state of a recreated table between two
    // reads, and what was found here is what commitTable goes on with
    TableDrainState state = states.get(request.tableReference());
    if (state != null) {
      return commitTable(request, state);
    }

    return request.envelopes().isEmpty() ? commitWithoutState(request) : commitTable(request);
  }

  /**
   * What {@link #pendingTables()} lists for a table not looked at yet, in the statistics lag or due
   * a re-check of its health has no state of its own, and gets none in {@link #commitTable}: the
   * reference lacks the table's UUID, and a drain under it would run beside the one the workers'
   * responses start under theirs.
   */
  private CommitResult commitWithoutState(TableCommitRequest request) {
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    // the lag token, taken before the table is loaded. Whatever removes the lag below does
    // it by
    // this token, and an evaluation by the head applies only at this version
    long lagToken = statisticsLag.token(tableIdentifier);
    long healthVersion = health.version(tableIdentifier);
    if (states.unresumed(tableIdentifier)) {
      return resumeAfterRestart(request, lagToken, healthVersion);
    }

    return lagStepRunner.commitWithoutResponses(request, null, lagToken, healthVersion);
  }

  /**
   * The first visit of this coordinator to a table the configuration names, whether or not anything
   * is buffered for it.
   *
   * <p>A drain a previous coordinator left half applied has spent its responses, so nothing in the
   * buffer names the table, and one whose stream has gone quiet would never be offered a commit
   * again. So {@link #pendingTables()} lists every configured table under a reference without a
   * UUID (only a loaded table can supply one) until its pointer has been read, and a drain to
   * resume runs under the reference a worker names the table by. A table routed dynamically is not
   * named here: its replay anchor (see {@link #replayAnchors()}) brings it back.
   *
   * <p>A table with a drain to resume has its state made first, under the reference with the UUID,
   * as {@link #commitTable} requires of a request without responses.
   */
  private CommitResult resumeAfterRestart(
      TableCommitRequest request, long lagToken, long healthVersion) {
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      // nothing to resume: once created and written to, it starts the ordinary way
      states.resumed(tableIdentifier);
      return CommitResult.committed(ImmutableList.of());
    }

    // before checking the active tasks, so that only a table with a drain to resume waits for a
    // cycle with tasks; and no state for the others, which would put every configured table under
    // the staging cleanup
    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();
    if (!changeSets.pointsAtChangeSet(table, branch)) {
      states.resumed(tableIdentifier);
      return lagStepRunner.commitWithoutResponses(request, table, lagToken, healthVersion);
    }

    // startDrain takes the table off the unresumed tables once it has read the pointer; until
    // then (a cycle with no tasks defers before reading it) the table is listed again, and finds
    // the state made here
    TableReference tableReference =
        TableReference.of(request.tableReference().catalog(), tableIdentifier, table.uuid());
    states.getOrCreate(tableReference);
    return commitTable(
        new TableCommitRequest(
            tableReference,
            request.envelopes(),
            request.controlTopicOffsets(),
            request.commitId(),
            request.validThroughTs(),
            request.activeTasks()));
  }

  /**
   * Under dynamic routing, holds the control topic at the latest response of the table in {@code
   * envelopes}, if there is one. A table the configuration names needs no anchor: it is probed
   * after a restart. A merge-on-read tail is not one: the start spends it outright.
   */
  private void anchorAt(TableDrainState state, List<Envelope> envelopes) {
    if (config.routingStrategy() != RecordRoutingStrategy.DYNAMIC_FIELD) {
      return;
    }
    Envelope latest =
        Envelopes.latest(
            envelopes.stream()
                .filter(
                    envelope ->
                        envelope.event().payload().type() == PayloadType.ROW_CHANGES_WRITTEN)
                .collect(Collectors.toList()));
    if (latest != null) {
      state.replayAnchor = latest;
    }
  }

  @VisibleForTesting
  CommitResult commitTable(TableCommitRequest request) {
    checkResponsesOrState(request);
    return commitTable(request, states.getOrCreate(request.tableReference()));
  }

  private CommitResult commitTable(TableCommitRequest request, TableDrainState state) {
    // tryLock, never lock: doCommit() joins this pool from the coordinator thread, and what holds
    // the lock is a slice being planned or an Iceberg commit retrying, for minutes. A table
    // mid-transition is skipped: its envelopes stay buffered, its offsets held, and pendingTables()
    // brings it back next cycle
    if (!state.lock.tryLock()) {
      return CommitResult.pending(ImmutableList.of());
    }
    try {
      // a request without responses found a state that a lag step removed since (the table was
      // created again): there is no drain to go on with, and the lag check takes the request
      if (removedUnderRequestWithoutResponses(request, state)) {
        return commitWithoutState(request);
      }

      // reported once, whether or not this cycle does anything else: the change set that spent
      // them is persisted, so the coordinator may drop them from its buffer
      List<Envelope> consumed = takeConsumed(state);

      states.stopIfSecondNameOfSameTable(state);

      if (state.stopped()) {
        // already reported at ERROR, once, when it happened. Repeating it every commit interval
        // would bury the one message that says what to fix
        return CommitResult.failed(consumed);
      }

      refreshActiveTasks(state, request);

      if (state.active()) {
        retrySliceThatWaited(state);
        return CommitResult.pending(consumed);
      }

      // what starting the drain spent outright: a tail of merge-on-read it committed, or envelopes
      // it found already applied. Spent whatever happens after, so it is reported on every path
      List<Envelope> spentByStart = Lists.newArrayList();
      try {
        DrainStartOutcome started = startDrain(state, request, spentByStart);
        if (started != DrainStartOutcome.DEFERRED) {
          clearRejection(state);
        }
        if (started == DrainStartOutcome.NOTHING_TO_DO) {
          // around a merge-on-read tail, startDrain may have spent only part of what the request
          // offered, holding the rest back for its own turn. That part is still owed a commit:
          // reporting COMMITTED would let CommitComplete's valid-through claim more than the table
          // has applied
          return Envelopes.without(request.envelopes(), spentByStart).isEmpty()
              ? CommitResult.committed(Envelopes.concat(consumed, spentByStart))
              : CommitResult.pending(Envelopes.concat(consumed, spentByStart));
        }
        if (started == DrainStartOutcome.DEFERRED) {
          // could not start, but the envelopes are untouched: hold them and the offsets
          return CommitResult.pending(Envelopes.concat(consumed, spentByStart));
        }
      } catch (TableCommitRejectedException e) {
        reportRejection(state, e);
        endDrain(state);
        return CommitResult.failed(Envelopes.concat(consumed, spentByStart));
      } catch (RuntimeException e) {
        if (!states.stopIfPermanent(state, e)) {
          LOG.warn(
              "Failed to start copy-on-write drain for table {}, will try again next cycle",
              request.tableReference().identifier(),
              e);
        }
        // failure reported to the caller right here, so the state does not have to carry it into
        // the next cycle as well
        endDrain(state);
        return CommitResult.failed(Envelopes.concat(consumed, spentByStart));
      }

      return CommitResult.pending(Envelopes.concat(consumed, spentByStart));
    } finally {
      state.lock.unlock();
    }
  }

  private static void refreshActiveTasks(TableDrainState state, TableCommitRequest request) {
    if (!request.activeTasks().isEmpty()) {
      // refreshed every cycle rather than pinned for the whole drain: after a rebalance
      // mid-drain, re-assignments would otherwise go to dead task ids
      state.activeTasks = request.activeTasks();
    }
  }

  private boolean removedUnderRequestWithoutResponses(
      TableCommitRequest request, TableDrainState state) {
    return request.envelopes().isEmpty() && states.get(request.tableReference()) != state;
  }

  /** Only responses make a state in {@link #commitTable}, so a table has one state. */
  private void checkResponsesOrState(TableCommitRequest request) {
    Preconditions.checkState(
        !request.envelopes().isEmpty() || states.contains(request.tableReference()),
        "Invalid commit of table %s: no responses and no copy-on-write state under the reference",
        request.tableReference().identifier());
  }

  /**
   * Tables the coordinator must offer a commit to even when no response for them is buffered.
   *
   * <p>A drain outlives the responses that started it: they are reported spent after the first
   * slice commits, and the table drops out of {@code tableCommitMap()}. A drain that then fails, or
   * that a restart cut short, would wait for the next record written to the table: indefinitely,
   * for a stream gone quiet. An active drain is listed too: {@code commit()} refreshes the active
   * tasks, and a drain spanning a rebalance needs that to stop handing slices to tasks that are
   * gone.
   *
   * <p>So are the tables of the statistics lag and those due a re-check of their health, under
   * references without a UUID. A table whose lock a lag step holds is not listed for it: the step
   * runs only on a table with nothing unapplied.
   */
  @Override
  public Set<TableReference> pendingTables() {
    Set<TableReference> pending = Sets.newLinkedHashSet();
    states.forEach(
        (tableReference, state) -> {
          if (!state.lock.tryLock()) {
            // a transition is running, so this table is plainly not idle; a lag step is not
            // one. Read after the failed tryLock: a step over since leaves the flag down, and the
            // table is listed, which errs on the safe side
            if (!state.lagStepHoldsLock) {
              pending.add(tableReference);
            }
            return;
          }
          try {
            // a stopped table is listed too: it stops with something unapplied (its envelopes, or
            // the tail of a change set whose envelopes are spent) and answering FAILED every
            // cycle is what keeps CommitComplete from claiming a valid-through it has not reached.
            // commit() answers without any IO and without repeating the ERROR
            if (!state.idle()) {
              pending.add(tableReference);
            }
          } finally {
            state.lock.unlock();
          }
        });
    // and a drain a previous coordinator left behind, before anything is known of it
    states
        .unresumed()
        .forEach(
            tableIdentifier ->
                pending.add(TableReference.of(catalog.name(), tableIdentifier, null)));
    lagStepRunner.addTables(pending);
    return pending;
  }

  /**
   * The replay anchors of the tables with a change set left to drain. Read without their locks: an
   * anchor a transition clears as this runs holds the offsets one cycle longer, and one is set only
   * in {@code commit()}, which runs before this in the same cycle.
   */
  @Override
  public Collection<Envelope> replayAnchors() {
    List<Envelope> anchors = Lists.newArrayList();
    states
        .all()
        .forEach(
            state -> {
              Envelope anchor = state.replayAnchor;
              if (anchor != null) {
                anchors.add(anchor);
              }
            });
    return anchors;
  }

  @Override
  public boolean receive(Envelope envelope) {
    if (envelope.event().payload().type() != PayloadType.REWRITE_COMPLETE) {
      return false;
    }

    RewriteComplete payload = (RewriteComplete) envelope.event().payload();
    TableDrainState state = states.get(payload.tableReference());
    if (state == null) {
      LOG.warn(
          "Ignoring rewrite response for table {} with no active drain; its files are orphans",
          payload.tableReference().identifier());
      return true;
    }

    // queued rather than applied here: this is the coordinator thread, and the lock may be held by
    // a transition that takes minutes. The next tick drains the queue
    state.answers.add(payload);
    drainAnswers(state);
    return true;
  }

  /** Applies whatever answers are queued, if the state is not busy. Never waits. */
  private void drainAnswers(TableDrainState state) {
    if (!state.lock.tryLock()) {
      return;
    }
    try {
      RewriteComplete answer;
      while ((answer = state.answers.poll()) != null) {
        applyAnswer(state, answer);
      }
    } finally {
      state.lock.unlock();
    }
  }

  private static boolean answersTheCurrentAttempt(TableDrainState state, RewriteComplete payload) {
    return state.awaiting && AttemptId.of(state).equals(AttemptId.of(payload));
  }

  private void applyAnswer(TableDrainState state, RewriteComplete payload) {
    if (!answersTheCurrentAttempt(state, payload)) {
      LOG.warn(
          "Ignoring stale rewrite response from task {} for table {} (commit {} change set {} "
              + "slice {}); its files are orphans",
          payload.taskId(),
          payload.tableReference().identifier(),
          payload.commitId(),
          payload.changeSetId(),
          payload.sliceSeq());
      return;
    }

    if (!state.attempt.isAssigned(payload.taskId())) {
      LOG.warn(
          "Ignoring rewrite response from task {}, which was not assigned slice {} of change "
              + "set {} for table {}",
          payload.taskId(),
          payload.sliceSeq(),
          payload.changeSetId(),
          payload.tableReference().identifier());
      return;
    }

    if (payload.status() != RewriteComplete.STATUS_OK) {
      // an explicit failure is worth acting on now rather than waiting out the timeout
      failSlice(state, payload);
      return;
    }

    SliceAttempt.ChunkResult chunk = state.attempt.acceptChunk(payload);
    if (chunk.outcome() == SliceAttempt.ChunkOutcome.CHUNK_ERROR) {
      cancelSlice(state, Cancellation.CHUNK_ERROR, chunk.reason());
      return;
    }
    if (chunk.outcome() == SliceAttempt.ChunkOutcome.DUPLICATE) {
      LOG.warn(
          "Ignoring duplicate chunk {} of {} from task {} for slice {} of change set {}, table {}",
          payload.chunkIndex(),
          payload.chunkCount(),
          payload.taskId(),
          payload.sliceSeq(),
          payload.changeSetId(),
          payload.tableReference().identifier());
      return;
    }

    if (chunk.outcome() == SliceAttempt.ChunkOutcome.COMPLETE) {
      state.awaiting = false;
      submit(state, () -> finishSlice(state));
    }
  }

  private void failSlice(TableDrainState state, RewriteComplete payload) {
    if (payload.failureKind() != RewriteComplete.FAILURE_PERMANENT) {
      cancelSlice(
          state,
          Cancellation.FAILED_REWRITE,
          "task " + payload.taskId() + " reported a failed rewrite");
      return;
    }

    PermanentCopyOnWriteException failure =
        new PermanentCopyOnWriteException(CopyOnWriteMessages.permanentRewriteFailure(payload));
    state.awaiting = false;
    submit(
        state,
        () -> {
          states.stopIfPermanent(state, failure);
          discardSliceFiles(state, false);
          failDrain(state);
        });
  }

  @Override
  public void process(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {
    for (TableDrainState state : states.all()) {
      // a table whose transition is in flight is simply skipped this tick.
      // Waiting here would stall the coordinator's poll loop behind an Iceberg commit
      if (!state.lock.tryLock()) {
        continue;
      }
      try {
        RewriteComplete answer;
        while ((answer = state.answers.poll()) != null) {
          applyAnswer(state, answer);
        }

        if (state.awaiting
            && nowMs - state.attempt.assignedAtMs() > config.copyOnWriteRewriteTimeoutMs()) {
          cancelSlice(
              state, Cancellation.TIMED_OUT, CopyOnWriteMessages.timeoutReason(state, config));
        }
      } finally {
        state.lock.unlock();
      }
    }
  }

  @Override
  public StagingCleanupSource stagingCleanupSource() {
    return stagingCleanupSource;
  }

  /** What {@link #startDrain} managed to do with this cycle's envelopes. */
  private enum DrainStartOutcome {
    /** A slice is out with the workers. */
    STARTED,
    /** Everything offered was already applied, or the table is gone: the request is spent. */
    NOTHING_TO_DO,
    /** Nothing was applied and nothing was frozen: the envelopes must stay buffered. */
    DEFERRED
  }

  /**
   * Resumes an interrupted drain or freezes a new change set, then starts its first slice. Adds to
   * {@code spent} what it spent outright, rather than froze.
   */
  private DrainStartOutcome startDrain(
      TableDrainState state, TableCommitRequest request, List<Envelope> spent) {
    TableReference tableReference = request.tableReference();
    TableIdentifier tableIdentifier = tableReference.identifier();
    state.pendingFinal = null;

    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      // the envelopes are reported spent by the caller, so say plainly what goes with them. In
      // merge-on-read nothing had been written yet; here every one of them names staged files that
      // are already in storage, and the staging cleanup cannot reach them: it needs a table to
      // resolve the staging location and its FileIO
      LOG.warn(
          "Table {} does not exist; dropping {} buffered response(s) naming {} staged change "
              + "file(s). Nothing will delete them: the staging cleanup needs the table to find "
              + "them. Remove them with the storage's own tooling if the table is not coming back",
          tableIdentifier,
          request.envelopes().size(),
          Envelopes.countStagedFiles(request.envelopes()),
          e);
      spent.addAll(request.envelopes());
      // and nothing of the table stays in memory: a state left behind (a drain it had leaves
      // unfinishedDrain set) would have pendingTables() list it, and this warning repeat, every
      // cycle for a table that is not there. One created again under the name starts afresh
      states.remove(state);
      return DrainStartOutcome.NOTHING_TO_DO;
    }

    if (writtenForDroppedTable(state, request, table)) {
      spent.addAll(request.envelopes());
      return DrainStartOutcome.NOTHING_TO_DO;
    }

    // the coordinator never reads or writes table data, so a cycle where no task reported in
    // defers rather than freezing a change set nobody can apply: checked before the freeze, so
    // the envelopes stay as they were
    if (state.activeTasks.isEmpty()) {
      LOG.warn(
          "No tasks available to rewrite table {}, deferring to the next cycle", tableIdentifier);
      return DrainStartOutcome.DEFERRED;
    }

    state.bindTable(table, config);
    Set<Integer> identifierFieldIds = planner.identifierFieldIds(table, tableReference);

    ChangeSetStore.DrainStart start =
        changeSets.loadDrainState(
            table, state.branch, tableReference, state.stagingLocation, identifierFieldIds);
    // the pointer is read: whatever a previous coordinator left is this state's from here on
    states.resumed(tableIdentifier);
    // a pointer naming a change set that was just dropped is a drain nobody has finished: until the
    // change set adopting its files first commits, a failure anywhere below, the freeze included,
    // must leave the table offered a commit every cycle, buffered responses or not
    if (!start.carryOverFiles().isEmpty()) {
      state.unfinishedDrain = true;
    } else if (start.manifest() == null) {
      // nothing left to drain: whatever the last change set anchored at is applied, and none is
      // unfinished, or its pointer would be there. The final snapshot of the last one may still
      // wait for its statistics: looked up before a tail takes the pointer off it
      state.replayAnchor = null;
      state.unfinishedDrain = false;
      state.pendingFinal = finalStatistics.lookUpPendingFinal(state, table, request.commitId());
    }

    TableCommitRequest offered = request;
    if (request.envelopes().stream().anyMatch(Envelopes::isDataWritten)) {
      offered = mergeOnReadTail.offer(request, drainInProgress(state, start), table, spent);
      if (offered == null) {
        return DrainStartOutcome.DEFERRED;
      }
    }

    if (start.manifest() == null) {
      ChangeSetStore.FreezeResult frozen =
          changeSets.freeze(
              table,
              state.branch,
              tableReference,
              state.stagingLocation,
              identifierFieldIds,
              offered,
              start);
      if (frozen == null) {
        return nothingToFreeze(state, request, offered, spent);
      }

      state.beginFrozen(
          frozen, changeSets.adoptDroppedChangeSet(tableReference, start), offered.envelopes());
      state.unfinishedDrain = !start.carryOverFiles().isEmpty();
      // a change set adopting the files of a dropped one, frozen from an empty buffer, keeps the
      // anchor the dropped one had
      anchorAt(state, offered.envelopes());
    } else {
      // resumed from the snapshot summary. This cycle's envelopes are not part of it and stay
      // buffered until it drains
      state.beginResumed(start);
      // the envelope a restart replayed to get here is the one to replay after the next restart,
      // though the freeze after this drain will find it applied
      anchorAt(state, request.envelopes());
      LOG.info(
          "Resuming copy-on-write change set {} for table {} from cursor {}",
          start.manifest().changeSetId(),
          tableIdentifier,
          start.cursor());
    }

    state.startChangeSet(request.commitId(), config.copyOnWriteCommitRetries());

    // normalizing and planning is the expensive half of starting a drain, and doCommit() must not
    // wait for it. Everything above is metadata IO, which merge-on-read does inline as well: the
    // freeze checks identifier fields from the staged files' descriptors without opening them
    submit(state, () -> startSlice(state));
    return DrainStartOutcome.STARTED;
  }

  private static boolean drainInProgress(TableDrainState state, ChangeSetStore.DrainStart start) {
    return start.manifest() != null
        || !start.carryOverFiles().isEmpty()
        || state.pendingFinal != null;
  }

  /**
   * A start that froze nothing: what it offered is spent, and a pending final snapshot it found
   * gets a recount transition of its own, since no first slice recounts it.
   */
  private DrainStartOutcome nothingToFreeze(
      TableDrainState state,
      TableCommitRequest request,
      TableCommitRequest offered,
      List<Envelope> spent) {
    spent.addAll(offered.envelopes());
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    TableDrainState.PendingFinal pending = state.pendingFinal;
    boolean tailWaits = pending != null && !Envelopes.without(request.envelopes(), spent).isEmpty();
    finalStatistics.logNothingFrozen(state, tableIdentifier, tailWaits);
    if (pending != null) {
      setRecoverPendingFinal(state);
    }

    return DrainStartOutcome.NOTHING_TO_DO;
  }

  private void setRecoverPendingFinal(TableDrainState state) {
    try {
      exec.execute(() -> recoverPendingFinal(state));
    } catch (RuntimeException e) {
      LOG.debug(
          "Cannot set the NDV recompute of the final snapshot of table {}",
          state.tableReference.identifier(),
          e);
    }
  }

  /**
   * The recount transition of a pending final snapshot, for a drain start that froze nothing. Its
   * guard is its own, not {@link #submit}'s, and it reads the pending snapshot from the state as it
   * runs: the first transition set for it recounts it, those set after leave by the guard. Takes no
   * slot or permit.
   */
  private void recoverPendingFinal(TableDrainState state) {
    state.lock.lock();
    try {
      TableDrainState.PendingFinal pending = state.pendingFinal;
      if (stopped || !states.registeredAndIdle(state) || pending == null) {
        LOG.debug(
            "NDV recompute of the final snapshot of table {} is not done here: it is done "
                + "already, or the table is gone, stopped or draining",
            state.tableReference.identifier());
        return;
      }

      finalStatistics.recountOrRefuse(state, pending, pending.commitId);
    } finally {
      state.lock.unlock();
    }
  }

  /**
   * Whether the responses were written for a table since dropped and created again under its name:
   * the UUID they carry is not the one of the table the name loads. Their reference keys a state of
   * its own beside the new table's, so drained, they would be a second drain into the new table,
   * laying changes older than its own over its newer values, from staged files written against
   * another schema. They are dropped instead, as the change set frozen for the old table is, their
   * staged files deleted by name: nothing else would, the staging cleanup of the new table keeps
   * what its buffer names. The state goes, and its replay anchor with it. A reference without a
   * UUID is not checked.
   */
  private boolean writtenForDroppedTable(
      TableDrainState state, TableCommitRequest request, Table table) {
    TableReference tableReference = request.tableReference();
    UUID writtenFor = tableReference.uuid();
    if (writtenFor == null || writtenFor.equals(table.uuid())) {
      return false;
    }

    List<String> staged = Envelopes.stagedFileLocations(request.envelopes());
    LOG.warn(
        "Table {} was dropped and created again: dropping {} buffered response(s) written for the "
            + "dropped one (UUID {}; the table is now {}) and deleting the {} staged change file(s) "
            + "they name. Their changes are not applied. Recreate a table while the connector is "
            + "stopped, or once its responses are drained",
        tableReference.identifier(),
        request.envelopes().size(),
        writtenFor,
        table.uuid(),
        staged.size());
    CopyOnWriteFiles.deleteQuietly(table.io(), staged);
    states.remove(state);
    states.forgetDrainUnder(writtenFor, tableReference);
    return true;
  }

  private void startSlice(TableDrainState state) {
    Table table = state.table;
    table.refresh();

    // a refresh goes by name, and not every catalog checks that the name still means the same
    // table: one dropped and created again under it comes back as the new table. What is left of a
    // change set frozen for the old one must not land there. The drain comes off uncommitted; the
    // next cycle's loadDrainState reads the new table, which has no pointer to this change set
    UUID frozenForTable = state.manifest.tableUuid();
    if (frozenForTable != null && !frozenForTable.equals(table.uuid())) {
      LOG.warn(
          "Table {} was replaced while change set {} was draining (table {} -> {}); slice {} is not "
              + "started and the change set is abandoned, the next cycle starts from the new table",
          state.tableReference.identifier(),
          state.manifest.changeSetId(),
          frozenForTable,
          table.uuid(),
          state.sliceSeq);
      // abandoned rather than interrupted: there is nothing to resume, so the table need not be
      // offered a commit again until something new is written to it, nor found after a restart
      state.unfinishedDrain = false;
      state.replayAnchor = null;
      failDrain(state);
      return;
    }

    // on every slice, not only when the drain starts: identifier fields changed between two slices
    // would have this one normalize and plan by the manifest's key, and replace rows that are
    // distinct under the table's. The drain comes off uncommitted; the next cycle's loadDrainState
    // checks the change set against the fields again and drops it, or resumes it if they are back
    Set<Integer> tableIdentifierFieldIds = planner.identifierFieldIds(table, state.tableReference);
    if (!state.manifest.identifierFieldsMatch(tableIdentifierFieldIds)) {
      LOG.warn(
          "Table {} identifier fields changed while change set {} was draining ({} -> {}); slice {} "
              + "is not started, the next cycle checks the change set against them again",
          state.tableReference.identifier(),
          state.manifest.changeSetId(),
          state.manifest.identifierFieldIds(),
          tableIdentifierFieldIds,
          state.sliceSeq);
      failDrain(state);
      return;
    }

    // the first slice of a drain that found a pending F recounts it before it plans
    TableDrainState.PendingFinal pending = state.pendingFinal;
    if (pending != null) {
      finalStatistics.recountOrRefuse(state, pending, state.commitId);
      if (stopped) {
        // the recount is a long read: nothing is planned or written for a coordinator that is gone
        return;
      }
    }

    Snapshot base = SliceCommitter.baseSnapshot(table, state.branch);
    state.baseSnapshotId = base == null ? null : base.snapshotId();

    ChangeSetSlice slice;
    if (state.baseSnapshotId == null) {
      // an empty table, or a new branch of one: nothing to plan or overwrite, so the slice is
      // append-only and no byte quota applies
      slice = planner.normalize(table, state.manifest, state.cursor, state.tableReference);
      state.planFiles = ImmutableList.of();
      state.conflictFilter = null;
      state.planSummary = null;
    } else {
      slice = planWithinByteQuota(state);
    }

    state.slice = slice;
    state.drained = !slice.truncated();

    if (slice.isEmpty()) {
      // nothing for the workers to do, but a draining table still owes a commit: it has a cursor
      // and offsets to advance even when this slice turned out empty
      state.attempt = SliceAttempt.empty();
      state.awaiting = false;
      finishSlice(state);
      return;
    }

    state.normalizedLocation =
        normalizedLocation(
            state.stagingLocation,
            state.tableReference,
            config.connectGroupId(),
            state.manifest.changeSetId(),
            state.sliceSeq);
    StagedChangeFile normalizedRef =
        ChangeSetNormalizer.writeNormalized(
            table, state.manifest.identifierFieldIds(), slice, state.normalizedLocation);
    if (state.planSummary != null) {
      // logged here, before anything is handed out, because only now is the size of the change
      // known to set the rewrite against. The append path planned nothing and has no summary
      state.planSummary =
          state.planSummary.withChanges(
              normalizedRef.fileSizeBytes(),
              state.changeSetBytesReplaced,
              state.changeSetSlicesCommitted);
      LOG.info(
          "Copy-on-write plan for slice {} of change set {} for table {}: {}",
          state.sliceSeq,
          state.manifest.changeSetId(),
          state.tableReference.identifier(),
          state.planSummary);
    }

    StructType wirePartitionType = table.spec().partitionType();
    List<Assignment> assignments =
        RewriteAssigner.assign(state.planFiles, state.activeTasks, wirePartitionType);
    // chunk counts are per slice: a cancelled slice's partial answer must not count towards the
    // replacement one, or a task that lost a chunk would look complete the second time round
    state.attempt =
        new SliceAttempt(
            assignments.stream()
                .map(Assignment::taskId)
                .collect(Collectors.toCollection(Sets::newHashSet)),
            System.currentTimeMillis());
    state.awaiting = true;

    // one message per task (several if its assignment does not fit one), since a task's file
    // descriptors are the bulk of the payload; all in one producer transaction, since a round that
    // lands partially leaves the tasks that got nothing silent and the slice waits out its timeout
    List<Integer> identifierFieldIds =
        state.manifest.identifierFieldIds().stream().sorted().collect(Collectors.toList());
    send(
        new AssignmentRound(
                config,
                wirePartitionType,
                state.commitId,
                state.tableReference,
                state.manifest.changeSetId(),
                state.sliceSeq,
                state.baseSnapshotId == null ? NO_BASE_SNAPSHOT : state.baseSnapshotId,
                normalizedRef,
                identifierFieldIds)
            .messages(assignments));

    LOG.info(
        "Assigned slice {} of change set {} for table {}: {} key(s) (~{} staged byte(s) held), "
            + "{} file(s) across {} task(s)",
        state.sliceSeq,
        state.manifest.changeSetId(),
        state.tableReference.identifier(),
        slice.size(),
        slice.estimatedRetainedBytes(),
        state.planFiles.size(),
        assignments.size());
  }

  private ChangeSetSlice planWithinByteQuota(TableDrainState state) {
    SlicePlanner.SlicePlan plan =
        planner.planWithinByteQuota(
            state.table, state.baseSnapshotId, state.manifest, state.cursor, state.tableReference);
    if (plan.firstKeyOverQuota()) {
      warnOverByteQuota(state, plan.summary(), plan.keys(), plan.normalizedKeys());
    }

    if (plan.keys() < plan.normalizedKeys()) {
      LOG.info(
          "Copy-on-write slice {} for table {} plans {} bytes against the {} byte quota; cut from "
              + "{} to {} key(s) planning {} bytes, after {} plan(s)",
          state.sliceSeq,
          state.tableReference.identifier(),
          plan.normalizedBytes(),
          plan.quotaBytes(),
          plan.normalizedKeys(),
          plan.keys(),
          plan.summary().totalRewriteBytes(),
          plan.plansMade());
    }

    state.planFiles = plan.planFiles();
    state.conflictFilter = plan.conflictFilter();
    state.planSummary = plan.summary();
    if (plan.summary().fractionFilesWithoutBounds() > 0) {
      warnFilesWithoutBounds(state, plan.summary());
    }
    return plan.slice();
  }

  private void warnOverByteQuota(
      TableDrainState state, PlanSummary summary, int keys, int sliceKeys) {
    UUID changeSetId = state.manifest.changeSetId();
    if (changeSetId.equals(state.overByteQuotaWarnedFor)) {
      return;
    }

    state.overByteQuotaWarnedFor = changeSetId;
    state.overByteQuotaWarning =
        CopyOnWriteMessages.overByteQuota(state, summary, keys, sliceKeys, config);
    LOG.warn("{}", state.overByteQuotaWarning);
  }

  private void warnFilesWithoutBounds(TableDrainState state, PlanSummary summary) {
    UUID changeSetId = state.manifest.changeSetId();
    if (changeSetId.equals(state.filesWithoutBoundsWarnedFor)) {
      return;
    }

    state.filesWithoutBoundsWarnedFor = changeSetId;
    LOG.warn("{}", CopyOnWriteMessages.filesWithoutBounds(state, summary));
  }

  private void finishSlice(TableDrainState state) {
    Table table = state.table;

    // Phase one: the commit itself. Its failure modes are the only ones that may delete the
    // replacement files, because they are the only ones where the files are still ours
    if (!commitPhase(state)) {
      return;
    }
    // the instant the commit has landed, these files belong to the table: a snapshot references
    // them. Taking them off the state here (not at the top of the next slice) is what keeps
    // any later failure from handing them to discardSliceFiles
    List<DataFile> committedFiles = state.attempt.takeCollected();
    // only now: the lookup is a catalog request that can fail, and it must not fail where the
    // files could still be reached. It reports a failure as no snapshot rather than throwing
    Long snapshotId = sliceCommitter.reportedSnapshotId(state);

    // Phase two: everything after a snapshot exists. Nothing here may delete a data file: the
    // slice's own are in the table, and the next slice has not written any yet
    try {
      // the change set is persisted now: its pointer is in this snapshot's summary, or the whole
      // thing is applied, so whatever it froze may leave the coordinator's buffer
      state.spent = true;
      // ... the pointer has moved off the change set whose files this one adopted, if any ...
      String droppedManifestLocation = state.droppedManifestLocation;
      unlessStopped(() -> changeSets.deleteDroppedChangeSet(table.io(), droppedManifestLocation));
      state.droppedManifestLocation = null;
      // ... and from here on the coordinator must keep offering this table a commit even with an
      // empty buffer, or a drain that fails later is never picked up again
      state.unfinishedDrain = !state.drained;
      if (state.drained) {
        // nothing is left to replay: released here, not after CommitToTable, which can fail and
        // leave a quiet table's offsets held for good
        state.replayAnchor = null;
      }

      state.resetSliceRetryQuotas(config.copyOnWriteCommitRetries());
      state.cancelledInARow = 0;

      state.countCommittedSlice();

      // deletions that cannot throw, ahead of anything that can (sending CommitToTable, reporting
      // lineage): the catch below cleans up nothing but the next slice's normalized file, so a
      // failure past this point would leave the change set's own files unreachable and unremoved
      if (state.normalizedLocation != null) {
        String normalizedLocation = state.normalizedLocation;
        unlessStopped(() -> CopyOnWriteFiles.deleteQuietly(table.io(), normalizedLocation));
        state.normalizedLocation = null;
      }
      if (state.drained) {
        finalStatistics.finishChangeSet(state, snapshotId);
      }

      send(
          ImmutableList.of(
              new Event(
                  config.connectGroupId(),
                  new CommitToTable(
                      state.commitId,
                      state.tableReference,
                      snapshotId,
                      state.drained ? state.validThroughTs : null))));
      // once when the change set first lands, and once when it is done: a drain of fifty slices
      // is one logical batch. The first is reported eagerly so that a table this drain created
      // shows up in the catalogue right away
      if (!state.lineageReported || state.drained) {
        metadataEvents.lineageCommit(
            state.manifest.sourceTopics(), state.tableReference.identifier(), table.schema());
        state.lineageReported = true;
      }

      LOG.info(
          "Copy-on-write commit complete to table {}, snapshot {}, commit ID {}, drained {}: {}",
          state.tableReference.identifier(),
          snapshotId,
          state.commitId,
          state.drained,
          CopyOnWriteMessages.rewriteOutcome(state, committedFiles));

      if (state.drained) {
        state.unfinishedDrain = false;
        state.reset();
      } else {
        state.cursor = state.slice.lastKey().orElse(state.cursor);
        state.sliceSeq += 1;
        // on every intermediate commit, unlike warnOverByteQuota: an operator filtering on WARN
        // must see write amplification while it happens
        state.sliceProgressWarningsLogged += 1;
        LOG.warn("{}", CopyOnWriteMessages.sliceProgress(state));
        startSliceGuarded(state);
      }
    } catch (RuntimeException e) {
      // the snapshot stands: a failure here (the control topic, the lineage sink, planning the
      // next slice) fails the drain until next cycle, never touches a committed file.
      // discardSliceFiles can only reach the next slice's normalized file: the committed files
      // were taken off the state above, and the next slice has collected no answers yet
      if (!states.stopIfPermanent(state, e)) {
        LOG.warn(
            "Copy-on-write bookkeeping after snapshot {} of table {} failed, the snapshot stands; "
                + "the drain resumes next cycle",
            snapshotId,
            state.tableReference.identifier(),
            e);
      }
      discardSliceFiles(state, false);
      failDrain(state);
    }
  }

  private boolean commitPhase(TableDrainState state) {
    if (stopped) {
      // the files stay where they are: the coordinator that took over may resume this very slice
      LOG.debug(
          "Coordinator is stopping, slice {} of table {} is not committed",
          state.sliceSeq,
          state.tableReference.identifier());
      return false;
    }
    try {
      sliceCommitter.commitSlice(state);
      return true;
    } catch (ValidationException e) {
      // a concurrent append touching our keys (validateAddedDataFiles), or a concurrent compaction
      // that removed a file we planned to replace (failMissingDeletePaths). Both invalidate the
      // replacement files, so the whole slice is redone: replan against the new snapshot and hand
      // it out again
      discardSliceFiles(state, false);
      if (state.retriesLeft <= 0) {
        LOG.warn(
            "Copy-on-write commit for table {} failed after exhausting {} retries, will try again "
                + "next cycle",
            state.tableReference.identifier(),
            config.copyOnWriteCommitRetries(),
            e);
        // the slice waits, the drain stays: a table under regular compaction must not lose a
        // cycle to starting over. A new sequence number, so that nothing of this attempt counts
        // towards the next one
        state.sliceSeq += 1;
        state.retryNextCycle = true;
        return false;
      }

      state.retriesLeft -= 1;
      LOG.warn(
          "Copy-on-write commit for table {} conflicted with a concurrent writer, retrying "
              + "({} attempt(s) left)",
          state.tableReference.identifier(),
          state.retriesLeft,
          e);
      state.sliceSeq += 1;
      startSliceGuarded(state);
      return false;
    } catch (SliceCommitter.TableReplacedException e) {
      // the same as the check at the start of the slice, found later: dropped and created again
      // while the slice was rewritten. Nothing landed, the files are this attempt's
      LOG.warn(
          "Table {} was replaced while slice {} of change set {} was being committed; nothing "
              + "is committed, the change set is abandoned and the next cycle starts from the "
              + "new table",
          state.tableReference.identifier(),
          state.sliceSeq,
          state.manifest.changeSetId(),
          e);
      discardSliceFiles(state, false);
      state.unfinishedDrain = false;
      state.replayAnchor = null;
      failDrain(state);
      return false;
    } catch (SliceCommitter.IdentifierFieldsChangedException e) {
      // the same as the check at the start of the slice, found later: changed while the slice was
      // rewritten. Nothing landed, the files are this attempt's; the next cycle's loadDrainState
      // checks the change set against the fields again and drops it, or resumes it if they are back
      LOG.warn(
          "Identifier fields of table {} changed while slice {} of change set {} was being "
              + "committed; nothing is committed, the next cycle checks the change set against "
              + "them again",
          state.tableReference.identifier(),
          state.sliceSeq,
          state.manifest.changeSetId(),
          e);
      discardSliceFiles(state, false);
      failDrain(state);
      return false;
    } catch (RuntimeException e) {
      if (SliceCommitter.outcomeUnknown(e)) {
        return settleUnknownOutcome(state, e);
      }
      // the commit did not happen: nothing references the files, and they are this attempt's
      if (!states.stopIfPermanent(state, e)) {
        LOG.warn(
            "Copy-on-write commit for table {} failed, will try again next cycle",
            state.tableReference.identifier(),
            e);
      }
      discardSliceFiles(state, false);
      failDrain(state);
      return false;
    }
  }

  /**
   * A commit that threw without saying whether it happened. The snapshot may exist and reference
   * the replacement files, so nothing is deleted; looking for the snapshot settles it.
   *
   * <p>Found, the slice is committed and phase two runs. Not found, or the lookup failed, the drain
   * fails with the files left where they are: orphans for {@code remove_orphan_files} if the commit
   * never landed, and a snapshot the next cycle resumes from if it did.
   *
   * @return true if the slice's snapshot was found
   */
  private boolean settleUnknownOutcome(TableDrainState state, RuntimeException failure) {
    // whatever the lookup finds, a snapshot may name the manifest now
    state.commitOutcomeUnknown();
    Long snapshotId;
    try {
      snapshotId = sliceCommitter.committedSnapshotId(state);
    } catch (RuntimeException e) {
      failure.addSuppressed(e);
      snapshotId = null;
    }

    if (snapshotId != null) {
      LOG.warn(
          "Copy-on-write commit for table {} did not report its outcome, but its snapshot {} is "
              + "in the branch; carrying on as committed",
          state.tableReference.identifier(),
          snapshotId,
          failure);
      return true;
    }

    LOG.warn(
        "Copy-on-write commit for table {} did not report its outcome and its snapshot could not "
            + "be found; its files are left for remove_orphan_files, and the drain resumes next "
            + "cycle",
        state.tableReference.identifier(),
        failure);
    if (state.drained) {
      finalStatistics.finalSliceNotFound(state);
    }
    // resetting the state lets go of the files without deleting them
    failDrain(state);
    return false;
  }

  private void startSliceGuarded(TableDrainState state) {
    if (stopped) {
      return;
    }
    try {
      startSlice(state);
    } catch (RuntimeException e) {
      if (!states.stopIfPermanent(state, e)) {
        LOG.warn(
            "Could not start slice {} of change set {} for table {}, the drain resumes next cycle",
            state.sliceSeq,
            state.manifest.changeSetId(),
            state.tableReference.identifier(),
            e);
      }
      discardSliceFiles(state, false);
      failDrain(state);
    }
  }

  /** Why a slice is cancelled, which decides when it is handed out again. */
  private enum Cancellation {
    /** A task's rewrite failed in a way that may pass: at once, within {@code commit-retries}. */
    FAILED_REWRITE,
    /** No complete answer in time: on the next cycle, to that cycle's tasks. */
    TIMED_OUT,
    /** A chunk that cannot belong to a complete answer: on the next cycle, as after a timeout. */
    CHUNK_ERROR
  }

  private void cancelSlice(TableDrainState state, Cancellation cause, String reason) {
    state.cancelledInARow += 1;
    boolean retryAtOnce = cause == Cancellation.FAILED_REWRITE && state.retriesLeft > 0;
    state.cancellationWarning = CopyOnWriteMessages.cancellation(state, reason, retryAtOnce);
    LOG.warn("{}", state.cancellationWarning);

    // handed to the commit pool rather than deleted here: cancelSlice is reached from the
    // coordinator thread, and a cancelled slice can carry hundreds of files to delete
    discardSliceFiles(state, true);
    state.awaiting = false;
    state.sliceSeq += 1;

    if (retryAtOnce) {
      state.retriesLeft -= 1;
      submit(state, () -> startSlice(state));
    } else {
      state.retryNextCycle = true;
    }
  }

  private void retrySliceThatWaited(TableDrainState state) {
    if (!state.retryNextCycle) {
      return;
    }

    state.retryNextCycle = false;
    state.resetSliceRetryQuotas(config.copyOnWriteCommitRetries());
    submit(state, () -> startSlice(state));
  }

  private void failDrain(TableDrainState state) {
    endDrain(state);
  }

  private void endDrain(TableDrainState state) {
    if (!stopped && state.manifest != null && state.freshlyFrozen && !state.spent) {
      CopyOnWriteFiles.deleteQuietly(state.table.io(), state.manifestLocation);
    }
    state.reset();
  }

  private void reportRejection(TableDrainState state, TableCommitRejectedException rejection) {
    if (rejection.reason().equals(state.rejectedReason)) {
      LOG.debug(
          "Copy-on-write commit of table {} is still rejected: {}",
          state.tableReference.identifier(),
          rejection.getMessage());
      return;
    }

    state.rejectedReason = rejection.reason();
    LOG.error(
        "Copy-on-write commit of table {} is rejected: {}. This table's responses stay in the "
            + "coordinator's buffer and its control topic offsets stay held; every commit cycle "
            + "checks again, and no restart is needed once the cause is undone",
        state.tableReference.identifier(),
        rejection.getMessage());
  }

  private void clearRejection(TableDrainState state) {
    if (state.rejectedReason == null) {
      return;
    }

    LOG.info(
        "Copy-on-write commit of table {} is no longer rejected ({})",
        state.tableReference.identifier(),
        state.rejectedReason);
    state.rejectedReason = null;
  }

  private void discardSliceFiles(TableDrainState state, boolean offThread) {
    if (stopped) {
      // not even an attempt's own files: the coordinator that took over may use the same names
      return;
    }
    List<String> locations = Lists.newArrayList();
    state.attempt.takeCollected().forEach(file -> locations.add(file.location()));
    if (state.normalizedLocation != null) {
      locations.add(state.normalizedLocation);
      state.normalizedLocation = null;
    }
    if (locations.isEmpty()) {
      return;
    }

    FileIO io = state.table.io();
    if (offThread) {
      exec.execute(() -> CopyOnWriteFiles.deleteQuietly(io, locations));
    } else {
      CopyOnWriteFiles.deleteQuietly(io, locations);
    }
  }

  private void submit(TableDrainState state, Runnable transition) {
    AttemptId attempt = AttemptId.of(state);
    exec.execute(
        () -> {
          state.lock.lock();
          try {
            if (stopped || state.manifest == null || !attempt.equals(AttemptId.of(state))) {
              return;
            }
            try {
              transition.run();
            } catch (RuntimeException e) {
              if (!states.stopIfPermanent(state, e)) {
                LOG.warn(
                    "Copy-on-write slice transition failed for table {}, will try again next cycle",
                    state.tableReference.identifier(),
                    e);
              }
              // startSlice is the transition that throws here: finishSlice and startSliceGuarded
              // handle their own. An unstarted slice leaves at most its normalized file, but this
              // outermost catch deletes whatever the state still claims
              discardSliceFiles(state, false);
              failDrain(state);
            }
          } finally {
            state.lock.unlock();
          }

          // answers that arrived while this transition held the lock were queued; picking them up
          // here rather than waiting for the next tick keeps a fast slice fast
          drainAnswers(state);
        });
  }

  private void send(List<Event> events) {
    if (stopped) {
      LOG.debug("Coordinator is stopping, {} event(s) not sent", events.size());
      return;
    }
    eventSender.accept(events);
  }

  private void unlessStopped(Runnable delete) {
    if (!stopped) {
      delete.run();
    }
  }

  private List<Envelope> takeConsumed(TableDrainState state) {
    if (!state.spent || state.frozenEnvelopes.isEmpty()) {
      return ImmutableList.of();
    }
    List<Envelope> consumed = state.frozenEnvelopes;
    state.frozenEnvelopes = ImmutableList.of();
    return consumed;
  }

  @VisibleForTesting
  static String normalizedLocation(
      String stagingLocation,
      TableReference tableReference,
      String groupId,
      UUID changeSetId,
      int sliceSeq) {
    return String.format(
        Locale.ROOT,
        CHANGESETS_NORMALIZED_AVRO_FILE_NAME_PATH_FORMAT,
        StagedChangeFileWriter.stagingDirectory(stagingLocation, tableReference, groupId),
        changeSetId,
        sliceSeq);
  }

  @VisibleForTesting
  PlanSummary planSummary(TableReference tableReference) {
    return underLock(tableReference, state -> state.planSummary);
  }

  @VisibleForTesting
  int cancelledInARow(TableReference tableReference) {
    return underLock(tableReference, state -> state.cancelledInARow);
  }

  @VisibleForTesting
  String overByteQuotaWarning(TableReference tableReference) {
    return underLock(tableReference, state -> state.overByteQuotaWarning);
  }

  @VisibleForTesting
  String cancellationWarning(TableReference tableReference) {
    return underLock(tableReference, state -> state.cancellationWarning);
  }

  @VisibleForTesting
  String sliceProgressWarning(TableReference tableReference) {
    return underLock(tableReference, CopyOnWriteMessages::sliceProgress);
  }

  @VisibleForTesting
  int sliceProgressWarningsLogged(TableReference tableReference) {
    return underLock(tableReference, state -> state.sliceProgressWarningsLogged);
  }

  @VisibleForTesting
  UUID filesWithoutBoundsWarnedFor(TableReference tableReference) {
    return underLock(tableReference, state -> state.filesWithoutBoundsWarnedFor);
  }
}
