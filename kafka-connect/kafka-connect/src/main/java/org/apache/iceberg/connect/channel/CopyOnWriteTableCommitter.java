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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
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
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
 * {@link SliceCommitter}, the staging sweep {@link StagingCleanupScheduler}. {@link
 * #discardSliceFiles} stays private to the state machine: it is where a replacement file changes
 * owner, and no collaborator may reach a data file.
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
  private final StagingCleanupScheduler stagingCleanup;
  private final MergeOnReadTailHandoff mergeOnReadTail;
  private final Map<TableReference, TableDrainState> states = Maps.newConcurrentMap();
  private final Map<UUID, TableReference> drainingUnder = Maps.newConcurrentMap();
  private final Set<TableIdentifier> configuredTables;
  private final Set<TableIdentifier> unresumedTables;
  private volatile boolean stopped;

  CopyOnWriteTableCommitter(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<List<Event>> eventSender,
      Executor exec,
      BooleanSupplier controlTopicCaughtUp) {
    this.catalog = catalog;
    this.config = config;
    this.metadataEvents = metadataEvents;
    this.eventSender = eventSender;
    this.exec = exec;
    this.configuredTables = configuredTables(config);
    this.unresumedTables = Sets.newConcurrentHashSet(configuredTables);
    this.changeSets = new ChangeSetStore(config);
    this.sliceCommitter = new SliceCommitter(config);
    this.planner = new SlicePlanner(config);
    this.stagingCleanup =
        new StagingCleanupScheduler(
            config,
            changeSets,
            exec,
            states.values(),
            controlTopicCaughtUp,
            () -> stopped,
            catalog::loadTable);
    this.mergeOnReadTail =
        new MergeOnReadTailHandoff(
            new MergeOnReadTableCommitter(
                catalog,
                config,
                metadataEvents,
                event -> eventSender.accept(ImmutableList.of(event))));
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
    LOG.info(
        "Coordinator is stopping: copy-on-write transitions in flight start no commit, delete or "
            + "send from here on");
  }

  @Override
  public CommitResult commit(TableCommitRequest request) {
    TableReference tableReference = request.tableReference();
    // what pendingTables() lists for a table not looked at yet has no state of its own, and never
    // gets one: the reference lacks the table's UUID, and a drain under it would run beside the one
    // the workers' responses start under theirs
    if (request.envelopes().isEmpty()
        && configuredTables.contains(tableReference.identifier())
        && !states.containsKey(tableReference)) {
      return resumeAfterRestart(request);
    }
    return commitTable(request);
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
   */
  private CommitResult resumeAfterRestart(TableCommitRequest request) {
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      // nothing to resume: once created and written to, it starts the ordinary way
      unresumedTables.remove(tableIdentifier);
      return CommitResult.committed(ImmutableList.of());
    }

    // before checking the active tasks, so that only a table with a drain to resume waits for a
    // cycle with tasks; and no state for the others, which would put every configured table under
    // the staging sweep
    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();
    if (!changeSets.pointsAtChangeSet(table, branch)) {
      unresumedTables.remove(tableIdentifier);
      return CommitResult.committed(ImmutableList.of());
    }

    // startDrain takes the table off unresumedTables once it has read the pointer; until then (a
    // cycle with no tasks defers before reading it) the table is listed again
    TableReference tableReference =
        TableReference.of(request.tableReference().catalog(), tableIdentifier, table.uuid());
    return commitTable(
        new TableCommitRequest(
            tableReference,
            request.envelopes(),
            request.controlTopicOffsets(),
            request.commitId(),
            request.validThroughTs(),
            request.activeTasks()));
  }

  private static Set<TableIdentifier> configuredTables(IcebergSinkConfig config) {
    Collection<String> names =
        config.routingStrategy() == RecordRoutingStrategy.TOPIC_TO_TABLE
            ? config.topicToTableMapping().values()
            : config.tables();
    if (names == null) {
      return ImmutableSet.of();
    }
    return names.stream().map(TableIdentifier::parse).collect(ImmutableSet.toImmutableSet());
  }

  /**
   * Under dynamic routing, holds the control topic at the latest response of the table in {@code
   * envelopes}, if there is one (ADR-0042). A table the configuration names needs no anchor: it is
   * probed after a restart. A merge-on-read tail is not one: the start spends it outright.
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

  private CommitResult commitTable(TableCommitRequest request) {
    TableDrainState state = states.computeIfAbsent(request.tableReference(), TableDrainState::new);

    // tryLock, never lock: doCommit() joins this pool from the coordinator thread, and what holds
    // the lock is a slice being planned or an Iceberg commit retrying, for minutes. A table
    // mid-transition is skipped: its envelopes stay buffered, its offsets held, and pendingTables()
    // brings it back next cycle
    if (!state.lock.tryLock()) {
      return CommitResult.pending(ImmutableList.of());
    }
    try {
      // reported once, whether or not this cycle does anything else: the change set that spent
      // them is persisted, so the coordinator may drop them from its buffer
      List<Envelope> consumed = takeConsumed(state);

      stopIfASecondNameOfTheSameTable(state);

      if (state.stopped()) {
        // already reported at ERROR, once, when it happened. Repeating it every commit interval
        // would bury the one message that says what to fix
        return CommitResult.failed(consumed);
      }

      if (!request.activeTasks().isEmpty()) {
        // refreshed every cycle rather than pinned for the whole drain: after a rebalance
        // mid-drain, re-assignments would otherwise go to dead task ids
        state.activeTasks = request.activeTasks();
      }

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
        if (!stopIfPermanent(state, e)) {
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

  /**
   * Tables the coordinator must offer a commit to even when no response for them is buffered.
   *
   * <p>A drain outlives the responses that started it: they are reported spent after the first
   * slice commits, and the table drops out of {@code tableCommitMap()}. A drain that then fails, or
   * that a restart cut short, would wait for the next record written to the table: indefinitely,
   * for a stream gone quiet. An active drain is listed too: {@code commit()} refreshes the active
   * tasks, and a drain spanning a rebalance needs that to stop handing slices to tasks that are
   * gone.
   */
  @Override
  public Set<TableReference> pendingTables() {
    Set<TableReference> pending = Sets.newLinkedHashSet();
    states.forEach(
        (tableReference, state) -> {
          if (!state.lock.tryLock()) {
            // a transition is running, so this table is plainly not idle
            pending.add(tableReference);
            return;
          }
          try {
            // a stopped table is listed too: it stops with something unapplied (its envelopes, or
            // the tail of a change set whose envelopes are spent) and answering FAILED every
            // cycle is what keeps CommitComplete from claiming a valid-through it has not reached.
            // commit() answers without any IO and without repeating the ERROR
            if (state.stopped() || state.active() || state.unfinishedDrain) {
              pending.add(tableReference);
            }
          } finally {
            state.lock.unlock();
          }
        });
    // and a drain a previous coordinator left behind, before anything is known of it
    unresumedTables.forEach(
        tableIdentifier -> pending.add(TableReference.of(catalog.name(), tableIdentifier, null)));
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
        .values()
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
        new PermanentCopyOnWriteException(
            String.format(
                Locale.ROOT,
                "task %s reported that slice %d of change set %s fails the same way on every "
                    + "attempt, and that task's log names the cause and the remedy",
                payload.taskId(),
                payload.sliceSeq(),
                payload.changeSetId()));
    state.awaiting = false;
    submit(
        state,
        () -> {
          stopIfPermanent(state, failure);
          discardSliceFiles(state, false);
          failDrain(state);
        });
  }

  @Override
  public void process(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {
    for (TableDrainState state : states.values()) {
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
          cancelSlice(state, Cancellation.TIMED_OUT, timeoutReason(state));
        }
      } finally {
        state.lock.unlock();
      }
    }

    stagingCleanup.maybeClean(nowMs, bufferedResponses);
  }

  private String timeoutReason(TableDrainState state) {
    long timeoutMs = config.copyOnWriteRewriteTimeoutMs();
    // no plan on the append path: an empty table or a new branch, with nothing to rewrite
    long planBytes = state.planSummary == null ? 0 : state.planSummary.totalRewriteBytes();
    if (planBytes > config.copyOnWriteMaxRewriteBytes()) {
      return String.format(
          Locale.ROOT,
          "the slice plans %d bytes, over the %d byte quota, and was not rewritten within "
              + "iceberg.tables.copy-on-write.rewrite-timeout-ms (%d ms); it is handed out again "
              + "with the same plan, so raise that timeout to fit %d bytes (chunks received: %s)",
          planBytes,
          config.copyOnWriteMaxRewriteBytes(),
          timeoutMs,
          planBytes,
          state.attempt.receivedChunks());
    }

    return String.format(
        Locale.ROOT,
        "no complete answer within %d ms from %s (chunks received: %s)",
        timeoutMs,
        state.attempt.missingTasks(),
        state.attempt.receivedChunks());
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
      states.remove(tableReference, state);
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

    state.table = table;
    state.branch = config.tableConfig(tableIdentifier.toString()).commitBranch();
    state.stagingLocation =
        StagedChangeFileWriter.stagingLocation(table, config.copyOnWriteStagingLocation());
    Set<Integer> identifierFieldIds = planner.identifierFieldIds(table, tableReference);

    ChangeSetStore.DrainStart start =
        changeSets.loadDrainState(
            table, state.branch, tableReference, state.stagingLocation, identifierFieldIds);
    // the pointer is read: whatever a previous coordinator left is this state's from here on
    unresumedTables.remove(tableIdentifier);
    // a pointer naming a change set that was just dropped is a drain nobody has finished: until the
    // change set adopting its files first commits, a failure anywhere below, the freeze included,
    // must leave the table offered a commit every cycle, buffered responses or not
    if (!start.carryOverFiles().isEmpty()) {
      state.unfinishedDrain = true;
    } else if (start.manifest() == null) {
      // nothing left to drain: whatever the last change set anchored at is applied
      state.replayAnchor = null;
    }

    TableCommitRequest offered = request;
    if (request.envelopes().stream().anyMatch(Envelopes::isDataWritten)) {
      boolean drainInProgress = start.manifest() != null || !start.carryOverFiles().isEmpty();
      offered = mergeOnReadTail.offer(request, drainInProgress, table, spent);
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
              start.carryOverFiles(),
              start.carryOverTopics());
      if (frozen == null) {
        LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
        spent.addAll(offered.envelopes());
        return DrainStartOutcome.NOTHING_TO_DO;
      }

      state.droppedManifestLocation = changeSets.adoptDroppedChangeSet(tableReference, start);
      state.manifest = frozen.manifest();
      state.manifestLocation = frozen.location();
      state.cursor = null;
      state.frozenEnvelopes = ImmutableList.copyOf(offered.envelopes());
      // a change set adopting the files of a dropped one, frozen from an empty buffer, keeps the
      // anchor the dropped one had
      anchorAt(state, offered.envelopes());
      state.spent = false;
      state.freshlyFrozen = true;
      state.unfinishedDrain = !start.carryOverFiles().isEmpty();
    } else {
      // resumed from the snapshot summary: this change set's envelopes were spent by whoever froze
      // it, possibly a previous coordinator. This cycle's envelopes are not part of it and stay
      // buffered until it drains
      state.manifest = start.manifest();
      state.manifestLocation = start.manifestLocation();
      state.cursor = start.cursor();
      state.frozenEnvelopes = ImmutableList.of();
      state.spent = false;
      // a snapshot points at this manifest: it is not ours to delete on failure
      state.freshlyFrozen = false;
      // a persisted change set that is not finished: the table has to be revisited every cycle
      // from here on, whether or not anything new is ever written to it again
      state.unfinishedDrain = true;
      // the envelope a restart replayed to get here is the one to replay after the next restart,
      // though the freeze after this drain will find it applied
      anchorAt(state, request.envelopes());
      LOG.info(
          "Resuming copy-on-write change set {} for table {} from cursor {}",
          start.manifest().changeSetId(),
          tableIdentifier,
          start.cursor());
    }

    state.commitId = request.commitId();
    // from the manifest, not from this cycle: a resumed drain exhausts a change set frozen in an
    // earlier cycle, whose validThroughTs is the one its commits may state. This cycle's responses
    // are not in it: they wait in the buffer for the next change set
    state.validThroughTs = state.manifest.validThroughTs();
    state.sliceSeq = 0;
    state.changeSetBytesReplaced = 0;
    state.changeSetSlicesCommitted = 0;
    state.sliceProgressWarningsLogged = 0;
    state.resetSliceRetryQuotas(config.copyOnWriteCommitRetries());

    // normalizing and planning is the expensive half of starting a drain, and doCommit() must not
    // wait for it. Everything above is metadata IO, which merge-on-read does inline as well: the
    // freeze checks identifier fields from the staged files' descriptors without opening them
    submit(state, () -> startSlice(state));
    return DrainStartOutcome.STARTED;
  }

  /**
   * Whether the responses were written for a table since dropped and created again under its name:
   * the UUID they carry is not the one of the table the name loads. Their reference keys a state of
   * its own beside the new table's, so drained, they would be a second drain into the new table,
   * laying changes older than its own over its newer values, from staged files written against
   * another schema. They are dropped instead, as the change set frozen for the old table is (6.13),
   * their staged files deleted by name: nothing else would, the staging cleanup of the new table
   * keeps what its buffer names. The state goes, and its replay anchor with it. A reference without
   * a UUID is not checked.
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
    states.remove(tableReference, state);
    drainingUnder.remove(writtenFor, tableReference);
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

  private static String sliceProgressWarning(TableDrainState state) {
    return String.format(
        Locale.ROOT,
        "Copy-on-write change set %s for table %s is being sliced: %d slice(s) committed so far, "
            + "%d byte(s) rewritten in them. To cut the slicing rather than live with it, raise "
            + "iceberg.control.commit.interval-ms, partition or sort the table by the key, or "
            + "switch the table back to merge-on-read",
        state.manifest.changeSetId(),
        state.tableReference.identifier(),
        state.changeSetSlicesCommitted,
        state.changeSetBytesReplaced);
  }

  private void warnOverByteQuota(
      TableDrainState state, PlanSummary summary, int keys, int sliceKeys) {
    UUID changeSetId = state.manifest.changeSetId();
    if (changeSetId.equals(state.overByteQuotaWarnedFor)) {
      return;
    }

    state.overByteQuotaWarnedFor = changeSetId;
    state.overByteQuotaWarning =
        String.format(
            Locale.ROOT,
            "Copy-on-write change set %s for table %s: a single key plans %d bytes, over the %d byte "
                + "quota, so slice %d goes out over it with the %d of %d key(s) that plan no more than "
                + "one key does; %d%% of the planned files have no bounds on the identifier columns. "
                + "Such a slice has to be rewritten within "
                + "iceberg.tables.copy-on-write.rewrite-timeout-ms (%d ms), or it is cancelled and "
                + "handed out again whole every cycle, never getting any further: raise that timeout "
                + "to fit %d bytes. Slices of this table stay over the quota until one of: raise "
                + "iceberg.tables.copy-on-write.max-rewrite-bytes, enable write.metadata.metrics.* for "
                + "the identifier columns, partition or sort the table by the key, or switch the table "
                + "back to merge-on-read",
            changeSetId,
            state.tableReference.identifier(),
            summary.totalRewriteBytes(),
            config.copyOnWriteMaxRewriteBytes(),
            state.sliceSeq,
            keys,
            sliceKeys,
            Math.round(summary.fractionFilesWithoutBounds() * 100),
            config.copyOnWriteRewriteTimeoutMs(),
            summary.totalRewriteBytes());
    LOG.warn("{}", state.overByteQuotaWarning);
  }

  private void warnFilesWithoutBounds(TableDrainState state, PlanSummary summary) {
    UUID changeSetId = state.manifest.changeSetId();
    if (changeSetId.equals(state.filesWithoutBoundsWarnedFor)) {
      return;
    }

    state.filesWithoutBoundsWarnedFor = changeSetId;
    LOG.warn(
        "Copy-on-write change set {} for table {}: {}% of the planned files have no bounds on "
            + "the identifier columns and cannot be pruned by them, so they are rewritten on "
            + "every slice that touches their key range. Enable write.metadata.metrics.* for the "
            + "identifier columns to avoid it",
        changeSetId,
        state.tableReference.identifier(),
        Math.round(summary.fractionFilesWithoutBounds() * 100));
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
        ChangeSetManifest manifest = state.manifest;
        String manifestLocation = state.manifestLocation;
        unlessStopped(() -> changeSets.cleanupChangeSet(table.io(), manifest, manifestLocation));
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
          rewriteOutcome(state, committedFiles));

      if (state.drained) {
        state.unfinishedDrain = false;
        state.reset();
      } else {
        state.cursor = state.slice.lastKey().orElse(state.cursor);
        state.sliceSeq += 1;
        // on every intermediate commit, unlike warnOverByteQuota: an operator filtering on WARN
        // must see write amplification while it happens
        state.sliceProgressWarningsLogged += 1;
        LOG.warn("{}", sliceProgressWarning(state));
        startSliceGuarded(state);
      }
    } catch (RuntimeException e) {
      // the snapshot stands: a failure here (the control topic, the lineage sink, planning the
      // next slice) fails the drain until next cycle, never touches a committed file.
      // discardSliceFiles can only reach the next slice's normalized file: the committed files
      // were taken off the state above, and the next slice has collected no answers yet
      if (!stopIfPermanent(state, e)) {
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
      if (!stopIfPermanent(state, e)) {
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
      if (!stopIfPermanent(state, e)) {
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

  private String rewriteOutcome(TableDrainState state, List<DataFile> committedFiles) {
    long bytesWritten = committedFiles.stream().mapToLong(DataFile::fileSizeInBytes).sum();
    if (state.planSummary == null) {
      // the append path: an empty table or a new branch, where nothing was planned and so there is
      // no before-and-after to report
      return String.format(
          Locale.ROOT,
          "appended %d file(s), %d byte(s), nothing replaced",
          committedFiles.size(),
          bytesWritten);
    }
    return state
        .planSummary
        .withRewriteResult(committedFiles.size(), bytesWritten, state.attempt.assignedTaskCount())
        .toString();
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
    state.cancellationWarning =
        String.format(
            Locale.ROOT,
            "Cancelling slice %d of change set %s for table %s: %s. Attempts at this slice "
                + "cancelled in a row: %d; handing it out again %s",
            state.sliceSeq,
            state.manifest.changeSetId(),
            state.tableReference.identifier(),
            reason,
            state.cancelledInARow,
            retryAtOnce ? "now" : "on the next cycle");
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

  /**
   * Stops a table that is already draining under another name, before it starts a drain of its own.
   *
   * <p>A state is keyed by the name a worker wrote its change files under, and a case-insensitive
   * catalog answers to more than one spelling: {@code iceberg.tables=db.t,DB.T} names one table
   * twice, and each name would get a drain of its own: two slice sequences and two {@code
   * OverwriteFiles} streams into one table, each writing the cursor the other one reads. The
   * table's UUID tells them apart from two different tables; the first reference to claim it keeps
   * draining, every other is stopped once, with the remedy. The claim is atomic because tables of
   * one cycle commit on the pool side by side.
   *
   * <p>Called with {@code state}'s lock held. A table whose UUID the catalog does not report cannot
   * be told apart this way and is left alone.
   */
  private void stopIfASecondNameOfTheSameTable(TableDrainState state) {
    UUID tableUuid = state.tableReference.uuid();
    if (tableUuid == null || state.stopped()) {
      return;
    }

    TableReference draining = drainingUnder.putIfAbsent(tableUuid, state.tableReference);
    if (draining == null || draining.equals(state.tableReference)) {
      return;
    }

    stopIfPermanent(
        state,
        new PermanentCopyOnWriteException(
            String.format(
                Locale.ROOT,
                "this is the table already draining as %s -- one Iceberg table (UUID %s) under two "
                    + "names, which a case-insensitive catalog accepts. Each name would plan, "
                    + "slice and commit a copy-on-write drain of its own into that one table, so "
                    + "this one is stopped: name the table once in iceberg.tables (or once per "
                    + "routing rule), keeping the spelling that is draining",
                draining.identifier(),
                tableUuid)));
  }

  private boolean stopIfPermanent(TableDrainState state, RuntimeException failure) {
    if (!(failure instanceof PermanentCopyOnWriteException)) {
      return false;
    }

    state.stoppedReason = failure.getMessage();
    // a manifest the pointer names that is gone stops the table as it is read: pendingTables()
    // must not go on offering it through resumeAfterRestart
    unresumedTables.remove(state.tableReference.identifier());
    LOG.error(
        "Copy-on-write is stopped for table {}: {}. Retrying would fail the same way every commit "
            + "interval, so it is not retried: this table's responses stay in the coordinator's "
            + "buffer and its control topic offsets stay held until the connector is restarted. "
            + "Fix what this message names, then restart the connector",
        state.tableReference.identifier(),
        failure.getMessage(),
        failure);
    return true;
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
              if (!stopIfPermanent(state, e)) {
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
    return underLock(tableReference, CopyOnWriteTableCommitter::sliceProgressWarning);
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
