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

import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsRecords;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;

/**
 * The statistics of the final snapshot F of a copy-on-write drain of change set X: the step of F at
 * the end of the drain, the lookup of a pending F (one whose step did not finish), its recount from
 * the manifest of X and the refusal of that recount, and the choice between the recount and the
 * step on the head for a lag step.
 *
 * <p>Every method runs under the lock of the state it is given, held by {@link
 * CopyOnWriteTableCommitter}. Only metadata, the manifest of X and the staged files the step reads
 * are touched here: the replacement data files of a slice are the committer's alone.
 */
class CopyOnWriteFinalStatistics {
  private final IcebergSinkConfig config;
  private final ChangeSetStore changeSets;
  private final StatisticsLag statisticsLag;
  private final StatisticsHealth health;
  private final StatisticsStep step;
  private final BooleanSupplier stopped;
  private final Logger log;

  CopyOnWriteFinalStatistics(
      IcebergSinkConfig config,
      ChangeSetStore changeSets,
      StatisticsLag statisticsLag,
      StatisticsHealth health,
      StatisticsStep step,
      BooleanSupplier stopped,
      Logger log) {
    this.config = config;
    this.changeSets = changeSets;
    this.statisticsLag = statisticsLag;
    this.health = health;
    this.step = step;
    this.stopped = stopped;
    this.log = log;
  }

  /**
   * A final slice whose commit did not report its outcome and whose snapshot was not found: if it
   * landed after all, F gets no step of its own, so the table enters the lag.
   */
  void finalSliceNotFound(TableDrainState state) {
    if (statisticsEnabled(state)) {
      statisticsLag.enter(state.tableReference.identifier());
    }
  }

  /** Logs a drain start that froze nothing, or a merge-on-read tail waiting for a recount of F. */
  void logNothingFrozen(TableDrainState state, TableIdentifier tableIdentifier, boolean tailWaits) {
    TableDrainState.PendingFinal pending = state.pendingFinal;
    if (!tailWaits) {
      log.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else if (pending.changeSetId.equals(state.tailWaitLoggedFor)) {
      log.debug(
          "MoR tail of table {} waits for the NDV recompute of snapshot {} (change set {})",
          tableIdentifier,
          pending.snapshotId,
          pending.changeSetId);
    } else {
      state.tailWaitLoggedFor = pending.changeSetId;
      log.info(
          "MoR tail of table {} waits for the NDV recompute of snapshot {} (change set {})",
          tableIdentifier,
          pending.snapshotId,
          pending.changeSetId);
    }
  }

  private boolean stoppedOrInterrupted() {
    return stopped.getAsBoolean() || Thread.currentThread().isInterrupted();
  }

  private void unlessStopped(Runnable delete) {
    if (!stopped.getAsBoolean()) {
      delete.run();
    }
  }

  /**
   * Like {@code findPendingFinal}, but a failure to read is no pending F: statistics never hold up
   * the commit of data.
   */
  TableDrainState.PendingFinal lookUpPendingFinal(
      TableDrainState state, Table table, UUID commitId) {
    try {
      return findPendingFinal(state, table, commitId);
    } catch (RuntimeException e) {
      if (stoppedOrInterrupted()) {
        log.debug(
            "Cannot look up the final snapshot of table {}: stopped",
            state.tableReference.identifier(),
            e);
        return null;
      }

      log.warn(
          "Cannot look up the final snapshot of table {} whose statistics step did not finish; "
              + "the drain starts without its NDV recompute",
          state.tableReference.identifier(),
          e);
      return null;
    }
  }

  /**
   * The pending final snapshot F: the latest snapshot of the connector on the branch, with the
   * statistics of the table on, if it points to a change set X and has no NDV record, X is neither
   * refused nor settled in this state, and the manifest of X is of use for a recount.
   *
   * @param table the table as the caller has just read it; the branch, the staging location and the
   *     marks of X are the state's
   * @return F, or {@code null} if there is none
   * @throws RuntimeException a failure to read the manifest of X that may pass
   */
  private TableDrainState.PendingFinal findPendingFinal(
      TableDrainState state, Table table, UUID commitId) {
    if (!statisticsEnabled(state)) {
      return null;
    }

    Snapshot latest = changeSets.latestConnectorSnapshot(table, state.branch);
    String pointer =
        latest == null ? null : latest.summary().get(ChangeSetStore.FINAL_CHANGE_SET_ID_PROP);
    // F with a statistics file waits for nothing
    if (pointer == null || StatisticsRecords.hasNdv(table, latest.snapshotId())) {
      return null;
    }

    TableIdentifier tableIdentifier = state.tableReference.identifier();
    UUID changeSetId;
    try {
      changeSetId = UUID.fromString(pointer);
    } catch (IllegalArgumentException e) {
      log.warn(
          "Final snapshot {} of table {} names change set {}, which is no change set id; its NDV "
              + "is not recomputed",
          latest.snapshotId(),
          tableIdentifier,
          pointer,
          e);
      return null;
    }

    if (changeSetId.equals(state.refusedFinalChangeSetId)) {
      log.debug(
          "NDV recompute of the final snapshot {} of table {} from change set {} was refused "
              + "already: it is not done again",
          latest.snapshotId(),
          tableIdentifier,
          changeSetId);
      return null;
    }

    if (changeSetId.equals(state.settledFinalChangeSetId)) {
      log.debug(
          "Statistics step of the final snapshot {} of table {} from change set {} ended already: "
              + "its NDV is not recomputed",
          latest.snapshotId(),
          tableIdentifier,
          changeSetId);
      return null;
    }

    String location =
        ChangeSetManifest.location(
            state.stagingLocation, state.tableReference, config.connectGroupId(), changeSetId);
    ChangeSetManifest manifest =
        pendingManifest(tableIdentifier, table, latest.snapshotId(), changeSetId, location);
    return manifest == null
        ? null
        : new TableDrainState.PendingFinal(
            latest.snapshotId(), changeSetId, manifest, location, commitId);
  }

  /**
   * The manifest of X if a recount of F can use it, else {@code null}.
   *
   * @throws RuntimeException a failure to read it other than a missing file or no manifest
   */
  private ChangeSetManifest pendingManifest(
      TableIdentifier tableIdentifier,
      Table table,
      long snapshotId,
      UUID changeSetId,
      String location) {
    ChangeSetManifest manifest;
    try {
      manifest = ChangeSetManifest.read(table.io(), location);
    } catch (NotFoundException e) {
      log.debug(
          "Change set {} of the final snapshot {} of table {} is gone: the statistics step of the "
              + "snapshot finished",
          changeSetId,
          snapshotId,
          tableIdentifier,
          e);
      return null;
    } catch (IllegalArgumentException e) {
      log.warn(
          "Change set {} of the final snapshot {} of table {} has no manifest at {}; its NDV is "
              + "not recomputed",
          changeSetId,
          snapshotId,
          tableIdentifier,
          location,
          e);
      return null;
    }

    if (manifest.tableUuid() != null && !manifest.tableUuid().equals(table.uuid())) {
      log.warn(
          "Change set {} of the final snapshot {} of table {} was frozen for table {}, not {}; its "
              + "NDV is not recomputed",
          changeSetId,
          snapshotId,
          tableIdentifier,
          manifest.tableUuid(),
          table.uuid());
      return null;
    }

    if (!manifest.formatVersionSupported()) {
      log.warn(
          "Change set {} of the final snapshot {} of table {} has manifest format version {}, and "
              + "this build reads version {}; its NDV is not recomputed",
          changeSetId,
          snapshotId,
          tableIdentifier,
          manifest.formatVersion(),
          ChangeSetManifest.CURRENT_FORMAT_VERSION);
      return null;
    }

    return manifest;
  }

  /**
   * The recount of F in a lag step. {@code FAILED} here is no refusal: X and the Puffin files stay,
   * and a later lag step recounts again.
   */
  private Result recountInLagStep(TableDrainState state, TableDrainState.PendingFinal pending) {
    Result result = runRecount(state, pending, pending.commitId, true);
    if (result.outcome() != StatisticsStep.Outcome.FAILED) {
      // spent: a recount transition set by the start of a drain leaves by its guard. After a
      // failure it recounts F in this cycle, so the tail does not wait for the next one
      state.pendingFinal = null;
    }

    if (stoppedOrInterrupted()) {
      logRecountStopped(state, pending, result.failure());
      return new Result(StatisticsStep.Outcome.STOPPED, null);
    }

    if (result.outcome() != StatisticsStep.Outcome.FAILED
        && result.outcome() != StatisticsStep.Outcome.STOPPED) {
      removeChangeSet(state, pending);
    }

    return result;
  }

  /**
   * The lag step: the recount of the pending F if there is one, else the step on the head with no
   * own snapshots and no answers. A failure to look up F is thrown.
   */
  Result lagStepOrRecount(TableDrainState state, Table table, UUID commitId) {
    TableIdentifier tableIdentifier = state.tableReference.identifier();
    TableDrainState.PendingFinal pending = findPendingFinal(state, table, commitId);
    if (pending != null) {
      return recountInLagStep(state, pending);
    }

    return step.run(
        StatisticsStepInput.builder()
            .table(table)
            .identifier(tableIdentifier)
            .commitId(commitId)
            .stopped(stopped)
            .config(config)
            .tableConfig(config.tableConfig(tableIdentifier.toString()))
            .health(health)
            .build());
  }

  /**
   * The end of a drained change set: the statistics step of F, then the files of the change set go,
   * unless the step left them to the recount of F.
   *
   * @param snapshotId F, or {@code null} if the committed final slice was not found
   */
  void finishChangeSet(TableDrainState state, Long snapshotId) {
    if (finalSnapshotStatistics(state, snapshotId)) {
      Table table = state.table;
      ChangeSetManifest manifest = state.manifest;
      String manifestLocation = state.manifestLocation;
      // with the statistics off no step deletes the Puffin files workers wrote while they were on
      boolean sketchFilesLeft = !statisticsEnabled(state) && !manifest.sketchFiles().isEmpty();
      // before the deletion, which may fail: no lookup takes F again
      state.settledFinalChangeSetId = manifest.changeSetId();
      unlessStopped(
          () -> {
            if (sketchFilesLeft) {
              changeSets.deleteSketchFiles(table.io(), manifest);
            }

            changeSets.cleanupChangeSet(table.io(), manifest, manifestLocation);
          });
    }
  }

  /**
   * The statistics step of F, before the files of the change set go: it reads the staged files no
   * Puffin file covers. Never throws.
   *
   * @return true if the files of the change set may go: the statistics of the table are off, or no
   *     recount of F needs them
   */
  private boolean finalSnapshotStatistics(TableDrainState state, Long snapshotId) {
    if (!statisticsEnabled(state)) {
      return true;
    }

    TableIdentifier identifier = state.tableReference.identifier();
    if (snapshotId == null) {
      // the pointer on F leads its recount to the files of the change set, so they stay
      log.info(
          "Final snapshot of change set {} of table {} is not known, so it gets no statistics step; "
              + "the table is in the statistics lag and the change set is kept for the recount",
          state.manifest.changeSetId(),
          identifier);
      statisticsLag.enter(identifier);
      return false;
    }

    // taken before the step: an entry into the lag after it may be of a snapshot newer than F
    long lagToken = statisticsLag.token(identifier);
    Result result = runFinalSnapshotStep(state, snapshotId);
    return actOnFinalSnapshotStep(state, snapshotId, lagToken, result);
  }

  private Result runFinalSnapshotStep(TableDrainState state, long snapshotId) {
    try {
      ChangeSetManifest manifest = state.manifest;
      // a failed step leaves the Puffin files to the recount of F
      return step.run(
          finalSnapshotInput(
              state, snapshotId, manifest.changeSetId(), manifest, state.commitId, true));
    } catch (RuntimeException e) {
      // run() never throws; whatever else does is a failed step
      return stoppedOrInterrupted()
          ? new Result(StatisticsStep.Outcome.STOPPED, null)
          : new Result(StatisticsStep.Outcome.FAILED, e);
    } catch (Error e) {
      enterLagBeforeRethrow(state.tableReference.identifier(), e);
      throw e;
    }
  }

  /**
   * Enters the table into the lag before an {@code Error} of a step propagates: F is left with no
   * step, and a lag step or a restart recounts it.
   */
  void enterLagBeforeRethrow(TableIdentifier identifier, Error error) {
    try {
      statisticsLag.enter(identifier);
    } catch (RuntimeException e) {
      error.addSuppressed(e);
    }
  }

  /**
   * The input of the step of F: own are F and the slices of X and of the change sets X adopted the
   * files of. F carries the pointer to X, not its id, so it is own by its snapshot id.
   */
  private StatisticsStepInput finalSnapshotInput(
      TableDrainState state,
      long snapshotId,
      UUID changeSetId,
      ChangeSetManifest manifest,
      UUID commitId,
      boolean keepSketchesOnFailure) {
    TableIdentifier identifier = state.tableReference.identifier();
    Set<String> ownChangeSetIds = Sets.newLinkedHashSet();
    ownChangeSetIds.add(changeSetId.toString());
    manifest.adoptedChangeSetIds().forEach(adopted -> ownChangeSetIds.add(adopted.toString()));
    return StatisticsStepInput.builder()
        .table(state.table)
        .identifier(identifier)
        .snapshotId(snapshotId)
        .ownSnapshotIds(ImmutableSet.of(snapshotId))
        .ownChangeSetIds(ownChangeSetIds)
        .answers(manifest.statisticsAnswers(snapshotId))
        .commitId(commitId)
        .stopped(stopped)
        .keepSketchesOnFailure(keepSketchesOnFailure)
        .config(config)
        .tableConfig(config.tableConfig(identifier.toString()))
        .health(health)
        .build();
  }

  private Result runRecount(
      TableDrainState state,
      TableDrainState.PendingFinal pending,
      UUID commitId,
      boolean keepSketchesOnFailure) {
    log.info(
        "Recomputing the NDV of snapshot {} of table {} from change set {}, whose statistics step "
            + "did not finish",
        pending.snapshotId,
        state.tableReference.identifier(),
        pending.changeSetId);
    return step.run(
        finalSnapshotInput(
            state,
            pending.snapshotId,
            pending.changeSetId,
            pending.manifest,
            commitId,
            keepSketchesOnFailure));
  }

  /**
   * The recount of F, or its refusal on {@code FAILED} or an exception; once stopped, nothing
   * changes. Never throws an exception. An {@code Error} enters the lag, propagates and is no
   * refusal: X stays, and a later lookup recounts F while it is still the latest snapshot of the
   * connector; after that X goes by the TTL.
   */
  void recountOrRefuse(TableDrainState state, TableDrainState.PendingFinal pending, UUID commitId) {
    // spent, whatever comes of it: a recount transition of F set after leaves by its guard
    state.pendingFinal = null;
    Throwable failure;
    try {
      long lagToken = statisticsLag.token(state.tableReference.identifier());
      Result result = runRecount(state, pending, commitId, false);
      if (result.outcome() != StatisticsStep.Outcome.FAILED && !stoppedOrInterrupted()) {
        if (actOnFinalSnapshotStep(state, pending.snapshotId, lagToken, result)) {
          removeChangeSet(state, pending);
        }

        return;
      }

      failure = result.failure();
    } catch (RuntimeException e) {
      failure = e;
    } catch (Error e) {
      // not a refusal: X stays
      enterLagBeforeRethrow(state.tableReference.identifier(), e);
      throw e;
    }

    if (stoppedOrInterrupted()) {
      logRecountStopped(state, pending, failure);
      return;
    }

    refuse(state, pending, failure);
  }

  /**
   * A refusal of the recount of F. X is marked first, so that no lookup takes F again even if X
   * cannot be deleted; then the table enters the lag and X goes with its Puffin files, which no
   * step is left to delete.
   */
  private void refuse(
      TableDrainState state, TableDrainState.PendingFinal pending, Throwable failure) {
    TableIdentifier identifier = state.tableReference.identifier();
    state.refusedFinalChangeSetId = pending.changeSetId;
    try {
      statisticsLag.enter(identifier);
      statisticsLag.recordFailure(identifier);
      log.warn(
          "NDV of change set {} of table {} is lost: the NDV recompute of its final snapshot {} "
              + "failed, and the change set is left to be deleted. Later statistics steps count "
              + "its changes as another writer's: over the gap limit, only ANALYZE of the table "
              + "restores its NDV",
          pending.changeSetId,
          identifier,
          pending.snapshotId,
          failure);
      Table table = state.table;
      unlessStopped(
          () -> {
            changeSets.deleteSketchFiles(table.io(), pending.manifest);
            changeSets.cleanupChangeSet(table.io(), pending.manifest, pending.manifestLocation);
          });
    } catch (RuntimeException e) {
      // whatever failed may have been the entry into the lag or the WARN above: this one has to
      // tell the operator on its own that the NDV may be gone and what brings it back
      log.warn(
          "Could not finish the refusal of the recount of change set {} of table {}: the drain "
              + "goes on, the change set is left to expire. NDV of the change set may be lost, "
              + "and the table may not be in the statistics lag: ANALYZE of the table restores it",
          pending.changeSetId,
          identifier,
          e);
    }
  }

  /** Deletes X after its recount; X is marked first, so that no lookup takes F again. */
  private void removeChangeSet(TableDrainState state, TableDrainState.PendingFinal pending) {
    state.settledFinalChangeSetId = pending.changeSetId;
    Table table = state.table;
    unlessStopped(
        () -> changeSets.cleanupChangeSet(table.io(), pending.manifest, pending.manifestLocation));
  }

  private void logRecountStopped(
      TableDrainState state, TableDrainState.PendingFinal pending, Throwable failure) {
    log.debug(
        "NDV recompute of the final snapshot {} of table {} is stopped, change set {} is kept",
        pending.snapshotId,
        state.tableReference.identifier(),
        pending.changeSetId,
        failure);
  }

  /** Returns false if the files of the change set stay for the recount of F. */
  private boolean actOnFinalSnapshotStep(
      TableDrainState state, long snapshotId, long lagToken, Result result) {
    StatisticsStep.Outcome outcome = result.outcome();
    boolean firstFailure =
        statisticsLag.applyOutcome(state.tableReference.identifier(), lagToken, outcome);
    if (outcome == StatisticsStep.Outcome.FAILED) {
      logFailedFinalSnapshotStep(state, snapshotId, firstFailure, result.failure());
    }

    return outcome != StatisticsStep.Outcome.FAILED && outcome != StatisticsStep.Outcome.STOPPED;
  }

  private void logFailedFinalSnapshotStep(
      TableDrainState state, long snapshotId, boolean firstFailure, Throwable failure) {
    TableIdentifier identifier = state.tableReference.identifier();
    if (firstFailure) {
      log.warn(
          "Statistics step of the final snapshot {} of change set {} of table {} failed; the table "
              + "is in the statistics lag and the change set is kept for the recount",
          snapshotId,
          state.manifest.changeSetId(),
          identifier,
          failure);
    } else {
      log.debug(
          "Statistics step of the final snapshot {} of change set {} of table {} failed again; the "
              + "table stays in the statistics lag",
          snapshotId,
          state.manifest.changeSetId(),
          identifier,
          failure);
    }
  }

  private boolean statisticsEnabled(TableDrainState state) {
    return config.tableConfig(state.tableReference.identifier().toString()).statisticsEnabled();
  }
}
