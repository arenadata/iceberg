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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.IntermediateSketchSet;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The statistics steps of a merge-on-read committer, one queue per table by {@link
 * TableIdentifier}, and the tables of its staging cleanup. Steps never run on the thread of {@code
 * commit()}: they go to the transition pool.
 *
 * <p>A table has at most one running task and one waiting task. A task set while none runs goes to
 * the pool at once; while one runs it waits; while one waits the two merge ({@link
 * StatisticsStepTask#mergedWith}). When the running task ends, the waiting one goes to the pool. A
 * lag step is not set while the table has any task; it waits for a permit outside the queue and
 * takes the queue only if it is empty.
 *
 * <p>A task loads its own {@link Table} right before the step: the {@code Table} of {@code
 * commit()} is not thread-safe, and the task only holds it for its {@code FileIO}. After the step
 * the task applies the committer's {@link OutcomeAction} and only then leaves the queue, so the
 * intermediate Puffin files of a task are reachable through {@link #inFlightSketchLocations} until
 * the task has tried to delete them.
 */
class StatisticsStepQueue {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsStepQueue.class);

  /** What the committer does by the outcome of a step; called on the pool thread of the task. */
  @FunctionalInterface
  interface OutcomeAction {
    void apply(StatisticsStepQueue queue, StatisticsStepTask task, Result result);
  }

  private final Executor transitionPool;
  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final StatisticsStep step;
  private final StatisticsHealth health;
  private final BooleanSupplier committerStopped;
  private final OutcomeAction outcomeAction;
  private final ConcurrentMap<TableIdentifier, TableSteps> tables = Maps.newConcurrentMap();
  private final ConcurrentMap<TableIdentifier, CleanupTable> cleanupTables =
      Maps.newConcurrentMap();
  private volatile boolean stopped = false;

  StatisticsStepQueue(
      Executor transitionPool,
      Catalog catalog,
      IcebergSinkConfig config,
      StatisticsStep step,
      StatisticsHealth health,
      BooleanSupplier stopped,
      OutcomeAction outcomeAction) {
    this.transitionPool = transitionPool;
    this.catalog = catalog;
    this.config = config;
    this.step = step;
    this.health = health;
    this.committerStopped = stopped;
    this.outcomeAction = outcomeAction;
  }

  /**
   * The action of a standalone merge-on-read committer: the outcome goes to its lag, and a table
   * that is gone leaves the staging cleanup.
   */
  static OutcomeAction standaloneOutcome(StatisticsLag lag) {
    return standaloneOutcome(lag, LOG);
  }

  static OutcomeAction standaloneOutcome(StatisticsLag lag, Logger log) {
    return (queue, task, result) -> {
      TableIdentifier id = task.tableReference().identifier();
      boolean firstFailure = lag.applyOutcome(id, task.lagToken(), result.outcome());
      if (result.outcome() == Outcome.TABLE_GONE) {
        queue.forgetTable(id);
      } else if (result.outcome() == Outcome.FAILED) {
        logFailure(log, firstFailure, task, result);
      }
    };
  }

  /**
   * The action of the merge-on-read tail of copy-on-write: it only puts the table into the lag of
   * copy-on-write and never removes it.
   */
  static OutcomeAction tailOutcome(StatisticsLag copyOnWriteLag) {
    return tailOutcome(copyOnWriteLag, LOG);
  }

  static OutcomeAction tailOutcome(StatisticsLag copyOnWriteLag, Logger log) {
    return (queue, task, result) -> {
      TableIdentifier id = task.tableReference().identifier();
      if (result.outcome() == Outcome.FAILED) {
        copyOnWriteLag.enter(id);
        logFailure(log, copyOnWriteLag.recordFailure(id), task, result);
      } else if (result.outcome() == Outcome.SNAPSHOT_GONE) {
        copyOnWriteLag.enter(id);
      }
    };
  }

  private static void logFailure(
      Logger log, boolean firstInARow, StatisticsStepTask task, Result result) {
    // a lag step has no snapshot of its own: it is run at the head
    String snapshot = task.snapshotId() == null ? "the head" : "snapshot " + task.snapshotId();
    if (firstInARow) {
      log.warn(
          "Statistics step of table {} at {} failed, the table is in the statistics lag "
              + "and its step is retried in a later commit cycle",
          task.tableReference().identifier(),
          snapshot,
          result.failure());
    } else {
      log.debug(
          "Statistics step of table {} at {} failed again, the table stays in the "
              + "statistics lag",
          task.tableReference().identifier(),
          snapshot,
          result.failure());
    }
  }

  /** Sets the task of a step of the table. Never throws; does nothing once stopped. */
  void enqueue(StatisticsStepTask task) {
    if (isStopped()) {
      LOG.debug("Statistics step queue is stopped, dropping {}", task);
      return;
    }

    TableIdentifier id = task.tableReference().identifier();
    boolean[] runNow = {false};
    tables.compute(
        id,
        (key, steps) -> {
          if (steps == null || steps.running == null) {
            runNow[0] = true;
            return new TableSteps(task, null, steps == null ? null : steps.lagStep);
          }

          StatisticsStepTask waiting =
              steps.waiting == null ? task : steps.waiting.mergedWith(task);
          return new TableSteps(steps.running, waiting, steps.lagStep);
        });

    if (runNow[0]) {
      submit(task);
    }
  }

  /**
   * Sets a lag step unless the table has any task, including a lag step that has not taken the
   * queue yet. A set step holds the table's slot in {@code limiter} until it ends.
   *
   * @return true if the step is set
   */
  boolean enqueueLagStep(
      TableReference tableReference,
      UUID commitId,
      StatisticsLagStepLimiter limiter,
      StatisticsLag lag) {
    TableIdentifier id = tableReference.identifier();
    LagStep lagStep = new LagStep(tableReference, commitId, limiter, lag);
    boolean[] set = {false};
    tables.compute(
        id,
        (key, steps) -> {
          if (steps != null || isStopped()) {
            return steps;
          }

          // inside compute, so that stop() cannot free the slot before it is taken
          limiter.enter(key);
          set[0] = true;
          return new TableSteps(null, null, lagStep);
        });

    if (!set[0]) {
      return false;
    }

    try {
      transitionPool.execute(() -> runLagStep(lagStep));
    } catch (RuntimeException e) {
      LOG.debug("Cannot set the lag step of table {}", id, e);
      dropLagStep(lagStep);
      return false;
    }

    return true;
  }

  /** The Puffin files of the table's running and waiting tasks; thread-safe, never waits. */
  Set<String> inFlightSketchLocations(TableIdentifier id) {
    TableSteps steps = tables.get(id);
    if (steps == null) {
      return ImmutableSet.of();
    }

    ImmutableSet.Builder<String> locations = ImmutableSet.builder();
    if (steps.running != null) {
      locations.addAll(steps.running.sketchLocations());
    }

    if (steps.waiting != null) {
      locations.addAll(steps.waiting.sketchLocations());
    }

    return locations.build();
  }

  boolean hasTask(TableIdentifier id) {
    return tables.containsKey(id);
  }

  /**
   * Keeps only what the staging cleanup needs of the table: with dynamic routing to many tables,
   * holding each would keep its metadata and its {@code FileIO} in the heap.
   */
  void rememberTable(TableIdentifier id, Table table) {
    cleanupTables.put(id, new CleanupTable(table.uuid(), table.location(), table.name()));
  }

  void forgetTable(TableIdentifier id) {
    cleanupTables.remove(id);
  }

  Map<TableIdentifier, CleanupTable> cleanupTables() {
    return ImmutableMap.copyOf(cleanupTables);
  }

  CleanupTable cleanupTable(TableIdentifier id) {
    return cleanupTables.get(id);
  }

  void stop() {
    this.stopped = true;
    for (TableIdentifier id : tables.keySet()) {
      tables.computeIfPresent(
          id,
          (key, steps) -> {
            if (steps.lagStep != null) {
              steps.lagStep.limiter.leave(key);
            }

            if (steps.waiting != null) {
              LOG.debug("Statistics step queue is stopped, dropping {}", steps.waiting);
            }

            return steps.running == null ? null : new TableSteps(steps.running, null, null);
          });
    }
  }

  private boolean isStopped() {
    return stopped || committerStopped.getAsBoolean();
  }

  private boolean isStoppedNow() {
    return isStopped() || Thread.currentThread().isInterrupted();
  }

  private void submit(StatisticsStepTask task) {
    try {
      transitionPool.execute(() -> runTask(task));
    } catch (RuntimeException e) {
      // the pool is shut down: nothing of the table runs any more
      LOG.debug("Cannot run {}, dropping it and the task waiting after it", task, e);
      tables.computeIfPresent(
          task.tableReference().identifier(),
          (key, steps) -> steps.running == task ? TableSteps.of(null, null, steps.lagStep) : steps);
    }
  }

  private void runLagStep(LagStep lagStep) {
    TableIdentifier id = lagStep.tableReference.identifier();
    if (!lagStep.limiter.acquire(this::isStopped)) {
      LOG.debug("Lag step of table {} is dropped: stopped while waiting for a permit", id);
      dropLagStep(lagStep);
      return;
    }

    StatisticsStepTask task = null;
    try {
      task = takeQueue(lagStep);
      if (task == null) {
        LOG.debug(
            "Lag step of table {} is dropped: the table has a step, which removes the lag", id);
        return;
      }

      applyStep(task);
    } finally {
      lagStep.limiter.release();
      if (task == null) {
        dropLagStep(lagStep);
      } else {
        // the slot is left while the step still holds the queue: a new lag step of the table
        // cannot be set before, so this never frees its slot
        lagStep.limiter.leave(id);
        finish(task);
      }
    }
  }

  /**
   * Atomically makes the lag step the running task if the table has no task, taking the token of
   * the lag only then; otherwise drops it and frees its slot.
   *
   * @return the running task, or {@code null} if the queue has a task or the step was dropped
   */
  private StatisticsStepTask takeQueue(LagStep lagStep) {
    StatisticsStepTask[] taken = {null};
    tables.computeIfPresent(
        lagStep.tableReference.identifier(),
        (key, steps) -> {
          if (steps.lagStep != lagStep) {
            return steps;
          }

          if (steps.running != null || steps.waiting != null || isStopped()) {
            lagStep.limiter.leave(key);
            return TableSteps.of(steps.running, steps.waiting, null);
          }

          taken[0] =
              StatisticsStepTask.lagStep(
                  lagStep.tableReference, lagStep.commitId, lagStep.lag.token(key));
          return new TableSteps(taken[0], null, null);
        });
    return taken[0];
  }

  /** Drops the lag step unless it took the queue or was dropped; frees its slot atomically. */
  private void dropLagStep(LagStep lagStep) {
    tables.computeIfPresent(
        lagStep.tableReference.identifier(),
        (key, steps) -> {
          if (steps.lagStep != lagStep) {
            return steps;
          }

          lagStep.limiter.leave(key);
          return TableSteps.of(steps.running, steps.waiting, null);
        });
  }

  private void runTask(StatisticsStepTask task) {
    try {
      applyStep(task);
    } finally {
      finish(task);
    }
  }

  private void applyStep(StatisticsStepTask task) {
    Result result;
    try {
      result = execute(task);
    } catch (Error e) {
      // an OutOfMemoryError of the step: the table still goes into the lag, the Error goes on
      applyOutcome(task, isStoppedNow() ? new Result(Outcome.STOPPED, null) : failed(e));
      throw e;
    }

    applyOutcome(task, result);
  }

  private void applyOutcome(StatisticsStepTask task, Result result) {
    try {
      outcomeAction.apply(this, task, result);
    } catch (RuntimeException e) {
      LOG.warn(
          "Cannot apply the outcome {} of the statistics step of table {}",
          result.outcome(),
          task.tableReference().identifier(),
          e);
    }
  }

  private Result execute(StatisticsStepTask task) {
    if (isStoppedNow()) {
      return new Result(Outcome.STOPPED, null);
    }

    TableIdentifier id = task.tableReference().identifier();
    Table table;
    try {
      table = catalog.loadTable(id);
    } catch (RuntimeException e) {
      return deletingSketches(task, loadFailure(id, e));
    }

    try {
      return step.run(
          StatisticsStepInput.builder()
              .table(table)
              .identifier(id)
              .snapshotId(task.snapshotId())
              .ownSnapshotIds(task.ownSnapshotIds())
              .answers(task.answers())
              .commitId(task.commitId())
              .stopped(this::isStopped)
              .config(config)
              .tableConfig(config.tableConfig(id.toString()))
              .health(health)
              .build());
    } catch (RuntimeException e) {
      // run() never throws; whatever else does is a failed step
      return deletingSketches(task, isStoppedNow() ? new Result(Outcome.STOPPED, null) : failed(e));
    }
  }

  private Result loadFailure(TableIdentifier id, RuntimeException failure) {
    if (isStoppedNow()) {
      LOG.debug("Statistics step of table {} stopped while loading the table", id, failure);
      return new Result(Outcome.STOPPED, null);
    }

    if (failure instanceof NoSuchTableException) {
      LOG.debug("Statistics step of table {}: the table is gone", id, failure);
      health.remove(id);
      return new Result(Outcome.TABLE_GONE, null);
    }

    return failed(failure);
  }

  private static Result failed(Throwable failure) {
    return new Result(Outcome.FAILED, failure);
  }

  /**
   * Deletes the task's intermediate Puffin files when no step did, through the {@code FileIO} of
   * {@code commit()}, unless stopped.
   */
  private Result deletingSketches(StatisticsStepTask task, Result result) {
    if (result.outcome() == Outcome.STOPPED || task.table() == null) {
      return result;
    }

    IntermediateSketchSet.delete(
        task.table().io(),
        task.sketchLocations(),
        task.tableReference().identifier(),
        this::isStoppedNow,
        LOG);
    return result;
  }

  private void finish(StatisticsStepTask task) {
    StatisticsStepTask[] next = {null};
    tables.computeIfPresent(
        task.tableReference().identifier(),
        (key, steps) -> {
          if (steps.running != task) {
            return steps;
          }

          if (steps.waiting == null || isStopped()) {
            return TableSteps.of(null, null, steps.lagStep);
          }

          next[0] = steps.waiting;
          return new TableSteps(steps.waiting, null, steps.lagStep);
        });

    if (next[0] != null) {
      submit(next[0]);
    }
  }

  /** What the staging cleanup keeps of the latest {@code Table} of {@code commit()}. */
  static final class CleanupTable {
    private final UUID uuid;
    private final String location;
    private final String name;

    CleanupTable(UUID uuid, String location, String name) {
      this.uuid = uuid;
      this.location = location;
      this.name = name;
    }

    UUID uuid() {
      return uuid;
    }

    String location() {
      return location;
    }

    String name() {
      return name;
    }
  }

  /** The state of one table's queue; immutable, replaced atomically in {@link #tables}. */
  private static final class TableSteps {
    private final StatisticsStepTask running;
    private final StatisticsStepTask waiting;
    // a lag step set and not in the queue yet: it waits for a permit
    private final LagStep lagStep;

    private TableSteps(StatisticsStepTask running, StatisticsStepTask waiting, LagStep lagStep) {
      this.running = running;
      this.waiting = waiting;
      this.lagStep = lagStep;
    }

    /** The state, or {@code null} (no entry) if it holds nothing. */
    private static TableSteps of(
        StatisticsStepTask running, StatisticsStepTask waiting, LagStep lagStep) {
      if (running == null && waiting == null && lagStep == null) {
        return null;
      }

      return new TableSteps(running, waiting, lagStep);
    }
  }

  /** A lag step that is set and has not taken the queue yet. */
  private static final class LagStep {
    private final TableReference tableReference;
    private final UUID commitId;
    private final StatisticsLagStepLimiter limiter;
    private final StatisticsLag lag;

    private LagStep(
        TableReference tableReference,
        UUID commitId,
        StatisticsLagStepLimiter limiter,
        StatisticsLag lag) {
      this.tableReference = tableReference;
      this.commitId = commitId;
      this.limiter = limiter;
      this.lag = lag;
    }
  }
}
