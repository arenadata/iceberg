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

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves the statistics lag and the health re-checks of copy-on-write tables with no drain active,
 * unfinished or stopped. A lag step runs on the transition pool under the lock of the table's
 * state. Statistics never fail a cycle.
 */
class CopyOnWriteLagStepRunner implements StatisticsLagChecker.Committer {
  private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteLagStepRunner.class);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final Executor exec;
  private final TableDrainRegistry states;
  private final StatisticsLag statisticsLag;
  private final StatisticsHealth health;
  private final CopyOnWriteFinalStatistics finalStatistics;
  // whether a statistics step of the table's merge-on-read tail is set
  private final Predicate<TableReference> tailStepSet;
  private final BooleanSupplier stopped;
  private final StatisticsLagStepLimiter lagStepLimiter;
  private final StatisticsLagChecker lagChecker;

  CopyOnWriteLagStepRunner(
      Catalog catalog,
      IcebergSinkConfig config,
      Executor exec,
      TableDrainRegistry states,
      StatisticsLag statisticsLag,
      StatisticsHealth health,
      CopyOnWriteFinalStatistics finalStatistics,
      Predicate<TableReference> tailStepSet,
      BooleanSupplier stopped) {
    this.catalog = catalog;
    this.config = config;
    this.exec = exec;
    this.states = states;
    this.statisticsLag = statisticsLag;
    this.health = health;
    this.finalStatistics = finalStatistics;
    this.tailStepSet = tailStepSet;
    this.stopped = stopped;
    this.lagStepLimiter = new StatisticsLagStepLimiter(config.commitThreads());
    this.lagChecker = new StatisticsLagChecker(catalog, config, statisticsLag, health);
  }

  /**
   * Adds to {@code pending} the tables of the statistics lag and those assigned a health re-check
   * in this cycle, admitting lag steps for the free slots of the limiter. Never throws.
   */
  void addTables(Set<TableReference> pending) {
    try {
      statisticsLag.admit(lagStepLimiter.freeSlots(), lagStepLimiter::inFlight);
      statisticsLag.tables().forEach(id -> pending.add(TableReference.of(catalog.name(), id)));
      health
          .assignRechecks(Math.max(1, config.commitThreads() / 2))
          .forEach(id -> pending.add(TableReference.of(catalog.name(), id)));
    } catch (RuntimeException e) {
      LOG.warn("Cannot list the tables of the statistics lag and of the health re-checks", e);
    }
  }

  /**
   * {@code commit()} of a request without responses for a table with no drain active, unfinished or
   * stopped. Always {@code COMMITTED} with nothing consumed.
   *
   * @param table the table if already loaded, or {@code null} to load it here
   */
  CommitResult commitWithoutResponses(
      TableCommitRequest request, Table table, long lagToken, long healthVersion) {
    lagChecker.visit(request, table, lagToken, healthVersion, this);
    return CommitResult.committed(ImmutableList.of());
  }

  /** Copy-on-write keeps nothing to drop for a table gone from the catalog. */
  @Override
  public void tableGone(TableIdentifier id) {}

  @Override
  public boolean mayCheck(TableIdentifier id, Table table) {
    return true;
  }

  /**
   * The step runs on the transition pool under the lock of the state of the table's UUID, created
   * if missing, and holds a slot of the limiter until it ends.
   */
  @Override
  public void setLagStep(TableCommitRequest request, Table table) {
    TableReference requested = request.tableReference();
    TableIdentifier tableIdentifier = requested.identifier();
    TableDrainState state =
        states.getOrCreate(TableReference.of(requested.catalog(), tableIdentifier, table.uuid()));
    UUID commitId = request.commitId();
    lagStepLimiter.enter(tableIdentifier);
    try {
      exec.execute(() -> runLagStep(state, commitId));
    } catch (RuntimeException e) {
      // the pool is shut down: nothing runs any more
      lagStepLimiter.leave(tableIdentifier);
      LOG.debug("Cannot set the lag step of table {}", tableIdentifier, e);
    }
  }

  /** A step not taken leaves the lag and the health as they are. */
  private void runLagStep(TableDrainState state, UUID commitId) {
    TableIdentifier tableIdentifier = state.tableReference.identifier();
    boolean permitted = false;
    boolean locked = false;
    try {
      // the permit before anything else, as waiting under the lock would keep the table PENDING
      permitted = lagStepLimiter.acquire(stopped);
      if (!permitted) {
        LOG.debug(
            "Lag step of table {} is dropped: stopped while waiting for a permit", tableIdentifier);
        return;
      }

      state.lock.lock();
      locked = true;
      // the lock is not interruptible: a stop that came while the step waited for it ends here,
      // before any read of the catalog or the storage
      if (stoppedOrInterrupted()) {
        LOG.debug("Lag step of table {} is not taken: stopped", tableIdentifier);
        return;
      }

      // a drain of the table has work of its own, and its answer must stay as it is; a state no
      // longer registered has a lock of its own, which keeps no drain of the registered one from
      // running beside the step
      if (!states.registeredAndIdle(state)) {
        LOG.debug(
            "Lag step of table {} is not taken: its state is not the registered one, a drain of "
                + "the table is active or unfinished, or the table is stopped",
            tableIdentifier);
        return;
      }

      state.lagStepHoldsLock = true;
      stepOfLag(state, commitId);
    } finally {
      if (locked) {
        state.lagStepHoldsLock = false;
        state.lock.unlock();
      }

      if (permitted) {
        lagStepLimiter.release();
      }

      lagStepLimiter.leave(tableIdentifier);
    }
  }

  /** Runs with the lock held and {@code lagStepHoldsLock} raised. */
  private void stepOfLag(TableDrainState state, UUID commitId) {
    TableIdentifier tableIdentifier = state.tableReference.identifier();
    // a step of the tail, running or waiting, goes first
    if (tailStepSet.test(state.tableReference)) {
      LOG.debug(
          "Lag step of table {} is not taken: a statistics step of its merge-on-read tail is set",
          tableIdentifier);
      return;
    }

    // an entry into the lag after it may be of a snapshot newer than the head of the step
    long lagToken = statisticsLag.token(tableIdentifier);
    Result result = lagStepOnHead(state, commitId);
    if (result != null) {
      actOnLagStep(tableIdentifier, lagToken, result);
    }
  }

  /**
   * Reads the table again, as cycles may have passed since {@code commit()} loaded it, and runs the
   * step on its head. A failure here is the step's outcome.
   *
   * @return the outcome of the step, or {@code null} if it is not taken
   */
  private Result lagStepOnHead(TableDrainState state, UUID commitId) {
    TableIdentifier tableIdentifier = state.tableReference.identifier();
    try {
      // loaded by name rather than refreshed, which a catalog checking that the name still
      // means the same table refuses for one created again, before the check below could tell it
      Table table = catalog.loadTable(tableIdentifier);
      if (!Objects.equals(table.uuid(), state.tableReference.uuid())) {
        // a table created again: its state goes, and a later cycle sets the step on the new table
        LOG.debug(
            "Lag step of table {} is not taken: the table was created again (UUID {} -> {})",
            tableIdentifier,
            state.tableReference.uuid(),
            table.uuid());
        states.remove(state);
        return null;
      }

      // as the commit of a drain does
      states.stopIfSecondNameOfSameTable(state);
      if (state.stopped()) {
        return null;
      }

      // as the start of a drain does, so that the staging cleanup sees the table as after a drain
      state.bindTable(table, config);
      return finalStatistics.lagStepOrRecount(state, table, commitId);
    } catch (NoSuchTableException e) {
      // a stop first, so that the health and the lag stay
      if (stoppedOrInterrupted()) {
        return new Result(StatisticsStep.Outcome.STOPPED, null);
      }

      LOG.debug("Lag step of table {}: the table is gone", tableIdentifier, e);
      health.remove(tableIdentifier);
      return new Result(StatisticsStep.Outcome.TABLE_GONE, null);
    } catch (RuntimeException e) {
      return stoppedOrInterrupted()
          ? new Result(StatisticsStep.Outcome.STOPPED, null)
          : new Result(StatisticsStep.Outcome.FAILED, e);
    } catch (Error e) {
      finalStatistics.enterLagBeforeRethrow(tableIdentifier, e);
      throw e;
    }
  }

  private void actOnLagStep(TableIdentifier tableIdentifier, long lagToken, Result result) {
    boolean firstFailure = statisticsLag.applyOutcome(tableIdentifier, lagToken, result.outcome());
    if (result.outcome() != StatisticsStep.Outcome.FAILED) {
      return;
    }

    Throwable failure = result.failure();
    if (firstFailure) {
      LOG.warn(
          "Statistics lag step of table {} failed, the table is in the statistics lag and its "
              + "step is retried in a later commit cycle",
          tableIdentifier,
          failure);
    } else {
      LOG.debug(
          "Statistics lag step of table {} failed again, the table stays in the statistics lag",
          tableIdentifier,
          failure);
    }
  }

  private boolean stoppedOrInterrupted() {
    return stopped.getAsBoolean() || Thread.currentThread().isInterrupted();
  }
}
