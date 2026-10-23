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
package org.apache.iceberg.connect.data;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.NdvResult.Kind;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The statistics step of snapshot S: writes what S lacks of the NDV and the partition statistics,
 * commits both in one transaction together with the removal of the records beyond the last N,
 * deletes what it created and did not commit and the intermediate Puffin files of the answers, and
 * applies the health transition.
 *
 * <p>The step changes only files and the {@link StatisticsHealth}. Not final so that tests can
 * substitute it.
 */
public class StatisticsStep {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsStep.class);

  public enum Outcome {
    /** The transaction is committed with at least one kind, and no kind failed. */
    WRITTEN,
    /**
     * Nothing to write: the records are there, no head, no kind enabled or writable, nothing to
     * count.
     */
    NOT_NEEDED,
    /**
     * The NDV is not written for a final reason (the gap over the limit, no baseline, no blobs), no
     * partition statistics are written, and no kind failed.
     */
    FINAL,
    /** A lag step found an intermediate copy-on-write slice at the head. */
    DEFERRED,
    /**
     * A kind failed, even if the other one is committed; another exception; or the outcome of the
     * commit is unknown and not confirmed.
     */
    FAILED,
    /** Snapshot S is no longer in the table. */
    SNAPSHOT_GONE,
    /** The table is no longer in the catalog. */
    TABLE_GONE,
    /** The step was stopped; the health is not changed. */
    STOPPED
  }

  private final Logger log;
  private final PartitionStatisticsWriter partitionStatistics;

  public StatisticsStep() {
    this(LOG);
  }

  StatisticsStep(Logger log) {
    this.log = log;
    this.partitionStatistics = new PartitionStatisticsWriter(log);
  }

  /**
   * Runs the step. Never throws a {@link RuntimeException} or a {@link LinkageError}: such an
   * exception is {@link Outcome#FAILED} with {@link Result#failure()}, or {@link Outcome#STOPPED}
   * once the step is stopped. Other {@link Error}s propagate.
   */
  public Result run(StatisticsStepInput input) {
    return new Execution(input).run();
  }

  public static final class Result {
    private final Outcome outcome;
    private final Throwable failure;

    public Result(Outcome outcome, Throwable failure) {
      Preconditions.checkArgument(outcome != null, "Invalid outcome: null");
      Preconditions.checkArgument(
          (outcome == Outcome.FAILED) == (failure != null),
          "Invalid failure for outcome %s: %s",
          outcome,
          failure);
      this.outcome = outcome;
      this.failure = failure;
    }

    public Outcome outcome() {
      return outcome;
    }

    /** The first failure of a failed step, others suppressed in it; {@code null} otherwise. */
    public Throwable failure() {
      return failure;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("outcome", outcome)
          .add("failure", failure)
          .toString();
    }
  }

  private final class Execution {
    private final StatisticsStepInput input;
    private final Table table;
    private final TableIdentifier identifier;
    private final StepFileTracker files;
    private Long snapshotId = null;
    private Throwable failure = null;
    private boolean kindFailed = false;
    private NdvResult ndv = null;
    private StatisticsFile ndvFile = null;
    private boolean ndvAppeared = false;
    private PartitionStatisticsFile partitionFile = null;
    // the version of the health record when the step starts: a record removed since then (the
    // table found gone) does not take the NDV transition
    private final long healthVersion;
    private boolean ndvCommitted = false;
    private boolean partitionsCommitted = false;

    private Execution(StatisticsStepInput input) {
      this.input = input;
      this.table = input.table();
      this.identifier = input.identifier();
      this.healthVersion = input.health().version(identifier);
      this.files = new StepFileTracker(table, identifier, this::stopped, log);
    }

    private Result run() {
      Outcome outcome;
      try {
        outcome = sequence();
      } catch (RuntimeException | LinkageError e) {
        // a LinkageError (say, datasketches missing from an incomplete plugin) is an ordinary
        // failure of the step; other Errors, an OutOfMemoryError included, are left to the caller
        outcome = stopped() ? Outcome.STOPPED : Outcome.FAILED;
        if (outcome == Outcome.FAILED) {
          fail(e);
        } else {
          log.debug("Statistics step of table {} stopped with an exception", identifier, e);
        }
      }

      if (outcome != Outcome.STOPPED) {
        files.deleteCreated();

        if (outcome != Outcome.FAILED || !input.keepSketchesOnFailure()) {
          deleteIntermediateFiles();
        }
      }

      logOutcome(outcome);
      if (outcome != Outcome.FAILED) {
        return new Result(outcome, null);
      }

      // every failed path records its exception; the fallback keeps run() from throwing
      return new Result(
          outcome, failure != null ? failure : new IllegalStateException("Statistics step failed"));
    }

    private Outcome sequence() {
      if (stopped()) {
        return Outcome.STOPPED;
      }

      Outcome early = resolveSnapshot();
      if (early != null) {
        return early;
      }

      // skip each kind whose record of S is already there, whoever wrote it
      boolean ndvNeeded = !StatisticsRecords.hasNdv(table, snapshotId);
      boolean partitionsNeeded = partitionStatisticsNeeded();
      if (!ndvNeeded && !partitionsNeeded) {
        return Outcome.NOT_NEEDED;
      }

      if (ndvNeeded && !computeNdv()) {
        return Outcome.STOPPED;
      }

      if (partitionsNeeded) {
        if (stopped()) {
          return Outcome.STOPPED;
        }

        computePartitionStatistics();
      }

      if (stopped()) {
        return Outcome.STOPPED;
      }

      writeNdvFile();
      return recheckAndCommit();
    }

    /** Refreshes the table and resolves S; an outcome to leave with, or {@code null} to go on. */
    private Outcome resolveSnapshot() {
      try {
        table.refresh();
      } catch (NoSuchTableException e) {
        if (stopped()) {
          // a stop first, so that the health stays
          log.debug("Refresh of table {} stopped", identifier, e);
          return Outcome.STOPPED;
        }

        log.debug("Table {} is gone", identifier, e);
        input.health().remove(identifier);
        return Outcome.TABLE_GONE;
      }

      if (input.snapshotId() != null) {
        this.snapshotId = input.snapshotId();
        return table.snapshot(snapshotId) == null ? Outcome.SNAPSHOT_GONE : null;
      }

      Snapshot head = SnapshotUtil.latestSnapshot(table, input.tableConfig().commitBranch());
      if (head == null) {
        return Outcome.NOT_NEEDED;
      }

      this.snapshotId = head.snapshotId();
      Map<String, String> summary = head.summary();
      if (summary != null && summary.containsKey(NdvChain.CHANGE_SET_ID_PROP)) {
        return Outcome.DEFERRED;
      }

      return null;
    }

    /** Calculates the NDV of S; {@code false} if the calculation was stopped. */
    private boolean computeNdv() {
      this.ndv =
          NdvComputation.compute(
              identifier,
              table,
              snapshotId,
              input.ownSnapshotIds(),
              input.ownChangeSetIds(),
              input.answers(),
              input.config(),
              input.tableConfig(),
              this::stopped);
      if (ndv.kind() == Kind.STOPPED) {
        return false;
      }

      if (ndv.kind() == Kind.ERROR) {
        kindFailed(
            ndv.failure() != null
                ? ndv.failure()
                : new IllegalStateException("Cannot calculate the NDV of snapshot " + snapshotId));
      }

      return true;
    }

    private boolean partitionStatisticsNeeded() {
      return partitionStatistics.needed(identifier, table, snapshotId, input.tableConfig());
    }

    private void computePartitionStatistics() {
      PartitionStatisticsWriter.Computed computed =
          partitionStatistics.compute(identifier, table, snapshotId, this::stopped);
      if (computed.failure() != null) {
        kindFailed(computed.failure());
      } else if (computed.file() != null) {
        this.partitionFile = computed.file();
        files.add(computed.file().path());
      }
    }

    private void writeNdvFile() {
      if (ndv == null || ndv.kind() != Kind.BLOBS) {
        return;
      }

      String path = NdvComputation.fileLocation(table, snapshotId, input.commitId());
      try {
        // the path is the step's only once create() succeeded: a file that create() refused or
        // could not check is never deleted, whatever the reason
        this.ndvFile =
            NdvComputation.writeFile(
                table, snapshotId, input.commitId(), ndv.blobs(), () -> files.add(path));
      } catch (RuntimeException | LinkageError e) {
        // any exception here, a LinkageError too (say, a codec missing from the plugin), fails
        // the NDV only
        if (stopped()) {
          throw e;
        }

        kindFailed(e);
      }
    }

    private Outcome recheckAndCommit() {
      boolean ndvInTransaction = ndvFile != null;
      boolean partitionsInTransaction = partitionFile != null;
      if (ndvInTransaction || partitionsInTransaction) {
        table.refresh();
        if (table.snapshot(snapshotId) == null) {
          // expired after it was resolved: a record of it would be left for no snapshot
          return Outcome.SNAPSHOT_GONE;
        }

        if (ndvInTransaction && StatisticsRecords.hasNdv(table, snapshotId)) {
          log.debug(
              "NDV statistics of table {} at snapshot {} appeared during the step, not replaced",
              identifier,
              snapshotId);
          ndvInTransaction = false;
          this.ndvAppeared = true;
        }

        if (partitionsInTransaction
            && StatisticsRecords.hasPartitionStatistics(table, snapshotId)) {
          log.debug(
              "Partition statistics of table {} at snapshot {} appeared during the step, "
                  + "not replaced",
              identifier,
              snapshotId);
          partitionsInTransaction = false;
        }
      }

      if (ndvInTransaction || partitionsInTransaction) {
        Outcome early = commit(ndvInTransaction, partitionsInTransaction);
        if (early != null) {
          return early;
        }
      }

      applyHealth();
      return outcome();
    }

    /** Commits the kinds in the transaction; an outcome to leave with, or {@code null} to go on. */
    private Outcome commit(boolean withNdv, boolean withPartitions) {
      StatisticsCommit commit =
          new StatisticsCommit(table, identifier, snapshotId, files, this::stopped, log);
      StatisticsCommit.Status status =
          commit.commit(
              withNdv ? ndvFile : null,
              withPartitions ? partitionFile : null,
              input.config().statisticsRetainSnapshots());
      switch (status) {
        case STOPPED:
          return Outcome.STOPPED;
        case FAILED:
          fail(commit.failure());
          return Outcome.FAILED;
        default:
          this.ndvCommitted = withNdv;
          this.partitionsCommitted = withPartitions;
          return null;
      }
    }

    /**
     * Applies the NDV transition of the health flags. A record changed during the step takes it
     * (the evaluation that ended last wins); a record removed during the step is not restored.
     */
    private void applyHealth() {
      if (ndv == null || ndvAppeared) {
        return;
      }

      Kind kind = ndv.kind();
      if (kind == Kind.ERROR || kind == Kind.STOPPED || (kind == Kind.BLOBS && !ndvCommitted)) {
        return;
      }

      HealthTransition transition = ndv.transition();
      Flags previous = input.health().updateUnlessRemoved(identifier, healthVersion, transition);
      if (previous == null) {
        log.debug(
            "Health of table {} removed during the step, the NDV transition is not applied",
            identifier);
        return;
      }

      Flags updated = transition.apply(previous);
      StatisticsHealth.logTransitions(
          log, identifier, snapshotId, previous, updated, transition.details());
    }

    private Outcome outcome() {
      if (kindFailed) {
        return Outcome.FAILED;
      }

      if (ndvCommitted || partitionsCommitted) {
        return Outcome.WRITTEN;
      }

      if (ndv != null && isFinal(ndv.kind())) {
        return Outcome.FINAL;
      }

      return Outcome.NOT_NEEDED;
    }

    private void deleteIntermediateFiles() {
      Set<String> locations = Sets.newLinkedHashSet();
      for (StatisticsAnswer answer : input.answers()) {
        if (answer.sketchFile() != null) {
          locations.add(answer.sketchFile().location());
        }
      }

      IntermediateSketchSet.delete(table.io(), locations, identifier, this::stopped, log);
    }

    private void logOutcome(Outcome outcome) {
      if (outcome == Outcome.WRITTEN) {
        log.info(
            "Committed statistics of table {} at snapshot {}: {} NDV blobs in {}, "
                + "partition statistics {}; {} files, {} bytes read to recount",
            identifier,
            snapshotId,
            ndvCommitted ? ndvFile.blobMetadata().size() : 0,
            ndvCommitted ? ndvFile.path() : "none",
            partitionsCommitted ? partitionFile.path() : "none",
            ndv == null ? 0L : ndv.filesRead(),
            ndv == null ? 0L : ndv.bytesRead());
      } else {
        // FAILED is logged by the caller, FINAL by the health transitions
        log.debug(
            "Statistics step of table {} at snapshot {}: {}", identifier, snapshotId, outcome);
      }
    }

    /** A failure of one kind: the kind stays out of the transaction, the step goes on. */
    private void kindFailed(Throwable throwable) {
      this.kindFailed = true;
      fail(throwable);
    }

    private void fail(Throwable throwable) {
      if (failure == null) {
        this.failure = throwable;
      } else if (failure != throwable) {
        failure.addSuppressed(throwable);
      }
    }

    private boolean stopped() {
      return input.stopped().getAsBoolean() || Thread.currentThread().isInterrupted();
    }
  }

  private static boolean isFinal(Kind kind) {
    return kind == Kind.NO_BLOBS || kind == Kind.OVER_LIMIT || kind == Kind.MISSING_BASELINE;
  }
}
