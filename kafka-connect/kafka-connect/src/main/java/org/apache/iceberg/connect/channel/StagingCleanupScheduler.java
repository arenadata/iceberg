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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagingOrphanCleaner;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides when the staging orphan cleanup runs, and hands it what is still reachable.
 *
 * <p>Deleting is {@link StagingOrphanCleaner}'s job; establishing reachability is this one's, and
 * it is the harder half. A file under the staging prefix is reachable from three places, and the
 * cleanup has to see all three or it deletes what is still needed: the responses sitting in the
 * coordinator's commit buffer, which belong to nothing else yet; what the table committer holds in
 * memory; and what the table's persisted state names. The first is read here, the other two are the
 * {@link StagingCleanupSource}'s.
 *
 * <p>{@link #maybeClean} is called on the coordinator thread outside {@code doCommit}, so every
 * file a response names is in the buffer or already with the committer. The work runs on the pool
 * of table transitions, not on the commit pool that joins {@code doCommit}: listing a prefix and
 * deleting files is object IO, and a slow store must not stall the coordinator thread or a commit.
 * One pass runs at a time regardless of how many threads the pool has: {@link #maybeClean} submits
 * the whole pass as a single task, so a slow cleanup occupies at most one of those threads instead
 * of one per due table.
 */
class StagingCleanupScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(StagingCleanupScheduler.class);

  private final IcebergSinkConfig config;
  private final Executor exec;
  private final StagingCleanupSource source;
  private final BooleanSupplier controlTopicCaughtUp;
  private final BooleanSupplier stopped;

  private final Map<TableReference, Long> lastCleanupMs = Maps.newHashMap();

  /** Tables a WARN already told about a {@code FileIO} that cannot list a prefix. */
  private final Set<TableReference> noPrefixSupportWarned = Sets.newHashSet();

  /** Tables a WARN already told about a failed read of their persisted state. */
  private final Set<TableReference> readFailureWarned = Sets.newHashSet();

  /** Whether a pass submitted by {@link #maybeClean} is still running. */
  private final AtomicBoolean cleaningUp = new AtomicBoolean(false);

  StagingCleanupScheduler(
      IcebergSinkConfig config,
      Executor transitionPool,
      StagingCleanupSource source,
      BooleanSupplier controlTopicCaughtUp,
      BooleanSupplier stopped) {
    this.config = config;
    this.exec = transitionPool;
    this.source = source;
    this.controlTopicCaughtUp = controlTopicCaughtUp;
    this.stopped = stopped;
  }

  /**
   * Cleans each ready table's staging prefix of files nothing reaches, once per table per {@code
   * iceberg.tables.staging-orphan-cleanup-interval-ms}.
   *
   * <p>Only files older than {@code iceberg.tables.staging-orphan-ttl-ms} and reached by nothing
   * are removed, so a file written for a response still on its way is never hit.
   *
   * @param bufferedResponses the coordinator's commit buffer, read only when a cleanup is actually
   *     due and the source could read at least one due table. It is a projection of a buffer that
   *     holds hundreds of thousands of envelopes during a slow drain, and the interval between two
   *     cleanups is an hour by default, so computing it on every tick would build a large set a few
   *     thousand times for each time it is used
   */
  void maybeClean(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {
    if (!cleaningUp.compareAndSet(false, true)) {
      // the previous pass has not finished on the shared pool yet: queuing another task would only
      // grow the lag in front of whatever transitions are queued behind it
      LOG.debug("Staging cleanup is still running a previous pass, skipping this tick");
      return;
    }

    boolean submitted = false;
    try {
      List<TableReference> due = dueTables(nowMs);
      if (due.isEmpty()) {
        return;
      }

      // the source first: a table it cannot read now, such as one whose drain holds the lock, would
      // otherwise cost a projection of the whole buffer on every tick until the lock is free
      List<CleanupTarget> targets = snapshots(due, nowMs);
      if (targets.isEmpty()) {
        return;
      }

      ReachableFileSet buffered = bufferedReachable(bufferedResponses.get());
      submitted = true;
      exec.execute(
          () -> {
            try {
              for (CleanupTarget target : targets) {
                if (stopped.getAsBoolean()) {
                  // the coordinator is stopping: no delete starts from here on, and the remaining
                  // tables wait for the next coordinator's pass
                  return;
                }
                cleanTable(target, buffered, nowMs);
              }
            } finally {
              cleaningUp.set(false);
            }
          });
    } finally {
      if (!submitted) {
        cleaningUp.set(false);
      }
    }
  }

  /**
   * The source's entries of the due tables, each marked as cleaned up as of {@code nowMs}. A table
   * the source cannot read now is left out without spending its interval; the next tick tries it
   * again.
   */
  private List<CleanupTarget> snapshots(List<TableReference> due, long nowMs) {
    List<CleanupTarget> targets = Lists.newArrayList();
    for (TableReference tableReference : due) {
      CleanupTarget target = source.snapshot(tableReference);
      if (target != null) {
        lastCleanupMs.put(tableReference, nowMs);
        targets.add(target);
      }
    }
    return targets;
  }

  /**
   * The tables whose cleanup is due now: ready by the source, and either never cleaned up by this
   * coordinator or cleaned up a whole cleanup interval ago.
   *
   * <p>Decided before the buffer is projected, so that a table that is not ready costs nothing but
   * the source's answer, not a projection of the whole commit buffer on every tick.
   */
  private List<TableReference> dueTables(long nowMs) {
    if (!controlTopicCaughtUp.getAsBoolean()) {
      // the buffer is one of the three sources of reachability, and until the control topic has
      // been read to where it ended when the partitions were assigned it holds only part of what
      // this connector wrote: the files of the responses still on their way are named by
      // nothing, and a cleanup past their TTL would delete them
      return Lists.newArrayList();
    }

    Collection<TableReference> ready = source.tables();
    List<TableReference> due = Lists.newArrayList();
    for (TableReference tableReference : ready) {
      Long last = lastCleanupMs.get(tableReference);
      if (last == null || nowMs - last >= config.stagingOrphanCleanupIntervalMs()) {
        due.add(tableReference);
      }
    }

    // a table the source no longer offers takes its timer with it, so that one created again under
    // the same name is cleaned up as soon as it is ready rather than an interval later. Safe
    // without a
    // lock: only the single task a previous maybeClean submitted writes noPrefixSupportWarned and
    // readFailureWarned, and cleaningUp being false here happens-after that task's own write of it
    Set<TableReference> known = Sets.newHashSet(ready);
    lastCleanupMs.keySet().retainAll(known);
    noPrefixSupportWarned.retainAll(known);
    readFailureWarned.retainAll(known);
    return due;
  }

  /**
   * What the buffered responses reach, for every table at once; each table's pass looks only under
   * its own prefix.
   */
  static ReachableFileSet bufferedReachable(Collection<Envelope> envelopes) {
    ReachableFileSet reachable = new ReachableFileSet();
    for (Envelope envelope : envelopes) {
      Payload payload = envelope.event().payload();
      if (payload instanceof RowChangesWritten) {
        RowChangesWritten rowChanges = (RowChangesWritten) payload;
        rowChanges.stagedFiles().forEach(file -> reachable.addAnchor(file.location()));
        addSketchFile(reachable, rowChanges.sketchFile());
      } else if (payload instanceof DataWritten) {
        addSketchFile(reachable, ((DataWritten) payload).sketchFile());
      }
    }
    return reachable;
  }

  private static void addSketchFile(ReachableFileSet reachable, SketchFile sketchFile) {
    if (sketchFile != null) {
      reachable.addLive(sketchFile.location());
    }
  }

  private void cleanTable(CleanupTarget target, ReachableFileSet buffered, long nowMs) {
    TableReference tableReference = target.tableReference();
    String cleanupPrefix = target.cleanupPrefix();
    try {
      // only this connector's group: other connectors of the table reach their files through
      // pointers and buffers this one cannot see
      String stagingDirectory =
          StagedChangeFileWriter.stagingDirectory(
              target.stagingLocation(), tableReference, config.connectGroupId());
      Preconditions.checkState(
          cleanupPrefix.equals(stagingDirectory)
              || cleanupPrefix.startsWith(stagingDirectory + "/"),
          "Cleanup prefix %s is outside the staging directory %s of this connector",
          cleanupPrefix,
          stagingDirectory);

      Table table = source.load(tableReference.identifier());
      if (!Objects.equals(table.uuid(), target.tableUuid())) {
        LOG.debug(
            "Skipping staging cleanup of {} under {}: the table was replaced ({} -> {})",
            target.tableName(),
            cleanupPrefix,
            target.tableUuid(),
            table.uuid());
        return;
      }

      if (!(table.io() instanceof SupportsPrefixOperations)) {
        if (noPrefixSupportWarned.add(tableReference)) {
          LOG.warn(
              "FileIO {} does not support prefix listing, skipping staging orphan cleanup "
                  + "of {}; configure a storage lifecycle rule on that location instead",
              table.io().getClass().getName(),
              cleanupPrefix);
        }
        return;
      }

      ReachableFileSet ofTable = new ReachableFileSet();
      ofTable.addAll(target.inMemory());
      source.addPersistedReachable(table, target, ofTable);
      StagingOrphanCleaner.cleanOrphans(
          table.io(),
          cleanupPrefix,
          Sets.union(buffered.live(), ofTable.live()),
          Sets.union(buffered.anchors(), ofTable.anchors()),
          config.stagingOrphanTtlMs(),
          nowMs,
          stopped);
      // the persisted state read without trouble this time: whatever failed before is fixed, so
      // the next failure is worth a WARN again
      readFailureWarned.remove(tableReference);
    } catch (NoSuchTableException e) {
      // dropped and not created again: nothing to keep reachable, and nothing to clean up with
      // either, since whatever is created under the name next is not the table the source loaded
      LOG.debug(
          "Skipping staging cleanup of {} under {}: the table no longer exists",
          target.tableName(),
          cleanupPrefix,
          e);
    } catch (RuntimeException e) {
      if (stopped.getAsBoolean() || Thread.currentThread().isInterrupted()) {
        // the coordinator is stopping and the catalog or the storage answered the interrupt: the
        // pass is given up on, not failed, and the next coordinator's pass starts afresh
        LOG.debug("Staging cleanup of {} stopped by the shutdown", cleanupPrefix, e);
        return;
      }
      // never delete on a partial view of what is reachable. A pointer to a change set whose
      // manifest is missing fails the same way every pass until someone fixes it, so only the
      // first pass of a run of failures is worth a WARN; the rest are DEBUG
      if (readFailureWarned.add(tableReference)) {
        LOG.warn(
            "Staging cleanup of {} failed, nothing is deleted; will keep retrying at DEBUG until "
                + "a pass succeeds",
            cleanupPrefix,
            e);
      } else {
        LOG.debug("Staging cleanup of {} still failing", cleanupPrefix, e);
      }
    }
  }

  /** Whether the next failed read of {@code tableReference}'s persisted state stays quiet. */
  @VisibleForTesting
  boolean hasWarnedOfCleanupFailure(TableReference tableReference) {
    return readFailureWarned.contains(tableReference);
  }
}
