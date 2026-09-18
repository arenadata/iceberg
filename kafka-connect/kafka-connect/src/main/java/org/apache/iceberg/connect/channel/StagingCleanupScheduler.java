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
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagingOrphanCleaner;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides when the staging orphan cleanup runs, and hands it what is still reachable.
 *
 * <p>Deleting is {@link StagingOrphanCleaner}'s job; establishing reachability is this one's, and
 * it is the harder half. A staged file is reachable from three places, and the cleanup has to see
 * all three or it deletes live data: the pointer stored in a snapshot summary, the change set a
 * drain is holding in memory (frozen, but not yet named by any snapshot), and the responses still
 * sitting in the coordinator's commit buffer, which belong to no change set at all yet.
 *
 * <p>Runs from the coordinator's tick but does its work on the pool copy-on-write's slice
 * transitions use, not on the commit pool that joins {@code doCommit}: listing a prefix and
 * deleting files is object IO, and a slow store must not stall the coordinator thread or a commit.
 * One pass runs at a time regardless of how many threads the pool has: {@link #maybeClean} submits
 * the whole pass as a single task, so a slow cleanup occupies at most one of those threads instead
 * of one per due table.
 */
class StagingCleanupScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(StagingCleanupScheduler.class);

  private final IcebergSinkConfig config;
  private final ChangeSetStore store;
  private final Executor exec;
  private final Collection<TableDrainState> states;
  private final BooleanSupplier controlTopicCaughtUp;
  private final BooleanSupplier stopped;
  private final Function<TableIdentifier, Table> loadTable;

  private final Map<TableReference, Long> lastCleanupMs = Maps.newHashMap();

  /** Tables a WARN already told about a {@code FileIO} that cannot list a prefix. */
  private final Set<TableReference> noPrefixSupportWarned = Sets.newHashSet();

  /** Tables a WARN already told about a failed read of the snapshot pointer or its manifest. */
  private final Set<TableReference> readFailureWarned = Sets.newHashSet();

  /** Whether a pass submitted by {@link #maybeClean} is still running. */
  private final AtomicBoolean sweeping = new AtomicBoolean(false);

  StagingCleanupScheduler(
      IcebergSinkConfig config,
      ChangeSetStore store,
      Executor exec,
      Collection<TableDrainState> states,
      BooleanSupplier controlTopicCaughtUp,
      BooleanSupplier stopped,
      Function<TableIdentifier, Table> loadTable) {
    this.config = config;
    this.store = store;
    this.exec = exec;
    this.states = states;
    this.controlTopicCaughtUp = controlTopicCaughtUp;
    this.stopped = stopped;
    this.loadTable = loadTable;
  }

  /**
   * Cleans each loaded table's staging prefix of files no change set can reach, once per table per
   * {@code iceberg.tables.copy-on-write.staging-sweep-interval-ms}.
   *
   * <p>Only files older than {@code iceberg.tables.copy-on-write.staging-orphan-ttl-ms} and named
   * by no live change set are removed, so a change set being frozen right now (manifest not yet
   * written) is never hit.
   *
   * @param bufferedResponses the coordinator's commit buffer, read only when a cleanup is actually
   *     due. It is a projection of a buffer that holds hundreds of thousands of envelopes during a
   *     slow drain, and the interval between two cleanups is an hour by default, so computing it on
   *     every tick would build a large set a few thousand times for each time it is used
   */
  void maybeClean(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {
    if (!sweeping.compareAndSet(false, true)) {
      // the previous pass has not finished on the shared pool yet: queuing another task would only
      // grow the backlog in front of whatever transitions are queued behind it
      LOG.debug(
          "Copy-on-write staging cleanup is still running a previous pass, skipping this tick");
      return;
    }

    boolean submitted = false;
    try {
      List<TableDrainState> due = dueTables(nowMs);
      if (due.isEmpty()) {
        return;
      }

      Set<String> buffered = stagedFileLocations(bufferedResponses.get());
      List<TableSweep> sweeps = prepareSweeps(due, buffered, nowMs);
      if (sweeps.isEmpty()) {
        return;
      }

      submitted = true;
      exec.execute(
          () -> {
            try {
              for (TableSweep sweep : sweeps) {
                if (stopped.getAsBoolean()) {
                  // the coordinator is stopping: no delete starts from here on (mirrors
                  // CopyOnWriteTableCommitter#stop), and the remaining tables wait for the next one
                  return;
                }
                cleanTable(
                    sweep.table,
                    sweep.tableUuid,
                    sweep.branch,
                    sweep.tableReference,
                    sweep.stagingLocation,
                    sweep.live,
                    nowMs);
              }
            } finally {
              sweeping.set(false);
            }
          });
    } finally {
      if (!submitted) {
        sweeping.set(false);
      }
    }
  }

  /**
   * Reads what each due table's state has under its own lock, marking it as swept as of {@code
   * nowMs}. A table mid-transition or without a table and staging location yet is left out; the
   * next tick tries it again.
   */
  private List<TableSweep> prepareSweeps(
      List<TableDrainState> due, Set<String> buffered, long nowMs) {
    List<TableSweep> sweeps = Lists.newArrayList();
    for (TableDrainState state : due) {
      if (!state.lock.tryLock()) {
        continue;
      }
      try {
        if (state.table == null || state.stagingLocation == null) {
          continue;
        }
        lastCleanupMs.put(state.tableReference, nowMs);
        Set<String> live = Sets.newHashSet(buffered);
        // in-memory reachability covers the window in which a change set is frozen but its
        // pointer is not yet in any snapshot: the snapshot pointer read later cannot see that one
        if (state.manifest != null) {
          state.manifest.stagedFiles().forEach(file -> live.add(file.location()));
          live.add(state.manifestLocation);
          if (state.normalizedLocation != null) {
            live.add(state.normalizedLocation);
          }
        }
        sweeps.add(
            new TableSweep(
                state.table,
                state.table.uuid(),
                state.branch,
                state.tableReference,
                state.stagingLocation,
                live));
      } finally {
        state.lock.unlock();
      }
    }
    return sweeps;
  }

  /** One table's share of a pass: what {@link #maybeClean} read from its state under its lock. */
  private static final class TableSweep {
    private final Table table;
    private final UUID tableUuid;
    private final String branch;
    private final TableReference tableReference;
    private final String stagingLocation;
    private final Set<String> live;

    TableSweep(
        Table table,
        UUID tableUuid,
        String branch,
        TableReference tableReference,
        String stagingLocation,
        Set<String> live) {
      this.table = table;
      this.tableUuid = tableUuid;
      this.branch = branch;
      this.tableReference = tableReference;
      this.stagingLocation = stagingLocation;
      this.live = live;
    }
  }

  /**
   * The tables whose sweep is due now: loaded by a transition of this coordinator, and either never
   * swept by it or swept a whole sweep interval ago.
   *
   * <p>Decided before the buffer is projected and without reading anything a drain writes, so that
   * a table that is not sweepable (never loaded, or mid-transition) costs a {@code tryLock} and not
   * a projection of the whole commit buffer on every tick. A table whose lock is held is simply
   * left for the next tick, which is well within the commit cycle the contract allows it.
   */
  private List<TableDrainState> dueTables(long nowMs) {
    if (!controlTopicCaughtUp.getAsBoolean()) {
      // the buffer is one of the three sources of reachability, and until the control topic has
      // been read to where it ended when the partitions were assigned it holds only part of what
      // this connector wrote: the staged files of the responses still on their way are named by
      // nothing, and a sweep past their TTL would delete them
      return ImmutableList.of();
    }

    List<TableDrainState> due = Lists.newArrayList();
    Set<TableReference> known = Sets.newHashSet();
    for (TableDrainState state : states) {
      known.add(state.tableReference);
      Long last = lastCleanupMs.get(state.tableReference);
      if (last != null && nowMs - last < config.copyOnWriteStagingSweepIntervalMs()) {
        continue;
      }
      if (!state.lock.tryLock()) {
        continue;
      }
      try {
        // a table is swept from its first load on: before that there is no FileIO to list with,
        // no staging location to list under, and nothing has been written to it by this drain
        if (state.table != null && state.stagingLocation != null) {
          due.add(state);
        }
      } finally {
        state.lock.unlock();
      }
    }

    // a table dropped from the set of swept tables takes its timer with it, so that one created
    // again under the same name is swept as soon as it is loaded rather than an interval later.
    // Safe without a lock: only the single task a previous maybeClean submitted writes
    // noPrefixSupportWarned and readFailureWarned, and sweeping being false here happens-after
    // that task's own write of it
    lastCleanupMs.keySet().retainAll(known);
    noPrefixSupportWarned.retainAll(known);
    readFailureWarned.retainAll(known);
    return due;
  }

  @VisibleForTesting
  static Set<String> stagedFileLocations(Collection<Envelope> buffered) {
    Set<String> locations = Sets.newHashSet();
    for (Envelope envelope : buffered) {
      Payload payload = envelope.event().payload();
      if (payload.type() == PayloadType.ROW_CHANGES_WRITTEN) {
        ((RowChangesWritten) payload).stagedFiles().forEach(file -> locations.add(file.location()));
      }
    }
    return locations;
  }

  private void cleanTable(
      Table loaded,
      UUID loadedUuid,
      String branch,
      TableReference tableReference,
      String stagingLocation,
      Set<String> live,
      long nowMs) {
    // only this connector's group: other connectors of the table reach their files through
    // pointers and buffers this one cannot see
    String groupId = config.connectGroupId();
    String stagingPrefix =
        StagedChangeFileWriter.stagingDirectory(stagingLocation, tableReference, groupId);
    String activeChangeSetId = null;
    try {
      Table table = loadTable.apply(tableReference.identifier());
      if (!Objects.equals(table.uuid(), loadedUuid)) {
        LOG.debug(
            "Skipping copy-on-write staging cleanup of {} under {}: the table was replaced "
                + "({} -> {})",
            loaded.name(),
            stagingPrefix,
            loadedUuid,
            table.uuid());
        return;
      }

      if (!(table.io() instanceof SupportsPrefixOperations)) {
        if (noPrefixSupportWarned.add(tableReference)) {
          LOG.warn(
              "FileIO {} does not support prefix listing, skipping copy-on-write orphan cleanup "
                  + "of {}; configure a storage lifecycle rule on that location instead",
              table.io().getClass().getName(),
              stagingPrefix);
        }
        return;
      }

      Set<String> reachable = Sets.newHashSet(live);
      activeChangeSetId =
          store.latestConnectorSummary(table, branch).get(ChangeSetStore.CHANGE_SET_ID_PROP);
      if (activeChangeSetId != null) {
        String manifestLocation =
            ChangeSetManifest.location(
                stagingLocation, tableReference, groupId, UUID.fromString(activeChangeSetId));
        reachable.add(manifestLocation);
        ChangeSetManifest.read(table.io(), manifestLocation)
            .stagedFiles()
            .forEach(file -> reachable.add(file.location()));
      }

      StagingOrphanCleaner.cleanOrphans(
          table.io(),
          stagingPrefix,
          reachable,
          config.copyOnWriteStagingOrphanTtlMs(),
          nowMs,
          stopped);
      // the snapshot pointer and the manifest read without trouble this time: whatever failed
      // before is fixed, so the next failure is worth a WARN again
      readFailureWarned.remove(tableReference);
    } catch (NoSuchTableException e) {
      // dropped and not created again: nothing to keep reachable, and nothing to sweep with either,
      // since whatever is created under the name next is not the table this state loaded
      LOG.debug(
          "Skipping copy-on-write staging cleanup of {} under {}: the table no longer exists",
          loaded.name(),
          stagingPrefix,
          e);
    } catch (RuntimeException e) {
      // never delete on a partial view of what is reachable. A pointer to a change set whose
      // manifest is missing fails the same way every pass until someone fixes it, so only the
      // first pass of a run of failures is worth a WARN; the rest are DEBUG
      String changeSetId = activeChangeSetId != null ? activeChangeSetId : "unknown";
      if (readFailureWarned.add(tableReference)) {
        LOG.warn(
            "Copy-on-write staging cleanup failed for {} (change set {}); will keep retrying at "
                + "DEBUG until a pass succeeds",
            stagingPrefix,
            changeSetId,
            e);
      } else {
        LOG.debug(
            "Copy-on-write staging cleanup still failing for {} (change set {})",
            stagingPrefix,
            changeSetId,
            e);
      }
    }
  }

  /** Whether the next failed read of {@code tableReference}'s snapshot pointer stays quiet. */
  @VisibleForTesting
  boolean hasWarnedOfCleanupFailure(TableReference tableReference) {
    return readFailureWarned.contains(tableReference);
  }
}
