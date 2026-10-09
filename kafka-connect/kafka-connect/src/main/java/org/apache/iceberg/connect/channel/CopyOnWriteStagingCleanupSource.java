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
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.StatisticsRecords;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * The staging cleanup's view of copy-on-write: the tables its transitions loaded, and in each the
 * change sets still needed. The whole staging directory of the connector is cleaned up.
 *
 * <p>A change set is needed while its drain goes on, in memory and by the pointer of its slices,
 * and, with the statistics of the table on, while its final snapshot F waits for the recount of its
 * statistics, in memory and by the pointer of F. Its manifest and staged files are anchors, the
 * Puffin files of its responses other live files. So are the Puffin files of the statistics steps
 * of the merge-on-read tail, which live under the same prefix.
 *
 * <p>Works over the committer's own {@link TableDrainState}s, read under their lock with {@code
 * tryLock} only: a transition holds it for minutes, and the coordinator thread must not wait.
 */
final class CopyOnWriteStagingCleanupSource implements StagingCleanupSource {

  private final IcebergSinkConfig config;
  private final ChangeSetStore changeSets;
  private final Map<TableReference, TableDrainState> states;
  private final Function<TableIdentifier, Table> loadTable;
  private final Function<TableReference, Set<String>> tailSketchLocations;

  // the states seen with a table and a staging location, which a state never loses: one whose lock
  // is held now is still offered and keeps the interval of its cleanup. Coordinator thread only
  private final Set<TableDrainState> ready = Sets.newHashSet();

  CopyOnWriteStagingCleanupSource(
      IcebergSinkConfig config,
      ChangeSetStore changeSets,
      Map<TableReference, TableDrainState> states,
      Function<TableIdentifier, Table> loadTable,
      Function<TableReference, Set<String>> tailSketchLocations) {
    this.config = config;
    this.changeSets = changeSets;
    this.states = states;
    this.loadTable = loadTable;
    this.tailSketchLocations = tailSketchLocations;
  }

  @Override
  public Collection<TableReference> tables() {
    ready.removeIf(state -> states.get(state.tableReference) != state);
    List<TableReference> tables = Lists.newArrayList();
    for (TableDrainState state : states.values()) {
      if (ready.contains(state) || isLoaded(state)) {
        ready.add(state);
        tables.add(state.tableReference);
      }
    }
    return tables;
  }

  /**
   * Whether the state has a table and a staging location; false while its lock is held. Before the
   * first load there is no {@code FileIO} to list with and no staging location to list under.
   */
  private static boolean isLoaded(TableDrainState state) {
    if (!state.lock.tryLock()) {
      return false;
    }
    try {
      return state.table != null && state.stagingLocation != null;
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public CleanupTarget snapshot(TableReference tableReference) {
    TableDrainState state = states.get(tableReference);
    // a table mid-transition is left for the next tick
    if (state == null || !state.lock.tryLock()) {
      return null;
    }
    try {
      if (state.table == null || state.stagingLocation == null) {
        return null;
      }

      // in-memory reachability covers the window in which a change set is frozen but its pointer
      // is not yet in any snapshot: the pointer read later cannot see that one
      ReachableFileSet inMemory = new ReachableFileSet();
      if (state.manifest != null) {
        addChangeSet(inMemory, state.manifest, state.manifestLocation);
        if (state.normalizedLocation != null) {
          inMemory.addAnchor(state.normalizedLocation);
        }
      }

      // the start of a drain found F: its recount reads X, whose pointer the drain is about to
      // take off the latest snapshot of the connector
      if (state.pendingFinal != null) {
        addChangeSet(inMemory, state.pendingFinal.manifest, state.pendingFinal.manifestLocation);
      }

      tailSketchLocations.apply(tableReference).forEach(inMemory::addLive);

      return new CleanupTarget(
          state.table.name(),
          state.table.uuid(),
          state.branch,
          tableReference,
          state.stagingLocation,
          StagedChangeFileWriter.stagingDirectory(
              state.stagingLocation, tableReference, config.connectGroupId()),
          inMemory);
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public Table load(TableIdentifier identifier) {
    return loadTable.apply(identifier);
  }

  /**
   * Adds the change set the latest snapshot of the connector points at and, with the statistics of
   * the table on, the change set X of a final snapshot F that waits for its recount. A missing
   * manifest of X is no failure: it is the usual state once the step of F has run.
   */
  @Override
  public void addPersistedReachable(Table table, CleanupTarget target, ReachableFileSet into) {
    String activeChangeSetId =
        changeSets
            .latestConnectorSummary(table, target.branch())
            .get(ChangeSetStore.CHANGE_SET_ID_PROP);
    if (activeChangeSetId != null) {
      String location = manifestLocation(target, UUID.fromString(activeChangeSetId));
      addChangeSet(into, ChangeSetManifest.read(table.io(), location), location);
    }

    if (statisticsEnabled(target.tableReference())) {
      addPendingFinalChangeSet(table, target, into);
    }
  }

  /**
   * Adds X if the latest snapshot of the connector is a final snapshot F without an NDV record.
   * Neither the table of the manifest nor its format is checked: an X the recount cannot use stays
   * referenced while F is the latest snapshot of the connector, and goes by the TTL after that. A
   * manifest that cannot be read, a file that is no manifest included, throws.
   */
  private void addPendingFinalChangeSet(Table table, CleanupTarget target, ReachableFileSet into) {
    Snapshot latest = changeSets.latestConnectorSnapshot(table, target.branch());
    String pointer =
        latest == null ? null : latest.summary().get(ChangeSetStore.FINAL_CHANGE_SET_ID_PROP);
    if (pointer == null || StatisticsRecords.hasNdv(table, latest.snapshotId())) {
      return;
    }

    UUID changeSetId;
    try {
      changeSetId = UUID.fromString(pointer);
    } catch (IllegalArgumentException e) {
      // names no change set a recount could find either: nothing waits for it
      return;
    }

    String location = manifestLocation(target, changeSetId);
    ChangeSetManifest manifest;
    try {
      manifest = ChangeSetManifest.read(table.io(), location);
    } catch (NotFoundException e) {
      return;
    }

    addChangeSet(into, manifest, location);
  }

  private String manifestLocation(CleanupTarget target, UUID changeSetId) {
    return ChangeSetManifest.location(
        target.stagingLocation(), target.tableReference(), config.connectGroupId(), changeSetId);
  }

  private boolean statisticsEnabled(TableReference tableReference) {
    return config.statisticsInUse()
        && config.tableConfig(tableReference.identifier().toString()).statisticsEnabled();
  }

  private static void addChangeSet(
      ReachableFileSet into, ChangeSetManifest manifest, String manifestLocation) {
    if (manifestLocation != null) {
      into.addAnchor(manifestLocation);
    }
    manifest.stagedFiles().forEach(file -> into.addAnchor(file.location()));
    manifest.sketchFiles().forEach(sketchFile -> into.addLive(sketchFile.location()));
  }
}
