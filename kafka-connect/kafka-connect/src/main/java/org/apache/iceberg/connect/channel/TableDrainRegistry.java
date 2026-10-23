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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The copy-on-write states of the tables, by the reference they are drained under: which state is
 * the registered one, which tables a previous coordinator may have left a drain in, which reference
 * a table drains under, and which tables are stopped for good.
 */
final class TableDrainRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TableDrainRegistry.class);

  private final Map<TableReference, TableDrainState> states = Maps.newConcurrentMap();
  private final Map<UUID, TableReference> drainingUnder = Maps.newConcurrentMap();
  // the configured tables whose change set pointer this coordinator has not read yet
  private final Set<TableIdentifier> unresumedTables;

  TableDrainRegistry(Set<TableIdentifier> configuredTables) {
    this.unresumedTables = Sets.newConcurrentHashSet(configuredTables);
  }

  TableDrainState get(TableReference tableReference) {
    return states.get(tableReference);
  }

  TableDrainState getOrCreate(TableReference tableReference) {
    return states.computeIfAbsent(tableReference, TableDrainState::new);
  }

  boolean contains(TableReference tableReference) {
    return states.containsKey(tableReference);
  }

  boolean isRegistered(TableDrainState state) {
    return states.get(state.tableReference) == state;
  }

  void remove(TableDrainState state) {
    states.remove(state.tableReference, state);
  }

  Collection<TableDrainState> all() {
    return states.values();
  }

  void forEach(BiConsumer<TableReference, TableDrainState> action) {
    states.forEach(action);
  }

  Map<TableReference, TableDrainState> view() {
    return Collections.unmodifiableMap(states);
  }

  boolean unresumed(TableIdentifier tableIdentifier) {
    return unresumedTables.contains(tableIdentifier);
  }

  void resumed(TableIdentifier tableIdentifier) {
    unresumedTables.remove(tableIdentifier);
  }

  Set<TableIdentifier> unresumed() {
    return Collections.unmodifiableSet(unresumedTables);
  }

  void forgetDrainUnder(UUID tableUuid, TableReference tableReference) {
    drainingUnder.remove(tableUuid, tableReference);
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
  void stopIfSecondNameOfSameTable(TableDrainState state) {
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
        new PermanentCopyOnWriteException(CopyOnWriteMessages.secondName(draining, tableUuid)));
  }

  boolean stopIfPermanent(TableDrainState state, RuntimeException failure) {
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

  boolean registeredAndIdle(TableDrainState state) {
    return isRegistered(state) && state.idle();
  }
}
