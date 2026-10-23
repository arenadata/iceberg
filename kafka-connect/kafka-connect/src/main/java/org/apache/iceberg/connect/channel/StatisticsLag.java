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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * The statistics lag of one committer: tables whose head the statistics step still owes records to;
 * a copy-on-write committer shares it with its merge-on-read tail. Keyed by {@link
 * TableIdentifier}, thread-safe.
 *
 * <p>An entry carries:
 *
 * <ul>
 *   <li>a <b>generation</b> from the instance's monotonic counter, new on every entry into the lag,
 *       including an entry into an existing record. Whoever may clear the lag takes a {@link
 *       #token} first and clears the entry only if its generation still equals the token ({@link
 *       #removeIfToken}): an entry made after the token may belong to a snapshot newer than the one
 *       the step covered, so it survives;
 *   <li>whether it is <b>verified</b>: entries made at startup are not, until the quick check of
 *       {@link StatisticsLagChecker} marks them; every other entry is;
 *   <li>the <b>entry order</b> and the <b>last admission</b> of a lag step, both in memory only;
 *   <li>the number of <b>failures in a row</b>, reset together with the entry.
 * </ul>
 *
 * <p>Entries are immutable and replaced through the map's atomic operations, so every change of one
 * table's entry is atomic with respect to the others.
 */
class StatisticsLag {

  private static final long NEVER_ADMITTED = 0L;

  private final Map<TableIdentifier, Entry> entries = Maps.newConcurrentMap();
  private final AtomicLong generations = new AtomicLong();
  private final AtomicLong entryOrder = new AtomicLong();
  private final AtomicLong admissions = new AtomicLong();

  void enter(TableIdentifier id) {
    entries.compute(
        id,
        (key, entry) -> {
          long generation = generations.incrementAndGet();
          return entry == null
              ? Entry.created(generation, true, entryOrder.incrementAndGet())
              : entry.reentered(generation);
        });
  }

  void enterAtStartup(Collection<TableIdentifier> ids) {
    for (TableIdentifier id : ids) {
      entries.computeIfAbsent(
          id,
          key -> Entry.created(generations.incrementAndGet(), false, entryOrder.incrementAndGet()));
    }
  }

  void markVerified(TableIdentifier id) {
    entries.computeIfPresent(id, (key, entry) -> entry.verified());
  }

  /** The generation of the table's entry, or 0 if the table is not in the lag. */
  long token(TableIdentifier id) {
    Entry entry = entries.get(id);
    return entry == null ? 0L : entry.generation;
  }

  boolean removeIfToken(TableIdentifier id, long token) {
    AtomicBoolean removed = new AtomicBoolean(false);
    entries.computeIfPresent(
        id,
        (key, entry) -> {
          if (entry.generation == token) {
            removed.set(true);
            return null;
          }

          return entry;
        });
    return removed.get();
  }

  boolean contains(TableIdentifier id) {
    return entries.containsKey(id);
  }

  boolean isVerified(TableIdentifier id) {
    Entry entry = entries.get(id);
    return entry != null && entry.verified;
  }

  Collection<TableIdentifier> tables() {
    return entries.entrySet().stream()
        .sorted(Comparator.comparingLong(entry -> entry.getValue().order))
        .map(Map.Entry::getKey)
        .collect(ImmutableList.toImmutableList());
  }

  boolean recordFailure(TableIdentifier id) {
    Entry updated = entries.computeIfPresent(id, (key, entry) -> entry.failed());
    return updated != null && updated.failures == 1;
  }

  /**
   * Applies the outcome of a statistics step taken with {@code token} to the lag.
   *
   * @return true for a {@code FAILED} that is the first failure in a row
   */
  boolean applyOutcome(TableIdentifier id, long token, StatisticsStep.Outcome outcome) {
    switch (outcome) {
      case WRITTEN:
      case NOT_NEEDED:
      case FINAL:
      case TABLE_GONE:
        removeIfToken(id, token);
        return false;
      case FAILED:
        enter(id);
        return recordFailure(id);
      case SNAPSHOT_GONE:
        enter(id);
        return false;
      default:
        return false;
    }
  }

  /**
   * Admits up to {@code freeSlots} verified tables without a lag step in flight, after dropping the
   * admissions left unspent from the previous call: first those never admitted, in entry order,
   * then the oldest last admission first. The admission time is set at the choice, so a table whose
   * step did not run still goes to the end of the line.
   *
   * <p>Atomic per entry only: one call per cycle, from {@code pendingTables()} on the coordinator
   * thread.
   *
   * @return the tables admitted in this cycle
   */
  Set<TableIdentifier> admit(int freeSlots, Predicate<TableIdentifier> inFlight) {
    entries.replaceAll((key, entry) -> entry.withoutAdmission());
    if (freeSlots <= 0) {
      return ImmutableSet.of();
    }

    List<TableIdentifier> chosen =
        entries.entrySet().stream()
            .filter(entry -> entry.getValue().verified)
            .filter(entry -> !inFlight.test(entry.getKey()))
            .sorted(Map.Entry.comparingByValue(Entry.ADMISSION_ORDER))
            .limit(freeSlots)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    ImmutableSet.Builder<TableIdentifier> admitted = ImmutableSet.builder();
    for (TableIdentifier id : chosen) {
      long admission = admissions.incrementAndGet();
      if (entries.computeIfPresent(id, (key, entry) -> entry.admitted(admission)) != null) {
        admitted.add(id);
      }
    }

    return admitted.build();
  }

  boolean takeAdmission(TableIdentifier id) {
    AtomicBoolean taken = new AtomicBoolean(false);
    entries.computeIfPresent(
        id,
        (key, entry) -> {
          if (entry.admitted) {
            taken.set(true);
            return entry.withoutAdmission();
          }

          return entry;
        });
    return taken.get();
  }

  private static final class Entry {
    private static final Comparator<Entry> ADMISSION_ORDER =
        Comparator.<Entry>comparingLong(entry -> entry.lastAdmission)
            .thenComparingLong(entry -> entry.order);

    private final long generation;
    private final boolean verified;
    private final long order;
    private final long lastAdmission;
    private final boolean admitted;
    private final int failures;

    private Entry(
        long generation,
        boolean verified,
        long order,
        long lastAdmission,
        boolean admitted,
        int failures) {
      this.generation = generation;
      this.verified = verified;
      this.order = order;
      this.lastAdmission = lastAdmission;
      this.admitted = admitted;
      this.failures = failures;
    }

    static Entry created(long generation, boolean verified, long order) {
      return new Entry(generation, verified, order, NEVER_ADMITTED, false, 0);
    }

    Entry reentered(long newGeneration) {
      return new Entry(newGeneration, true, order, lastAdmission, admitted, failures);
    }

    Entry verified() {
      return new Entry(generation, true, order, lastAdmission, admitted, failures);
    }

    Entry failed() {
      return new Entry(generation, verified, order, lastAdmission, admitted, failures + 1);
    }

    Entry admitted(long admission) {
      return new Entry(generation, verified, order, admission, true, failures);
    }

    Entry withoutAdmission() {
      return admitted
          ? new Entry(generation, verified, order, lastAdmission, false, failures)
          : this;
    }
  }
}
