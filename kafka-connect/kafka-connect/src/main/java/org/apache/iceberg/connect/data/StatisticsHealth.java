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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Statistics health flags of tables, one instance per coordinator.
 *
 * <p>Keyed by {@link TableIdentifier}. Besides the flags a record keeps a version (grows with every
 * applied transition), the monotonic time of the last evaluation and the mark "re-check assigned in
 * this cycle".
 */
public class StatisticsHealth {

  private static final Logger LOG = LoggerFactory.getLogger(StatisticsHealth.class);

  public enum Flag {
    GAP_OVER_LIMIT("gapOverLimit") {
      @Override
      boolean isSet(Flags flags) {
        return flags.gapOverLimit();
      }
    },
    MISSING_BASELINE("missingBaseline") {
      @Override
      boolean isSet(Flags flags) {
        return flags.missingBaseline();
      }
    },
    STALE("stale") {
      @Override
      boolean isSet(Flags flags) {
        return flags.stale();
      }
    };

    private final String label;

    Flag(String label) {
      this.label = label;
    }

    String label() {
      return label;
    }

    abstract boolean isSet(Flags flags);
  }

  /** Immutable three health flags. */
  public static final class Flags {
    public static final Flags NONE = new Flags(false, false, false);

    private final boolean gapOverLimit;
    private final boolean missingBaseline;
    private final boolean stale;

    public Flags(boolean gapOverLimit, boolean missingBaseline, boolean stale) {
      this.gapOverLimit = gapOverLimit;
      this.missingBaseline = missingBaseline;
      this.stale = stale;
    }

    public boolean gapOverLimit() {
      return gapOverLimit;
    }

    public boolean missingBaseline() {
      return missingBaseline;
    }

    public boolean stale() {
      return stale;
    }

    boolean any() {
      return gapOverLimit || missingBaseline || stale;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Flags)) {
        return false;
      }
      Flags that = (Flags) other;
      return gapOverLimit == that.gapOverLimit
          && missingBaseline == that.missingBaseline
          && stale == that.stale;
    }

    @Override
    public int hashCode() {
      return Objects.hash(gapOverLimit, missingBaseline, stale);
    }

    @Override
    public String toString() {
      return "Flags{gapOverLimit="
          + gapOverLimit
          + ", missingBaseline="
          + missingBaseline
          + ", stale="
          + stale
          + '}';
    }
  }

  private static final class Entry {
    private final Flags flags;
    private final long version;
    private final long evaluatedNanos;
    private final boolean recheckAssigned;

    private Entry(Flags flags, long version, long evaluatedNanos, boolean recheckAssigned) {
      this.flags = flags;
      this.version = version;
      this.evaluatedNanos = evaluatedNanos;
      this.recheckAssigned = recheckAssigned;
    }

    private Entry withRecheck(boolean assigned, long nanos) {
      return new Entry(flags, version, nanos, assigned);
    }
  }

  /** Stores nothing and assigns nothing. */
  public static final StatisticsHealth NOOP =
      new StatisticsHealth(0L) {
        @Override
        public Flags update(TableIdentifier table, UnaryOperator<Flags> transition) {
          return Flags.NONE;
        }

        @Override
        public Flags updateIfUnchanged(
            TableIdentifier table, long version, UnaryOperator<Flags> transition) {
          return Flags.NONE;
        }

        @Override
        public Flags updateUnlessRemoved(
            TableIdentifier table, long version, UnaryOperator<Flags> transition) {
          return Flags.NONE;
        }

        @Override
        public Set<TableIdentifier> assignRechecks(int limit) {
          return Collections.emptySet();
        }

        @Override
        public boolean takeRecheck(TableIdentifier table) {
          return false;
        }

        @Override
        public void remove(TableIdentifier table) {}
      };

  private final ConcurrentMap<TableIdentifier, Entry> entries = Maps.newConcurrentMap();
  // global, so a version never repeats for a table even after remove()
  private final AtomicLong versions = new AtomicLong();
  private final long recheckIntervalNanos;
  private final LongSupplier nanoClock;

  public StatisticsHealth(long recheckIntervalMs) {
    this(recheckIntervalMs, System::nanoTime);
  }

  /** With the clock given, so that tests of other packages move time forward explicitly. */
  public StatisticsHealth(long recheckIntervalMs, LongSupplier nanoClock) {
    this.recheckIntervalNanos = TimeUnit.MILLISECONDS.toNanos(recheckIntervalMs);
    this.nanoClock = nanoClock;
  }

  /**
   * Atomically applies the transition.
   *
   * @return flags before the transition; all {@code false} for an absent table
   */
  public Flags update(TableIdentifier table, UnaryOperator<Flags> transition) {
    Flags[] previous = {Flags.NONE};
    entries.compute(table, (key, entry) -> apply(entry, transition, previous));
    return previous[0];
  }

  /** Version of the table record; 0 if the table is absent. */
  public long version(TableIdentifier table) {
    Entry entry = entries.get(table);
    return entry == null ? 0L : entry.version;
  }

  /**
   * Applies the transition only if the record still has the given version.
   *
   * @return flags before the transition, or {@code null} if the version changed and the transition
   *     was not applied
   */
  public Flags updateIfUnchanged(
      TableIdentifier table, long version, UnaryOperator<Flags> transition) {
    Flags[] previous = {null};
    entries.compute(
        table,
        (key, entry) -> {
          long current = entry == null ? 0L : entry.version;
          if (current != version) {
            return entry;
          }
          Flags[] applied = {Flags.NONE};
          Entry updated = apply(entry, transition, applied);
          previous[0] = applied[0];
          return updated;
        });
    return previous[0];
  }

  /**
   * Applies the transition unless the record of the given version was removed since: a record
   * changed meanwhile takes the transition, so the flags describe the evaluation that ended last. A
   * record absent at the given version (0) is created.
   *
   * @return flags before the transition, or {@code null} if the record was removed and the
   *     transition was not applied
   */
  public Flags updateUnlessRemoved(
      TableIdentifier table, long version, UnaryOperator<Flags> transition) {
    Flags[] previous = {null};
    entries.compute(
        table,
        (key, entry) -> {
          if (entry == null && version != 0L) {
            return null;
          }
          Flags[] applied = {Flags.NONE};
          Entry updated = apply(entry, transition, applied);
          previous[0] = applied[0];
          return updated;
        });
    return previous[0];
  }

  private Entry apply(Entry entry, UnaryOperator<Flags> transition, Flags[] previous) {
    Flags before = entry == null ? Flags.NONE : entry.flags;
    previous[0] = before;
    Flags after = transition.apply(before);
    return new Entry(
        after,
        versions.incrementAndGet(),
        nanoClock.getAsLong(),
        entry != null && entry.recheckAssigned);
  }

  /**
   * Drops the assignments of the previous cycle and assigns up to {@code limit} tables that have a
   * raised flag and whose last evaluation is older than the re-check interval, the oldest first.
   * The evaluation time of an assigned table becomes now (an attempt). Nothing is assigned if the
   * interval is 0.
   */
  public Set<TableIdentifier> assignRechecks(int limit) {
    for (TableIdentifier table : ImmutableSet.copyOf(entries.keySet())) {
      entries.computeIfPresent(
          table,
          (key, entry) ->
              entry.recheckAssigned ? entry.withRecheck(false, entry.evaluatedNanos) : entry);
    }

    if (recheckIntervalNanos <= 0 || limit <= 0) {
      return Collections.emptySet();
    }

    long now = nanoClock.getAsLong();
    List<Map.Entry<TableIdentifier, Entry>> candidates = Lists.newArrayList();
    for (Map.Entry<TableIdentifier, Entry> item : entries.entrySet()) {
      if (isDue(item.getValue(), now)) {
        candidates.add(item);
      }
    }
    candidates.sort(Comparator.comparingLong(item -> item.getValue().evaluatedNanos));

    ImmutableSet.Builder<TableIdentifier> assigned = ImmutableSet.builder();
    int count = 0;
    for (Map.Entry<TableIdentifier, Entry> candidate : candidates) {
      if (count >= limit) {
        break;
      }
      boolean[] taken = {false};
      entries.computeIfPresent(
          candidate.getKey(),
          (key, entry) -> {
            if (!isDue(entry, now)) {
              return entry;
            }
            taken[0] = true;
            return entry.withRecheck(true, now);
          });
      if (taken[0]) {
        assigned.add(candidate.getKey());
        count++;
      }
    }
    return assigned.build();
  }

  private boolean isDue(Entry entry, long now) {
    return entry.flags.any() && now - entry.evaluatedNanos >= recheckIntervalNanos;
  }

  /** Consumes the re-check assignment of the table; {@code false} if it is not assigned. */
  public boolean takeRecheck(TableIdentifier table) {
    boolean[] taken = {false};
    entries.computeIfPresent(
        table,
        (key, entry) -> {
          if (!entry.recheckAssigned) {
            return entry;
          }
          taken[0] = true;
          return entry.withRecheck(false, entry.evaluatedNanos);
        });
    return taken[0];
  }

  public void remove(TableIdentifier table) {
    entries.remove(table);
  }

  /** Number of tables whose flags satisfy the predicate; does not block writers. */
  public int count(Predicate<Flags> predicate) {
    int count = 0;
    for (Entry entry : entries.values()) {
      if (predicate.test(entry.flags)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Logs the flag transitions of a table from {@code previous} to {@code updated}: a raise at WARN,
   * a flag still raised at DEBUG, a clear at INFO. Does nothing if either is {@code null}, as for a
   * transition that was not applied.
   *
   * @param snapshotId the snapshot the flags describe, or {@code null}
   */
  public static void logTransitions(
      TableIdentifier table,
      Long snapshotId,
      Flags previous,
      Flags updated,
      Map<Flag, String> details) {
    logTransitions(LOG, table, snapshotId, previous, updated, details);
  }

  static void logTransitions(
      Logger logger,
      TableIdentifier table,
      Long snapshotId,
      Flags previous,
      Flags updated,
      Map<Flag, String> details) {
    if (previous == null || updated == null) {
      return;
    }

    for (Flag flag : Flag.values()) {
      boolean was = flag.isSet(previous);
      boolean is = flag.isSet(updated);
      if (!was && !is) {
        continue;
      }

      String snapshot = snapshotId == null ? "unknown" : String.valueOf(snapshotId);
      if (!is) {
        logger.info(
            "Statistics state '{}' of table {} cleared at snapshot {}",
            flag.label(),
            table,
            snapshot);
      } else if (was) {
        logger.debug(
            "Statistics state '{}' of table {} still raised at snapshot {}: {}",
            flag.label(),
            table,
            snapshot,
            details.getOrDefault(flag, "no details"));
      } else {
        logger.warn(
            "Statistics state '{}' of table {} raised at snapshot {}: {}",
            flag.label(),
            table,
            snapshot,
            details.getOrDefault(flag, "no details"));
      }
    }
  }
}
