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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.StatisticsHealth.Flag;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;

public class TestStatisticsHealth {

  private static final TableIdentifier T1 = TableIdentifier.of("db", "t1");
  private static final TableIdentifier T2 = TableIdentifier.of("db", "t2");
  private static final TableIdentifier T3 = TableIdentifier.of("db", "t3");
  private static final Flags GAP = new Flags(true, false, false);
  private static final Flags STALE = new Flags(false, false, true);
  private static final long INTERVAL_MS = 1000L;

  private final AtomicLong nanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
  private StatisticsHealth health;

  @BeforeEach
  public void before() {
    health = new StatisticsHealth(INTERVAL_MS, nanos::get);
  }

  private void advanceMs(long millis) {
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
  }

  @Test
  public void testUpdateReturnsPreviousFlags() {
    assertThat(health.update(T1, flags -> GAP)).isEqualTo(Flags.NONE);
    assertThat(health.update(T1, flags -> STALE)).isEqualTo(GAP);
    assertThat(health.update(T1, flags -> flags)).isEqualTo(STALE);
    assertThat(health.update(T2, flags -> flags)).isEqualTo(Flags.NONE);
  }

  @Test
  public void testUpdateTransitionSeesCurrentFlags() {
    health.update(T1, flags -> GAP);
    health.update(T1, flags -> new Flags(flags.gapOverLimit(), true, false));

    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertThat(health.count(Flags::missingBaseline)).isEqualTo(1);
  }

  @Test
  public void testVersion() {
    assertThat(health.version(T1)).isZero();

    health.update(T1, flags -> GAP);
    long first = health.version(T1);
    assertThat(first).isPositive();

    health.update(T1, flags -> STALE);
    long second = health.version(T1);
    assertThat(second).isGreaterThan(first);

    assertThat(health.version(T2)).isZero();

    // the version does not restart after remove: a stale reader must not match a new record
    health.remove(T1);
    assertThat(health.version(T1)).isZero();
    health.update(T1, flags -> GAP);
    assertThat(health.version(T1)).isGreaterThan(second);
  }

  @Test
  public void testUpdateIfUnchanged() {
    long absent = health.version(T1);
    assertThat(health.updateIfUnchanged(T1, absent, flags -> GAP)).isEqualTo(Flags.NONE);
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);

    long taken = health.version(T1);
    health.update(T1, flags -> STALE);

    assertThat(health.updateIfUnchanged(T1, taken, flags -> Flags.NONE)).isNull();
    assertThat(health.count(Flags::stale)).isEqualTo(1);
    assertThat(health.count(Flags::gapOverLimit)).isZero();

    long current = health.version(T1);
    assertThat(health.updateIfUnchanged(T1, current, flags -> GAP)).isEqualTo(STALE);
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertThat(health.version(T1)).isGreaterThan(current);
  }

  @Test
  public void testUpdateIfUnchangedAfterRemove() {
    health.update(T1, flags -> GAP);
    long taken = health.version(T1);
    health.remove(T1);

    assertThat(health.updateIfUnchanged(T1, taken, flags -> STALE)).isNull();
    assertThat(health.count(flags -> true)).isZero();
  }

  @Test
  public void testUpdateUnlessRemoved() {
    long absent = health.version(T1);
    assertThat(health.updateUnlessRemoved(T1, absent, flags -> GAP)).isEqualTo(Flags.NONE);
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);

    long taken = health.version(T1);
    health.update(T1, flags -> STALE);

    // a record changed since the version taken still takes the transition
    assertThat(health.updateUnlessRemoved(T1, taken, flags -> GAP)).isEqualTo(STALE);
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertThat(health.count(Flags::stale)).isZero();
  }

  @Test
  public void testUpdateUnlessRemovedAfterRemove() {
    health.update(T1, flags -> GAP);
    long taken = health.version(T1);
    health.remove(T1);

    assertThat(health.updateUnlessRemoved(T1, taken, flags -> STALE)).isNull();
    assertThat(health.count(flags -> true)).isZero();

    // removed and created again: the transition applies to the new record
    health.update(T1, flags -> GAP);
    assertThat(health.updateUnlessRemoved(T1, taken, flags -> STALE)).isEqualTo(GAP);
    assertThat(health.count(Flags::stale)).isEqualTo(1);
  }

  @Test
  public void testAssignRechecksRespectsInterval() {
    health.update(T1, flags -> GAP);

    assertThat(health.assignRechecks(10)).isEmpty();

    advanceMs(INTERVAL_MS - 1);
    assertThat(health.assignRechecks(10)).isEmpty();

    advanceMs(1);
    assertThat(health.assignRechecks(10)).containsExactly(T1);
  }

  @Test
  public void testAssignRechecksOnlyRaisedFlags() {
    health.update(T1, flags -> GAP);
    health.update(T2, flags -> Flags.NONE);
    advanceMs(INTERVAL_MS);

    assertThat(health.assignRechecks(10)).containsExactly(T1);
  }

  @Test
  public void testAssignRechecksLimitOldestFirst() {
    health.update(T2, flags -> GAP);
    advanceMs(10);
    health.update(T1, flags -> GAP);
    advanceMs(10);
    health.update(T3, flags -> STALE);
    advanceMs(INTERVAL_MS);

    assertThat(health.assignRechecks(1)).containsExactly(T2);
    // the assignment is an attempt: it renews the evaluation time, so the next one is the oldest
    // of the rest
    assertThat(health.assignRechecks(2)).containsExactlyInAnyOrder(T1, T3);
    assertThat(health.assignRechecks(2)).isEmpty();
  }

  @Test
  public void testAssignRechecksResetsPreviousAssignments() {
    health.update(T1, flags -> GAP);
    advanceMs(INTERVAL_MS);
    assertThat(health.assignRechecks(1)).containsExactly(T1);

    // the next cycle: the unused assignment is dropped; the interval has not expired again
    assertThat(health.assignRechecks(1)).isEmpty();
    assertThat(health.takeRecheck(T1)).isFalse();
  }

  @Test
  public void testAssignRechecksZeroInterval() {
    StatisticsHealth disabled = new StatisticsHealth(0L, nanos::get);
    disabled.update(T1, flags -> GAP);
    advanceMs(TimeUnit.HOURS.toMillis(1));

    assertThat(disabled.assignRechecks(10)).isEmpty();
    assertThat(disabled.takeRecheck(T1)).isFalse();
  }

  @Test
  public void testAssignRechecksLimitZero() {
    health.update(T1, flags -> GAP);
    advanceMs(INTERVAL_MS);

    assertThat(health.assignRechecks(0)).isEmpty();
    assertThat(health.assignRechecks(1)).containsExactly(T1);
  }

  @Test
  public void testUpdateRenewsEvaluationTime() {
    health.update(T1, flags -> GAP);
    advanceMs(INTERVAL_MS - 1);
    health.update(T1, flags -> GAP);
    advanceMs(1);
    assertThat(health.assignRechecks(10)).isEmpty();

    advanceMs(INTERVAL_MS - 1);
    health.updateIfUnchanged(T1, health.version(T1), flags -> GAP);
    advanceMs(1);
    assertThat(health.assignRechecks(10)).isEmpty();

    advanceMs(INTERVAL_MS);
    assertThat(health.assignRechecks(10)).containsExactly(T1);
  }

  @Test
  public void testTakeRecheck() {
    health.update(T1, flags -> GAP);
    advanceMs(10);
    health.update(T2, flags -> GAP);
    advanceMs(INTERVAL_MS);
    health.assignRechecks(1);
    long version = health.version(T1);

    // one of the two is assigned; T1 was evaluated first, hence the oldest
    assertThat(health.takeRecheck(T2)).isFalse();
    assertThat(health.takeRecheck(T1)).isTrue();
    assertThat(health.takeRecheck(T1)).isFalse();
    // taking does not change the record
    assertThat(health.version(T1)).isEqualTo(version);
    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(2);
    assertThat(health.takeRecheck(T3)).isFalse();
  }

  @Test
  public void testRemove() {
    health.update(T1, flags -> GAP);
    health.update(T2, flags -> GAP);
    advanceMs(INTERVAL_MS);
    health.assignRechecks(10);

    health.remove(T1);

    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(1);
    assertThat(health.takeRecheck(T1)).isFalse();
    assertThat(health.takeRecheck(T2)).isTrue();
    assertThat(health.update(T1, flags -> flags)).isEqualTo(Flags.NONE);

    // absent table is not an error
    health.remove(T3);
  }

  @Test
  public void testCount() {
    assertThat(health.count(flags -> true)).isZero();

    health.update(T1, flags -> GAP);
    health.update(T2, flags -> new Flags(true, true, true));
    health.update(T3, flags -> Flags.NONE);

    assertThat(health.count(Flags::gapOverLimit)).isEqualTo(2);
    assertThat(health.count(Flags::missingBaseline)).isEqualTo(1);
    assertThat(health.count(Flags::stale)).isEqualTo(1);
    assertThat(health.count(flags -> !flags.gapOverLimit())).isEqualTo(1);
  }

  @Test
  public void testFlagsEquality() {
    assertThat(new Flags(true, false, true)).isEqualTo(new Flags(true, false, true));
    assertThat(new Flags(true, false, true)).isNotEqualTo(new Flags(true, true, true));
    assertThat(new Flags(false, false, false)).isEqualTo(Flags.NONE);
    assertThat(new Flags(true, false, true)).hasSameHashCodeAs(new Flags(true, false, true));
  }

  @Test
  public void testNoop() {
    StatisticsHealth noop = StatisticsHealth.NOOP;

    assertThat(noop.update(T1, flags -> GAP)).isEqualTo(Flags.NONE);
    assertThat(noop.version(T1)).isZero();
    assertThat(noop.updateIfUnchanged(T1, 0L, flags -> GAP)).isEqualTo(Flags.NONE);
    assertThat(noop.updateUnlessRemoved(T1, 0L, flags -> GAP)).isEqualTo(Flags.NONE);
    assertThat(noop.count(flags -> true)).isZero();
    assertThat(noop.count(Flags::gapOverLimit)).isZero();
    assertThat(noop.assignRechecks(10)).isEmpty();
    assertThat(noop.takeRecheck(T1)).isFalse();
    noop.remove(T1);
  }

  @Test
  public void testLogRepeatedRaiseIsWarnOnceThenDebug() {
    Logger logger = mock(Logger.class);
    Map<Flag, String> details = ImmutableMap.of(Flag.GAP_OVER_LIMIT, "gap files=5");

    Flags first = health.update(T1, flags -> GAP);
    StatisticsHealth.logTransitions(logger, T1, 7L, first, GAP, details);
    Flags second = health.update(T1, flags -> GAP);
    StatisticsHealth.logTransitions(logger, T1, 8L, second, GAP, details);
    Flags third = health.update(T1, flags -> Flags.NONE);
    StatisticsHealth.logTransitions(logger, T1, 9L, third, Flags.NONE, ImmutableMap.of());

    InOrder order = inOrder(logger);
    order
        .verify(logger)
        .warn(contains("raised at"), eq("gapOverLimit"), eq(T1), eq("7"), eq("gap files=5"));
    order
        .verify(logger)
        .debug(contains("still raised"), eq("gapOverLimit"), eq(T1), eq("8"), eq("gap files=5"));
    order.verify(logger).info(contains("cleared"), eq("gapOverLimit"), eq(T1), eq("9"));
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testLogEachFlagOnItsOwn() {
    Logger logger = mock(Logger.class);

    // gap cleared, stale raised, missingBaseline stays down
    StatisticsHealth.logTransitions(
        logger, T1, 3L, GAP, STALE, ImmutableMap.of(Flag.STALE, "c=0.9"));

    verify(logger).warn(contains("raised at"), eq("stale"), eq(T1), eq("3"), eq("c=0.9"));
    verify(logger).info(contains("cleared"), eq("gapOverLimit"), eq(T1), eq("3"));
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testLogNoTransitionsIsSilent() {
    Logger logger = mock(Logger.class);

    StatisticsHealth.logTransitions(logger, T1, 3L, Flags.NONE, Flags.NONE, ImmutableMap.of());
    StatisticsHealth.logTransitions(logger, T1, null, Flags.NONE, Flags.NONE, ImmutableMap.of());

    verifyNoInteractions(logger);
  }

  @Test
  public void testLogWithoutSnapshotAndDetails() {
    Logger logger = mock(Logger.class);

    StatisticsHealth.logTransitions(logger, T1, null, Flags.NONE, GAP, ImmutableMap.of());

    verify(logger)
        .warn(contains("raised at"), eq("gapOverLimit"), eq(T1), eq("unknown"), eq("no details"));
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void testLogWithoutAppliedTransitionIsSilent() {
    Logger logger = mock(Logger.class);

    // NOOP and a not applied updateIfUnchanged do not run the transition: nothing to compare
    Flags[] updated = {null};
    Flags previous = StatisticsHealth.NOOP.update(T1, flags -> updated[0] = GAP);
    assertThat(updated[0]).isNull();

    StatisticsHealth.logTransitions(logger, T1, 3L, previous, updated[0], ImmutableMap.of());
    StatisticsHealth.logTransitions(logger, T1, 3L, null, GAP, ImmutableMap.of());
    StatisticsHealth.logTransitions(logger, T1, 3L, null, null, ImmutableMap.of());

    verifyNoInteractions(logger);
  }
}
