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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

public class TestStatisticsLag {

  private static final TableIdentifier TABLE_A = TableIdentifier.of("db", "a");
  private static final TableIdentifier TABLE_B = TableIdentifier.of("db", "b");
  private static final TableIdentifier TABLE_C = TableIdentifier.of("db", "c");
  private static final TableIdentifier TABLE_D = TableIdentifier.of("db", "d");

  private static final Predicate<TableIdentifier> NONE_IN_FLIGHT = id -> false;

  @Test
  public void tokenIsZeroWithoutEntry() {
    StatisticsLag lag = new StatisticsLag();

    assertThat(lag.token(TABLE_A)).isZero();
    assertThat(lag.contains(TABLE_A)).isFalse();
    assertThat(lag.isVerified(TABLE_A)).isFalse();
    assertThat(lag.tables()).isEmpty();
  }

  @Test
  public void everyEnterGivesNewGeneration() {
    StatisticsLag lag = new StatisticsLag();

    lag.enter(TABLE_A);
    long first = lag.token(TABLE_A);
    lag.enter(TABLE_A);
    long second = lag.token(TABLE_A);
    lag.enter(TABLE_B);
    long other = lag.token(TABLE_B);

    assertThat(first).isPositive();
    assertThat(second).isGreaterThan(first);
    assertThat(other).isGreaterThan(second);
    assertThat(lag.contains(TABLE_A)).isTrue();
    assertThat(lag.isVerified(TABLE_A)).isTrue();
  }

  @Test
  public void removeByCurrentTokenRemovesEntry() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    long token = lag.token(TABLE_A);

    assertThat(lag.removeIfToken(TABLE_A, token)).isTrue();

    assertThat(lag.contains(TABLE_A)).isFalse();
    assertThat(lag.token(TABLE_A)).isZero();
    assertThat(lag.removeIfToken(TABLE_A, token)).isFalse();
  }

  @Test
  public void removeByOldTokenDoesNotRemoveNewerEnter() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    long token = lag.token(TABLE_A);

    lag.enter(TABLE_A);

    assertThat(lag.removeIfToken(TABLE_A, token)).isFalse();
    assertThat(lag.contains(TABLE_A)).isTrue();
    assertThat(lag.token(TABLE_A)).isGreaterThan(token);
  }

  @Test
  public void zeroTokenTakenWithoutEntryDoesNotRemoveLaterEnter() {
    StatisticsLag lag = new StatisticsLag();
    long token = lag.token(TABLE_A);

    lag.enter(TABLE_A);

    assertThat(lag.removeIfToken(TABLE_A, token)).isFalse();
    assertThat(lag.contains(TABLE_A)).isTrue();
  }

  @Test
  public void removedAndReenteredTableDoesNotMatchOldToken() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    long token = lag.token(TABLE_A);
    assertThat(lag.removeIfToken(TABLE_A, token)).isTrue();

    lag.enter(TABLE_A);

    assertThat(lag.removeIfToken(TABLE_A, token)).isFalse();
    assertThat(lag.contains(TABLE_A)).isTrue();
  }

  @Test
  public void startupEntriesAreNotVerified() {
    StatisticsLag lag = new StatisticsLag();

    lag.enterAtStartup(ImmutableList.of(TABLE_A, TABLE_B));

    assertThat(lag.contains(TABLE_A)).isTrue();
    assertThat(lag.contains(TABLE_B)).isTrue();
    assertThat(lag.isVerified(TABLE_A)).isFalse();
    assertThat(lag.isVerified(TABLE_B)).isFalse();
    assertThat(lag.token(TABLE_A)).isPositive();
    assertThat(lag.token(TABLE_B)).isPositive().isNotEqualTo(lag.token(TABLE_A));
    assertThat(lag.tables()).containsExactly(TABLE_A, TABLE_B);
  }

  @Test
  public void startupDoesNotUnverifyExistingEntry() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    long token = lag.token(TABLE_A);

    lag.enterAtStartup(ImmutableList.of(TABLE_A));

    assertThat(lag.isVerified(TABLE_A)).isTrue();
    assertThat(lag.token(TABLE_A)).isEqualTo(token);
  }

  @Test
  public void markVerifiedKeepsGeneration() {
    StatisticsLag lag = new StatisticsLag();
    lag.enterAtStartup(ImmutableList.of(TABLE_A));
    long token = lag.token(TABLE_A);

    lag.markVerified(TABLE_A);

    assertThat(lag.isVerified(TABLE_A)).isTrue();
    assertThat(lag.token(TABLE_A)).isEqualTo(token);
    assertThat(lag.removeIfToken(TABLE_A, token)).isTrue();
  }

  @Test
  public void markVerifiedWithoutEntryDoesNotCreateOne() {
    StatisticsLag lag = new StatisticsLag();

    lag.markVerified(TABLE_A);

    assertThat(lag.contains(TABLE_A)).isFalse();
  }

  @Test
  public void enterVerifiesStartupEntryWithNewGeneration() {
    StatisticsLag lag = new StatisticsLag();
    lag.enterAtStartup(ImmutableList.of(TABLE_A));
    long token = lag.token(TABLE_A);

    lag.enter(TABLE_A);

    assertThat(lag.isVerified(TABLE_A)).isTrue();
    assertThat(lag.token(TABLE_A)).isGreaterThan(token);
  }

  @Test
  public void tablesAreInEntryOrderAndReenterKeepsPlace() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_C);
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);

    lag.enter(TABLE_C);

    assertThat(lag.tables()).containsExactly(TABLE_C, TABLE_A, TABLE_B);

    lag.removeIfToken(TABLE_C, lag.token(TABLE_C));
    lag.enter(TABLE_C);

    assertThat(lag.tables()).containsExactly(TABLE_A, TABLE_B, TABLE_C);
  }

  @Test
  public void failureWarnsOnlyOnFirstInRow() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);

    assertThat(lag.recordFailure(TABLE_A)).isTrue();
    lag.enter(TABLE_A);
    assertThat(lag.recordFailure(TABLE_A)).isFalse();
    lag.enter(TABLE_A);
    assertThat(lag.recordFailure(TABLE_A)).isFalse();
  }

  @Test
  public void failureCountIsResetWithEntry() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    assertThat(lag.recordFailure(TABLE_A)).isTrue();
    assertThat(lag.recordFailure(TABLE_A)).isFalse();

    assertThat(lag.removeIfToken(TABLE_A, lag.token(TABLE_A))).isTrue();
    lag.enter(TABLE_A);

    assertThat(lag.recordFailure(TABLE_A)).isTrue();
  }

  @Test
  public void failureCountIsPerTable() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);

    assertThat(lag.recordFailure(TABLE_A)).isTrue();
    assertThat(lag.recordFailure(TABLE_B)).isTrue();
    assertThat(lag.recordFailure(TABLE_A)).isFalse();
  }

  @Test
  public void failureWithoutEntryIsNotCounted() {
    StatisticsLag lag = new StatisticsLag();

    assertThat(lag.recordFailure(TABLE_A)).isFalse();
    assertThat(lag.contains(TABLE_A)).isFalse();

    lag.enter(TABLE_A);
    assertThat(lag.recordFailure(TABLE_A)).isTrue();
  }

  @Test
  public void identifiersDifferingInCaseAreDifferentTables() {
    StatisticsLag lag = new StatisticsLag();
    TableIdentifier upper = TableIdentifier.of("DB", "A");
    lag.enter(TABLE_A);
    lag.enter(upper);

    assertThat(lag.tables()).containsExactly(TABLE_A, upper);
    assertThat(lag.removeIfToken(upper, lag.token(upper))).isTrue();
    assertThat(lag.contains(TABLE_A)).isTrue();
  }

  @Test
  public void admitsOnlyVerifiedTablesWithoutStepInFlight() {
    StatisticsLag lag = new StatisticsLag();
    lag.enterAtStartup(ImmutableList.of(TABLE_A));
    lag.enter(TABLE_B);
    lag.enter(TABLE_C);
    lag.enter(TABLE_D);

    Set<TableIdentifier> admitted = lag.admit(10, TABLE_C::equals);

    assertThat(admitted).containsExactlyInAnyOrder(TABLE_B, TABLE_D);
    assertThat(lag.takeAdmission(TABLE_A)).isFalse();
    assertThat(lag.takeAdmission(TABLE_C)).isFalse();
    assertThat(lag.takeAdmission(TABLE_B)).isTrue();
    assertThat(lag.takeAdmission(TABLE_D)).isTrue();
  }

  @Test
  public void admitsNoMoreThanFreeSlots() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);
    lag.enter(TABLE_C);

    assertThat(lag.admit(0, NONE_IN_FLIGHT)).isEmpty();
    assertThat(lag.admit(-1, NONE_IN_FLIGHT)).isEmpty();
    assertThat(lag.admit(2, NONE_IN_FLIGHT)).containsExactlyInAnyOrder(TABLE_A, TABLE_B);
  }

  @Test
  public void neverAdmittedTablesGoFirstInEntryOrder() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_B);
    lag.enter(TABLE_A);
    assertThat(lag.admit(2, NONE_IN_FLIGHT)).containsExactlyInAnyOrder(TABLE_B, TABLE_A);

    lag.enter(TABLE_D);
    lag.enter(TABLE_C);

    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_D);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_C);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_B);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_A);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_D);
  }

  @Test
  public void notAdmittedStartupEntryBecomesEligibleOnceVerified() {
    StatisticsLag lag = new StatisticsLag();
    lag.enterAtStartup(ImmutableList.of(TABLE_A));
    lag.enter(TABLE_B);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_B);

    lag.markVerified(TABLE_A);

    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_A);
  }

  @Test
  public void unspentAdmissionIsResetByNextAdmit() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_A);

    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_B);

    assertThat(lag.takeAdmission(TABLE_A)).isFalse();
    assertThat(lag.takeAdmission(TABLE_B)).isTrue();
  }

  @Test
  public void admissionIsSpentByFirstTake() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.admit(1, NONE_IN_FLIGHT);

    assertThat(lag.takeAdmission(TABLE_A)).isTrue();
    assertThat(lag.takeAdmission(TABLE_A)).isFalse();
  }

  @Test
  public void admissionWithoutEntryIsNotTaken() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.admit(1, NONE_IN_FLIGHT);
    lag.removeIfToken(TABLE_A, lag.token(TABLE_A));

    assertThat(lag.takeAdmission(TABLE_A)).isFalse();
    assertThat(lag.takeAdmission(TABLE_B)).isFalse();
  }

  @Test
  public void reenterKeepsAdmissionAndItsTime() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_A);

    lag.enter(TABLE_A);

    assertThat(lag.takeAdmission(TABLE_A)).isTrue();
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_B);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_A);
  }

  @Test
  public void admissionTimeIsSetAtChoiceNotAtTake() {
    List<List<TableIdentifier>> forward = admissionsOverCycles(false);
    List<List<TableIdentifier>> reverse = admissionsOverCycles(true);

    // cycle 1 chooses A before B, so A is the older admission whatever the order of takes; a time
    // set in takeAdmission would make B the older one when B is taken first
    assertThat(forward.get(0)).containsExactly(TABLE_A, TABLE_B);
    assertThat(forward.get(1)).containsExactly(TABLE_A, TABLE_C);
    assertThat(reverse).isEqualTo(forward);
  }

  private static List<List<TableIdentifier>> admissionsOverCycles(boolean reverseTakes) {
    StatisticsLag lag = new StatisticsLag();
    List<TableIdentifier> tables = ImmutableList.of(TABLE_A, TABLE_B, TABLE_C);
    tables.forEach(lag::enter);

    List<List<TableIdentifier>> cycles = Lists.newArrayList();
    for (int cycle = 0; cycle < 2; cycle += 1) {
      Set<TableIdentifier> admitted = lag.admit(2, NONE_IN_FLIGHT);
      List<TableIdentifier> ordered = Lists.newArrayList(tables);
      ordered.retainAll(admitted);
      cycles.add(ordered);

      List<TableIdentifier> takes = Lists.newArrayList(tables);
      if (reverseTakes) {
        takes = Lists.reverse(takes);
      }

      takes.forEach(lag::takeAdmission);
    }

    return cycles;
  }

  @Test
  public void constantlyFailingTableDoesNotStarveOthers() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);
    lag.enter(TABLE_C);
    Set<TableIdentifier> stepped = Sets.newHashSet();

    for (int cycle = 0; cycle < 3; cycle += 1) {
      Set<TableIdentifier> admitted = lag.admit(1, NONE_IN_FLIGHT);
      assertThat(admitted).hasSize(1);
      TableIdentifier table = admitted.iterator().next();
      long token = lag.token(table);
      assertThat(lag.takeAdmission(table)).isTrue();
      stepped.add(table);

      if (table.equals(TABLE_A)) {
        lag.enter(TABLE_A);
        lag.recordFailure(TABLE_A);
      } else {
        assertThat(lag.removeIfToken(table, token)).isTrue();
      }
    }

    assertThat(stepped).containsExactlyInAnyOrder(TABLE_A, TABLE_B, TABLE_C);
    assertThat(lag.tables()).containsExactly(TABLE_A);
  }

  @Test
  public void constantlyFailingTablesShareSlotsInTurn() {
    StatisticsLag lag = new StatisticsLag();
    List<TableIdentifier> tables = ImmutableList.of(TABLE_A, TABLE_B, TABLE_C);
    tables.forEach(lag::enter);
    List<TableIdentifier> order = Lists.newArrayList();

    for (int cycle = 0; cycle < 6; cycle += 1) {
      Set<TableIdentifier> admitted = lag.admit(1, NONE_IN_FLIGHT);
      TableIdentifier table = admitted.iterator().next();
      lag.takeAdmission(table);
      lag.enter(table);
      lag.recordFailure(table);
      order.add(table);
    }

    assertThat(order).containsExactly(TABLE_A, TABLE_B, TABLE_C, TABLE_A, TABLE_B, TABLE_C);
  }

  @Test
  public void tableWithStepInFlightKeepsItsTurn() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);
    lag.enter(TABLE_B);

    assertThat(lag.admit(1, TABLE_A::equals)).containsExactly(TABLE_B);
    assertThat(lag.admit(1, NONE_IN_FLIGHT)).containsExactly(TABLE_A);
  }

  @Test
  public void concurrentEntersGiveDistinctGenerations() throws Exception {
    StatisticsLag lag = new StatisticsLag();
    int threads = 8;
    int perThread = 500;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    Set<Long> generations = Sets.newHashSet();
    try {
      List<Future<List<Long>>> futures = Lists.newArrayList();
      for (int thread = 0; thread < threads; thread += 1) {
        TableIdentifier table = TableIdentifier.of("db", "concurrent_" + thread);
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  List<Long> seen = Lists.newArrayList();
                  for (int iteration = 0; iteration < perThread; iteration += 1) {
                    lag.enter(table);
                    long token = lag.token(table);
                    seen.add(token);
                    if (!lag.removeIfToken(table, token)) {
                      throw new AssertionError("Own token did not remove the entry of " + table);
                    }
                  }
                  return seen;
                }));
      }

      start.countDown();
      for (Future<List<Long>> future : futures) {
        generations.addAll(future.get(30, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(generations).hasSize(threads * perThread).doesNotContain(0L);
    assertThat(lag.tables()).isEmpty();
  }

  @Test
  public void admitReturnsOnlyAdmittedTables() {
    StatisticsLag lag = new StatisticsLag();
    lag.enter(TABLE_A);

    assertThat(lag.admit(3, NONE_IN_FLIGHT)).isEqualTo(ImmutableSet.of(TABLE_A));
  }
}
