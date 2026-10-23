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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestStatisticsLagStepLimiter {

  private static final TableIdentifier TABLE_A = TableIdentifier.of("db", "a");
  private static final TableIdentifier TABLE_B = TableIdentifier.of("db", "b");
  private static final TableIdentifier TABLE_C = TableIdentifier.of("db", "c");
  private static final TableIdentifier TABLE_D = TableIdentifier.of("db", "d");
  private static final TableIdentifier TABLE_E = TableIdentifier.of("db", "e");

  private static final BooleanSupplier NOT_STOPPED = () -> false;

  @Test
  public void slotLimitIsHalfOfCommitThreadsButAtLeastOne() {
    assertThat(new StatisticsLagStepLimiter(1).freeSlots()).isEqualTo(1);
    assertThat(new StatisticsLagStepLimiter(2).freeSlots()).isEqualTo(1);
    assertThat(new StatisticsLagStepLimiter(3).freeSlots()).isEqualTo(1);
    assertThat(new StatisticsLagStepLimiter(8).freeSlots()).isEqualTo(4);
    assertThat(new StatisticsLagStepLimiter(9).freeSlots()).isEqualTo(4);
  }

  @Test
  public void enteredTablesTakeSlotsUntilTheyLeave() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);

    limiter.enter(TABLE_A);
    limiter.enter(TABLE_B);

    assertThat(limiter.freeSlots()).isEqualTo(2);
    assertThat(limiter.inFlight(TABLE_A)).isTrue();
    assertThat(limiter.inFlight(TABLE_B)).isTrue();
    assertThat(limiter.inFlight(TABLE_C)).isFalse();

    limiter.leave(TABLE_A);

    assertThat(limiter.freeSlots()).isEqualTo(3);
    assertThat(limiter.inFlight(TABLE_A)).isFalse();
  }

  @Test
  public void sameTableTakesOneSlot() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);

    limiter.enter(TABLE_A);
    limiter.enter(TABLE_A);

    assertThat(limiter.freeSlots()).isEqualTo(3);

    limiter.leave(TABLE_A);

    assertThat(limiter.freeSlots()).isEqualTo(4);
  }

  @Test
  public void leaveOfTableNotInFlightChangesNothing() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(4);
    limiter.enter(TABLE_A);

    limiter.leave(TABLE_B);

    assertThat(limiter.freeSlots()).isEqualTo(1);
    assertThat(limiter.inFlight(TABLE_A)).isTrue();
  }

  @Test
  public void freeSlotsIsNeverNegative() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(4);

    limiter.enter(TABLE_A);
    limiter.enter(TABLE_B);
    limiter.enter(TABLE_C);

    assertThat(limiter.freeSlots()).isZero();
  }

  @Test
  public void identifiersDifferingInCaseAreDifferentTables() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);

    limiter.enter(TABLE_A);
    limiter.enter(TableIdentifier.of("DB", "A"));

    assertThat(limiter.freeSlots()).isEqualTo(2);
  }

  @Test
  public void twoPermitsRegardlessOfCommitThreads() throws Exception {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(16);

    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();

    AtomicBoolean stopped = new AtomicBoolean(false);
    CompletableFuture<Boolean> third =
        CompletableFuture.supplyAsync(() -> limiter.acquire(stopped::get));
    Thread.sleep(300);
    assertThat(third).isNotDone();

    limiter.release();

    assertThat(third.get(5, TimeUnit.SECONDS)).isTrue();
    assertThat(limiter.availablePermits()).isZero();

    limiter.release();
    limiter.release();
    assertThat(limiter.availablePermits()).isEqualTo(2);
  }

  @Test
  public void permitsAreFair() {
    assertThat(new StatisticsLagStepLimiter(4).isFair()).isTrue();
  }

  /**
   * The order holds only within one try of {@code acquire}: a waiter whose try runs out queues
   * again behind the other one. Here a try outlasts the test timeout, so neither waiter leaves the
   * queue before the release. That a newcomer cannot overtake the queued threads is checked by
   * {@link #permitsAreFair()}.
   */
  @Test
  @Timeout(20)
  public void releaseGoesToHeadOfQueue() throws Exception {
    StatisticsLagStepLimiter limiter =
        new StatisticsLagStepLimiter(8, TimeUnit.SECONDS.toMillis(60));
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();

    CompletableFuture<Boolean> first =
        CompletableFuture.supplyAsync(() -> limiter.acquire(NOT_STOPPED));
    awaitQueued(limiter, 1);
    CompletableFuture<Boolean> second =
        CompletableFuture.supplyAsync(() -> limiter.acquire(NOT_STOPPED));
    awaitQueued(limiter, 2);
    assertThat(first).isNotDone();
    assertThat(second).isNotDone();

    limiter.release();

    assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(300);
    assertThat(second).isNotDone();

    limiter.release();

    assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
    limiter.release();
    limiter.release();
    assertThat(limiter.availablePermits()).isEqualTo(2);
  }

  private static void awaitQueued(StatisticsLagStepLimiter limiter, int count)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (limiter.queuedThreads() < count && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }

    assertThat(limiter.queuedThreads()).isEqualTo(count);
  }

  @Test
  public void commitThreadsOfOneStillHasTwoPermits() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(1);

    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.availablePermits()).isZero();
  }

  @Test
  public void neverMoreThanTwoHoldPermits() throws Exception {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);
    AtomicInteger running = new AtomicInteger();
    AtomicInteger maxRunning = new AtomicInteger();
    int steps = 6;
    CountDownLatch done = new CountDownLatch(steps);

    for (int step = 0; step < steps; step += 1) {
      CompletableFuture.runAsync(
          () -> {
            if (limiter.acquire(NOT_STOPPED)) {
              try {
                maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                running.decrementAndGet();
                limiter.release();
              }
            }

            done.countDown();
          },
          runnable -> new Thread(runnable).start());
    }

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(maxRunning.get()).isLessThanOrEqualTo(2);
    assertThat(limiter.availablePermits()).isEqualTo(2);
  }

  @Test
  public void slotsAndPermitsAreIndependent() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(8);

    limiter.enter(TABLE_A);
    limiter.enter(TABLE_B);
    limiter.enter(TABLE_C);
    limiter.enter(TABLE_D);
    limiter.enter(TABLE_E);

    assertThat(limiter.freeSlots()).isZero();
    assertThat(limiter.availablePermits()).isEqualTo(2);
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.freeSlots()).isZero();
  }

  @Test
  public void alreadyStoppedReturnsWithoutPermitEvenIfOneIsFree() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(4);

    assertThat(limiter.acquire(() -> true)).isFalse();

    assertThat(limiter.availablePermits()).isEqualTo(2);
  }

  @Test
  @Timeout(10)
  public void stopWhileWaitingReturnsWithoutPermit() throws Exception {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(4);
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    AtomicBoolean stopped = new AtomicBoolean(false);

    CompletableFuture<Boolean> waiting =
        CompletableFuture.supplyAsync(() -> limiter.acquire(stopped::get));
    Thread.sleep(300);
    assertThat(waiting).isNotDone();

    long stoppedAt = System.nanoTime();
    stopped.set(true);

    assertThat(waiting.get(5, TimeUnit.SECONDS)).isFalse();
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedAt)).isLessThan(2_500);

    limiter.release();
    limiter.release();
    assertThat(limiter.availablePermits()).isEqualTo(2);
  }

  @Test
  @Timeout(10)
  public void interruptWhileWaitingReturnsWithoutPermitAndKeepsFlag() throws Exception {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(4);
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    assertThat(limiter.acquire(NOT_STOPPED)).isTrue();
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    AtomicBoolean interruptedAfter = new AtomicBoolean(false);

    Thread waiter =
        new Thread(
            () -> {
              result.complete(limiter.acquire(NOT_STOPPED));
              interruptedAfter.set(Thread.currentThread().isInterrupted());
            });
    waiter.start();
    Thread.sleep(300);
    assertThat(result).isNotDone();

    waiter.interrupt();
    waiter.join(5_000);

    assertThat(result.get(1, TimeUnit.SECONDS)).isFalse();
    assertThat(interruptedAfter.get()).isTrue();

    limiter.release();
    limiter.release();
    assertThat(limiter.availablePermits()).isEqualTo(2);
  }

  @Test
  public void interruptedThreadDoesNotTakePermit() {
    StatisticsLagStepLimiter limiter = new StatisticsLagStepLimiter(4);

    Thread.currentThread().interrupt();
    try {
      assertThat(limiter.acquire(NOT_STOPPED)).isFalse();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }

    assertThat(limiter.availablePermits()).isEqualTo(2);
  }
}
