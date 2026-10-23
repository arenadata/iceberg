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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

/**
 * Limits the lag steps of one committer, merge-on-read or copy-on-write alike.
 *
 * <p><b>Slots</b> are the tables whose lag step is set and not over yet, including while it waits
 * for a permit or for the table's lock: at most {@code max(1, commitThreads / 2)}, so with two or
 * more commit threads at least half of the transition pool stays free of lag steps.
 *
 * <p><b>Permits</b>: of the lag steps in flight at most {@code PERMITS} run; the rest wait for a
 * permit of a fair semaphore in their own thread.
 */
class StatisticsLagStepLimiter {

  private static final int PERMITS = 2;
  private static final long WAIT_MILLIS = 1_000L;

  private final int slots;
  private final long waitMillis;
  private final Set<TableIdentifier> lagStepsInFlight = ConcurrentHashMap.newKeySet();
  private final Semaphore permits = new Semaphore(PERMITS, true);

  StatisticsLagStepLimiter(int commitThreads) {
    this(commitThreads, WAIT_MILLIS);
  }

  @VisibleForTesting
  StatisticsLagStepLimiter(int commitThreads, long waitMillis) {
    this.slots = Math.max(1, commitThreads / 2);
    this.waitMillis = waitMillis;
  }

  int freeSlots() {
    return Math.max(0, slots - lagStepsInFlight.size());
  }

  void enter(TableIdentifier id) {
    lagStepsInFlight.add(id);
  }

  void leave(TableIdentifier id) {
    lagStepsInFlight.remove(id);
  }

  boolean inFlight(TableIdentifier id) {
    return lagStepsInFlight.contains(id);
  }

  /**
   * Waits for a permit to run a lag step, in tries of {@code waitMillis}; between tries checks
   * {@code stopped} and the interruption of the thread. Fairness holds only within a try: a thread
   * whose try runs out queues again behind those that queued meanwhile.
   *
   * @return true with a permit held, which the caller releases with {@link #release()}; false
   *     without one when stopped or interrupted, keeping the interrupt flag
   */
  boolean acquire(BooleanSupplier stopped) {
    while (!stopped.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
      try {
        if (permits.tryAcquire(1, waitMillis, TimeUnit.MILLISECONDS)) {
          return true;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    return false;
  }

  void release() {
    permits.release();
  }

  int queuedThreads() {
    return permits.getQueueLength();
  }

  boolean isFair() {
    return permits.isFair();
  }

  int availablePermits() {
    return permits.availablePermits();
  }
}
