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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Detects two statistics steps of one (table, S) running at once. S is the snapshot of the input,
 * or for a lag step, which has none, the table head given to the step. A step whose key is already
 * running is added to {@link #overlaps()}, which the tests check to be empty in {@code @AfterEach}.
 *
 * <p>Only steps on threads of their own can overlap: with inline steps, or steps run one after
 * another off a held pool, an empty {@link #overlaps()} proves nothing, and a lost lock or guard
 * shows only in what the steps leave behind, such as more than one record of a snapshot.
 */
final class StepOverlaps {
  private final Map<String, AtomicInteger> running = Maps.newConcurrentMap();
  private final List<String> overlaps = new CopyOnWriteArrayList<>();

  <T> T track(StatisticsStepInput input, Supplier<T> step) {
    String key = input.identifier() + " at " + snapshotOf(input);
    AtomicInteger count = running.computeIfAbsent(key, k -> new AtomicInteger());
    if (count.incrementAndGet() > 1) {
      overlaps.add(key);
    }

    try {
      return step.get();
    } finally {
      count.decrementAndGet();
    }
  }

  List<String> overlaps() {
    return overlaps;
  }

  private static String snapshotOf(StatisticsStepInput input) {
    if (input.snapshotId() != null) {
      return String.valueOf(input.snapshotId());
    }

    Table table = input.table();
    Snapshot head = table == null ? null : table.currentSnapshot();
    return head == null ? "an empty head" : String.valueOf(head.snapshotId());
  }
}
