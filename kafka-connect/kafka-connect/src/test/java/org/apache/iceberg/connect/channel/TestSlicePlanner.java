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
import java.util.NavigableMap;
import java.util.function.IntFunction;
import java.util.function.ToLongFunction;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

/**
 * The byte quota's search over prefixes of a slice, with a prefix's plan standing for the bytes it
 * rewrites: {@code Long} in place of a {@code PlanResult}.
 */
public class TestSlicePlanner {

  private static final ToLongFunction<Long> BYTES = Long::longValue;

  private static final int SLICE_KEYS = 64;

  // ceil(log2(64)): the whole slice is planned before the search, the rest bisects 64 keys
  private static final int MAX_SEARCH_PLANS = 6;

  @Test
  public void testLargestPrefixWithinTakesAPrefixThatPlansExactlyTheQuota() {
    PlannedPrefixes prefixes = new PlannedPrefixes(keys -> 10L * keys);

    assertThat(prefixes.largestWithin(50)).isEqualTo(5);
    assertThat(prefixes.plans).containsEntry(5, 50L).containsEntry(6, 60L);
  }

  @Test
  public void testLargestPrefixWithinIsZeroWhenNotEvenOneKeyFits() {
    PlannedPrefixes prefixes = new PlannedPrefixes(keys -> 100L + keys);

    assertThat(prefixes.largestWithin(50)).isZero();
    // the one-key prefix is planned on the way down: the second search starts from its bytes
    assertThat(prefixes.plans).containsEntry(1, 101L);
    assertThat(prefixes.requested).hasSizeLessThanOrEqualTo(MAX_SEARCH_PLANS);
  }

  @Test
  public void testLargestPrefixWithinPlansNoMoreThanLogOfTheSliceKeysTimes() {
    for (long quota = 0; quota <= 10L * SLICE_KEYS + 10; quota += 1) {
      PlannedPrefixes prefixes = new PlannedPrefixes(keys -> 10L * keys);

      int keys = prefixes.largestWithin(quota);

      assertThat(keys).as("quota %s", quota).isEqualTo((int) Math.min(quota / 10, SLICE_KEYS));
      assertThat(prefixes.requested)
          .as("quota %s", quota)
          .hasSizeLessThanOrEqualTo(MAX_SEARCH_PLANS);
    }
  }

  @Test
  public void testLargestPrefixWithinSearchesOnlyBetweenThePrefixesAlreadyPlanned() {
    PlannedPrefixes prefixes = new PlannedPrefixes(keys -> 10L * keys);
    prefixes.plans.put(4, 40L);
    prefixes.plans.put(8, 80L);

    assertThat(prefixes.largestWithin(65)).isEqualTo(6);
    assertThat(prefixes.requested).isNotEmpty().allMatch(keys -> keys > 4 && keys < 8);
    assertThat(prefixes.requested).hasSizeLessThanOrEqualTo(2);
  }

  @Test
  public void testLargestPrefixWithinKeepsPlansMonotoneAcrossASecondSearch() {
    // the first key alone plans over the quota, and so do the next nine: they touch the same file
    PlannedPrefixes prefixes = new PlannedPrefixes(keys -> keys <= 10 ? 100L : 100L + keys);

    assertThat(prefixes.largestWithin(50)).isZero();
    long oneKeyBytes = prefixes.plans.get(1);
    int requestedByTheFirstSearch = prefixes.requested.size();

    assertThat(prefixes.largestWithin(oneKeyBytes)).isEqualTo(10);
    // every prefix planned is within the second quota up to the answer, and over it past the answer
    prefixes.plans.forEach(
        (keys, bytes) ->
            assertThat(bytes <= oneKeyBytes).as("keys %s", keys).isEqualTo(keys <= 10));
    assertThat(prefixes.plans).containsKeys(10, 11);
    assertThat(prefixes.requested.subList(requestedByTheFirstSearch, prefixes.requested.size()))
        .hasSizeLessThanOrEqualTo(MAX_SEARCH_PLANS);
  }

  /** A slice of {@link #SLICE_KEYS} keys with the whole of it planned, as the search starts. */
  private static class PlannedPrefixes {
    private final IntFunction<Long> bytesOfPrefix;
    private final NavigableMap<Integer, Long> plans = Maps.newTreeMap();
    private final List<Integer> requested = Lists.newArrayList();

    PlannedPrefixes(IntFunction<Long> bytesOfPrefix) {
      this.bytesOfPrefix = bytesOfPrefix;
      plans.put(SLICE_KEYS, bytesOfPrefix.apply(SLICE_KEYS));
    }

    int largestWithin(long maxBytes) {
      return SlicePlanner.largestPrefixWithin(
          SLICE_KEYS,
          plans,
          BYTES,
          maxBytes,
          keys -> {
            requested.add(keys);
            return bytesOfPrefix.apply(keys);
          });
    }
  }
}
