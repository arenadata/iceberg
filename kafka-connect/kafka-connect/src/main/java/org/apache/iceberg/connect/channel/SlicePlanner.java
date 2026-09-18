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
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.ToLongFunction;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.IdentifierFields;
import org.apache.iceberg.connect.data.copyonwrite.AffectedFilePlanner;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetNormalizer;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.data.copyonwrite.PlanResult;
import org.apache.iceberg.connect.data.copyonwrite.PlanSummary;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;

/**
 * Normalizes the next slice of a change set and plans it within the byte quota. Reads the table and
 * the staged files, writes nothing: what it finds goes back to the caller, which owns the drain
 * state and the warnings about it.
 */
final class SlicePlanner {

  private final IcebergSinkConfig config;

  SlicePlanner(IcebergSinkConfig config) {
    this.config = config;
  }

  Set<Integer> identifierFieldIds(Table table, TableReference tableReference) {
    try {
      return IdentifierFields.resolveForCopyOnWrite(table, tableReference, config);
    } catch (ConfigException e) {
      // the writers refused such a table before staging a row, so it changed after they wrote:
      // its identifier fields were dropped or altered. No later cycle changes them back
      throw new PermanentCopyOnWriteException(e.getMessage());
    }
  }

  /**
   * Normalizes the next slice of the frozen change set, after {@code cursor}.
   *
   * <p>A staged file that is gone stays gone: retrying would fail every cycle while the table holds
   * the whole connector's control topic offsets, so the table is stopped once, with the remedy. Any
   * other read failure may pass and is left to the ordinary retry.
   */
  ChangeSetSlice normalize(
      Table table, ChangeSetManifest manifest, StructLike cursor, TableReference tableReference) {
    long maxRecords = config.copyOnWriteMaxChangeSetRecords();
    try {
      return ChangeSetNormalizer.normalize(
          table, manifest.identifierFieldIds(), manifest.stagedFiles(), cursor, maxRecords);
    } catch (NotFoundException e) {
      throw new PermanentCopyOnWriteException(
          String.format(
              "change set %s of table %s cannot be applied: %s. Restore the file (object "
                  + "versioning, a backup) and restart the connector. If it cannot be restored, the "
                  + "changes it held are lost -- the offsets of the records behind them were "
                  + "committed when the change set was frozen, so no restart reads them again",
              manifest.changeSetId(), tableReference.identifier(), e.getMessage()));
    }
  }

  /**
   * Normalizes the next slice and plans the largest prefix of it whose rewrite fits {@code
   * copy-on-write.max-rewrite-bytes}.
   *
   * <p>The plan itself is never truncated: dropping a planned file would leave the old version of a
   * key beside the new one, so the quota is met with fewer keys, replanned, and the rest moves to
   * the next slice. The slice is normalized once and only its prefixes are planned: a prefix's plan
   * is no bigger than the slice's, so the largest one within the quota is found by bisecting on the
   * number of keys. Bytes are not proportional to keys: one key can pull in a 512 MB file and the
   * next thousand none, so the prefix is searched for, not scaled from the overshoot.
   *
   * <p>A slice whose first key alone plans over the quota still has to go for the drain to move. It
   * goes out as the largest prefix that plans no more than that one key: fewer keys would rewrite
   * the same files once per slice for nothing: the whole table, when the identifier columns have no
   * bounds. The caller warns about the table once per change set.
   */
  SlicePlan planWithinByteQuota(
      Table table,
      long baseSnapshotId,
      ChangeSetManifest manifest,
      StructLike cursor,
      TableReference tableReference) {
    ChangeSetSlice slice = normalize(table, manifest, cursor, tableReference);
    long maxRewriteBytes = config.copyOnWriteMaxRewriteBytes();
    NavigableMap<Integer, PlanResult> plans = Maps.newTreeMap();
    plans.put(slice.size(), plan(table, baseSnapshotId, slice));

    IntFunction<PlanResult> planPrefix = prefix -> plan(table, baseSnapshotId, slice.head(prefix));
    int keys =
        largestPrefixWithin(
            slice.size(), plans, SlicePlanner::rewriteBytes, maxRewriteBytes, planPrefix);
    boolean firstKeyOverQuota = keys == 0 && !slice.isEmpty();
    if (firstKeyOverQuota) {
      // the search ends having planned the one-key prefix, and nothing fewer plans less than it
      long oneKeyBytes = plans.get(1).summary().totalRewriteBytes();
      keys =
          largestPrefixWithin(
              slice.size(), plans, SlicePlanner::rewriteBytes, oneKeyBytes, planPrefix);
    }

    return new SlicePlan(
        slice.isEmpty() ? slice : slice.head(keys),
        plans.get(keys),
        keys,
        slice.size(),
        plans.get(slice.size()).summary().totalRewriteBytes(),
        maxRewriteBytes,
        plans.size(),
        firstKeyOverQuota);
  }

  /**
   * The most keys a prefix of a slice of {@code sliceSize} keys holds with its plan within {@code
   * maxBytes}, 0 if not even the first key's plan is. Every prefix planned goes into {@code plans},
   * by its number of keys. Plans do not shrink as keys are added, so each prefix planned before
   * already bounds the answer from one side, and the search bisects only what lies between.
   */
  @VisibleForTesting
  static <P> int largestPrefixWithin(
      int sliceSize,
      NavigableMap<Integer, P> plans,
      ToLongFunction<P> bytes,
      long maxBytes,
      IntFunction<P> planPrefix) {
    int within = 0;
    int over = sliceSize + 1;
    for (Map.Entry<Integer, P> planned : plans.entrySet()) {
      if (bytes.applyAsLong(planned.getValue()) <= maxBytes) {
        within = Math.max(within, planned.getKey());
      } else {
        over = Math.min(over, planned.getKey());
      }
    }

    while (over - within > 1) {
      int prefix = within + (over - within) / 2;
      P plan = planPrefix.apply(prefix);
      plans.put(prefix, plan);
      if (bytes.applyAsLong(plan) <= maxBytes) {
        within = prefix;
      } else {
        over = prefix;
      }
    }

    return within;
  }

  private PlanResult plan(Table table, long baseSnapshotId, ChangeSetSlice slice) {
    return AffectedFilePlanner.plan(
        table,
        baseSnapshotId,
        slice,
        config.copyOnWritePruningMaxInCardinality(),
        config.copyOnWriteMaxRewriteBytes());
  }

  private static long rewriteBytes(PlanResult plan) {
    return plan.summary().totalRewriteBytes();
  }

  /** A slice cut to the byte quota, with the plan of what it rewrites. */
  static final class SlicePlan {

    private final ChangeSetSlice slice;
    private final PlanResult plan;
    private final int keys;
    private final int normalizedKeys;
    private final long normalizedBytes;
    private final long quotaBytes;
    private final int plansMade;
    private final boolean firstKeyOverQuota;

    private SlicePlan(
        ChangeSetSlice slice,
        PlanResult plan,
        int keys,
        int normalizedKeys,
        long normalizedBytes,
        long quotaBytes,
        int plansMade,
        boolean firstKeyOverQuota) {
      this.slice = slice;
      this.plan = plan;
      this.keys = keys;
      this.normalizedKeys = normalizedKeys;
      this.normalizedBytes = normalizedBytes;
      this.quotaBytes = quotaBytes;
      this.plansMade = plansMade;
      this.firstKeyOverQuota = firstKeyOverQuota;
    }

    ChangeSetSlice slice() {
      return slice;
    }

    List<FileScanTask> planFiles() {
      return plan.fileScanTasks();
    }

    Expression conflictFilter() {
      return plan.conflictDetectionFilter();
    }

    PlanSummary summary() {
      return plan.summary();
    }

    int keys() {
      return keys;
    }

    int normalizedKeys() {
      return normalizedKeys;
    }

    long normalizedBytes() {
      return normalizedBytes;
    }

    long quotaBytes() {
      return quotaBytes;
    }

    int plansMade() {
      return plansMade;
    }

    boolean firstKeyOverQuota() {
      return firstKeyOverQuota;
    }
  }
}
