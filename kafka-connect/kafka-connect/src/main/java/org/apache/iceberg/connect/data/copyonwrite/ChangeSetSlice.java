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
package org.apache.iceberg.connect.data.copyonwrite;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.NestedField;

/**
 * One normalized, quota-bounded slice of a change set: the smallest {@code N} identifier keys
 * strictly after the cursor, each collapsed to its final state.
 *
 * <p>Produced by {@link ChangeSetNormalizer}, consumed by {@link AffectedFilePlanner} (to build the
 * pruning predicate) and by {@link CopyOnWriteRewriter} (to decide, per row, whether it survives).
 */
public final class ChangeSetSlice {

  private final List<NestedField> identifierFields;
  private final NavigableMap<StructLike, ChangeRecord> changes;
  private final boolean truncated;
  private final long estimatedRetainedBytes;

  ChangeSetSlice(
      List<NestedField> identifierFields,
      NavigableMap<StructLike, ChangeRecord> changes,
      boolean truncated,
      long estimatedRetainedBytes) {
    this.identifierFields = identifierFields;
    this.changes = changes;
    this.truncated = truncated;
    this.estimatedRetainedBytes = estimatedRetainedBytes;
  }

  /** Identifier fields, in the fixed order every key in this slice is built with. */
  public List<NestedField> identifierFields() {
    return identifierFields;
  }

  /** The slice's keys, each mapped to its final state, in ascending key order. */
  public NavigableMap<StructLike, ChangeRecord> changes() {
    return changes;
  }

  /** True if the change set had more keys after the cursor than this slice could hold. */
  public boolean truncated() {
    return truncated;
  }

  public int size() {
    return changes.size();
  }

  /** Roughly how much this slice occupies, in staged-file bytes. */
  public long estimatedRetainedBytes() {
    return estimatedRetainedBytes;
  }

  public boolean isEmpty() {
    return changes.isEmpty();
  }

  /**
   * The first {@code count} keys of this slice: what {@link ChangeSetNormalizer#normalize} returns
   * with a record quota of {@code count}, truncated whenever a key is left out. A view over this
   * slice's keys, cut in memory without reading a staged file again.
   *
   * <p>This is how the byte quota shrinks a slice: fewer keys plan no more files, so the largest
   * prefix within the quota is searched for among prefixes, not by normalizing with smaller quotas.
   */
  public ChangeSetSlice head(int count) {
    Preconditions.checkArgument(count > 0, "A prefix holds at least one key: %s", count);
    if (count >= changes.size()) {
      return this;
    }

    Iterator<StructLike> keys = changes.keySet().iterator();
    StructLike last = keys.next();
    for (int taken = 1; taken < count; taken++) {
      last = keys.next();
    }

    // the estimate is the average record size times the keys held, so it scales with them
    return new ChangeSetSlice(
        identifierFields,
        changes.headMap(last, true),
        true,
        estimatedRetainedBytes / changes.size() * count);
  }

  Optional<ChangeRecord> get(StructLike key) {
    return Optional.ofNullable(changes.get(key));
  }

  /**
   * The keys one rewrite owner is responsible for emitting: a contiguous block of this slice's
   * ascending key order ({@code [i*K/n, (i+1)*K/n)}).
   *
   * <p>Every worker of a slice derives the same blocks from the same normalized file and the same
   * list of active tasks, so the blocks partition the slice's keys: each key has exactly one owner.
   * That is what lets a worker decide on its own whether to write a key's final row, without
   * knowing which files (if any) held the key before. Contiguous rather than round robin: adjacent
   * keys more often share a partition, so the owner's final pass writes fewer files.
   */
  List<Map.Entry<StructLike, ChangeRecord>> ownedEntries(int ownerIndex, int ownerCount) {
    Preconditions.checkArgument(ownerCount > 0, "Owner count must be positive: %s", ownerCount);
    Preconditions.checkArgument(
        ownerIndex >= 0 && ownerIndex < ownerCount,
        "Owner index %s out of range for %s owner(s)",
        ownerIndex,
        ownerCount);

    int size = changes.size();
    int from = (int) ((long) ownerIndex * size / ownerCount);
    int to = (int) ((long) (ownerIndex + 1) * size / ownerCount);
    if (from >= to) {
      return ImmutableList.of();
    }

    List<Map.Entry<StructLike, ChangeRecord>> entries = Lists.newArrayList(changes.entrySet());
    return entries.subList(from, to);
  }

  /** The last (greatest) key in the slice, the new cursor once this slice commits. */
  public Optional<StructLike> lastKey() {
    return changes.isEmpty() ? Optional.empty() : Optional.of(changes.lastKey());
  }

  /** Distinct values a given identifier field takes across every key in the slice. */
  Set<Object> distinctValues(NestedField identifierField) {
    int pos = identifierFields.indexOf(identifierField);
    Set<Object> values = Sets.newLinkedHashSetWithExpectedSize(changes.size());
    for (StructLike key : changes.keySet()) {
      values.add(key.get(pos, Object.class));
    }
    return values;
  }
}
