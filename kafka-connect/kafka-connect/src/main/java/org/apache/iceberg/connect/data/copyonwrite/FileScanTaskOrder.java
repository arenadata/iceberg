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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;

/**
 * {@code (specId, partition)} ordering shared by {@link RewriteAssigner} (keeps one task's files
 * clustered for its {@code ClusteredDataWriter}) and {@link CopyOnWriteRewriter} (splits a plan
 * into clustered chunks for {@code rewrite-threads}).
 */
final class FileScanTaskOrder {

  private FileScanTaskOrder() {}

  /**
   * A comparator over {@code (specId, partition)}.
   *
   * <p>{@code tasks} only primes the per-spec comparators; the comparator builds one for a spec it
   * has not seen, so it stays correct for a task from outside that list.
   */
  static Comparator<FileScanTask> bySpecAndPartition(List<FileScanTask> tasks) {
    Map<Integer, Comparator<StructLike>> partitionComparators = Maps.newConcurrentMap();
    for (FileScanTask task : tasks) {
      comparatorFor(partitionComparators, task);
    }
    return (a, b) -> {
      int specCmp = Integer.compare(a.spec().specId(), b.spec().specId());
      if (specCmp != 0) {
        return specCmp;
      }
      return comparatorFor(partitionComparators, a)
          .compare(a.file().partition(), b.file().partition());
    };
  }

  private static Comparator<StructLike> comparatorFor(
      Map<Integer, Comparator<StructLike>> partitionComparators, FileScanTask task) {
    return partitionComparators.computeIfAbsent(
        task.spec().specId(), specId -> Comparators.forType(task.spec().partitionType()));
  }
}
