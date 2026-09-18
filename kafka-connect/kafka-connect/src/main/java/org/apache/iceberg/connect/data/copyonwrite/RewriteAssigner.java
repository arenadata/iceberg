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
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.StructType;

/**
 * Splits a plan into one non-overlapping file list per active task.
 *
 * <p>Runs on the coordinator. Distributes execution, never the decision of what gets rewritten: the
 * plan it slices up already came from {@link AffectedFilePlanner}.
 */
public final class RewriteAssigner {

  private RewriteAssigner() {}

  /**
   * Assigns every planned file to exactly one task.
   *
   * @param activeTasks every task that must receive an assignment, mapped to the topic partitions
   *     it owned when the assignment was planned (including a task with no partitions right now,
   *     since a task with nothing to do still has to be distinguishable from one that never
   *     answered)
   * @param wirePartitionType the partition type {@code RewriteAssigned} will be built with; every
   *     planned file has to fit it, which {@code AffectedFilePlanner} guarantees by refusing a plan
   *     with any file outside the table's current spec, not only one that spans specs
   */
  public static List<Assignment> assign(
      List<FileScanTask> plan,
      Map<String, List<TopicPartitionRef>> activeTasks,
      StructType wirePartitionType) {
    Preconditions.checkArgument(!activeTasks.isEmpty(), "Active task set cannot be empty");

    List<String> taskIds = Lists.newArrayList(activeTasks.keySet());
    taskIds.sort(Comparator.naturalOrder());

    Map<String, List<FileScanTask>> buckets = Maps.newLinkedHashMap();
    Map<String, Long> bucketBytes = Maps.newHashMap();
    for (String taskId : taskIds) {
      buckets.put(taskId, Lists.newArrayList());
      bucketBytes.put(taskId, 0L);
    }

    // largest processing time first: sorting by descending size before greedily filling the
    // currently-lightest bucket is what makes this LPT rather than plain round robin
    List<FileScanTask> bySizeDesc = Lists.newArrayList(plan);
    bySizeDesc.sort(
        Comparator.comparingLong((FileScanTask t) -> t.file().fileSizeInBytes()).reversed());

    for (FileScanTask task : bySizeDesc) {
      String lightest =
          taskIds.stream().min(Comparator.comparingLong(bucketBytes::get)).orElseThrow();
      buckets.get(lightest).add(task);
      bucketBytes.merge(lightest, task.file().fileSizeInBytes(), Long::sum);
    }

    Comparator<FileScanTask> clusterOrder = FileScanTaskOrder.bySpecAndPartition(plan);
    List<Assignment> assignments = Lists.newArrayList();
    for (String taskId : taskIds) {
      List<FileScanTask> tasks = buckets.get(taskId);
      tasks.sort(clusterOrder);
      List<FileScanTaskDescriptor> descriptors =
          tasks.stream()
              .map(task -> toDescriptor(task, wirePartitionType))
              .collect(ImmutableList.toImmutableList());
      assignments.add(new Assignment(taskId, activeTasks.get(taskId), descriptors));
    }
    return ImmutableList.copyOf(assignments);
  }

  private static FileScanTaskDescriptor toDescriptor(
      FileScanTask task, StructType wirePartitionType) {
    // rebuilt, not passed through: a file read from a manifest cannot be encoded as it stands
    return new FileScanTaskDescriptor(
        WireContentFiles.forWire(task.spec(), task.file()),
        task.deletes().stream()
            .map(delete -> WireContentFiles.forWire(task.spec(), delete))
            .collect(ImmutableList.toImmutableList()),
        task.spec().specId(),
        wirePartitionType);
  }
}
