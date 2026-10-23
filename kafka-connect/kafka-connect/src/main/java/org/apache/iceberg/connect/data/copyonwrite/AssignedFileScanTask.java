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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A {@link FileScanTask} rebuilt on a worker from the descriptor the coordinator sent it.
 *
 * <p>{@code BaseFileScanTask} is not public API, and there is nothing to reuse from a scan here
 * anyway: the coordinator already did the planning, and what travels is its outcome. The task
 * always covers the whole file: {@code planFiles()} does not split, splitting happens in {@code
 * planTasks()}, which the planner never calls.
 *
 * <p>The residual (the part of a scan filter Iceberg could not prove from metadata and expects the
 * reader to evaluate per row) is {@link Expressions#alwaysTrue()} on purpose. The scan predicate
 * was a coarse "might hold a changed key" filter over identifier columns, chosen to select files
 * rather than rows, and {@code AffectedFileReader} deliberately does not apply it. Carrying a real
 * residual here would invite exactly the mistake of dropping untouched rows from the rewritten
 * remainder.
 */
public final class AssignedFileScanTask implements FileScanTask {

  private final DataFile file;
  private final List<DeleteFile> deletes;
  private final PartitionSpec spec;

  private AssignedFileScanTask(DataFile file, List<DeleteFile> deletes, PartitionSpec spec) {
    this.file = file;
    this.deletes = deletes;
    this.spec = spec;
  }

  /** Rebuilds one assignment's files against the table's specs. */
  public static List<FileScanTask> from(Table table, List<FileScanTaskDescriptor> descriptors) {
    Map<Integer, PartitionSpec> specs = table.specs();
    ImmutableList.Builder<FileScanTask> tasks = ImmutableList.builder();
    for (FileScanTaskDescriptor descriptor : descriptors) {
      PartitionSpec spec = specs.get(descriptor.specId());
      Preconditions.checkArgument(
          spec != null,
          "Cannot rewrite %s: partition spec %s is not known to table %s",
          descriptor.dataFile().location(),
          descriptor.specId(),
          table.name());
      List<DeleteFile> deletes =
          descriptor.deleteFiles() == null
              ? ImmutableList.of()
              : ImmutableList.copyOf(descriptor.deleteFiles());
      tasks.add(new AssignedFileScanTask(descriptor.dataFile(), deletes, spec));
    }
    return tasks.build();
  }

  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public List<DeleteFile> deletes() {
    return deletes;
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return Expressions.alwaysTrue();
  }

  @Override
  public Iterable<FileScanTask> split(long targetSplitSize) {
    return ImmutableList.of(this);
  }
}
