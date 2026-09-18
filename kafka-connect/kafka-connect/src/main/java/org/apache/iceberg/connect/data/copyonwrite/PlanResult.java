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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;

/** What {@link AffectedFilePlanner} found: the files a slice's rewrite has to touch, and why. */
public final class PlanResult {

  private final List<FileScanTask> fileScanTasks;
  private final PlanSummary summary;
  private final Expression conflictDetectionFilter;

  PlanResult(
      List<FileScanTask> fileScanTasks, PlanSummary summary, Expression conflictDetectionFilter) {
    this.fileScanTasks = fileScanTasks;
    this.summary = summary;
    this.conflictDetectionFilter = conflictDetectionFilter;
  }

  public List<FileScanTask> fileScanTasks() {
    return fileScanTasks;
  }

  public PlanSummary summary() {
    return summary;
  }

  /**
   * The same predicate the plan was scanned with; the caller uses it, unchanged, as the commit's
   * {@code conflictDetectionFilter}. Returned rather than rebuilt so the two cannot drift apart.
   */
  public Expression conflictDetectionFilter() {
    return conflictDetectionFilter;
  }
}
