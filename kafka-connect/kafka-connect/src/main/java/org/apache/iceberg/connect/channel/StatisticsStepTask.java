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
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.data.StatisticsAnswer;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A task of the merge-on-read statistics step of one table: the step of snapshot S with the own
 * snapshots O and their answers, or a lag step. Immutable.
 */
class StatisticsStepTask {
  private final TableReference tableReference;
  private final Table table;
  private final Long snapshotId;
  private final UUID commitId;
  private final Set<Long> ownSnapshotIds;
  private final long lagToken;
  private final List<StatisticsAnswer> answers;

  /**
   * @param table the table of {@code commit()}, held until the task ends: its {@code FileIO}
   *     deletes the intermediate Puffin files when the task cannot load its own table, and a
   *     catalog that vends credentials per table may close that {@code FileIO} once the table is
   *     collected; {@code null} for a lag step
   * @param snapshotId S, or {@code null} for a lag step
   * @param lagToken the generation of the table's lag taken when the task was set, 0 if none
   */
  StatisticsStepTask(
      TableReference tableReference,
      Table table,
      Long snapshotId,
      UUID commitId,
      Set<Long> ownSnapshotIds,
      long lagToken,
      List<StatisticsAnswer> answers) {
    Preconditions.checkArgument(tableReference != null, "Invalid table reference: null");
    Preconditions.checkArgument(commitId != null, "Invalid commit id: null");
    Preconditions.checkArgument(ownSnapshotIds != null, "Invalid own snapshot ids: null");
    Preconditions.checkArgument(answers != null, "Invalid answers: null");
    this.tableReference = tableReference;
    this.table = table;
    this.snapshotId = snapshotId;
    this.commitId = commitId;
    this.ownSnapshotIds = ImmutableSet.copyOf(ownSnapshotIds);
    this.lagToken = lagToken;
    this.answers = ImmutableList.copyOf(answers);
  }

  /** A lag step: S is the head of the commit branch when the step runs, O and answers empty. */
  static StatisticsStepTask lagStep(TableReference tableReference, UUID commitId, long lagToken) {
    return new StatisticsStepTask(
        tableReference, null, null, commitId, ImmutableSet.of(), lagToken, ImmutableList.of());
  }

  TableReference tableReference() {
    return tableReference;
  }

  Table table() {
    return table;
  }

  Long snapshotId() {
    return snapshotId;
  }

  UUID commitId() {
    return commitId;
  }

  Set<Long> ownSnapshotIds() {
    return ownSnapshotIds;
  }

  long lagToken() {
    return lagToken;
  }

  List<StatisticsAnswer> answers() {
    return answers;
  }

  Set<String> sketchLocations() {
    Set<String> locations = Sets.newLinkedHashSet();
    for (StatisticsAnswer answer : answers) {
      if (answer.sketchFile() != null) {
        locations.add(answer.sketchFile().location());
      }
    }

    return locations;
  }

  StatisticsStepTask mergedWith(StatisticsStepTask newer) {
    return new StatisticsStepTask(
        newer.tableReference,
        newer.table,
        newer.snapshotId,
        newer.commitId,
        Sets.union(ownSnapshotIds, newer.ownSnapshotIds),
        newer.lagToken,
        ImmutableList.<StatisticsAnswer>builder().addAll(answers).addAll(newer.answers).build());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table", tableReference.identifier())
        .add("snapshotId", snapshotId)
        .add("commitId", commitId)
        .add("ownSnapshotIds", ownSnapshotIds)
        .add("lagToken", lagToken)
        .add("answers", answers.size())
        .toString();
  }
}
