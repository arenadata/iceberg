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
package org.apache.iceberg.connect.data;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/** What a {@link StatisticsStep} runs on. */
public final class StatisticsStepInput {
  private final Table table;
  private final TableIdentifier identifier;
  private final Long snapshotId;
  private final Set<Long> ownSnapshotIds;
  private final Set<String> ownChangeSetIds;
  private final List<StatisticsAnswer> answers;
  private final UUID commitId;
  private final BooleanSupplier stopped;
  private final boolean keepSketchesOnFailure;
  private final IcebergSinkConfig config;
  private final TableSinkConfig tableConfig;
  private final StatisticsHealth health;

  private StatisticsStepInput(Builder builder) {
    this.table = builder.table;
    this.identifier = builder.identifier;
    this.snapshotId = builder.snapshotId;
    this.ownSnapshotIds = ImmutableSet.copyOf(builder.ownSnapshotIds);
    this.ownChangeSetIds = ImmutableSet.copyOf(builder.ownChangeSetIds);
    this.answers = ImmutableList.copyOf(builder.answers);
    this.commitId = builder.commitId;
    this.stopped = builder.stopped;
    this.keepSketchesOnFailure = builder.keepSketchesOnFailure;
    this.config = builder.config;
    this.tableConfig = builder.tableConfig;
    this.health = builder.health;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** The table, owned by the step while it runs: the step refreshes it. */
  public Table table() {
    return table;
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  /** S, or {@code null} for a lag step: then S is the head of the commit branch. */
  public Long snapshotId() {
    return snapshotId;
  }

  public Set<Long> ownSnapshotIds() {
    return ownSnapshotIds;
  }

  /** The change sets whose slices are own snapshots (copy-on-write). */
  public Set<String> ownChangeSetIds() {
    return ownChangeSetIds;
  }

  /** The answers of the own snapshots; the step deletes their intermediate Puffin files. */
  public List<StatisticsAnswer> answers() {
    return answers;
  }

  /** The id of the cycle, which goes into the name of the NDV file. */
  public UUID commitId() {
    return commitId;
  }

  /** Whether to stop the step; an interrupted thread stops it, too. */
  public BooleanSupplier stopped() {
    return stopped;
  }

  public boolean keepSketchesOnFailure() {
    return keepSketchesOnFailure;
  }

  public IcebergSinkConfig config() {
    return config;
  }

  public TableSinkConfig tableConfig() {
    return tableConfig;
  }

  public StatisticsHealth health() {
    return health;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identifier", identifier)
        .add("snapshotId", snapshotId)
        .add("ownSnapshotIds", ownSnapshotIds)
        .add("ownChangeSetIds", ownChangeSetIds)
        .add("answers", answers.size())
        .add("commitId", commitId)
        .add("keepSketchesOnFailure", keepSketchesOnFailure)
        .toString();
  }

  public static final class Builder {
    private Table table = null;
    private TableIdentifier identifier = null;
    private Long snapshotId = null;
    private Set<Long> ownSnapshotIds = ImmutableSet.of();
    private Set<String> ownChangeSetIds = ImmutableSet.of();
    private List<StatisticsAnswer> answers = ImmutableList.of();
    private UUID commitId = null;
    private BooleanSupplier stopped = null;
    private boolean keepSketchesOnFailure = false;
    private IcebergSinkConfig config = null;
    private TableSinkConfig tableConfig = null;
    private StatisticsHealth health = null;

    private Builder() {}

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder identifier(TableIdentifier newIdentifier) {
      this.identifier = newIdentifier;
      return this;
    }

    public Builder snapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    public Builder ownSnapshotIds(Set<Long> ids) {
      this.ownSnapshotIds = ids;
      return this;
    }

    public Builder ownChangeSetIds(Set<String> ids) {
      this.ownChangeSetIds = ids;
      return this;
    }

    public Builder answers(List<StatisticsAnswer> newAnswers) {
      this.answers = newAnswers;
      return this;
    }

    public Builder commitId(UUID newCommitId) {
      this.commitId = newCommitId;
      return this;
    }

    public Builder stopped(BooleanSupplier newStopped) {
      this.stopped = newStopped;
      return this;
    }

    public Builder keepSketchesOnFailure(boolean keep) {
      this.keepSketchesOnFailure = keep;
      return this;
    }

    public Builder config(IcebergSinkConfig newConfig) {
      this.config = newConfig;
      return this;
    }

    public Builder tableConfig(TableSinkConfig newTableConfig) {
      this.tableConfig = newTableConfig;
      return this;
    }

    public Builder health(StatisticsHealth newHealth) {
      this.health = newHealth;
      return this;
    }

    public StatisticsStepInput build() {
      Preconditions.checkArgument(table != null, "Invalid table: null");
      Preconditions.checkArgument(identifier != null, "Invalid table identifier: null");
      Preconditions.checkArgument(ownSnapshotIds != null, "Invalid own snapshot ids: null");
      Preconditions.checkArgument(ownChangeSetIds != null, "Invalid own change set ids: null");
      Preconditions.checkArgument(answers != null, "Invalid answers: null");
      Preconditions.checkArgument(commitId != null, "Invalid commit id: null");
      Preconditions.checkArgument(stopped != null, "Invalid stop flag: null");
      Preconditions.checkArgument(config != null, "Invalid config: null");
      Preconditions.checkArgument(tableConfig != null, "Invalid table config: null");
      Preconditions.checkArgument(health != null, "Invalid statistics health: null");
      return new StatisticsStepInput(this);
    }
  }
}
