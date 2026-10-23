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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A writer's answer as the statistics step sees it: the snapshot it was committed in, the files to
 * read when its sketches do not cover a column, and its intermediate Puffin file.
 *
 * <p>A merge-on-read answer carries the data files of its commit, a copy-on-write answer the staged
 * change files of the change set; the other list is empty.
 */
public class StatisticsAnswer {
  private final long snapshotId;
  private final List<DataFile> dataFiles;
  private final List<StagedChangeFile> stagedFiles;
  private final SketchFile sketchFile;

  private StatisticsAnswer(
      long snapshotId,
      List<DataFile> dataFiles,
      List<StagedChangeFile> stagedFiles,
      SketchFile sketchFile) {
    this.snapshotId = snapshotId;
    this.dataFiles = dataFiles;
    this.stagedFiles = stagedFiles;
    this.sketchFile = sketchFile;
  }

  /** A merge-on-read answer. */
  public static StatisticsAnswer ofDataFiles(
      long snapshotId, List<DataFile> dataFiles, SketchFile sketchFile) {
    Preconditions.checkArgument(dataFiles != null, "Invalid data files: null");
    return new StatisticsAnswer(
        snapshotId, ImmutableList.copyOf(dataFiles), ImmutableList.of(), sketchFile);
  }

  /** A copy-on-write answer. */
  public static StatisticsAnswer ofStagedFiles(
      long snapshotId, List<StagedChangeFile> stagedFiles, SketchFile sketchFile) {
    Preconditions.checkArgument(stagedFiles != null, "Invalid staged files: null");
    return new StatisticsAnswer(
        snapshotId, ImmutableList.of(), ImmutableList.copyOf(stagedFiles), sketchFile);
  }

  /** The snapshot the answer was committed in: its commit in MoR, S in CoW. */
  public long snapshotId() {
    return snapshotId;
  }

  public List<DataFile> dataFiles() {
    return dataFiles;
  }

  public List<StagedChangeFile> stagedFiles() {
    return stagedFiles;
  }

  /** The intermediate Puffin file of the writer, or {@code null}: then nothing is covered. */
  public SketchFile sketchFile() {
    return sketchFile;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("dataFiles", dataFiles.size())
        .add("stagedFiles", stagedFiles.size())
        .add("sketchFile", sketchFile == null ? null : sketchFile.location())
        .toString();
  }
}
