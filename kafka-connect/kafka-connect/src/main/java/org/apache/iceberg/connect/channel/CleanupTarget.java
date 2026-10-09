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

import java.util.UUID;
import org.apache.iceberg.connect.events.TableReference;

/**
 * One table's entry to a pass of the staging cleanup, as its {@link StagingCleanupSource} read it
 * on the coordinator thread: which table, where its staging is, what part of it to clean up, and
 * what is reachable in memory at that moment. Nothing in it changes after {@link
 * StagingCleanupSource#snapshot} returns it.
 */
final class CleanupTarget {

  private final String tableName;
  private final UUID tableUuid;
  private final String branch;
  private final TableReference tableReference;
  private final String stagingLocation;
  private final String cleanupPrefix;
  private final ReachableFileSet inMemory;

  /**
   * @param tableUuid the UUID a fresh load must still have, or the table was replaced
   * @param branch the branch of the table's commits, or {@code null} if the source needs none
   * @param cleanupPrefix what is listed and cleaned up: inside {@code
   *     StagedChangeFileWriter.stagingDirectory(stagingLocation, tableReference, connectGroupId)}
   */
  CleanupTarget(
      String tableName,
      UUID tableUuid,
      String branch,
      TableReference tableReference,
      String stagingLocation,
      String cleanupPrefix,
      ReachableFileSet inMemory) {
    this.tableName = tableName;
    this.tableUuid = tableUuid;
    this.branch = branch;
    this.tableReference = tableReference;
    this.stagingLocation = stagingLocation;
    this.cleanupPrefix = cleanupPrefix;
    this.inMemory = inMemory;
  }

  String tableName() {
    return tableName;
  }

  UUID tableUuid() {
    return tableUuid;
  }

  String branch() {
    return branch;
  }

  TableReference tableReference() {
    return tableReference;
  }

  String stagingLocation() {
    return stagingLocation;
  }

  String cleanupPrefix() {
    return cleanupPrefix;
  }

  ReachableFileSet inMemory() {
    return inMemory;
  }
}
