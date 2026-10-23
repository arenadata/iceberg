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
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;

/** The change files a copy-on-write writer staged during one commit cycle. */
public class StagedChangesResult implements RecordWriteResult {

  private final TableReference tableReference;
  private final String taskId;
  private final List<StagedChangeFile> stagedFiles;
  private final Set<String> sourceTopics;

  public StagedChangesResult(
      TableReference tableReference,
      String taskId,
      List<StagedChangeFile> stagedFiles,
      Set<String> sourceTopics) {
    this.tableReference = tableReference;
    this.taskId = taskId;
    this.stagedFiles = stagedFiles;
    this.sourceTopics = sourceTopics;
  }

  @Override
  public Kind kind() {
    return Kind.STAGED_CHANGES;
  }

  @Override
  public TableReference tableReference() {
    return tableReference;
  }

  @Override
  public Set<String> sourceTopics() {
    return sourceTopics;
  }

  public String taskId() {
    return taskId;
  }

  public List<StagedChangeFile> stagedFiles() {
    return stagedFiles;
  }
}
