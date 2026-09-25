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

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** Everything a {@link TableCommitter} needs to commit one table for the current cycle. */
class TableCommitRequest {

  private final TableReference tableReference;
  private final List<Envelope> envelopes;
  private final Map<Integer, Long> controlTopicOffsets;
  private final UUID commitId;
  private final OffsetDateTime validThroughTs;
  private final Map<String, List<TopicPartitionRef>> activeTasks;

  TableCommitRequest(
      TableReference tableReference,
      List<Envelope> envelopes,
      Map<Integer, Long> controlTopicOffsets,
      UUID commitId,
      OffsetDateTime validThroughTs) {
    this(
        tableReference,
        envelopes,
        controlTopicOffsets,
        commitId,
        validThroughTs,
        ImmutableMap.of());
  }

  TableCommitRequest(
      TableReference tableReference,
      List<Envelope> envelopes,
      Map<Integer, Long> controlTopicOffsets,
      UUID commitId,
      OffsetDateTime validThroughTs,
      Map<String, List<TopicPartitionRef>> activeTasks) {
    this.tableReference = tableReference;
    this.envelopes = envelopes;
    this.controlTopicOffsets = controlTopicOffsets;
    this.commitId = commitId;
    this.validThroughTs = validThroughTs;
    this.activeTasks = activeTasks;
  }

  TableReference tableReference() {
    return tableReference;
  }

  List<Envelope> envelopes() {
    return envelopes;
  }

  Map<Integer, Long> controlTopicOffsets() {
    return controlTopicOffsets;
  }

  UUID commitId() {
    return commitId;
  }

  OffsetDateTime validThroughTs() {
    return validThroughTs;
  }

  Map<String, List<TopicPartitionRef>> activeTasks() {
    return activeTasks;
  }
}
