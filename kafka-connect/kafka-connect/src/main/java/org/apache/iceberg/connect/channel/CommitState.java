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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  /**
   * Buffered responses past which the coordinator warns, repeatedly.
   *
   * <p>In merge-on-read the buffer is one cycle deep. In copy-on-write a drain spans many cycles
   * and everything arriving meanwhile waits, so the buffer grows with the stream, and the control
   * topic offsets (hence restart replay) are held back to its oldest response; a table stopped by
   * {@code checkSingleMode} grows it forever. Not a cap: dropping a response loses records the
   * worker has already acknowledged to Kafka.
   */
  private static final int RESPONSE_BUFFER_WARN_THRESHOLD = 100_000;

  private final List<Envelope> commitBuffer = Lists.newArrayList();
  private final List<DataComplete> readyBuffer = Lists.newArrayList();
  private long startTime;
  private UUID currentCommitId;

  /**
   * Every commit id this coordinator issued, in copy-on-write only. A {@code DataWritten} under any
   * other is the tail of merge-on-read from before the switch, and is committed; one under an id
   * from here is refused. Kept for the coordinator's life, since a response may come any number of
   * cycles late: one id per cycle, until the next rebalance.
   */
  private final Set<UUID> issuedCommitIds = Sets.newHashSet();

  private final IcebergSinkConfig config;
  private int warnedAtBufferSize;

  CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit response when no commit in progress, this can happen during recovery. Commit ID: {}",
          commitId(envelope));
    }
    warnIfBufferIsGrowing();
  }

  private void warnIfBufferIsGrowing() {
    if (commitBuffer.size() < RESPONSE_BUFFER_WARN_THRESHOLD
        || commitBuffer.size() < warnedAtBufferSize * 2) {
      return;
    }

    warnedAtBufferSize = commitBuffer.size();
    LOG.warn(
        "Coordinator is holding {} uncommitted responses. They are held because the tables they "
            + "belong to have not committed them -- a copy-on-write drain that is slower than its "
            + "input, or a table whose commit keeps failing. The control topic offsets are held "
            + "back with them, so this bounds both heap and restart time. Check for a table that "
            + "is not committing; if it is a drain, raise iceberg.control.commit.interval-ms or "
            + "lower iceberg.tables.copy-on-write.max-change-set-records",
        commitBuffer.size());
  }

  void addReady(Envelope envelope) {
    DataComplete dataComplete = (DataComplete) envelope.event().payload();
    readyBuffer.add(dataComplete);
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit ready when no commit in progress, this can happen during recovery. Commit ID: {}",
          dataComplete.commitId());
    }
  }

  UUID currentCommitId() {
    return currentCommitId;
  }

  boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    if (config.isCopyOnWriteMode()) {
      issuedCommitIds.add(currentCommitId);
    }
    startTime = System.currentTimeMillis();
  }

  boolean isIssued(UUID commitId) {
    return issuedCommitIds.contains(commitId);
  }

  void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  List<Envelope> clearResponses(Collection<Envelope> consumed) {
    Set<Envelope> spent = Collections.newSetFromMap(new IdentityHashMap<>());
    spent.addAll(consumed);
    commitBuffer.removeIf(spent::contains);
    if (commitBuffer.size() < RESPONSE_BUFFER_WARN_THRESHOLD) {
      // drained back to normal: the next growth spurt should be reported from scratch
      warnedAtBufferSize = 0;
    }
    return ImmutableList.copyOf(commitBuffer);
  }

  /**
   * A view of the buffered responses, for a committer that has to look at what is in flight.
   *
   * <p>Not a copy: the buffer runs to hundreds of thousands of envelopes during a slow
   * copy-on-write drain, and its one reader, the staging cleanup, asks about once an hour. Read on
   * the coordinator thread only, which is also the only thread that mutates the buffer.
   */
  Collection<Envelope> bufferedResponses() {
    return Collections.unmodifiableList(commitBuffer);
  }

  /**
   * Every task that reported in for the current commit, mapped to the partitions it owned: the
   * executors available to a copy-on-write rewrite. Taken from {@code DataComplete}, the one event
   * every live task sends every cycle, since a task that landed no rows still takes a rewrite.
   */
  Map<String, List<TopicPartitionRef>> activeTasks() {
    Map<String, List<TopicPartitionRef>> activeTasks = Maps.newHashMap();
    readyBuffer.stream()
        .filter(payload -> payload.commitId().equals(currentCommitId))
        .filter(payload -> payload.taskId() != null)
        .forEach(
            payload ->
                activeTasks.put(
                    payload.taskId(),
                    payload.assignments().stream()
                        .map(tp -> new TopicPartitionRef(tp.topic(), tp.partition()))
                        .collect(Collectors.toList())));
    return activeTasks;
  }

  boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    if (System.currentTimeMillis() - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached. Commit ID: {}", currentCommitId);
      return true;
    }
    return false;
  }

  boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  Map<TableReference, List<Envelope>> tableCommitMap() {
    return commitBuffer.stream().collect(Collectors.groupingBy(CommitState::tableReference));
  }

  private static TableReference tableReference(Envelope envelope) {
    Payload payload = envelope.event().payload();
    if (payload.type() == PayloadType.ROW_CHANGES_WRITTEN) {
      return ((RowChangesWritten) payload).tableReference();
    }
    return ((DataWritten) payload).tableReference();
  }

  static UUID commitId(Envelope envelope) {
    Payload payload = envelope.event().payload();
    if (payload.type() == PayloadType.ROW_CHANGES_WRITTEN) {
      return ((RowChangesWritten) payload).commitId();
    }
    return ((DataWritten) payload).commitId();
  }

  OffsetDateTime validThroughTs(boolean partialCommit) {
    boolean hasValidThroughTs =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

    OffsetDateTime result;
    if (hasValidThroughTs) {
      result =
          readyBuffer.stream()
              .flatMap(event -> event.assignments().stream())
              .map(TopicPartitionOffset::timestamp)
              .min(Comparator.naturalOrder())
              .orElse(null);
    } else {
      result = null;
    }
    return result;
  }
}
