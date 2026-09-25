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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.RecordWriteResult;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.data.StagedChangesResult;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Worker extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;
  private final RewriteAssignmentRunner assignmentRunner;

  /**
   * Chunks of an assignment still waiting for the rest, one per table. A plain map: only the poll
   * thread touches it, and {@link RewriteAssignmentRunner} gets an assignment only whole.
   */
  private final Map<TableReference, PartialAssignment> pendingAssignments = Maps.newHashMap();

  Worker(
      Catalog catalog,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
    this.assignmentRunner =
        config.isCopyOnWriteMode()
            ? new RewriteAssignmentRunner(catalog, config, this::send)
            : null;
  }

  void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.payload().type() == PayloadType.REWRITE_ASSIGNED) {
      return receiveRewriteAssigned((RewriteAssigned) event.payload());
    }
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }

    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    UUID commitId = ((StartCommit) event.payload()).commitId();

    List<Event> events =
        results.writerResults().stream()
            .map(writeResult -> toEvent(commitId, writeResult))
            .collect(Collectors.toList());

    // copy-on-write needs the task id to list rewrite executors. A coordinator one version behind
    // cannot decode a DataComplete carrying it, so merge-on-read, which has no use for it, leaves
    // it out and stays safe to upgrade task by task
    String taskId = config.isCopyOnWriteMode() ? config.taskId() : null;
    Event readyEvent =
        new Event(config.connectGroupId(), new DataComplete(commitId, assignments, taskId));
    events.add(readyEvent);

    send(events, results.sourceOffsets());

    return true;
  }

  /**
   * Takes on this task's share of a slice's rewrite, without blocking the poll thread.
   *
   * <p>Every active task is assigned, including one with no files: it still owns a block of the
   * slice's keys and still has to answer. A task that does not find itself in the message was not
   * active when the slice was planned, and stays out of it.
   */
  private boolean receiveRewriteAssigned(RewriteAssigned payload) {
    if (assignmentRunner == null) {
      return false;
    }

    // a newer slice of the table supersedes whatever this task still runs for it, whether or not
    // this task has a share in the new one: a task that missed the cycle's active tasks, or has
    // been rebalanced since, is exactly the one still holding an attempt the coordinator gave up on
    assignmentRunner.remember(payload);
    if (assignmentRunner.superseded(payload)) {
      // a message of an attempt already replaced: redelivered, or overtaken while it was read.
      // Nothing of it may start, and it must not be mixed into the chunks of the slice that
      // replaced it either
      return true;
    }

    PartialAssignment pending = dropChunksOfOlderSlices(payload);

    Assignment mine =
        payload.assignments().stream()
            .filter(assignment -> assignment.taskId().equals(config.taskId()))
            .findFirst()
            .orElse(null);
    if (mine == null) {
      return true;
    }

    if (payload.chunkCount() > 1) {
      if (pending == null || pending.chunkCount != payload.chunkCount()) {
        pending = new PartialAssignment(payload);
        pendingAssignments.put(payload.tableReference(), pending);
      }
      mine = pending.add(payload, mine);
      if (mine == null) {
        // an assignment is only an assignment whole: started on part of one, this task would
        // rewrite the files of the chunks it holds and answer for the slice, and the coordinator
        // would commit it with the keys of the missing files left beside their replacements
        return true;
      }
      pendingAssignments.remove(payload.tableReference());
    }

    // the active tasks were taken before this rewrite was planned; if this task's partitions have
    // changed since, it has been rebalanced. Dropping the assignment wastes nothing (the
    // coordinator times the slice out and replans) while acting on it would rewrite files someone
    // else may also hold
    Set<TopicPartition> expected =
        mine.expectedPartitions().stream()
            .map(ref -> new TopicPartition(ref.topic(), ref.partition()))
            .collect(Collectors.toSet());
    if (!expected.equals(Sets.newHashSet(context.assignment()))) {
      LOG.warn(
          "Ignoring stale rewrite assignment for table {}: partitions {} no longer match {}",
          payload.tableReference().identifier(),
          expected,
          context.assignment());
      return true;
    }

    assignmentRunner.submit(payload, mine);
    return true;
  }

  /**
   * Forgets the chunks held for a slice this message replaces, and returns what is left for it.
   *
   * <p>Every message of a round names its slice, so one addressed to another task drops the chunks
   * of an older slice just as well as one addressed to this task: the coordinator takes no answer
   * to the attempt it replaced, and a rewrite run over a plan it has given up on reads files the
   * slice that replaced it may already be replacing.
   */
  private PartialAssignment dropChunksOfOlderSlices(RewriteAssigned payload) {
    PartialAssignment pending = pendingAssignments.get(payload.tableReference());
    if (pending == null || pending.isOf(payload)) {
      return pending;
    }

    LOG.warn(
        "Dropping {} chunk(s) of slice {} of change set {} for table {}: slice {} of change set {} "
            + "has been handed out since, so the assignment they belong to will never be answered",
        pending.chunks.size(),
        pending.sliceSeq,
        pending.changeSetId,
        payload.tableReference().identifier(),
        payload.sliceSeq(),
        payload.changeSetId());
    pendingAssignments.remove(payload.tableReference());
    return null;
  }

  /** The chunks of one task's assignment that have arrived, and the assignment once all have. */
  private static final class PartialAssignment {
    private final UUID commitId;
    private final UUID changeSetId;
    private final int sliceSeq;
    private final int chunkCount;
    private final Map<Integer, Assignment> chunks = Maps.newHashMap();

    private PartialAssignment(RewriteAssigned payload) {
      this.commitId = payload.commitId();
      this.changeSetId = payload.changeSetId();
      this.sliceSeq = payload.sliceSeq();
      this.chunkCount = payload.chunkCount();
    }

    /** True if {@code payload} is a message of the same attempt of the same slice. */
    private boolean isOf(RewriteAssigned payload) {
      return commitId.equals(payload.commitId())
          && changeSetId.equals(payload.changeSetId())
          && sliceSeq == payload.sliceSeq();
    }

    /**
     * The whole assignment once this chunk completed it, or null while any is missing.
     *
     * <p>Counted by distinct index rather than by messages, so that a chunk delivered twice does
     * not stand in for one that never arrived. An index outside the count is not one of this
     * assignment's chunks: the sender that wrote it is broken, and counting it would start the
     * rewrite on a set with a hole in it, so it is dropped, and the slice times out.
     */
    private Assignment add(RewriteAssigned payload, Assignment mine) {
      if (payload.chunkIndex() < 0 || payload.chunkIndex() >= chunkCount) {
        LOG.warn(
            "Ignoring chunk {} of {} of the assignment of slice {} of change set {} for table {}: "
                + "the index is outside the count",
            payload.chunkIndex(),
            chunkCount,
            sliceSeq,
            changeSetId,
            payload.tableReference().identifier());
        return null;
      }

      chunks.put(payload.chunkIndex(), mine);
      if (chunks.size() < chunkCount) {
        return null;
      }

      List<FileScanTaskDescriptor> files = Lists.newArrayList();
      for (int index = 0; index < chunkCount; index++) {
        files.addAll(chunks.get(index).files());
      }
      return new Assignment(mine.taskId(), mine.expectedPartitions(), files);
    }
  }

  private Event toEvent(UUID commitId, RecordWriteResult writeResult) {
    switch (writeResult.kind()) {
      case DATA_FILES:
        IcebergWriterResult dataResult = (IcebergWriterResult) writeResult;
        return new Event(
            config.connectGroupId(),
            new DataWritten(
                dataResult.partitionStruct(),
                commitId,
                dataResult.tableReference(),
                dataResult.dataFiles(),
                dataResult.deleteFiles(),
                ImmutableList.copyOf(dataResult.sourceTopics())));
      case STAGED_CHANGES:
        StagedChangesResult stagedResult = (StagedChangesResult) writeResult;
        return new Event(
            config.connectGroupId(),
            new RowChangesWritten(
                commitId,
                stagedResult.tableReference(),
                stagedResult.taskId(),
                ImmutableList.copyOf(stagedResult.sourceTopics()),
                stagedResult.stagedFiles()));
      default:
        throw new IllegalStateException("Unknown writer result kind: " + writeResult.kind());
    }
  }

  @Override
  void stop() {
    if (assignmentRunner != null) {
      assignmentRunner.stop();
    }
    super.stop();
    sinkWriter.close();
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }
}
