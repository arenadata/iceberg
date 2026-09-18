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

import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The messages one slice of a drain is handed out as.
 *
 * <p>One message per assigned task, all of them in the single transaction the caller sends them in:
 * a round that lands partially leaves the tasks that got nothing silent, and the slice waits out
 * its timeout for an answer nobody was asked for.
 *
 * <p>A task whose assignment does not fit one control message is sent several, each with its own
 * chunk index and the total, and the worker starts only once it holds them all. The slice is never
 * shrunk to fit: its plan is every file that may hold a key of the slice, and one key with no
 * bounds to prune by plans the whole table, so there is no size a slice can be cut to that bounds
 * the assignment. What cannot be split is a single data file descriptor past the limit: the table
 * is stopped over that, as the worker's answer is, since replanning it produces the same descriptor
 * every cycle.
 */
class AssignmentRound {

  private static final Logger LOG = LoggerFactory.getLogger(AssignmentRound.class);

  private final IcebergSinkConfig config;
  private final StructType wirePartitionType;
  private final UUID commitId;
  private final TableReference tableReference;
  private final UUID changeSetId;
  private final int sliceSeq;
  private final long baseSnapshotId;
  private final StagedChangeFile normalizedRef;
  private final List<Integer> identifierFieldIds;

  AssignmentRound(
      IcebergSinkConfig config,
      StructType wirePartitionType,
      UUID commitId,
      TableReference tableReference,
      UUID changeSetId,
      int sliceSeq,
      long baseSnapshotId,
      StagedChangeFile normalizedRef,
      List<Integer> identifierFieldIds) {
    this.config = config;
    this.wirePartitionType = wirePartitionType;
    this.commitId = commitId;
    this.tableReference = tableReference;
    this.changeSetId = changeSetId;
    this.sliceSeq = sliceSeq;
    this.baseSnapshotId = baseSnapshotId;
    this.normalizedRef = normalizedRef;
    this.identifierFieldIds = identifierFieldIds;
  }

  /**
   * Every message of the round, in one list for one transaction.
   *
   * <p>The owner block is the coordinator's to hand out: {@code ownerIndex} is the position of the
   * task in {@code assignments} and {@code ownerCount} their number, which is the number of tasks
   * assigned rather than the number of messages: chunks of one assignment share one block.
   */
  List<Event> messages(List<Assignment> assignments) {
    List<Event> events = Lists.newArrayListWithExpectedSize(assignments.size());
    for (int owner = 0; owner < assignments.size(); owner++) {
      Assignment assignment = assignments.get(owner);
      List<List<FileScanTaskDescriptor>> chunks = splitToFit(assignment, owner, assignments.size());
      for (int chunk = 0; chunk < chunks.size(); chunk++) {
        events.add(
            message(
                assignment, chunks.get(chunk), owner, assignments.size(), chunk, chunks.size()));
      }
      if (chunks.size() > 1) {
        LOG.info(
            "Handing slice {} of change set {} for table {} to task {} in {} messages: {} file(s) "
                + "do not fit one control message",
            sliceSeq,
            changeSetId,
            tableReference.identifier(),
            assignment.taskId(),
            chunks.size(),
            assignment.files().size());
      }
    }
    return events;
  }

  /**
   * Splits one task's files into chunks that each encode within {@link
   * IcebergSinkConfig#controlMessageMaxBytes()}.
   *
   * <p>Measured rather than counted, and halved rather than scaled from an estimate, for the same
   * reasons the answer is (see {@code RewriteAssignmentRunner.splitToFit}): a descriptor carries
   * per-column metrics, so no file count is safe for every table, and the encoded size is not
   * proportional to the count since every message repeats the Avro schema and the normalized file's
   * descriptor.
   *
   * <p>A task with no files still gets one message: silence and "nothing to do" have to stay
   * distinguishable to the coordinator.
   */
  @VisibleForTesting
  List<List<FileScanTaskDescriptor>> splitToFit(
      Assignment assignment, int ownerIndex, int ownerCount) {
    Deque<List<FileScanTaskDescriptor>> pending = Lists.newLinkedList();
    pending.add(assignment.files() == null ? ImmutableList.of() : assignment.files());

    int maxBytes = config.controlMessageMaxBytes();
    List<List<FileScanTaskDescriptor>> fitted = Lists.newArrayList();
    while (!pending.isEmpty()) {
      List<FileScanTaskDescriptor> chunk = pending.removeFirst();
      // the chunk index and count are two small ints wherever they land, so measuring with a
      // placeholder is exact enough against a limit that already reserves a fifth of the producer's
      int encodedBytes =
          AvroUtil.encode(message(assignment, chunk, ownerIndex, ownerCount, 0, 1)).length;
      if (encodedBytes <= maxBytes) {
        fitted.add(chunk);
      } else if (chunk.size() <= 1) {
        throw new PermanentCopyOnWriteException(
            String.format(
                Locale.ROOT,
                "The assignment of slice %d of change set %s for table %s to task %s encodes to "
                    + "%d bytes with %d file descriptor(s), past the %d byte limit, and cannot be "
                    + "split any further: the producer would reject this message, and the next "
                    + "cycle would plan the same file into a descriptor of the same size. Raise "
                    + "iceberg.kafka.%s and the control topic's %s with it, or narrow "
                    + "write.metadata.metrics.* and rewrite the affected data files -- the metrics "
                    + "of a file are written with the file and do not shrink on their own. Then "
                    + "restart the connector",
                sliceSeq,
                changeSetId,
                tableReference.identifier(),
                assignment.taskId(),
                encodedBytes,
                chunk.size(),
                maxBytes,
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                TopicConfig.MAX_MESSAGE_BYTES_CONFIG));
      } else {
        int half = chunk.size() / 2;
        pending.addFirst(chunk.subList(half, chunk.size()));
        pending.addFirst(chunk.subList(0, half));
      }
    }

    return fitted;
  }

  private Event message(
      Assignment assignment,
      List<FileScanTaskDescriptor> files,
      int ownerIndex,
      int ownerCount,
      int chunkIndex,
      int chunkCount) {
    return new Event(
        config.connectGroupId(),
        new RewriteAssigned(
            wirePartitionType,
            commitId,
            tableReference,
            changeSetId,
            sliceSeq,
            baseSnapshotId,
            normalizedRef,
            ImmutableList.of(
                new Assignment(assignment.taskId(), assignment.expectedPartitions(), files)),
            ownerIndex,
            ownerCount,
            identifierFieldIds,
            chunkIndex,
            chunkCount));
  }
}
