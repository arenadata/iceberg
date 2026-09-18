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
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Hands a table over from merge-on-read to copy-on-write: the {@code DataWritten} still buffered
 * from before the switch are committed in their turn, ahead of or behind the change set a drain
 * freezes.
 */
final class MergeOnReadTailHandoff {

  private final MergeOnReadTableCommitter mergeOnReadTail;

  MergeOnReadTailHandoff(MergeOnReadTableCommitter mergeOnReadTail) {
    this.mergeOnReadTail = mergeOnReadTail;
  }

  /**
   * Commits the tail of merge-on-read when its turn has come, and returns what a freeze may take of
   * the rest, or {@code null} if the tail's commit was refused.
   *
   * <p>The tail is {@code DataWritten} buffered from merge-on-read cycles before the table switched
   * to copy-on-write: the coordinator refuses one under a commit id it issued itself. It is
   * committed by {@link MergeOnReadTableCommitter} in a snapshot of its own, the one Iceberg commit
   * the copy-on-write {@code commit()} makes itself, as merge-on-read would.
   *
   * <p>Only a control topic partition orders envelopes by age, so on each partition the run of one
   * type below the first envelope of the other may go. {@code RowChangesWritten} below a {@code
   * DataWritten} are older than it and freeze first; otherwise the tail goes first, after the drain
   * in progress. Every request made here holds the offsets back to the oldest envelope it leaves
   * out, and carries no valid-through while it leaves anything out: the freeze drops everything
   * below the committed offsets as applied, so a tail snapshot recording this cycle's position
   * would lose the {@code RowChangesWritten} above it.
   */
  TableCommitRequest offer(
      TableCommitRequest request, boolean drainInProgress, Table table, List<Envelope> spent) {
    List<Envelope> remaining = request.envelopes();
    List<Envelope> rowChangesAhead =
        Envelopes.leadingOnTheirPartition(remaining, PayloadType.ROW_CHANGES_WRITTEN);
    boolean rowChangesFirst =
        rowChangesAhead.stream()
            .anyMatch(
                rowChanges ->
                    request.envelopes().stream()
                        .anyMatch(
                            envelope ->
                                Envelopes.isDataWritten(envelope)
                                    && envelope.partition() == rowChanges.partition()));

    List<Envelope> freezable;
    if (rowChangesFirst) {
      freezable = rowChangesAhead;
    } else if (drainInProgress) {
      // the tail goes after the drain in progress, and what came after the tail after the tail
      freezable = ImmutableList.of();
    } else {
      List<Envelope> tail = Envelopes.leadingOnTheirPartition(remaining, PayloadType.DATA_WRITTEN);
      remaining = Envelopes.without(remaining, tail);
      CommitResult tailResult =
          mergeOnReadTail.commit(Envelopes.narrowedTo(request, tail, remaining));
      if (tailResult.outcome() != TableCommitter.Outcome.COMMITTED) {
        return null;
      }
      spent.addAll(tail);
      // the drain plans against this table: without the tail's files, a slice rewriting its keys
      // would leave the rows the tail wrote next to their replacements
      table.refresh();
      freezable = Envelopes.leadingOnTheirPartition(remaining, PayloadType.ROW_CHANGES_WRITTEN);
    }

    return Envelopes.narrowedTo(request, freezable, Envelopes.without(remaining, freezable));
  }
}
