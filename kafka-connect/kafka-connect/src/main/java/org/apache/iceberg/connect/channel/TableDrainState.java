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
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.PlanSummary;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * One table's place in the drain, carried between {@code commit()}, {@code receive()} and {@code
 * process()} of {@link CopyOnWriteTableCommitter}.
 *
 * <p><b>Every field is guarded by {@link #lock}</b> (a {@link ReentrantLock}, not this object's
 * monitor), and the difference matters: {@code synchronized (state)} would compile, read as
 * protection and protect nothing. The lock is explicit because the coordinator thread is only ever
 * allowed to {@code tryLock} it: a transition can hold it for minutes (planning a slice, an Iceberg
 * commit that retries), and a coordinator thread waiting on it misses {@code max.poll.interval.ms}
 * and leaves the group. The one field outside the lock is {@link #answers}, which is a concurrent
 * queue precisely so that {@code receive()} can add to it without holding anything.
 *
 * <p>Plain fields, no accessors: this is the committer's own working memory, and every reader of it
 * is one of the committer's collaborators in this package.
 */
final class TableDrainState {

  final TableReference tableReference;
  final ReentrantLock lock = new ReentrantLock();

  /**
   * Answers that reached the coordinator thread while the lock was held. Applied by whichever of
   * {@code process()}, {@code receive()} or a finishing transition next gets the lock.
   */
  final Queue<RewriteComplete> answers = new ConcurrentLinkedQueue<>();

  Table table;
  String branch;
  String stagingLocation;
  Map<String, List<TopicPartitionRef>> activeTasks = ImmutableMap.of();

  ChangeSetManifest manifest;
  String manifestLocation;
  StructLike cursor;
  UUID commitId;
  OffsetDateTime validThroughTs;

  /**
   * True while this coordinator is the one that froze the current change set and no snapshot can
   * name it yet. Only then may its manifest be deleted on failure: a resumed change set is
   * referenced by a snapshot summary, a spent one has been committed at least once, and one whose
   * commit ended with an unknown outcome may have been; see {@link #commitOutcomeUnknown()}.
   */
  boolean freshlyFrozen;

  /**
   * The manifest of a change set dropped for changed identifier fields, whose staged files the
   * current change set adopted. Deleted when the current change set first commits: until then the
   * snapshot summary still points at it, and a failed drain or a restart drops it again from there.
   */
  String droppedManifestLocation;

  int sliceSeq;
  ChangeSetSlice slice;
  List<FileScanTask> planFiles = ImmutableList.of();
  Expression conflictFilter;
  PlanSummary planSummary;
  Long baseSnapshotId;
  boolean drained;
  String normalizedLocation;

  /**
   * What the change set has rewritten so far (bytes to rewrite and slices, over its committed
   * slices only) for the plan summary. Zeroed at every drain start, fresh or resumed, and so
   * counted from zero again after a restart; {@link #reset()} leaves them for the start to zero.
   */
  long changeSetBytesReplaced;

  int changeSetSlicesCommitted;

  /**
   * How many times the intermediate-commit slicing warning has been logged for this change set.
   * Zeroed with {@link #changeSetSlicesCommitted}, for the same reason; a test that cannot capture
   * the log reads this instead.
   */
  int sliceProgressWarningsLogged;

  /**
   * The change set last warned about for slices that go out over the byte quota, the warning being
   * once per change set. Neither {@link #reset()} nor a drain start clears it, so a drain of the
   * same change set started again by this coordinator does not repeat it; a new one does.
   */
  UUID overByteQuotaWarnedFor;

  /**
   * The change set last warned about for planned files with no bounds on an identifier column, the
   * warning being once per change set. Neither {@link #reset()} nor a drain start clears it, for
   * the same reason as {@link #overByteQuotaWarnedFor}.
   */
  UUID filesWithoutBoundsWarnedFor;

  /** The over-quota warning last logged for this table, for a test that cannot capture the log. */
  String overByteQuotaWarning;

  SliceAttempt attempt = SliceAttempt.empty();
  boolean awaiting;

  /**
   * Immediate retries left to the current slice, one quota for a conflicting commit and a task's
   * failed rewrite alike: each is another full round of rewriting by every task.
   */
  int retriesLeft;

  /**
   * True while the current slice waits for the next cycle to be handed out again: it timed out,
   * sent chunks that cannot add up, or used up its immediate retries. The drain is kept; that
   * cycle's {@code commit()} hands the slice to its own tasks, with the retries back in full.
   */
  boolean retryNextCycle;

  /**
   * Attempts at the current slice cancelled in a row, across every cycle it waited for, for the
   * cancellation warning. Only a commit of a slice zeroes it: {@link #reset()} leaves it, since a
   * drain started again begins from the same slice.
   */
  int cancelledInARow;

  /** The cancellation warning last logged for this table, for a test that cannot capture it. */
  String cancellationWarning;

  boolean lineageReported;

  /**
   * Set by a failure that the next cycle would hit identically; see {@code
   * PermanentCopyOnWriteException}. Never cleared: this table is not attempted again until the
   * connector is restarted, and it is reported once rather than every cycle.
   */
  String stoppedReason;

  /**
   * Why the last attempt to freeze was rejected; see {@code TableCommitRejectedException}. Kept
   * across cycles so the rejection is reported at ERROR once, and its end at INFO.
   */
  String rejectedReason;

  List<Envelope> frozenEnvelopes = ImmutableList.of();
  boolean spent;
  boolean unfinishedDrain;

  /**
   * Under dynamic routing, the envelope the committed control topic offsets stay at while this
   * table has a change set left to drain (ADR-0042): the latest one frozen into it, or the one a
   * restart replayed to resume it. A change set adopting the files of a dropped one keeps it.
   * Cleared once the change set drains or is abandoned; {@link #reset()} leaves it, since a failed
   * drain is resumed from the pointer. Read without the lock, by the coordinator thread.
   */
  volatile Envelope replayAnchor;

  TableDrainState(TableReference tableReference) {
    this.tableReference = tableReference;
  }

  /**
   * Retries are a quota per slice and per cycle, not per drain: a slice that committed is progress,
   * and a drain of fifty slices must not be held back by three transient conflicts spread across
   * it. Full again when a drain starts, when a slice commits, and when a slice that waited is
   * handed out on the next cycle.
   */
  void resetSliceRetryQuotas(int commitRetries) {
    retriesLeft = commitRetries;
  }

  /**
   * A slice commit ended without saying whether it happened. A snapshot may name the manifest from
   * here on, so a failed drain keeps it; if nothing does, the staging TTL collects it. The
   * envelopes are not spent: the commit may just as well not have happened, and then they are
   * frozen again.
   */
  void commitOutcomeUnknown() {
    freshlyFrozen = false;
  }

  /**
   * Adds the slice just committed to what the change set has rewritten so far. Committed slices
   * only: a cancelled or conflicting attempt replaced nothing.
   */
  void countCommittedSlice() {
    changeSetSlicesCommitted += 1;
    if (planSummary != null) {
      changeSetBytesReplaced += planSummary.totalRewriteBytes();
    }
  }

  /** True while a change set is being drained: no new one may be frozen. */
  boolean active() {
    return manifest != null;
  }

  /** True once a permanent failure stopped this table. */
  boolean stopped() {
    return stoppedReason != null;
  }

  /** Back to idle, keeping only what {@code commit()} still has to report. */
  void reset() {
    manifest = null;
    manifestLocation = null;
    freshlyFrozen = false;
    droppedManifestLocation = null;
    cursor = null;
    slice = null;
    planFiles = ImmutableList.of();
    conflictFilter = null;
    planSummary = null;
    baseSnapshotId = null;
    drained = false;
    normalizedLocation = null;
    attempt = SliceAttempt.empty();
    awaiting = false;
    retryNextCycle = false;
    sliceSeq = 0;
    lineageReported = false;
  }
}
