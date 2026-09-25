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

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Commits one table's buffered responses for a cycle, in whichever row-level mode the connector is
 * configured for.
 *
 * <p>Merge-on-read needs only {@link #commit}: everything it does happens inside that call.
 * Copy-on-write cannot, since a slice's rewrite runs on the workers and its answers arrive over the
 * control topic on later cycles. It therefore drives its state machine from three entry points, of
 * which the coordinator calls all three:
 *
 * <ul>
 *   <li>{@link #commit}: once per commit cycle, with that cycle's envelopes;
 *   <li>{@link #receive}: as {@code RewriteComplete} events arrive;
 *   <li>{@link #process}: on every coordinator tick, to time out and re-assign a stuck slice, and
 *       to move a slice on without waiting for the next commit cycle. Tying slice progress to
 *       {@code commit()} would stretch a K-slice drain over K commit intervals.
 * </ul>
 *
 * <p>{@link Outcome#PENDING} means the table committed nothing this cycle but is not in trouble:
 * its responses stay buffered and its control topic offsets held back, as a thrown exception does
 * for merge-on-read.
 */
interface TableCommitter {

  enum Outcome {
    COMMITTED,
    PENDING,
    FAILED
  }

  CommitResult commit(TableCommitRequest request);

  /**
   * Handles a control event addressed to this committer.
   *
   * @return true if the event was handled
   */
  default boolean receive(Envelope envelope) {
    return false;
  }

  /**
   * Advances whatever this committer has in flight. Must not block the coordinator thread.
   *
   * @param bufferedResponses the coordinator's commit buffer, as a supplier rather than a value.
   *     Copy-on-write's staging cleanup needs the staged files these responses name (they belong to
   *     no change set yet, so no manifest and no snapshot points at them, and nothing else can tell
   *     the cleanup to leave them alone) but it needs them once an hour, while this method runs on
   *     every poll. Merge-on-read never calls it at all. Invoked only on the coordinator thread,
   *     and only inside this call: the buffer is that thread's own.
   */
  default void process(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {}

  /**
   * Tables that must be offered a commit this cycle even though no response for them is buffered.
   *
   * <p>Merge-on-read has none: a table's work begins and ends with the responses it has. In
   * copy-on-write a drain outlives them, so a table whose change set is half applied has to keep
   * being visited whether or not anything new was written to it.
   */
  default Set<TableReference> pendingTables() {
    return ImmutableSet.of();
  }

  /**
   * Envelopes the committed control topic offsets must not move past, though they are spent.
   *
   * <p>Merge-on-read has none. In copy-on-write, a table routed dynamically is named by its records
   * alone: once the responses of a change set are spent, nothing a restarted coordinator reads
   * names a table whose change set is half applied. One envelope of it, read again, brings the
   * table into the first cycle, and its drain resumes from the snapshot's pointer. Called on the
   * coordinator thread after every cycle's commits; the committer's transitions may be running.
   */
  default Collection<Envelope> replayAnchors() {
    return ImmutableList.of();
  }

  /**
   * The coordinator is stopping: another may already have been elected for the group, so nothing
   * this committer has in flight may start an Iceberg commit, delete a file or send an event from
   * here on. A commit already under way is atomic and is let finish.
   *
   * <p>Called once, from {@code Coordinator.terminate()}, before its pools are shut down; the
   * transitions still running see it from their own threads. Merge-on-read has nothing in flight
   * outside {@link #commit}, which the coordinator thread has finished by then.
   */
  default void stop() {}
}
