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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * One attempt at a slice, as far as the workers' answers go: which tasks it went to, which chunks
 * of their answers have arrived, and the replacement files they carry.
 *
 * <p>Answers are counted here and nothing else is done with them: whether an answer belongs to this
 * attempt at all is {@link AttemptId}'s, and what becomes of the files is the committer's. Guarded
 * by the lock of the {@link TableDrainState} it belongs to.
 */
final class SliceAttempt {

  /** What one chunk of an answer did to the attempt. */
  enum ChunkOutcome {
    /** A chunk index of that task that has arrived before: not counted again. */
    DUPLICATE,
    /** A chunk that cannot belong to a complete answer; see {@link ChunkResult#reason()}. */
    CHUNK_ERROR,
    /** Counted, and the attempt still waits for other chunks or tasks. */
    PARTIAL,
    /** Counted, and every task the slice went to has now answered in full. */
    COMPLETE
  }

  /** A {@link ChunkOutcome}, with the reason when it is a chunk error. */
  static final class ChunkResult {
    private static final ChunkResult DUPLICATE = new ChunkResult(ChunkOutcome.DUPLICATE, null);
    private static final ChunkResult PARTIAL = new ChunkResult(ChunkOutcome.PARTIAL, null);
    private static final ChunkResult COMPLETE = new ChunkResult(ChunkOutcome.COMPLETE, null);

    private final ChunkOutcome outcome;
    private final String reason;

    private ChunkResult(ChunkOutcome outcome, String reason) {
      this.outcome = outcome;
      this.reason = reason;
    }

    private static ChunkResult chunkError(String reason) {
      return new ChunkResult(ChunkOutcome.CHUNK_ERROR, reason);
    }

    ChunkOutcome outcome() {
      return outcome;
    }

    /** Why the chunk cannot be counted, for the cancellation warning; null unless a chunk error. */
    String reason() {
      return reason;
    }
  }

  private final Set<String> assignedTasks;
  private final Set<String> receivedTasks = Sets.newHashSet();
  private final Map<String, Set<Integer>> receivedChunks = Maps.newHashMap();

  /**
   * How many chunks each task announced in the first chunk of its answer. Chunks of two attempts of
   * one task carry different totals, and a later one standing in for the first would let as few as
   * two of five chunks look like a complete answer.
   */
  private final Map<String, Integer> announcedChunks = Maps.newHashMap();

  private final long assignedAtMs;
  private List<DataFile> collected = Lists.newArrayList();

  SliceAttempt(Set<String> assignedTasks, long assignedAtMs) {
    this.assignedTasks = assignedTasks;
    this.assignedAtMs = assignedAtMs;
  }

  /** No attempt: idle, or a slice with nothing to hand out. */
  static SliceAttempt empty() {
    return new SliceAttempt(Sets.newHashSet(), 0L);
  }

  boolean isAssigned(String taskId) {
    return assignedTasks.contains(taskId);
  }

  /**
   * Counts one chunk of a task's successful answer and keeps its files. The answer is known to be
   * this attempt's, from a task it went to.
   */
  ChunkResult acceptChunk(RewriteComplete payload) {
    // a task has answered once every chunk index it announced has arrived, not as many chunks.
    // Each chunk is its own producer transaction, so one can be lost while the rest commit, and a
    // redelivered chunk must not make up for it: that would commit one chunk's files twice and
    // another's not at all. A missing index never completes; the slice times out and is replanned
    Set<Integer> seen =
        receivedChunks.computeIfAbsent(payload.taskId(), ignored -> Sets.newHashSet());
    if (payload.chunkIndex() >= payload.chunkCount()) {
      return ChunkResult.chunkError(
          String.format(
              Locale.ROOT,
              "task %s sent chunk %d of an announced %d",
              payload.taskId(),
              payload.chunkIndex(),
              payload.chunkCount()));
    }
    Integer announced = announcedChunks.putIfAbsent(payload.taskId(), payload.chunkCount());
    if (announced != null && announced != payload.chunkCount()) {
      // chunks of two attempts of this task, one having split its answer differently from the
      // other: their indexes are distinct, so as many as the last one announced can arrive with
      // whole chunks of both answers missing
      return ChunkResult.chunkError(
          String.format(
              Locale.ROOT,
              "task %s announced %d chunks and then %d",
              payload.taskId(),
              announced,
              payload.chunkCount()));
    }
    if (!seen.add(payload.chunkIndex())) {
      return ChunkResult.DUPLICATE;
    }

    collected.addAll(payload.dataFiles());

    if (seen.size() == payload.chunkCount()) {
      receivedTasks.add(payload.taskId());
    }

    return receivedTasks.containsAll(assignedTasks) ? ChunkResult.COMPLETE : ChunkResult.PARTIAL;
  }

  /** The files collected so far, handed over: the attempt holds none afterwards. */
  List<DataFile> takeCollected() {
    List<DataFile> taken = collected;
    collected = Lists.newArrayList();
    return taken;
  }

  /** The files collected so far, left where they are. */
  List<DataFile> collected() {
    return Collections.unmodifiableList(collected);
  }

  int assignedTaskCount() {
    return assignedTasks.size();
  }

  /** Tasks the slice went to that have not answered in full. */
  Set<String> missingTasks() {
    return Sets.difference(assignedTasks, receivedTasks);
  }

  /** The chunk indexes received, by task: a task with a chunk error shows with none. */
  Map<String, Set<Integer>> receivedChunks() {
    return Collections.unmodifiableMap(receivedChunks);
  }

  long assignedAtMs() {
    return assignedAtMs;
  }
}
