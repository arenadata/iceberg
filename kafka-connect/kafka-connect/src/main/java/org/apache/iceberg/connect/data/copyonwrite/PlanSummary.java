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
package org.apache.iceberg.connect.data.copyonwrite;

import java.util.Locale;

/**
 * Unconditionally-logged metrics for one slice of a copy-on-write drain: how much work its rewrite
 * costs, and (once the workers have answered) what it actually produced.
 *
 * <p>Filled in three times: {@link AffectedFilePlanner} fills in the planned side; the drain adds
 * with {@link #withChanges}, once the slice's normalized file is written and before the slice is
 * handed out, how many bytes actually change and what the change set's committed slices have
 * rewritten so far; the commit path adds with {@link #withRewriteResult} the files the tasks named
 * in their responses.
 *
 * <p>{@link #rewriteAmplification()} is the number that says whether copy-on-write suits the table:
 * bytes rewritten per byte that changes. It stays small only while the keys cluster in few files.
 * Slicing multiplies it, because a change set whose keys are spread over the table rewrites nearly
 * the same files in every slice, which only the change set's running total makes visible.
 *
 * <p>{@link #fileAmplification()} is a different ratio: files written per file replaced. It grows
 * with {@code tasks.max}, and not because anything is wrong: each task writes the remainder of its
 * own files plus the rows for its own block of keys, so a slice spread over more tasks lands as
 * more, smaller files even when the data is unchanged. It is why compaction stays mandatory in
 * copy-on-write, where there are no delete files to merge.
 */
public final class PlanSummary {

  private static final int UNKNOWN = -1;

  private final int filesCount;
  private final long totalRewriteBytes;
  private final long changeSetRecords;
  private final double fractionFilesWithoutBounds;
  private final boolean truncated;
  private final boolean exceedsMaxRewriteBytes;
  private final long changeBytes;
  private final long changeSetBytesReplaced;
  private final int changeSetSlicesCommitted;
  private final int filesWritten;
  private final long bytesWritten;
  private final int taskCount;

  PlanSummary(
      int filesCount,
      long totalRewriteBytes,
      long changeSetRecords,
      double fractionFilesWithoutBounds,
      boolean truncated,
      boolean exceedsMaxRewriteBytes) {
    this(
        filesCount,
        totalRewriteBytes,
        changeSetRecords,
        fractionFilesWithoutBounds,
        truncated,
        exceedsMaxRewriteBytes,
        UNKNOWN,
        UNKNOWN,
        UNKNOWN,
        UNKNOWN,
        UNKNOWN,
        UNKNOWN);
  }

  private PlanSummary(
      int filesCount,
      long totalRewriteBytes,
      long changeSetRecords,
      double fractionFilesWithoutBounds,
      boolean truncated,
      boolean exceedsMaxRewriteBytes,
      long changeBytes,
      long changeSetBytesReplaced,
      int changeSetSlicesCommitted,
      int filesWritten,
      long bytesWritten,
      int taskCount) {
    this.filesCount = filesCount;
    this.totalRewriteBytes = totalRewriteBytes;
    this.changeSetRecords = changeSetRecords;
    this.fractionFilesWithoutBounds = fractionFilesWithoutBounds;
    this.truncated = truncated;
    this.exceedsMaxRewriteBytes = exceedsMaxRewriteBytes;
    this.changeBytes = changeBytes;
    this.changeSetBytesReplaced = changeSetBytesReplaced;
    this.changeSetSlicesCommitted = changeSetSlicesCommitted;
    this.filesWritten = filesWritten;
    this.bytesWritten = bytesWritten;
    this.taskCount = taskCount;
  }

  /**
   * The same summary with what the drain knows once the slice's normalized file is written.
   *
   * @param normalizedBytes size of the slice's normalized change file: the bytes that actually
   *     change, as an estimate, since that file is not in the table's data file format
   * @param bytesReplacedSoFar bytes to rewrite summed over the change set's slices committed before
   *     this one; kept in memory only, so counted from zero again after a restart
   * @param slicesCommittedSoFar how many of the change set's slices were committed before this one
   */
  public PlanSummary withChanges(
      long normalizedBytes, long bytesReplacedSoFar, int slicesCommittedSoFar) {
    return new PlanSummary(
        filesCount,
        totalRewriteBytes,
        changeSetRecords,
        fractionFilesWithoutBounds,
        truncated,
        exceedsMaxRewriteBytes,
        normalizedBytes,
        bytesReplacedSoFar,
        slicesCommittedSoFar,
        filesWritten,
        bytesWritten,
        taskCount);
  }

  /** The same summary with what the rewrite produced, summed over every task, filled in. */
  public PlanSummary withRewriteResult(int writtenFiles, long writtenBytes, int tasks) {
    return new PlanSummary(
        filesCount,
        totalRewriteBytes,
        changeSetRecords,
        fractionFilesWithoutBounds,
        truncated,
        exceedsMaxRewriteBytes,
        changeBytes,
        changeSetBytesReplaced,
        changeSetSlicesCommitted,
        writtenFiles,
        writtenBytes,
        tasks);
  }

  /** Data files the plan replaces. */
  public int filesCount() {
    return filesCount;
  }

  public long totalRewriteBytes() {
    return totalRewriteBytes;
  }

  public long changeSetRecords() {
    return changeSetRecords;
  }

  /** Fraction of planned files with no recorded bounds for at least one identifier column. */
  public double fractionFilesWithoutBounds() {
    return fractionFilesWithoutBounds;
  }

  public boolean truncated() {
    return truncated;
  }

  /**
   * True if {@link #totalRewriteBytes()} exceeds the configured {@code max-rewrite-bytes} quota.
   * Informational only: the planner never drops files to fit (see {@link AffectedFilePlanner}).
   */
  public boolean exceedsMaxRewriteBytes() {
    return exceedsMaxRewriteBytes;
  }

  /** Size of the slice's normalized change file, or {@code -1} before it was written. */
  public long changeBytes() {
    return changeBytes;
  }

  /**
   * Bytes to rewrite per byte that changes, or {@code -1} before the normalized file was written.
   */
  public double rewriteAmplification() {
    if (changeBytes <= 0) {
      return UNKNOWN;
    }
    return (double) totalRewriteBytes / changeBytes;
  }

  /**
   * Bytes to rewrite summed over the change set's slices committed before this one, or {@code -1}
   * before the drain filled it in.
   */
  public long changeSetBytesReplaced() {
    return changeSetBytesReplaced;
  }

  /** The change set's slices committed before this one, or {@code -1} before the drain says. */
  public int changeSetSlicesCommitted() {
    return changeSetSlicesCommitted;
  }

  /** Replacement data files written, or {@code -1} before the tasks have answered. */
  public int filesWritten() {
    return filesWritten;
  }

  /** Total size of the replacement data files, or {@code -1} before the tasks have answered. */
  public long bytesWritten() {
    return bytesWritten;
  }

  /** Tasks the slice was spread over, or {@code -1} before it was assigned. */
  public int taskCount() {
    return taskCount;
  }

  /**
   * Files written per file replaced, or {@code -1} if either side is unknown.
   *
   * <p>Above one means the slice fragmented the table; how far above is set mostly by {@code
   * tasks.max} and by how many partitions the slice touched.
   */
  public double fileAmplification() {
    if (filesWritten < 0 || filesCount <= 0) {
      return UNKNOWN;
    }
    return (double) filesWritten / filesCount;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("PlanSummary{")
            .append("filesReplaced=")
            .append(filesCount)
            .append(", bytesReplaced=")
            .append(totalRewriteBytes)
            .append(", changeSetRecords=")
            .append(changeSetRecords)
            .append(", fractionFilesWithoutBounds=")
            .append(fractionFilesWithoutBounds)
            .append(", truncated=")
            .append(truncated)
            .append(", exceedsMaxRewriteBytes=")
            .append(exceedsMaxRewriteBytes);
    if (changeBytes >= 0) {
      sb.append(", changeBytes=")
          .append(changeBytes)
          .append(", rewriteAmplification=")
          .append(String.format(Locale.ROOT, "%.2f", rewriteAmplification()))
          .append(", changeSetBytesReplaced=")
          .append(changeSetBytesReplaced)
          .append(", changeSetSlicesCommitted=")
          .append(changeSetSlicesCommitted);
    }
    if (filesWritten >= 0) {
      sb.append(", filesWritten=")
          .append(filesWritten)
          .append(", bytesWritten=")
          .append(bytesWritten)
          .append(", tasks=")
          .append(taskCount)
          .append(", fileAmplification=")
          .append(String.format(Locale.ROOT, "%.2f", fileAmplification()));
    }
    return sb.append('}').toString();
  }
}
