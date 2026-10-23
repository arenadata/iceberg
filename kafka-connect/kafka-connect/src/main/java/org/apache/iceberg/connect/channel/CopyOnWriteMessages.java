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
import java.util.Locale;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.PlanSummary;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.TableReference;

/** The texts of what copy-on-write tells the operator about a drain. */
final class CopyOnWriteMessages {
  private CopyOnWriteMessages() {}

  static String timeoutReason(TableDrainState state, IcebergSinkConfig config) {
    long timeoutMs = config.copyOnWriteRewriteTimeoutMs();
    // no plan on the append path: an empty table or a new branch, with nothing to rewrite
    long planBytes = state.planSummary == null ? 0 : state.planSummary.totalRewriteBytes();
    if (planBytes > config.copyOnWriteMaxRewriteBytes()) {
      return String.format(
          Locale.ROOT,
          "the slice plans %d bytes, over the %d byte quota, and was not rewritten within "
              + "iceberg.tables.copy-on-write.rewrite-timeout-ms (%d ms); it is handed out again "
              + "with the same plan, so raise that timeout to fit %d bytes (chunks received: %s)",
          planBytes,
          config.copyOnWriteMaxRewriteBytes(),
          timeoutMs,
          planBytes,
          state.attempt.receivedChunks());
    }

    return String.format(
        Locale.ROOT,
        "no complete answer within %d ms from %s (chunks received: %s)",
        timeoutMs,
        state.attempt.missingTasks(),
        state.attempt.receivedChunks());
  }

  static String cancellation(TableDrainState state, String reason, boolean retryAtOnce) {
    return String.format(
        Locale.ROOT,
        "Cancelling slice %d of change set %s for table %s: %s. Attempts at this slice "
            + "cancelled in a row: %d; handing it out again %s",
        state.sliceSeq,
        state.manifest.changeSetId(),
        state.tableReference.identifier(),
        reason,
        state.cancelledInARow,
        retryAtOnce ? "now" : "on the next cycle");
  }

  static String permanentRewriteFailure(RewriteComplete payload) {
    return String.format(
        Locale.ROOT,
        "task %s reported that slice %d of change set %s fails the same way on every "
            + "attempt, and that task's log names the cause and the remedy",
        payload.taskId(),
        payload.sliceSeq(),
        payload.changeSetId());
  }

  static String sliceProgress(TableDrainState state) {
    return String.format(
        Locale.ROOT,
        "Copy-on-write change set %s for table %s is being sliced: %d slice(s) committed so far, "
            + "%d byte(s) rewritten in them. To cut the slicing rather than live with it, raise "
            + "iceberg.control.commit.interval-ms, partition or sort the table by the key, or "
            + "switch the table back to merge-on-read",
        state.manifest.changeSetId(),
        state.tableReference.identifier(),
        state.changeSetSlicesCommitted,
        state.changeSetBytesReplaced);
  }

  static String overByteQuota(
      TableDrainState state,
      PlanSummary summary,
      int keys,
      int sliceKeys,
      IcebergSinkConfig config) {
    return String.format(
        Locale.ROOT,
        "Copy-on-write change set %s for table %s: a single key plans %d bytes, over the %d byte "
            + "quota, so slice %d goes out over it with the %d of %d key(s) that plan no more than "
            + "one key does; %d%% of the planned files have no bounds on the identifier columns. "
            + "Such a slice has to be rewritten within "
            + "iceberg.tables.copy-on-write.rewrite-timeout-ms (%d ms), or it is cancelled and "
            + "handed out again whole every cycle, never getting any further: raise that timeout "
            + "to fit %d bytes. Slices of this table stay over the quota until one of: raise "
            + "iceberg.tables.copy-on-write.max-rewrite-bytes, enable write.metadata.metrics.* for "
            + "the identifier columns, partition or sort the table by the key, or switch the table "
            + "back to merge-on-read",
        state.manifest.changeSetId(),
        state.tableReference.identifier(),
        summary.totalRewriteBytes(),
        config.copyOnWriteMaxRewriteBytes(),
        state.sliceSeq,
        keys,
        sliceKeys,
        Math.round(summary.fractionFilesWithoutBounds() * 100),
        config.copyOnWriteRewriteTimeoutMs(),
        summary.totalRewriteBytes());
  }

  static String filesWithoutBounds(TableDrainState state, PlanSummary summary) {
    return String.format(
        Locale.ROOT,
        "Copy-on-write change set %s for table %s: %d%% of the planned files have no bounds on "
            + "the identifier columns and cannot be pruned by them, so they are rewritten on "
            + "every slice that touches their key range. Enable write.metadata.metrics.* for the "
            + "identifier columns to avoid it",
        state.manifest.changeSetId(),
        state.tableReference.identifier(),
        Math.round(summary.fractionFilesWithoutBounds() * 100));
  }

  static String rewriteOutcome(TableDrainState state, List<DataFile> committedFiles) {
    long bytesWritten = committedFiles.stream().mapToLong(DataFile::fileSizeInBytes).sum();
    if (state.planSummary == null) {
      // the append path: nothing was planned, so there is no before-and-after to report
      return String.format(
          Locale.ROOT,
          "appended %d file(s), %d byte(s), nothing replaced",
          committedFiles.size(),
          bytesWritten);
    }

    return state
        .planSummary
        .withRewriteResult(committedFiles.size(), bytesWritten, state.attempt.assignedTaskCount())
        .toString();
  }

  static String secondName(TableReference draining, UUID tableUuid) {
    return String.format(
        Locale.ROOT,
        "this is the table already draining as %s -- one Iceberg table (UUID %s) under two "
            + "names, which a case-insensitive catalog accepts. Each name would plan, "
            + "slice and commit a copy-on-write drain of its own into that one table, so "
            + "this one is stopped: name the table once in iceberg.tables (or once per "
            + "routing rule), keeping the spelling that is draining",
        draining.identifier(),
        tableUuid);
  }
}
