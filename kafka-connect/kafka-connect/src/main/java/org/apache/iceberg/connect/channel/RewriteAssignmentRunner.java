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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.AssignedFileScanTask;
import org.apache.iceberg.connect.data.copyonwrite.CancelledCopyOnWriteException;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetNormalizer;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetSlice;
import org.apache.iceberg.connect.data.copyonwrite.CopyOnWriteRewriter;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the rewrites this task is assigned, off the poll thread.
 *
 * <p>{@code Worker.receive()} hands an assignment over and returns immediately: a rewrite reads and
 * writes data files, and must never happen in the stack of {@code put()}. What comes back is one or
 * more {@link RewriteComplete} events, chunked so that no single message outgrows the control
 * topic.
 *
 * <p>{@code iceberg.tables.copy-on-write.rewrite-threads} is a limit on this task, shared by every
 * assignment it runs: no more than that many units of rewrite work, each holding a data file open,
 * run at once, all of them on {@code rewriteExec}. Assignments run on a pool of their own, where
 * they load the table, normalize the slice, wait for their chunks and answer. Waiting takes nothing
 * from the limit, and could not: assignments waiting inside the pool their chunks queue on would
 * deadlock as soon as they filled it.
 */
class RewriteAssignmentRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteAssignmentRunner.class);

  /**
   * How long {@link #stop()} waits for rewrites to stop or finish answering before interrupting
   * them: the default {@code task.shutdown.graceful.timeout.ms} of Connect.
   */
  private static final long SHUTDOWN_GRACE_MS = 5_000;

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final Consumer<Event> eventSender;

  /**
   * Assignments. A {@link ThreadPoolExecutor} rather than an {@link ExecutorService} for its queue
   * length, which an assignment dropped for waiting too long reports.
   */
  private final ThreadPoolExecutor exec;

  /** Units of rewrite work of every assignment; never runs an assignment itself. */
  private final ExecutorService rewriteExec;

  /**
   * The newest slice this task has been handed for each table.
   *
   * <p>An assignment already superseded here is dropped before it starts, and one superseded while
   * it runs stops at the rewriter's next check (or, past the last one, before it answers), deleting
   * what it wrote and sending nothing. The coordinator has already handed out its replacement, and
   * running the dead one would delay that by a whole rewrite: at {@code rewrite-threads = 1}, a
   * livelock. Keyed by table, not by change set, so it stays the size of the table set: a new
   * change set supersedes the old one whole.
   */
  private final Map<TableReference, SliceRef> latestAssigned = Maps.newConcurrentMap();

  /** Set by {@link #stop()}: nothing starts any more, and nothing that has not answered will. */
  private volatile boolean stopping;

  RewriteAssignmentRunner(Catalog catalog, IcebergSinkConfig config, Consumer<Event> eventSender) {
    this.catalog = catalog;
    this.config = config;
    this.eventSender = eventSender;
    int rewriteThreads = Math.max(1, config.copyOnWriteRewriteThreads());
    this.exec =
        new ThreadPoolExecutor(
            rewriteThreads,
            rewriteThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            daemonThreads("iceberg-copy-on-write-rewrite-assignment-%d"));
    this.rewriteExec =
        Executors.newFixedThreadPool(
            rewriteThreads, daemonThreads("iceberg-copy-on-write-rewrite-worker-%d"));
  }

  private static ThreadFactory daemonThreads(String nameFormat) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
  }

  void submit(RewriteAssigned assigned, Assignment mine) {
    remember(assigned);
    // the queue is timed from here, by this task's own clock: an assignment is received no earlier
    // than the coordinator handed it out, so one this task drops for waiting too long is one the
    // coordinator has already stopped waiting for
    long receivedNanos = System.nanoTime();
    exec.execute(() -> runAssignment(assigned, mine, receivedNanos));
  }

  /**
   * Runs one assignment, keeping what it throws inside this class.
   *
   * <p>A pool {@code Runnable} that throws prints its stack to {@code System.err}, past the
   * connector's log. What reaches here is a failure to answer, so there is nothing left to answer
   * with: the coordinator times the slice out.
   */
  private void runAssignment(RewriteAssigned assigned, Assignment mine, long receivedNanos) {
    try {
      if (waitedOutTheTimeout(assigned, receivedNanos)) {
        return;
      }
      rewrite(assigned, mine);
    } catch (Exception e) {
      LOG.error(
          "Could not report the outcome of slice {} of change set {} for table {}; the coordinator "
              + "will time this slice out and hand it out again",
          assigned.sliceSeq(),
          assigned.changeSetId(),
          assigned.tableReference().identifier(),
          e);
    }
  }

  /**
   * True if this assignment spent longer in the queue than {@code rewrite-timeout-ms}, which is as
   * long as the coordinator waits for it: it is not started.
   *
   * <p>By then the coordinator has handed out a replacement, which would queue behind the dead one:
   * at {@code rewrite-threads = 1}, with several tables draining, every replacement would time out
   * behind the attempt it replaced. Nothing was written, so nothing is answered. A rewrite that
   * runs too long is left to the coordinator's timeout.
   */
  private boolean waitedOutTheTimeout(RewriteAssigned assigned, long receivedNanos) {
    long timeoutMs = config.copyOnWriteRewriteTimeoutMs();
    long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - receivedNanos);
    if (waitedMs <= timeoutMs) {
      return false;
    }

    LOG.warn(
        "Not starting slice {} of change set {} for table {}: it waited {} ms in this task's "
            + "rewrite queue, longer than iceberg.tables.copy-on-write.rewrite-timeout-ms ({} ms), "
            + "so the coordinator has already timed this slice out and handed it out again. {} "
            + "more assignment(s) are queued. Raise "
            + "iceberg.tables.copy-on-write.rewrite-threads to rewrite more slices at once, or "
            + "rewrite-timeout-ms to leave room for the queue, or spread the copy-on-write tables "
            + "of this connector over more tasks or over more connectors",
        assigned.sliceSeq(),
        assigned.changeSetId(),
        assigned.tableReference().identifier(),
        waitedMs,
        timeoutMs,
        exec.getQueue().size());
    return true;
  }

  /**
   * Records this assignment as the table's newest, before the task is queued: the mark has to be in
   * place by the time whatever is running now reaches its own check.
   *
   * <p>Never moves backwards. Assignments arrive in control topic order, so the last one submitted
   * is normally the newest, but a redelivered message must not un-supersede the slice it was
   * already replaced by.
   */
  void remember(RewriteAssigned assigned) {
    latestAssigned.merge(
        assigned.tableReference(),
        new SliceRef(assigned.commitId(), assigned.changeSetId(), assigned.sliceSeq()),
        SliceRef::newer);
  }

  /** One slice of one change set of one drain: what {@link #latestAssigned} remembers per table. */
  private static final class SliceRef {
    private final UUID commitId;
    private final UUID changeSetId;
    private final int sliceSeq;

    private SliceRef(UUID commitId, UUID changeSetId, int sliceSeq) {
      this.commitId = commitId;
      this.changeSetId = changeSetId;
      this.sliceSeq = sliceSeq;
    }

    /**
     * Of the mark already held and the one just submitted, the one that supersedes the other.
     *
     * <p>Within one drain of one change set, the higher slice sequence. Across change sets or
     * drains (commit ids), the one submitted last: a table drains one change set at a time, a drain
     * is only resumed once the one before it is gone, and slice numbers restart from zero in every
     * drain, so they say nothing across them.
     */
    private static SliceRef newer(SliceRef held, SliceRef submitted) {
      if (!held.commitId.equals(submitted.commitId)
          || !held.changeSetId.equals(submitted.changeSetId)) {
        return submitted;
      }
      return submitted.sliceSeq >= held.sliceSeq ? submitted : held;
    }
  }

  /** True once a later assignment for the same table has arrived: this one cannot be committed. */
  boolean superseded(RewriteAssigned assigned) {
    SliceRef latest = latestAssigned.get(assigned.tableReference());
    return latest != null
        && (!latest.commitId.equals(assigned.commitId())
            || !latest.changeSetId.equals(assigned.changeSetId())
            || latest.sliceSeq > assigned.sliceSeq());
  }

  /**
   * Why this assignment is no longer worth finishing, or null while it still is. The rewriter asks
   * between files and between rows.
   */
  private String cancellation(RewriteAssigned assigned) {
    if (stopping) {
      return "this task is stopping";
    }
    return superseded(assigned) ? "a later assignment for this table has arrived" : null;
  }

  /**
   * Stops rewriting: whatever has not begun to answer is cancelled, whatever has is given a moment
   * to finish.
   *
   * <p>Called on every revocation of this task's partitions, and the caller closes the producer
   * right afterwards. A rewrite that has sent nothing answers nothing, not even FAILED: that would
   * spend one of the slice's commit retries and hand it out again at once to the active tasks that
   * still include this task, while silence has the coordinator time it out and replan it with the
   * tasks that remain. A rewrite that has sent a chunk finishes the answer: the coordinator may
   * hold part of it.
   *
   * <p>Threads are interrupted only past the bound: an interrupt inside {@code commitTransaction}
   * leaves the outcome of the chunk unknown.
   */
  void stop() {
    stopping = true;
    exec.shutdown();
    try {
      long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SHUTDOWN_GRACE_MS);
      boolean stopped = exec.awaitTermination(SHUTDOWN_GRACE_MS, TimeUnit.MILLISECONDS);
      // assignments are all that hand units of work to rewriteExec, and each waits for its own:
      // shut down any earlier and a rewrite that has not reached the rewriter yet is refused
      rewriteExec.shutdown();
      stopped =
          stopped
              && rewriteExec.awaitTermination(
                  deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
      if (!stopped) {
        LOG.warn(
            "Rewrite threads did not stop within {} ms; interrupting them. The coordinator times "
                + "out a slice still being rewritten and replans it",
            SHUTDOWN_GRACE_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      exec.shutdownNow();
      rewriteExec.shutdownNow();
    }
  }

  private void rewrite(RewriteAssigned assigned, Assignment mine) {
    String skipped = cancellation(assigned);
    if (skipped != null) {
      // never started, so there is nothing to clean up and nothing to answer: the coordinator
      // cancelled this slice before this task got to it and is waiting on the replacement, or
      // this task is stopping and the coordinator will time the slice out
      LOG.info(
          "Skipping slice {} of change set {} for table {}: {}",
          assigned.sliceSeq(),
          assigned.changeSetId(),
          assigned.tableReference().identifier(),
          skipped);
      return;
    }

    Table table;
    try {
      table = catalog.loadTable(assigned.tableReference().identifier());
    } catch (RuntimeException | LinkageError e) {
      LOG.error(
          "Cannot load table {} to rewrite slice {} of change set {}",
          assigned.tableReference().identifier(),
          assigned.sliceSeq(),
          assigned.changeSetId(),
          e);
      sendFailure(assigned, null, failureKind(e));
      return;
    }

    // held outside the try that produces it: from the moment rewrite() returns these files are
    // this attempt's to clean up, and nobody else's until the coordinator commits them
    List<DataFile> written = null;
    AtomicBoolean answering = new AtomicBoolean();
    try {
      Set<Integer> identifierFieldIds = Sets.newHashSet(assigned.identifierFieldIds());

      // cursor null and quota unlimited, always: both were already applied by the coordinator when
      // it normalized this slice, and re-applying either here would hand this worker a different
      // slice than its peers
      ChangeSetSlice slice =
          ChangeSetNormalizer.normalize(
              table,
              identifierFieldIds,
              ImmutableList.of(assigned.normalizedRef()),
              null,
              Long.MAX_VALUE);

      List<FileScanTask> myFiles = AssignedFileScanTask.from(table, mine.files());

      LOG.info(
          "Rewriting slice {} of change set {} for table {}: {} file(s), keys {}/{}",
          assigned.sliceSeq(),
          assigned.changeSetId(),
          assigned.tableReference().identifier(),
          myFiles.size(),
          assigned.ownerIndex(),
          assigned.ownerCount());

      written =
          new CopyOnWriteRewriter(table, config.copyOnWriteRewriteThreads(), rewriteExec)
              .rewrite(
                  assigned.baseSnapshotId(),
                  myFiles,
                  slice,
                  assigned.ownerIndex(),
                  assigned.ownerCount(),
                  () -> cancellation(assigned) != null);

      checkOnePartitionSpec(table, written);

      // checked once more with the files in hand, the last time before the first chunk: this
      // rewrite may have outrun rewrite-timeout-ms, in which case the coordinator cancelled the
      // slice and would throw the answer away as stale (applyAnswer) without ever deleting what it
      // names
      String dropped = cancellation(assigned);
      if (dropped != null) {
        LOG.warn(
            "Dropping the answer to slice {} of change set {} for table {} and its {} replacement "
                + "file(s): {}",
            assigned.sliceSeq(),
            assigned.changeSetId(),
            assigned.tableReference().identifier(),
            written.size(),
            dropped);
        discardWritten(table, written);
        return;
      }

      sendChunked(assigned, table, written, answering);
    } catch (Throwable e) {
      if (e instanceof CancelledCopyOnWriteException) {
        // superseded or stopped mid-rewrite: the rewriter deleted what it wrote on the way out, and
        // nobody is waiting for an answer: the coordinator already handed the slice out again,
        // or will time it out
        LOG.info(
            "Stopped rewriting slice {} of change set {} for table {}: {}",
            assigned.sliceSeq(),
            assigned.changeSetId(),
            assigned.tableReference().identifier(),
            cancellation(assigned));
        return;
      }

      int failureKind = failureKind(e);
      LOG.error(
          "Rewrite of slice {} of change set {} for table {} failed{}",
          assigned.sliceSeq(),
          assigned.changeSetId(),
          assigned.tableReference().identifier(),
          failureKind == RewriteComplete.FAILURE_PERMANENT
              ? ", and every retry would fail the same way: the coordinator stops this table until "
                  + "the connector is restarted"
              : "",
          e);
      // the rewriter deletes what it wrote when it is the one that threw, and a failure before
      // the first chunk (the spec check, the split) leaves the whole set behind with nothing
      // else owning it: the coordinator never hears of an answer that was not sent
      if (answering.get()) {
        LOG.warn(
            "Leaving the {} replacement file(s) of slice {} of change set {} for table {} in "
                + "place: a chunk of this answer has already gone to the producer, so the "
                + "coordinator may hold it -- a commitTransaction that raises a timeout or an "
                + "interrupt may well have committed. It deletes what it collected if it cancels "
                + "the slice, and remove_orphan_files covers the rest",
            written == null ? 0 : written.size(),
            assigned.sliceSeq(),
            assigned.changeSetId(),
            assigned.tableReference().identifier());
      } else {
        discardWritten(table, written);
      }
      sendFailure(assigned, table, failureKind);
      if (e instanceof VirtualMachineError) {
        // the slice has been answered, which is all this class owes anyone; a thread that carried
        // on as if the JVM were well would only bury the error under whatever fails next
        throw e;
      }
    }
  }

  /**
   * Retryable, or permanent because the next cycle fails the same way and only an operator can
   * change that.
   *
   * <p>Searched in the whole cause chain: the producer raises a rejected record out of {@code
   * commitTransaction}, wrapped.
   *
   * <ul>
   *   <li>{@link PermanentCopyOnWriteException}: the message names the remedy;
   *   <li>{@link RecordTooLargeException}: the same files encode to the same bytes every cycle;
   *   <li>{@link LinkageError}: a JVM caches a failed class initialization for its lifetime.
   * </ul>
   */
  private static int failureKind(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof PermanentCopyOnWriteException
          || cause instanceof RecordTooLargeException
          || cause instanceof LinkageError) {
        return RewriteComplete.FAILURE_PERMANENT;
      }
    }
    return RewriteComplete.FAILURE_RETRYABLE;
  }

  /** Deletes the replacement files of an attempt nobody will commit. */
  private void discardWritten(Table table, List<DataFile> written) {
    if (written == null) {
      return;
    }
    for (DataFile file : written) {
      try {
        table.io().deleteFile(file.location());
      } catch (RuntimeException e) {
        LOG.warn("Failed to delete copy-on-write replacement file: {}", file.location(), e);
      }
    }
  }

  private static void checkOnePartitionSpec(Table table, List<DataFile> written) {
    int currentSpecId = table.spec().specId();
    for (DataFile file : written) {
      if (file.specId() != currentSpecId) {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Rewrote %s under partition spec %s while table %s is at spec %s; the rewrite "
                    + "response cannot carry both",
                file.location(),
                file.specId(),
                table.name(),
                currentSpecId));
      }
    }
  }

  private void sendChunked(
      RewriteAssigned assigned, Table table, List<DataFile> written, AtomicBoolean answering) {
    List<List<DataFile>> chunks = splitToFit(assigned, table, written);
    for (int index = 0; index < chunks.size(); index++) {
      // raised before the send, not after it: a send that throws may still have delivered the
      // chunk, and the coordinator counts a chunk it holds whether or not this task heard so
      answering.set(true);
      eventSender.accept(event(assigned, table, chunks.get(index), index, chunks.size()));
    }
  }

  @VisibleForTesting
  List<List<DataFile>> splitToFit(RewriteAssigned assigned, Table table, List<DataFile> written) {
    int maxFiles = Math.max(1, config.copyOnWriteRewriteResponseChunkFiles());
    Deque<List<DataFile>> pending = Lists.newLinkedList();
    for (int start = 0; start < written.size(); start += maxFiles) {
      pending.add(written.subList(start, Math.min(start + maxFiles, written.size())));
    }
    if (pending.isEmpty()) {
      pending.add(ImmutableList.of());
    }

    int maxBytes = config.controlMessageMaxBytes();
    List<List<DataFile>> fitted = Lists.newArrayList();
    while (!pending.isEmpty()) {
      List<DataFile> chunk = pending.removeFirst();
      // the chunk count is one small int wherever it lands, so measuring with a placeholder is
      // exact enough to decide against a limit that already reserves a fifth of the producer's
      int encodedBytes = AvroUtil.encode(event(assigned, table, chunk, 0, 1)).length;
      if (encodedBytes <= maxBytes) {
        fitted.add(chunk);
      } else if (chunk.size() <= 1) {
        throw new PermanentCopyOnWriteException(
            String.format(
                Locale.ROOT,
                "One data file descriptor of table %s encodes to %d bytes, past the %d byte "
                    + "limit, and a single descriptor cannot be split: the producer would reject "
                    + "this response, and the next cycle would rewrite the same rows into a "
                    + "descriptor of the same size. Raise iceberg.kafka.%s and the control "
                    + "topic's %s with it, or narrow write.metadata.metrics.* and rewrite the "
                    + "affected data files -- the metrics of a file are written with the file and "
                    + "do not shrink on their own. Then restart the connector",
                assigned.tableReference().identifier(),
                encodedBytes,
                maxBytes,
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                TopicConfig.MAX_MESSAGE_BYTES_CONFIG));
      } else {
        int half = chunk.size() / 2;
        pending.addFirst(chunk.subList(half, chunk.size()));
        pending.addFirst(chunk.subList(0, half));
      }
    }

    if (fitted.size() > 1) {
      LOG.info(
          "Answering slice {} of change set {} for table {} in {} messages",
          assigned.sliceSeq(),
          assigned.changeSetId(),
          assigned.tableReference().identifier(),
          fitted.size());
    }
    return fitted;
  }

  @VisibleForTesting
  Event event(
      RewriteAssigned assigned,
      Table table,
      List<DataFile> dataFiles,
      int chunkIndex,
      int chunkCount) {
    return new Event(
        config.connectGroupId(),
        new RewriteComplete(
            table.spec().partitionType(),
            assigned.commitId(),
            assigned.tableReference(),
            assigned.changeSetId(),
            assigned.sliceSeq(),
            config.taskId(),
            RewriteComplete.STATUS_OK,
            dataFiles,
            chunkIndex,
            chunkCount));
  }

  private void sendFailure(RewriteAssigned assigned, Table table, int failureKind) {
    if (stopping) {
      // see stop(): a failure the task stops over is answered by silence, like a cancelled rewrite
      LOG.info(
          "Not reporting the failure of slice {} of change set {} for table {}: this task is "
              + "stopping, and the coordinator will time the slice out",
          assigned.sliceSeq(),
          assigned.changeSetId(),
          assigned.tableReference().identifier());
      return;
    }
    eventSender.accept(
        new Event(
            config.connectGroupId(),
            new RewriteComplete(
                table == null ? StructType.of() : table.spec().partitionType(),
                assigned.commitId(),
                assigned.tableReference(),
                assigned.changeSetId(),
                assigned.sliceSeq(),
                config.taskId(),
                RewriteComplete.STATUS_FAILED,
                ImmutableList.of(),
                0,
                1,
                failureKind)));
  }
}
