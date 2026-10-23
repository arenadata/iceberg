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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.connect.data.RecordProjection;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites the data files a change-set slice touches, as one of possibly several owners of that
 * slice.
 *
 * <p>Two independent local phases, with no shared state between them:
 *
 * <ul>
 *   <li><b>my files</b>: a row whose key is in the slice is dropped, unconditionally: whatever its
 *       final state, and whoever owns the key. Every other row is carried over to the remainder.
 *   <li><b>my keys</b>: the final row of every {@code PRESENT} key in this owner's block is
 *       written. Whether anyone found that key in a file is irrelevant and, in a distributed
 *       rewrite, unknowable.
 * </ul>
 *
 * <p>Splitting "who found the old row" from "who writes the new one" makes the rewrite
 * distributable without a second round trip. It relies on the plan being complete for the slice's
 * keys: {@link AffectedFilePlanner} never truncates it. A surviving key is written by its owner
 * even when its row could have stayed in place: that costs extra files, but keeping it in place
 * would tie the owner of the key to the holder of the file, the coupling the two phases break.
 *
 * <p>Runs on a worker, and touches nothing but the {@link Table} handle it is given.
 */
public final class CopyOnWriteRewriter {

  private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteRewriter.class);

  private final Table table;
  private final int rewriteThreads;
  private final ExecutorService rewriteExec;

  /**
   * A rewriter whose chunks run on threads of its own, up to {@code rewriteThreads} per phase. That
   * bounds one call, not several at once.
   */
  public CopyOnWriteRewriter(Table table, int rewriteThreads) {
    this(table, rewriteThreads, null);
  }

  /**
   * A rewriter whose every unit of work (anything holding a data file open, in either phase) runs
   * on {@code rewriteExec}, never on the calling thread.
   *
   * <p>Sharing one executor between all assignments of a task makes {@code rewrite-threads} a limit
   * on the task. The caller must not itself run on {@code rewriteExec}: callers waiting in the
   * slots their own chunks queue for deadlock once they take every slot.
   *
   * @param rewriteThreads how many chunks each phase of one call is split into
   */
  public CopyOnWriteRewriter(Table table, int rewriteThreads, ExecutorService rewriteExec) {
    this.table = table;
    this.rewriteThreads = Math.max(1, rewriteThreads);
    this.rewriteExec = rewriteExec;
  }

  /** Rewrites as the sole owner of the slice: the single-process case. */
  public List<DataFile> rewrite(
      long baseSnapshotId, List<FileScanTask> fileScanTasks, ChangeSetSlice slice) {
    return rewrite(baseSnapshotId, fileScanTasks, slice, 0, 1);
  }

  /** Rewrites this owner's share of a slice to the end, for a caller that never abandons one. */
  public List<DataFile> rewrite(
      long baseSnapshotId,
      List<FileScanTask> fileScanTasks,
      ChangeSetSlice slice,
      int ownerIndex,
      int ownerCount) {
    return rewrite(baseSnapshotId, fileScanTasks, slice, ownerIndex, ownerCount, () -> false);
  }

  /**
   * Rewrites this owner's share of a slice, unless the caller gives up on it first.
   *
   * @param baseSnapshotId the snapshot {@code fileScanTasks} was planned against; not read here, it
   *     is the caller's business for {@code validateFromSnapshot()} on commit
   * @param fileScanTasks the files assigned to this owner, disjoint from every other owner's
   * @param slice the whole normalized slice: every owner sees all of it and derives its own block
   * @param ownerIndex this owner's position in the sorted list of active task ids
   * @param ownerCount how many active tasks share this slice
   * @param cancelled whether the caller has given up on this call, checked between files and
   *     between rows in both phases: once true, the call deletes what it wrote and throws {@link
   *     CancelledCopyOnWriteException}
   * @return the replacement data files: untouched rows carried over, plus this owner's keys
   */
  public List<DataFile> rewrite(
      long baseSnapshotId,
      List<FileScanTask> fileScanTasks,
      ChangeSetSlice slice,
      int ownerIndex,
      int ownerCount,
      BooleanSupplier cancelled) {
    List<NestedField> orderedIdFields = slice.identifierFields();
    Schema stagedSchema =
        StagedChangeSchema.stagedSchema(
            table.schema(),
            orderedIdFields.stream().map(NestedField::fieldId).collect(Collectors.toSet()));
    Schema tableSchema = table.schema();
    long targetFileSizeBytes =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    String operationId = UUID.randomUUID().toString();
    AtomicInteger nextWriterId = new AtomicInteger();

    // filled as each writer closes rather than returned from the phase: a phase that throws returns
    // nothing, and the files its successful chunks wrote would never be deleted
    List<DataFile> written = Lists.newCopyOnWriteArrayList();
    try {
      carryOverMyFiles(
          fileScanTasks,
          slice,
          tableSchema,
          orderedIdFields,
          operationId,
          nextWriterId,
          targetFileSizeBytes,
          written,
          cancelled);
      emitMyKeys(
          slice,
          stagedSchema,
          ownerIndex,
          ownerCount,
          operationId,
          nextWriterId,
          targetFileSizeBytes,
          written,
          cancelled);
    } catch (RuntimeException e) {
      // nothing will ever reference these: a failed slice is cancelled whole, a cancelled call is
      // answered by nobody, and only this owner knows what it managed to write before stopping
      deleteQuietly(written);
      throw e instanceof FileNotFound ? classify((FileNotFound) e) : e;
    } catch (Error e) {
      // an OutOfMemoryError on a row group needs the same cleanup
      deleteQuietly(written);
      throw e;
    }

    return ImmutableList.copyOf(written);
  }

  /**
   * A read of one planned file that failed because some file was not in storage.
   *
   * <p>Carries the task out of the phase, so the failure is classified once every thread of the
   * phase has stopped: {@link #classify} refreshes the table handle they all share.
   */
  private static final class FileNotFound extends RuntimeException {
    private final transient FileScanTask task;

    private FileNotFound(FileScanTask task, RuntimeException cause) {
      super(cause);
      this.task = task;
    }
  }

  private static boolean isNotFound(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof NotFoundException || cause instanceof FileNotFoundException) {
        return true;
      }
    }
    return false;
  }

  /**
   * A planned data file missing from storage while the table still lists it is a permanent failure:
   * every replan hands out the same file, and every rewrite fails on it the same way. Anything else
   * not found stays the failure it was.
   *
   * <p>The table is refreshed first: this handle is as old as the assignment, and a compaction plus
   * {@code expire_snapshots} since then leaves a file the next plan never asks for. A missing
   * delete file, or a check that cannot be made, leaves the original failure to be retried.
   */
  private RuntimeException classify(FileNotFound notFound) {
    RuntimeException failure = (RuntimeException) notFound.getCause();
    String location = notFound.task.file().location();
    long snapshotId;
    try {
      if (exists(location)) {
        return failure;
      }
      table.refresh();
      if (!listedInCurrentSnapshot(notFound.task)) {
        return failure;
      }
      snapshotId = table.currentSnapshot().snapshotId();
    } catch (RuntimeException e) {
      failure.addSuppressed(e);
      return failure;
    }

    PermanentCopyOnWriteException permanent =
        new PermanentCopyOnWriteException(
            String.format(
                Locale.ROOT,
                "Data file %s of table %s is missing from storage, yet snapshot %s still lists it: "
                    + "it was deleted outside Iceberg, by a storage lifecycle rule or by "
                    + "remove_orphan_files with too short an older-than, and every rewrite that "
                    + "reads it fails the same way. Restore the file (object versioning, a backup) "
                    + "and restart the connector; if it cannot be restored, remove it from the "
                    + "table with DeleteFiles, which loses its rows, and restart the connector",
                location,
                table.name(),
                snapshotId));
    permanent.addSuppressed(failure);
    return permanent;
  }

  private boolean exists(String location) {
    try {
      return table.io().newInputFile(location).exists();
    } catch (NotFoundException e) {
      return false;
    }
  }

  private boolean listedInCurrentSnapshot(FileScanTask task) {
    try (CloseableIterable<DataFile> files =
        FindFiles.in(table).inPartition(task.spec(), task.file().partition()).collect()) {
      for (DataFile file : files) {
        if (file.location().equals(task.file().location())) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Phase "my files": every row whose key the slice touches is dropped, the rest carried over. */
  private void carryOverMyFiles(
      List<FileScanTask> fileScanTasks,
      ChangeSetSlice slice,
      Schema tableSchema,
      List<NestedField> orderedIdFields,
      String operationId,
      AtomicInteger nextWriterId,
      long targetFileSizeBytes,
      List<DataFile> written,
      BooleanSupplier cancelled) {
    List<List<FileScanTask>> chunks = split(fileScanTasks, rewriteThreads);
    if (chunks.isEmpty()) {
      return;
    }

    runUnits(
        chunks,
        "carry-over",
        chunk ->
            carryOverChunk(
                chunk,
                slice,
                tableSchema,
                orderedIdFields,
                operationId,
                nextWriterId.getAndIncrement(),
                targetFileSizeBytes,
                written,
                cancelled));
  }

  private void carryOverChunk(
      List<FileScanTask> chunk,
      ChangeSetSlice slice,
      Schema tableSchema,
      List<NestedField> orderedIdFields,
      String operationId,
      int writerId,
      long targetFileSizeBytes,
      List<DataFile> written,
      BooleanSupplier cancelled) {
    // one builder per chunk, never shared: it wraps rows by mutating a single view
    IdentifierKeys.KeyBuilder keyBuilder = IdentifierKeys.keyBuilder(tableSchema, orderedIdFields);
    ClusteredDataWriter<Record> remainderWriter =
        newClusteredWriter(operationId, writerId, targetFileSizeBytes);

    try {
      for (FileScanTask task : chunk) {
        stopIfCancelled(cancelled);
        try (CloseableIterable<Record> rows =
            AffectedFileReader.open(table.io(), task, table.schema(), table.schema())) {
          for (Record row : rows) {
            stopIfCancelled(cancelled);
            StructLike key = keyBuilder.keyOf(row);
            // in the slice at all -> gone from here, whatever its final state and whoever owns it
            if (!slice.get(key).isPresent()) {
              remainderWriter.write(row, task.spec(), task.file().partition());
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to read " + task.file().location(), e);
        } catch (RuntimeException e) {
          // classified by rewrite(), once no thread of this phase is using the table any more
          throw isNotFound(e) ? new FileNotFound(task, e) : e;
        }
      }
    } finally {
      closeAndCollect(remainderWriter, written);
    }
  }

  /** Phase "my keys": the final row of every {@code PRESENT} key in this owner's block. */
  private void emitMyKeys(
      ChangeSetSlice slice,
      Schema stagedSchema,
      int ownerIndex,
      int ownerCount,
      String operationId,
      AtomicInteger nextWriterId,
      long targetFileSizeBytes,
      List<DataFile> written,
      BooleanSupplier cancelled) {
    List<Map.Entry<StructLike, ChangeRecord>> owned = slice.ownedEntries(ownerIndex, ownerCount);
    if (owned.isEmpty()) {
      return;
    }

    // one sequential clustered pass over the whole assignment, not split into rewriteThreads
    // pieces: a key's partition does not correlate with its position in the ownership block, so
    // splitting first and clustering each piece separately would recur the same partition across
    // several pieces and multiply small files by rewriteThreads
    runUnits(
        ImmutableList.of(owned),
        "emit",
        block ->
            emitBlock(
                block,
                stagedSchema,
                operationId,
                nextWriterId.getAndIncrement(),
                targetFileSizeBytes,
                written,
                cancelled));
  }

  /**
   * Runs one unit of work per item, stopping at the first failure and throwing it once the units
   * already running have finished.
   *
   * <p>On the task's executor whenever there is one, a single unit included: a unit run on the
   * calling thread would be one more than the task's limit allows.
   */
  private <T> void runUnits(List<T> items, String phase, Tasks.Task<T, RuntimeException> unit) {
    ExecutorService own =
        rewriteExec == null && items.size() > 1 ? newRewriteExecutor(phase) : null;
    try {
      Tasks.foreach(items)
          .executeWith(rewriteExec != null ? rewriteExec : own)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .run(unit);
    } finally {
      if (own != null) {
        own.shutdown();
      }
    }
  }

  /**
   * One block of owned keys: projected onto the table schema, sorted by partition, written through
   * one clustered writer.
   *
   * <p>{@link RecordProjection} reuses a single view, nested structs included, so each block builds
   * its own and every row is copied out before it is kept for sorting: otherwise every row would
   * end up with the last row's nested values.
   */
  private void emitBlock(
      List<Map.Entry<StructLike, ChangeRecord>> block,
      Schema stagedSchema,
      String operationId,
      int writerId,
      long targetFileSizeBytes,
      List<DataFile> written,
      BooleanSupplier cancelled) {
    RecordProjection stagedToTable = RecordProjection.create(stagedSchema, table.schema());
    // partition transforms take Iceberg's internal values (days, micros, ByteBuffer) and a
    // generic row holds LocalDate, OffsetDateTime, byte[]. Per block, like the projection: it wraps
    // by mutating a single view
    InternalRecordWrapper internal = new InternalRecordWrapper(table.schema().asStruct());

    // partition keys computed once per row, not O(n log n) times inside the sort comparator
    List<PartitionedRow> rows = Lists.newArrayListWithExpectedSize(block.size());
    for (Map.Entry<StructLike, ChangeRecord> entry : block) {
      stopIfCancelled(cancelled);
      if (entry.getValue().state() == ChangeRecord.State.PRESENT) {
        Record row = stagedToTable.wrap(entry.getValue().row()).copy();
        rows.add(new PartitionedRow(row, partitionKeyOf(internal.wrap(row))));
      }
    }
    if (rows.isEmpty()) {
      return;
    }

    Comparator<StructLike> partitionComparator = Comparators.forType(table.spec().partitionType());
    rows.sort((a, b) -> partitionComparator.compare(a.partitionKey, b.partitionKey));

    ClusteredDataWriter<Record> writer =
        newClusteredWriter(operationId, writerId, targetFileSizeBytes);
    try {
      for (PartitionedRow row : rows) {
        stopIfCancelled(cancelled);
        writer.write(row.row, table.spec(), row.partitionKey);
      }
    } finally {
      closeAndCollect(writer, written);
    }
  }

  /** A row together with the partition it belongs to, so the transforms run once. */
  private static final class PartitionedRow {
    private final Record row;
    private final PartitionKey partitionKey;

    private PartitionedRow(Record row, PartitionKey partitionKey) {
      this.row = row;
      this.partitionKey = partitionKey;
    }
  }

  private PartitionKey partitionKeyOf(StructLike internalRow) {
    PartitionKey key = new PartitionKey(table.spec(), table.schema());
    key.partition(internalRow);
    return key;
  }

  private ClusteredDataWriter<Record> newClusteredWriter(
      String operationId, int writerId, long targetFileSizeBytes) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, writerId, 0L)
            .operationId(operationId + "-copy-on-write-" + writerId)
            .build();
    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).build();
    return new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), targetFileSizeBytes);
  }

  /**
   * Splits an already {@code (specId, partition)}-sorted plan into up to {@code rewriteThreads}
   * contiguous chunks, so each thread's {@code ClusteredDataWriter} still sees clustered input.
   */
  private static List<List<FileScanTask>> split(List<FileScanTask> plan, int rewriteThreads) {
    if (plan.isEmpty()) {
      return ImmutableList.of();
    }

    List<FileScanTask> sorted = Lists.newArrayList(plan);
    sorted.sort(FileScanTaskOrder.bySpecAndPartition(plan));
    return splitEvenly(sorted, rewriteThreads);
  }

  /** Contiguous, near-equal blocks: order within a block is preserved. */
  private static <T> List<List<T>> splitEvenly(List<T> items, int blockCount) {
    if (items.isEmpty()) {
      return ImmutableList.of();
    }

    int blocks = Math.min(blockCount, items.size());
    int blockSize = (items.size() + blocks - 1) / blocks;
    List<List<T>> result = Lists.newArrayList();
    for (int start = 0; start < items.size(); start += blockSize) {
      result.add(items.subList(start, Math.min(start + blockSize, items.size())));
    }
    return result;
  }

  private static ExecutorService newRewriteExecutor(String phase) {
    return Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("iceberg-copy-on-write-" + phase + "-%d")
            .build());
  }

  /**
   * Stops the call once its caller has given up on it: see {@link CancelledCopyOnWriteException}.
   */
  private void stopIfCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean()) {
      throw new CancelledCopyOnWriteException(
          String.format(
              Locale.ROOT, "Rewrite of table %s was cancelled by its caller", table.name()));
    }
  }

  private void deleteQuietly(List<DataFile> files) {
    for (DataFile file : files) {
      try {
        table.io().deleteFile(file.location());
      } catch (RuntimeException e) {
        LOG.warn("Failed to delete copy-on-write replacement file: {}", file.location(), e);
      }
    }
  }

  /**
   * Closes a writer and records whatever it managed to write, failure or not.
   *
   * <p>Collected in a {@code finally}: files nobody knows about are files nobody deletes.
   */
  private static void closeAndCollect(ClusteredDataWriter<Record> writer, List<DataFile> written) {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      try {
        written.addAll(writer.result().dataFiles());
      } catch (RuntimeException e) {
        LOG.warn("Could not list the files written before the failure; they may be left behind", e);
      }
    }
  }
}
