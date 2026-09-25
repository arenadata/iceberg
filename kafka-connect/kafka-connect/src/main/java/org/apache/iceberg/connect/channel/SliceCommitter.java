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

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Iceberg commit of one slice, and nothing else.
 *
 * <p>The only place in copy-on-write where {@code OverwriteFiles} and the snapshot properties live.
 * It is a class of its own because of what surrounds it rather than what it is: the moment {@link
 * #commitSlice} returns, the replacement files stop belonging to the attempt and start belonging to
 * the table, and no failure after that point may delete one. Keeping the commit behind one call is
 * what lets {@code CopyOnWriteTableCommitter.finishSlice} put that boundary in the structure of its
 * code instead of in a comment.
 */
class SliceCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(SliceCommitter.class);

  private final IcebergSinkConfig config;
  private final String offsetsProp;

  SliceCommitter(IcebergSinkConfig config) {
    this.config = config;
    this.offsetsProp = ChangeSetStore.offsetsProp(config);
  }

  /**
   * Commits one slice through {@code OverwriteFiles}, validated from the base snapshot it was
   * planned against, or, with no base, against every data file the branch has.
   *
   * <p>The commit and nothing after it. Finding the snapshot it produced is a catalog request of
   * its own, and one that fails here would reach the caller's handler while the files are still the
   * attempt's; see {@link #reportedSnapshotId}.
   */
  void commitSlice(TableDrainState state) {
    Table table =
        committingOnlyTo(state.table, state.manifest.tableUuid(), identifierFieldsToHold(state));
    OffsetDateTime validThroughTs = state.drained ? state.validThroughTs : null;

    if (state.baseSnapshotId == null) {
      // nothing was planned, so a data file the branch holds by now arrived after the slice was
      // cut. Without a starting snapshot Iceberg validates the whole history of the branch, and the
      // conflict sends the slice back to be planned against that data. An append would leave its
      // rows beside rows of the same keys, and its deletes unapplied
      OverwriteFiles overwriteOp =
          table
              .newOverwrite()
              .conflictDetectionFilter(Expressions.alwaysTrue())
              .validateNoConflictingData();
      if (state.branch != null) {
        overwriteOp.toBranch(state.branch);
      }
      applyCommitProperties(overwriteOp, state, validThroughTs);
      state.attempt.collected().forEach(overwriteOp::addFile);
      overwriteOp.commit();
    } else {
      List<DataFile> oldDataFiles =
          state.planFiles.stream().map(FileScanTask::file).collect(Collectors.toList());
      // only file-scoped position deletes are safe to drop here: a partition-scoped one may also
      // cover a data file this slice never touched
      List<DeleteFile> fileScopedDeletes =
          state.planFiles.stream()
              .flatMap(task -> task.deletes().stream())
              .filter(d -> d.content() == FileContent.POSITION_DELETES)
              .filter(ContentFileUtil::isFileScoped)
              .collect(Collectors.toList());

      OverwriteFiles overwriteOp =
          table
              .newOverwrite()
              .validateFromSnapshot(state.baseSnapshotId)
              .conflictDetectionFilter(state.conflictFilter)
              .validateNoConflictingData()
              .validateNoConflictingDeletes();
      if (state.branch != null) {
        overwriteOp.toBranch(state.branch);
      }
      applyCommitProperties(overwriteOp, state, validThroughTs);
      overwriteOp.deleteFiles(DataFileSet.of(oldDataFiles), DeleteFileSet.of(fileScopedDeletes));
      state.attempt.collected().forEach(overwriteOp::addFile);
      overwriteOp.commit();
    }
  }

  /**
   * The identifier fields a commit of this slice must find in the metadata it goes on top of, or
   * null when the table's schema does not decide them.
   *
   * <p>Only a table keyed by its own identifier fields can have them changed under a running slice.
   * {@code id-columns} in the connector configuration changes with a restart of the tasks, and that
   * takes the coordinator down with it.
   */
  private Set<Integer> identifierFieldsToHold(TableDrainState state) {
    String name = state.tableReference.identifier().toString();
    if (!config.tableConfig(name).idColumns().isEmpty()) {
      return null;
    }
    return state.manifest.identifierFieldIds();
  }

  static Snapshot baseSnapshot(Table table, String branch) {
    if (branch != null && !table.refs().containsKey(branch)) {
      return table.currentSnapshot();
    }
    return ChangeSetStore.latestSnapshot(table, branch);
  }

  Long committedSnapshotId(TableDrainState state) {
    Table table = state.table;
    String commitId = state.commitId.toString();
    String sliceSeq = String.valueOf(state.sliceSeq);

    table.refresh();
    Snapshot snapshot = ChangeSetStore.latestSnapshot(table, state.branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      if (commitId.equals(summary.get(ChangeSetStore.COMMIT_ID_PROP))
          && sliceSeq.equals(summary.get(ChangeSetStore.SLICE_SEQ_PROP))) {
        return snapshot.snapshotId();
      }
      if (state.baseSnapshotId != null && snapshot.snapshotId() == state.baseSnapshotId) {
        // walked past our own commit: it cannot be an ancestor of the base it was validated against
        break;
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return null;
  }

  /**
   * True if {@code failure}, thrown by {@link #commitSlice}, leaves open whether the snapshot
   * exists: {@code CommitStateUnknownException}, or the thread interrupted in the middle of the
   * commit. Which exception a catalog client throws for an interruption is up to the client, so it
   * is recognised by the thread's flag or by a cause. Every other failure means the commit did not
   * happen: Iceberg classifies them the same way, cleaning up after them in {@code
   * SnapshotProducer.commit} and after {@code CommitStateUnknownException} not.
   */
  static boolean outcomeUnknown(RuntimeException failure) {
    if (Thread.currentThread().isInterrupted()) {
      return true;
    }

    Set<Throwable> seen = Sets.newIdentityHashSet();
    for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
      if (cause instanceof CommitStateUnknownException
          || cause instanceof InterruptedException
          || cause instanceof InterruptedIOException
          || cause instanceof ClosedByInterruptException) {
        return true;
      }
    }
    return false;
  }

  Long reportedSnapshotId(TableDrainState state) {
    try {
      Long snapshotId = committedSnapshotId(state);
      if (snapshotId == null) {
        LOG.warn(
            "Could not find the snapshot of slice {} (commit {}) of table {} in its branch; "
                + "reporting none",
            state.sliceSeq,
            state.commitId,
            state.tableReference.identifier());
      }
      return snapshotId;
    } catch (RuntimeException e) {
      LOG.warn(
          "Could not look up the snapshot of slice {} (commit {}) of table {}; reporting none",
          state.sliceSeq,
          state.commitId,
          state.tableReference.identifier(),
          e);
      return null;
    }
  }

  /**
   * {@code table}, except that a commit through it lands in the table {@code tableUuid} or nowhere.
   *
   * <p>Every commit attempt refreshes the table first, and a refresh goes by name: a REST catalog
   * quietly hands back whatever table the name means by then. A table dropped and created again
   * while the slice was rewritten would take the slice, and with no base, nothing in its empty
   * history conflicts. The check at the start of the slice cannot close that window, so the commit
   * checks the metadata it goes on top of, and the catalog holds it to that metadata: a REST commit
   * requires its UUID, a metastore its location.
   *
   * <p>The same metadata holds the commit to {@code identifierFieldIds}. Iceberg validates a commit
   * against the data that landed since its base, never against the schema's identifier fields, so a
   * slice keyed by fields changed while it was rewritten would commit and leave rows that share a
   * key under the fields the table has now. Checked on every attempt, the retries {@code commit()}
   * makes on top of refreshed metadata included; what is left is the window between this check and
   * the catalog's swap of the metadata, which no Iceberg API makes atomic.
   *
   * <p>A manifest without a table UUID is not checked for it, as on recovery, and null identifier
   * fields are not checked at all. A table that is not a {@code BaseTable} is checked for neither:
   * without its operations there is nothing to hold the commit to.
   */
  private static Table committingOnlyTo(
      Table table, UUID tableUuid, Set<Integer> identifierFieldIds) {
    if ((tableUuid == null && identifierFieldIds == null) || !(table instanceof BaseTable)) {
      return table;
    }
    BaseTable loaded = (BaseTable) table;
    return new BaseTable(
        new SameTableOperations(loaded.operations(), loaded.name(), tableUuid, identifierFieldIds),
        loaded.name(),
        loaded.reporter());
  }

  static class TableReplacedException extends RuntimeException implements CleanableFailure {
    TableReplacedException(String message) {
      super(message);
    }
  }

  static class IdentifierFieldsChangedException extends RuntimeException
      implements CleanableFailure {
    IdentifierFieldsChangedException(
        String name, Set<Integer> committedUnder, Set<Integer> committingOnto) {
      super(
          String.format(
              Locale.ROOT,
              "Table %s identifier fields changed: the commit is keyed by %s, the table is now "
                  + "keyed by %s",
              name,
              committedUnder,
              committingOnto));
    }
  }

  private static final class SameTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final String name;
    private final UUID tableUuid;
    private final Set<Integer> identifierFieldIds;

    private SameTableOperations(
        TableOperations delegate, String name, UUID tableUuid, Set<Integer> identifierFieldIds) {
      this.delegate = delegate;
      this.name = name;
      this.tableUuid = tableUuid;
      this.identifierFieldIds = identifierFieldIds;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      // base is what this attempt's refresh read by name
      UUID committingOnto =
          base == null || base.uuid() == null ? null : UUID.fromString(base.uuid());
      if (tableUuid != null && !tableUuid.equals(committingOnto)) {
        throw new TableReplacedException(
            String.format(
                Locale.ROOT,
                "Table %s was replaced: the commit is for table %s, the name now means table %s",
                name,
                tableUuid,
                committingOnto));
      }
      if (identifierFieldIds != null && base != null) {
        Set<Integer> keyedBy = base.schema().identifierFieldIds();
        if (!identifierFieldIds.equals(keyedBy)) {
          throw new IdentifierFieldsChangedException(name, identifierFieldIds, keyedBy);
        }
      }
      delegate.commit(base, metadata);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }

  private void applyCommitProperties(
      SnapshotUpdate<?> op, TableDrainState state, OffsetDateTime validThroughTs) {
    ChangeSetManifest manifest = state.manifest;
    op.set(ChangeSetStore.COMMIT_ID_PROP, state.commitId.toString());
    // one commit id spans every snapshot of a drain; the slice sequence keeps the pair unique:
    // for outside tooling, and for committedSnapshotId
    op.set(ChangeSetStore.SLICE_SEQ_PROP, String.format(Locale.ROOT, "%d", state.sliceSeq));
    // the frozen position, on every commit of this change set including the exhausting one. It is
    // the watermark of what this change set accounts for, and nothing else may be read into it:
    // responses that arrived after the freeze (or, on a resumed drain, that were buffered when
    // it resumed) belong to no change set yet, and moving the watermark past them would have
    // the next freeze filter them out as already applied and drop them silently
    op.set(offsetsProp, ChangeSetStore.offsetsToJson(manifest.controlOffsets()));
    if (state.drained) {
      if (validThroughTs != null) {
        op.set(ChangeSetStore.VALID_THROUGH_TS_PROP, validThroughTs.toString());
      }
    } else {
      StructLike newCursor = state.slice.lastKey().orElse(state.cursor);
      op.set(ChangeSetStore.CHANGE_SET_ID_PROP, manifest.changeSetId().toString());
      op.set(
          ChangeSetStore.CHANGE_SET_CURSOR_PROP,
          SingleValueParser.toJson(manifest.cursorType(state.table.schema()), newCursor));
    }
  }
}
