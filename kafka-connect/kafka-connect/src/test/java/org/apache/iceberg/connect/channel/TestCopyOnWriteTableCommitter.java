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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.RecordRoutingStrategy;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Functional tests for {@link CopyOnWriteTableCommitter}: freezing a change set, draining it one
 * slice at a time through {@code OverwriteFiles}/{@code AppendFiles}, and the state (snapshot
 * properties plus manifest) that makes it resumable.
 *
 * <p>Driven through {@link CopyOnWriteRewriteDriver}, which plays the coordinator loop and the
 * workers. What these assert about the table is the contract distributing the rewrite was not
 * allowed to alter, which is why {@link TestCopyOnWriteTableCommitterMultiTask} reruns every one of
 * them across three tasks without changing a line.
 */
public class TestCopyOnWriteTableCommitter {

  private static final String CTL_TOPIC = "ctl-topic";
  private static final String GROUP_ID = "cg-connect";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String COPY_ON_WRITE_CHANGE_SET_ID_PROP =
      "kafka.connect.copy-on-write.change-set-id";
  private static final String COPY_ON_WRITE_CURSOR_PROP =
      "kafka.connect.copy-on-write.change-set-cursor";
  private static final String SLICE_SEQ_PROP = "kafka.connect.copy-on-write.slice-seq";
  private static final String VALID_THROUGH_TS_PROP = "kafka.connect.valid-through-ts";
  private static final String OFFSETS_PROP = "kafka.connect.offsets." + CTL_TOPIC + "." + GROUP_ID;

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);

  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private List<Event> sentEvents;
  private RecordingMetadataEvents metadataEvents;
  private CopyOnWriteRewriteDriver committer;

  /** Captures the source topics of every lineage event the commit path emits. */
  private static final class RecordingMetadataEvents extends MetadataEvents {
    private final List<Set<String>> lineageTopics = Lists.newArrayList();

    private RecordingMetadataEvents() {
      super(null, null, null, null, null);
    }

    @Override
    public void lineageCommit(
        Set<String> sourceTopics, TableIdentifier targetTable, Schema targetSchema) {
      lineageTopics.add(sourceTopics);
    }
  }

  /**
   * How many tasks share the rewrite. Overridden by {@link TestCopyOnWriteTableCommitterMultiTask}
   * to run every scenario below distributed as well: same assertions, same expected table, more
   * owners.
   */
  protected int taskCount() {
    return 1;
  }

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn(GROUP_ID);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.copyOnWriteStagingLocation()).thenReturn(null);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1_000_000L);
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(Long.MAX_VALUE);
    when(config.copyOnWritePruningMaxInCardinality()).thenReturn(1000);
    when(config.copyOnWriteCommitRetries()).thenReturn(2);
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(600_000L);
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
    when(config.controlMessageMaxBytes()).thenReturn(1024 * 1024);
    // sweeping is exercised by TestStagingSweeper; here it would only race the drain it observes
    when(config.copyOnWriteStagingOrphanCleanupIntervalMs()).thenReturn(Long.MAX_VALUE);
    when(config.copyOnWriteStagingOrphanTtlMs()).thenReturn(86_400_000L);

    sentEvents = Lists.newArrayList();
    metadataEvents = new RecordingMetadataEvents();
    // the rewrite lives on the workers, so the committer is driven through the protocol rather
    // than called once: it starts a slice and returns, and the drain advances as answers arrive
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testUpdateCommitsThroughOverwriteFilesWithNoDeleteFiles() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));

    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L);
    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"), tuple(3L, "c"));
    assertThat(hasAnyDeleteFile(table)).isFalse();
    assertThat(sentEvents).hasSize(1);
    assertThat(((CommitToTable) sentEvents.get(0).payload()).tableReference())
        .isEqualTo(TABLE_REFERENCE);
  }

  @Test
  public void testDeleteRemovesRow() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));

    TableCommitRequest request = requestOf(stagedFiles(delete(2L)), 0, 5L, 6L);
    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(3L, "c"));
  }

  @Test
  public void testMixedBatchInsertUpdateDelete() {
    appendRows(table, row(1L, "a"), row(2L, "b"));

    TableCommitRequest request =
        requestOf(stagedFiles(insert(100L, "new"), update(1L, "a2"), delete(2L)), 0, 5L, 6L);
    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(100L, "new"));
  }

  @Test
  public void testASliceTakesAlongOnlyThePositionDeletesScopedToTheFilesItRewrites() {
    // a table leaving a merge-on-read period, with every kind of delete file it leaves behind. A
    // position delete scoped to a rewritten file would dangle, so the commit drops it; one scoped
    // to the partition, or an equality delete, also covers rows of a file the slice never rewrote
    DataFile rewritten = appendFile(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"));
    DataFile untouched = appendFile(row(10L, "j"), row(11L, "k"), row(12L, "l"));
    DeleteFile fileScoped = positionDelete(List.of(Pair.of(rewritten.location(), 3L)));
    DeleteFile partitionScoped =
        positionDelete(
            List.of(Pair.of(rewritten.location(), 1L), Pair.of(untouched.location(), 1L)));
    // its id bounds cover the slice's key; one whose bounds miss every key is tested separately
    // below
    DeleteFile equality = equalityDelete(1L, 12L);
    RowDelta rowDelta = table.newRowDelta();
    List.of(fileScoped, partitionScoped, equality).forEach(rowDelta::addDeletes);
    rowDelta.commit();
    assertThat(ContentFileUtil.isFileScoped(fileScoped)).isTrue();
    assertThat(ContentFileUtil.isFileScoped(partitionScoped)).isFalse();

    TableCommitter.Outcome outcome =
        committer.commit(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    // dropped with the rewritten file, a delete that also names 11 or 12 would bring them back
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(3L, "c"), tuple(10L, "j"));
    Snapshot slice = table.currentSnapshot();
    assertThat(slice.removedDataFiles(table.io()))
        .extracting(DataFile::location)
        .containsExactly(rewritten.location());
    assertThat(slice.removedDeleteFiles(table.io()))
        .extracting(DeleteFile::location)
        .as("only the position delete that named nothing but the rewritten file")
        .containsExactly(fileScoped.location());
  }

  @Test
  public void testARowAnEqualityDeleteRemovedStaysDeletedWhenTheDeleteMissesTheSliceKeys() {
    // a merge-on-read period deleted key 3 by equality from a file that also holds key 1. The
    // slice changes key 1 only, yet its rewrite carries the file's other rows over, into a file
    // with a newer sequence number that no older equality delete reaches any more
    appendFile(row(1L, "a"), row(3L, "c"));
    table.newRowDelta().addDeletes(equalityDelete(3L)).commit();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a"));

    TableCommitter.Outcome outcome =
        committer.commit(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));
  }

  @Test
  public void testARowAGlobalEqualityDeleteOfAnOlderSpecRemovedStaysDeleted() {
    // the table was partitioned after its merge-on-read period, which left an equality delete under
    // the unpartitioned spec. Such a delete applies to data files of every spec, and the planned
    // file is under the current one: the check on plan files passes, the delete has to reach the
    // worker as it is
    PartitionSpec unpartitioned = table.spec();
    table.updateSpec().addField("data").commit();
    table.refresh();
    appendFile(row(1L, "a"), row(3L, "a"));
    table.newRowDelta().addDeletes(equalityDelete(unpartitioned, 3L)).commit();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a"));

    TableCommitter.Outcome outcome =
        committer.commit(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));
  }

  @Test
  public void testAnEmptyTableTakesTheSliceAsAppendedRowsAndItsDeletesAsNoOps() {
    Table emptyTable = catalog.createTable(TableIdentifier.of(NAMESPACE, "emptytbl"), SCHEMA);
    TableReference emptyRef =
        TableReference.of("catalog", TableIdentifier.of(NAMESPACE, "emptytbl"));

    List<StagedChangeFile> files =
        stagedFiles(emptyTable, emptyRef, insert(1L, "a"), update(2L, "b"), delete(3L));
    TableCommitRequest request = requestOf(emptyRef, files, 0, 5L, 6L);

    TableCommitter.Outcome outcome = committer.commit(request);
    emptyTable.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(emptyTable)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(2L, "b"));
    // nothing to overwrite, so the snapshot is an append whichever update committed it
    assertThat(emptyTable.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);
    assertThat(emptyTable.currentSnapshot().summary())
        .containsKeys(COMMIT_ID_SNAPSHOT_PROP, OFFSETS_PROP);
  }

  @Test
  public void testAnEmptyTableDrainedOverSeveralSlicesNamesItsChangeSetFromTheFirst() {
    Table emptyTable = catalog.createTable(TableIdentifier.of(NAMESPACE, "emptytbl"), SCHEMA);
    TableReference emptyRef =
        TableReference.of("catalog", TableIdentifier.of(NAMESPACE, "emptytbl"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    List<StagedChangeFile> files =
        stagedFiles(emptyTable, emptyRef, insert(1L, "a"), insert(2L, "b"));
    TableCommitter.Outcome outcome = committer.commit(requestOf(emptyRef, files, 0, 5L, 6L));
    emptyTable.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(emptyTable)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(2L, "b"));
    // the first slice commits with no base. Without the pointer and the cursor on its snapshot, a
    // restart before the next slice would leave the drain nothing to resume from
    Snapshot unbased =
        Iterables.getOnlyElement(
            Iterables.filter(emptyTable.snapshots(), snapshot -> snapshot.parentId() == null));
    assertThat(unbased.summary())
        .containsKeys(
            COMMIT_ID_SNAPSHOT_PROP,
            OFFSETS_PROP,
            COPY_ON_WRITE_CHANGE_SET_ID_PROP,
            COPY_ON_WRITE_CURSOR_PROP);
    assertThat(emptyTable.currentSnapshot().summary())
        .doesNotContainKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
  }

  @Test
  public void testTheFirstCommitToANewBranchOfAnEmptyTableCreatesTheBranch() {
    Table emptyTable = catalog.createTable(TableIdentifier.of(NAMESPACE, "emptytbl"), SCHEMA);
    TableReference emptyRef =
        TableReference.of("catalog", TableIdentifier.of(NAMESPACE, "emptytbl"));
    String branch = "new-branch";
    TableSinkConfig branchConfig = tableConfigWithBranch(branch);
    when(config.tableConfig(any())).thenReturn(branchConfig);

    List<StagedChangeFile> files = stagedFiles(emptyTable, emptyRef, update(1L, "a"), delete(2L));
    TableCommitter.Outcome outcome = committer.commit(requestOf(emptyRef, files, 0, 5L, 6L));
    emptyTable.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAllOnRef(emptyTable, branch)).containsExactly(tuple(1L, "a"));
    assertThat(emptyTable.currentSnapshot()).as("main is untouched").isNull();
  }

  @Test
  public void testTheFirstCommitToANewBranchOfATableWithDataRewritesTheRowsOfMain() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    long mainHead = table.currentSnapshot().snapshotId();
    String branch = "new-branch";
    TableSinkConfig branchConfig = tableConfigWithBranch(branch);
    when(config.tableConfig(any())).thenReturn(branchConfig);

    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2"), delete(2L)), 0, 5L, 6L);
    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    // Iceberg forks a branch that does not exist yet from the head of main, so the slice is planned
    // against main's rows: appended beside them it would leave key 1 twice and key 2 not deleted
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAllOnRef(table, branch)).containsExactly(tuple(1L, "a2"));
    assertThat(table.snapshot(branch).parentId()).isEqualTo(mainHead);
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(2L, "b"));
  }

  @Test
  public void testCommitInBranch() {
    appendRows(table, row(1L, "a"));
    String branch = "test-branch";
    table.manageSnapshots().createBranch(branch, table.currentSnapshot().snapshotId()).commit();
    TableSinkConfig branchConfig = tableConfigWithBranch(branch);
    when(config.tableConfig(any())).thenReturn(branchConfig);

    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L);
    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAllOnRef(table, branch)).containsExactly(tuple(1L, "a2"));
    // main is untouched
    assertThat(readAll(table)).containsExactly(tuple(1L, "a"));
  }

  @Test
  public void testLineageIsReportedOnceAtTheStartAndOnceAtTheEndOfADrain() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    List<StagedChangeFile> files =
        stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2"));

    committer.commit(requestOf(files, 0, 5L, 6L));

    // three slices, three snapshots, but one logical batch: reporting the same edge per slice is
    // noise, and reporting only at the end hides a table this drain created for the whole drain
    assertThat(metadataEvents.lineageTopics).hasSize(2);
  }

  @Test
  public void testMultiSliceDrainsWithinOneCallAndCleansUp() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // force one key per slice

    List<StagedChangeFile> files =
        stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2"));
    TableCommitRequest request = requestOf(files, 0, 5L, 6L);

    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"));
    // three keys, one per slice, plus the initial append -> at least 3 additional commits
    assertThat(sentEvents).hasSizeGreaterThanOrEqualTo(3);
    // draining cleared the change-set pointer
    assertThat(table.currentSnapshot().summary())
        .doesNotContainKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(table.currentSnapshot().summary()).doesNotContainKey(COPY_ON_WRITE_CURSOR_PROP);
    // and swept the staged files + manifest it used along the way
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    files.forEach(f -> assertThat(io.fileExists(f.location())).isFalse());
  }

  /**
   * The exhausting slice's cleanup (its staged files and manifest) runs ahead of sending {@code
   * CommitToTable} and reporting lineage, not after: nothing else names them once the change set's
   * pointer has moved off them, so a failure sending the report must not leave them behind as
   * orphans only {@code remove_orphan_files} or the staging sweep would ever collect.
   */
  @Test
  public void testTheExhaustingCommitCleansUpBeforeReportingEvenWhenReportingFails() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    List<StagedChangeFile> files = stagedFiles(update(1L, "a2"), update(2L, "b2"));

    AtomicInteger sends = new AtomicInteger();
    CopyOnWriteRewriteDriver failing =
        new CopyOnWriteRewriteDriver(
            catalog,
            config,
            metadataEvents,
            event -> {
              sentEvents.add(event);
              // the second CommitToTable is the exhausting slice's
              if (sends.incrementAndGet() == 2) {
                throw new IllegalStateException("producer failed reporting the exhausting commit");
              }
            },
            taskCount());

    failing.commit(requestOf(files, 0, 5L, 6L));
    table.refresh();

    Snapshot firstSlice = connectorSnapshots().get(0);
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            UUID.fromString(firstSlice.summary().get(COPY_ON_WRITE_CHANGE_SET_ID_PROP)));

    // the exhausting commit landed even though reporting it never made it out
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"));
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    files.forEach(f -> assertThat(io.fileExists(f.location())).as(f.location()).isFalse());
    assertThat(io.fileExists(manifestLocation)).as("the change set's manifest").isFalse();
  }

  /**
   * Every slice of a drain is a snapshot of its own, and each says whose it is: the drain's commit
   * id and the slice's sequence. Only the exhausting one says how far the data is valid: in its
   * summary and in its {@code CommitToTable} alike. Read off an intermediate snapshot, that
   * timestamp would promise a table in which part of the change set's keys are still old.
   */
  @Test
  public void testEverySliceNamesItsCommitAndSequenceAndOnlyTheLastOneAValidThroughTs() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice
    UUID commitId = UUID.randomUUID();
    OffsetDateTime validThroughTs = OffsetDateTime.parse("2026-09-15T01:02:03.456Z");

    TableCommitter.Outcome outcome =
        committer.commit(
            requestOf(
                TABLE_REFERENCE,
                stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")),
                0,
                5L,
                6L,
                commitId,
                validThroughTs));
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    // told apart by the offsets key, which every slice carries: the commit id is under test here
    List<Snapshot> slices = Lists.newArrayList();
    table
        .snapshots()
        .forEach(
            snapshot -> {
              if (snapshot.summary().containsKey(OFFSETS_PROP)) {
                slices.add(snapshot);
              }
            });
    assertThat(slices).hasSize(3);
    assertThat(slices)
        .allSatisfy(
            slice ->
                assertThat(slice.summary())
                    .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
                    .containsKey(SLICE_SEQ_PROP));
    assertThat(slices)
        .extracting(slice -> Long.parseLong(slice.summary().get(SLICE_SEQ_PROP)))
        .isSorted()
        .doesNotHaveDuplicates();
    assertThat(slices.subList(0, 2))
        .allSatisfy(slice -> assertThat(slice.summary()).doesNotContainKey(VALID_THROUGH_TS_PROP));
    assertThat(slices.get(2).summary()).containsKey(VALID_THROUGH_TS_PROP);
    assertThat(OffsetDateTime.parse(slices.get(2).summary().get(VALID_THROUGH_TS_PROP)))
        .isAtSameInstantAs(validThroughTs);

    List<CommitToTable> commits = Lists.newArrayList();
    sentEvents.forEach(
        event -> {
          if (event.payload() instanceof CommitToTable) {
            commits.add((CommitToTable) event.payload());
          }
        });
    assertThat(commits).hasSize(3);
    assertThat(commits).extracting(CommitToTable::commitId).containsOnly(commitId);
    assertThat(commits).extracting(CommitToTable::tableReference).containsOnly(TABLE_REFERENCE);
    assertThat(commits)
        .extracting(CommitToTable::snapshotId)
        .containsExactly(
            slices.get(0).snapshotId(), slices.get(1).snapshotId(), slices.get(2).snapshotId());
    assertThat(commits.subList(0, 2)).extracting(CommitToTable::validThroughTs).containsOnlyNulls();
    assertThat(commits.get(2).validThroughTs()).isAtSameInstantAs(validThroughTs);
  }

  /**
   * A change set is frozen in one cycle and exhausted in another, by another coordinator. How far
   * the data is valid is a property of the freeze, not of the cycle that happens to finish the
   * drain: the responses of the later cycle are not part of this change set and are still waiting
   * in the buffer. Written from the resuming cycle, {@code valid-through-ts} would tell a consumer
   * that changes nowhere near the table are already in it.
   */
  @Test
  public void testTheExhaustingCommitOfAResumedDrainIsValidThroughTheCycleOfItsFreeze() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice
    OffsetDateTime frozenAt = OffsetDateTime.parse("2026-09-15T01:02:03.456Z");
    OffsetDateTime resumedAt = OffsetDateTime.parse("2026-09-15T02:04:05.789Z");

    // the change set freezes in the cycle valid through frozenAt; one slice lands, then the
    // coordinator dies
    committer.commitOneSlice(
        requestOf(
            TABLE_REFERENCE,
            stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")),
            0,
            5L,
            6L,
            UUID.randomUUID(),
            frozenAt));

    // its successor resumes the change set in a later cycle and drains the rest of it
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitRequest next =
        requestOf(TABLE_REFERENCE, List.of(), 0, 20L, 21L, UUID.randomUUID(), resumedAt);
    assertThat(committer.commit(next)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"));
    List<Snapshot> slices = connectorSnapshots();
    assertThat(slices).hasSize(3);
    assertThat(slices.subList(0, 2))
        .allSatisfy(slice -> assertThat(slice.summary()).doesNotContainKey(VALID_THROUGH_TS_PROP));
    assertThat(OffsetDateTime.parse(slices.get(2).summary().get(VALID_THROUGH_TS_PROP)))
        .as("the exhausting commit is valid through the cycle the change set froze in")
        .isAtSameInstantAs(frozenAt);

    List<CommitToTable> commits = Lists.newArrayList();
    sentEvents.forEach(
        event -> {
          if (event.payload() instanceof CommitToTable) {
            commits.add((CommitToTable) event.payload());
          }
        });
    assertThat(commits).hasSize(3);
    assertThat(commits.subList(0, 2)).extracting(CommitToTable::validThroughTs).containsOnlyNulls();
    assertThat(commits.get(2).validThroughTs())
        .as("and so is the CommitToTable of that commit")
        .isAtSameInstantAs(frozenAt);
  }

  /**
   * The commit and everything after it are two different phases for the replacement files: before
   * the commit they are this attempt's and a failure must delete them; after it a snapshot
   * references them, and deleting one leaves the table unreadable. The trigger here is the control
   * topic: {@code Channel.send} rethrows a producer failure after aborting its transaction, one
   * statement after the Iceberg commit returns.
   */
  @Test
  public void testAFailureAfterTheCommitLeavesTheSnapshotReadable() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    AtomicInteger sends = new AtomicInteger();
    CopyOnWriteRewriteDriver failing =
        new CopyOnWriteRewriteDriver(
            catalog,
            config,
            metadataEvents,
            event -> {
              if (sends.incrementAndGet() == 1) {
                throw new IllegalStateException("producer failed right after the commit");
              }
              sentEvents.add(event);
            },
            taskCount());

    // one cycle: the next would resume the drain from the snapshot and finish it
    failing.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));
    table.refresh();

    // the snapshot the first slice committed stands, and every file it added is still there
    Snapshot committed = table.currentSnapshot();
    assertThat(committed.operation()).isEqualTo(DataOperations.OVERWRITE);
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    committed
        .addedDataFiles(io)
        .forEach(file -> assertThat(io.fileExists(file.location())).isTrue());

    // ... which is to say the table reads, with the first slice applied and the rest still pending
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"), tuple(3L, "c"));
    // and the drain is resumable: the change set is still pointed at, with its cursor
    assertThat(committed.summary()).containsKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(committed.summary()).containsKey(COPY_ON_WRITE_CURSOR_PROP);
  }

  /**
   * The same boundary from the other side: planning the next slice throws {@code
   * ValidationException} (what the expression binder itself throws) and a handler that discards
   * files on such a failure and retries would take the already-committed files down with it.
   */
  @Test
  public void testAFailureStartingTheNextSliceLeavesTheSnapshotReadable() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    // blows up when the second slice asks for it, i.e. after the first slice has committed
    AtomicInteger cardinalityReads = new AtomicInteger();
    when(config.copyOnWritePruningMaxInCardinality())
        .thenAnswer(
            invocation -> {
              if (cardinalityReads.incrementAndGet() > 1) {
                throw new ValidationException("planning blew up");
              }
              return 1000;
            });

    // one cycle: planning fails on every later cycle as well, and every one starts the drain again
    committer.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));
    table.refresh();

    Snapshot committed = table.currentSnapshot();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    committed
        .addedDataFiles(io)
        .forEach(file -> assertThat(io.fileExists(file.location())).isTrue());
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"), tuple(3L, "c"));
  }

  /**
   * The catalog applied the slice's commit and the answer was lost. Deleting the files would leave
   * that snapshot unreadable, so the committer looks for it instead, and finding it, carries on as
   * if the commit had returned.
   */
  @Test
  public void testACommitWithAnUnknownOutcomeThatLandedCarriesOnFromItsSnapshot() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  if (attempt == 1) {
                    throw new CommitStateUnknownException(
                        new IOException("connection reset after the commit was sent"));
                  }
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    TableCommitter.Outcome outcome =
        driver.commit(
            requestOf(
                stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));
    table.refresh();

    List<Snapshot> slices = connectorSnapshots();
    assertAddedFilesExist(slices.get(0));
    // committed as far as the drain is concerned: the next slices follow it, and it is not redone
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(slices).hasSize(3);
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"));
    assertThat(((CommitToTable) sentEvents.get(0).payload()).snapshotId())
        .isEqualTo(slices.get(0).snapshotId());
  }

  /**
   * The same lost answer, and the catalog cannot be read to look for the snapshot either. Nothing
   * is deleted (not the files, and not the manifest the snapshot may point at) and the next cycle
   * finds the drain by its pointer.
   */
  @Test
  public void testACommitWithAnUnknownOutcomeThatCannotBeEstablishedKeepsItsFilesAndManifest() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    AtomicBoolean catalogDown = new AtomicBoolean();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  if (attempt == 1) {
                    catalogDown.set(true);
                    throw new CommitStateUnknownException(
                        new IOException("connection reset after the commit was sent"));
                  }
                },
                loaded ->
                    doAnswer(
                            invocation -> {
                              if (catalogDown.getAndSet(false)) {
                                throw new UncheckedIOException(
                                    new IOException("catalog unreachable"));
                              }
                              return invocation.callRealMethod();
                            })
                        .when(loaded)
                        .refresh()),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());
    TableCommitRequest request =
        requestOf(stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L);

    // the coordinator gets no further than the first slice's commit before the drain comes off
    driver.commitOneSlice(request);
    table.refresh();

    Snapshot landed = Iterables.getOnlyElement(connectorSnapshots());
    assertAddedFilesExist(landed);
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            UUID.fromString(landed.summary().get(COPY_ON_WRITE_CHANGE_SET_ID_PROP)));
    assertThat(((InMemoryFileIO) table.io()).fileExists(manifestLocation))
        .as("the manifest the landed snapshot points at")
        .isTrue();

    // the next cycle resumes from that snapshot rather than committing its slice again
    assertThat(driver.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"));
    assertThat(connectorSnapshots()).hasSize(3);
  }

  /**
   * A coordinator resumes a change set a previous one had already committed a slice of, and its
   * first slice fails to start. The manifest is not the resuming coordinator's to delete: the
   * snapshot points at it, and without it the next cycle could not resume at all.
   */
  @Test
  public void testAResumedDrainThatFailsBeforeItsFirstCommitKeepsTheManifestTheSnapshotPointsAt() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    // one slice lands, then the coordinator dies with the change set half applied
    committer.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));
    table.refresh();
    Snapshot landed = Iterables.getOnlyElement(connectorSnapshots());
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            UUID.fromString(landed.summary().get(COPY_ON_WRITE_CHANGE_SET_ID_PROP)));

    // the new coordinator resumes it, and planning its first slice blows up once
    AtomicInteger failures = new AtomicInteger();
    when(config.copyOnWritePruningMaxInCardinality())
        .thenAnswer(
            invocation -> {
              if (failures.getAndIncrement() == 0) {
                throw new ValidationException("planning blew up");
              }
              return 1000;
            });
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitRequest next = requestOf(List.of(), 0, 20L, 21L);
    committer.commitOneSlice(next);
    table.refresh();

    assertThat(failures).as("the resumed slice failed to start").hasValue(1);
    assertThat(connectorSnapshots()).as("nothing committed after the resume").hasSize(1);
    assertThat(((InMemoryFileIO) table.io()).fileExists(manifestLocation))
        .as("the manifest the landed snapshot points at")
        .isTrue();

    // the next cycle resumes from the same pointer and drains the rest
    assertThat(committer.commit(next)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"));
    assertThat(connectorSnapshots()).hasSize(3);
  }

  /**
   * Two coordinators resume the same change set one after the other: both number their slices from
   * zero, so both hand out a slice 0 of it. The answer to the first one's assignment is not an
   * answer to the second one's slice: it was rewritten against another plan, for another set of
   * active tasks, and counting it would commit files that do not cover the slice's keys.
   */
  @Test
  public void testAnAnswerToTheSliceOfAPreviousCoordinatorDoesNotCommitTheResumedOne() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    // one slice lands, then the coordinator dies with the change set half applied
    committer.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));

    // its successor resumes the change set, hands slice 0 of the resumed drain out and dies too,
    // with its worker still rewriting
    CopyOnWriteRewriteDriver previous =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    List<RewriteAssigned> bySucceeded = previous.handOutOneSlice(requestOf(List.of(), 0, 20L, 21L));
    assertThat(bySucceeded).as("the coordinator that died had a slice out").isNotEmpty();

    // the coordinator after it resumes from the same pointer, under its own commit id, and hands
    // out its own slice 0 of the same change set
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitRequest next = requestOf(List.of(), 0, 30L, 31L);
    List<RewriteAssigned> mine = committer.handOutOneSlice(next);
    assertThat(mine).as("the running coordinator has its own slice out").isNotEmpty();

    // the worker of the coordinator that died answers now
    committer.answerAssignments(bySucceeded);
    table.refresh();
    assertThat(connectorSnapshots())
        .as("an answer to the assignment of a previous coordinator commits no slice")
        .hasSize(1);

    // the drain ends on the answers to its own assignments, with every row applied exactly once
    committer.answerAssignments(mine);
    assertThat(committer.commit(next)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"));
  }

  private static Stream<Arguments> interruptions() {
    return Stream.of(
        Arguments.of(
            "interrupt flag",
            (Runnable)
                () -> {
                  Thread.currentThread().interrupt();
                  throw new IllegalStateException("the catalog client gave up");
                }),
        Arguments.of(
            "interrupted I/O",
            (Runnable)
                () -> {
                  throw new UncheckedIOException(
                      new InterruptedIOException("interrupted waiting for the catalog"));
                }));
  }

  /**
   * The pool thread interrupted in the middle of {@code commit()} (a leader change shuts the pool
   * down) after the catalog applied it. Which exception the client throws depends on the client, so
   * the interruption is recognised by the thread's flag or by its cause.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("interruptions")
  public void testACommitInterruptedAfterItLandedKeepsItsFiles(
      String interruption, Runnable interrupt) {
    appendRows(table, row(1L, "a"), row(2L, "b"));

    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  if (attempt == 1) {
                    interrupt.run();
                  }
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    try {
      driver.commit(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));
    } finally {
      // the test thread is the pool thread here
      Thread.interrupted();
    }
    table.refresh();

    assertAddedFilesExist(Iterables.getOnlyElement(connectorSnapshots()));
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"));
  }

  /**
   * The coordinator is told to stop while a slice's commit is under way: another coordinator may
   * already be elected. The commit is atomic and lands, and nothing after it starts: no {@code
   * CommitToTable}, no deleted file, no next slice handed out ahead of the new coordinator's.
   */
  @Test
  public void testAStopDuringASliceCommitLetsItLandAndStartsNothingAfterIt() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    AtomicReference<CopyOnWriteRewriteDriver> stopping = new AtomicReference<>();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  stopping.get().stop();
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());
    stopping.set(driver);

    driver.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));
    table.refresh();

    assertAddedFilesExist(Iterables.getOnlyElement(connectorSnapshots()));
    assertThat(sentEvents).as("no CommitToTable after the stop").isEmpty();
    assertThat(driver.handedOut())
        .as("no next slice after the stop")
        .extracting(RewriteAssigned::sliceSeq)
        .containsOnly(0);
    String normalized = driver.handedOut().get(0).normalizedRef().location();
    assertThat(((InMemoryFileIO) table.io()).fileExists(normalized))
        .as("no file deleted after the stop")
        .isTrue();
  }

  /** The same stop under the last slice: the change set it drained is not cleaned up either. */
  @Test
  public void testAStopDuringTheLastSliceCommitLeavesTheChangeSetInPlace() {
    appendRows(table, row(1L, "a"));
    List<StagedChangeFile> staged = stagedFiles(update(1L, "a2"));
    AtomicReference<CopyOnWriteRewriteDriver> stopping = new AtomicReference<>();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  stopping.get().stop();
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());
    stopping.set(driver);

    driver.commitOneSlice(requestOf(staged, 0, 5L, 6L));
    table.refresh();

    assertAddedFilesExist(Iterables.getOnlyElement(connectorSnapshots()));
    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));
    assertThat(sentEvents).as("no CommitToTable after the stop").isEmpty();
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            driver.handedOut().get(0).changeSetId());
    assertThat(io.fileExists(manifestLocation)).as("manifest after the stop").isTrue();
    assertThat(staged)
        .as("staged files after the stop")
        .allSatisfy(file -> assertThat(io.fileExists(file.location())).isTrue());
  }

  /** A commit that plainly did not happen is still the attempt's to clean up. */
  @Test
  public void testACommitThatFailedOutrightDeletesTheFilesOfItsAttempt() {
    appendRows(table, row(1L, "a"), row(2L, "b"));

    List<String> attemptFiles = Lists.newArrayList();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  updated
                      .currentSnapshot()
                      .addedDataFiles(table.io())
                      .forEach(file -> attemptFiles.add(file.location()));
                  throw new IllegalStateException("the catalog refused the commit");
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    // one attempt: a catalog that refuses every commit would have every later cycle start again
    driver.commitOneSlice(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));

    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(attemptFiles)
        .isNotEmpty()
        .allSatisfy(location -> assertThat(io.fileExists(location)).as(location).isFalse());
    table.refresh();
    assertThat(connectorSnapshots()).isEmpty();
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(2L, "b"));
  }

  /**
   * The commit returned, another writer landed on top, and walking back to the slice's own snapshot
   * needs a catalog request that fails: as with snapshots loaded lazily. The snapshot is the
   * table's by then: the failed lookup costs {@code CommitToTable} its id and nothing else.
   */
  @Test
  public void testASnapshotLookupThatFailsAfterTheCommitLeavesTheSnapshotReadable() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    AtomicBoolean lookupFails = new AtomicBoolean();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  if (attempt == 1) {
                    appendRows(catalog.loadTable(TABLE_IDENTIFIER), row(10L, "outsider"));
                    lookupFails.set(true);
                  }
                },
                loaded ->
                    doAnswer(
                            invocation -> {
                              if (lookupFails.getAndSet(false)) {
                                throw new UncheckedIOException(
                                    new IOException("could not load the snapshot"));
                              }
                              return invocation.callRealMethod();
                            })
                        .when(loaded)
                        .snapshot(anyLong())),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    TableCommitter.Outcome outcome =
        driver.commit(
            requestOf(
                stagedFiles(update(1L, "a2"), update(2L, "b2"), update(3L, "c2")), 0, 5L, 6L));
    table.refresh();

    assertAddedFilesExist(connectorSnapshots().get(0));
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(
            tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c2"), tuple(10L, "outsider"));
    assertThat(((CommitToTable) sentEvents.get(0).payload()).snapshotId()).isNull();
  }

  /**
   * A snapshot that is no longer in the branch by the time it is looked for is reported as no
   * snapshot, not as whatever the head is now.
   */
  @Test
  public void testASliceSnapshotRolledBackBeforeTheLookupIsNotReportedAsTheBranchHead() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    long before = table.currentSnapshot().snapshotId();

    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  if (attempt == 1) {
                    catalog
                        .loadTable(TABLE_IDENTIFIER)
                        .manageSnapshots()
                        .rollbackTo(before)
                        .commit();
                  }
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    // one cycle: the rollback took the applied change set out of the table after its staged files
    // were cleaned up, so every later cycle freezes it again and fails to read them
    driver.commitOneSlice(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));

    assertThat(sentEvents).hasSize(1);
    assertThat(((CommitToTable) sentEvents.get(0).payload()).snapshotId()).isNull();
  }

  /**
   * Another writer lands on top of the slice before its snapshot is looked up. {@code
   * CommitToTable} still names the slice's own snapshot, not the head of the branch.
   */
  @Test
  public void testASliceReportsItsOwnSnapshotWhenAnotherWriterLandedOnTopBeforeTheLookup() {
    appendRows(table, row(1L, "a"), row(2L, "b"));

    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            committingThrough(
                (attempt, updated, apply) -> {
                  apply.run();
                  if (attempt == 1) {
                    appendRows(catalog.loadTable(TABLE_IDENTIFIER), row(10L, "outsider"));
                  }
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    TableCommitter.Outcome outcome =
        driver.commit(requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L));
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    Snapshot slice = Iterables.getOnlyElement(connectorSnapshots());
    assertThat(table.currentSnapshot().snapshotId())
        .as("the other writer's snapshot is the head")
        .isNotEqualTo(slice.snapshotId());
    assertThat(sentEvents).hasSize(1);
    assertThat(((CommitToTable) sentEvents.get(0).payload()).snapshotId())
        .isEqualTo(slice.snapshotId());
  }

  @Test
  public void testRedeliveredResponseIsDeduplicatedByCommittedOffsets() {
    appendRows(table, row(1L, "a"));

    List<StagedChangeFile> files = stagedFiles(update(1L, "a2"));
    TableCommitRequest first = requestOf(files, 0, 5L, 6L);
    assertThat(committer.commit(first)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));

    long snapshotsAfterFirst = countSnapshots(table);

    // the same control-topic message redelivered (e.g. after a restart before
    // commitConsumerOffsets())
    TableCommitRequest redelivered = requestOf(files, 0, 5L, 6L);
    assertThat(committer.commit(redelivered)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));
    assertThat(countSnapshots(table)).isEqualTo(snapshotsAfterFirst);
  }

  @Test
  public void testAPlanSpanningPartitionSpecsStopsTheTableInsteadOfFailingForever() {
    // the one failure that cannot resolve itself: files under the old spec do not go away on their
    // own, so every cycle would plan them, throw the same exception and warn again, while the
    // responses pile up in the coordinator's buffer and the offsets stay held. The operator's only
    // signal would be a heap warning some hours later
    appendRows(table, row(1L, "a"), row(2L, "b"));
    table.updateSpec().addField("data").commit();
    table.refresh();

    InMemoryCatalog watched = spy(catalog);
    CopyOnWriteRewriteDriver stopping =
        new CopyOnWriteRewriteDriver(watched, config, metadataEvents, sentEvents::add, taskCount());

    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L);
    assertThat(stopping.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);

    // the table is untouched, and nothing was handed to any worker
    table.refresh();
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(2L, "b"));

    // the second cycle does not even load the table: a stopped table is reported once and then
    // left alone until the connector is restarted
    assertThat(stopping.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    verify(watched, times(1)).loadTable(TABLE_IDENTIFIER);
    // listed, and answered FAILED without the table being loaded: CommitComplete stays without
    // valid-through while the table holds its changes
    assertThat(stopping.pendingTables()).containsExactly(request.tableReference());
  }

  @Test
  public void testATableThatLostItsIdentifierFieldsStopsInsteadOfFailingForever() {
    // the writers checked the key before staging these files; the table's identifier fields were
    // dropped since. No later cycle brings them back, so the coordinator refuses the table the way
    // the writer would, and stops it: one ERROR naming the table, not a warning every interval
    appendRows(table, row(1L, "a"));
    List<StagedChangeFile> files = stagedFiles(update(1L, "a2"));
    table.updateSchema().setIdentifierFields(ImmutableSet.of()).commit();
    table.refresh();

    InMemoryCatalog watched = spy(catalog);
    CopyOnWriteRewriteDriver stopping =
        new CopyOnWriteRewriteDriver(watched, config, metadataEvents, sentEvents::add, taskCount());

    TableCommitRequest request = requestOf(files, 0, 5L, 6L);
    assertThat(stopping.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);

    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a"));

    assertThat(stopping.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    verify(watched, times(1)).loadTable(TABLE_IDENTIFIER);
    // listed, and answered FAILED without the table being loaded: CommitComplete stays without
    // valid-through while the table holds its changes
    assertThat(stopping.pendingTables()).containsExactly(request.tableReference());
  }

  @Test
  public void testChangeSetWithMismatchedIdentifierFieldsIsDroppedAndFilesRejoinNextOne() {
    appendRows(table, row(1L, "a"), row(2L, "b"));

    // a stale change set frozen under an identifier field set that no longer matches the table's
    List<StagedChangeFile> staleFiles = stagedFiles(update(1L, "stale"));
    ChangeSetManifest staleManifest =
        ChangeSetManifest.freeze(
            table,
            ImmutableSet.of(),
            staleFiles,
            ImmutableSet.of("stale-topic"),
            ImmutableMap.of(0, 1L),
            null);
    String stagingLocation = StagedChangeFileWriter.stagingLocation(table, null);
    String staleLocation =
        ChangeSetManifest.location(
            stagingLocation, TABLE_REFERENCE, GROUP_ID, staleManifest.changeSetId());
    staleManifest.write(table.io(), staleLocation);
    // the offsets of this connector's group are what mark a snapshot as ours; without them
    // latestConnectorSummary walks straight past this one and never sees the pointer
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, staleManifest.changeSetId().toString())
        .set(COPY_ON_WRITE_CURSOR_PROP, "{}")
        .commit();

    List<StagedChangeFile> freshFiles = stagedFiles(update(2L, "b2"));
    TableCommitRequest request = requestOf(freshFiles, 0, 5L, 6L);

    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    // both the dropped change set's staged update and the fresh one landed
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "stale"), tuple(2L, "b2"));
    // and the dropped change set's source topics moved to the one that adopted its files: nothing
    // else knows where they came from
    assertThat(metadataEvents.lineageTopics)
        .isNotEmpty()
        .allSatisfy(topics -> assertThat(topics).contains("stale-topic", "src-topic"));
  }

  /**
   * A manifest a future build wrote under a {@code format-version} this one does not know cannot be
   * trusted to resume: whatever changed about its encoding, applying it as though nothing did would
   * misread it silently. It is dropped exactly like a change set whose identifier fields no longer
   * match: its staged files carry over into a freshly frozen one, in the same cycle.
   */
  @Test
  public void testChangeSetWithAnUnsupportedFormatVersionIsDroppedAndFilesRejoinNextOne()
      throws IOException {
    appendRows(table, row(1L, "a"), row(2L, "b"));

    // a change set frozen by a build that wrote a format-version this one does not recognize
    List<StagedChangeFile> staleFiles = stagedFiles(update(1L, "stale"));
    ChangeSetManifest staleManifest =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            staleFiles,
            ImmutableSet.of("stale-topic"),
            ImmutableMap.of(0, 1L),
            null);
    String stagingLocation = StagedChangeFileWriter.stagingLocation(table, null);
    String staleLocation =
        ChangeSetManifest.location(
            stagingLocation, TABLE_REFERENCE, GROUP_ID, staleManifest.changeSetId());
    staleManifest.write(table.io(), staleLocation);
    String content;
    try (InputStream in = table.io().newInputFile(staleLocation).newStream()) {
      content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    // works whether this build already writes a format-version (replace it) or not yet (add one)
    String corrupted =
        content.contains("\"format-version\":")
            ? content.replaceFirst("\"format-version\":\\d+", "\"format-version\":999")
            : content.replaceFirst("\\{", "{\"format-version\":999,");
    try (OutputStream out = table.io().newOutputFile(staleLocation).createOrOverwrite()) {
      out.write(corrupted.getBytes(StandardCharsets.UTF_8));
    }
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, staleManifest.changeSetId().toString())
        .set(COPY_ON_WRITE_CURSOR_PROP, "{}")
        .commit();

    List<StagedChangeFile> freshFiles = stagedFiles(update(2L, "b2"));
    TableCommitRequest request = requestOf(freshFiles, 0, 5L, 6L);

    TableCommitter.Outcome outcome = committer.commit(request);
    table.refresh();

    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    // both the dropped change set's staged update and the fresh one landed in the same cycle,
    // rather than the unrecognized manifest being resumed and the fresh one left buffered
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "stale"), tuple(2L, "b2"));
    assertThat(metadataEvents.lineageTopics)
        .isNotEmpty()
        .allSatisfy(topics -> assertThat(topics).contains("stale-topic", "src-topic"));
  }

  @Test
  public void testAFileStagedUnderANarrowerIdentifierSetIsRejectedAtFreezeUntilTheSetIsRestored() {
    appendRows(table, row(7L, "x"), row(8L, "y"));

    // an update of key 7 staged while the identifier fields were {id}
    TableCommitRequest request = requestOf(stagedFiles(update(7L, "x2")), 0, 5L, 6L);

    // the set is widened before the change set freezes
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .requireColumn("data")
        .setIdentifierFields("id", "data")
        .commit();

    TableCommitter.Outcome rejected = committer.commit(request);
    table.refresh();

    // applied under (id, data) the update matches no row: (7, x2) is added next to (7, x) instead
    // of replacing it, and the offsets move on
    assertThat(rejected).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(7L, "x"), tuple(8L, "y"));
    assertThat(connectorSnapshots()).isEmpty();

    // the remedy is the previous set; the held responses then apply as they are
    table.updateSchema().setIdentifierFields("id").commit();

    assertThat(committer.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(7L, "x2"), tuple(8L, "y"));
  }

  @Test
  public void testFilesOfADroppedChangeSetAreNotCarriedOverUnderAWiderIdentifierSet() {
    appendRows(table, row(7L, "x"), row(8L, "y"));

    // a change set frozen under {id} and not yet drained when the set is widened
    ChangeSetManifest frozen =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            stagedFiles(delete(7L)),
            Set.of("src-topic"),
            ImmutableMap.of(0, 1L),
            null);
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            frozen.changeSetId());
    frozen.write(table.io(), manifestLocation);
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, frozen.changeSetId().toString())
        .commit();

    table
        .updateSchema()
        .allowIncompatibleChanges()
        .requireColumn("data")
        .setIdentifierFields("id", "data")
        .commit();

    // this cycle's own file is written under the wider set and is acceptable on its own
    StagedChangeFileWriter wideWriter =
        new StagedChangeFileWriter(table, TABLE_REFERENCE, Set.of(1, 2), null, GROUP_ID, "task-0");
    GenericRecord update = GenericRecord.create(SCHEMA);
    update.setField("id", 8L);
    update.setField("data", "y");
    wideWriter.write(wideWriter.stagedRow(update, StagedChangeSchema.OP_UPDATE, "src-topic", 0, 1));
    TableCommitRequest request = requestOf(wideWriter.complete(), 0, 5L, 6L);

    TableCommitter.Outcome rejected = committer.commit(request);
    table.refresh();

    assertThat(rejected).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(7L, "x"), tuple(8L, "y"));
    // rejected before the drop: the change set is still there to resume once the set is restored
    assertThat(((InMemoryFileIO) table.io()).fileExists(manifestLocation)).isTrue();

    table.updateSchema().setIdentifierFields("id").commit();

    assertThat(committer.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(8L, "y"));
  }

  @Test
  public void testIdentifierFieldsWidenedBetweenSlicesStopTheDrainBeforeTheNextSlice() {
    appendRows(table, row(1L, "a"), row(7L, "x"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    // right after the first slice commits, and before the second starts: another writer widens
    // the set to (id, data) and adds a row that shares id 7 with the one the change set deletes
    AtomicInteger sliceCommits = new AtomicInteger();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            catalog,
            config,
            metadataEvents,
            event -> {
              sentEvents.add(event);
              if (event.payload() instanceof CommitToTable && sliceCommits.incrementAndGet() == 1) {
                Table live = catalog.loadTable(TABLE_IDENTIFIER);
                live.updateSchema()
                    .allowIncompatibleChanges()
                    .requireColumn("data")
                    .setIdentifierFields("id", "data")
                    .commit();
                appendRows(live, row(7L, "y"));
              }
            },
            taskCount());

    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2"), delete(7L)), 0, 5L, 6L);
    TableCommitter.Outcome stopped = driver.commit(request);
    table.refresh();

    // the second slice, planned by the old key (id), would delete (7, y) along with (7, x)
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(7L, "x"), tuple(7L, "y"));
    assertThat(stopped).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(connectorSnapshots()).hasSize(1);
    String changeSetId =
        connectorSnapshots().get(0).summary().get(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(changeSetId).isNotNull();
    String manifestLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            UUID.fromString(changeSetId));

    // the next cycle drops the change set, and its files, staged under (id), cannot join a change
    // set keyed by (id, data): rejected, nothing applied, the change set left to resume
    assertThat(driver.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(7L, "x"), tuple(7L, "y"));
    assertThat(connectorSnapshots()).hasSize(1);
    assertThat(((InMemoryFileIO) table.io()).fileExists(manifestLocation)).isTrue();

    // the set restored: the change set resumes from its pointer and drains
    table.updateSchema().setIdentifierFields("id").commit();

    assertThat(driver.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));
    assertThat(connectorSnapshots()).hasSize(2);
    assertThat(table.currentSnapshot().summary())
        .doesNotContainKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(((InMemoryFileIO) table.io()).fileExists(manifestLocation)).isFalse();
  }

  @Test
  public void testATableDroppedMidDrainIsLetGoOfByTheNextCommit() {
    appendRows(table, row(1L, "a"), row(7L, "x"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    // right after the first slice commits, the table is dropped: the next slice's refresh finds it
    // gone
    AtomicInteger sliceCommits = new AtomicInteger();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            refreshingByName(),
            config,
            metadataEvents,
            event -> {
              sentEvents.add(event);
              if (event.payload() instanceof CommitToTable && sliceCommits.incrementAndGet() == 1) {
                catalog.dropTable(TABLE_IDENTIFIER, false);
              }
            },
            taskCount());

    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2"), delete(7L)), 0, 5L, 6L);
    assertThat(driver.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(sliceCommits).hasValue(1);

    // nothing of the table is kept: still listed, it would be offered a commit and warned about
    // every cycle, for a table that is not there
    assertThat(driver.pendingTables()).isEmpty();
  }

  @Test
  public void testATableRecreatedMidDrainTakesNothingOfTheChangeSetFrozenForTheOldOne() {
    appendRows(table, row(1L, "a"), row(7L, "x"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    // right after the first slice commits, the table is dropped and created again under its name,
    // with a row of its own under a key the change set deletes
    AtomicInteger sliceCommits = new AtomicInteger();
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            refreshingByName(),
            config,
            metadataEvents,
            event -> {
              sentEvents.add(event);
              if (event.payload() instanceof CommitToTable && sliceCommits.incrementAndGet() == 1) {
                catalog.dropTable(TABLE_IDENTIFIER, false);
                appendRows(catalog.createTable(TABLE_IDENTIFIER, SCHEMA), row(7L, "new"));
              }
            },
            taskCount());

    // the first slice spent the envelopes, so the cycles after it have nothing new to offer
    driver.commitOneSlice(requestOf(stagedFiles(update(1L, "a2"), delete(7L)), 0, 5L, 6L));
    assertThat(sliceCommits).hasValue(1);
    TableCommitRequest nothingNew = requestOf(ImmutableList.of(), ImmutableMap.of());
    assertThat(driver.commit(nothingNew)).isEqualTo(TableCommitter.Outcome.COMMITTED);

    Table recreated = catalog.loadTable(TABLE_IDENTIFIER);
    assertThat(readAll(recreated))
        .as("the new table, which the rest of the change set must not reach")
        .containsExactly(tuple(7L, "new"));
    assertThat(recreated.currentSnapshot().summary()).doesNotContainKey(COMMIT_ID_SNAPSHOT_PROP);
    // abandoned, the change set is not a drain to come back to
    assertThat(driver.pendingTables()).isEmpty();

    // and caught before the next slice goes out, not when its commit is refused: that would have
    // normalized and planned it against the new table and had the workers rewrite it in full
    assertThat(driver.handedOut())
        .extracting(RewriteAssigned::sliceSeq)
        .as("the slices handed out, none of them after the table was recreated")
        .containsOnly(driver.handedOut().get(0).sliceSeq());
  }

  @Test
  public void testATableRecreatedWhileASliceIsCommittedTakesNoneOfTheChangeSetFrozenForTheOldOne() {
    // an empty table: the slice has no base, so its commit looks for conflicts in the whole history
    // of the branch, and a table created again under the name, empty, has none
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L); // one key per slice

    // after the check at the start of the first slice, on its commit: the table is dropped and
    // created again, empty. The attempt against the old one fails, and the retry refreshes by name
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            refreshingByName(
                (attempt, updated, apply) -> {
                  if (attempt == 1) {
                    catalog.dropTable(TABLE_IDENTIFIER, false);
                    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
                  }
                  apply.run();
                }),
            config,
            metadataEvents,
            sentEvents::add,
            taskCount());

    TableCommitRequest request =
        requestOf(stagedFiles(update(1L, "a2"), update(7L, "x2")), 0, 5L, 6L);
    assertThat(driver.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);

    // no slice of the change set frozen for the old table reached the new one: that would have
    // spent the envelopes on one key and left the other unapplied. Still buffered, they are frozen
    // again for the new table, whole
    assertThat(readAll(catalog.loadTable(TABLE_IDENTIFIER)))
        .as("the new table, which must get all of the buffered changes or none")
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(7L, "x2"));
    assertThat(driver.pendingTables()).isEmpty();
  }

  @Test
  public void testAPointerToAMissingManifestStopsTheTableInsteadOfFailingEveryCycle() {
    // the summary names a change set whose manifest is gone: remove_orphan_files with a window
    // shorter than the drain, or a staging directory cleaned by hand. No later cycle brings it
    // back, so the table is stopped with one ERROR rather than retried every interval
    appendRows(table, row(1L, "a"));
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, UUID.randomUUID().toString())
        .set(COPY_ON_WRITE_CURSOR_PROP, "{}")
        .commit();

    InMemoryCatalog watched = spy(catalog);
    CopyOnWriteRewriteDriver stopping =
        new CopyOnWriteRewriteDriver(watched, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitRequest request = requestOf(stagedFiles(update(1L, "a2")), 0, 5L, 6L);

    assertThat(stopping.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(stopping.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    verify(watched, times(1)).loadTable(TABLE_IDENTIFIER);
    // listed, and answered FAILED without the table being loaded: CommitComplete stays without
    // valid-through while the table holds its changes
    assertThat(stopping.pendingTables()).containsExactly(request.tableReference());

    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a"));
  }

  /**
   * One physical table, one drain. A case-insensitive catalog answers to {@code db.tbl} and to
   * {@code DB.TBL} with the same table, so {@code iceberg.tables=db.tbl,DB.TBL} has the workers
   * stage change files under two names for one table. Grouped by the name they were written under,
   * each set would get a drain of its own: two planners, two slice sequences and two OverwriteFiles
   * streams into one table, each writing the cursor and change set id into the summary the other
   * one reads. The second name is stopped instead, once, and the first drains on.
   */
  @Test
  public void testASecondNameForTheSameTableIsStoppedInsteadOfDrainingBesideTheFirst() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    TableReference asConfigured = TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());
    TableIdentifier otherCase = TableIdentifier.of(Namespace.of("DB"), "TBL");
    TableReference asSpelledAgain = TableReference.of(catalog.name(), otherCase, table.uuid());

    // the catalog resolves the other spelling to the same table, as a case-insensitive one does;
    // both references carry the UUID the worker read off the table it loaded
    InMemoryCatalog caseInsensitive = spy(catalog);
    doAnswer(invocation -> catalog.loadTable(TABLE_IDENTIFIER))
        .when(caseInsensitive)
        .loadTable(otherCase);
    CopyOnWriteRewriteDriver driver =
        new CopyOnWriteRewriteDriver(
            caseInsensitive, config, metadataEvents, sentEvents::add, taskCount());

    TableCommitter.Outcome first =
        driver.commit(
            requestOf(asConfigured, stagedFiles(table, asConfigured, update(1L, "a2")), 0, 5L, 6L));
    TableCommitRequest second =
        requestOf(asSpelledAgain, stagedFiles(table, asSpelledAgain, update(2L, "b2")), 1, 7L, 8L);

    assertThat(first).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(driver.commit(second))
        .as("the second name for the table draining under the first one")
        .isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(driver.commit(second))
        .as("stopped, so it is not attempted again every cycle")
        .isEqualTo(TableCommitter.Outcome.FAILED);

    table.refresh();
    assertThat(readAll(table))
        .as("what the one drain applied: the second name committed nothing of its own")
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b"));
    assertThat(connectorSnapshots()).as("one drain, one slice, one snapshot").hasSize(1);
  }

  @Test
  public void testATableStoppedWithItsResponsesSpentStaysInEveryCycle() {
    // stopped mid-drain, after the first slice spent the responses: nothing is buffered for the
    // table any more. A cycle that did not visit it would count only the other tables (or none)
    // and publish a valid-through the table has not reached, the tail of an acknowledged change
    // set still outside it. Visited, it answers FAILED, as a failing merge-on-read table does
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    TableReference written = TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());
    List<StagedChangeFile> staged =
        stagedFiles(table, written, update(1L, "a2"), update(2L, "b2"), update(3L, "c2"));
    // the first slice commits and the second is handed out, normalized already
    committer.commitOneSlice(requestOf(written, staged, 0, 5L, 6L));
    // the file the third slice is read from, deleted by a bucket lifecycle rule
    staged.forEach(file -> table.io().deleteFile(file.location()));

    assertThat(committer.pendingTables()).containsExactly(written);
    assertThat(committer.commit(emptyRequest(written))).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(committer.pendingTables())
        .as("a table stopped with the tail of its change set unapplied")
        .containsExactly(written);
    assertThat(committer.commit(emptyRequest(written))).isEqualTo(TableCommitter.Outcome.FAILED);

    // the same after a restart, when the pointer names a manifest that is gone: the table is
    // stopped on the visit that reads it, with nothing buffered for it
    when(config.tables()).thenReturn(List.of(TABLE_IDENTIFIER.toString()));
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    table.refresh();
    UUID changeSetId =
        UUID.fromString(table.currentSnapshot().summary().get(COPY_ON_WRITE_CHANGE_SET_ID_PROP));
    table
        .io()
        .deleteFile(
            ChangeSetManifest.location(
                StagedChangeFileWriter.stagingLocation(table, null),
                written,
                GROUP_ID,
                changeSetId));
    for (TableReference visited : committer.pendingTables()) {
      assertThat(committer.commit(emptyRequest(visited))).isEqualTo(TableCommitter.Outcome.FAILED);
    }
    assertThat(committer.pendingTables())
        .as("a table stopped as its pointer was read after a restart")
        .containsExactly(written);
    assertThat(committer.commit(emptyRequest(written))).isEqualTo(TableCommitter.Outcome.FAILED);

    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"), tuple(3L, "c"));
  }

  @Test
  public void testAManifestThatCannotBeReadNamesItsChangeSetPathAndRemedy() throws IOException {
    appendRows(table, row(1L, "a"));
    UUID changeSetId = UUID.randomUUID();
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, changeSetId.toString())
        .commit();
    String stagingLocation = StagedChangeFileWriter.stagingLocation(table, null);
    String location =
        ChangeSetManifest.location(stagingLocation, TABLE_REFERENCE, GROUP_ID, changeSetId);
    ChangeSetStore store = new ChangeSetStore(config);

    assertThatThrownBy(
            () -> store.loadDrainState(table, null, TABLE_REFERENCE, stagingLocation, ID_FIELDS))
        .isInstanceOf(PermanentCopyOnWriteException.class)
        .hasMessageContaining(TABLE_IDENTIFIER.toString())
        .hasMessageContaining(changeSetId.toString())
        .hasMessageContaining(location)
        .hasMessageContaining("does not exist")
        .hasMessageContaining("Restore the manifest")
        .hasMessageContaining(OFFSETS_PROP)
        .hasMessageContaining(COPY_ON_WRITE_CHANGE_SET_ID_PROP);

    // written, but not a manifest: cut short, or a required field missing
    for (String content :
        List.of("{\"changeSetId\":", "{\"changeSetId\":\"" + changeSetId + "\"}")) {
      try (OutputStream out = table.io().newOutputFile(location).createOrOverwrite()) {
        out.write(content.getBytes(StandardCharsets.UTF_8));
      }
      assertThatThrownBy(
              () -> store.loadDrainState(table, null, TABLE_REFERENCE, stagingLocation, ID_FIELDS))
          .isInstanceOf(PermanentCopyOnWriteException.class)
          .hasMessageContaining(location)
          .hasMessageContaining("is not a change-set manifest");
    }

    // storage failing to serve it may pass on its own: an ordinary failure, retried next cycle
    Table unreachable = spy(table);
    FileIO io = mock(FileIO.class);
    doThrow(new UncheckedIOException(new IOException("connection reset")))
        .when(io)
        .newInputFile(location);
    doReturn(io).when(unreachable).io();
    assertThatThrownBy(
            () ->
                store.loadDrainState(
                    unreachable, null, TABLE_REFERENCE, stagingLocation, ID_FIELDS))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("connection reset");
  }

  @Test
  public void testAStagedFileOfTheChangeSetThatIsGoneStopsTheTableInsteadOfFailingEveryCycle() {
    // a staged file deleted behind the connector's back: a bucket lifecycle rule shorter than the
    // drain, a staging directory cleaned by hand, a staging location on file: while the tasks sit
    // on another node. The rows it held were acknowledged when their envelopes were sent, so no
    // cycle can read them again: normalizing the change set fails the same way every interval
    appendRows(table, row(1L, "a"));
    List<StagedChangeFile> staged = stagedFiles(update(1L, "a2"));
    TableCommitRequest request = requestOf(staged, 0, 5L, 6L);
    table.io().deleteFile(staged.get(0).location());

    // one ERROR with the remedy and the table is stopped, rather than a warning per cycle forever
    assertThat(committer.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(committer.commit(request)).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(committer.pendingTables()).containsExactly(request.tableReference());

    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a"));
  }

  @Test
  public void testAStagedFileTheStorageCannotServeIsRetriedRatherThanStoppingTheTable() {
    // the other side of the same classification: a read that fails once may pass on its own, and
    // stopping the table on it would need a restart to drain a change set nothing is wrong with
    appendRows(table, row(1L, "a"));
    List<StagedChangeFile> staged = stagedFiles(update(1L, "a2"));
    String location = staged.get(0).location();
    AtomicBoolean refused = new AtomicBoolean();

    InMemoryCatalog watched = spy(catalog);
    doAnswer(
            invocation -> {
              Table loaded = spy(catalog.loadTable(TABLE_IDENTIFIER));
              FileIO io = spy(loaded.io());
              doAnswer(
                      open -> {
                        if (refused.compareAndSet(false, true)) {
                          throw new UncheckedIOException(new IOException("connection reset"));
                        }
                        return open.callRealMethod();
                      })
                  .when(io)
                  .newInputFile(location);
              doReturn(io).when(loaded).io();
              return loaded;
            })
        .when(watched)
        .loadTable(TABLE_IDENTIFIER);
    CopyOnWriteRewriteDriver retrying =
        new CopyOnWriteRewriteDriver(watched, config, metadataEvents, sentEvents::add, taskCount());

    assertThat(retrying.commit(requestOf(staged, 0, 5L, 6L)))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(refused).isTrue();

    table.refresh();
    assertThat(readAll(table)).containsExactly(tuple(1L, "a2"));
  }

  @Test
  public void testTheFreezeTellsAcceptedFilesFromRejectedOnesWithoutOpeningAny() {
    // the freeze runs on the call doCommit() waits for: a header read per staged file grows with
    // every file a long drain left buffered, and holds the coordinator thread for all of them
    appendRows(table, row(7L, "x"), row(8L, "y"));
    List<StagedChangeFile> files =
        ImmutableList.<StagedChangeFile>builder()
            .addAll(stagedFiles(update(7L, "x2")))
            .addAll(stagedFiles(delete(8L)))
            .build();
    TableCommitRequest request = requestOf(files, 0, 5L, 6L);

    Table watched = spy(table);
    FileIO io = spy(table.io());
    doReturn(io).when(watched).io();
    ChangeSetStore changeSets = new ChangeSetStore(config);
    String stagingLocation = StagedChangeFileWriter.stagingLocation(table, null);

    assertThatThrownBy(
            () ->
                changeSets.freeze(
                    watched,
                    null,
                    TABLE_REFERENCE,
                    stagingLocation,
                    Set.of(1, 2),
                    request,
                    List.of(),
                    Set.of()))
        .isInstanceOf(TableCommitRejectedException.class)
        .hasMessageContaining("[1, 2] of table db.tbl are not a subset of [1]");
    assertThat(
            changeSets.freeze(
                watched,
                null,
                TABLE_REFERENCE,
                stagingLocation,
                ID_FIELDS,
                request,
                List.of(),
                Set.of()))
        .isNotNull();

    verify(io, never()).newInputFile(anyString());
    verify(io, never()).newInputFile(anyString(), anyLong());

    // a descriptor that does not say which set its file was written with is accepted by none
    StagedChangeFile written = files.get(0);
    StagedChangeFile unspoken =
        new StagedChangeFile(
            written.location(),
            written.fileSizeBytes(),
            written.recordCount(),
            written.schemaId(),
            written.formatVersion(),
            written.lowerBounds(),
            written.upperBounds());
    assertThatThrownBy(
            () ->
                changeSets.freeze(
                    watched,
                    null,
                    TABLE_REFERENCE,
                    stagingLocation,
                    ID_FIELDS,
                    requestOf(List.of(unspoken), 0, 7L, 8L),
                    List.of(),
                    Set.of()))
        .isInstanceOf(TableCommitRejectedException.class)
        .hasMessageContaining("[1] of table db.tbl are not a subset of []")
        .hasMessageContaining(written.location());
  }

  // -- fixtures -------------------------------------------------------------------------------

  @Test
  public void testAResumedDrainKeepsResponsesThatWereNeverPartOfIt() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    // one slice lands, then the coordinator dies with the change set half applied
    List<StagedChangeFile> frozen = stagedFiles(update(1L, "a1"), update(2L, "b2"));
    committer.commitOneSlice(requestOf(frozen, 0, 10L, 11L));

    // a new coordinator over the same catalogue, holding a response that no change set froze
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitRequest late = requestOf(stagedFiles(update(3L, "c3")), 0, 20L, 21L);

    assertThat(committer.commit(late)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    // the resumed drain finished and the late response was applied too, not swallowed by the
    // watermark the resumed drain committed
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"));
  }

  @Test
  public void testAFreezeNeverMovesTheWatermarkOfAPartitionBelowTheConnectorSnapshot() {
    // this connector has applied the responses below offset 10 on partition 0 and 20 on partition 1
    AppendFiles applied = table.newAppend();
    writeRows(table, row(1L, "a"), row(2L, "b")).forEach(applied::appendFile);
    applied.set(OFFSETS_PROP, "{\"0\":10,\"1\":20}").commit();

    // a consumer back from a restart has read nothing from partition 1 yet and is still behind the
    // snapshot on partition 0: the position this change set freezes at names neither
    Envelope fresh = rowChangesAt(2, 0L, stagedFiles(update(1L, "a2"), update(2L, "b2")));
    assertThat(committer.commit(requestOf(List.of(fresh), ImmutableMap.of(0, 5L, 2, 1L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    String frozenAt = table.currentSnapshot().summary().get(OFFSETS_PROP);
    long snapshotsAfterFreeze = countSnapshots(table);

    // reading on, it meets the responses that snapshot had already applied
    Envelope appliedOnPartitionZero = rowChangesAt(0, 9L, stagedFiles(update(1L, "a-again")));
    Envelope appliedOnPartitionOne = rowChangesAt(1, 19L, stagedFiles(update(2L, "b-again")));
    assertThat(
            committer.commit(
                requestOf(
                    List.of(appliedOnPartitionZero, appliedOnPartitionOne),
                    ImmutableMap.of(0, 10L, 1, 20L, 2, 1L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    // the next freeze filtered both out: the watermark kept both partitions where the snapshot had
    // them, even though the position it was frozen at lagged on one and missed the other
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a2"), tuple(2L, "b2"));
    assertThat(countSnapshots(table)).isEqualTo(snapshotsAfterFreeze);
    assertThat(frozenAt).isEqualTo("{\"0\":10,\"1\":20,\"2\":1}");
  }

  @Test
  public void testADrainIsResumedEvenWhenAnotherWriterCommittedInBetween() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    List<StagedChangeFile> frozen = stagedFiles(update(1L, "a1"), update(2L, "b2"));
    committer.commitOneSlice(requestOf(frozen, 0, 10L, 11L));

    // somebody else commits between two slices; summaries are not inherited in Iceberg, so the
    // branch's latest snapshot no longer carries the change-set pointer
    appendRows(table, row(3L, "outsider"));

    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    assertThat(committer.commit(requestOf(List.of(), 0, 20L, 21L)))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "outsider"));
  }

  @Test
  public void testADrainIsResumedEvenWhenAnotherConnectorCommittedInBetween() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    List<StagedChangeFile> frozen = stagedFiles(update(1L, "a1"), update(2L, "b2"));
    committer.commitOneSlice(requestOf(frozen, 0, 10L, 11L));

    // another connector on the same table commits between two slices, with a commit id of its own
    commitAsAnotherConnector(row(3L, "outsider"));

    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitter.Outcome outcome = committer.commit(requestOf(List.of(), 0, 20L, 21L));
    table.refresh();

    // taken for this connector's, that snapshot says no change set is in progress: the part not yet
    // applied is never applied, and the responses that carried it were spent by the first slice
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "outsider"));
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
  }

  @Test
  public void testTheTailOfMergeOnReadWaitsForADrainAnotherConnectorCommittedOver() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    // one slice lands and another connector commits on top of it; this one goes to merge-on-read,
    // which buffers its DataWritten, and back to copy-on-write
    committer.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a1"), update(2L, "b2")), 0, 10L, 11L));
    commitAsAnotherConnector(row(4L, "outsider"));
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    Envelope tail = dataWrittenAt(0, 20L, row(3L, "c"));

    TableCommitter.Outcome outcome =
        committer.commit(requestOf(List.of(tail), ImmutableMap.of(0, 21L)));
    table.refresh();

    // a drain hidden by that snapshot sends the tail to merge-on-read, which finds the pointer
    // under this connector's offsets and refuses it: every cycle, with nothing resuming the drain
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(
            tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c"), tuple(4L, "outsider"));
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
  }

  @Test
  public void testAPointerToAChangeSetOfAnotherTableFreezesANewOneAndLeavesThatOneAlone() {
    appendRows(table, row(7L, "x"), row(8L, "y"));

    // another table, keyed by more fields, whose change set this table's pointer resolves to: a
    // staging location the two share, or a table dropped and recreated under the same name
    TableIdentifier otherIdentifier = TableIdentifier.of(NAMESPACE, "other");
    Schema otherSchema =
        new Schema(
            List.of(
                required(1, "id", Types.LongType.get()),
                required(2, "data", Types.StringType.get())),
            Set.of(1, 2));
    Table other = catalog.createTable(otherIdentifier, otherSchema, PartitionSpec.unpartitioned());
    StagedChangeFileWriter otherWriter =
        new StagedChangeFileWriter(
            other,
            TableReference.of("catalog", otherIdentifier),
            Set.of(1, 2),
            null,
            GROUP_ID,
            "task-0");
    GenericRecord otherRow = GenericRecord.create(otherSchema);
    otherRow.setField("id", 7L);
    otherRow.setField("data", "x");
    otherWriter.write(
        otherWriter.stagedRow(otherRow, StagedChangeSchema.OP_DELETE, "other-topic", 0, 0L));
    List<StagedChangeFile> otherFiles = otherWriter.complete();

    ChangeSetManifest foreign =
        ChangeSetManifest.freeze(
            other, Set.of(1, 2), otherFiles, Set.of("other-topic"), ImmutableMap.of(0, 1L), null);
    String foreignLocation =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            foreign.changeSetId());
    foreign.write(table.io(), foreignLocation);
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, foreign.changeSetId().toString())
        .commit();

    TableCommitter.Outcome outcome =
        committer.commit(requestOf(stagedFiles(update(8L, "y2")), 0, 5L, 6L));
    table.refresh();

    // taken for this table's, its files would be carried over into this change set as a dropped
    // one's (its identifier fields differ) and its delete applied here
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(7L, "x"), tuple(8L, "y2"));
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(io.fileExists(foreignLocation)).as("the other table's manifest").isTrue();
    assertThat(otherFiles).allSatisfy(file -> assertThat(io.fileExists(file.location())).isTrue());
  }

  @Test
  public void testAManifestThatNamesNoTableIsResumed() throws IOException {
    appendRows(table, row(7L, "x"), row(8L, "y"));

    ChangeSetManifest frozen =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            stagedFiles(update(7L, "x7")),
            Set.of("src-topic"),
            ImmutableMap.of(0, 1L),
            null);
    String location =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            frozen.changeSetId());
    frozen.write(table.io(), location);
    // a manifest that does not say which table it was frozen for: nothing to hold it against
    String content;
    try (InputStream in = table.io().newInputFile(location).newStream()) {
      content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (OutputStream out = table.io().newOutputFile(location).createOrOverwrite()) {
      out.write(
          content
              .replace("\"tableUuid\":\"" + table.uuid() + "\",", "")
              .getBytes(StandardCharsets.UTF_8));
    }
    assertThat(ChangeSetManifest.read(table.io(), location).tableUuid()).isNull();
    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, frozen.changeSetId().toString())
        .commit();

    TableCommitter.Outcome outcome = committer.commit(requestOf(List.of(), 0, 5L, 6L));
    table.refresh();

    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(7L, "x7"), tuple(8L, "y"));
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
  }

  /** Key types whose cursor a JSON number or a plain string would not carry as they are. */
  private static Stream<Arguments> keysOfOtherRepresentations() {
    return Stream.of(
        Arguments.of(
            Types.FixedType.ofLength(2), new byte[] {0x01, 0x02}, new byte[] {(byte) 0xf0, 0x00}),
        Arguments.of(
            Types.UUIDType.get(),
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            UUID.fromString("00000000-0000-0001-0000-000000000000")),
        Arguments.of(Types.DecimalType.of(9, 2), new BigDecimal("1.50"), new BigDecimal("12.25")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("keysOfOtherRepresentations")
  public void testADrainResumesFromTheCursorInTheSnapshotSummaryWhateverTheKeyType(
      Type.PrimitiveType type, Object first, Object second) {
    TableIdentifier keyedIdentifier = TableIdentifier.of(NAMESPACE, "keyed");
    TableReference keyedReference = TableReference.of("catalog", keyedIdentifier);
    Schema keyedSchema =
        new Schema(
            List.of(required(1, "key", type), optional(2, "data", Types.StringType.get())),
            ID_FIELDS);
    Table keyed = catalog.createTable(keyedIdentifier, keyedSchema, PartitionSpec.unpartitioned());
    appendRows(keyed, keyedRow(keyedSchema, first, "a"), keyedRow(keyedSchema, second, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(keyed, keyedReference, ID_FIELDS, null, GROUP_ID, "task-0");
    writer.write(
        writer.stagedRow(
            keyedRow(keyedSchema, first, "a1"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, 0L));
    writer.write(
        writer.stagedRow(
            keyedRow(keyedSchema, second, "b2"), StagedChangeSchema.OP_UPDATE, "src-topic", 0, 1L));
    committer.commitOneSlice(requestOf(keyedReference, writer.complete(), 0, 10L, 11L));
    keyed.refresh();
    assertThat(keyed.currentSnapshot().summary()).containsKey(COPY_ON_WRITE_CURSOR_PROP);

    // a new coordinator has only the summary's cursor to tell the first key from the second
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableCommitter.Outcome outcome =
        committer.commit(requestOf(keyedReference, List.of(), 0, 20L, 21L));
    keyed.refresh();

    assertThat(readAll(keyed))
        .extracting(keyedRow -> keyedRow.toList().get(1))
        .containsExactlyInAnyOrder("a1", "b2");
    assertThat(outcome).isEqualTo(TableCommitter.Outcome.COMMITTED);
  }

  private static Record keyedRow(Schema schema, Object key, String data) {
    GenericRecord record = GenericRecord.create(schema);
    record.setField("key", key);
    record.setField("data", data);
    return record;
  }

  @Test
  public void testATableWithADrainInFlightIsOfferedACommitEvenWithAnEmptyBuffer() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    committer.commitOneSlice(requestOf(stagedFiles(update(1L, "a1"), update(2L, "b2")), 0, 1L, 2L));

    // the responses that started the drain are spent, so the table is out of tableCommitMap()
    // entirely: pendingTables() is the only thing that brings it back
    assertThat(committer.pendingTables()).containsExactly(TABLE_REFERENCE);
    assertThat(committer.commit(requestOf(List.of(), 0, 3L, 4L)))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committer.pendingTables()).isEmpty();
  }

  @Test
  public void testADrainCutShortByARestartResumesThoughNothingIsWrittenToTheTableAgain() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    TableIdentifier neverCreated = TableIdentifier.of(NAMESPACE, "never_created");
    when(config.tables()).thenReturn(List.of(TABLE_IDENTIFIER.toString(), neverCreated.toString()));
    // as a worker names it
    TableReference written = TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());

    // one slice lands, then the coordinator dies: the responses of the change set are spent, and
    // the stream of the table goes quiet for good
    committer.commitOneSlice(
        requestOf(
            written,
            stagedFiles(update(1L, "a1"), update(2L, "b2"), update(3L, "c3")),
            0,
            10L,
            11L));

    // a new coordinator: nothing is buffered, so pendingTables() is all its cycle visits
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    assertThat(committer.pendingTables())
        .as("a table of iceberg.tables is visited by the first cycle of a new coordinator")
        .extracting(TableReference::identifier)
        .contains(TABLE_IDENTIFIER);

    // the first cycle has no active tasks: the drain cannot resume yet, and the table must not
    // drop out of the next cycle for it. The table that does not exist has nothing to resume
    for (TableReference visited : committer.pendingTables()) {
      committer.commitWithNoTasks(emptyRequest(visited));
    }
    assertThat(committer.pendingTables())
        .extracting(TableReference::identifier)
        .containsExactly(TABLE_IDENTIFIER);

    for (TableReference visited : committer.pendingTables()) {
      assertThat(committer.commit(emptyRequest(visited)))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
    }
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"));
    assertThat(committer.pendingTables()).isEmpty();
  }

  @Test
  public void testADrainCutShortByARestartResumesUnderDynamicRoutingThoughNothingIsWrittenAgain() {
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    // the record names the table: the configuration does not, so no probe finds it after a restart
    when(config.routingStrategy()).thenReturn(RecordRoutingStrategy.DYNAMIC_FIELD);
    when(config.tables()).thenReturn(null);
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    TableReference written = TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());
    Envelope older = rowChangesAt(written, 0, 10L, stagedFiles(update(1L, "a1"), update(2L, "b2")));
    Envelope latest = rowChangesAt(written, 0, 12L, stagedFiles(update(3L, "c3")));
    TableCommitRequest cycle =
        new TableCommitRequest(
            written,
            List.of(latest, older),
            ImmutableMap.of(0, 13L),
            UUID.randomUUID(),
            OffsetDateTime.now());

    // one slice lands, then the coordinator dies: the envelopes of the change set are spent, and
    // the stream of the table goes quiet for good
    committer.commitOneSlice(cycle);
    assertThat(committer.replayAnchors())
        .as("the control topic is held at the latest envelope of a change set left to drain")
        .extracting(Envelope::partition, Envelope::offset)
        .containsExactly(tuple(0, 12L));

    // a new coordinator reads the control topic again from the anchor: only that envelope, which
    // brings the table into its first cycle and resumes the drain from the snapshot's pointer.
    // It dies too, one slice further
    TableCommitRequest replayed =
        new TableCommitRequest(
            written, List.of(latest), ImmutableMap.of(0, 13L), UUID.randomUUID(), null);
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    assertThat(committer.pendingTables()).as("nothing in the configuration names it").isEmpty();
    committer.commitOneSlice(replayed);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c"));
    assertThat(committer.replayAnchors())
        .as("a drain resumed by a replay anchors again, or a second restart loses the table")
        .extracting(Envelope::partition, Envelope::offset)
        .containsExactly(tuple(0, 12L));

    // the third coordinator drains the rest; the replayed envelope is found applied and spent
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    assertThat(committer.commit(replayed)).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"));
    assertThat(committer.replayAnchors()).as("drained: nothing holds the offsets").isEmpty();
    assertThat(committer.pendingTables()).isEmpty();
  }

  @Test
  public void testAChangeSetAdoptingTheFilesOfADroppedOneKeepsItsReplayAnchor() {
    // staged while the identifier fields were (id, data); narrowed to (id) mid-drain, the change
    // set is dropped and its files join the next one. That one freezes from an empty buffer and
    // has no envelope of its own to anchor at
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .requireColumn("data")
        .setIdentifierFields("id", "data")
        .commit();
    appendRows(table, row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    when(config.routingStrategy()).thenReturn(RecordRoutingStrategy.DYNAMIC_FIELD);
    when(config.tables()).thenReturn(null);
    AtomicInteger sliceCommits = new AtomicInteger();
    committer =
        new CopyOnWriteRewriteDriver(
            catalog,
            config,
            metadataEvents,
            event -> {
              sentEvents.add(event);
              if (event.payload() instanceof CommitToTable && sliceCommits.incrementAndGet() == 1) {
                catalog
                    .loadTable(TABLE_IDENTIFIER)
                    .updateSchema()
                    .setIdentifierFields("id")
                    .commit();
              }
            },
            taskCount());
    TableReference written = TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());
    StagedChangeFileWriter wideWriter =
        new StagedChangeFileWriter(table, written, Set.of(1, 2), null, GROUP_ID, "task-0");
    for (long id = 1; id <= 3; id++) {
      GenericRecord deleted = GenericRecord.create(SCHEMA);
      deleted.setField("id", id);
      deleted.setField("data", String.valueOf((char) ('a' + id - 1)));
      wideWriter.write(
          wideWriter.stagedRow(deleted, StagedChangeSchema.OP_DELETE, "src-topic", 0, id));
    }
    Envelope envelope = rowChangesAt(written, 0, 5L, wideWriter.complete());

    // the first slice lands, the second finds the fields changed and takes the drain off
    committer.commitOneSlice(
        new TableCommitRequest(
            written, List.of(envelope), ImmutableMap.of(0, 6L), UUID.randomUUID(), null));
    table.refresh();
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(2L, "b"), tuple(3L, "c"));
    assertThat(committer.pendingTables()).extracting(TableReference::identifier).hasSize(1);

    // the next cycle, nothing buffered: dropped, adopted, and the first slice of the adopting
    // change set committed (key 1, already deleted). Its drain is as unfinished as the dropped
    // one's was
    for (TableReference visited : committer.pendingTables()) {
      committer.commitOneSlice(emptyRequest(visited));
    }
    table.refresh();
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(2L, "b"), tuple(3L, "c"));
    assertThat(sliceCommits).hasValue(2);
    assertThat(committer.replayAnchors())
        .as("the adopting change set holds the anchor of the dropped one")
        .extracting(Envelope::partition, Envelope::offset)
        .containsExactly(tuple(0, 5L));

    for (TableReference visited : committer.pendingTables()) {
      assertThat(committer.commit(emptyRequest(visited)))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
    }
    table.refresh();
    assertThat(readAll(table)).isEmpty();
    assertThat(committer.replayAnchors()).isEmpty();
  }

  private static TableCommitRequest emptyRequest(TableReference tableReference) {
    return new TableCommitRequest(
        tableReference,
        List.of(),
        ImmutableMap.of(0, 11L),
        UUID.randomUUID(),
        OffsetDateTime.now());
  }

  @Test
  public void testTheTailOfMergeOnReadCommitsAheadOfTheRowChangesAfterIt() {
    // merge-on-read -> copy-on-write with a cycle in flight: its DataWritten carries a commit id
    // the
    // copy-on-write coordinator never issued. It commits the merge-on-read way, in a snapshot of
    // its
    // own, before the change set of what came after it, whose slice then has to see its rows
    appendRows(table, row(1L, "a"), row(2L, "b"));
    Envelope tail = dataWrittenAt(0, 1L, row(3L, "c"), row(4L, "d"));
    Envelope rowChanges = rowChangesAt(0, 2L, stagedFiles(update(2L, "b2"), update(3L, "c3")));

    assertThat(committer.commit(requestOf(List.of(tail, rowChanges), ImmutableMap.of(0, 3L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table))
        .containsExactlyInAnyOrder(
            tuple(1L, "a"), tuple(2L, "b2"), tuple(3L, "c3"), tuple(4L, "d"));
    List<Snapshot> snapshots = connectorSnapshots();
    assertThat(snapshots).hasSize(2);
    assertThat(snapshots.get(0).operation()).isEqualTo(DataOperations.APPEND);
    // no further than the RowChangesWritten the tail's snapshot did not apply: the freeze filters
    // everything below the committed offsets out as already applied
    assertThat(snapshots.get(0).summary()).containsEntry(OFFSETS_PROP, "{\"0\":2}");
    assertThat(snapshots.get(1).summary()).containsEntry(OFFSETS_PROP, "{\"0\":3}");
  }

  @Test
  public void testTheTailOfMergeOnReadWaitsForTheDrainInProgressToFinish() {
    appendRows(table, row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    // one slice lands, then the connector goes to merge-on-read (which refuses the table over the
    // unfinished change set and buffers its DataWritten) and back to copy-on-write
    committer.commitOneSlice(
        requestOf(stagedFiles(update(1L, "a1"), update(2L, "b2")), 0, 10L, 11L));
    committer =
        new CopyOnWriteRewriteDriver(catalog, config, metadataEvents, sentEvents::add, taskCount());
    Envelope tail = dataWrittenAt(0, 20L, row(3L, "c"));

    assertThat(committer.commit(requestOf(List.of(tail), ImmutableMap.of(0, 21L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c"));
    // the change set first, both slices of it, and only then the tail
    List<Snapshot> snapshots = connectorSnapshots();
    assertThat(snapshots).hasSize(3);
    assertThat(snapshots.get(0).summary()).containsKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(snapshots.get(1).summary()).doesNotContainKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(snapshots.get(2).operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshots.get(2).summary()).containsEntry(OFFSETS_PROP, "{\"0\":21}");
  }

  @Test
  public void testRowChangesOlderThanTheTailOnItsPartitionAreAppliedBeforeIt() {
    // copy-on-write -> merge-on-read -> copy-on-write before anything froze: the RowChangesWritten
    // merge-on-read refused, the DataWritten it buffered above them, and the RowChangesWritten of
    // the copy-on-write that came back, all on one partition. They apply in that order
    appendRows(table, row(1L, "a"), row(2L, "b"));
    Envelope older = rowChangesAt(0, 1L, stagedFiles(update(1L, "a1")));
    Envelope tail = dataWrittenAt(0, 2L, row(3L, "c"));
    Envelope newer = rowChangesAt(0, 3L, stagedFiles(update(3L, "c3")));

    // one cycle: the leading row-changes drain in full, but the tail and what comes after it on
    // this partition are still owed a commit of their own: COMMITTED here would let a cycle's
    // CommitComplete claim more than this table has actually applied
    assertThat(committer.commitOnce(requestOf(List.of(older, tail, newer), ImmutableMap.of(0, 4L))))
        .isEqualTo(TableCommitter.Outcome.PENDING);
    table.refresh();

    // only what is older than the tail; the change set's watermark stops at the tail
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b"));
    assertThat(connectorSnapshots()).hasSize(1);
    assertThat(connectorSnapshots().get(0).summary()).containsEntry(OFFSETS_PROP, "{\"0\":2}");

    // the next cycle's buffer, without what this one spent
    assertThat(committer.commit(requestOf(List.of(tail, newer), ImmutableMap.of(0, 4L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table))
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b"), tuple(3L, "c3"));
    List<Snapshot> snapshots = connectorSnapshots();
    assertThat(snapshots).hasSize(3);
    assertThat(snapshots.get(1).operation()).isEqualTo(DataOperations.APPEND);
  }

  @Test
  public void testALeftoverMergeOnReadTailKeepsTheCycleFromReportingTheTableCommitted() {
    // the row-changes envelope was already applied by an earlier cycle (committed offsets cover
    // it) but the merge-on-read tail after it on the same partition has not been. freeze() finds
    // nothing new to freeze once it filters the row-changes out, but the tail is still owed a
    // commit: the cycle must not report this table COMMITTED, or CommitComplete.validThroughTs
    // would claim more than the table has actually applied
    appendRows(table, row(1L, "a"));
    table.newAppend().set(OFFSETS_PROP, "{\"0\":6}").commit();
    Envelope alreadyApplied = rowChangesAt(0, 5L, stagedFiles(update(1L, "a2")));
    Envelope tail = dataWrittenAt(0, 7L, row(2L, "b"));

    TableCommitter.Outcome first =
        committer.commitOnce(requestOf(List.of(alreadyApplied, tail), ImmutableMap.of(0, 8L)));
    assertThat(first)
        .as("a withheld merge-on-read tail must not let the cycle report this table committed")
        .isEqualTo(TableCommitter.Outcome.PENDING);
    table.refresh();
    assertThat(readAll(table)).as("nothing applied yet").containsExactly(tuple(1L, "a"));

    // the next cycle's buffer, without what this one spent
    TableCommitter.Outcome second =
        committer.commitOnce(requestOf(List.of(tail), ImmutableMap.of(0, 8L)));
    assertThat(second).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a"), tuple(2L, "b"));
  }

  @Test
  public void testExceedingMaxRewriteBytesSlicesTheChangeSetInsteadOfRejectingIt() {
    // two appends, so the two keys sit in two files and the plan can be shrunk to one of them
    appendRows(table, row(1L, "a"));
    appendRows(table, row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1_000_000L);
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(1L);

    assertThat(
            committer.commit(requestOf(stagedFiles(update(1L, "a1"), update(2L, "b2")), 0, 1L, 2L)))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    // the quota sliced the change set, it did not refuse it
    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"));
    assertThat(connectorSnapshots()).hasSize(2);
  }

  @Test
  public void testStayingWithinMaxRewriteBytesCommitsInOneSlice() {
    appendRows(table, row(1L, "a"));
    appendRows(table, row(2L, "b"));

    assertThat(
            committer.commit(requestOf(stagedFiles(update(1L, "a1"), update(2L, "b2")), 0, 1L, 2L)))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table)).containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"));
    assertThat(connectorSnapshots()).hasSize(1);
  }

  @Test
  public void testTheByteQuotaTakesTheLargestPrefixOfTheSliceThatFits() {
    // one key per file, and the quota holds the files of the first three keys: the slice is those
    // three, not the two that halving the slice stops at
    long quota =
        appendFile(row(1L, "a")).fileSizeInBytes()
            + appendFile(row(2L, "b")).fileSizeInBytes()
            + appendFile(row(3L, "c")).fileSizeInBytes();
    appendFile(row(4L, "d"));
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(quota);

    committer.commitOneSlice(
        requestOf(
            stagedFiles(update(1L, "a1"), update(2L, "b2"), update(3L, "c3"), update(4L, "d4")),
            0,
            5L,
            6L));
    table.refresh();

    assertThat(readAll(table))
        .containsExactlyInAnyOrder(
            tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"), tuple(4L, "d"));
  }

  @Test
  public void testASliceThatTakesMoreThanFiveHalvingsToFitStillGoesOutWithinTheByteQuota() {
    // one key per file and a quota of one file: 64 keys halved five times are still two files
    long quota = 0;
    ChangeOp[] updates = new ChangeOp[64];
    for (int id = 1; id <= updates.length; id++) {
      long fileSize = appendFile(row(id, "v")).fileSizeInBytes();
      quota = id == 1 ? fileSize : quota;
      updates[id - 1] = update(id, "v" + id);
    }
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(quota);

    committer.commitOneSlice(requestOf(stagedFiles(updates), 0, 70L, 71L));
    table.refresh();

    Snapshot slice = Iterables.getOnlyElement(connectorSnapshots());
    assertThat(Long.parseLong(slice.summary().get(SnapshotSummary.REMOVED_FILE_SIZE_PROP)))
        .isLessThanOrEqualTo(quota);
    assertThat(readAll(table)).contains(tuple(1L, "v1")).doesNotContain(tuple(2L, "v2"));
  }

  @Test
  public void testTheByteQuotaPlansTheSliceNoMoreThanLogOfItsKeysTimes() {
    // every plan scans the table's manifests on the commit pool: shrinking the slice a key at a
    // time finds the same prefix, after as many plans as the slice has keys
    long quota = 0;
    ChangeOp[] updates = new ChangeOp[64];
    for (int id = 1; id <= updates.length; id++) {
      long fileSize = appendFile(row(id, "v")).fileSizeInBytes();
      quota = id == 1 ? fileSize : quota;
      updates[id - 1] = update(id, "v" + id);
    }
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(quota);

    AtomicInteger plans = new AtomicInteger();
    InMemoryCatalog watched = spy(catalog);
    doAnswer(
            invocation -> {
              Table loaded = spy(catalog.loadTable(TABLE_IDENTIFIER));
              doAnswer(
                      scan -> {
                        plans.incrementAndGet();
                        return scan.callRealMethod();
                      })
                  .when(loaded)
                  .newScan();
              return loaded;
            })
        .when(watched)
        .loadTable(TABLE_IDENTIFIER);
    CopyOnWriteRewriteDriver counting =
        new CopyOnWriteRewriteDriver(watched, config, metadataEvents, sentEvents::add, taskCount());

    // the first slice only: answering it would start the next one, with plans of its own
    assertThat(counting.handOutOneSlice(requestOf(stagedFiles(updates), 0, 70L, 71L))).isNotEmpty();

    // the whole slice, then a bisection of its 64 keys down to the one that fits
    assertThat(plans).hasValueBetween(1, 1 + 6);
  }

  @Test
  public void testWithoutKeyBoundsASliceOverTheByteQuotaGoesOutWhole() {
    // no column metrics, so every key plans every file: no fewer keys plan any less, and slicing
    // the change set would only rewrite the whole table once per slice
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none").commit();
    appendFile(row(1L, "a"));
    appendFile(row(2L, "b"));
    appendFile(row(3L, "c"));
    appendFile(row(4L, "d"));
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(1L);

    assertThat(
            committer.commit(
                requestOf(
                    stagedFiles(
                        update(1L, "a1"), update(2L, "b2"), update(3L, "c3"), update(4L, "d4")),
                    0,
                    5L,
                    6L)))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(readAll(table))
        .containsExactlyInAnyOrder(
            tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"), tuple(4L, "d4"));
    assertThat(connectorSnapshots()).hasSize(1);
  }

  /** Snapshots this connector produced, i.e. the ones a drain's slices are counted in. */
  private List<Snapshot> connectorSnapshots() {
    List<Snapshot> snapshots = Lists.newArrayList();
    table
        .snapshots()
        .forEach(
            snapshot -> {
              if (snapshot.summary().containsKey(COMMIT_ID_SNAPSHOT_PROP)) {
                snapshots.add(snapshot);
              }
            });
    return snapshots;
  }

  private void assertAddedFilesExist(Snapshot snapshot) {
    InMemoryFileIO io = (InMemoryFileIO) table.io();
    assertThat(snapshot.addedDataFiles(io))
        .isNotEmpty()
        .allSatisfy(
            file ->
                assertThat(io.fileExists(file.location()))
                    .as("%s, added by snapshot %s", file.location(), snapshot.snapshotId())
                    .isTrue());
  }

  /** One catalog commit of the table the committer loaded: {@code apply} makes the real one. */
  @FunctionalInterface
  private interface CatalogCommit {
    void commit(int attempt, TableMetadata updated, Runnable apply);
  }

  private InMemoryCatalog committingThrough(CatalogCommit commit) {
    return committingThrough(commit, loaded -> {});
  }

  /**
   * The catalog, except that the test table it hands out (to the committer and to the workers
   * alike) commits through {@code commit} and is a spy {@code stubs} can reach. Commits through
   * {@link #catalog} itself go straight in.
   */
  private InMemoryCatalog committingThrough(CatalogCommit commit, Consumer<Table> stubs) {
    InMemoryCatalog watched = spy(catalog);
    AtomicInteger attempts = new AtomicInteger();
    doAnswer(
            invocation -> {
              Table real = catalog.loadTable(TABLE_IDENTIFIER);
              TableOperations ops = ((HasTableOperations) real).operations();
              Table loaded =
                  spy(new BaseTable(new CommittingOperations(ops, commit, attempts), real.name()));
              stubs.accept(loaded);
              return loaded;
            })
        .when(watched)
        .loadTable(TABLE_IDENTIFIER);
    return watched;
  }

  private static final class CommittingOperations implements TableOperations {
    private final TableOperations delegate;
    private final CatalogCommit hook;
    private final AtomicInteger attempts;

    private CommittingOperations(
        TableOperations delegate, CatalogCommit hook, AtomicInteger attempts) {
      this.delegate = delegate;
      this.hook = hook;
      this.attempts = attempts;
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
      hook.commit(attempts.incrementAndGet(), metadata, () -> delegate.commit(base, metadata));
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

  /**
   * The catalog, except that the test table it hands out refreshes by name, as a REST catalog does:
   * once dropped, a refresh throws {@code NoSuchTableException}; created again under the name, a
   * refresh quietly turns into the new table. The in-memory table does neither: dropped, it stops
   * refreshing; recreated, it fails the refresh on the UUID.
   */
  private InMemoryCatalog refreshingByName() {
    return refreshingByName((attempt, updated, apply) -> apply.run());
  }

  /** {@link #refreshingByName()}, committing each table it hands out through {@code commit}. */
  private InMemoryCatalog refreshingByName(CatalogCommit commit) {
    InMemoryCatalog watched = spy(catalog);
    AtomicInteger attempts = new AtomicInteger();
    doAnswer(
            invocation ->
                new BaseTable(
                    new ByNameOperations(catalog, TABLE_IDENTIFIER, commit, attempts),
                    catalog.loadTable(TABLE_IDENTIFIER).name()))
        .when(watched)
        .loadTable(TABLE_IDENTIFIER);
    return watched;
  }

  private static final class ByNameOperations implements TableOperations {
    private final InMemoryCatalog catalog;
    private final TableIdentifier identifier;
    private final CatalogCommit hook;
    private final AtomicInteger attempts;
    private TableOperations delegate;

    private ByNameOperations(
        InMemoryCatalog catalog,
        TableIdentifier identifier,
        CatalogCommit hook,
        AtomicInteger attempts) {
      this.catalog = catalog;
      this.identifier = identifier;
      this.hook = hook;
      this.attempts = attempts;
      this.delegate = load();
    }

    private TableOperations load() {
      return ((HasTableOperations) catalog.loadTable(identifier)).operations();
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      delegate = load();
      return delegate.current();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      TableOperations target = delegate;
      hook.commit(attempts.incrementAndGet(), metadata, () -> target.commit(base, metadata));
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

  private TableSinkConfig tableConfigWithBranch(String branch) {
    TableSinkConfig cfg = mock(TableSinkConfig.class);
    when(cfg.commitBranch()).thenReturn(branch);
    return cfg;
  }

  private TableCommitRequest requestOf(
      List<StagedChangeFile> files, int partition, long offset, long nextOffset) {
    return requestOf(TABLE_REFERENCE, files, partition, offset, nextOffset);
  }

  private TableCommitRequest requestOf(
      TableReference tableReference,
      List<StagedChangeFile> files,
      int partition,
      long offset,
      long nextOffset) {
    return requestOf(
        tableReference,
        files,
        partition,
        offset,
        nextOffset,
        UUID.randomUUID(),
        OffsetDateTime.now());
  }

  private TableCommitRequest requestOf(
      TableReference tableReference,
      List<StagedChangeFile> files,
      int partition,
      long offset,
      long nextOffset,
      UUID commitId,
      OffsetDateTime validThroughTs) {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), tableReference, "task-0", List.of("src-topic"), files);
    Event event = new Event(GROUP_ID, payload);
    Envelope envelope = new Envelope(event, partition, offset);
    return new TableCommitRequest(
        tableReference,
        List.of(envelope),
        ImmutableMap.of(partition, nextOffset),
        commitId,
        validThroughTs);
  }

  private TableCommitRequest requestOf(
      List<Envelope> envelopes, Map<Integer, Long> controlTopicOffsets) {
    return new TableCommitRequest(
        TABLE_REFERENCE, envelopes, controlTopicOffsets, UUID.randomUUID(), OffsetDateTime.now());
  }

  private Envelope rowChangesAt(int partition, long offset, List<StagedChangeFile> files) {
    return rowChangesAt(TABLE_REFERENCE, partition, offset, files);
  }

  private Envelope rowChangesAt(
      TableReference tableReference, int partition, long offset, List<StagedChangeFile> files) {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), tableReference, "task-0", List.of("src-topic"), files);
    return new Envelope(new Event(GROUP_ID, payload), partition, offset);
  }

  /** A merge-on-read response: data files written but not committed, under another commit id. */
  private Envelope dataWrittenAt(int partition, long offset, Record... rows) {
    DataWritten payload =
        new DataWritten(
            table.spec().partitionType(),
            UUID.randomUUID(),
            TABLE_REFERENCE,
            writeRows(table, rows),
            List.of());
    return new Envelope(new Event(GROUP_ID, payload), partition, offset);
  }

  private List<StagedChangeFile> stagedFiles(ChangeOp... ops) {
    return stagedFiles(table, TABLE_REFERENCE, ops);
  }

  private List<StagedChangeFile> stagedFiles(
      Table target, TableReference tableReference, ChangeOp... ops) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(target, tableReference, ID_FIELDS, null, GROUP_ID, "task-0");
    long offset = 0;
    for (ChangeOp op : ops) {
      GenericRecord tableRow = GenericRecord.create(SCHEMA);
      tableRow.setField("id", op.id);
      tableRow.setField("data", op.data);
      writer.write(writer.stagedRow(tableRow, op.code, "src-topic", 0, offset++));
    }
    return writer.complete();
  }

  private ChangeOp insert(long id, String data) {
    return new ChangeOp(id, StagedChangeSchema.OP_INSERT, data);
  }

  private ChangeOp update(long id, String data) {
    return new ChangeOp(id, StagedChangeSchema.OP_UPDATE, data);
  }

  private ChangeOp delete(long id) {
    return new ChangeOp(id, StagedChangeSchema.OP_DELETE, null);
  }

  private static final class ChangeOp {
    private final long id;
    private final int code;
    private final String data;

    private ChangeOp(long id, int code, String data) {
      this.id = id;
      this.code = code;
      this.data = data;
    }
  }

  private void appendRows(Table target, Record... rows) {
    AppendFiles append = target.newAppend();
    writeRows(target, rows).forEach(append::appendFile);
    append.commit();
  }

  /** A commit of another connector on the table: a commit id and offsets of its own group. */
  private void commitAsAnotherConnector(Record... rows) {
    AppendFiles append = table.newAppend();
    writeRows(table, rows).forEach(append::appendFile);
    append
        .set(COMMIT_ID_SNAPSHOT_PROP, UUID.randomUUID().toString())
        .set("kafka.connect.offsets." + CTL_TOPIC + ".cg-another-connector", "{\"0\":7}")
        .commit();
  }

  private List<DataFile> writeRows(Table target, Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            writerFactory(target),
            OutputFileFactory.builderFor(target, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            target.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(target.spec(), target.schema());
        key.partition(row);
        writer.write(row, target.spec(), key);
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return writer.result().dataFiles();
  }

  private FileWriterFactory<Record> writerFactory(Table target) {
    return new GenericFileWriterFactory.Builder(target).dataSchema(target.schema()).build();
  }

  /** One data file of the test table, {@code rows} at positions 0, 1, 2... in the order given. */
  private DataFile appendFile(Record... rows) {
    DataFile file = Iterables.getOnlyElement(writeRows(table, rows));
    table.newAppend().appendFile(file).commit();
    return file;
  }

  /** A position delete file, not committed: scoped to one data file if it names one, else wider. */
  private DeleteFile positionDelete(List<Pair<String, Long>> positions) {
    List<Pair<String, Long>> sorted = Lists.newArrayList(positions);
    sorted.sort(
        Comparator.comparing((Pair<String, Long> position) -> position.first())
            .thenComparing(Pair::second));
    PositionDeleteWriter<Record> writer =
        writerFactory(table)
            .newPositionDeleteWriter(
                OutputFileFactory.builderFor(table, 1, 1).build().newOutputFile(),
                table.spec(),
                null);
    PositionDelete<Record> delete = PositionDelete.create();
    try (PositionDeleteWriter<Record> open = writer) {
      for (Pair<String, Long> position : sorted) {
        open.write(delete.set(position.first(), position.second(), null));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return writer.toDeleteFile();
  }

  /** An equality delete file on {@code id}, not committed. */
  private DeleteFile equalityDelete(long... ids) {
    return equalityDelete(table.spec(), ids);
  }

  /** An equality delete file on {@code id} written under {@code spec}, not committed. */
  private DeleteFile equalityDelete(PartitionSpec spec, long... ids) {
    Schema idSchema = table.schema().select("id");
    EqualityDeleteWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .equalityDeleteRowSchema(idSchema)
            .equalityFieldIds(new int[] {1})
            .build()
            .newEqualityDeleteWriter(
                OutputFileFactory.builderFor(table, 1, 1).build().newOutputFile(), spec, null);
    try (EqualityDeleteWriter<Record> open = writer) {
      for (long id : ids) {
        GenericRecord key = GenericRecord.create(idSchema);
        key.setField("id", id);
        open.write(key);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return writer.toDeleteFile();
  }

  private Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private List<org.assertj.core.groups.Tuple> readAll(Table target) {
    List<org.assertj.core.groups.Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(target).build()) {
      for (Record record : records) {
        rows.add(tuple(record.get(0, Object.class), record.get(1, Object.class)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private List<org.assertj.core.groups.Tuple> readAllOnRef(Table target, String ref) {
    List<org.assertj.core.groups.Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(target).useSnapshot(target.snapshot(ref).snapshotId()).build()) {
      for (Record record : records) {
        rows.add(tuple(record.get(0, Object.class), record.get(1, Object.class)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private boolean hasAnyDeleteFile(Table target) {
    try (CloseableIterable<FileScanTask> tasks = target.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (!task.deletes().isEmpty()) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long countSnapshots(Table target) {
    return Lists.newArrayList(target.snapshots()).size();
  }
}
