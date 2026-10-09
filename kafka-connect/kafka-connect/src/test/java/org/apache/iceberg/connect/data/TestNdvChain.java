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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.NdvChain.Kind;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

/** Base and gap of the NDV of snapshot S: own and adopted slices, empty points, marked gaps. */
public class TestNdvChain {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final String MARKER = "kafka.connect.statistics-enabled";
  private static final String CHANGE_SET = "kafka.connect.copy-on-write.change-set-id";
  private static final String COMMIT_ID = "kafka.connect.commit-id";
  private static final String OWN_CHANGE_SET = "8b6c1f3e-0000-0000-0000-000000000001";
  private static final String ADOPTED_CHANGE_SET = "8b6c1f3e-0000-0000-0000-000000000002";

  private InMemoryCatalog catalog;
  private Table table;
  private int fileCount = 0;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned());
  }

  @AfterEach
  public void after() throws Exception {
    catalog.close();
  }

  @Test
  public void baseIsParentWithStatistics() {
    long parent = connectorCommit();
    StatisticsFile record = writeStatistics(parent);
    long snapshot = connectorCommit();
    StatisticsAnswer answer = answer(snapshot);

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of(), answer);

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.baseSchema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(chain.gap()).isEmpty();
    assertThat(chain.answers()).containsExactly(answer);
  }

  @Test
  public void foreignAppendBetweenBaseAndSIsGap() {
    long analyzed = foreignAppend();
    StatisticsFile record = writeStatistics(analyzed); // any author: an external ANALYZE
    long foreign = foreignAppend();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
    assertThat(gapIds(chain)).containsExactly(foreign);
  }

  @Test
  public void foreignReplaceInGapIsNotRead() {
    long parent = connectorCommit();
    StatisticsFile record = writeStatistics(parent);
    compact();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.gap()).isEmpty();
  }

  @Test
  public void optimizeCopyingOlderRecordOverForeignAppendIsNotBase() {
    long analyzed = connectorCommit();
    StatisticsFile record = writeStatistics(analyzed, analyzed);
    long foreign = foreignAppend();
    long optimized = compact();
    writeStatistics(optimized, analyzed); // Trino copies the blobs of the nearest record
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(gapIds(chain)).containsExactly(foreign);
  }

  @Test
  public void optimizeCopyingRecordOfItsParentIsBase() {
    long analyzed = connectorCommit();
    writeStatistics(analyzed, analyzed);
    long optimized = compact();
    StatisticsFile copy = writeStatistics(optimized, analyzed);
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(copy);
    assertThat(chain.gap()).isEmpty();
  }

  @Test
  public void optimizeCopyingRecordOverDeleteIsBase() {
    foreignAppend();
    long analyzed = connectorCommit();
    writeStatistics(analyzed, analyzed);
    long deleted = deleteFirstFile();
    long optimized = compact();
    StatisticsFile copy = writeStatistics(optimized, analyzed);
    long snapshot = connectorCommit();
    assertThat(table.snapshot(deleted).summary())
        .doesNotContainKey("added-data-files")
        .containsKey("total-data-files");

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.base()).as("a delete adds no values").isEqualTo(copy);
    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.gap()).isEmpty();
  }

  @Test
  public void copyOverSnapshotWithoutFileCountsIsNotBase() {
    foreignAppend();
    long analyzed = connectorCommit();
    StatisticsFile record = writeStatistics(analyzed, analyzed);
    long deleted = deleteFirstFile();
    long optimized = compact();
    writeStatistics(optimized, analyzed);
    long snapshot = connectorCommit();
    Snapshot withoutCounts = spy(table.snapshot(deleted));
    Map<String, String> summary = Maps.newHashMap(withoutCounts.summary());
    summary.remove("total-data-files");
    doReturn(summary).when(withoutCounts).summary();
    Table withoutTotals = spy(table);
    doReturn(withoutCounts).when(withoutTotals).snapshot(deleted);

    NdvChain chain =
        NdvChain.resolve(
            withoutTotals,
            snapshot,
            ImmutableSet.of(snapshot),
            ImmutableSet.of(),
            ImmutableList.of());

    assertThat(chain.base()).as("unknown whether the snapshot added values").isEqualTo(record);
    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(gapIds(chain)).containsExactly(deleted);
  }

  @Test
  public void partialAnalyzeOfForeignAppendIsNotBase() {
    long analyzed = connectorCommit();
    StatisticsFile record = writeStatistics(analyzed, analyzed);
    long foreign = foreignAppend();
    writeStatistics(foreign, foreign, analyzed); // column 2 is copied from the older record
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(gapIds(chain)).containsExactly(foreign);
  }

  @Test
  public void recordWithBlobsOfSnapshotOutsideChainIsNotBase() {
    long analyzed = connectorCommit();
    StatisticsFile record = writeStatistics(analyzed, analyzed);
    long optimized = compact();
    writeStatistics(optimized, 123456789L); // the source is not an ancestor
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.gap()).isEmpty();
  }

  @Test
  public void slicesOfOwnChangeSetAreSkippedAcrossRestart() {
    long parent = connectorCommit();
    StatisticsFile record = writeStatistics(parent);
    // slices before the restart carry the commit id of the drain's first cycle
    slice(OWN_CHANGE_SET, "commit-a");
    slice(OWN_CHANGE_SET, "commit-a");
    long last = finalSlice("commit-b");

    NdvChain chain =
        resolve(
            last,
            ImmutableSet.of(last),
            ImmutableSet.of(OWN_CHANGE_SET),
            answer(last),
            answer(last));

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.gap()).isEmpty();
    assertThat(chain.answers()).hasSize(2);
  }

  @Test
  public void slicesOfAdoptedChangeSetAreOwn() {
    long parent = connectorCommit();
    StatisticsFile record = writeStatistics(parent);
    slice(ADOPTED_CHANGE_SET, "commit-a"); // a dropped change set adopted by the drain
    slice(OWN_CHANGE_SET, "commit-b");
    long last = finalSlice("commit-b");

    NdvChain adopted =
        resolve(last, ImmutableSet.of(last), ImmutableSet.of(OWN_CHANGE_SET, ADOPTED_CHANGE_SET));
    NdvChain notAdopted = resolve(last, ImmutableSet.of(last), ImmutableSet.of(OWN_CHANGE_SET));

    assertThat(adopted.kind()).isEqualTo(Kind.BASE);
    assertThat(adopted.base()).isEqualTo(record);
    assertThat(adopted.gap()).isEmpty();
    assertThat(notAdopted.kind()).isEqualTo(Kind.BASE);
    assertThat(gapIds(notAdopted)).containsExactly(parentOf(parentOf(last)));
  }

  @Test
  public void foreignSnapshotBetweenOwnSlicesIsInGap() {
    long parent = connectorCommit();
    writeStatistics(parent);
    slice(OWN_CHANGE_SET, "commit-a");
    long foreign = foreignAppend();
    slice(OWN_CHANGE_SET, "commit-a");
    long last = finalSlice("commit-a");

    NdvChain chain = resolve(last, ImmutableSet.of(last), ImmutableSet.of(OWN_CHANGE_SET));

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base().snapshotId()).isEqualTo(parent);
    assertThat(gapIds(chain)).containsExactly(foreign);
  }

  @Test
  public void ownSnapshotsOfMergedJobsAreSkippedWhereverTheyAre() {
    long parent = connectorCommit();
    writeStatistics(parent);
    long first = connectorCommit();
    long foreign = foreignAppend();
    long second = connectorCommit();
    StatisticsAnswer firstAnswer = answer(first);
    StatisticsAnswer secondAnswer = answer(second);

    NdvChain chain =
        resolve(
            second, ImmutableSet.of(first, second), ImmutableSet.of(), firstAnswer, secondAnswer);

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base().snapshotId()).isEqualTo(parent);
    assertThat(gapIds(chain)).containsExactly(foreign);
    assertThat(chain.answers()).containsExactly(firstAnswer, secondAnswer);
  }

  @Test
  public void ownSnapshotsOffChainAreDroppedWithTheirAnswers() {
    long parent = connectorCommit();
    StatisticsFile record = writeStatistics(parent);
    long rolledBack = connectorCommit();
    table.manageSnapshots().rollbackTo(parent).commit();
    long snapshot = connectorCommit();
    StatisticsAnswer rolledBackAnswer = answer(rolledBack);
    StatisticsAnswer answer = answer(snapshot);
    StatisticsAnswer notOwn = answer(parent);

    NdvChain chain =
        resolve(
            snapshot,
            ImmutableSet.of(rolledBack, snapshot),
            ImmutableSet.of(),
            rolledBackAnswer,
            answer,
            notOwn);

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
    assertThat(chain.gap()).isEmpty();
    assertThat(chain.answers()).containsExactly(answer);
  }

  @Test
  public void firstCommitIntoEmptyTableStartsFromScratch() {
    long snapshot = connectorCommit();
    List<String> logs = Lists.newArrayList();

    NdvChain chain =
        resolve(NdvTestFiles.recordingLogger(logs), snapshot, ImmutableSet.of(snapshot));

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(chain.base()).isNull();
    assertThat(chain.gap()).isEmpty();
    assertThat(logs).as("no recovery of a failed start").isEmpty();
  }

  @Test
  public void commitAfterTruncateStartsFromScratch() {
    foreignAppend();
    truncate();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(chain.gap()).isEmpty();
  }

  @Test
  public void emptySnapshotWithStatisticsIsBase() {
    foreignAppend();
    long truncated = truncate();
    StatisticsFile record = writeStatistics(truncated);
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
  }

  @Test
  public void nonEmptyTableWithoutStatisticsHasNoBaseline() {
    long foreign = foreignAppend();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    writeStatistics(foreign); // the first external ANALYZE
    NdvChain analyzed = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(chain.base()).isNull();
    assertThat(analyzed.kind()).isEqualTo(Kind.BASE);
    assertThat(analyzed.base().snapshotId()).isEqualTo(foreign);
  }

  @Test
  public void emptySnapshotInsideUnmarkedGapIsNotBase() {
    foreignAppend();
    truncate();
    long foreign = foreignAppend();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    NdvChain withoutGap = resolve(snapshot, ImmutableSet.of(foreign, snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(withoutGap.kind())
        .as("the empty snapshot right behind O")
        .isEqualTo(Kind.EMPTY_BASE);
  }

  @Test
  public void lagStepPutsSIntoGap() {
    long parent = connectorCommit();
    StatisticsFile record = writeStatistics(parent);
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.BASE);
    assertThat(chain.base()).isEqualTo(record);
    assertThat(gapIds(chain)).containsExactly(snapshot);
  }

  @Test
  public void lagStepOnEmptyHeadStartsFromScratch() {
    foreignAppend();
    long truncated = truncate();

    NdvChain chain = resolve(truncated, ImmutableSet.of(), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(chain.gap()).isEmpty();
  }

  @Test
  public void failedFirstStepIsRecoveredFromEmptyPoint() {
    long first = connectorCommit(); // its step never wrote statistics
    long snapshot = connectorCommit();
    List<String> logs = Lists.newArrayList();
    List<String> lagLogs = Lists.newArrayList();

    NdvChain chain =
        resolve(NdvTestFiles.recordingLogger(logs), snapshot, ImmutableSet.of(snapshot));
    NdvChain lag = resolve(NdvTestFiles.recordingLogger(lagLogs), first, ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(chain)).containsExactly(first);
    assertThat(logs)
        .singleElement()
        .asString()
        .startsWith("INFO ")
        .contains(TABLE.toString(), String.valueOf(snapshot), "1 snapshots", "1 data files");
    assertThat(lag.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(lag)).containsExactly(first);
    assertThat(lagLogs).singleElement().asString().startsWith("INFO ");
  }

  @Test
  public void failedFirstStepAfterForeignTruncateIsRecovered() {
    foreignAppend();
    truncate();
    long first = connectorCommit();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(chain)).as("the truncate is not in the gap").containsExactly(first);
  }

  @Test
  public void markedGapWithForeignReplaceStartsFromScratch() {
    long first = connectorCommit();
    compact();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(chain)).containsExactly(first);
  }

  @Test
  public void markedGapWithForeignAppendHasNoBaseline() {
    long first = connectorCommit();
    long foreign = foreignAppend();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    NdvChain appendIsOwn = resolve(snapshot, ImmutableSet.of(foreign, snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(appendIsOwn.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(appendIsOwn)).containsExactly(first);
  }

  @Test
  public void unmarkedRootOfGapHasNoBaseline() {
    long root = foreignAppend(); // written before statistics were enabled
    long marked = connectorCommit();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    NdvChain rootIsOwn = resolve(snapshot, ImmutableSet.of(root, snapshot), ImmutableSet.of());

    assertThat(chain.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(rootIsOwn.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(rootIsOwn)).containsExactly(marked);
  }

  @Test
  public void expiredParentHasNoBaseline() {
    long expired = connectorCommit();
    writeStatistics(expired);
    long first = connectorCommit();
    long snapshot = connectorCommit();
    NdvChain beforeExpire = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    table.expireSnapshots().expireSnapshotId(expired).cleanExpiredFiles(false).commit();

    NdvChain markedGap = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    NdvChain emptyGap = resolve(first, ImmutableSet.of(first), ImmutableSet.of());

    assertThat(beforeExpire.kind()).isEqualTo(Kind.BASE);
    assertThat(gapIds(beforeExpire)).containsExactly(first);
    assertThat(markedGap.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(emptyGap.kind()).isEqualTo(Kind.NO_BASELINE);
  }

  @Test
  public void gapIsNotReadWhileResolving() {
    long first = connectorCommit();
    long second = connectorCommit();
    long third = connectorCommit();
    long snapshot = connectorCommit();
    Table noReads = spy(table);
    doReturn(new ReadFileIO().refuseAll()).when(noReads).io();

    NdvChain chain =
        NdvChain.resolve(
            noReads, snapshot, ImmutableSet.of(snapshot), ImmutableSet.of(), ImmutableList.of());

    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(chain)).containsExactly(third, second, first);
  }

  @Test
  public void markedGapToEmptyPointOverLimitIsNotListedPastExcess() {
    long first = connectorCommit(); // the failed start: its step never wrote statistics
    long second = connectorCommit();
    long third = connectorCommit();
    long snapshot = connectorCommit();

    NdvChain chain = resolve(snapshot, ImmutableSet.of(snapshot), ImmutableSet.of());
    assertThat(chain.kind()).isEqualTo(Kind.EMPTY_BASE);
    assertThat(gapIds(chain)).containsExactly(third, second, first);

    List<Snapshot> gap = chain.gap().stream().map(Mockito::spy).collect(Collectors.toList());
    IntermediateSketchSet coverage = mock(IntermediateSketchSet.class);
    NdvReadPlan plan =
        NdvReadPlan.build(
            table, gap, chain.answers(), coverage, SCHEMA.columns(), 1L, Long.MAX_VALUE);

    assertThat(plan.kind()).isEqualTo(NdvReadPlan.Kind.OVER_LIMIT);
    assertThat(plan.gapFiles()).as("counted up to the first file past the limit").isEqualTo(2L);
    assertThat(plan.files()).isEmpty();
    verify(gap.get(2), never()).dataManifests(any());
    verifyNoInteractions(coverage);
  }

  private NdvChain resolve(
      long snapshotId,
      Set<Long> ownSnapshotIds,
      Set<String> ownChangeSetIds,
      StatisticsAnswer... answers) {
    return NdvChain.resolve(
        table, snapshotId, ownSnapshotIds, ownChangeSetIds, ImmutableList.copyOf(answers));
  }

  private NdvChain resolve(Logger logger, long snapshotId, Set<Long> ownSnapshotIds) {
    return NdvChain.resolve(
        logger, table, snapshotId, ownSnapshotIds, ImmutableSet.of(), ImmutableList.of());
  }

  private static List<Long> gapIds(NdvChain chain) {
    return chain.gap().stream().map(Snapshot::snapshotId).collect(Collectors.toList());
  }

  private static StatisticsAnswer answer(long snapshotId) {
    return StatisticsAnswer.ofDataFiles(snapshotId, ImmutableList.of(), null);
  }

  private long parentOf(long snapshotId) {
    return table.snapshot(snapshotId).parentId();
  }

  private long foreignAppend() {
    return append(ImmutableMap.of());
  }

  private long connectorCommit() {
    return append(ImmutableMap.of(MARKER, "true", COMMIT_ID, "commit-" + fileCount));
  }

  private long append(Map<String, String> properties) {
    AppendFiles append = table.newAppend().appendFile(dataFile());
    properties.forEach(append::set);
    append.commit();
    return table.currentSnapshot().snapshotId();
  }

  private void slice(String changeSetId, String commitId) {
    overwrite(ImmutableMap.of(MARKER, "true", COMMIT_ID, commitId, CHANGE_SET, changeSetId));
  }

  private long finalSlice(String commitId) {
    return overwrite(ImmutableMap.of(MARKER, "true", COMMIT_ID, commitId));
  }

  private long overwrite(Map<String, String> properties) {
    OverwriteFiles overwrite = table.newOverwrite().addFile(dataFile());
    properties.forEach(overwrite::set);
    overwrite.commit();
    return table.currentSnapshot().snapshotId();
  }

  private long truncate() {
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    assertThat(table.currentSnapshot().summary()).containsEntry("total-data-files", "0");
    return table.currentSnapshot().snapshotId();
  }

  /** An external DELETE of the first data file: core writes no added count for it. */
  private long deleteFirstFile() {
    table.newDelete().deleteFile("mem://data/file-1.parquet").commit();
    assertThat(table.currentSnapshot().operation()).isEqualTo("delete");
    return table.currentSnapshot().snapshotId();
  }

  private long compact() {
    Set<DataFile> current = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> current.add(task.file()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    table.newRewrite().rewriteFiles(current, ImmutableSet.of(dataFile())).commit();
    assertThat(table.currentSnapshot().operation()).isEqualTo("replace");
    return table.currentSnapshot().snapshotId();
  }

  private StatisticsFile writeStatistics(long snapshotId) {
    StatisticsFile file =
        new GenericStatisticsFile(
            snapshotId,
            "mem://stats/stats-" + snapshotId + ".puffin",
            100L,
            50L,
            ImmutableList.of());
    table.updateStatistics().setStatistics(file).commit();
    return file;
  }

  /** A record of the snapshot with a theta blob per column; blob i comes from snapshot i. */
  private StatisticsFile writeStatistics(long snapshotId, long... blobSources) {
    List<BlobMetadata> blobs = Lists.newArrayList();
    for (int i = 0; i < blobSources.length; i += 1) {
      long source = blobSources[i];
      blobs.add(
          new GenericBlobMetadata(
              StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
              source,
              table.snapshot(snapshotId).sequenceNumber(),
              ImmutableList.of(i + 1),
              ImmutableMap.of()));
    }

    StatisticsFile file =
        new GenericStatisticsFile(
            snapshotId, "mem://stats/stats-" + snapshotId + ".puffin", 100L, 50L, blobs);
    table.updateStatistics().setStatistics(file).commit();
    return file;
  }

  private DataFile dataFile() {
    fileCount += 1;
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("mem://data/file-" + fileCount + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(10L)
        .build();
  }
}
