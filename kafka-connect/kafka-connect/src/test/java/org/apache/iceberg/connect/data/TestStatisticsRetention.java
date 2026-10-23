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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.StatisticsRetention.Removal;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** N last statistics records of the branch chain, and the files no record refers to any more. */
public class TestStatisticsRetention {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

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
  public void keepsLastNRecordsAndRecordOfSIsFirst() {
    List<Long> chain = commits(6);
    recordAll(chain.subList(0, 5)); // S = the sixth snapshot, its record is added by the step
    long s6 = chain.get(5);

    Removal removal = StatisticsRetention.plan(table, s6, 3);

    // S counts first with the record the step adds, then the fifth and the fourth snapshot
    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    assertThat(removal.isEmpty()).isFalse();
  }

  @Test
  public void recordOfSItselfDoesNotChangeWindow() {
    List<Long> chain = commits(6);
    recordAll(chain);

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 3);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
  }

  @Test
  public void sIsNeverRemoved() {
    List<Long> chain = commits(4);
    recordAll(chain);

    Removal removal = StatisticsRetention.plan(table, chain.get(3), 1);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    assertThat(removal.statisticsSnapshotIds()).doesNotContain(chain.get(3));
    assertThat(removal.partitionStatisticsSnapshotIds()).doesNotContain(chain.get(3));
  }

  @Test
  public void snapshotsWithoutRecordAreNotCounted() {
    List<Long> chain = commits(6);
    // records only on the first, second and fourth snapshot: a foreign append without ANALYZE,
    // merged jobs and intermediate slices leave the other snapshots bare
    record(chain.get(0));
    record(chain.get(1));
    record(chain.get(3));

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 2);

    // S and the fourth snapshot are the two records of the window, the fifth has none
    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(1), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(1), chain.get(0));
  }

  @Test
  public void recordsOfAnyAuthorAreCountedAndRemoved() {
    List<Long> chain = commits(4);
    ndvRecordAt(chain.get(0), "s3://bucket/trino/analyze-0.stats");
    ndvRecordAt(chain.get(1), "s3://bucket/spark/compute-1.stats");
    ndvRecordAt(chain.get(2), ndvPath(chain.get(2)));

    Removal removal = StatisticsRetention.plan(table, chain.get(3), 2);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(1), chain.get(0));
  }

  @Test
  public void minusOneRemovesNothing() {
    List<Long> chain = commits(8);
    recordAll(chain);

    Removal removal = StatisticsRetention.plan(table, chain.get(7), -1);

    assertThat(removal.isEmpty()).isTrue();
    assertThat(removal.statisticsSnapshotIds()).isEmpty();
    assertThat(removal.partitionStatisticsSnapshotIds()).isEmpty();
  }

  @Test
  public void nothingIsRemovedWhileRecordsFitWindow() {
    List<Long> chain = commits(4);
    recordAll(chain.subList(0, 3)); // with S: exactly N records

    assertThat(StatisticsRetention.plan(table, chain.get(3), 4).isEmpty()).isTrue();
    assertThat(StatisticsRetention.plan(table, chain.get(3), 10).isEmpty()).isTrue();
  }

  @Test
  public void oneRecordOverWindowIsRemoved() {
    List<Long> chain = commits(4);
    recordAll(chain.subList(0, 3));

    Removal removal = StatisticsRetention.plan(table, chain.get(3), 3);

    assertThat(removal.statisticsSnapshotIds()).containsExactly(chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds()).containsExactly(chain.get(0));
  }

  @Test
  public void tagInsideWindowIsNotCountedAndNotRemoved() {
    List<Long> chain = commits(6);
    recordAll(chain.subList(0, 5));
    table.manageSnapshots().createTag("audit", chain.get(4)).commit();

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 3);

    // S, the fourth and the third snapshot are the window; the tagged fifth is one more record
    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(1), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(1), chain.get(0));
  }

  @Test
  public void tagOnSDoesNotChangeWindow() {
    List<Long> chain = commits(6);
    recordAll(chain.subList(0, 5));
    table.manageSnapshots().createTag("audit", chain.get(5)).commit();

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 3);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
  }

  @Test
  public void tagBeyondWindowKeepsItsRecord() {
    List<Long> chain = commits(6);
    recordAll(chain);
    table.manageSnapshots().createTag("release", chain.get(1)).commit();

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 3);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(0));
  }

  @Test
  public void headOfAnotherBranchOnChainIsKept() {
    List<Long> chain = commits(6);
    recordAll(chain);
    table.manageSnapshots().createBranch("dev", chain.get(1)).commit();

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 2);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(3), chain.get(2), chain.get(0));
  }

  @Test
  public void headOfBranchRolledBackToAncestorIsNotCountedAndNotRemoved() {
    List<Long> chain = commits(6);
    recordAll(chain);
    // the branch of the commit is rolled back to the third snapshot; S is the sixth one
    table.manageSnapshots().rollbackTo(chain.get(2)).commit();

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 2);

    // S and the fifth snapshot are the window, the head (third) is skipped
    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(3), chain.get(1), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(3), chain.get(1), chain.get(0));
  }

  @Test
  public void recordsOutsideChainAreNotTouched() {
    List<Long> chain = commits(4);
    table.manageSnapshots().createBranch("other", chain.get(1)).commit();
    long other1 = commitOn("other");
    long other2 = commitOn("other");
    recordAll(chain);
    recordAll(ImmutableList.of(other1, other2));

    Removal removal = StatisticsRetention.plan(table, chain.get(3), 1);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    assertThat(removal.statisticsSnapshotIds()).doesNotContain(other1, other2);
    assertThat(removal.partitionStatisticsSnapshotIds()).doesNotContain(other1, other2);
  }

  @Test
  public void everyKindHasItsOwnWindow() {
    List<Long> chain = commits(6);
    // NDV was not written on the last two snapshots, the partition statistics were
    ndvOnly(chain.subList(0, 4));
    partitionsOnly(chain.subList(0, 5));

    Removal removal = StatisticsRetention.plan(table, chain.get(5), 2);

    // NDV: S counts first, the fourth snapshot is the second record
    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    // partitions: S and the fifth snapshot
    assertThat(removal.partitionStatisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(3), chain.get(2), chain.get(1), chain.get(0));
  }

  @Test
  public void kindWithoutRecordsHasNothingToRemove() {
    List<Long> chain = commits(5);
    ndvOnly(chain.subList(0, 4));

    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(2), chain.get(1), chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds()).isEmpty();
  }

  @Test
  public void limitedToKeepsOnlyKindsStepWrites() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2);

    Removal ndv = removal.limitedTo(true, false);
    assertThat(ndv.statisticsSnapshotIds()).isEqualTo(removal.statisticsSnapshotIds());
    assertThat(ndv.partitionStatisticsSnapshotIds()).isEmpty();

    Removal partitions = removal.limitedTo(false, true);
    assertThat(partitions.statisticsSnapshotIds()).isEmpty();
    assertThat(partitions.partitionStatisticsSnapshotIds())
        .isEqualTo(removal.partitionStatisticsSnapshotIds());

    assertThat(removal.limitedTo(false, false).isEmpty()).isTrue();
    assertThat(removal.limitedTo(true, true).statisticsSnapshotIds())
        .isEqualTo(removal.statisticsSnapshotIds());
    assertThat(removal.limitedTo(true, true).partitionStatisticsSnapshotIds())
        .isEqualTo(removal.partitionStatisticsSnapshotIds());
  }

  @Test
  public void snapshotThatIsNotInTableRemovesNothing() {
    List<Long> chain = commits(4);
    recordAll(chain);

    assertThat(StatisticsRetention.plan(table, 123456789L, 1).isEmpty()).isTrue();
  }

  @Test
  public void onlyMinusOneOrPositiveValuesAreAccepted() {
    List<Long> chain = commits(2);

    assertThatThrownBy(() -> StatisticsRetention.plan(table, chain.get(1), 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("retain");
    assertThatThrownBy(() -> StatisticsRetention.plan(table, chain.get(1), -2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("retain");
  }

  @Test
  public void planChangesNothingInTable() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    String before = committed().metadataFileLocation();

    StatisticsRetention.plan(table, chain.get(4), 2);

    assertThat(committed().metadataFileLocation()).isEqualTo(before);
    assertThat(table.statisticsFiles()).hasSize(4);
    assertThat(table.partitionStatisticsFiles()).hasSize(4);
  }

  @Test
  public void applyRemovesRecordsInTransactionOfStep() {
    List<Long> chain = commits(6);
    recordAll(chain.subList(0, 5));
    Removal removal = StatisticsRetention.plan(table, chain.get(5), 3);

    Transaction transaction = table.newTransaction();
    transaction.updateStatistics().setStatistics(ndvRecord(chain.get(5))).commit();
    transaction
        .updatePartitionStatistics()
        .setPartitionStatistics(partitionRecord(chain.get(5)))
        .commit();
    StatisticsRetention.apply(transaction, removal);

    // nothing is committed before the transaction
    assertThat(table.statisticsFiles()).hasSize(5);
    assertThat(table.partitionStatisticsFiles()).hasSize(5);

    transaction.commitTransaction();

    assertThat(table.statisticsFiles())
        .extracting(file -> file.snapshotId())
        .containsExactlyInAnyOrder(chain.get(3), chain.get(4), chain.get(5));
    assertThat(table.partitionStatisticsFiles())
        .extracting(file -> file.snapshotId())
        .containsExactlyInAnyOrder(chain.get(3), chain.get(4), chain.get(5));
  }

  @Test
  public void applyOfLimitedRemovalLeavesOtherKind() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2).limitedTo(false, true);

    Transaction transaction = table.newTransaction();
    StatisticsRetention.apply(transaction, removal);
    transaction.commitTransaction();

    assertThat(table.statisticsFiles()).hasSize(4);
    assertThat(table.partitionStatisticsFiles())
        .extracting(file -> file.snapshotId())
        .containsExactly(chain.get(3));
  }

  @Test
  public void applyOfEmptyRemovalChangesNothing() {
    List<Long> chain = commits(3);
    recordAll(chain);
    String before = committed().metadataFileLocation();

    Transaction transaction = table.newTransaction();
    StatisticsRetention.apply(transaction, StatisticsRetention.plan(table, chain.get(2), -1));
    transaction.commitTransaction();

    assertThat(committed().metadataFileLocation()).isEqualTo(before);
  }

  @Test
  public void applySurvivesConcurrentCommit() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2);

    Transaction transaction = table.newTransaction();
    StatisticsRetention.apply(transaction, removal);
    // another writer commits data before the transaction of the step
    Table other = catalog.loadTable(TABLE);
    other.newAppend().appendFile(dataFile()).commit();
    transaction.commitTransaction();

    assertThat(table.statisticsFiles())
        .extracting(file -> file.snapshotId())
        .containsExactly(chain.get(3));
    assertThat(table.partitionStatisticsFiles())
        .extracting(file -> file.snapshotId())
        .containsExactly(chain.get(3));
  }

  @Test
  public void filesOfRemovedRecordsAreDeletedWhenNothingReferencesThem() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2);
    commitRemoval(removal);

    assertThat(StatisticsRetention.filesToDelete(committed(), removal))
        .containsExactlyInAnyOrder(
            ndvPath(chain.get(2)),
            ndvPath(chain.get(1)),
            ndvPath(chain.get(0)),
            partitionPath(chain.get(2)),
            partitionPath(chain.get(1)),
            partitionPath(chain.get(0)));
  }

  @Test
  public void pathOfRemovedNdvRecordThatAnotherNdvRecordUsesIsKept() {
    List<Long> chain = commits(5);
    String shared = "mem://metadata/shared-ndv.puffin";
    ndvRecordAt(chain.get(0), shared);
    ndvRecordAt(chain.get(1), ndvPath(chain.get(1)));
    ndvRecordAt(chain.get(2), shared); // stays in the window with the same path
    ndvRecordAt(chain.get(3), ndvPath(chain.get(3)));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 3);
    commitRemoval(removal);

    assertThat(removal.statisticsSnapshotIds())
        .containsExactlyInAnyOrder(chain.get(1), chain.get(0));
    assertThat(StatisticsRetention.filesToDelete(committed(), removal))
        .containsExactly(ndvPath(chain.get(1)));
  }

  @Test
  public void pathOfRemovedNdvRecordThatPartitionRecordUsesIsKept() {
    List<Long> chain = commits(4);
    String shared = "mem://metadata/shared.puffin";
    ndvRecordAt(chain.get(0), shared);
    ndvRecordAt(chain.get(1), ndvPath(chain.get(1)));
    partitionRecordAt(chain.get(2), shared); // stays in the window
    Removal removal = StatisticsRetention.plan(table, chain.get(3), 2);
    commitRemoval(removal);

    assertThat(removal.statisticsSnapshotIds()).containsExactly(chain.get(0));
    assertThat(removal.partitionStatisticsSnapshotIds()).isEmpty();
    assertThat(committed().partitionStatisticsFiles())
        .extracting(file -> file.path())
        .containsExactly(shared);
    assertThat(StatisticsRetention.filesToDelete(committed(), removal)).isEmpty();
  }

  @Test
  public void pathOfRemovedPartitionRecordThatNdvRecordUsesIsKept() {
    List<Long> chain = commits(4);
    String shared = "mem://metadata/shared.stats";
    partitionRecordAt(chain.get(0), shared);
    partitionRecordAt(chain.get(1), partitionPath(chain.get(1)));
    ndvRecordAt(chain.get(2), shared); // stays in the window
    Removal removal = StatisticsRetention.plan(table, chain.get(3), 2);
    commitRemoval(removal);

    assertThat(removal.partitionStatisticsSnapshotIds()).containsExactly(chain.get(0));
    assertThat(removal.statisticsSnapshotIds()).isEmpty();
    assertThat(committed().statisticsFiles())
        .extracting(file -> file.path())
        .containsExactly(shared);
    assertThat(StatisticsRetention.filesToDelete(committed(), removal)).isEmpty();
  }

  @Test
  public void pathTwoRemovedRecordsShareIsReturnedOnce() {
    List<Long> chain = commits(5);
    String shared = "mem://metadata/shared.puffin";
    ndvRecordAt(chain.get(0), shared);
    ndvRecordAt(chain.get(1), shared);
    partitionRecordAt(chain.get(0), shared);
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 1);
    commitRemoval(removal);

    assertThat(StatisticsRetention.filesToDelete(committed(), removal)).containsExactly(shared);
  }

  @Test
  public void nothingIsDeletedWhileRecordsAreStillCommitted() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2);
    TableMetadata beforeTheCommit = committed();
    commitRemoval(removal);

    // the outcome of the commit is unknown and the refresh shows the transaction not applied
    assertThat(StatisticsRetention.filesToDelete(beforeTheCommit, removal)).isEmpty();
    assertThat(StatisticsRetention.filesToDelete(committed(), removal)).isNotEmpty();
  }

  @Test
  public void recordThatAnotherWriterAddedForSamePathKeepsFile() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2);
    commitRemoval(removal);
    // after the commit another writer registers the file of a removed record for a new snapshot
    long later = commits(1).get(0);
    ndvRecordAt(later, ndvPath(chain.get(0)));

    assertThat(StatisticsRetention.filesToDelete(committed(), removal))
        .doesNotContain(ndvPath(chain.get(0)))
        .contains(ndvPath(chain.get(1)), ndvPath(chain.get(2)));
  }

  @Test
  public void filesOfLimitedRemovalAreOfItsKindsOnly() {
    List<Long> chain = commits(5);
    recordAll(chain.subList(0, 4));
    Removal removal = StatisticsRetention.plan(table, chain.get(4), 2).limitedTo(true, false);
    commitRemoval(removal);

    assertThat(StatisticsRetention.filesToDelete(committed(), removal))
        .containsExactlyInAnyOrder(
            ndvPath(chain.get(2)), ndvPath(chain.get(1)), ndvPath(chain.get(0)));
  }

  @Test
  public void emptyRemovalHasNoFilesToDelete() {
    List<Long> chain = commits(3);
    recordAll(chain);

    Removal removal = StatisticsRetention.plan(table, chain.get(2), -1);

    assertThat(StatisticsRetention.filesToDelete(committed(), removal)).isEmpty();
  }

  private void commitRemoval(Removal removal) {
    Transaction transaction = table.newTransaction();
    StatisticsRetention.apply(transaction, removal);
    transaction.commitTransaction();
  }

  private TableMetadata committed() {
    table.refresh();
    return ((HasTableOperations) table).operations().current();
  }

  private List<Long> commits(int count) {
    List<Long> ids = Lists.newArrayList();
    for (int index = 0; index < count; index += 1) {
      table.newAppend().appendFile(dataFile()).commit();
      ids.add(table.currentSnapshot().snapshotId());
    }
    return ids;
  }

  private long commitOn(String branch) {
    table.newAppend().appendFile(dataFile()).toBranch(branch).commit();
    return table.snapshot(branch).snapshotId();
  }

  private void recordAll(List<Long> snapshots) {
    snapshots.forEach(this::record);
  }

  private void record(long snapshotId) {
    ndvRecordAt(snapshotId, ndvPath(snapshotId));
    partitionRecordAt(snapshotId, partitionPath(snapshotId));
  }

  private void ndvOnly(List<Long> snapshots) {
    snapshots.forEach(id -> ndvRecordAt(id, ndvPath(id)));
  }

  private void partitionsOnly(List<Long> snapshots) {
    snapshots.forEach(id -> partitionRecordAt(id, partitionPath(id)));
  }

  private void ndvRecordAt(long snapshotId, String path) {
    table
        .updateStatistics()
        .setStatistics(new GenericStatisticsFile(snapshotId, path, 100L, 50L, ImmutableList.of()))
        .commit();
  }

  private void partitionRecordAt(long snapshotId, String path) {
    table
        .updatePartitionStatistics()
        .setPartitionStatistics(
            ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(snapshotId)
                .path(path)
                .fileSizeInBytes(100L)
                .build())
        .commit();
  }

  private static GenericStatisticsFile ndvRecord(long snapshotId) {
    return new GenericStatisticsFile(
        snapshotId, ndvPath(snapshotId), 100L, 50L, ImmutableList.of());
  }

  private static ImmutableGenericPartitionStatisticsFile partitionRecord(long snapshotId) {
    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .path(partitionPath(snapshotId))
        .fileSizeInBytes(100L)
        .build();
  }

  private static String ndvPath(long snapshotId) {
    return "mem://metadata/stats-" + snapshotId + "-commit.puffin";
  }

  private static String partitionPath(long snapshotId) {
    return "mem://metadata/partition-stats-" + snapshotId + ".parquet";
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
