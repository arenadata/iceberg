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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The statistics lag of the standalone merge-on-read committer, driven by hand as the coordinator
 * drives it: {@code pendingTables()} once per cycle, then {@code commit()} of every table it lists
 * with a request without responses. The steps go to a pool the test runs by hand. The records of
 * the heads are registered without files: the quick check reads metadata only.
 */
public class TestMergeOnReadStatisticsLag extends ChannelTestBase {
  private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);
  private static final long RECHECK_MS = TimeUnit.SECONDS.toMillis(1);
  private static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";
  private static final PartitionSpec BY_DATE =
      PartitionSpec.builderFor(SCHEMA).identity("date").build();
  private static final Flags MISSING_BASELINE = new Flags(false, true, false);

  private final ManualPool pool = new ManualPool();
  private final StatisticsLag lag = new StatisticsLag();
  private final ScriptedStep step = new ScriptedStep();
  private final AtomicLong nanos = new AtomicLong();
  private StatisticsHealth health = new StatisticsHealth(HOUR_MS, nanos::get);
  private CountingCatalog countingCatalog;

  @BeforeEach
  public void countLoads() {
    countingCatalog = new CountingCatalog(catalog);
  }

  @Test
  public void staticTablesWithStatisticsAreInLagAtStartupNotVerified() {
    TableIdentifier withStatistics = TableIdentifier.of(NAMESPACE, "tbl2");
    TableIdentifier withoutStatistics = TableIdentifier.of(NAMESPACE, "tbl3");
    enableStatistics(TABLE_IDENTIFIER, withStatistics);
    when(config.configuredTables())
        .thenReturn(ImmutableSet.of(TABLE_IDENTIFIER, withStatistics, withoutStatistics));

    MergeOnReadTableCommitter committer = committer();

    assertThat(lag.tables()).containsExactlyInAnyOrder(TABLE_IDENTIFIER, withStatistics);
    assertThat(lag.isVerified(TABLE_IDENTIFIER)).isFalse();
    assertThat(lag.isVerified(withStatistics)).isFalse();
    assertThat(committer.pendingTables())
        .as("under references without a UUID")
        .containsExactlyInAnyOrder(reference(TABLE_IDENTIFIER), reference(withStatistics));
  }

  @Test
  public void withStatisticsOffThereIsNoLagAndNoTableToVisit() {
    health = StatisticsHealth.NOOP;
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));

    MergeOnReadTableCommitter committer = committer();

    assertThat(lag.tables()).isEmpty();
    assertThat(committer.pendingTables()).isEmpty();
    assertThat(committer.pendingTables()).isEmpty();
  }

  @Test
  public void restartWithRecordsOfEveryKindOnHeadsLeavesNoLagAfterFirstCycle() {
    enableStatistics();
    Set<TableIdentifier> tables = Sets.newLinkedHashSet();
    for (int index = 0; index < 20; index++) {
      TableIdentifier id = partitionedTableWithHead("part" + index);
      withPartitionStatistics(id);
      recordNdvOnHead(id);
      recordPartitionsOnHead(id);
      tables.add(id);
    }

    when(config.configuredTables()).thenReturn(tables);
    MergeOnReadTableCommitter committer = committer();
    assertThat(lag.tables()).hasSize(20);

    assertThat(cycle(committer)).hasSize(20);

    assertThat(lag.tables()).isEmpty();
    assertThat(pool.size()).as("no lag step").isZero();
    assertThat(cycle(committer)).as("nothing to visit in the next cycle").isEmpty();
    assertThat(step.inputs()).isEmpty();
  }

  @Test
  public void restartVerifiesTablesWhoseHeadsLackRecordAndAdmitsThemFromSecondCycle() {
    when(config.commitThreads()).thenReturn(8);
    Set<TableIdentifier> recorded = Sets.newLinkedHashSet();
    Set<TableIdentifier> unrecorded = Sets.newLinkedHashSet();
    for (int index = 0; index < 5; index++) {
      TableIdentifier id = tableWithHead("recorded" + index);
      recordNdvOnHead(id);
      recorded.add(id);
    }

    for (int index = 0; index < 3; index++) {
      unrecorded.add(tableWithHead("unrecorded" + index));
    }

    Set<TableIdentifier> all = Sets.union(recorded, unrecorded);
    enableStatistics(all.toArray(new TableIdentifier[0]));
    when(config.configuredTables()).thenReturn(all);
    MergeOnReadTableCommitter committer = committer();

    cycle(committer);
    assertThat(lag.tables()).containsExactlyInAnyOrderElementsOf(unrecorded);
    assertThat(unrecorded).allMatch(lag::isVerified);
    assertThat(pool.size()).as("no step in the first cycle: none was admitted").isZero();

    cycle(committer);
    assertThat(pool.size()).as("a lag step for each").isEqualTo(3);
    pool.runAll();
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::identifier)
        .containsExactlyInAnyOrderElementsOf(unrecorded);
    assertThat(step.inputs()).extracting(StatisticsStepInput::snapshotId).containsOnlyNulls();
    assertThat(lag.tables()).isEmpty();
  }

  @Test
  public void tailOfCopyOnWriteHasNoLagOfItsOwnToVisit() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));

    MergeOnReadTableCommitter tail = committer(MergeOnReadTableCommitter.Mode.TAIL);
    assertThat(lag.tables()).as("the lag of copy-on-write gets no startup entry here").isEmpty();
    lag.enter(TABLE_IDENTIFIER);
    long generation = lag.token(TABLE_IDENTIFIER);

    assertThat(tail.pendingTables()).isEmpty();
    // a request without responses goes the way it went without statistics: nothing to commit
    CommitResult result = tail.commit(requestWithoutResponses(reference(TABLE_IDENTIFIER)));
    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(1);
    assertThat(pool.size()).isZero();
    assertThat(lag.token(TABLE_IDENTIFIER)).isEqualTo(generation);
    assertThat(lag.isVerified(TABLE_IDENTIFIER)).isTrue();
  }

  @Test
  public void verifiedTableNotAdmittedInCycleAnswersWithoutBeingLoaded() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier first = tableWithHead("first");
    TableIdentifier second = tableWithHead("second");
    enableStatistics(first, second);
    lag.enter(first);
    lag.enter(second);
    MergeOnReadTableCommitter committer = committer();

    assertThat(committer.pendingTables()).containsExactly(reference(first), reference(second));
    // the coordinator calls commit() in any order: the admission was chosen in pendingTables()
    commitWithoutResponses(committer, reference(second));
    commitWithoutResponses(committer, reference(first));

    assertThat(countingCatalog.loads(second)).isZero();
    assertThat(countingCatalog.loads(first)).isEqualTo(1);
    assertThat(pool.size()).isEqualTo(1);
    pool.runAll();
    assertThat(step.inputs()).extracting(StatisticsStepInput::identifier).containsExactly(first);
    assertThat(lag.tables()).containsExactly(second);
  }

  @Test
  public void lagRemovedBetweenPendingTablesAndCommitAnswersWithoutLoadingTable() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    lag.enter(TABLE_IDENTIFIER);
    MergeOnReadTableCommitter committer = committer();

    Set<TableReference> pending = committer.pendingTables();
    // the step of an earlier cycle ends meanwhile and removes the lag
    lag.removeIfToken(TABLE_IDENTIFIER, lag.token(TABLE_IDENTIFIER));
    pending.forEach(reference -> commitWithoutResponses(committer, reference));

    assertThat(pending).containsExactly(reference(TABLE_IDENTIFIER));
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isZero();
    assertThat(pool.size()).isZero();
  }

  @Test
  public void tableOfLagThatFailsToLoadAnswersCommittedAndStaysInLag() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    lag.enter(TABLE_IDENTIFIER);
    long generation = lag.token(TABLE_IDENTIFIER);
    countingCatalog.failLoading(TABLE_IDENTIFIER, new IllegalStateException("the catalog fails"));
    MergeOnReadTableCommitter committer = committer();

    cycle(committer);
    cycle(committer);

    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(2);
    assertThat(lag.token(TABLE_IDENTIFIER)).as("the lag stays as it was").isEqualTo(generation);
    assertThat(pool.size()).isZero();
    // the failures in a row are counted with those of the step: the next one is not the first,
    // so the WARN is not repeated
    assertThat(lag.recordFailure(TABLE_IDENTIFIER)).isFalse();
  }

  @Test
  public void tableGoneFromCatalogLeavesLagItsHealthAndStagingCleanup() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    MergeOnReadTableCommitter committer = committer();
    health.update(TABLE_IDENTIFIER, flags -> MISSING_BASELINE);

    cycle(committer);
    assertThat(lag.isVerified(TABLE_IDENTIFIER)).as("verified by the quick check").isTrue();
    assertThat(cleanupTables(committer)).contains(TABLE_IDENTIFIER);

    catalog.dropTable(TABLE_IDENTIFIER, false);
    cycle(committer);

    assertThat(lag.contains(TABLE_IDENTIFIER)).isFalse();
    assertThat(health.version(TABLE_IDENTIFIER)).as("no record of health").isZero();
    assertThat(cleanupTables(committer)).doesNotContain(TABLE_IDENTIFIER);
    assertThat(pool.size()).isZero();
  }

  @Test
  public void changeSetOnLastSnapshotOfConnectorKeepsLagWithoutStepOrRefusal() {
    enableStatistics(TABLE_IDENTIFIER);
    connectorAppend(TABLE_IDENTIFIER, "change-set");
    lag.enter(TABLE_IDENTIFIER);
    long generation = lag.token(TABLE_IDENTIFIER);
    MergeOnReadTableCommitter committer = committer();

    assertThat(cycle(committer)).containsExactly(reference(TABLE_IDENTIFIER));
    assertThat(pool.size()).as("no lag step").isZero();
    assertThat(lag.token(TABLE_IDENTIFIER)).isEqualTo(generation);
    assertThat(cycle(committer)).as("still visited").containsExactly(reference(TABLE_IDENTIFIER));
    assertThat(rejectedReasons(committer)).as("no refusal in cycles without responses").isEmpty();

    // a request with responses is still refused, and this is the first refusal
    CommitResult refused = committer.commit(requestWithResponse(TABLE_IDENTIFIER, 20L));
    assertThat(refused.outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    assertThat(refused.consumed()).isEmpty();
    assertThat(rejectedReasons(committer)).containsOnlyKeys(TABLE_IDENTIFIER);
  }

  @Test
  public void tableWithoutHeadLeavesLagWithoutStep() {
    enableStatistics(TABLE_IDENTIFIER);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    MergeOnReadTableCommitter committer = committer();
    assertThat(lag.contains(TABLE_IDENTIFIER)).as("in the lag at startup").isTrue();

    cycle(committer);

    assertThat(lag.contains(TABLE_IDENTIFIER)).isFalse();
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(1);
    assertThat(pool.size()).isZero();
  }

  @Test
  public void admittedTableWhoseHeadIsIntermediateSliceGivesNextCycleToAnother() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier other = tableWithHead("tbl2");
    enableStatistics(TABLE_IDENTIFIER, other);
    // a slice of another writer on top: the change set is on the head, not on this connector's
    // own last snapshot
    catalog
        .loadTable(TABLE_IDENTIFIER)
        .newAppend()
        .appendFile(fakeFile(PartitionSpec.unpartitioned(), null))
        .set(CHANGE_SET_ID_PROP, "change-set")
        .commit();
    lag.enter(TABLE_IDENTIFIER);
    lag.enter(other);
    long generation = lag.token(TABLE_IDENTIFIER);
    MergeOnReadTableCommitter committer = committer();

    cycle(committer);
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).as("admitted").isEqualTo(1);
    assertThat(pool.size()).as("no step on an intermediate slice").isZero();
    assertThat(lag.token(TABLE_IDENTIFIER)).isEqualTo(generation);

    cycle(committer);
    assertThat(pool.size()).isEqualTo(1);
    pool.runAll();
    assertThat(step.inputs()).extracting(StatisticsStepInput::identifier).containsExactly(other);
    assertThat(lag.tables()).containsExactly(TABLE_IDENTIFIER);
  }

  @Test
  public void tableOfLagWithNothingToCommitGetsItsLagStepAndSameAnswer() {
    enableStatistics(TABLE_IDENTIFIER);
    connectorAppend(TABLE_IDENTIFIER, null);
    lag.enter(TABLE_IDENTIFIER);
    MergeOnReadTableCommitter committer = committer();
    assertThat(committer.pendingTables()).containsExactly(reference(TABLE_IDENTIFIER));

    // the response is below the committed offset: nothing to commit
    TableCommitRequest request = requestWithResponse(TABLE_IDENTIFIER, 5L);
    CommitResult result = committer.commit(request);

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(result.consumed()).containsExactlyElementsOf(request.envelopes());
    assertThat(pool.size()).as("the lag step").isEqualTo(1);
    pool.runAll();
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly((Long) null);
    assertThat(lag.tables()).isEmpty();
  }

  @Test
  public void tableOfLagWithNothingToCommitJoinsStagingCleanupAfterRestart() {
    enableStatistics(TABLE_IDENTIFIER);
    connectorAppend(TABLE_IDENTIFIER, null);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    // a fresh committer after a restart: the table is in the lag at startup and has not been
    // committed to, so the cleanup does not know it yet
    MergeOnReadTableCommitter committer = committer();
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(cleanupTables(committer)).isEmpty();

    // the replayed response is below the committed offset: nothing to commit
    committer.commit(requestWithResponse(TABLE_IDENTIFIER, 5L));

    assertThat(cleanupTables(committer))
        .as("the sketches left before the restart are cleaned without waiting for new data")
        .contains(TABLE_IDENTIFIER);
  }

  @Test
  public void tableOutOfLagWithNothingToCommitJoinsStagingCleanupAsWell() {
    enableStatistics(TABLE_IDENTIFIER);
    connectorAppend(TABLE_IDENTIFIER, null);
    MergeOnReadTableCommitter committer = committer();
    assertThat(lag.contains(TABLE_IDENTIFIER)).isFalse();

    CommitResult result = committer.commit(requestWithResponse(TABLE_IDENTIFIER, 5L));

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    // every request with responses that loads the table puts it in the staging cleanup, in the
    // lag or not
    assertThat(cleanupTables(committer)).contains(TABLE_IDENTIFIER);
  }

  @Test
  public void quickCheckRemovesLagOnlyByTokenTakenBeforeTableIsLoaded() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    recordNdvOnHead(TABLE_IDENTIFIER);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    MergeOnReadTableCommitter committer = committer();
    // the table enters the lag again, for a newer snapshot, while commit() loads it
    countingCatalog.beforeLoading(TABLE_IDENTIFIER, () -> lag.enter(TABLE_IDENTIFIER));

    cycle(committer);

    assertThat(lag.contains(TABLE_IDENTIFIER)).as("the newer entry stays").isTrue();
    assertThat(lag.isVerified(TABLE_IDENTIFIER)).isTrue();
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(1);
  }

  @Test
  public void evaluationByHeadIsAppliedOnlyIfNoStepChangedHealthSince() {
    TableIdentifier quiet = tableWithHead("quiet");
    TableIdentifier raced = tableWithHead("raced");
    enableStatistics(quiet, raced);
    recordNdvOnHead(quiet);
    recordNdvOnHead(raced);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(quiet, raced));
    MergeOnReadTableCommitter committer = committer();
    Flags raised = new Flags(false, true, true);
    health.update(quiet, flags -> raised);
    health.update(raced, flags -> raised);
    Flags ofTheStep = new Flags(true, false, false);
    countingCatalog.beforeLoading(raced, () -> health.update(raced, flags -> ofTheStep));

    cycle(committer);

    assertThat(flagsOf(quiet))
        .as("a foreign record of every column puts the flags down")
        .isEqualTo(Flags.NONE);
    assertThat(flagsOf(raced)).as("a step ended since commit() began").isEqualTo(ofTheStep);
    assertThat(lag.tables()).isEmpty();
  }

  @Test
  public void quietFlaggedTableIsReCheckedOnceDueAndLosesFlagAfterForeignAnalyze() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    health.update(TABLE_IDENTIFIER, flags -> MISSING_BASELINE);
    // ANALYZE writes the head after the flag was raised
    recordNdvOnHead(TABLE_IDENTIFIER);

    assertThat(cycle(committer())).as("not due before the interval").isEmpty();
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isZero();
    assertThat(flagsOf(TABLE_IDENTIFIER)).isEqualTo(MISSING_BASELINE);

    health = new StatisticsHealth(RECHECK_MS, nanos::get);
    health.update(TABLE_IDENTIFIER, flags -> MISSING_BASELINE);
    MergeOnReadTableCommitter committer = committer();
    assertThat(cycle(committer)).as("not due yet").isEmpty();
    advance(RECHECK_MS);

    assertThat(cycle(committer)).containsExactly(reference(TABLE_IDENTIFIER));
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(1);
    assertThat(flagsOf(TABLE_IDENTIFIER)).isEqualTo(Flags.NONE);
    assertThat(lag.tables()).isEmpty();
    assertThat(pool.size()).isZero();
  }

  @Test
  public void reCheckThatFailsToLoadKeepsFlagsAndWaitsForNextInterval() {
    enableStatistics(TABLE_IDENTIFIER);
    append(TABLE_IDENTIFIER);
    health = new StatisticsHealth(RECHECK_MS, nanos::get);
    health.update(TABLE_IDENTIFIER, flags -> MISSING_BASELINE);
    advance(RECHECK_MS);
    countingCatalog.failLoading(TABLE_IDENTIFIER, new IllegalStateException("the catalog fails"));
    MergeOnReadTableCommitter committer = committer();

    assertThat(cycle(committer)).containsExactly(reference(TABLE_IDENTIFIER));
    assertThat(cycle(committer)).as("the attempt counts as an evaluation").isEmpty();

    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(1);
    assertThat(flagsOf(TABLE_IDENTIFIER)).isEqualTo(MISSING_BASELINE);

    advance(RECHECK_MS);
    assertThat(cycle(committer))
        .as("the next interval")
        .containsExactly(reference(TABLE_IDENTIFIER));
    assertThat(countingCatalog.loads(TABLE_IDENTIFIER)).isEqualTo(2);
  }

  @Test
  public void reCheckEvaluationIsDroppedIfStepEndedSinceTableWasLoaded() {
    TableIdentifier raced = tableWithHead("raced");
    enableStatistics(raced);
    recordNdvOnHead(raced);
    health = new StatisticsHealth(RECHECK_MS, nanos::get);
    health.update(raced, flags -> MISSING_BASELINE);
    advance(RECHECK_MS);
    Flags ofTheStep = new Flags(true, false, false);
    // the table is not in the lag: the re-check loads it, and a step ends before the evaluation
    countingCatalog.beforeLoading(raced, () -> health.update(raced, flags -> ofTheStep));
    MergeOnReadTableCommitter committer = committer();
    assertThat(lag.contains(raced)).isFalse();

    assertThat(cycle(committer)).containsExactly(reference(raced));

    assertThat(countingCatalog.loads(raced)).isEqualTo(1);
    assertThat(flagsOf(raced)).as("a step ended since commit() began").isEqualTo(ofTheStep);
  }

  @Test
  public void reCheckOfTableThatIsGoneRemovesItsHealth() {
    enableStatistics(TABLE_IDENTIFIER);
    health = new StatisticsHealth(RECHECK_MS, nanos::get);
    health.update(TABLE_IDENTIFIER, flags -> MISSING_BASELINE);
    catalog.dropTable(TABLE_IDENTIFIER, false);
    advance(RECHECK_MS);
    MergeOnReadTableCommitter committer = committer();

    assertThat(cycle(committer)).containsExactly(reference(TABLE_IDENTIFIER));

    assertThat(health.version(TABLE_IDENTIFIER)).as("no record of health").isZero();
  }

  @Test
  public void reChecksOfCycleAreHalfCommitThreadsAndAreListedOnceWithLag() {
    when(config.commitThreads()).thenReturn(4);
    health = new StatisticsHealth(RECHECK_MS, nanos::get);
    List<TableIdentifier> flagged = Lists.newArrayList();
    for (String name : ImmutableList.of("oldest", "older", "newest")) {
      TableIdentifier id = tableWithHead(name);
      health.update(id, flags -> MISSING_BASELINE);
      flagged.add(id);
      advance(1L);
    }
    advance(RECHECK_MS);

    enableStatistics(flagged.toArray(new TableIdentifier[0]));
    lag.enter(flagged.get(1));
    MergeOnReadTableCommitter committer = committer();

    assertThat(committer.pendingTables())
        .as("the two with the oldest evaluation, the table of the lag once")
        .containsExactlyInAnyOrder(reference(flagged.get(0)), reference(flagged.get(1)));
  }

  @Test
  public void lagStepsInFlightAreHalfCommitThreads() {
    when(config.commitThreads()).thenReturn(4);
    List<TableIdentifier> tables = Lists.newArrayList();
    for (int index = 0; index < 5; index++) {
      TableIdentifier id = tableWithHead("tbl" + (index + 10));
      lag.enter(id);
      tables.add(id);
    }

    enableStatistics(tables.toArray(new TableIdentifier[0]));
    MergeOnReadTableCommitter committer = committer();

    cycle(committer);
    assertThat(pool.size()).as("two slots").isEqualTo(2);
    cycle(committer);
    assertThat(pool.size()).as("no free slot while the steps are set").isEqualTo(2);
    assertThat(lag.tables()).hasSize(5);

    pool.runAll();
    assertThat(lag.tables()).containsExactlyElementsOf(tables.subList(2, 5));
    cycle(committer);
    assertThat(pool.size()).isEqualTo(2);
    pool.runAll();
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::identifier)
        .containsExactlyElementsOf(tables.subList(0, 4));
  }

  @Test
  public void withOneSlotEachOfThreeTablesOfLagGetsStepThoughOneAlwaysFails() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier failing = tableWithHead("failing");
    TableIdentifier second = tableWithHead("second");
    TableIdentifier third = tableWithHead("third");
    enableStatistics(failing, second, third);
    lag.enter(failing);
    lag.enter(second);
    lag.enter(third);
    step.behave(
        input ->
            input.identifier().equals(failing)
                ? new Result(Outcome.FAILED, new IllegalStateException("the step fails"))
                : new Result(Outcome.WRITTEN, null));
    MergeOnReadTableCommitter committer = committer();

    for (int cycle = 0; cycle < 4; cycle++) {
      cycle(committer);
      assertThat(pool.size()).as("one lag step in cycle %s", cycle).isEqualTo(1);
      pool.runAll();
    }

    assertThat(step.inputs())
        .extracting(StatisticsStepInput::identifier)
        .containsExactly(failing, second, third, failing);
    assertThat(lag.tables()).containsExactly(failing);
  }

  /**
   * Five tables in the lag: the first cycle admits four, at most two of them are inside {@code
   * StatisticsStep.run} at a time, and when one ends a waiting one starts in the same cycle, with
   * no new {@code pendingTables()}.
   */
  @Test
  public void eightCommitThreadsAdmitFourLagStepsAndRunTwoAtTime() throws Exception {
    when(config.commitThreads()).thenReturn(8);
    List<TableIdentifier> tables = Lists.newArrayList();
    for (int index = 0; index < 5; index++) {
      TableIdentifier id = tableWithHead("permits" + index);
      lag.enter(id);
      tables.add(id);
    }

    enableStatistics(tables.toArray(new TableIdentifier[0]));
    AtomicInteger running = new AtomicInteger();
    AtomicInteger mostRunning = new AtomicInteger();
    Semaphore ends = new Semaphore(0);
    step.behave(
        input -> {
          mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
          try {
            if (!ends.tryAcquire(10, TimeUnit.SECONDS)) {
              throw new IllegalStateException("the test did not end the step");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          } finally {
            running.decrementAndGet();
          }

          return new Result(Outcome.WRITTEN, null);
        });
    ExecutorService threads = Executors.newFixedThreadPool(8);
    try {
      MergeOnReadTableCommitter committer =
          new MergeOnReadTableCommitter(
              countingCatalog,
              config,
              MetadataEvents.NOOP,
              event -> {},
              threads,
              health,
              step,
              lag,
              MergeOnReadTableCommitter.Mode.STANDALONE);

      cycle(committer);
      awaitThat("two steps run", () -> running.get() == 2);
      Thread.sleep(300);
      assertThat(step.inputs()).as("the other two admitted wait for a permit").hasSize(2);

      ends.release();
      awaitThat("a waiting step starts in the same cycle", () -> step.inputs().size() == 3);
      ends.release(3);
      awaitThat("the four admitted are done", () -> lag.tables().size() == 1);

      assertThat(mostRunning.get()).as("steps inside run() at a time").isEqualTo(2);
      assertThat(step.inputs())
          .extracting(StatisticsStepInput::identifier)
          .containsExactlyInAnyOrderElementsOf(tables.subList(0, 4));
      assertThat(lag.tables()).containsExactly(tables.get(4));
      committer.stop();
    } finally {
      threads.shutdownNow();
      assertThat(threads.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  public void lagStepLongerThanCycleLeavesNoAdmissionToNextCycle() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier first = tableWithHead("first");
    TableIdentifier second = tableWithHead("second");
    enableStatistics(first, second);
    lag.enter(first);
    lag.enter(second);
    MergeOnReadTableCommitter committer = committer();

    cycle(committer);
    assertThat(pool.size()).isEqualTo(1);
    cycle(committer);
    assertThat(pool.size()).as("the slot is taken: no admission").isEqualTo(1);
    assertThat(countingCatalog.loads(second)).isZero();

    pool.runAll();
    cycle(committer);
    assertThat(pool.size()).isEqualTo(1);
    pool.runAll();
    assertThat(step.inputs())
        .extracting(StatisticsStepInput::identifier)
        .containsExactly(first, second);
    assertThat(lag.tables()).isEmpty();
  }

  @Test
  public void admissionLeftUnspentIsDroppedByNextPendingTables() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier first = tableWithHead("first");
    TableIdentifier second = tableWithHead("second");
    enableStatistics(first, second);
    lag.enter(first);
    lag.enter(second);
    MergeOnReadTableCommitter committer = committer();

    // the first is admitted in a cycle that does not visit it
    committer.pendingTables();
    committer.pendingTables();
    commitWithoutResponses(committer, reference(first));
    commitWithoutResponses(committer, reference(second));

    assertThat(countingCatalog.loads(first)).as("its admission is gone").isZero();
    assertThat(pool.size()).isEqualTo(1);
    pool.runAll();
    assertThat(step.inputs()).extracting(StatisticsStepInput::identifier).containsExactly(second);
  }

  @Test
  public void stopFreesSlotOfLagStepThatHasNotRun() {
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier first = tableWithHead("first");
    TableIdentifier second = tableWithHead("second");
    enableStatistics(first, second);
    lag.enter(first);
    lag.enter(second);
    MergeOnReadTableCommitter committer = committer();
    cycle(committer);
    assertThat(pool.size()).as("the step of the first waits for the pool").isEqualTo(1);

    committer.stop();
    committer.pendingTables();
    commitWithoutResponses(committer, reference(second));

    assertThat(countingCatalog.loads(second)).as("admitted to the freed slot").isEqualTo(1);
    pool.runAll();
    assertThat(step.inputs()).as("nothing runs after stop()").isEmpty();
  }

  @Test
  public void partitionStatisticsTurnedOnForHeadWithOnlyNdvGetStepThatWritesThemAlone() {
    enableStatistics();
    TableIdentifier id = partitionedTableWithHead("partitioned");
    withPartitionStatistics(id);
    String ndv = recordNdvOnHead(id);
    when(config.configuredTables()).thenReturn(ImmutableSet.of(id));
    step.behave(null);
    MergeOnReadTableCommitter committer = committer();

    cycle(committer);
    assertThat(lag.isVerified(id)).as("the head lacks partition statistics").isTrue();
    cycle(committer);
    pool.runAll();

    Table loaded = catalog.loadTable(id);
    long head = loaded.currentSnapshot().snapshotId();
    assertThat(loaded.partitionStatisticsFiles())
        .extracting(PartitionStatisticsFile::snapshotId)
        .containsExactly(head);
    assertThat(loaded.statisticsFiles()).extracting(StatisticsFile::path).containsExactly(ndv);
    assertThat(lag.tables()).isEmpty();
  }

  private MergeOnReadTableCommitter committer() {
    return committer(MergeOnReadTableCommitter.Mode.STANDALONE);
  }

  private MergeOnReadTableCommitter committer(MergeOnReadTableCommitter.Mode mode) {
    return new MergeOnReadTableCommitter(
        countingCatalog, config, MetadataEvents.NOOP, event -> {}, pool, health, step, lag, mode);
  }

  /**
   * One cycle as the coordinator runs it for tables without responses.
   *
   * @return the references {@code pendingTables()} listed
   */
  private Set<TableReference> cycle(MergeOnReadTableCommitter committer) {
    Set<TableReference> pending = committer.pendingTables();
    pending.forEach(reference -> commitWithoutResponses(committer, reference));
    return pending;
  }

  private void commitWithoutResponses(
      MergeOnReadTableCommitter committer, TableReference reference) {
    CommitResult result = committer.commit(requestWithoutResponses(reference));
    assertThat(result.outcome())
        .as("the answer for %s", reference.identifier())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(result.consumed()).isEmpty();
  }

  private TableCommitRequest requestWithoutResponses(TableReference reference) {
    return new TableCommitRequest(
        reference, ImmutableList.of(), ImmutableMap.of(), UUID.randomUUID(), null);
  }

  private TableCommitRequest requestWithResponse(TableIdentifier id, long offset) {
    DataWritten payload =
        new DataWritten(
            StructType.of(),
            UUID.randomUUID(),
            TableReference.of(catalog.name(), id, catalog.loadTable(id).uuid()),
            ImmutableList.of(fakeFile(PartitionSpec.unpartitioned(), null)),
            ImmutableList.of(),
            ImmutableList.of(SRC_TOPIC_NAME));
    Envelope envelope = new Envelope(new Event(CONNECT_CONSUMER_GROUP_ID, payload), 0, offset);
    return new TableCommitRequest(
        payload.tableReference(),
        ImmutableList.of(envelope),
        ImmutableMap.of(0, offset + 1),
        payload.commitId(),
        null);
  }

  private TableReference reference(TableIdentifier id) {
    return TableReference.of(catalog.name(), id);
  }

  private Flags flagsOf(TableIdentifier id) {
    return health.update(id, flags -> flags);
  }

  /** Moves the clock of the health records forward. */
  private void advance(long millis) {
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
  }

  /** The refusals of merge-on-read commits that {@code reject()} keeps to log them once. */
  @SuppressWarnings("unchecked")
  private static Map<TableIdentifier, String> rejectedReasons(MergeOnReadTableCommitter committer) {
    try {
      Field reasons = MergeOnReadTableCommitter.class.getDeclaredField("rejectedReasons");
      reasons.setAccessible(true);
      return (Map<TableIdentifier, String>) reasons.get(committer);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Set<TableIdentifier> cleanupTables(MergeOnReadTableCommitter committer) {
    return committer.stagingCleanupSource().tables().stream()
        .map(TableReference::identifier)
        .collect(Collectors.toSet());
  }

  private TableIdentifier tableWithHead(String name) {
    TableIdentifier id = TableIdentifier.of(NAMESPACE, name);
    catalog.createTable(id, SCHEMA);
    append(id);
    return id;
  }

  private TableIdentifier partitionedTableWithHead(String name) {
    TableIdentifier id = TableIdentifier.of(NAMESPACE, name);
    catalog
        .createTable(id, SCHEMA, BY_DATE)
        .newAppend()
        .appendFile(fakeFile(BY_DATE, "date=2026-09-30"))
        .commit();
    return id;
  }

  private void withPartitionStatistics(TableIdentifier id) {
    when(config.tableConfig(id.toString()))
        .thenReturn(
            new TableSinkConfig(
                null, ImmutableList.of(), ImmutableList.of(), null, true, true, null));
  }

  private void append(TableIdentifier id) {
    catalog
        .loadTable(id)
        .newAppend()
        .appendFile(fakeFile(PartitionSpec.unpartitioned(), null))
        .commit();
  }

  /** A data commit of this connector with, if given, the pointer of an unfinished change set. */
  private void connectorAppend(TableIdentifier id, String changeSetId) {
    AppendFiles append =
        catalog
            .loadTable(id)
            .newAppend()
            .appendFile(fakeFile(PartitionSpec.unpartitioned(), null))
            .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":10}");
    if (changeSetId != null) {
      append.set(CHANGE_SET_ID_PROP, changeSetId);
    }

    append.commit();
  }

  /**
   * Registers a foreign NDV record of every column on the head, as ANALYZE does, whose file does
   * not exist.
   *
   * @return its path
   */
  private String recordNdvOnHead(TableIdentifier id) {
    Table target = catalog.loadTable(id);
    Snapshot head = target.currentSnapshot();
    List<BlobMetadata> blobs = Lists.newArrayList();
    for (int fieldId = 1; fieldId <= 3; fieldId++) {
      blobs.add(
          new GenericBlobMetadata(
              StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
              head.snapshotId(),
              head.sequenceNumber(),
              ImmutableList.of(fieldId),
              ImmutableMap.of("ndv", "1")));
    }

    String path = "mem://nowhere/stats-" + head.snapshotId() + "-" + UUID.randomUUID() + ".puffin";
    target
        .updateStatistics()
        .setStatistics(new GenericStatisticsFile(head.snapshotId(), path, 100L, 10L, blobs))
        .commit();
    return path;
  }

  private void recordPartitionsOnHead(TableIdentifier id) {
    Table target = catalog.loadTable(id);
    long headId = target.currentSnapshot().snapshotId();
    target
        .updatePartitionStatistics()
        .setPartitionStatistics(
            ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(headId)
                .path("mem://nowhere/partition-stats-" + headId + "-" + UUID.randomUUID())
                .fileSizeInBytes(10L)
                .build())
        .commit();
  }

  private static DataFile fakeFile(PartitionSpec spec, String partitionPath) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("mem://nowhere/data/" + UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(1L);
    if (partitionPath != null) {
      builder.withPartitionPath(partitionPath);
    }

    return builder.build();
  }

  private static void awaitThat(String description, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as(description).isLessThan(deadline);
      Thread.sleep(10);
    }
  }

  /**
   * The step of the tests: records its inputs and answers what {@link #behave} says, {@code
   * WRITTEN} by default; a behavior of {@code null} runs the real step.
   */
  private static final class ScriptedStep extends StatisticsStep {
    private final List<StatisticsStepInput> inputs = new CopyOnWriteArrayList<>();
    private volatile Function<StatisticsStepInput, Result> behavior =
        input -> new Result(Outcome.WRITTEN, null);

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      Function<StatisticsStepInput, Result> scripted = behavior;
      return scripted == null ? super.run(input) : scripted.apply(input);
    }

    void behave(Function<StatisticsStepInput, Result> newBehavior) {
      this.behavior = newBehavior;
    }

    List<StatisticsStepInput> inputs() {
      return inputs;
    }
  }

  /**
   * The catalog of the committer and of its steps: counts the loads of each table, and can run a
   * hook right before a load of a table or fail it.
   */
  private static final class CountingCatalog implements Catalog {
    private final Catalog delegate;
    private final Map<TableIdentifier, AtomicInteger> loads = Maps.newConcurrentMap();
    private final Map<TableIdentifier, Runnable> hooks = Maps.newConcurrentMap();
    private final Map<TableIdentifier, RuntimeException> failures = Maps.newConcurrentMap();

    private CountingCatalog(Catalog delegate) {
      this.delegate = delegate;
    }

    int loads(TableIdentifier identifier) {
      AtomicInteger count = loads.get(identifier);
      return count == null ? 0 : count.get();
    }

    void beforeLoading(TableIdentifier identifier, Runnable hook) {
      hooks.put(identifier, hook);
    }

    void failLoading(TableIdentifier identifier, RuntimeException failure) {
      failures.put(identifier, failure);
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return delegate.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return delegate.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      delegate.renameTable(from, to);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      loads.computeIfAbsent(identifier, id -> new AtomicInteger()).incrementAndGet();
      Runnable hook = hooks.get(identifier);
      if (hook != null) {
        hook.run();
      }

      RuntimeException failure = failures.get(identifier);
      if (failure != null) {
        throw failure;
      }

      return delegate.loadTable(identifier);
    }
  }
}
