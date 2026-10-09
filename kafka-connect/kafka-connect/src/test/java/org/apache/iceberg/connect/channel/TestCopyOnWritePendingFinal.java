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

import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.EndlessAvroFile;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.NdvBounds;
import org.apache.iceberg.connect.StatisticsAssertions;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.WarningCapture;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.connect.data.NdvComputation;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStep.Outcome;
import org.apache.iceberg.connect.data.StatisticsStep.Result;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

/**
 * The pending final snapshot F of copy-on-write and its recount: a step of F that did not finish
 * leaves its change set X, and F is recounted from it by the lag step of a quiet table, by the
 * first slice of the next drain, or by the recount transition (one that only recounts F) of a start
 * that froze nothing, which the merge-on-read tail waits for.
 *
 * <p>Cycles are driven as {@code Coordinator.doCommit} drives the committer, and the test plays the
 * workers, which write the intermediate Puffin file of what they write. The statistics step is real
 * unless scripted ({@link ScriptedStep}), transitions run inline unless held ({@link HeldPool}),
 * and the storage can be failed and watched ({@link FaultyCatalog}).
 *
 * <p>The committer's map of states and its statistics lag are private and read by reflection.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class TestCopyOnWritePendingFinal {
  private static final String CTL_TOPIC = "ctl-topic";
  private static final String GROUP_ID = "cg-connect";
  private static final String SRC_TOPIC = "src-topic";
  private static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  private static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";
  private static final String FINAL_CHANGE_SET_ID_PROP =
      "kafka.connect.copy-on-write.final-change-set-id";
  private static final long AWAIT_SECONDS = 10L;
  private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);
  private static final int LG_K = 12;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<String, List<TopicPartitionRef>> ACTIVE_TASKS =
      ImmutableMap.of("task-0", ImmutableList.of());
  private static final Set<String> LEVELS = ImmutableSet.of("trace", "debug", "info", "warn");

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);
  private static final Schema CHANGE =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "op", Types.IntegerType.get()));

  private InMemoryCatalog catalog;
  private FaultyCatalog faulty;
  private Table table;
  private IcebergSinkConfig config;
  private final StatisticsHealth health = new StatisticsHealth(HOUR_MS);
  private final ScriptedStep step = new ScriptedStep();
  private final HeldPool pool = new HeldPool();
  private final List<LogLine> log = Lists.newCopyOnWriteArrayList();
  private final List<TableCommitRequest> offered = Lists.newCopyOnWriteArrayList();
  private final ThreadLocal<List<String>> opening = new ThreadLocal<>();
  private final Map<StatisticsStepInput, List<String>> openedBy = Maps.newConcurrentMap();
  private final Map<String, RuntimeException> readFailures = Maps.newConcurrentMap();
  private volatile Runnable onReadFailure = () -> {};
  private final AtomicBoolean deletesFail = new AtomicBoolean();
  private final AtomicBoolean loseFinalAnswer = new AtomicBoolean();
  private final AtomicBoolean lookupFails = new AtomicBoolean();
  private final AtomicBoolean failLookupOfFinal = new AtomicBoolean();
  private final AtomicBoolean failLostWarning = new AtomicBoolean();
  private final AtomicBoolean refreshByName = new AtomicBoolean();
  private volatile EndlessAvroFile endless = null;
  private CopyOnWriteRewriteDriver driver;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
    faulty = new FaultyCatalog(catalog);

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn(GROUP_ID);
    when(config.commitThreads()).thenReturn(2);
    when(config.tableConfig(any())).thenReturn(statistics(true));
    when(config.stagingLocation()).thenReturn(null);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1_000_000L);
    when(config.copyOnWriteMaxRewriteBytes()).thenReturn(Long.MAX_VALUE);
    when(config.copyOnWritePruningMaxInCardinality()).thenReturn(1000);
    when(config.copyOnWriteCommitRetries()).thenReturn(2);
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(600_000L);
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
    when(config.controlMessageMaxBytes()).thenReturn(1024 * 1024);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(Long.MAX_VALUE);
    when(config.stagingOrphanTtlMs()).thenReturn(86_400_000L);
    when(config.statisticsInUse()).thenReturn(true);
    when(config.statisticsLgNominalEntries()).thenReturn(LG_K);
    when(config.statisticsGapMaxFiles()).thenReturn(1_000);
    when(config.statisticsGapMaxBytes()).thenReturn(Long.MAX_VALUE);
    when(config.statisticsRetainSnapshots()).thenReturn(10);
    when(config.statisticsStaleDeletedRatio()).thenReturn(0.2);
    when(config.statisticsHealthRecheckIntervalMs()).thenReturn(HOUR_MS);
    when(config.configuredTables()).thenReturn(ImmutableSet.of());
    newDriver();
  }

  @AfterEach
  public void after() throws IOException {
    driver.stop();
    if (catalog.tableExists(TABLE_IDENTIFIER)) {
      assertStatisticsOnOwnSnapshot(catalog.loadTable(TABLE_IDENTIFIER));
    }

    catalog.close();
    assertThat(step.overlaps.overlaps()).as("two steps of one (table, S) at once").isEmpty();
  }

  /**
   * The step of F failed, and the table goes quiet. Its lag step recounts F instead of stepping on
   * the head, from the answers of the manifest of X and with its own commit id: F gets its NDV from
   * the Puffin file with no data file opened, and X goes with the lag.
   */
  @Test
  public void lagStepOfQuietTableRecountsPendingFinalSnapshot() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    assertThat(lag().contains(tbl())).isTrue();

    assertCommitted(cycle());

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.ownSnapshotIds()).containsExactly(pending.snapshotId);
    assertThat(recount.ownChangeSetIds()).containsExactly(pending.changeSetId.toString());
    assertThat(recount.answers())
        .extracting(answer -> answer.sketchFile().location())
        .containsExactly(pending.puffin);
    assertThat(recount.commitId()).isEqualTo(offeredWithoutUuid().commitId());
    assertThat(recount.keepSketchesOnFailure()).isTrue();
    assertThat(step.headSteps()).as("no step on the head").isEmpty();
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
    assertThat(lines("INFO", "Recomputing the NDV of snapshot " + pending.snapshotId))
        .singleElement()
        .satisfies(line -> assertThat(line.message).contains(pending.changeSetId.toString()));
  }

  /**
   * The recount of F by the first slice of the next drain throws an {@code Error}: the table enters
   * the lag with a new generation, X is not deleted as on a refusal, and the Error goes on. A later
   * recount of F is not checked: only a lookup that still finds F as the latest snapshot of the
   * connector recounts it.
   */
  @Test
  public void recountThatThrowsErrorPutsTableIntoLagAndErrorGoesOn() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    long generation = lag().token(tbl());
    OutOfMemoryError error = new OutOfMemoryError("the recount throws");
    step.script(
        isRecountOf(pending),
        input -> {
          throw error;
        });
    TableCommitRequest next = requestOf(rowChangesAt(30L, stagedFiles(update(2L, "b2")), null));

    assertThatThrownBy(() -> driver.commitDrain(next)).isSameAs(error);

    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(lag().contains(tbl())).isTrue();
    assertThat(lag().token(tbl())).isGreaterThan(generation);
    assertThat(exists(pending.manifest)).as("X is not deleted").isTrue();
  }

  /**
   * The step of F failed, another writer appends H on top of F, and the table goes quiet. The lag
   * step looks F up past H, the latest snapshot of the connector, not by the head: the record goes
   * on F, H gets none, and X goes with the lag.
   */
  @Test
  public void lagStepRecountsPendingFinalSnapshotUnderForeignHead() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    appendRows(row(9L, "z"));
    long head = headId();
    assertThat(head).as("the foreign append is the head").isNotEqualTo(pending.snapshotId);
    assertThat(table.currentSnapshot().parentId()).isEqualTo(pending.snapshotId);

    assertCommitted(cycle());

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.snapshotId()).isEqualTo(pending.snapshotId);
    assertThat(step.headSteps()).as("no step on the head").isEmpty();
    assertThat(recordOf(head)).as("no record on the foreign head").isNull();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
  }

  /**
   * The step of F stopped leaves X, and the lag as it is. After a restart the table, named by the
   * configuration, is in the lag at startup, and its lag step recounts F.
   */
  @Test
  public void stepOfFinalSnapshotStoppedIsRecountedByLagAtStartup() {
    stoppedStepOfFIsRecountedAtStartup(ImmutableMap.of("iceberg.tables", tbl().toString()));
  }

  /** The same for a table the topic-to-table mapping names. */
  @Test
  public void stepOfFinalSnapshotStoppedIsRecountedByLagAtStartupOfTopicMappedTable() {
    stoppedStepOfFIsRecountedAtStartup(
        ImmutableMap.of(
            "routing.strategy",
            "topic-to-table",
            "iceberg.tables.topic-to-table-mapping",
            SRC_TOPIC + ":" + tbl()));
  }

  private void stoppedStepOfFIsRecountedAtStartup(Map<String, String> routing) {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.STOPPED);
    assertThat(lag().contains(tbl())).isFalse();

    Map<String, String> props = Maps.newHashMap(routing);
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", SRC_TOPIC);
    Set<TableIdentifier> configured = new IcebergSinkConfig(props).configuredTables();
    assertThat(configured).containsExactly(tbl());
    when(config.configuredTables()).thenReturn(configured);
    restart();
    // the pointer is read and the table verified, then its lag step
    assertCommitted(cycle());
    assertCommitted(cycle());

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.keepSketchesOnFailure()).isTrue();
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
  }

  /**
   * The step of F failed, and the next cycle brings changes. The first slice of the new drain
   * recounts F before it plans, with the commit id of the drain and no Puffin file kept; the step
   * of the new final snapshot F2 then takes F as its base: no gap, no data file opened.
   */
  @Test
  public void firstSliceOfNextDrainRecountsPendingFinalSnapshotFirst() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    AtomicReference<Long> headAtRecount = new AtomicReference<>();
    step.script(
        isRecountOf(pending),
        input -> {
          headAtRecount.set(input.table().currentSnapshot().snapshotId());
          return step.realRun(input);
        });
    Record[] changes = {update(2L, "b2"), insert(4L, "d")};
    TableCommitRequest next = requestOf(rowChangesAt(30L, stagedFiles(changes), sketchOf(changes)));

    driver.commitDrain(next);

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.commitId()).isEqualTo(next.commitId());
    assertThat(recount.keepSketchesOnFailure()).isFalse();
    assertThat(headAtRecount.get())
        .as("before the first slice of the drain")
        .isEqualTo(pending.snapshotId);
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);

    long secondFinal = finalSnapshot().snapshotId();
    assertThat(secondFinal).isNotEqualTo(pending.snapshotId);
    StatisticsStepInput stepOfSecondFinal = stepOf(secondFinal);
    assertThat(step.inputs.indexOf(recount)).isLessThan(step.inputs.indexOf(stepOfSecondFinal));
    assertThat(dataFilesOpenedBy(stepOfSecondFinal)).as("F2 on the base F: no gap").isEmpty();
    NdvBounds.of(table, secondFinal, "id")
        .base(List.of(1L, 2L, 3L))
        .written(2L, 4L)
        .checkRecorded(LG_K);
    assertThat(readAll())
        .containsExactlyInAnyOrder(
            tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c"), tuple(4L, "d"));
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  /**
   * The recount in the first slice fails: a refusal. «NDV of change set X is lost» at WARN, X goes,
   * and it is marked refused; the drain goes on, and its final snapshot gets a step of its own.
   */
  @Test
  public void recountFailingInFirstSliceIsRefusedAndDrainGoesOn() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    step.script(isRecountOf(pending), input -> failed("the recount fails"));
    Record[] changes = {update(2L, "b2")};
    TableCommitRequest next = requestOf(rowChangesAt(30L, stagedFiles(changes), sketchOf(changes)));

    driver.commitDrain(next);

    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(readAll())
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c"));
    long secondFinal = finalSnapshot().snapshotId();
    assertThat(secondFinal).isNotEqualTo(pending.snapshotId);
    assertThat(stepOf(secondFinal).commitId()).isEqualTo(next.commitId());
    assertThat(recordOf(secondFinal)).as("the drain's own step writes").isNotNull();
    assertLost(pending, "the recount fails");
    assertChangeSetGone(pending);
    assertThat(recordOf(pending.snapshotId)).isNull();
    assertThat(refusedMark()).isEqualTo(pending.changeSetId);
  }

  /**
   * The step of F failed, and the next cycle has only the merge-on-read tail. The table answers
   * PENDING without committing it; the recount transition recounts F with the commit id of that
   * cycle, and X goes. The cycle after commits the tail, whose step takes F as its base.
   */
  @Test
  public void tailWaitsForRecountOfPendingFinalSnapshotAndThenCommitsOnIt() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    TableCommitRequest first = requestOf(tail);
    pool.hold();

    CommitResult waiting = cycle(first).get(written());
    assertThat(waiting.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(waiting.consumed()).doesNotContain(tail);
    assertThat(headId()).as("the tail is not committed").isEqualTo(pending.snapshotId);
    assertThat(pool.held()).as("the recount transition").isEqualTo(1);
    assertThat(recountsOf(pending)).isEmpty();

    pool.runHeld();
    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.commitId()).isEqualTo(first.commitId());
    assertThat(recount.keepSketchesOnFailure()).isFalse();
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);

    pool.runInline();
    CommitResult committed = cycle(requestOf(tail)).get(written());
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    table.refresh();
    Snapshot tailSnapshot = table.currentSnapshot();
    assertThat(tailSnapshot.parentId()).isEqualTo(pending.snapshotId);
    StatisticsStepInput stepOfTail = stepOf(tailSnapshot.snapshotId());
    assertThat(dataFilesOpenedBy(stepOfTail)).as("the tail on the base F").isEmpty();
    assertThat(recordOf(tailSnapshot.snapshotId())).isNotNull();
    assertThat(recountsOf(pending)).hasSize(1);
  }

  /**
   * The step of F failed, and another writer appends H on top of F; the head is other than F only
   * this way. The recount transition recounts F, not the head: H gets no record. The tail then
   * commits on H and gets a record of its own snapshot.
   */
  @Test
  public void transitionRecountsPendingFinalSnapshotUnderForeignHead() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    appendRows(row(9L, "z"));
    long head = headId();
    assertThat(head).as("the foreign append is the head").isNotEqualTo(pending.snapshotId);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    TableCommitRequest first = requestOf(tail);
    pool.hold();

    assertThat(cycle(first).get(written()).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).as("the recount transition").isEqualTo(1);
    pool.runHeld();

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.snapshotId()).isEqualTo(pending.snapshotId);
    assertThat(recount.commitId()).isEqualTo(first.commitId());
    assertThat(step.headSteps()).as("no step on the head").isEmpty();
    assertThat(recordOf(head)).as("no record on the foreign head").isNull();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);

    pool.runInline();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    Snapshot tailSnapshot = table.currentSnapshot();
    assertThat(tailSnapshot.parentId()).isEqualTo(head);
    stepOf(tailSnapshot.snapshotId());
    assertThat(recordOf(tailSnapshot.snapshotId())).isNotNull();
    assertThat(recordOf(head)).as("no record on the foreign head").isNull();
    assertThat(recountsOf(pending)).hasSize(1);
    assertStatisticsOnOwnSnapshot(table);
  }

  /**
   * The one thread of the transition pool is busy for two cycles, and each finds F and sets a
   * transition. Once the pool is free, the first recounts F with the commit id of the latest cycle
   * that found it, the second leaves by its guard, and the next cycle commits the tail.
   */
  @Test
  public void transitionsSetWhilePoolIsBusyRecountOnceAndThenTailCommits() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    TableCommitRequest first = requestOf(tail);
    TableCommitRequest second = requestOf(tail);
    pool.hold();

    assertThat(cycle(first).get(written()).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(cycle(second).get(written()).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).as("a transition of each cycle").isEqualTo(2);

    pool.runHeld();
    assertThat(recountsOf(pending))
        .singleElement()
        .satisfies(input -> assertThat(input.commitId()).isEqualTo(second.commitId()));
    assertRecountedNdv(pending);

    pool.runInline();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(pending.snapshotId);
    assertThat(recountsOf(pending)).hasSize(1);
  }

  /**
   * The step of F left {@code stats-<F>-<c>.puffin} of the commit id c of its drain, with no
   * record. The transition recounts F with the commit id of the cycle that found it, so its file
   * has a name of its own and the step writes it. The same after a restart, where c is not known.
   */
  @ParameterizedTest(name = "restarted = {0}")
  @ValueSource(booleans = {false, true})
  public void recountWritesFileOfItsOwnBesideOneStepOfFinalSnapshotLeft(boolean restarted) {
    baseline();
    AtomicReference<String> left = new AtomicReference<>();
    Pending pending =
        drainWithStepOfF(
            input -> {
              String location =
                  NdvComputation.fileLocation(input.table(), input.snapshotId(), input.commitId());
              write(location, "an unknown outcome of the commit of the record");
              left.set(location);
              return failed("the commit of the record of F did not report its outcome");
            });
    if (restarted) {
      restart();
    }

    TableCommitRequest tail = requestOf(dataWrittenAt(40L, row(5L, "e")));
    assertThat(cycle(tail).get(written()).outcome()).isEqualTo(TableCommitter.Outcome.PENDING);

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.commitId()).isEqualTo(tail.commitId()).isNotEqualTo(pending.commitId);
    StatisticsFile record = recordOf(pending.snapshotId);
    assertThat(record).as("the recount writes").isNotNull();
    assertThat(record.path())
        .isEqualTo(NdvComputation.fileLocation(table, pending.snapshotId, tail.commitId()));
    assertThat(exists(left.get())).as("the file of the step of F is not the recount's").isTrue();
    assertThat(lines("WARN", "is lost")).isEmpty();
    assertStatisticsFilesExist(table);
  }

  /**
   * An exception of the recount in the transition is a refusal (the substituted step throws; the
   * assembly of its answers is in the same try/catch): «NDV of change set X is lost» at WARN, X
   * goes with its Puffin file, the table enters the lag and X is marked. The next cycle commits the
   * tail.
   */
  @Test
  public void recountThatThrowsInTransitionIsRefusedAndTailCommitsNextCycle() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    long generation = lag().token(tbl());
    step.script(
        isRecountOf(pending),
        input -> {
          throw new IllegalStateException("the answers of the recount cannot be assembled");
        });
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));

    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);

    assertThat(recountsOf(pending)).hasSize(1);
    assertLost(pending, "the answers of the recount cannot be assembled");
    assertChangeSetGone(pending);
    assertThat(lag().token(tbl()))
        .as("entered, as on a failed step of F")
        .isGreaterThan(generation);
    assertThat(refusedMark()).isEqualTo(pending.changeSetId);

    CommitResult committed = cycle(requestOf(tail)).get(written());
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    assertThat(recountsOf(pending)).hasSize(1);
  }

  /**
   * The recount throws in the first slice of a drain: a refusal, and the same {@code startSlice}
   * goes on: the slice of the new change set is handed out, its manifest in place, and the drain
   * commits.
   */
  @Test
  public void recountThatThrowsInFirstSliceIsRefusedAndSliceIsHandedOut() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    step.script(
        isRecountOf(pending),
        input -> {
          throw new IllegalStateException("the answers of the recount cannot be assembled");
        });
    TableCommitRequest next = requestOf(rowChangesAt(30L, stagedFiles(update(2L, "b2")), null));

    List<RewriteAssigned> slice = driver.handOutOneSlice(next);

    assertThat(recountsOf(pending)).hasSize(1);
    assertLost(pending, "the answers of the recount cannot be assembled");
    assertThat(slice).as("the slice of the new change set").isNotEmpty();
    UUID newChangeSet = slice.get(0).changeSetId();
    assertThat(newChangeSet).isNotEqualTo(pending.changeSetId);
    assertThat(exists(manifestOf(newChangeSet))).isTrue();
    assertChangeSetGone(pending);

    driver.answerAssignments(slice);
    assertThat(readAll())
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c"));
  }

  /**
   * The refusal is inside the try/catch of the recount as well. What it does after the mark fails
   * (here, its WARN): the drain goes on all the same, the slice is handed out, and the WARN of the
   * refusal left unfinished tells that the NDV of the change set may be lost.
   */
  @Test
  public void refusalThatFailsHalfWayDoesNotFailDrain() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    step.script(isRecountOf(pending), input -> failed("the recount fails"));
    failLostWarning.set(true);
    TableCommitRequest next = requestOf(rowChangesAt(30L, stagedFiles(update(2L, "b2")), null));

    List<RewriteAssigned> slice = driver.handOutOneSlice(next);

    assertThat(slice).as("the slice of the new change set").isNotEmpty();
    assertThat(refusedMark()).isEqualTo(pending.changeSetId);
    assertThat(lines("WARN", "Could not finish the refusal"))
        .singleElement()
        .satisfies(
            line -> {
              assertThat(line.message)
                  .as("the operator is told the NDV may be lost, and what restores it")
                  .contains("NDV of the change set may be lost")
                  .contains("ANALYZE of the table restores it");
              assertThat(line.throwable).hasMessage("the log fails");
            });
  }

  /**
   * The request holds a {@code RowChangesWritten} below a {@code DataWritten} of the tail on the
   * partition, so the drain freezes first and the tail waits behind it. The first slice recounts F
   * with the commit id of the cycle, before the step of the new final snapshot F2; the tail is
   * committed by a cycle after the drain, on the base F2.
   */
  @Test
  public void drainFrozenBeforeTailRecountsPendingFinalSnapshotAndTailCommitsAfterIt() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    Record[] changes = {update(2L, "b2")};
    Envelope rowChanges = rowChangesAt(30L, stagedFiles(changes), sketchOf(changes));
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    TableCommitRequest next = requestOf(rowChanges, tail);

    driver.commitDrain(next);

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.commitId()).isEqualTo(next.commitId());
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    long secondFinal = finalSnapshot().snapshotId();
    assertThat(secondFinal).isNotEqualTo(pending.snapshotId);
    assertThat(headId())
        .as("the tail is not committed by the cycle of the drain")
        .isEqualTo(secondFinal);
    assertThat(step.inputs.indexOf(recount)).isLessThan(step.inputs.indexOf(stepOf(secondFinal)));

    CommitResult committed = cycle(requestOf(tail)).get(written());

    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    table.refresh();
    Snapshot tailSnapshot = table.currentSnapshot();
    assertThat(tailSnapshot.parentId()).isEqualTo(secondFinal);
    assertThat(recordOf(tailSnapshot.snapshotId())).isNotNull();
    assertThat(recountsOf(pending)).hasSize(1);
  }

  /**
   * A table in the lag whose cycle brings a {@code RowChangesWritten} below a {@code DataWritten}
   * of the tail. The drain freezes first and recounts F, the next cycle commits the tail, and a
   * cycle then gives the table its lag step. No two steps of one (table, S) overlap, each snapshot
   * has one record, and no {@code stats-<S>-*} file is left without one.
   */
  @Test
  public void tableInLagWithTailFrozenBehindItsDrainEndsWithOneRecordPerSnapshot() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    assertThat(lag().contains(tbl())).isTrue();
    Record[] changes = {update(2L, "b2")};
    Envelope rowChanges = rowChangesAt(30L, stagedFiles(changes), sketchOf(changes));
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));

    driver.commitDrain(requestOf(rowChanges, tail));
    long secondFinal = finalSnapshot().snapshotId();
    assertThat(headId()).as("the tail waits behind the drain").isEqualTo(secondFinal);
    // the step of the tail fails, and the lag step is the one to give the head its record
    AtomicBoolean once = new AtomicBoolean(true);
    step.script(
        input -> input.snapshotId() != null && once.getAndSet(false),
        input -> failed("the step of the tail fails"));
    assertCommitted(cycle(requestOf(tail)));
    assertThat(once).as("the step of the tail").isFalse();
    long tailSnapshot = headId();
    assertThat(tailSnapshot).isNotEqualTo(secondFinal);
    assertThat(recordOf(tailSnapshot)).isNull();
    assertThat(lag().contains(tbl())).isTrue();

    assertCommitted(cycle());

    assertRecountedNdv(pending);
    assertThat(recordOf(secondFinal)).as("the record of the second final snapshot").isNotNull();
    assertThat(recordOf(tailSnapshot)).as("the record of the tail").isNotNull();
    assertThat(step.headSteps()).as("the lag step ran on the head").hasSize(1);
    assertThat(lag().tables()).isEmpty();
    StatisticsAssertions.assertOneRecordPerSnapshotAndNoStrayFinalFiles(table);
  }

  /**
   * A restart with the pointer of an unfinished drain and the table in the lag at startup. The
   * first cycle has no tasks, the second resumes the drain to F, and a cycle then gives the table
   * its lag step. Steps, records and files are checked as above.
   */
  @Test
  public void restartWithDrainToResumeAndLagAtStartupEndsWithOneRecordPerSnapshot() {
    baseline();
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c")};
    driver.commitOneSlice(requestOf(rowChangesAt(20L, stagedFiles(changes), sketchOf(changes))));
    when(config.configuredTables()).thenReturn(ImmutableSet.of(tbl()));
    restart();
    assertThat(lag().contains(tbl())).as("in the lag at startup").isTrue();
    // the step of F of the resumed drain fails, and the lag step recounts F
    AtomicBoolean once = new AtomicBoolean(true);
    step.script(
        input -> input.snapshotId() != null && once.getAndSet(false),
        input -> failed("the step of the final snapshot fails"));

    for (TableReference visited : committer().pendingTables()) {
      assertThat(
              driver.commitWithNoTasks(
                  new TableCommitRequest(
                      visited,
                      ImmutableList.of(),
                      ImmutableMap.of(),
                      UUID.randomUUID(),
                      OffsetDateTime.now())))
          .isEqualTo(TableCommitter.Outcome.PENDING);
    }

    for (TableReference visited : committer().pendingTables()) {
      assertThat(driver.commit(withoutResponses(visited)))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
    }

    Snapshot resumedFinal = finalSnapshot();
    assertThat(readAll())
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b"), tuple(3L, "c"));

    assertThat(once).as("the step of F").isFalse();
    assertThat(recordOf(resumedFinal.snapshotId())).isNull();
    assertThat(lag().contains(tbl())).isTrue();

    assertCommitted(cycle());

    assertThat(recordOf(resumedFinal.snapshotId())).as("the record of F").isNotNull();
    assertThat(lag().tables()).isEmpty();
    StatisticsAssertions.assertOneRecordPerSnapshotAndNoStrayFinalFiles(table);
  }

  /**
   * The table is replaced under its name between the start of the drain, which found F, and its
   * first slice. The slice leaves by the check of the table's UUID, before the recount: F of the
   * old table is not recounted against the new one, and the drain drops F it found.
   */
  @Test
  public void tableReplacedBeforeFirstSliceIsNotOnePendingFinalSnapshotIsRecountedOn() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    refreshByName.set(true);
    pool.hold();
    assertThat(
            cycle(requestOf(rowChangesAt(30L, stagedFiles(update(2L, "b2")), null)))
                .get(written())
                .outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).as("the first slice").isEqualTo(1);
    assertThat(pendingFinalOfState()).as("the start found F").isNotNull();
    catalog.dropTable(tbl(), false);
    Table replacement =
        catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
    assertThat(replacement.uuid()).isNotEqualTo(table.uuid());

    pool.runHeld();

    assertThat(recountsOf(pending)).isEmpty();
    assertThat(pendingFinalOfState()).isNull();
    assertThat(exists(pending.manifest)).isTrue();
    replacement.refresh();
    assertThat(replacement.currentSnapshot()).isNull();
  }

  /**
   * A refusal whose deletion of X fails. The mark keeps the lookup off F in this life of the
   * coordinator: the next cycle finds no pending F, commits the tail and recounts no more.
   */
  @Test
  public void refusedChangeSetThatCannotBeDeletedIsNotRecountedAgain() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    step.script(isRecountOf(pending), input -> failed("the recount fails"));
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    deletesFail.set(true);

    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(recountsOf(pending)).hasSize(1);
    assertLost(pending, "the recount fails");
    assertThat(exists(pending.manifest)).as("X could not be deleted").isTrue();
    deletesFail.set(false);

    CommitResult committed = cycle(requestOf(tail)).get(written());
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(pending.snapshotId);
    assertThat(recountsOf(pending)).as("no second recount").hasSize(1);
    assertThat(lines("DEBUG", pending.changeSetId.toString()))
        .anySatisfy(line -> assertThat(line.message).contains("refused"));
  }

  /**
   * The recount in the transition ends without a record of F (FINAL, or WRITTEN by the partitions
   * only), and X cannot be deleted. The tail waited for that one recount: the next cycle finds no
   * pending F and commits it.
   */
  @ParameterizedTest
  @EnumSource(
      value = Outcome.class,
      names = {"FINAL", "WRITTEN"})
  public void recountWithoutRecordWhoseChangeSetCannotBeDeletedLetsTailCommit(Outcome outcome) {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    step.script(isRecountOf(pending), input -> new Result(outcome, null));
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    deletesFail.set(true);

    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(recordOf(pending.snapshotId)).isNull();
    assertThat(exists(pending.manifest)).as("X could not be deleted").isTrue();

    CommitResult committed = cycle(requestOf(tail)).get(written());
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(pending.snapshotId);
    assertThat(recountsOf(pending)).as("no second recount").hasSize(1);
  }

  /**
   * The step of F in {@code finishSlice} ends without a record of F, and X cannot be deleted. F is
   * not pending: the next cycle commits the tail, no recount.
   */
  @ParameterizedTest
  @EnumSource(
      value = Outcome.class,
      names = {"FINAL", "WRITTEN"})
  public void stepOfFWithoutRecordWhoseChangeSetCannotBeDeletedLeavesNothingToRecount(
      Outcome outcome) {
    baseline();
    deletesFail.set(true);
    Pending pending = drainWithStepOfF(outcome);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));

    CommitResult committed = cycle(requestOf(tail)).get(written());

    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(pending.snapshotId);
    assertThat(recountsOf(pending)).isEmpty();
  }

  /**
   * The recount by the lag step ends without a record of F, and X cannot be deleted. The tail of a
   * later cycle does not wait for a second recount.
   */
  @Test
  public void recountInLagStepWithoutRecordLetsLaterTailCommit() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    step.script(isRecountOf(pending), input -> new Result(Outcome.FINAL, null));
    deletesFail.set(true);

    assertCommitted(cycle());
    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(exists(pending.manifest)).as("X could not be deleted").isTrue();

    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    CommitResult committed = cycle(requestOf(tail)).get(written());
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    assertThat(recountsOf(pending)).as("no second recount").hasSize(1);
  }

  /**
   * A recount transition is set, then the start of a drain finds the table gone and drops its
   * state. The transition leaves by its guard: no recount.
   */
  @Test
  public void transitionOfTableDroppedSinceLeavesByItsGuard() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    pool.hold();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).isEqualTo(1);

    catalog.dropTable(tbl(), false);
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(states()).doesNotContainKey(written());
    int steps = step.inputs.size();
    pool.runHeld();

    assertThat(recountsOf(pending)).isEmpty();
    assertThat(step.inputs).hasSize(steps);
  }

  /**
   * An exception in the start of a drain resets its state, and with it the pending F it found: a
   * transition set before leaves by its guard. The next cycle finds F again.
   */
  @Test
  public void exceptionInStartOfDrainDropsPendingFinalSnapshotItFound() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    pool.hold();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).isEqualTo(1);

    // changes staged by identifier fields the table does not have: the freeze rejects them
    TableCommitRequest rejected =
        requestOf(rowChangesAt(41L, stagedFiles(Set.of(2), update(2L, "b2")), null));
    assertThat(cycle(rejected).get(written()).outcome()).isEqualTo(TableCommitter.Outcome.FAILED);
    pool.runHeld();
    assertThat(recountsOf(pending)).as("the transition leaves by its guard").isEmpty();

    pool.runInline();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(recountsOf(pending)).hasSize(1);
    assertRecountedNdv(pending);
  }

  /**
   * The lag step waits for its turn while a drain passes and its step of F fails. Under the lock it
   * looks F up by the table as of then, and recounts F instead of stepping on the head, with the
   * commit id of its own cycle.
   */
  @Test
  public void lagStepSetBeforeDrainWhoseStepOfFinalSnapshotFailedRecountsIt() {
    baseline();
    // another writer's snapshot on top: the quick check finds no record and verifies the lag
    appendRows(row(9L, "z"));
    lag().enter(tbl());
    pool.hold();
    assertCommitted(cycle());
    assertThat(pool.held()).as("the lag step").isEqualTo(1);
    UUID lagCycle = offeredWithoutUuid().commitId();
    Runnable lagStep = pool.take();
    pool.runInline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);

    lagStep.run();

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(recount.commitId()).isEqualTo(lagCycle);
    assertThat(recount.keepSketchesOnFailure()).isTrue();
    assertThat(step.headSteps()).as("no step on the head").isEmpty();
    assertThat(recordOf(pending.snapshotId)).isNotNull();
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
  }

  /**
   * A recount by the lag step that fails is no refusal. X stays with its Puffin file, the table
   * stays in the lag without a mark and nothing is lost; the next lag step recounts again.
   */
  @Test
  public void recountFailingInLagStepKeepsChangeSetForNextOne() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    AtomicBoolean failOnce = new AtomicBoolean(true);
    step.script(
        isRecountOf(pending).and(input -> failOnce.getAndSet(false)),
        input -> failed("the recount fails once"));

    assertCommitted(cycle());
    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(exists(pending.manifest)).isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).as("kept for the next recount").isTrue();
    assertThat(lag().contains(tbl())).isTrue();
    assertThat(refusedMark()).isNull();
    assertThat(lines("WARN", "is lost")).isEmpty();

    assertCommitted(cycle());
    assertThat(recountsOf(pending)).hasSize(2);
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
  }

  /**
   * A recount by the lag step fails while the recount transition set by the start of a drain in the
   * same cycle waits for the lock. The transition still finds F pending and recounts it, so the
   * tail commits in the next cycle, not one cycle later after a recount of its own.
   */
  @Test
  public void recountFailingInLagStepLeavesFToTransitionOfCycle() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    AtomicBoolean failOnce = new AtomicBoolean(true);
    step.script(
        isRecountOf(pending).and(input -> failOnce.getAndSet(false)),
        input -> failed("the recount by the lag step fails"));
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    pool.hold();
    assertCommitted(cycle());
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).as("the lag step, then the transition").isEqualTo(2);

    pool.runHeld();

    assertThat(recountsOf(pending))
        .as("the transition recounts after the failed recount of the lag step")
        .hasSize(2);
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(refusedMark()).isNull();
    pool.runInline();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(recountsOf(pending)).hasSize(2);
  }

  /**
   * The manifest of X cannot be read in the lag step for a reason that may pass: the step is
   * FAILED, the table stays in the lag, X stays; the next lag step recounts F.
   */
  @Test
  public void manifestThatCannotBeReadInLagStepFailsItAndNextOneRecounts() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    long generation = lag().token(tbl());
    readFailures.put(
        pending.manifest, new UncheckedIOException(new IOException("storage unreachable")));
    int steps = step.inputs.size();

    assertCommitted(cycle());

    assertThat(lag().token(tbl())).as("entered again, as FAILED").isGreaterThan(generation);
    assertThat(step.inputs).as("no step, of F or of the head").hasSize(steps);
    assertThat(exists(pending.manifest)).isTrue();

    assertCommitted(cycle());
    onlyRecountOf(pending);
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
  }

  /**
   * A manifest of X frozen for another table is no pending F: WARN, the step on the head, and X is
   * left to the cleanup.
   */
  @Test
  public void manifestFrozenForAnotherTableIsNoPendingFinalSnapshot() throws IOException {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    UUID another = UUID.randomUUID();
    rewriteManifest(pending, root -> root.put("tableUuid", another.toString()));
    step.script(input -> input.snapshotId() == null, input -> new Result(Outcome.NOT_NEEDED, null));

    assertCommitted(cycle());

    assertThat(step.headSteps()).as("the step on the head").hasSize(1);
    assertThat(recountsOf(pending)).isEmpty();
    assertThat(lines("WARN", pending.changeSetId.toString()))
        .singleElement()
        .satisfies(line -> assertThat(line.message).contains(another.toString()));
    assertThat(exists(pending.manifest)).isTrue();
  }

  /**
   * The start of a drain cannot read the manifest of X for a reason that may pass: no pending F,
   * WARN, and the start goes on: the tail is committed at once.
   */
  @Test
  public void startOfDrainThatCannotLookUpFinalSnapshotGoesOnWithoutRecount() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    readFailures.put(
        pending.manifest, new UncheckedIOException(new IOException("storage unreachable")));
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));

    CommitResult committed = cycle(requestOf(tail)).get(written());

    assertThat(lines("WARN", tbl().toString()))
        .singleElement()
        .satisfies(line -> assertThat(line.throwable).hasRootCauseMessage("storage unreachable"));
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    assertThat(recountsOf(pending)).isEmpty();
  }

  /**
   * The manifest of X is cut short by the stop that interrupts the pool of transitions: the lookup
   * is no failure of the storage, and the start does not claim it goes on without the recount.
   */
  @Test
  public void lookupOfFinalSnapshotCutShortByStopIsNoWarn() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    readFailures.put(
        pending.manifest, new UncheckedIOException(new IOException("closed by interrupt")));
    onReadFailure = driver::stop;
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));

    cycle(requestOf(tail));

    assertThat(readFailures).as("the manifest was read").doesNotContainKey(pending.manifest);
    assertThat(lines("WARN", tbl().toString())).as("a stop is no failure").isEmpty();
    assertThat(lines("WARN", "Cannot look up")).isEmpty();
  }

  /**
   * The final slice of a drain of several slices landed with an unknown outcome, and its snapshot
   * was not found. The next cycle without data sees a fresh start: no drain is unfinished, F is
   * recounted, the lag goes, and the table is not listed any more.
   */
  @Test
  public void finalSliceOfUnknownOutcomeNotFoundIsRecountedByNextCycle() {
    baseline();
    Pending pending = drainWithUnknownFinalSlice();
    assertThat(lag().contains(tbl())).isTrue();

    assertCommitted(cycle());

    onlyRecountOf(pending);
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
    assertThat(committer().pendingTables()).isEmpty();
  }

  /**
   * The final slice of a drain of several slices committed with an answer, and the lookup of its
   * snapshot for the report failed. F gets no step, and the table is in the lag. The next cycle,
   * with nothing buffered, recounts F by the lag step: its record, X gone, the lag gone.
   */
  @Test
  public void finalSliceLandedWhoseLookupFailedIsRecountedByLagStepOfLaterCycle() {
    baseline();
    Pending pending = drainWithFinalSliceWhoseLookupFails();
    assertThat(lag().contains(tbl())).isTrue();
    assertThat(exists(pending.manifest)).as("X is kept for the recount").isTrue();

    assertCommitted(cycle());

    onlyRecountOf(pending);
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
    assertThat(lag().tables()).isEmpty();
    assertThat(cycle()).as("nothing is left to the next cycle").isEmpty();
  }

  /**
   * A final slice of unknown outcome whose snapshot was not found, with only the merge-on-read tail
   * in the buffer: the tail waits for the recount of F, then commits.
   */
  @Test
  public void finalSliceOfUnknownOutcomeNotFoundLetsTailCommitAfterRecount() {
    baseline();
    Pending pending = drainWithUnknownFinalSlice();
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));

    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertRecountedNdv(pending);

    CommitResult committed = cycle(requestOf(tail)).get(written());
    assertThat(committed.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(committed.consumed()).contains(tail);
    table.refresh();
    assertThat(table.currentSnapshot().parentId()).isEqualTo(pending.snapshotId);
    assertThat(committer().pendingTables()).isEmpty();
  }

  /**
   * With the statistics off a final slice of unknown outcome whose snapshot was not found leaves no
   * lag, and from the second cycle the table is not listed any more.
   */
  @Test
  public void withStatisticsOffFinalSliceOfUnknownOutcomeLeavesNoTableListed() {
    when(config.tableConfig(any())).thenReturn(statistics(false));
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    loseFinalAnswer.set(true);
    driver.commitDrain(
        requestOf(rowChangesAt(20L, stagedFiles(update(1L, "a1"), insert(3L, "c")))));
    assertThat(loseFinalAnswer).as("the final slice landed without an answer").isFalse();
    assertThat(committer().pendingTables()).contains(written());

    assertCommitted(cycle());

    assertThat(committer().pendingTables()).isEmpty();
    assertThat(step.inputs).isEmpty();
  }

  enum Ending {
    THROWS,
    FAILED
  }

  enum Place {
    TRANSITION,
    FIRST_SLICE,
    LAG_STEP
  }

  static Stream<Arguments> placesAndEndings() {
    return Stream.of(Place.values())
        .flatMap(place -> Stream.of(Ending.values()).map(ending -> Arguments.of(place, ending)));
  }

  /**
   * The coordinator stops while F is recounted at {@code place}, and the recount then throws or
   * returns FAILED with the flag up. Either is STOPPED: X stays whole, nothing is marked or «lost»,
   * the lag is as it was, and a first slice hands nothing out.
   */
  @ParameterizedTest
  @MethodSource("placesAndEndings")
  public void coordinatorStoppingDuringRecountKeepsChangeSet(Place place, Ending ending) {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    long generation = lag().token(tbl());
    int handedOut = driver.handedOut().size();
    CountDownLatch recounting = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    step.script(
        isRecountOf(pending),
        input -> {
          recounting.countDown();
          await(release, "the coordinator stops");
          if (ending == Ending.THROWS) {
            throw new UncheckedIOException(new IOException("the stream was closed"));
          }

          return failed("the recount fails");
        });
    pool.hold();
    startRecountAt(place);
    assertThat(pool.held()).isEqualTo(1);

    Thread recount = pool.startHeld();
    await(recounting, "the recount runs");
    driver.stop();
    release.countDown();
    join(recount);

    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(exists(pending.manifest)).isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).isTrue();
    assertThat(lines("WARN", "is lost")).isEmpty();
    assertThat(lag().token(tbl())).isEqualTo(generation);
    assertThat(lag().contains(tbl())).isTrue();
    assertThat(refusedMark()).isNull();
    assertThat(driver.handedOut()).as("no slice after the stop").hasSize(handedOut);
    if (place == Place.FIRST_SLICE) {
      assertThat(normalizedLocationOfState())
          .as("nothing planned or written after the stop")
          .isNull();
    }
  }

  /** The cycle that has {@code place} recount F, its job held by the pool. */
  private void startRecountAt(Place place) {
    switch (place) {
      case TRANSITION:
        assertThat(cycle(requestOf(dataWrittenAt(40L, row(5L, "e")))).get(written()).outcome())
            .isEqualTo(TableCommitter.Outcome.PENDING);
        break;
      case FIRST_SLICE:
        assertThat(
                cycle(requestOf(rowChangesAt(30L, stagedFiles(update(2L, "b2")), null)))
                    .get(written())
                    .outcome())
            .isEqualTo(TableCommitter.Outcome.PENDING);
        break;
      default:
        assertCommitted(cycle());
        break;
    }
  }

  /** How the stop reaches the real step reading the gap. */
  enum Stop {
    /** The flag of the committer alone, as terminate() raises it first. */
    FLAG,
    /** The thread interrupted alone, and the read of the file throws on it. */
    INTERRUPT
  }

  /**
   * The real step of F, in {@code finishSlice}, reads a file of another writer in the gap that does
   * not end, and the coordinator stops it. The step is STOPPED within the next rows of the stop
   * check: X stays whole for the recount, nothing is «lost» or marked, no WARN, the lag and the
   * health as they were.
   */
  @ParameterizedTest
  @EnumSource(Stop.class)
  public void stopDuringReadOfGapByStepOfFinalSnapshotKeepsChangeSet(Stop stop) {
    baseline();
    EndlessAvroFile gap = endlessForeignAppend();
    long generation = lag().token(tbl());
    boolean inLag = lag().contains(tbl());
    long version = health.version(tbl());
    AtomicReference<Result> result = new AtomicReference<>();
    AtomicReference<WarningCapture> warnings = new AtomicReference<>();

    Pending pending;
    boolean interrupted;
    try {
      pending =
          drainWithStepOfF(
              input -> {
                warnings.set(WarningCapture.capture());
                return realRunStopped(input, gap, stop, result);
              });
    } finally {
      interrupted = Thread.interrupted();
      WarningCapture opened = warnings.get();
      if (opened != null) {
        opened.close();
      }
    }

    assertThat(interrupted).as("the interrupt is left in place").isEqualTo(stop == Stop.INTERRUPT);
    assertThat(warnings.get()).as("the step of F ran").isNotNull();
    assertThat(warnings.get().lines()).as("no WARN").isEmpty();
    assertThat(result.get().outcome()).isEqualTo(Outcome.STOPPED);
    assertThat(gap.rowsServed()).isBetween(15_000L, 30_000L);
    assertStoppedKeepingChangeSet(pending, generation, inLag, version);
  }

  /**
   * The real recount of F, by the recount transition, reads a file of another writer in the gap
   * that does not end, and the coordinator stops it. The transition is over within the wait of the
   * test, the recount STOPPED: X stays whole, nothing is «lost» or marked, no WARN, the lag and the
   * health as they were.
   */
  @ParameterizedTest
  @EnumSource(Stop.class)
  public void stopDuringReadOfGapByRecountInTransitionKeepsChangeSet(Stop stop) {
    baseline();
    EndlessAvroFile gap = endlessForeignAppend();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    long generation = lag().token(tbl());
    long version = health.version(tbl());
    AtomicReference<Result> result = new AtomicReference<>();
    step.script(isRecountOf(pending), input -> realRunStopped(input, gap, null, result));
    pool.hold();
    assertThat(cycle(requestOf(dataWrittenAt(40L, row(5L, "e")))).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).isEqualTo(1);
    if (stop == Stop.INTERRUPT) {
      gap.armInterruptible();
    } else {
      gap.arm();
    }

    try (WarningCapture warnings = WarningCapture.capture()) {
      Thread transition = pool.startHeld();
      gap.awaitServed(15_000L);
      if (stop == Stop.INTERRUPT) {
        gap.reader().interrupt();
      } else {
        driver.stop();
      }

      join(transition);
      assertThat(warnings.lines()).as("no WARN").isEmpty();
    }

    assertThat(recountsOf(pending)).hasSize(1);
    assertThat(result.get().outcome()).isEqualTo(Outcome.STOPPED);
    assertStoppedKeepingChangeSet(pending, generation, true, version);
  }

  private EndlessAvroFile endlessForeignAppend() {
    List<Record> rows = Lists.newArrayList();
    for (long id = 1_000L; id < 2_000L; id++) {
      rows.add(row(id, "g" + id % 10));
    }

    EndlessAvroFile gap = EndlessAvroFile.write(table, SCHEMA, rows);
    endless = gap;
    table.refresh();
    table.newAppend().appendFile(gap.dataFile()).commit();
    return gap;
  }

  private Result realRunStopped(
      StatisticsStepInput input, EndlessAvroFile gap, Stop stop, AtomicReference<Result> result) {
    if (stop == Stop.FLAG) {
      gap.arm().whenServed(15_000L, driver::stop);
    } else if (stop == Stop.INTERRUPT) {
      gap.armInterruptible().whenServed(15_000L, () -> Thread.currentThread().interrupt());
    }

    try {
      result.set(step.realRun(input));
      return result.get();
    } finally {
      gap.disarm();
    }
  }

  private void assertStoppedKeepingChangeSet(
      Pending pending, long generation, boolean inLag, long version) {
    assertThat(recordOf(pending.snapshotId)).isNull();
    assertThat(exists(pending.manifest)).isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).isTrue();
    assertThat(lines("WARN", "is lost")).isEmpty();
    assertThat(refusedMark()).isNull();
    assertThat(lag().token(tbl())).isEqualTo(generation);
    assertThat(lag().contains(tbl())).isEqualTo(inLag);
    assertThat(health.version(tbl())).as("no health transition").isEqualTo(version);
  }

  /**
   * The step of F failed; a lag step and a recount transition are both set. The first to run
   * recounts F holding the lock, the other waits for it and then finds no pending F: one recount,
   * one record of F, and no file of F without a record.
   */
  @Test
  public void lagStepAndTransitionOfOnePendingFinalSnapshotRecountItOnce()
      throws InterruptedException {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    CountDownLatch recounting = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    step.script(
        isRecountOf(pending),
        input -> {
          recounting.countDown();
          await(release, "the release of the recount");
          return step.realRun(input);
        });
    pool.hold();
    assertCommitted(cycle());
    assertThat(cycle(requestOf(dataWrittenAt(40L, row(5L, "e")))).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).as("the lag step, then the transition").isEqualTo(2);

    Thread first = pool.startHeld();
    await(recounting, "the recount holds the lock");
    Thread second = pool.startHeld();
    awaitBlocked(second);
    release.countDown();
    join(first);
    join(second);

    assertThat(recountsOf(pending)).hasSize(1);
    table.refresh();
    assertThat(table.statisticsFiles())
        .filteredOn(file -> file.snapshotId() == pending.snapshotId)
        .hasSize(1);
    Set<String> filesOfF =
        step.inputs.stream()
            .filter(input -> Long.valueOf(pending.snapshotId).equals(input.snapshotId()))
            .map(input -> NdvComputation.fileLocation(table, pending.snapshotId, input.commitId()))
            .filter(this::exists)
            .collect(Collectors.toSet());
    assertThat(filesOfF).containsExactly(recordOf(pending.snapshotId).path());
    assertChangeSetGone(pending);
  }

  /**
   * The tail waiting for the recount of F is logged at INFO once for X, then at DEBUG, in place of
   * «Nothing to commit»; the transitions of the cycles recount once.
   */
  @Test
  public void waitOfTailIsLoggedAtInfoOnceAndThenAtDebug() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    pool.hold();

    for (int cycle = 0; cycle < 3; cycle++) {
      assertThat(cycle(requestOf(tail)).get(written()).outcome())
          .isEqualTo(TableCommitter.Outcome.PENDING);
    }

    String waits =
        String.format(
            Locale.ROOT,
            "MoR tail of table %s waits for the NDV recompute of snapshot %d (change set %s)",
            tbl(),
            pending.snapshotId,
            pending.changeSetId);
    assertThat(lines("INFO", waits)).hasSize(1);
    assertThat(lines("DEBUG", waits)).hasSize(2);
    assertThat(lines("INFO", "Nothing to commit")).isEmpty();

    pool.runHeld();
    assertThat(recountsOf(pending)).hasSize(1);
    pool.runInline();
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(lines("INFO", waits)).hasSize(1);
  }

  /**
   * A start that finds F and has nothing to freeze and no tail (what the drain froze, offered
   * again, is applied) logs «Nothing to commit», and its transition recounts F.
   */
  @Test
  public void transitionWithoutTailLogsNothingToCommitAndRecountsFinalSnapshot() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);

    CommitResult result = cycle(requestOf(pending.envelope)).get(written());

    assertThat(result.outcome()).isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(lines("INFO", "Nothing to commit to table " + tbl() + ", skipping")).hasSize(1);
    assertThat(lines("INFO", "waits for the NDV recompute")).isEmpty();
    onlyRecountOf(pending);
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
  }

  /**
   * The step of F failed, and a pass of the staging cleanup with a TTL of 0 runs before the
   * recount, nothing in the buffer and nothing of X in memory. The pointer of F, which has no
   * record, keeps the manifest, the staged files and the Puffin file of X; the lag step then
   * recounts F from the Puffin file, no data file opened.
   */
  @Test
  public void cleanupBeforeRecountKeepsChangeSetOfPendingFinalSnapshot() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    String orphan = orphanPuffin();

    cleanUp(() -> false, ImmutableList.of());

    assertThat(exists(pending.manifest)).as("the manifest of X").isTrue();
    assertThat(pending.stagedFiles).as("the staged files of X").allMatch(this::exists);
    assertThat(exists(pending.puffin)).as("the Puffin file of X").isTrue();
    assertThat(exists(orphan)).as("named by nothing: the pass ran").isFalse();

    assertCommitted(cycle());

    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
    assertChangeSetGone(pending);
  }

  /**
   * The start of a drain found F, and its recount transition waits on the pool with the tail
   * waiting for it in the buffer. A pass meanwhile keeps X, and the Puffin file of the tail: the
   * recount then reads the Puffin file of X, no data file opened.
   */
  @Test
  public void cleanupWhileRecountWaitsOnPoolKeepsChangeSetAndTail() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    pool.hold();
    Envelope tail = dataWrittenAt(40L, row(5L, "e"));
    assertThat(cycle(requestOf(tail)).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    assertThat(pool.held()).as("the recount transition").isEqualTo(1);
    assertThat(pendingFinalOfState()).as("the start found F").isNotNull();
    String orphan = orphanPuffin();

    cleanUp(() -> false, ImmutableList.of(tail));

    assertThat(exists(pending.manifest)).isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).isTrue();
    String ofTheTail = ((DataWritten) tail.event().payload()).sketchFile().location();
    assertThat(exists(ofTheTail)).as("named by the response of the tail in the buffer").isTrue();
    assertThat(exists(orphan)).isFalse();

    pool.runInline();
    pool.runHeld();
    StatisticsStepInput recount = onlyRecountOf(pending);
    assertThat(dataFilesOpenedBy(recount)).isEmpty();
    assertRecountedNdv(pending);
  }

  /**
   * The coordinator stops while the transition recounts F. Its own cleanup deletes nothing after
   * the flag, and X stays whole: the next coordinator finds F again, a pass of its cleanup keeps X,
   * and its first slice recounts F from the Puffin file of X.
   */
  @Test
  public void coordinatorStoppingDuringRecountLeavesChangeSetToNextOne() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    CountDownLatch recounting = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    step.script(
        isRecountOf(pending),
        input -> {
          recounting.countDown();
          await(release, "the coordinator stops");
          return failed("the recount fails");
        });
    pool.hold();
    assertThat(cycle(requestOf(dataWrittenAt(40L, row(5L, "e")))).get(written()).outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);
    Thread transition = pool.startHeld();
    await(recounting, "the recount runs");
    driver.stop();
    release.countDown();
    join(transition);
    String orphan = orphanPuffin();

    cleanUp(() -> true, ImmutableList.of());

    assertThat(exists(orphan)).as("no delete after the flag of the stopping coordinator").isTrue();
    assertThat(exists(pending.manifest)).isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).isTrue();

    restart();
    step.scripts.clear();
    pool.hold();
    Record[] changes = {insert(6L, "f")};
    assertThat(
            cycle(requestOf(rowChangesAt(50L, stagedFiles(changes), sketchOf(changes))))
                .get(written())
                .outcome())
        .isEqualTo(TableCommitter.Outcome.PENDING);

    cleanUp(() -> false, ImmutableList.of());

    assertThat(exists(orphan)).as("the pass of the next coordinator ran").isFalse();
    assertThat(exists(pending.manifest)).isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).isTrue();

    pool.runInline();
    pool.runHeld();
    List<StatisticsStepInput> recounts = recountsOf(pending);
    assertThat(recounts).as("the stopped one and the one of the first slice").hasSize(2);
    assertThat(dataFilesOpenedBy(recounts.get(1))).isEmpty();
    assertRecountedNdv(pending);
  }

  /**
   * The recount wrote the record of F but could not delete X. Nothing reaches X any more, and a
   * pass with a TTL of 0 deletes it.
   */
  @Test
  public void changeSetRecountCouldNotDeleteGoesByTtlOnceFHasItsRecord() {
    baseline();
    Pending pending = drainWithStepOfF(Outcome.FAILED);
    deletesFail.set(true);
    assertCommitted(cycle());
    deletesFail.set(false);
    assertThat(recordOf(pending.snapshotId)).as("the record of F").isNotNull();
    assertThat(exists(pending.manifest)).as("the recount could not delete X").isTrue();

    cleanUp(() -> false, ImmutableList.of());

    assertChangeSetGone(pending);
  }

  /** A first drain whose final snapshot F0 gets the NDV of its inserts: the base of the tests. */
  private void baseline() {
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(driver.commit(requestOf(rowChangesAt(10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(recordOf(finalSnapshot().snapshotId())).as("the base").isNotNull();
    log.clear();
  }

  /**
   * A drain X of two slices whose step of the final snapshot F ends with {@code outcome}: X stays
   * for the recount. Driven through its cycle only.
   */
  private Pending drainWithStepOfF(Outcome outcome) {
    return drainWithStepOfF(
        input ->
            new Result(
                outcome,
                outcome == Outcome.FAILED
                    ? new IllegalStateException("the step of the final snapshot fails")
                    : null));
  }

  private Pending drainWithStepOfF(Function<StatisticsStepInput, Result> stepOfF) {
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c")};
    List<StagedChangeFile> files = stagedFiles(changes);
    SketchFile puffin = sketchOf(changes);
    Envelope envelope = rowChangesAt(20L, files, puffin);
    TableCommitRequest request = requestOf(envelope);
    AtomicBoolean once = new AtomicBoolean(true);
    step.script(
        input -> input.commitId().equals(request.commitId()) && once.getAndSet(false), stepOfF);

    driver.commitDrain(request);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1_000_000L);

    Snapshot last = finalSnapshot();
    Pending pending =
        new Pending(
            last.snapshotId(),
            changeSetOf(last),
            request.commitId(),
            envelope,
            files.stream().map(StagedChangeFile::location).collect(Collectors.toList()),
            puffin.location(),
            manifestOf(changeSetOf(last)));
    assertThat(once).as("the step of F").isFalse();
    assertThat(recordOf(pending.snapshotId)).isNull();
    assertThat(exists(pending.manifest)).as("X is kept for the recount").isTrue();
    assertThat(pending.stagedFiles).allMatch(this::exists);
    assertThat(exists(pending.puffin)).isTrue();
    log.clear();
    return pending;
  }

  private Pending drainWithUnknownFinalSlice() {
    return drainWithLostFinalSlice(loseFinalAnswer);
  }

  private Pending drainWithFinalSliceWhoseLookupFails() {
    return drainWithLostFinalSlice(failLookupOfFinal);
  }

  private Pending drainWithLostFinalSlice(AtomicBoolean fault) {
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c")};
    List<StagedChangeFile> files = stagedFiles(changes);
    SketchFile puffin = sketchOf(changes);
    Envelope envelope = rowChangesAt(20L, files, puffin);
    TableCommitRequest request = requestOf(envelope);
    fault.set(true);
    int steps = step.inputs.size();

    driver.commitDrain(request);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1_000_000L);

    assertThat(fault).as("the final slice landed with the fault of the test").isFalse();
    assertThat(lookupFails).as("its snapshot was looked up").isFalse();
    assertThat(step.inputs).as("no step of F").hasSize(steps);
    Snapshot last = finalSnapshot();
    log.clear();
    return new Pending(
        last.snapshotId(),
        changeSetOf(last),
        request.commitId(),
        envelope,
        files.stream().map(StagedChangeFile::location).collect(Collectors.toList()),
        puffin.location(),
        manifestOf(changeSetOf(last)));
  }

  private void assertRecountedNdv(Pending pending) {
    assertThat(recordOf(pending.snapshotId)).as("the record of F").isNotNull();
    NdvBounds.of(table, pending.snapshotId, "id")
        .base(List.of(1L, 2L))
        .written(1L, 3L)
        .checkRecorded(LG_K);
    NdvBounds.of(table, pending.snapshotId, "data")
        .base(List.of("a", "b"))
        .written("a1", "c")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  private void assertChangeSetGone(Pending pending) {
    assertThat(exists(pending.manifest)).as("the manifest of X").isFalse();
    assertThat(pending.stagedFiles).as("the staged files of X").noneMatch(this::exists);
    assertThat(exists(pending.puffin)).as("the Puffin file of X").isFalse();
  }

  private void assertLost(Pending pending, String failure) {
    assertThat(lines("WARN", "is lost"))
        .singleElement()
        .satisfies(
            line -> {
              assertThat(line.message)
                  .contains(pending.changeSetId.toString())
                  .contains(tbl().toString())
                  .contains(String.valueOf(pending.snapshotId));
              assertThat(line.throwable).hasMessage(failure);
            });
  }

  private static final class Pending {
    private final long snapshotId;
    private final UUID changeSetId;
    // the commit id of the drain of X
    private final UUID commitId;
    private final Envelope envelope;
    private final List<String> stagedFiles;
    private final String puffin;
    private final String manifest;

    private Pending(
        long snapshotId,
        UUID changeSetId,
        UUID commitId,
        Envelope envelope,
        List<String> stagedFiles,
        String puffin,
        String manifest) {
      this.snapshotId = snapshotId;
      this.changeSetId = changeSetId;
      this.commitId = commitId;
      this.envelope = envelope;
      this.stagedFiles = stagedFiles;
      this.puffin = puffin;
      this.manifest = manifest;
    }
  }

  private static Predicate<StatisticsStepInput> isRecountOf(Pending pending) {
    return input ->
        Long.valueOf(pending.snapshotId).equals(input.snapshotId())
            && !pending.commitId.equals(input.commitId());
  }

  private List<StatisticsStepInput> recountsOf(Pending pending) {
    return step.inputs.stream().filter(isRecountOf(pending)).collect(Collectors.toList());
  }

  private StatisticsStepInput onlyRecountOf(Pending pending) {
    List<StatisticsStepInput> recounts = recountsOf(pending);
    assertThat(recounts).as("the recount of F").hasSize(1);
    StatisticsStepInput recount = recounts.get(0);
    assertThat(recount.identifier()).isEqualTo(tbl());
    assertThat(recount.ownSnapshotIds()).containsExactly(pending.snapshotId);
    assertThat(recount.ownChangeSetIds()).containsExactly(pending.changeSetId.toString());
    assertThat(recount.answers())
        .allSatisfy(answer -> assertThat(answer.snapshotId()).isEqualTo(pending.snapshotId));
    return recount;
  }

  private StatisticsStepInput stepOf(long snapshotId) {
    List<StatisticsStepInput> steps =
        step.inputs.stream()
            .filter(input -> Long.valueOf(snapshotId).equals(input.snapshotId()))
            .collect(Collectors.toList());
    assertThat(steps).as("the step of %s", snapshotId).hasSize(1);
    return steps.get(0);
  }

  private List<String> dataFilesOpenedBy(StatisticsStepInput input) {
    List<String> opened = openedBy.get(input);
    assertThat(opened).as("the real step ran").isNotNull();
    String data = table.location() + "/data/";
    return opened.stream().filter(path -> path.startsWith(data)).collect(Collectors.toList());
  }

  private void newDriver() {
    driver =
        new CopyOnWriteRewriteDriver(
            faulty,
            config,
            MetadataEvents.NOOP,
            event -> {},
            1,
            health,
            step,
            pool,
            recordingLogger());
  }

  private void restart() {
    driver.stop();
    newDriver();
  }

  private CopyOnWriteTableCommitter committer() {
    return driver.committer();
  }

  /**
   * One cycle as {@code Coordinator.doCommit} runs it: {@code pendingTables()}, the tables of the
   * cycle, and {@code commit()} of each with its request of {@code buffered}, or one without
   * responses.
   */
  private Map<TableReference, CommitResult> cycle(TableCommitRequest... buffered) {
    Map<TableReference, TableCommitRequest> requests = Maps.newLinkedHashMap();
    for (TableCommitRequest request : buffered) {
      requests.put(request.tableReference(), request);
    }

    Set<TableReference> pending = committer().pendingTables();
    Map<TableReference, CommitResult> results = Maps.newLinkedHashMap();
    for (TableReference reference : cycleTables(requests.keySet(), pending)) {
      TableCommitRequest request =
          requests.containsKey(reference) ? requests.get(reference) : withoutResponses(reference);
      offered.add(request);
      results.put(reference, committer().commit(request));
    }

    return results;
  }

  private static void assertCommitted(Map<TableReference, CommitResult> results) {
    assertThat(results).isNotEmpty();
    assertThat(results)
        .allSatisfy(
            (reference, result) ->
                assertThat(result.outcome())
                    .as("the answer for %s", reference)
                    .isEqualTo(TableCommitter.Outcome.COMMITTED));
  }

  private TableCommitRequest offeredWithoutUuid() {
    List<TableCommitRequest> withoutUuid =
        offered.stream()
            .filter(request -> request.tableReference().uuid() == null)
            .collect(Collectors.toList());
    assertThat(withoutUuid).isNotEmpty();
    return withoutUuid.get(withoutUuid.size() - 1);
  }

  @SuppressWarnings("unchecked")
  private static Set<TableReference> cycleTables(
      Set<TableReference> buffered, Set<TableReference> pending) {
    try {
      Method method = Coordinator.class.getDeclaredMethod("cycleTables", Set.class, Set.class);
      method.setAccessible(true);
      return (Set<TableReference>) method.invoke(null, buffered, pending);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private StatisticsLag lag() {
    return (StatisticsLag) field(committer(), "statisticsLag");
  }

  @SuppressWarnings("unchecked")
  private Map<TableReference, TableDrainState> states() {
    return (Map<TableReference, TableDrainState>) field(field(committer(), "states"), "states");
  }

  private UUID refusedMark() {
    return inState(state -> state.refusedFinalChangeSetId);
  }

  private String normalizedLocationOfState() {
    return inState(state -> state.normalizedLocation);
  }

  private TableDrainState.PendingFinal pendingFinalOfState() {
    return inState(state -> state.pendingFinal);
  }

  private <T> T inState(Function<TableDrainState, T> read) {
    TableDrainState state = states().get(written());
    assertThat(state).as("the state of the table").isNotNull();
    state.lock.lock();
    try {
      return read.apply(state);
    } finally {
      state.lock.unlock();
    }
  }

  private static Object field(Object target, String name) {
    try {
      Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static TableIdentifier tbl() {
    return TABLE_IDENTIFIER;
  }

  private TableReference written() {
    return TableReference.of(catalog.name(), TABLE_IDENTIFIER, table.uuid());
  }

  private long headId() {
    table.refresh();
    return table.currentSnapshot().snapshotId();
  }

  private Snapshot finalSnapshot() {
    table.refresh();
    List<Snapshot> finals = Lists.newArrayList();
    table
        .snapshots()
        .forEach(
            snapshot -> {
              if (snapshot.summary().containsKey(COMMIT_ID_PROP)
                  && snapshot.summary().containsKey(FINAL_CHANGE_SET_ID_PROP)) {
                finals.add(snapshot);
              }
            });
    assertThat(finals).as("a drain ended").isNotEmpty();
    return finals.get(finals.size() - 1);
  }

  private static UUID changeSetOf(Snapshot snapshot) {
    return UUID.fromString(snapshot.summary().get(FINAL_CHANGE_SET_ID_PROP));
  }

  private StatisticsFile recordOf(long snapshotId) {
    table.refresh();
    return table.statisticsFiles().stream()
        .filter(file -> file.snapshotId() == snapshotId)
        .findFirst()
        .orElse(null);
  }

  private String manifestOf(UUID changeSetId) {
    return ChangeSetManifest.location(
        StagedChangeFileWriter.stagingLocation(table, null), written(), GROUP_ID, changeSetId);
  }

  private void cleanUp(BooleanSupplier stopped, List<Envelope> buffered) {
    when(config.stagingOrphanTtlMs()).thenReturn(0L);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(0L);
    InMemoryStagingCleanup.over(config, committer().stagingCleanupSource(), stopped)
        .maybeClean(System.currentTimeMillis(), () -> buffered);
  }

  private String orphanPuffin() {
    String location = stagingDirectory() + "/stats/" + UUID.randomUUID() + "-task-9.puffin";
    ((InMemoryFileIO) table.io()).addFile(location, new byte[] {1});
    return location;
  }

  private String stagingDirectory() {
    return StagedChangeFileWriter.stagingDirectory(
        StagedChangeFileWriter.stagingLocation(table, null), written(), GROUP_ID);
  }

  private void rewriteManifest(Pending pending, Consumer<ObjectNode> change) throws IOException {
    ObjectNode root;
    try (InputStream in = table.io().newInputFile(pending.manifest).newStream()) {
      root = (ObjectNode) MAPPER.readTree(in);
    }

    change.accept(root);
    try (OutputStream out = table.io().newOutputFile(pending.manifest).createOrOverwrite()) {
      MAPPER.writeValue(out, root);
    }
  }

  private void write(String location, String content) {
    try (OutputStream out = table.io().newOutputFile(location).createOrOverwrite()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean exists(String location) {
    return ((InMemoryFileIO) table.io()).fileExists(location);
  }

  private TableCommitRequest requestOf(Envelope... envelopes) {
    long next = Arrays.stream(envelopes).mapToLong(Envelope::offset).max().orElse(0L) + 1;
    return new TableCommitRequest(
        written(),
        ImmutableList.copyOf(envelopes),
        ImmutableMap.of(0, next),
        UUID.randomUUID(),
        OffsetDateTime.now(),
        ACTIVE_TASKS);
  }

  private static TableCommitRequest withoutResponses(TableReference reference) {
    return new TableCommitRequest(
        reference,
        ImmutableList.of(),
        ImmutableMap.of(),
        UUID.randomUUID(),
        OffsetDateTime.now(),
        ACTIVE_TASKS);
  }

  private Envelope rowChangesAt(long offset, List<StagedChangeFile> files) {
    return rowChangesAt(offset, files, null);
  }

  private Envelope rowChangesAt(long offset, List<StagedChangeFile> files, SketchFile sketchFile) {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), written(), "task-0", List.of(SRC_TOPIC), files, sketchFile);
    return new Envelope(new Event(GROUP_ID, payload), 0, offset);
  }

  private Envelope dataWrittenAt(long offset, Record... rows) {
    DataWritten payload =
        new DataWritten(
            table.spec().partitionType(),
            UUID.randomUUID(),
            written(),
            writeRows(rows),
            List.of(),
            List.of(SRC_TOPIC),
            sketchOfRows(rows));
    return new Envelope(new Event(GROUP_ID, payload), 0, offset);
  }

  private List<StagedChangeFile> stagedFiles(Record... changes) {
    return stagedFiles(ID_FIELDS, changes);
  }

  private List<StagedChangeFile> stagedFiles(Set<Integer> idFields, Record... changes) {
    table.refresh();
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, written(), idFields, null, GROUP_ID, "task-0");
    long offset = 0;
    for (Record change : changes) {
      int op = (int) change.getField("op");
      writer.write(writer.stagedRow(tableRow(change), op, SRC_TOPIC, 0, offset++));
    }

    return writer.complete();
  }

  /**
   * The intermediate Puffin file a worker writes along with {@code changes}: the values of every
   * change but a delete, which stages a key and no value.
   */
  private SketchFile sketchOf(Record... changes) {
    List<Record> rows = Lists.newArrayList();
    for (Record change : changes) {
      if ((int) change.getField("op") != StagedChangeSchema.OP_DELETE) {
        rows.add(tableRow(change));
      }
    }

    return sketchOfRows(rows.toArray(new Record[0]));
  }

  private SketchFile sketchOfRows(Record... rows) {
    table.refresh();
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(
            TABLE_IDENTIFIER,
            table,
            stagingDirectory(),
            "task-0",
            LG_K,
            null,
            false,
            table.schema());
    for (Record row : rows) {
      sketches.update(row);
    }

    SketchFile sketchFile = sketches.write(UUID.randomUUID());
    assertThat(sketchFile).as("the Puffin file of the worker").isNotNull();
    return sketchFile;
  }

  private static Record tableRow(Record change) {
    GenericRecord tableRow = GenericRecord.create(SCHEMA);
    tableRow.setField("id", change.getField("id"));
    tableRow.setField("data", change.getField("data"));
    return tableRow;
  }

  private static Record change(long id, String data, int op) {
    GenericRecord change = GenericRecord.create(CHANGE);
    change.setField("id", id);
    change.setField("data", data);
    change.setField("op", op);
    return change;
  }

  private static Record insert(long id, String data) {
    return change(id, data, StagedChangeSchema.OP_INSERT);
  }

  private static Record update(long id, String data) {
    return change(id, data, StagedChangeSchema.OP_UPDATE);
  }

  private void appendRows(Record... rows) {
    table.refresh();
    AppendFiles append = table.newAppend();
    writeRows(rows).forEach(append::appendFile);
    append.commit();
  }

  private List<DataFile> writeRows(Record... rows) {
    table.refresh();
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).build(),
            OutputFileFactory.builderFor(table, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            table.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(table.spec(), table.schema());
        key.partition(row);
        writer.write(row, table.spec(), key);
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

  private static Record row(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private List<org.assertj.core.groups.Tuple> readAll() {
    table.refresh();
    List<org.assertj.core.groups.Tuple> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(tuple(record.get(0, Object.class), record.get(1, Object.class)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return rows;
  }

  private static TableSinkConfig statistics(boolean enabled) {
    return new TableSinkConfig(
        null, ImmutableList.of(), ImmutableList.of(), null, enabled, false, null);
  }

  private static Result failed(String message) {
    return new Result(Outcome.FAILED, new IllegalStateException(message));
  }

  private static void await(CountDownLatch latch, String what) {
    try {
      assertThat(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS)).as(what).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(what, e);
    }
  }

  private static void awaitBlocked(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
    while (thread.getState() != Thread.State.WAITING) {
      assertThat(System.nanoTime()).as("%s waits for the lock", thread).isLessThan(deadline);
      Thread.sleep(5);
    }
  }

  private static void join(Thread thread) {
    try {
      thread.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }

    assertThat(thread.isAlive()).as("%s is over", thread.getName()).isFalse();
  }

  private static final class LogLine {
    private final String level;
    private final String message;
    private final Throwable throwable;

    private LogLine(String level, String message, Throwable throwable) {
      this.level = level;
      this.message = message;
      this.throwable = throwable;
    }

    @Override
    public String toString() {
      return level + " " + message;
    }
  }

  private List<LogLine> lines(String level, String text) {
    return log.stream()
        .filter(line -> line.level.equals(level) && line.message.contains(text))
        .collect(Collectors.toList());
  }

  private Logger recordingLogger() {
    return (Logger)
        Proxy.newProxyInstance(
            Logger.class.getClassLoader(),
            new Class<?>[] {Logger.class},
            (proxy, method, args) -> {
              String name = method.getName();
              if (LEVELS.contains(name)
                  && args != null
                  && args.length > 0
                  && args[0] instanceof String) {
                Object[] formatArgs =
                    args.length == 2 && args[1] instanceof Object[]
                        ? (Object[]) args[1]
                        : Arrays.copyOfRange(args, 1, args.length);
                FormattingTuple formatted =
                    MessageFormatter.arrayFormat((String) args[0], formatArgs);
                log.add(
                    new LogLine(
                        name.toUpperCase(Locale.ROOT),
                        formatted.getMessage(),
                        formatted.getThrowable()));
                if (failLostWarning.get() && formatted.getMessage().contains("is lost")) {
                  throw new IllegalStateException("the log fails");
                }

                return null;
              }

              switch (name) {
                case "getName":
                  return "recording";
                case "toString":
                  return "the recording log";
                case "hashCode":
                  return System.identityHashCode(proxy);
                case "equals":
                  return proxy == args[0];
                default:
                  return method.getReturnType() == boolean.class ? Boolean.TRUE : null;
              }
            });
  }

  private final class ScriptedStep extends StatisticsStep {
    private final StatisticsStep real = new StatisticsStep();
    private final List<StatisticsStepInput> inputs = Lists.newCopyOnWriteArrayList();
    private final StepOverlaps overlaps = new StepOverlaps();
    private final List<
            Map.Entry<Predicate<StatisticsStepInput>, Function<StatisticsStepInput, Result>>>
        scripts = Lists.newCopyOnWriteArrayList();

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      return overlaps.track(input, () -> scripted(input));
    }

    private Result scripted(StatisticsStepInput input) {
      for (Map.Entry<Predicate<StatisticsStepInput>, Function<StatisticsStepInput, Result>> script :
          scripts) {
        if (script.getKey().test(input)) {
          return script.getValue().apply(input);
        }
      }

      return realRun(input);
    }

    void script(Predicate<StatisticsStepInput> when, Function<StatisticsStepInput, Result> then) {
      scripts.add(Maps.immutableEntry(when, then));
    }

    Result realRun(StatisticsStepInput input) {
      List<String> opened = Lists.newCopyOnWriteArrayList();
      openedBy.put(input, opened);
      opening.set(opened);
      try {
        return real.run(input);
      } finally {
        opening.remove();
      }
    }

    /** The lag steps on the head: S is the head. */
    List<StatisticsStepInput> headSteps() {
      return inputs.stream()
          .filter(input -> input.snapshotId() == null)
          .collect(Collectors.toList());
    }
  }

  /**
   * The catalog of the committer: hands out the tables of {@code delegate} over {@link
   * FaultyOperations}, whose refresh fails once after the answer of a final slice is lost.
   */
  private final class FaultyCatalog implements Catalog {
    private final Catalog delegate;

    private FaultyCatalog(Catalog delegate) {
      this.delegate = delegate;
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
      Table loaded = delegate.loadTable(identifier);
      return new BaseTable(
          new FaultyOperations(((HasTableOperations) loaded).operations()), loaded.name()) {
        @Override
        public void refresh() {
          super.refresh();
          if (lookupFails.getAndSet(false)) {
            throw new UncheckedIOException(new IOException("catalog unreachable"));
          }
        }
      };
    }
  }

  /**
   * Operations over {@link FaultyIO}, whose commit of a final slice lands without an answer when
   * the test says so.
   */
  private final class FaultyOperations implements TableOperations {
    private TableOperations delegate;

    private FaultyOperations(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      if (refreshByName.get()) {
        delegate = ((HasTableOperations) catalog.loadTable(TABLE_IDENTIFIER)).operations();
        return delegate.current();
      }

      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
      Snapshot head = metadata.currentSnapshot();
      // the final slice of a drain: the connector's commit without the pointer of an unfinished one
      boolean finalSlice =
          head != null
              && head.summary().containsKey(COMMIT_ID_PROP)
              && !head.summary().containsKey(CHANGE_SET_ID_PROP);
      if (finalSlice && loseFinalAnswer.getAndSet(false)) {
        lookupFails.set(true);
        throw new CommitStateUnknownException(
            new IOException("connection reset after the commit was sent"));
      }

      if (finalSlice && failLookupOfFinal.getAndSet(false)) {
        lookupFails.set(true);
      }
    }

    @Override
    public FileIO io() {
      return new FaultyIO(delegate.io());
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
   * Storage the test fails and watches: a read of a location of {@link #readFailures} fails once, a
   * delete under the staging directory fails while {@link #deletesFail}, and what the step being
   * run opens is recorded.
   */
  private final class FaultyIO implements FileIO {
    private final FileIO delegate;

    private FaultyIO(FileIO delegate) {
      this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
      opened(path);
      return served(path, delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      opened(path);
      return served(path, delegate.newInputFile(path, length));
    }

    private InputFile served(String path, InputFile file) {
      EndlessAvroFile endlessFile = endless;
      return endlessFile == null ? file : endlessFile.serve(path, file);
    }

    private void opened(String path) {
      RuntimeException failure = readFailures.remove(path);
      if (failure != null) {
        onReadFailure.run();
        throw failure;
      }

      List<String> opened = opening.get();
      if (opened != null) {
        opened.add(path);
      }
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      if (deletesFail.get() && path.startsWith(stagingDirectory())) {
        throw new UncheckedIOException(new IOException("delete denied: " + path));
      }

      delegate.deleteFile(path);
    }
  }
}
