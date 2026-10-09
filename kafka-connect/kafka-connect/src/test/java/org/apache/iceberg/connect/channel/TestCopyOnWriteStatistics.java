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

import static org.apache.iceberg.connect.StatisticsAssertions.assertNoStrayPuffins;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsFilesExist;
import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.RowLevelOperationMode;
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
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.NdvBounds;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.connect.data.NdvChain;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
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
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

/**
 * Copy-on-write with statistics: the Puffin files and adopted change sets in the manifest, the
 * statistics marker and the final change set pointer on the slices, the merge-on-read tail with its
 * own step, and the step of the final snapshot F of a drain in {@code finishSlice}.
 *
 * <p>Driven through {@link CopyOnWriteRewriteDriver}. The statistics step is {@link ScriptedStep};
 * the NDV tests hand it the real step and play the workers, writing the intermediate Puffin file of
 * the changes they stage.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class TestCopyOnWriteStatistics {
  private static final String CTL_TOPIC = "ctl-topic";
  private static final String GROUP_ID = "cg-connect";
  private static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  private static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";
  private static final String CURSOR_PROP = "kafka.connect.copy-on-write.change-set-cursor";
  private static final String FINAL_CHANGE_SET_ID_PROP =
      "kafka.connect.copy-on-write.final-change-set-id";
  private static final String OFFSETS_PROP = "kafka.connect.offsets." + CTL_TOPIC + "." + GROUP_ID;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final TableIdentifier OTHER_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl2");
  private static final TableReference OTHER_REFERENCE =
      TableReference.of("catalog", OTHER_IDENTIFIER);
  private static final int LG_K = 12;
  private static final long AWAIT_SECONDS = 10L;
  private static final Predicate<Map<String, String>> FINAL_SLICE =
      summary -> summary.containsKey(COMMIT_ID_PROP) && !summary.containsKey(CHANGE_SET_ID_PROP);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);

  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private TableSinkConfig tableConfig;
  private ScriptedStep step;
  private CopyOnWriteRewriteDriver committer;
  private final List<Event> sentEvents = Lists.newCopyOnWriteArrayList();
  private final List<String> puffins = Lists.newArrayList();
  private volatile Consumer<Event> onEvent = event -> {};

  private static final class ScriptedStep extends StatisticsStep {
    private final List<StatisticsStepInput> inputs = Lists.newCopyOnWriteArrayList();
    private final StepOverlaps overlaps = new StepOverlaps();
    private volatile Function<StatisticsStepInput, Result> behavior =
        input -> new Result(Outcome.NOT_NEEDED, null);

    @Override
    public Result run(StatisticsStepInput input) {
      inputs.add(input);
      return overlaps.track(input, () -> behavior.apply(input));
    }
  }

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());

    tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.statisticsEnabled()).thenReturn(true);
    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn(GROUP_ID);
    when(config.tableConfig(any())).thenReturn(tableConfig);
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

    step = new ScriptedStep();
    committer = newDriver();
  }

  @AfterEach
  public void after() throws IOException {
    committer.stop();
    assertStatisticsOnOwnSnapshot(table);
    catalog.close();
    assertThat(step.overlaps.overlaps()).as("two steps of one (table, S) at once").isEmpty();
  }

  @Test
  public void everySliceCarriesMarkerAndOnlyFinalOnePointerToItsChangeSet() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    assertThat(
            committer.commit(
                requestOf(
                    rowChangesAt(
                        0,
                        10L,
                        stagedFiles(update(1L, "a1"), update(2L, "b2"), update(3L, "c3"))))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    List<Snapshot> slices = connectorSnapshots();
    assertThat(slices).hasSize(3);
    assertThat(slices)
        .allSatisfy(
            slice ->
                assertThat(slice.summary())
                    .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true"));
    String changeSetId = slices.get(0).summary().get(CHANGE_SET_ID_PROP);
    assertThat(changeSetId).isNotNull();
    assertThat(slices.subList(0, 2))
        .allSatisfy(
            slice ->
                assertThat(slice.summary())
                    .containsEntry(CHANGE_SET_ID_PROP, changeSetId)
                    .doesNotContainKey(FINAL_CHANGE_SET_ID_PROP));
    assertThat(slices.get(2).summary())
        .containsEntry(FINAL_CHANGE_SET_ID_PROP, changeSetId)
        .doesNotContainKey(CHANGE_SET_ID_PROP);
  }

  @Test
  public void drainOfOneSliceMarksItAsFinalOneOnEmptyTable() {
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(insert(1L, "a"), insert(2L, "b"))))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    Snapshot only = onlySnapshot(connectorSnapshots());
    assertThat(only.summary())
        .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true")
        .containsKey(FINAL_CHANGE_SET_ID_PROP);
    assertThat(UUID.fromString(only.summary().get(FINAL_CHANGE_SET_ID_PROP))).isNotNull();
  }

  /** Without the statistics of the table the summaries are the ones of the released version. */
  @Test
  public void withStatisticsOfTableOffNoSliceCarriesMarkerOrPointer() {
    when(tableConfig.statisticsEnabled()).thenReturn(false);
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);

    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(update(1L, "a1"), update(2L, "b2"))))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    assertThat(connectorSnapshots())
        .hasSize(2)
        .allSatisfy(
            slice ->
                assertThat(slice.summary())
                    .doesNotContainKeys(
                        NdvChain.STATISTICS_ENABLED_PROP, FINAL_CHANGE_SET_ID_PROP));
    assertThat(step.inputs).isEmpty();
  }

  /**
   * The statistics turned off by a restart mid-drain leave the slices after it without the marker,
   * and the final one without the pointer.
   */
  @Test
  public void markerIsTakenForEverySliceSoRestartWithStatisticsOffDropsIt() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    committer.commitOneSlice(
        requestOf(
            rowChangesAt(
                0, 10L, stagedFiles(update(1L, "a1"), update(2L, "b2"), update(3L, "c3")))));

    when(tableConfig.statisticsEnabled()).thenReturn(false);
    committer.stop();
    committer = restartedDriver();
    table.refresh();

    assertThat(readAll())
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"), tuple(3L, "c3"));
    List<Snapshot> slices = connectorSnapshots();
    assertThat(slices).hasSize(3);
    assertThat(slices.get(0).summary()).containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true");
    assertThat(slices.subList(1, 3))
        .allSatisfy(
            slice ->
                assertThat(slice.summary())
                    .doesNotContainKeys(
                        NdvChain.STATISTICS_ENABLED_PROP, FINAL_CHANGE_SET_ID_PROP));
  }

  /**
   * The statistics turned off by a restart mid-drain leave the final snapshot without a step, and
   * so without anyone else to delete the Puffin files the manifest names: the end of the change set
   * deletes them with its staged files.
   */
  @Test
  public void drainEndingWithStatisticsOffDeletesPuffinFilesOfItsManifest() {
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c")};
    List<StagedChangeFile> files = stagedFiles(changes);
    SketchFile puffin = sketchOf(changes);
    committer.commitOneSlice(requestOf(rowChangesAt(0, 20L, files, puffin)));
    assertThat(exists(puffin.location())).isTrue();

    when(tableConfig.statisticsEnabled()).thenReturn(false);
    committer.stop();
    committer = restartedDriver();
    table.refresh();

    assertThat(exists(puffin.location())).as("the Puffin file of the change set").isFalse();
    assertThat(readAll())
        .containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b"), tuple(3L, "c"));
    assertThat(locationsOf(files)).noneMatch(this::exists);
    assertThat(step.inputs).isEmpty();
  }

  @Test
  public void tailCarriesMarkerAndItsOwnStepAsSlicesCarryTheirs() {
    appendRows(row(1L, "a"), row(2L, "b"));
    Envelope tail = dataWrittenAt(0, 1L, null, row(3L, "c"));
    Envelope rowChanges = rowChangesAt(0, 2L, stagedFiles(update(2L, "b2")));

    assertThat(committer.commit(requestOf(List.of(tail, rowChanges), ImmutableMap.of(0, 3L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();

    List<Snapshot> snapshots = connectorSnapshots();
    assertThat(snapshots).hasSize(2);
    Snapshot tailSnapshot = snapshots.get(0);
    assertThat(tailSnapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(tailSnapshot.summary())
        .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true")
        .doesNotContainKey(FINAL_CHANGE_SET_ID_PROP);
    assertThat(snapshots.get(1).summary())
        .containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true")
        .containsKey(FINAL_CHANGE_SET_ID_PROP);
    // the step of the tail is of its own snapshot T, on the pool of copy-on-write; the step of the
    // drain's final snapshot after it
    assertThat(step.inputs)
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(tailSnapshot.snapshotId(), snapshots.get(1).snapshotId());
    assertThat(step.inputs.get(0).keepSketchesOnFailure()).isFalse();
  }

  /**
   * A failed step of the tail, or its snapshot gone, puts the table into the lag of copy-on-write,
   * which the tail shares (the test checks the lag itself rather than {@code pendingTables()}).
   */
  @ParameterizedTest
  @EnumSource(
      value = StatisticsStep.Outcome.class,
      names = {"FAILED", "SNAPSHOT_GONE"})
  public void stepOfTailThatDidNotWriteEntersLagOfCopyOnWrite(StatisticsStep.Outcome outcome) {
    step.behavior =
        input ->
            new StatisticsStep.Result(
                outcome,
                outcome == StatisticsStep.Outcome.FAILED
                    ? new IllegalStateException("the step of the tail fails")
                    : null);
    StatisticsLag lag = lagOf(committer);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isFalse();

    assertThat(
            committer.commit(
                requestOf(
                    List.of(dataWrittenAt(0, 1L, null, row(1L, "a"))), ImmutableMap.of(0, 2L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    assertThat(step.inputs).hasSize(1);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
  }

  @Test
  public void writtenStepOfTailLeavesLagOfCopyOnWriteAsItIs() {
    step.behavior = input -> new StatisticsStep.Result(StatisticsStep.Outcome.WRITTEN, null);
    StatisticsLag lag = lagOf(committer);
    lag.enter(TABLE_IDENTIFIER);
    long generation = lag.token(TABLE_IDENTIFIER);

    committer.commit(
        requestOf(List.of(dataWrittenAt(0, 1L, null, row(1L, "a"))), ImmutableMap.of(0, 2L)));

    assertThat(step.inputs).hasSize(1);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(lag.token(TABLE_IDENTIFIER)).isEqualTo(generation);
  }

  /**
   * While the step of the tail runs, the handoff tells it and its Puffin files, which the staging
   * cleanup must keep; once it ends, neither.
   */
  @Test
  public void handoffTellsStepOfTailAndItsPuffinFilesWhileItRuns() {
    MergeOnReadTailHandoff handoff = handoffOf(committer);
    String puffin = table.location() + "/kc-staging/puffin-" + UUID.randomUUID() + ".stats";
    AtomicReference<Boolean> hasStepWhileRunning = new AtomicReference<>();
    AtomicReference<Set<String>> locationsWhileRunning = new AtomicReference<>();
    step.behavior =
        input -> {
          hasStepWhileRunning.set(handoff.hasStep(TABLE_REFERENCE));
          locationsWhileRunning.set(handoff.inFlightSketchLocations(TABLE_REFERENCE));
          return new StatisticsStep.Result(StatisticsStep.Outcome.WRITTEN, null);
        };

    committer.commit(
        requestOf(
            List.of(dataWrittenAt(0, 1L, new SketchFile(puffin, 100L, 10L), row(1L, "a"))),
            ImmutableMap.of(0, 2L)));

    assertThat(step.inputs).hasSize(1);
    assertThat(hasStepWhileRunning.get()).isTrue();
    assertThat(locationsWhileRunning.get()).containsExactly(puffin);
    assertThat(handoff.hasStep(TABLE_REFERENCE)).isFalse();
    assertThat(handoff.inFlightSketchLocations(TABLE_REFERENCE)).isEmpty();
  }

  @Test
  public void freezeRecordsPuffinFileOfEveryResponseWithStagedFilesItCovers() throws IOException {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    List<StagedChangeFile> covered = stagedFiles(update(1L, "a1"), update(2L, "b2"));
    List<StagedChangeFile> uncovered = stagedFiles(update(3L, "c3"));
    SketchFile puffin = new SketchFile(table.location() + "/puffin-a.stats", 300L, 30L);

    committer.commitOneSlice(
        requestOf(
            List.of(rowChangesAt(0, 10L, covered, puffin), rowChangesAt(0, 11L, uncovered, null)),
            ImmutableMap.of(0, 12L)));

    JsonNode manifest = manifestPointerNames();
    assertThat(manifest.get("sketchFiles"))
        .isEqualTo(asRead(MAPPER.createArrayNode().add(sketchFileJson(puffin, covered))));
    assertThat(manifest.has("adoptedChangeSetIds")).isFalse();
  }

  /**
   * The identifier fields changed after a slice of change set D: D is dropped, and the change set N
   * adopting its files adopts its Puffin files and its id after those D adopted.
   */
  @Test
  public void droppedChangeSetHandsItsPuffinFilesAndItsIdToOneAdoptingItsFiles()
      throws IOException {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    UUID adoptedByD = UUID.randomUUID();
    List<StagedChangeFile> filesOfD = stagedFiles(update(1L, "d1"));
    SketchFile puffinOfD = new SketchFile(table.location() + "/puffin-d.stats", 200L, 20L);
    UUID changeSetD =
        dropped(
            ChangeSetManifest.CURRENT_FORMAT_VERSION,
            filesOfD,
            List.of(sketchFileJson(puffinOfD, filesOfD)),
            List.of(adoptedByD));

    List<StagedChangeFile> filesOfN = stagedFiles(update(2L, "n2"), update(3L, "n3"));
    SketchFile puffinOfN = new SketchFile(table.location() + "/puffin-n.stats", 100L, 10L);
    committer.commitOneSlice(
        requestOf(List.of(rowChangesAt(0, 10L, filesOfN, puffinOfN)), ImmutableMap.of(0, 11L)));

    JsonNode manifest = manifestPointerNames();
    assertThat(manifest.get("changeSetId").asText()).isNotEqualTo(changeSetD.toString());
    assertThat(manifest.get("adoptedChangeSetIds"))
        .isEqualTo(MAPPER.createArrayNode().add(adoptedByD.toString()).add(changeSetD.toString()));
    assertThat(manifest.get("sketchFiles"))
        .isEqualTo(
            asRead(
                MAPPER
                    .createArrayNode()
                    .add(sketchFileJson(puffinOfD, filesOfD))
                    .add(sketchFileJson(puffinOfN, filesOfN))));
  }

  /**
   * Of a dropped change set in a format this build does not know, only the id is adopted; its
   * staged files become an answer without a Puffin file.
   */
  @Test
  public void droppedChangeSetOfUnsupportedFormatHandsOnlyItsId() throws IOException {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    List<StagedChangeFile> filesOfD = stagedFiles(update(1L, "d1"));
    UUID changeSetD =
        dropped(
            ChangeSetManifest.CURRENT_FORMAT_VERSION + 1,
            filesOfD,
            List.of(
                sketchFileJson(
                    new SketchFile(table.location() + "/puffin-d.stats", 200L, 20L), filesOfD)),
            List.of(UUID.randomUUID()));

    List<StagedChangeFile> filesOfN = stagedFiles(update(2L, "n2"), update(3L, "n3"));
    committer.commitOneSlice(
        requestOf(List.of(rowChangesAt(0, 10L, filesOfN, null)), ImmutableMap.of(0, 11L)));

    JsonNode manifest = manifestPointerNames();
    assertThat(manifest.get("adoptedChangeSetIds"))
        .isEqualTo(MAPPER.createArrayNode().add(changeSetD.toString()));
    assertThat(manifest.has("sketchFiles")).isFalse();
    assertThat(manifest.get("stagedFiles").findValuesAsText("location"))
        .contains(filesOfD.get(0).location());
  }

  /**
   * A restart between the freeze of the change set N adopting a dropped D and the first slice of N:
   * D is dropped again, and the change set N' the restarted drain freezes has the adopted change
   * sets and the Puffin files N had, each entry once. N stays behind as an orphan of the staging
   * cleanup.
   */
  @Test
  public void restartBeforeFirstSliceOfAdoptingChangeSetFreezesItAgainWithoutDuplicates()
      throws IOException {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    identifierFieldsAre("id", "data");
    Set<Integer> wide = Set.of(1, 2);
    SketchFile puffinOfD = new SketchFile(table.location() + "/puffin-d.stats", 200L, 20L);
    List<StagedChangeFile> filesOfD =
        stagedFiles(table, TABLE_REFERENCE, wide, update(1L, "d1"), update(2L, "d2"));
    committer.commitOneSlice(
        requestOf(List.of(rowChangesAt(0, 10L, filesOfD, puffinOfD)), ImmutableMap.of(0, 11L)));
    String changeSetD = lastConnectorSnapshot().summary().get(CHANGE_SET_ID_PROP);
    assertThat(changeSetD).as("the first slice of D landed").isNotNull();
    identifierFieldsAre("id");
    restart();

    SketchFile puffinOfN = new SketchFile(table.location() + "/puffin-n.stats", 100L, 10L);
    List<StagedChangeFile> filesOfN = stagedFiles(table, TABLE_REFERENCE, wide, insert(4L, "n4"));
    Envelope responseOfN = rowChangesAt(0, 20L, filesOfN, puffinOfN);
    List<RewriteAssigned> handedOutForN =
        committer.handOutOneSlice(requestOf(List.of(responseOfN), ImmutableMap.of(0, 21L)));
    assertThat(handedOutForN).hasSize(1);
    assertThat(lastConnectorSnapshot().summary().get(CHANGE_SET_ID_PROP))
        .as("N has no slice yet")
        .isEqualTo(changeSetD);
    JsonNode manifestOfN = manifestOf(handedOutForN.get(0));

    restart();
    List<RewriteAssigned> handedOutForNAgain =
        committer.handOutOneSlice(requestOf(List.of(responseOfN), ImmutableMap.of(0, 21L)));

    assertThat(handedOutForNAgain).hasSize(1);
    assertThat(handedOutForNAgain.get(0).changeSetId())
        .isNotEqualTo(handedOutForN.get(0).changeSetId());
    JsonNode manifestOfNAgain = manifestOf(handedOutForNAgain.get(0));
    assertThat(manifestOfN.get("adoptedChangeSetIds"))
        .isEqualTo(MAPPER.createArrayNode().add(changeSetD));
    assertThat(manifestOfNAgain.get("adoptedChangeSetIds"))
        .isEqualTo(manifestOfN.get("adoptedChangeSetIds"));
    assertThat(manifestOfN.get("sketchFiles").findValuesAsText("location"))
        .containsExactly(puffinOfD.location(), puffinOfN.location());
    assertThat(manifestOfNAgain.get("sketchFiles")).isEqualTo(manifestOfN.get("sketchFiles"));
  }

  /**
   * D → N → N2: N adopted the dropped D, and the identifier fields change again after a slice of N.
   * N2 adopts the ids of D and N, and its Puffin files are those of D and N, each once.
   */
  @Test
  public void changeSetDroppedAfterDroppedOneHandsOnItsPuffinFilesAndIdsOnceEach()
      throws IOException {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    identifierFieldsAre("id", "data");
    Set<Integer> wide = Set.of(1, 2);
    SketchFile puffinOfD = new SketchFile(table.location() + "/puffin-d.stats", 200L, 20L);
    List<StagedChangeFile> filesOfD =
        stagedFiles(table, TABLE_REFERENCE, wide, update(1L, "d1"), update(2L, "d2"));
    committer.commitOneSlice(
        requestOf(List.of(rowChangesAt(0, 10L, filesOfD, puffinOfD)), ImmutableMap.of(0, 11L)));
    String changeSetD = lastConnectorSnapshot().summary().get(CHANGE_SET_ID_PROP);
    identifierFieldsAre("id");
    restart();

    SketchFile puffinOfN = new SketchFile(table.location() + "/puffin-n.stats", 100L, 10L);
    List<StagedChangeFile> filesOfN =
        stagedFiles(table, TABLE_REFERENCE, wide, insert(4L, "n4"), insert(5L, "n5"));
    committer.commitOneSlice(
        requestOf(List.of(rowChangesAt(0, 20L, filesOfN, puffinOfN)), ImmutableMap.of(0, 21L)));
    String changeSetN = lastConnectorSnapshot().summary().get(CHANGE_SET_ID_PROP);
    assertThat(changeSetN).as("the first slice of N landed").isNotEqualTo(changeSetD);
    identifierFieldsAre("data");
    restart();

    List<StagedChangeFile> filesOfN2 = stagedFiles(table, TABLE_REFERENCE, wide, insert(6L, "n6"));
    committer.commitOneSlice(
        requestOf(List.of(rowChangesAt(0, 30L, filesOfN2)), ImmutableMap.of(0, 31L)));

    JsonNode manifestOfN2 = manifestPointerNames();
    assertThat(manifestOfN2.get("changeSetId").asText()).isNotIn(changeSetD, changeSetN);
    assertThat(manifestOfN2.get("adoptedChangeSetIds"))
        .isEqualTo(MAPPER.createArrayNode().add(changeSetD).add(changeSetN));
    assertThat(manifestOfN2.get("sketchFiles"))
        .isEqualTo(
            asRead(
                MAPPER
                    .createArrayNode()
                    .add(sketchFileJson(puffinOfD, filesOfD))
                    .add(sketchFileJson(puffinOfN, filesOfN))));
  }

  /**
   * A drain of three slices gets one step, of its final snapshot F: its own snapshots O = {F} and
   * its change set, the answers of its manifest at F, the commit id of the drain, its Puffin files
   * kept on a failure. The step comes while the files of the change set, which it may read, are
   * still there; they go after it.
   */
  @Test
  public void drainOfSeveralSlicesGetsOneStepOfItsFinalSnapshotBeforeItsChangeSetGoes() {
    appendRows(row(1L, "a"), row(2L, "b"), row(3L, "c"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    List<StagedChangeFile> covered = stagedFiles(update(1L, "a1"), update(2L, "b2"));
    List<StagedChangeFile> uncovered = stagedFiles(update(3L, "c3"));
    List<String> staged = locationsOf(covered, uncovered);
    SketchFile puffin = new SketchFile(table.location() + "/puffin-a.stats", 300L, 30L);
    AtomicBoolean changeSetDuringStep = new AtomicBoolean();
    step.behavior =
        input -> {
          Snapshot finalSnapshot = input.table().snapshot(input.snapshotId());
          changeSetDuringStep.set(
              exists(manifestOf(changeSetOf(finalSnapshot)))
                  && staged.stream().allMatch(this::exists));
          return new StatisticsStep.Result(StatisticsStep.Outcome.WRITTEN, null);
        };
    TableCommitRequest request =
        requestOf(
            List.of(rowChangesAt(0, 10L, covered, puffin), rowChangesAt(0, 11L, uncovered, null)),
            ImmutableMap.of(0, 12L));

    assertThat(committer.commit(request)).isEqualTo(TableCommitter.Outcome.COMMITTED);

    List<Snapshot> slices = connectorSnapshots();
    assertThat(slices).hasSize(3);
    Snapshot last = finalSnapshot();
    assertThat(last.snapshotId()).isEqualTo(slices.get(2).snapshotId());
    UUID changeSetId = changeSetOf(last);
    assertThat(step.inputs)
        .singleElement()
        .satisfies(
            input -> {
              assertThat(input.identifier()).isEqualTo(TABLE_IDENTIFIER);
              assertThat(input.snapshotId()).isEqualTo(last.snapshotId());
              assertThat(input.ownSnapshotIds()).containsExactly(last.snapshotId());
              assertThat(input.ownChangeSetIds()).containsExactly(changeSetId.toString());
              assertThat(input.commitId()).isEqualTo(request.commitId());
              assertThat(input.keepSketchesOnFailure()).isTrue();
              assertThat(input.tableConfig()).isSameAs(tableConfig);
              assertThat(input.answers())
                  .extracting(
                      answer -> answer.snapshotId(),
                      answer -> answer.sketchFile() == null ? null : answer.sketchFile().location(),
                      answer -> locationsOf(answer.stagedFiles()))
                  .containsExactly(
                      tuple(last.snapshotId(), puffin.location(), locationsOf(covered)),
                      tuple(last.snapshotId(), null, locationsOf(uncovered)));
            });
    assertThat(changeSetDuringStep).as("the change set is there during the step").isTrue();
    assertThat(exists(manifestOf(changeSetId))).isFalse();
    assertThat(staged).noneMatch(this::exists);
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId()).isEqualTo(last.snapshotId());
  }

  /**
   * A step of F that did not finish leaves the files of its change set to the recount of F. FAILED
   * puts the table into the lag with a new generation, STOPPED changes nothing. Neither fails the
   * drain: CommitToTable of F goes, and the table is idle. The cycle of the drain only: the next
   * one recounts F.
   */
  @ParameterizedTest
  @EnumSource(
      value = StatisticsStep.Outcome.class,
      names = {"FAILED", "STOPPED"})
  public void stepOfFinalSnapshotThatDidNotFinishKeepsItsChangeSet(StatisticsStep.Outcome outcome) {
    StatisticsLag lag = lagOf(committer);
    lag.enter(TABLE_IDENTIFIER);
    long generation = lag.token(TABLE_IDENTIFIER);
    step.behavior =
        input ->
            new StatisticsStep.Result(
                outcome,
                outcome == StatisticsStep.Outcome.FAILED
                    ? new IllegalStateException("the step of the final snapshot fails")
                    : null);
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"), update(2L, "b2"));

    assertThat(
            committer.commitDrain(
                requestOf(
                    rowChangesAt(
                        0,
                        10L,
                        files,
                        new SketchFile(table.location() + "/puffin-a.stats", 300L, 30L)))))
        .isEqualTo(TableCommitter.Outcome.PENDING);

    Snapshot last = finalSnapshot();
    assertThat(step.inputs).hasSize(1);
    assertThat(exists(manifestOf(changeSetOf(last)))).isTrue();
    assertThat(locationsOf(files)).allMatch(this::exists);
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId()).isEqualTo(last.snapshotId());
    assertThat(committer.pendingTables())
        .as("the drain is over: only the reference of the lag is listed")
        .containsExactly(lagReference());
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    if (outcome == StatisticsStep.Outcome.FAILED) {
      assertThat(lag.token(TABLE_IDENTIFIER)).isGreaterThan(generation);
    } else {
      assertThat(lag.token(TABLE_IDENTIFIER)).isEqualTo(generation);
    }
  }

  /**
   * A step of F that throws an {@code Error} (an {@code OutOfMemoryError}, say) is not swallowed,
   * but the table enters the lag first, with a new generation, and the files of the change set stay
   * for the recount of F.
   */
  @Test
  public void stepOfFinalSnapshotThatThrowsErrorPutsTableIntoLagAndErrorGoesOn() {
    StatisticsLag lag = lagOf(committer);
    OutOfMemoryError error = new OutOfMemoryError("the step of the final snapshot throws");
    step.behavior =
        input -> {
          throw error;
        };
    appendRows(row(1L, "a"));
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"));
    long generation = lag.token(TABLE_IDENTIFIER);

    assertThatThrownBy(() -> committer.commitDrain(requestOf(rowChangesAt(0, 10L, files))))
        .isSameAs(error);

    assertThat(step.inputs).hasSize(1);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(lag.token(TABLE_IDENTIFIER)).isGreaterThan(generation);
    assertThat(exists(manifestOf(changeSetOf(finalSnapshot())))).isTrue();
    assertThat(locationsOf(files)).allMatch(this::exists);
  }

  /**
   * A step of F that throws is a failed one and fails nothing of the drain: CommitToTable of F
   * goes, the state is back to idle, and the next drain freezes and drains a change set of its own.
   * Each drain is driven through its cycle only; the first slice of the second recounts the first
   * F, and its step throws too: a refusal, and the drain goes on.
   */
  @Test
  public void stepOfFinalSnapshotThatThrowsFailsNothingOfDrain() {
    step.behavior =
        input -> {
          throw new IllegalStateException("the step of the final snapshot throws");
        };
    appendRows(row(1L, "a"));
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"));

    TableCommitRequest request = requestOf(rowChangesAt(0, 10L, files));
    assertThat(committer.commitDrain(request)).isEqualTo(TableCommitter.Outcome.PENDING);

    Snapshot first = finalSnapshot();
    assertThat(step.inputs).hasSize(1);
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId()).isEqualTo(first.snapshotId());
    assertThat(lagOf(committer).contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(exists(manifestOf(changeSetOf(first)))).isTrue();
    assertThat(locationsOf(files)).allMatch(this::exists);
    assertThat(committer.pendingTables())
        .as("the drain is over: only the reference of the lag is listed")
        .containsExactly(lagReference());

    TableCommitRequest next = requestOf(rowChangesAt(0, 11L, stagedFiles(update(1L, "a2"))));
    assertThat(committer.commitDrain(next)).isEqualTo(TableCommitter.Outcome.PENDING);
    Snapshot second = finalSnapshot();
    assertThat(second.snapshotId()).isNotEqualTo(first.snapshotId());
    assertThat(readAll()).containsExactly(tuple(1L, "a2"));
    assertThat(step.inputs)
        .extracting(StatisticsStepInput::snapshotId, StatisticsStepInput::commitId)
        .as("the step of the first F, its recount by the second drain, the step of the second F")
        .containsExactly(
            tuple(first.snapshotId(), request.commitId()),
            tuple(first.snapshotId(), next.commitId()),
            tuple(second.snapshotId(), next.commitId()));
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId()).isEqualTo(second.snapshotId());
  }

  /**
   * A step of F that finished removes the lag by the token taken before it, and the change set goes
   * (its Puffin files the step deleted itself).
   */
  @ParameterizedTest
  @EnumSource(
      value = StatisticsStep.Outcome.class,
      names = {"WRITTEN", "NOT_NEEDED", "FINAL", "TABLE_GONE"})
  public void stepOfFinalSnapshotThatFinishedRemovesLagAndChangeSet(
      StatisticsStep.Outcome outcome) {
    StatisticsLag lag = lagOf(committer);
    lag.enter(TABLE_IDENTIFIER);
    step.behavior = input -> new StatisticsStep.Result(outcome, null);
    appendRows(row(1L, "a"));
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"));

    assertThat(committer.commit(requestOf(rowChangesAt(0, 10L, files))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    assertThat(step.inputs).hasSize(1);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isFalse();
    assertThat(exists(manifestOf(changeSetOf(finalSnapshot())))).isFalse();
    assertThat(locationsOf(files)).noneMatch(this::exists);
  }

  /** The lag entered during the step of F is of a newer generation and outlives it. */
  @Test
  public void entryIntoLagDuringStepOfFinalSnapshotOutlivesIt() {
    StatisticsLag lag = lagOf(committer);
    lag.enter(TABLE_IDENTIFIER);
    step.behavior =
        input -> {
          lag.enter(TABLE_IDENTIFIER);
          return new StatisticsStep.Result(StatisticsStep.Outcome.WRITTEN, null);
        };
    appendRows(row(1L, "a"));

    committer.commit(requestOf(rowChangesAt(0, 10L, stagedFiles(update(1L, "a1")))));

    assertThat(step.inputs).hasSize(1);
    assertThat(lag.contains(TABLE_IDENTIFIER)).isTrue();
  }

  @Test
  public void finalSnapshotGoneBeforeItsStepPutsTableIntoLagAndLetsChangeSetGo() {
    step.behavior = input -> new StatisticsStep.Result(StatisticsStep.Outcome.SNAPSHOT_GONE, null);
    appendRows(row(1L, "a"));
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"));

    committer.commit(requestOf(rowChangesAt(0, 10L, files)));

    assertThat(step.inputs).hasSize(1);
    assertThat(lagOf(committer).contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(exists(manifestOf(changeSetOf(finalSnapshot())))).isFalse();
    assertThat(locationsOf(files)).noneMatch(this::exists);
  }

  /**
   * The final slice of a drain of several slices committed, and the lookup of its snapshot fails
   * ({@code reportedSnapshotId} = null). F gets no step, the table is in the lag, and the files of
   * the change set stay for the recount of F by its pointer, which the next cycle does: the cycle
   * of the drain only.
   */
  @Test
  public void finalSliceThatCannotBeLookedUpGetsNoStepAndKeepsItsChangeSet() {
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    committer.stop();
    committer = newDriver(lookupFailsAfter(FINAL_SLICE, false), Runnable::run);
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"), update(2L, "b2"));

    assertThat(committer.commitDrain(requestOf(rowChangesAt(0, 10L, files))))
        .isEqualTo(TableCommitter.Outcome.PENDING);

    Snapshot last = finalSnapshot();
    assertThat(connectorSnapshots()).hasSize(2);
    assertThat(step.inputs).isEmpty();
    assertThat(lagOf(committer).contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(exists(manifestOf(changeSetOf(last)))).isTrue();
    assertThat(locationsOf(files)).allMatch(this::exists);
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId()).isNull();
  }

  /**
   * The commit of the final slice lands without an answer, and its snapshot cannot be found. The
   * drain fails, the table is in the lag, and the files of the change set stay for the recount of
   * F, which the next cycle does: the cycle of the drain only.
   */
  @Test
  public void finalSliceOfUnknownOutcomeThatCannotBeFoundPutsTableIntoLag() {
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    committer.stop();
    committer = newDriver(lookupFailsAfter(FINAL_SLICE, true), Runnable::run);
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"), update(2L, "b2"));

    committer.commitDrain(requestOf(rowChangesAt(0, 10L, files)));

    Snapshot last = finalSnapshot();
    assertThat(readAll()).containsExactlyInAnyOrder(tuple(1L, "a1"), tuple(2L, "b2"));
    assertThat(step.inputs).isEmpty();
    assertThat(lagOf(committer).contains(TABLE_IDENTIFIER)).isTrue();
    assertThat(exists(manifestOf(changeSetOf(last)))).isTrue();
    assertThat(locationsOf(files)).allMatch(this::exists);
  }

  /**
   * The same for an intermediate slice leaves the lag as it is; the drain resumes from the pointer,
   * and its final snapshot gets the step.
   */
  @Test
  public void intermediateSliceOfUnknownOutcomeThatCannotBeFoundLeavesLagAsItIs() {
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    committer.stop();
    committer =
        newDriver(
            lookupFailsAfter(summary -> summary.containsKey(CHANGE_SET_ID_PROP), true),
            Runnable::run);

    committer.commitOneSlice(
        requestOf(rowChangesAt(0, 10L, stagedFiles(update(1L, "a1"), update(2L, "b2")))));

    assertThat(connectorSnapshots()).hasSize(1);
    assertThat(lagOf(committer).contains(TABLE_IDENTIFIER)).isFalse();
    assertThat(committer.commit(emptyRequest())).isEqualTo(TableCommitter.Outcome.COMMITTED);
    long last = finalSnapshot().snapshotId();
    assertThat(step.inputs)
        .singleElement()
        .satisfies(input -> assertThat(input.snapshotId()).isEqualTo(last));
  }

  /** With the statistics of the table off, a final slice not found gets no step and no lag. */
  @Test
  public void withStatisticsOfTableOffFinalSliceNotFoundLetsItsChangeSetGo() {
    when(tableConfig.statisticsEnabled()).thenReturn(false);
    appendRows(row(1L, "a"), row(2L, "b"));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    committer.stop();
    committer = newDriver(lookupFailsAfter(FINAL_SLICE, false), Runnable::run);
    List<StagedChangeFile> files = stagedFiles(update(1L, "a1"), update(2L, "b2"));

    assertThat(committer.commit(requestOf(rowChangesAt(0, 10L, files))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    UUID changeSetId =
        UUID.fromString(connectorSnapshots().get(0).summary().get(CHANGE_SET_ID_PROP));
    assertThat(step.inputs).isEmpty();
    assertThat(lagOf(committer).contains(TABLE_IDENTIFIER)).isFalse();
    assertThat(exists(manifestOf(changeSetId))).isFalse();
    assertThat(locationsOf(files)).noneMatch(this::exists);
  }

  /**
   * While the step of F holds the lock of the drain, the table answers PENDING with nothing
   * consumed and stays listed, another table commits, and CommitToTable of F waits for the step.
   * After it, the table is idle and commits.
   */
  @Test
  public void tableWaitsForStepOfItsFinalSnapshotWhileAnotherTableCommits()
      throws InterruptedException {
    Table other = catalog.createTable(OTHER_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
    appendRows(row(1L, "a"));
    CountDownLatch stepStarted = new CountDownLatch(1);
    CountDownLatch stepReleased = new CountDownLatch(1);
    step.behavior =
        input -> {
          if (input.identifier().equals(TABLE_IDENTIFIER)) {
            stepStarted.countDown();
            await(stepReleased, "the step of the final snapshot is released");
          }
          return new StatisticsStep.Result(StatisticsStep.Outcome.WRITTEN, null);
        };
    List<RewriteAssigned> slice =
        committer.handOutOneSlice(requestOf(rowChangesAt(0, 10L, stagedFiles(update(1L, "a1")))));
    assertThat(slice).isNotEmpty();
    Thread finishing = new Thread(() -> committer.answerAssignments(slice), "final-slice");
    finishing.start();
    try {
      await(stepStarted, "the step of the final snapshot starts");

      CommitResult during =
          committer
              .committer()
              .commit(requestOf(rowChangesAt(0, 11L, stagedFiles(update(1L, "a2")))));
      assertThat(during.outcome()).isEqualTo(TableCommitter.Outcome.PENDING);
      assertThat(during.consumed()).isEmpty();
      assertThat(committer.pendingTables()).contains(TABLE_REFERENCE);
      Envelope otherChanges =
          rowChangesAt(
              OTHER_REFERENCE,
              1,
              20L,
              stagedFiles(other, OTHER_REFERENCE, ID_FIELDS, insert(7L, "x")),
              null);
      assertThat(
              committer.commit(
                  new TableCommitRequest(
                      OTHER_REFERENCE,
                      List.of(otherChanges),
                      ImmutableMap.of(1, 21L),
                      UUID.randomUUID(),
                      OffsetDateTime.now())))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
      assertThat(commitsToTable(OTHER_IDENTIFIER)).hasSize(1);
      assertThat(commitsToTable(TABLE_IDENTIFIER)).isEmpty();
    } finally {
      stepReleased.countDown();
      finishing.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    }

    assertThat(finishing.isAlive()).isFalse();
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId())
        .isEqualTo(finalSnapshot().snapshotId());
    assertThat(committer.pendingTables()).doesNotContain(TABLE_REFERENCE);
    assertThat(committer.commit(emptyRequest())).isEqualTo(TableCommitter.Outcome.COMMITTED);
  }

  /**
   * The same through the {@link Coordinator} with the copy-on-write committer: a cycle that finds
   * the table held by the step of its final snapshot completes without valid-through, and the
   * timestamp shows up only in a cycle after the step is released.
   */
  @Test
  public void cycleDuringStepOfFinalSnapshotCompletesWithoutValidThrough()
      throws InterruptedException {
    appendRows(row(1L, "a"));
    CountDownLatch stepStarted = new CountDownLatch(1);
    CountDownLatch stepReleased = new CountDownLatch(1);
    step.behavior =
        input -> {
          stepStarted.countDown();
          await(stepReleased, "the step of the final snapshot is released");
          return new StatisticsStep.Result(StatisticsStep.Outcome.WRITTEN, null);
        };
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    when(config.commitThreads()).thenReturn(1);
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.rowLevelMode()).thenReturn(RowLevelOperationMode.COPY_ON_WRITE);
    MockProducer<String, byte[]> producer =
        new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();
    MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);
    when(clientFactory.createProducer(any())).thenReturn(producer);
    when(clientFactory.createConsumer(any())).thenReturn(consumer);
    when(clientFactory.createAdmin()).thenReturn(mock(Admin.class));
    AtomicReference<CopyOnWriteRewriteDriver> workers = new AtomicReference<>();
    Coordinator coordinator =
        new Coordinator(
            config,
            ImmutableList.of(),
            clientFactory,
            mock(SinkTaskContext.class),
            step,
            (send, sendInOneTransaction, transitionPool, health, stepOf) -> {
              // the workers' side: the driver answers the assignments of the committer
              workers.set(
                  new CopyOnWriteRewriteDriver(
                      catalog,
                      config,
                      MetadataEvents.NOOP,
                      sentEvents::add,
                      1,
                      health,
                      stepOf,
                      transitionPool));
              return workers.get().committer();
            });
    TopicPartition controlPartition = new TopicPartition(CTL_TOPIC, 0);
    try {
      coordinator.start();
      consumer.rebalance(ImmutableList.of(controlPartition));
      consumer.updateBeginningOffsets(ImmutableMap.of(controlPartition, 0L));

      // cycle 1: the changes come in, and a drain of one slice starts
      coordinator.process();
      UUID first = startedCommit(producer, 0);
      OffsetDateTime firstTs = EventTestUtil.now();
      addToControlTopic(
          consumer,
          0L,
          new RowChangesWritten(
              first,
              TABLE_REFERENCE,
              "task-0",
              List.of("src-topic"),
              stagedFiles(update(1L, "a1")),
              null));
      addToControlTopic(consumer, 1L, dataCompleteOf(first, firstTs));
      coordinator.process();

      assertThat(completedCommit(producer, first).validThroughTs()).isNull();
      // the transitions run in a pool of their own: the slice goes out after the cycle returns
      long sliceDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
      while (workers.get().handedOut().isEmpty() && System.nanoTime() < sliceDeadline) {
        Thread.sleep(10L);
      }
      List<RewriteAssigned> slice = workers.get().handedOut();
      assertThat(slice).isNotEmpty();
      workers.get().answerAssignments(slice);
      await(stepStarted, "the step of the final snapshot starts");

      // cycle 2: the step holds the table
      coordinator.process();
      UUID second = startedCommit(producer, 1);
      addToControlTopic(consumer, 2L, dataCompleteOf(second, EventTestUtil.now()));
      coordinator.process();

      assertThat(completedCommit(producer, second).validThroughTs())
          .as("the table has not applied everything up to the timestamp")
          .isNull();
      assertThat(commitsToTable(TABLE_IDENTIFIER)).isEmpty();
      assertThat(workers.get().pendingTables()).contains(TABLE_REFERENCE);

      // cycles after the release: the table commits, and the timestamp is claimed
      stepReleased.countDown();
      OffsetDateTime thirdTs = EventTestUtil.now();
      UUID third = null;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
      OffsetDateTime claimed = null;
      long offset = 3L;
      int cycle = 2;
      while (claimed == null && System.nanoTime() < deadline) {
        coordinator.process();
        third = startedCommit(producer, cycle++);
        addToControlTopic(consumer, offset++, dataCompleteOf(third, thirdTs));
        coordinator.process();
        claimed = completedCommit(producer, third).validThroughTs();
        if (claimed == null) {
          Thread.sleep(10L);
        }
      }

      assertThat(claimed).isEqualTo(thirdTs);
      assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId())
          .isEqualTo(finalSnapshot().snapshotId());
      assertThat(workers.get().pendingTables()).doesNotContain(TABLE_REFERENCE);
    } finally {
      stepReleased.countDown();
      coordinator.terminate();
    }
  }

  private static UUID startedCommit(MockProducer<String, byte[]> producer, int idx) {
    List<StartCommit> started = controlEventsOf(producer, StartCommit.class);
    assertThat(started).as("StartCommit #%s", idx).hasSizeGreaterThan(idx);
    return started.get(idx).commitId();
  }

  private static CommitComplete completedCommit(
      MockProducer<String, byte[]> producer, UUID commitId) {
    List<CommitComplete> completed =
        controlEventsOf(producer, CommitComplete.class).stream()
            .filter(complete -> complete.commitId().equals(commitId))
            .collect(Collectors.toList());
    assertThat(completed).as("CommitComplete of %s", commitId).hasSize(1);
    return completed.get(0);
  }

  private static <T extends Payload> List<T> controlEventsOf(
      MockProducer<String, byte[]> producer, Class<T> type) {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()).payload())
        .filter(type::isInstance)
        .map(type::cast)
        .collect(Collectors.toList());
  }

  private static DataComplete dataCompleteOf(UUID commitId, OffsetDateTime ts) {
    return new DataComplete(
        commitId, ImmutableList.of(new TopicPartitionOffset("src-topic", 0, 1L, ts)), "task-0");
  }

  private static void addToControlTopic(
      MockConsumer<String, byte[]> consumer, long offset, Payload payload) {
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC, 0, offset, "key", AvroUtil.encode(new Event(GROUP_ID, payload))));
  }

  /**
   * Two drains of several slices, the second with an update, a delete and inserts; one of its
   * responses comes without a Puffin file. Only the final snapshot of each drain gets a record,
   * with the NDV within its bounds (lower bound L &lt; upper bound U for the second). The staged
   * files no Puffin file covers are counted, so the step ran before they went; the Puffin files and
   * the change sets are gone after it.
   */
  @Test
  public void everyDrainGetsNdvOfItsFinalSnapshotOnly() {
    realStatistics();
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(2L);
    Record[] inserts = {insert(1L, "a"), insert(2L, "b"), insert(3L, "c"), insert(4L, "d")};

    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long first = finalSnapshot().snapshotId();
    NdvBounds.of(table, first, "id").written(1L, 2L, 3L, 4L).checkRecorded(LG_K);
    NdvBounds.of(table, first, "data").written("a", "b", "c", "d").checkRecorded(LG_K);

    Record[] covered = {update(1L, "a1"), delete(2L), insert(5L, "e")};
    Record[] uncovered = {update(3L, "c1"), insert(6L, "f"), insert(7L, "g")};
    List<StagedChangeFile> coveredFiles = stagedFiles(covered);
    List<StagedChangeFile> uncoveredFiles = stagedFiles(uncovered);
    assertThat(
            committer.commit(
                requestOf(
                    List.of(
                        rowChangesAt(0, 20L, coveredFiles, sketchOf(covered)),
                        rowChangesAt(0, 21L, uncoveredFiles, null)),
                    ImmutableMap.of(0, 22L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    long second = finalSnapshot().snapshotId();
    assertThat(connectorSnapshots()).hasSizeGreaterThan(4);
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(first, second);
    NdvBounds ids =
        NdvBounds.of(table, second, "id").base(List.of(1L, 2L, 3L, 4L)).written(1L, 3L, 5L, 6L, 7L);
    assertThat(ids.lower()).isLessThan(ids.upper());
    ids.checkRecorded(LG_K);
    NdvBounds data =
        NdvBounds.of(table, second, "data")
            .base(List.of("a", "b", "c", "d"))
            .written("a1", "e", "c1", "f", "g");
    assertThat(data.lower()).isLessThan(data.upper());
    data.checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(puffins).isNotEmpty().noneMatch(this::exists);
    assertThat(locationsOf(coveredFiles, uncoveredFiles)).noneMatch(this::exists);
  }

  /**
   * Another writer appends H right after the final slice F of a drain of two slices has landed. The
   * step goes on F, found by the commit id of the drain and not taken from the head: the record and
   * CommitToTable are of F, and H gets no record.
   */
  @Test
  public void foreignAppendRightAfterFinalSliceLeavesRecordOnFinalSnapshot() {
    realStatistics();
    AtomicReference<List<DataFile>> foreign = new AtomicReference<>();
    committer.stop();
    committer = newDriver(foreignAppendAfter(FINAL_SLICE, foreign), Runnable::run);
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long first = finalSnapshot().snapshotId();

    foreign.set(writeRows(row(9L, "z")));
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 20L, stagedFiles(changes), sketchOf(changes)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    assertThat(foreign.get()).as("the foreign append ran").isNull();
    long last = finalSnapshot().snapshotId();
    assertThat(connectorSnapshots()).hasSize(3);
    Snapshot head = table.currentSnapshot();
    assertThat(head.snapshotId()).as("the foreign append is the head").isNotEqualTo(last);
    assertThat(head.parentId()).isEqualTo(last);
    assertThat(step.inputs)
        .extracting(StatisticsStepInput::snapshotId)
        .containsExactly(first, last);
    assertThat(lastCommitToTable(TABLE_IDENTIFIER).snapshotId()).isEqualTo(last);
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(first, last);
    NdvBounds.of(table, last, "id").base(List.of(1L, 2L)).written(1L, 3L).checkRecorded(LG_K);
    NdvBounds.of(table, last, "data")
        .base(List.of("a", "b"))
        .written("a1", "c")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void drainResumedAfterRestartTakesPuffinFilesFromItsManifest() {
    realStatistics();
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long first = finalSnapshot().snapshotId();

    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c"), insert(4L, "d")};
    SketchFile puffin = sketchOf(changes);
    committer.commitOneSlice(requestOf(rowChangesAt(0, 20L, stagedFiles(changes), puffin)));

    committer.stop();
    committer = restartedDriver();

    long last = finalSnapshot().snapshotId();
    assertThat(step.inputs)
        .last()
        .satisfies(
            input -> {
              assertThat(input.snapshotId()).isEqualTo(last);
              assertThat(input.answers())
                  .extracting(answer -> answer.sketchFile().location())
                  .containsExactly(puffin.location());
            });
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(first, last);
    NdvBounds.of(table, last, "id").base(List.of(1L, 2L)).written(1L, 3L, 4L).checkRecorded(LG_K);
    NdvBounds.of(table, last, "data")
        .base(List.of("a", "b"))
        .written("a1", "c", "d")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(puffins).isNotEmpty().noneMatch(this::exists);
  }

  /**
   * The Puffin file of one of two responses of a slice is gone by the step of the final snapshot.
   * The answer of the manifest that names it is uncovered, and the step reads the staged files of
   * that answer (matched by their locations), not the files of the other one: the values only they
   * hold are in the NDV.
   */
  @Test
  public void deletedPuffinIsReadFromStagedFilesOfItsAnswer() {
    realStatistics();
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long first = finalSnapshot().snapshotId();

    Record[] covered = {update(1L, "a1"), insert(3L, "c")};
    Record[] lost = {update(2L, "b1"), insert(4L, "d")};
    List<StagedChangeFile> coveredFiles = stagedFiles(covered);
    List<StagedChangeFile> lostFiles = stagedFiles(lost);
    SketchFile coveredPuffin = sketchOf(covered);
    SketchFile lostPuffin = sketchOf(lost);
    table.io().deleteFile(lostPuffin.location());
    assertThat(
            committer.commit(
                requestOf(
                    List.of(
                        rowChangesAt(0, 20L, coveredFiles, coveredPuffin),
                        rowChangesAt(0, 21L, lostFiles, lostPuffin)),
                    ImmutableMap.of(0, 22L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    long last = finalSnapshot().snapshotId();
    assertThat(step.inputs)
        .last()
        .satisfies(
            input ->
                assertThat(input.answers())
                    .extracting(
                        answer -> answer.sketchFile().location(),
                        answer -> locationsOf(answer.stagedFiles()))
                    .containsExactlyInAnyOrder(
                        tuple(coveredPuffin.location(), locationsOf(coveredFiles)),
                        tuple(lostPuffin.location(), locationsOf(lostFiles))));
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(first, last);
    NdvBounds.of(table, last, "id")
        .base(List.of(1L, 2L))
        .written(1L, 2L, 3L, 4L)
        .checkRecorded(LG_K);
    NdvBounds.of(table, last, "data")
        .base(List.of("a", "b"))
        .written("a1", "c", "b1", "d")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(locationsOf(coveredFiles, lostFiles)).noneMatch(this::exists);
  }

  /**
   * A dropped change set D of a format this build does not know hands its staged files but not its
   * Puffin file to the change set N adopting them. The step of the final snapshot of N gets them as
   * an answer without a Puffin file, and counts the values of D from them.
   */
  @Test
  public void filesOfDroppedChangeSetOfUnsupportedFormatAreCountedFromFilesThemselves()
      throws IOException {
    realStatistics();
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long first = finalSnapshot().snapshotId();

    Record[] ofD = {insert(3L, "c"), insert(4L, "d")};
    List<StagedChangeFile> filesOfD = stagedFiles(ofD);
    SketchFile puffinOfD = sketchOf(ofD);
    dropped(
        ChangeSetManifest.CURRENT_FORMAT_VERSION + 1,
        filesOfD,
        List.of(sketchFileJson(puffinOfD, filesOfD)),
        List.of(UUID.randomUUID()));

    Record[] ofN = {insert(5L, "e")};
    List<StagedChangeFile> filesOfN = stagedFiles(ofN);
    SketchFile puffinOfN = sketchOf(ofN);
    assertThat(committer.commit(requestOf(rowChangesAt(0, 30L, filesOfN, puffinOfN))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    long last = finalSnapshot().snapshotId();
    assertThat(step.inputs)
        .last()
        .satisfies(
            input ->
                assertThat(input.answers())
                    .extracting(
                        answer ->
                            answer.sketchFile() == null ? null : answer.sketchFile().location(),
                        answer -> locationsOf(answer.stagedFiles()))
                    .containsExactlyInAnyOrder(
                        tuple(null, locationsOf(filesOfD)),
                        tuple(puffinOfN.location(), locationsOf(filesOfN))));
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(first, last);
    NdvBounds.of(table, last, "id").base(List.of(1L, 2L)).written(3L, 4L, 5L).checkRecorded(LG_K);
    NdvBounds.of(table, last, "data")
        .base(List.of("a", "b"))
        .written("c", "d", "e")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
  }

  /**
   * The identifier fields narrowed after the first of three slices of change set D; D is dropped,
   * and N adopts its files. Another writer appends a file between the first slice of D and N. The
   * slices of D are the step's own and only the foreign file is the gap: with room for exactly one
   * file in it, F of N still gets the NDV, from the Puffin files of D and of N and the foreign
   * file.
   */
  @Test
  public void changeSetAdoptingFilesOfDroppedOneCountsItsSlicesAsItsOwn() {
    realStatistics();
    when(config.statisticsGapMaxFiles()).thenReturn(1);
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .requireColumn("data")
        .setIdentifierFields("id", "data")
        .commit();
    Set<Integer> wide = Set.of(1, 2);
    Record[] inserts = {insert(1L, "a"), insert(2L, "b"), insert(3L, "c")};
    assertThat(
            committer.commit(
                requestOf(
                    rowChangesAt(
                        0,
                        10L,
                        stagedFiles(table, TABLE_REFERENCE, wide, inserts),
                        sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long base = finalSnapshot().snapshotId();

    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    AtomicBoolean narrow = new AtomicBoolean(true);
    onEvent =
        event -> {
          if (event.payload() instanceof CommitToTable && narrow.getAndSet(false)) {
            catalog.loadTable(TABLE_IDENTIFIER).updateSchema().setIdentifierFields("id").commit();
          }
        };
    Record[] ofD = {insert(4L, "d"), insert(5L, "e"), insert(6L, "f")};
    committer.commitOneSlice(
        requestOf(
            rowChangesAt(0, 20L, stagedFiles(table, TABLE_REFERENCE, wide, ofD), sketchOf(ofD))));
    List<Snapshot> slicesOfD = connectorSnapshots();
    String changeSetD = slicesOfD.get(slicesOfD.size() - 1).summary().get(CHANGE_SET_ID_PROP);
    assertThat(changeSetD).as("the first slice of D landed").isNotNull();

    appendRows(row(9L, "z"));

    Record[] ofN = {insert(7L, "g")};
    assertThat(committer.commit(requestOf(rowChangesAt(0, 30L, stagedFiles(ofN), sketchOf(ofN)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    Snapshot last = finalSnapshot();
    String changeSetN = changeSetOf(last).toString();
    assertThat(changeSetN).isNotEqualTo(changeSetD);
    assertThat(step.inputs)
        .last()
        .satisfies(
            input ->
                assertThat(input.ownChangeSetIds())
                    .containsExactlyInAnyOrder(changeSetN, changeSetD));
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(base, last.snapshotId());
    NdvBounds.of(table, last.snapshotId(), "id")
        .base(List.of(1L, 2L, 3L))
        .written(4L, 5L, 6L, 7L, 9L)
        .checkRecorded(LG_K);
    NdvBounds.of(table, last.snapshotId(), "data")
        .base(List.of("a", "b", "c"))
        .written("d", "e", "f", "g", "z")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(puffins).isNotEmpty().noneMatch(this::exists);
  }

  /**
   * The step of a merge-on-read tail T, on a thread of its own, and the step of the final snapshot
   * F both reach the commit of their records before either commits. Both take the oldest record off
   * by the view without the other's record (two records are kept), one of the commits conflicts and
   * is retried over the other. The records of both stand on existing files, and there is at most
   * one record over the window.
   */
  @Test
  public void stepsOfTailAndOfFinalSnapshotAtOnceKeepFilesOfBothRecords()
      throws InterruptedException {
    realStatistics();
    int retain = 2;
    when(config.statisticsRetainSnapshots()).thenReturn(retain);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean nextOffThread = new AtomicBoolean();
    List<Thread> offThread = Lists.newCopyOnWriteArrayList();
    committer.stop();
    committer =
        newDriver(
            catalog,
            task -> {
              if (nextOffThread.getAndSet(false)) {
                Thread thread = new Thread(recording(task, failure), "step-of-the-tail");
                offThread.add(thread);
                thread.start();
              } else {
                task.run();
              }
            });
    StatisticsStep real = new StatisticsStep();
    AtomicBoolean atOnce = new AtomicBoolean();
    CyclicBarrier bothAtCommit = new CyclicBarrier(2);
    AtomicInteger statisticsCommits = new AtomicInteger();
    List<StatisticsStep.Result> results = Lists.newCopyOnWriteArrayList();
    step.behavior =
        input -> {
          if (!atOnce.get()) {
            return real.run(input);
          }

          Table meeting =
              new BaseTable(
                  new MeetingOperations(
                      ((HasTableOperations) input.table()).operations(),
                      bothAtCommit,
                      statisticsCommits,
                      failure),
                  input.table().name());
          StatisticsStep.Result result = real.run(onTable(input, meeting));
          results.add(result);
          return result;
        };

    assertThat(
            committer.commit(
                requestOf(
                    List.of(dataWrittenAt(0, 1L, null, row(1L, "a"), row(2L, "b"))),
                    ImmutableMap.of(0, 2L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(
            committer.commit(
                requestOf(
                    List.of(dataWrittenAt(0, 2L, null, row(3L, "c"), row(4L, "d"))),
                    ImmutableMap.of(0, 3L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    table.refresh();
    long oldestId = connectorSnapshots().get(0).snapshotId();
    StatisticsFile oldest =
        table.statisticsFiles().stream()
            .filter(file -> file.snapshotId() == oldestId)
            .findFirst()
            .orElseThrow();
    assertThat(table.statisticsFiles()).hasSize(retain);

    // the first task handed to the pool in this cycle is the step of the tail
    nextOffThread.set(true);
    atOnce.set(true);
    Record[] changes = {update(1L, "a1")};
    assertThat(
            committer.commit(
                requestOf(
                    List.of(
                        dataWrittenAt(0, 3L, null, row(5L, "e")),
                        rowChangesAt(0, 4L, stagedFiles(changes), sketchOf(changes))),
                    ImmutableMap.of(0, 5L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(offThread).hasSize(1);
    offThread.get(0).join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    assertThat(offThread.get(0).isAlive()).isFalse();
    assertThat(failure.get()).as("the steps of T and F meet at their commits").isNull();

    table.refresh();
    List<Snapshot> snapshots = connectorSnapshots();
    assertThat(snapshots).hasSize(4);
    long tail = snapshots.get(2).snapshotId();
    long last = finalSnapshot().snapshotId();
    assertThat(step.inputs)
        .extracting(StatisticsStepInput::snapshotId)
        .contains(tail, last)
        .doesNotHaveDuplicates();
    assertThat(results)
        .extracting(StatisticsStep.Result::outcome)
        .containsExactly(StatisticsStep.Outcome.WRITTEN, StatisticsStep.Outcome.WRITTEN);
    // both committed on the same metadata: one of them conflicted and was retried
    assertThat(statisticsCommits).hasValue(3);
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .contains(tail, last)
        .doesNotContain(oldest.snapshotId())
        .hasSizeLessThanOrEqualTo(retain + 1);
    assertThat(table.statisticsFiles())
        .filteredOn(file -> file.snapshotId() == tail || file.snapshotId() == last)
        .hasSize(2)
        .allSatisfy(file -> assertThat(exists(file.path())).isTrue());
    assertThat(exists(oldest.path())).isFalse();
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(puffins).isNotEmpty().noneMatch(this::exists);
  }

  /**
   * A pass of the staging cleanup after every answer of a drain of three slices, with a TTL of 0
   * and nothing left in the buffer. The Puffin files of its responses are named by the change set
   * in memory and by the pointer of its slices: the step of its final snapshot finds every one of
   * them, and gets its NDV with no gap to recount (no file may be read).
   */
  @Test
  public void cleanupsBetweenSlicesOfDrainLeaveItsPuffinFilesToStepOfItsFinalSnapshot() {
    realStatistics();
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    long base = finalSnapshot().snapshotId();

    when(config.statisticsGapMaxFiles()).thenReturn(0);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    AtomicInteger passes = cleanupOnEveryTick(committer);
    String orphan = orphanPuffin();
    Map<String, Boolean> atTheStep = recordingPuffinsAtStep();
    Record[] first = {update(1L, "a1"), insert(3L, "c")};
    Record[] second = {insert(4L, "d")};
    SketchFile firstPuffin = sketchOf(first);
    SketchFile secondPuffin = sketchOf(second);

    assertThat(
            committer.commit(
                requestOf(
                    List.of(
                        rowChangesAt(0, 20L, stagedFiles(first), firstPuffin),
                        rowChangesAt(0, 21L, stagedFiles(second), secondPuffin)),
                    ImmutableMap.of(0, 22L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    long last = finalSnapshot().snapshotId();
    assertThat(connectorSnapshots()).as("a drain of several slices").hasSizeGreaterThan(3);
    assertThat(passes.get()).as("passes between the slices").isGreaterThanOrEqualTo(2);
    assertThat(exists(orphan)).as("named by nothing: the passes ran").isFalse();
    assertThat(atTheStep)
        .as("the Puffin files of the step of the final snapshot, when it ran")
        .containsOnlyKeys(firstPuffin.location(), secondPuffin.location())
        .containsValues(true)
        .doesNotContainValue(false);
    assertThat(table.statisticsFiles())
        .extracting(StatisticsFile::snapshotId)
        .containsExactlyInAnyOrder(base, last);
    NdvBounds.of(table, last, "id").base(List.of(1L, 2L)).written(1L, 3L, 4L).checkRecorded(LG_K);
    NdvBounds.of(table, last, "data")
        .base(List.of("a", "b"))
        .written("a1", "c", "d")
        .checkRecorded(LG_K);
    assertStatisticsFilesExist(table);
    assertStatisticsOnOwnSnapshot(table);
    assertThat(puffins).isNotEmpty().noneMatch(this::exists);
  }

  /**
   * The coordinator dies after the first slice of a drain, one pass between it and the next. The
   * coordinator that takes over resumes the drain from the pointer, a pass after every answer: its
   * step of the final snapshot finds the Puffin file of the change set, with no gap to recount.
   */
  @Test
  public void cleanupsBeforeAndAfterRestartLeavePuffinFilesToStepOfResumedDrain() {
    realStatistics();
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    assertThat(
            committer.commit(
                requestOf(rowChangesAt(0, 10L, stagedFiles(inserts), sketchOf(inserts)))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);

    when(config.statisticsGapMaxFiles()).thenReturn(0);
    when(config.copyOnWriteMaxSliceKeys()).thenReturn(1L);
    Record[] changes = {update(1L, "a1"), insert(3L, "c"), insert(4L, "d")};
    SketchFile puffin = sketchOf(changes);
    committer.commitOneSlice(requestOf(rowChangesAt(0, 20L, stagedFiles(changes), puffin)));
    String orphan = orphanPuffin();
    cleanupOf(committer).maybeClean(System.currentTimeMillis(), ImmutableList::of);
    assertThat(exists(orphan)).as("named by nothing: the pass ran").isFalse();
    assertThat(exists(puffin.location())).as("named by the change set of the drain").isTrue();

    committer.stop();
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    committer = newDriver();
    AtomicInteger passes = cleanupOnEveryTick(committer);
    String orphanAfterTheRestart = orphanPuffin();
    Map<String, Boolean> atTheStep = recordingPuffinsAtStep();
    for (TableReference visited : committer.pendingTables()) {
      assertThat(committer.commit(emptyRequest(visited)))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
    }

    long last = finalSnapshot().snapshotId();
    assertThat(passes.get()).as("passes between the resumed slices").isGreaterThanOrEqualTo(1);
    assertThat(exists(orphanAfterTheRestart)).isFalse();
    assertThat(atTheStep).containsOnlyKeys(puffin.location()).doesNotContainValue(false);
    assertThat(table.statisticsFiles()).extracting(StatisticsFile::snapshotId).contains(last);
    NdvBounds.of(table, last, "id").base(List.of(1L, 2L)).written(1L, 3L, 4L).checkRecorded(LG_K);
    assertThat(puffins).isNotEmpty().noneMatch(this::exists);
  }

  /**
   * The step of the merge-on-read tail waits on the pool; its response has left the buffer. A pass
   * meanwhile keeps its Puffin file: the queue of the tail names it.
   */
  @Test
  public void cleanupKeepsPuffinFileOfStepOfTailWaitingOnPool() {
    List<Runnable> held = Lists.newArrayList();
    committer.stop();
    committer = newDriver(catalog, held::add);
    String puffin = orphanPuffin();
    String orphan = orphanPuffin();

    assertThat(
            committer.commitOnce(
                requestOf(
                    List.of(dataWrittenAt(0, 1L, new SketchFile(puffin, 1L, 1L), row(1L, "a"))),
                    ImmutableMap.of(0, 2L))))
        .isEqualTo(TableCommitter.Outcome.COMMITTED);
    assertThat(held).as("the step of the tail").hasSize(1);

    cleanupOf(committer).maybeClean(System.currentTimeMillis(), ImmutableList::of);

    assertThat(exists(puffin)).as("named by the step of the tail in the queue").isTrue();
    assertThat(exists(orphan)).as("named by nothing: the pass ran").isFalse();
    held.forEach(Runnable::run);
    assertThat(step.inputs).hasSize(1);
  }

  /**
   * The step of F of a partitioned table commits the NDV, and the partition statistics fail: the
   * outcome is {@code FAILED} with the record of F written, and its change set X stays. No recount
   * follows (F has its record), so a pass with a TTL of 0 deletes the manifest, the staged files
   * and the Puffin files of X.
   */
  @Test
  public void changeSetOfStepFailedAfterItsNdvWasCommittedGoesByTtl() {
    realStatistics();
    tableConfig =
        new TableSinkConfig(null, ImmutableList.of(), ImmutableList.of(), null, true, true, null);
    when(config.tableConfig(any())).thenReturn(tableConfig);
    committer.stop();
    catalog.dropTable(TABLE_IDENTIFIER, false);
    table =
        catalog.createTable(
            TABLE_IDENTIFIER, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("data").build());
    committer = newDriver();
    Record[] inserts = {insert(1L, "a"), insert(2L, "b")};
    List<StagedChangeFile> files = stagedFiles(inserts);
    SketchFile puffin = sketchOf(inserts);

    try (MockedStatic<PartitionStatsHandler> handler =
        mockStatic(PartitionStatsHandler.class, CALLS_REAL_METHODS)) {
      handler
          .when(() -> PartitionStatsHandler.computeAndWriteStatsFile(any(Table.class), anyLong()))
          .thenThrow(new UncheckedIOException(new IOException("the partition view fails")));
      assertThat(committer.commit(requestOf(rowChangesAt(0, 10L, files, puffin))))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
      handler.verify(
          () -> PartitionStatsHandler.computeAndWriteStatsFile(any(Table.class), anyLong()));
    }

    Snapshot last = finalSnapshot();
    String manifest = manifestOf(changeSetOf(last));
    assertThat(table.statisticsFiles())
        .as("the NDV of F is committed")
        .extracting(StatisticsFile::snapshotId)
        .containsExactly(last.snapshotId());
    assertThat(table.partitionStatisticsFiles()).as("the partition view failed").isEmpty();
    assertThat(exists(manifest)).as("X is kept after FAILED").isTrue();
    assertThat(locationsOf(files)).allMatch(this::exists);
    assertThat(exists(puffin.location())).isTrue();

    cleanupOf(committer).maybeClean(System.currentTimeMillis(), ImmutableList::of);

    assertThat(exists(manifest)).as("no recount of F follows: the TTL takes X").isFalse();
    assertThat(locationsOf(files)).noneMatch(this::exists);
    assertNoStrayPuffins(stagingDirectory(), ImmutableList.of());
    assertThat(exists(puffin.location())).isFalse();
  }

  private StagingCleanupScheduler cleanupOf(CopyOnWriteRewriteDriver driver) {
    when(config.stagingOrphanTtlMs()).thenReturn(0L);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(0L);
    return InMemoryStagingCleanup.over(
        config, driver.committer().stagingCleanupSource(), () -> false);
  }

  /** Runs the staging cleanup of {@code driver} after each of its ticks; returns the pass count. */
  private AtomicInteger cleanupOnEveryTick(CopyOnWriteRewriteDriver driver) {
    StagingCleanupScheduler cleanup = cleanupOf(driver);
    AtomicInteger passes = new AtomicInteger();
    driver.afterEachTick(
        () -> {
          passes.incrementAndGet();
          cleanup.maybeClean(System.currentTimeMillis(), ImmutableList::of);
        });
    return passes;
  }

  private String orphanPuffin() {
    String location = stagingDirectory() + "/stats/" + UUID.randomUUID() + "-task-9.puffin";
    ((InMemoryFileIO) table.io()).addFile(location, new byte[] {1});
    return location;
  }

  /**
   * Hands the step to the real one; the map tells whether each Puffin file of the answers of the
   * last step existed when it ran.
   */
  private Map<String, Boolean> recordingPuffinsAtStep() {
    StatisticsStep real = new StatisticsStep();
    Map<String, Boolean> atTheStep = Maps.newConcurrentMap();
    step.behavior =
        input -> {
          atTheStep.clear();
          input.answers().stream()
              .filter(answer -> answer.sketchFile() != null)
              .forEach(
                  answer ->
                      atTheStep.put(
                          answer.sketchFile().location(), exists(answer.sketchFile().location())));
          return real.run(input);
        };
    return atTheStep;
  }

  private CopyOnWriteRewriteDriver newDriver() {
    return newDriver(catalog, Runnable::run);
  }

  /**
   * A new coordinator over the same catalog, the table named by the configuration: its first cycle
   * visits what {@code pendingTables()} lists, nothing being buffered, and resumes the drain a
   * previous coordinator left.
   */
  private CopyOnWriteRewriteDriver restartedDriver() {
    when(config.configuredTables()).thenReturn(ImmutableSet.of(TABLE_IDENTIFIER));
    CopyOnWriteRewriteDriver restarted = newDriver();
    for (TableReference visited : restarted.pendingTables()) {
      assertThat(restarted.commit(emptyRequest(visited)))
          .isEqualTo(TableCommitter.Outcome.COMMITTED);
    }

    return restarted;
  }

  private TableReference lagReference() {
    return TableReference.of(catalog.name(), TABLE_IDENTIFIER);
  }

  private CopyOnWriteRewriteDriver newDriver(Catalog driverCatalog, Executor transitions) {
    return new CopyOnWriteRewriteDriver(
        driverCatalog,
        config,
        MetadataEvents.NOOP,
        event -> {
          sentEvents.add(event);
          onEvent.accept(event);
        },
        1,
        new StatisticsHealth(0L),
        step,
        transitions);
  }

  private void realStatistics() {
    when(config.statisticsLgNominalEntries()).thenReturn(LG_K);
    when(config.statisticsGapMaxFiles()).thenReturn(1_000);
    when(config.statisticsGapMaxBytes()).thenReturn(Long.MAX_VALUE);
    when(config.statisticsRetainSnapshots()).thenReturn(10);
    when(config.statisticsStaleDeletedRatio()).thenReturn(0.2);
    when(config.statisticsHealthRecheckIntervalMs()).thenReturn(TimeUnit.HOURS.toMillis(1));
    tableConfig =
        new TableSinkConfig(null, ImmutableList.of(), ImmutableList.of(), null, true, false, null);
    when(config.tableConfig(any())).thenReturn(tableConfig);
    step.behavior = new StatisticsStep()::run;
  }

  /**
   * The catalog, except that the test table it hands out cannot be refreshed once, right after the
   * slice {@code slice} tells by its summary has landed: the lookup of its snapshot fails. With
   * {@code unknownOutcome}, the commit of that slice also lands without an answer, once.
   */
  private InMemoryCatalog lookupFailsAfter(
      Predicate<Map<String, String>> slice, boolean unknownOutcome) {
    AtomicBoolean lookupFails = new AtomicBoolean(true);
    AtomicBoolean answerLost = new AtomicBoolean(unknownOutcome);
    InMemoryCatalog watched = spy(catalog);
    doAnswer(
            invocation -> {
              Table real = catalog.loadTable(TABLE_IDENTIFIER);
              TableOperations ops =
                  new AnswerLosingOperations(
                      ((HasTableOperations) real).operations(), slice, answerLost);
              Table loaded = spy(new BaseTable(ops, real.name()));
              doAnswer(
                      refresh -> {
                        refresh.callRealMethod();
                        Snapshot head = ops.current().currentSnapshot();
                        if (head != null
                            && slice.test(head.summary())
                            && lookupFails.getAndSet(false)) {
                          throw new UncheckedIOException(new IOException("catalog unreachable"));
                        }
                        return null;
                      })
                  .when(loaded)
                  .refresh();
              return loaded;
            })
        .when(watched)
        .loadTable(TABLE_IDENTIFIER);
    return watched;
  }

  /**
   * The catalog, except that right after the commit of the slice {@code slice} tells, another
   * writer appends the files {@code foreign} holds, once: the commit of the slice sees them as the
   * head.
   */
  private InMemoryCatalog foreignAppendAfter(
      Predicate<Map<String, String>> slice, AtomicReference<List<DataFile>> foreign) {
    InMemoryCatalog watched = spy(catalog);
    doAnswer(
            invocation -> {
              Table real = catalog.loadTable(TABLE_IDENTIFIER);
              TableOperations ops =
                  new ForeignAppendOperations(
                      ((HasTableOperations) real).operations(), slice, foreign);
              return new BaseTable(ops, real.name());
            })
        .when(watched)
        .loadTable(TABLE_IDENTIFIER);
    return watched;
  }

  private final class ForeignAppendOperations extends ForwardingOperations {
    private final Predicate<Map<String, String>> slice;
    private final AtomicReference<List<DataFile>> foreign;

    private ForeignAppendOperations(
        TableOperations delegate,
        Predicate<Map<String, String>> slice,
        AtomicReference<List<DataFile>> foreign) {
      super(delegate);
      this.slice = slice;
      this.foreign = foreign;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      super.commit(base, metadata);
      Snapshot head = metadata.currentSnapshot();
      if (head != null && slice.test(head.summary())) {
        List<DataFile> files = foreign.getAndSet(null);
        if (files != null) {
          AppendFiles append = catalog.loadTable(TABLE_IDENTIFIER).newAppend();
          files.forEach(append::appendFile);
          append.commit();
        }
      }
    }
  }

  private static final class AnswerLosingOperations extends ForwardingOperations {
    private final Predicate<Map<String, String>> slice;
    private final AtomicBoolean answerLost;

    private AnswerLosingOperations(
        TableOperations delegate, Predicate<Map<String, String>> slice, AtomicBoolean answerLost) {
      super(delegate);
      this.slice = slice;
      this.answerLost = answerLost;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      super.commit(base, metadata);
      Snapshot head = metadata.currentSnapshot();
      if (head != null && slice.test(head.summary()) && answerLost.getAndSet(false)) {
        throw new CommitStateUnknownException(
            new IOException("connection reset after the commit was sent"));
      }
    }
  }

  /**
   * Operations of a step whose first commit waits at {@code barrier} for the other step: both
   * commit on the metadata they read before either commits. A failed wait goes to {@code failure}.
   */
  private static final class MeetingOperations extends ForwardingOperations {
    private final CyclicBarrier barrier;
    private final AtomicInteger commits;
    private final AtomicReference<Throwable> failure;
    private final AtomicBoolean met = new AtomicBoolean();

    private MeetingOperations(
        TableOperations delegate,
        CyclicBarrier barrier,
        AtomicInteger commits,
        AtomicReference<Throwable> failure) {
      super(delegate);
      this.barrier = barrier;
      this.commits = commits;
      this.failure = failure;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      commits.incrementAndGet();
      if (!met.getAndSet(true)) {
        try {
          barrier.await(AWAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }

          failure.compareAndSet(null, e);
          throw new IllegalStateException("The other step did not reach its commit", e);
        }
      }

      super.commit(base, metadata);
    }
  }

  private abstract static class ForwardingOperations implements TableOperations {
    private final TableOperations delegate;

    private ForwardingOperations(TableOperations delegate) {
      this.delegate = delegate;
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

  private static StatisticsStepInput onTable(StatisticsStepInput input, Table newTable) {
    return StatisticsStepInput.builder()
        .table(newTable)
        .identifier(input.identifier())
        .snapshotId(input.snapshotId())
        .ownSnapshotIds(input.ownSnapshotIds())
        .ownChangeSetIds(input.ownChangeSetIds())
        .answers(input.answers())
        .commitId(input.commitId())
        .stopped(input.stopped())
        .keepSketchesOnFailure(input.keepSketchesOnFailure())
        .config(input.config())
        .tableConfig(input.tableConfig())
        .health(input.health())
        .build();
  }

  private static Runnable recording(Runnable task, AtomicReference<Throwable> failure) {
    return () -> {
      try {
        task.run();
      } catch (Throwable t) {
        failure.compareAndSet(null, t);
        throw t;
      }
    };
  }

  private static void await(CountDownLatch latch, String what) {
    try {
      assertThat(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS)).as(what).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(what, e);
    }
  }

  private Snapshot finalSnapshot() {
    table.refresh();
    List<Snapshot> finals =
        connectorSnapshots().stream()
            .filter(snapshot -> snapshot.summary().containsKey(FINAL_CHANGE_SET_ID_PROP))
            .collect(Collectors.toList());
    assertThat(finals).as("a drain ended").isNotEmpty();
    return finals.get(finals.size() - 1);
  }

  private static UUID changeSetOf(Snapshot snapshot) {
    return UUID.fromString(snapshot.summary().get(FINAL_CHANGE_SET_ID_PROP));
  }

  private String manifestOf(UUID changeSetId) {
    return ChangeSetManifest.location(
        StagedChangeFileWriter.stagingLocation(table, null),
        TABLE_REFERENCE,
        GROUP_ID,
        changeSetId);
  }

  private String stagingDirectory() {
    return StagedChangeFileWriter.stagingDirectory(
        StagedChangeFileWriter.stagingLocation(table, null), TABLE_REFERENCE, GROUP_ID);
  }

  private boolean exists(String location) {
    return ((InMemoryFileIO) table.io()).fileExists(location);
  }

  @SafeVarargs
  private static List<String> locationsOf(List<StagedChangeFile>... files) {
    return Stream.of(files)
        .flatMap(List::stream)
        .map(StagedChangeFile::location)
        .collect(Collectors.toList());
  }

  private List<CommitToTable> commitsToTable(TableIdentifier identifier) {
    return sentEvents.stream()
        .map(Event::payload)
        .filter(CommitToTable.class::isInstance)
        .map(CommitToTable.class::cast)
        .filter(commit -> commit.tableReference().identifier().equals(identifier))
        .collect(Collectors.toList());
  }

  private CommitToTable lastCommitToTable(TableIdentifier identifier) {
    List<CommitToTable> sent = commitsToTable(identifier);
    assertThat(sent).as("CommitToTable of %s", identifier).isNotEmpty();
    return sent.get(sent.size() - 1);
  }

  /**
   * The intermediate Puffin file a worker writes along with {@code changes}: the values of every
   * change but a delete, which stages a key and no value.
   */
  private SketchFile sketchOf(Record... changes) {
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
    for (Record change : changes) {
      if ((int) change.getField("op") != StagedChangeSchema.OP_DELETE) {
        sketches.update(tableRow(change));
      }
    }

    SketchFile sketchFile = sketches.write(UUID.randomUUID());
    assertThat(sketchFile).as("the Puffin file of the worker").isNotNull();
    puffins.add(sketchFile.location());
    return sketchFile;
  }

  /**
   * A change set D the pointer of a slice names, frozen under identifier fields that no longer
   * match the table's, with the statistics fields given, in format {@code formatVersion}.
   */
  private UUID dropped(
      int formatVersion,
      List<StagedChangeFile> files,
      List<ObjectNode> sketchFiles,
      List<UUID> adopted)
      throws IOException {
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table, ImmutableSet.of(), files, ImmutableSet.of("src-topic"), Map.of(0, 1L), null);
    String location =
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            manifest.changeSetId());
    manifest.write(table.io(), location);

    ObjectNode root = (ObjectNode) readJson(location);
    root.put("format-version", formatVersion);
    root.putArray("sketchFiles").addAll(sketchFiles);
    ArrayNode adoptedIds = root.putArray("adoptedChangeSetIds");
    adopted.forEach(id -> adoptedIds.add(id.toString()));
    try (OutputStream out = table.io().newOutputFile(location).createOrOverwrite()) {
      MAPPER.writeValue(out, root);
    }

    table
        .newAppend()
        .set(OFFSETS_PROP, "{\"0\":1}")
        .set(CHANGE_SET_ID_PROP, manifest.changeSetId().toString())
        .set(CURSOR_PROP, "{}")
        .commit();
    return manifest.changeSetId();
  }

  private void restart() {
    committer.stop();
    committer = newDriver();
  }

  private Snapshot lastConnectorSnapshot() {
    List<Snapshot> snapshots = connectorSnapshots();
    return snapshots.get(snapshots.size() - 1);
  }

  private void identifierFieldsAre(String... names) {
    table.refresh();
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .requireColumn("data")
        .setIdentifierFields(names)
        .commit();
  }

  private JsonNode manifestOf(RewriteAssigned assigned) throws IOException {
    return readJson(
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            assigned.changeSetId()));
  }

  private JsonNode manifestPointerNames() throws IOException {
    table.refresh();
    String changeSetId = table.currentSnapshot().summary().get(CHANGE_SET_ID_PROP);
    assertThat(changeSetId).as("the drain is in the middle of a change set").isNotNull();
    return readJson(
        ChangeSetManifest.location(
            StagedChangeFileWriter.stagingLocation(table, null),
            TABLE_REFERENCE,
            GROUP_ID,
            UUID.fromString(changeSetId)));
  }

  private static ObjectNode sketchFileJson(SketchFile sketchFile, List<StagedChangeFile> files) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("location", sketchFile.location());
    node.put("fileSizeBytes", sketchFile.fileSizeBytes());
    node.put("footerSizeBytes", sketchFile.footerSizeBytes());
    ArrayNode staged = node.putArray("stagedFiles");
    files.forEach(file -> staged.add(file.location()));
    return node;
  }

  private static JsonNode asRead(JsonNode node) {
    try {
      return MAPPER.readTree(MAPPER.writeValueAsString(node));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private JsonNode readJson(String location) throws IOException {
    try (InputStream in = table.io().newInputFile(location).newStream()) {
      return MAPPER.readTree(in);
    }
  }

  private static StatisticsLag lagOf(CopyOnWriteRewriteDriver driver) {
    return (StatisticsLag) field(driver.committer(), "statisticsLag");
  }

  private static MergeOnReadTailHandoff handoffOf(CopyOnWriteRewriteDriver driver) {
    return (MergeOnReadTailHandoff) field(driver.committer(), "mergeOnReadTail");
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

  private static Snapshot onlySnapshot(List<Snapshot> snapshots) {
    assertThat(snapshots).hasSize(1);
    return snapshots.get(0);
  }

  private List<Snapshot> connectorSnapshots() {
    table.refresh();
    List<Snapshot> snapshots = Lists.newArrayList();
    table
        .snapshots()
        .forEach(
            snapshot -> {
              if (snapshot.summary().containsKey(COMMIT_ID_PROP)) {
                snapshots.add(snapshot);
              }
            });
    return snapshots;
  }

  private static TableCommitRequest requestOf(Envelope envelope) {
    return requestOf(List.of(envelope), ImmutableMap.of(0, envelope.offset() + 1));
  }

  private static TableCommitRequest requestOf(
      List<Envelope> envelopes, Map<Integer, Long> controlTopicOffsets) {
    return new TableCommitRequest(
        TABLE_REFERENCE, envelopes, controlTopicOffsets, UUID.randomUUID(), OffsetDateTime.now());
  }

  private static TableCommitRequest emptyRequest() {
    return emptyRequest(TABLE_REFERENCE);
  }

  private static TableCommitRequest emptyRequest(TableReference reference) {
    return new TableCommitRequest(
        reference, List.of(), ImmutableMap.of(0, 11L), UUID.randomUUID(), OffsetDateTime.now());
  }

  private static Envelope rowChangesAt(int partition, long offset, List<StagedChangeFile> files) {
    return rowChangesAt(partition, offset, files, null);
  }

  private static Envelope rowChangesAt(
      int partition, long offset, List<StagedChangeFile> files, SketchFile sketchFile) {
    return rowChangesAt(TABLE_REFERENCE, partition, offset, files, sketchFile);
  }

  private static Envelope rowChangesAt(
      TableReference reference,
      int partition,
      long offset,
      List<StagedChangeFile> files,
      SketchFile sketchFile) {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), reference, "task-0", List.of("src-topic"), files, sketchFile);
    return new Envelope(new Event(GROUP_ID, payload), partition, offset);
  }

  private Envelope dataWrittenAt(
      int partition, long offset, SketchFile sketchFile, Record... rows) {
    DataWritten payload =
        new DataWritten(
            table.spec().partitionType(),
            UUID.randomUUID(),
            TABLE_REFERENCE,
            writeRows(rows),
            List.of(),
            List.of("src-topic"),
            sketchFile);
    return new Envelope(new Event(GROUP_ID, payload), partition, offset);
  }

  private List<StagedChangeFile> stagedFiles(Record... changes) {
    return stagedFiles(table, TABLE_REFERENCE, ID_FIELDS, changes);
  }

  private static List<StagedChangeFile> stagedFiles(
      Table target, TableReference reference, Set<Integer> idFields, Record... changes) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(target, reference, idFields, null, GROUP_ID, "task-0");
    long offset = 0;
    for (Record change : changes) {
      int op = (int) change.getField("op");
      writer.write(writer.stagedRow(tableRow(change), op, "src-topic", 0, offset++));
    }
    return writer.complete();
  }

  private static Record tableRow(Record change) {
    GenericRecord tableRow = GenericRecord.create(SCHEMA);
    tableRow.setField("id", change.getField("id"));
    tableRow.setField("data", change.getField("data"));
    return tableRow;
  }

  private static final Schema CHANGE =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "op", Types.IntegerType.get()));

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

  private static Record delete(long id) {
    return change(id, null, StagedChangeSchema.OP_DELETE);
  }

  private void appendRows(Record... rows) {
    AppendFiles append = table.newAppend();
    writeRows(rows).forEach(append::appendFile);
    append.commit();
  }

  private List<DataFile> writeRows(Record... rows) {
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
}
