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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Where staged files, manifests and the sweep prefix live, and that one connector's sweep stays out
 * of every staging directory that is not its own.
 *
 * <p>Runs on {@code HadoopFileIO} over the local file system: the sweep compares the locations a
 * listing returns with the ones the writer and the manifest were given, so an escaped directory
 * name has to come back from a real listing spelled exactly as it was written.
 */
public class TestStagingLayout {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final Set<Integer> ID_FIELDS = ImmutableSet.of(1);
  private static final String CTL_TOPIC = "control";
  // task.id is the task number alone, so two connectors' tasks share it
  private static final String TASK_ID = "0";
  private static final long ORPHAN_TTL_MS = 1_000L;
  private static final long SWEEP_INTERVAL_MS = 3_600_000L;
  private static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  private static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";

  @TempDir private Path dir;

  private HadoopTables tables;
  private String staging;

  @BeforeEach
  public void before() {
    tables = new HadoopTables(new Configuration());
    staging = "file:" + dir.resolve("staging");
  }

  @Test
  public void testASweepLeavesTheStagingOfATableThatPrintsWithTheSameName() {
    // table c in namespace a.b and table b.c in namespace a both render as a.b.c
    TableReference nested = reference(Namespace.of("a", "b"), "c");
    TableReference dotted = reference(Namespace.of("a"), "b.c");
    Table nestedTable = table("nested");
    Table dottedTable = table("dotted");
    String group = "connect-orders";

    StagedChangeFile nestedOrphan = stage(nestedTable, nested, group);
    StagedChangeFile dottedFile = stage(dottedTable, dotted, group);
    String dottedManifest = freeze(dottedTable, dotted, group, ImmutableList.of(dottedFile));

    sweep(drainState(nestedTable, nested), group);

    assertThat(exists(dottedTable, dottedFile.location()))
        .as("a staged file of a.b%2Ec, unreachable from a%2Eb.c")
        .isTrue();
    assertThat(exists(dottedTable, dottedManifest))
        .as("the manifest of a.b%2Ec, unreachable from a%2Eb.c")
        .isTrue();
    assertThat(exists(nestedTable, nestedOrphan.location()))
        .as("the sweeping table's own orphan: the sweep did run")
        .isFalse();
  }

  @Test
  public void testASweepLeavesTheStagingOfAnotherConnectorOnTheSameTable() {
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");

    StagedChangeFile ownOrphan = stage(table, orders, "connect-a");
    // connector b stands past the TTL in the middle of a drain: nothing a can see names its files
    StagedChangeFile otherFile = stage(table, orders, "connect-b");
    String otherManifest = freeze(table, orders, "connect-b", ImmutableList.of(otherFile));

    sweep(drainState(table, orders), "connect-a");

    assertThat(exists(table, otherFile.location()))
        .as("a staged file of connector b, unreachable from connector a")
        .isTrue();
    assertThat(exists(table, otherManifest))
        .as("the manifest of connector b, unreachable from connector a")
        .isTrue();
    assertThat(exists(table, ownOrphan.location()))
        .as("connector a's own orphan: the sweep did run")
        .isFalse();
  }

  @Test
  public void testASweepKeepsTheChangeSetAnotherConnectorCommittedOver() {
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");

    // connector a's drain failed after its first slice, which cleared its memory: the pointer in
    // the summary of that slice is all that still names the change set it has to resume
    StagedChangeFile live = stage(table, orders, "connect-a");
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            ImmutableList.of(live),
            ImmutableSet.of("orders"),
            ImmutableMap.of(),
            null);
    String manifestLocation = manifestLocation(orders, "connect-a", manifest);
    manifest.write(table.io(), manifestLocation);
    table
        .newAppend()
        .set(COMMIT_ID_PROP, UUID.randomUUID().toString())
        .set(offsetsProp("connect-a"), "{\"0\":1}")
        .set(CHANGE_SET_ID_PROP, manifest.changeSetId().toString())
        .commit();
    // connector b commits on top of it: a commit id and offsets of its own, no pointer
    table
        .newAppend()
        .set(COMMIT_ID_PROP, UUID.randomUUID().toString())
        .set(offsetsProp("connect-b"), "{\"0\":7}")
        .commit();
    StagedChangeFile ownOrphan = stage(table, orders, "connect-a");

    sweep(drainState(table, orders), "connect-a");

    assertThat(exists(table, live.location()))
        .as("a staged file named by connector a's pointer, below connector b's snapshot")
        .isTrue();
    assertThat(exists(table, manifestLocation))
        .as("the manifest connector a's pointer names")
        .isTrue();
    assertThat(exists(table, ownOrphan.location()))
        .as("connector a's own orphan: the sweep did run")
        .isFalse();
  }

  @Test
  public void testASweepFindsItsOwnFilesUnderEscapedDirectories() {
    // every character that is escaped, in the table and in the group: a listing that spelled a
    // directory differently from the writer would make the live file look like an orphan
    TableReference escaped = reference(Namespace.of("a.b"), "c/d%e");
    Table table = table("escaped");
    String group = "connect-cow.sink%1";

    StagedChangeFile live = stage(table, escaped, group);
    StagedChangeFile orphan = stage(table, escaped, group);
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            ImmutableList.of(live),
            ImmutableSet.of("orders"),
            ImmutableMap.of(),
            null);
    String manifestLocation = manifestLocation(escaped, group, manifest);
    manifest.write(table.io(), manifestLocation);

    TableDrainState state = drainState(table, escaped);
    state.manifest = manifest;
    state.manifestLocation = manifestLocation;
    sweep(state, group);

    assertThat(exists(table, live.location())).as("named by the change set in memory").isTrue();
    assertThat(exists(table, manifestLocation)).as("the change set's own manifest").isTrue();
    assertThat(exists(table, orphan.location())).as("named by nothing").isFalse();
  }

  @Test
  public void testASweepKeepsTheStagedFilesOfResponsesStillInTheCommitBuffer() {
    // a drain longer than the orphan TTL: the responses of its later cycles wait in the
    // coordinator's buffer until the drain ends, belonging to no change set at all: neither a
    // manifest nor the pointer in the summary names their staged files
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile buffered = stage(table, orders, "connect-a");
    StagedChangeFile orphan = stage(table, orders, "connect-a");

    scheduler("connect-a", 0L, () -> true, drainState(table, orders))
        .maybeClean(pastTtl(), () -> ImmutableList.of(response(orders, buffered)));

    assertThat(exists(table, buffered.location()))
        .as("named by a RowChangesWritten the coordinator is still holding")
        .isTrue();
    assertThat(exists(table, orphan.location()))
        .as("named by nothing: the sweep did run")
        .isFalse();
  }

  @Test
  public void testASweepKeepsTheNormalizedFileOfTheSliceBeingRewritten() {
    // the normalized file of the attempt in flight is named by nothing persisted: the manifest
    // lists the staged files it was built from, not itself. A rewrite slower than the TTL (the
    // two are not validated against each other) would have it deleted out from under the workers
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile live = stage(table, orders, "connect-a");
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            ImmutableList.of(live),
            ImmutableSet.of("orders"),
            ImmutableMap.of(),
            null);
    String manifestLocation = manifestLocation(orders, "connect-a", manifest);
    manifest.write(table.io(), manifestLocation);
    String inFlight = normalized(orders, "connect-a", manifest.changeSetId(), 3);
    String ofASliceThatIsOver = normalized(orders, "connect-a", manifest.changeSetId(), 2);
    write(table, inFlight);
    write(table, ofASliceThatIsOver);

    TableDrainState state = drainState(table, orders);
    state.manifest = manifest;
    state.manifestLocation = manifestLocation;
    state.normalizedLocation = inFlight;
    sweep(state, "connect-a");

    assertThat(exists(table, inFlight))
        .as("the normalized file the workers are rewriting right now")
        .isTrue();
    assertThat(exists(table, ofASliceThatIsOver))
        .as("the normalized file of a slice that is over, left behind by its cleanup")
        .isFalse();
  }

  @Test
  public void testASweepDeletesTheManifestOfAChangeSetNothingNamesAnyMore() {
    // a coordinator that died between writing the manifest and the first slice commit leaves a
    // frozen change set no one can reach: no snapshot names it, and the memory that held it went
    // with the coordinator. That leaves it to the TTL, which only works if the sweep treats a
    // manifest like any other file under the prefix: a drain whose first commit keeps failing
    // leaves one behind every cycle
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile abandoned = stage(table, orders, "connect-a");
    String orphanedManifest = freeze(table, orders, "connect-a", ImmutableList.of(abandoned));

    sweep(drainState(table, orders), "connect-a");

    assertThat(exists(table, orphanedManifest))
        .as("the manifest of a change set no pointer and no memory names, past the TTL")
        .isFalse();
    assertThat(exists(table, abandoned.location()))
        .as("its staged files, deleted by the same rule")
        .isFalse();
  }

  @Test
  public void testTableAndGroupDirectoriesAreEscaped() {
    TableReference escaped = reference(Namespace.of("a.b", "c"), "d/e%f");
    Table table = table("escaped");
    UUID changeSetId = UUID.randomUUID();

    assertThat(stage(table, escaped, "connect-my.sink").location())
        .startsWith(staging + "/a%2Eb.c.d%2Fe%25f/connect-my%2Esink/" + TASK_ID + "/")
        .endsWith("/0.avro");
    assertThat(ChangeSetManifest.location(staging, escaped, "connect-my.sink", changeSetId))
        .isEqualTo(
            staging + "/a%2Eb.c.d%2Fe%25f/connect-my%2Esink/_changesets/" + changeSetId + ".json");
    // names without those characters keep the directory they always had
    assertThat(
            ChangeSetManifest.location(
                staging, reference(Namespace.of("db"), "orders"), "connect-orders", changeSetId))
        .isEqualTo(staging + "/db.orders/connect-orders/_changesets/" + changeSetId + ".json");
  }

  @Test
  public void testASweepOfATableDroppedAndCreatedAgainLeavesTheChangeSetOfTheNewOne() {
    // staging configured outside the table, so the old table and the new one share a prefix: it is
    // spelled from the name alone. The coordinator was not restarted, and still holds the state of
    // the old table (its drain long over, or abandoned when the table was found replaced) next
    // to the state the workers' responses opened for the new one
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("db"), "orders");
    Table dropped = table("orders");
    TableReference oldOrders = TableReference.of("catalog", identifier, dropped.uuid());
    TableDrainState ofTheDroppedTable = drainState(dropped, oldOrders);
    tables.dropTable(dropped.location());

    Table recreated = table("orders");
    TableReference newOrders = TableReference.of("catalog", identifier, recreated.uuid());
    // the new table's change set has committed its first slice: its responses are spent, and its
    // drain is between a failure and the cycle that resumes it, so its memory is empty. The pointer
    // in the summary is all that names it
    StagedChangeFile live = stage(recreated, newOrders, "connect-a");
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            recreated,
            ID_FIELDS,
            ImmutableList.of(live),
            ImmutableSet.of("orders"),
            ImmutableMap.of(),
            null);
    String manifestLocation = manifestLocation(newOrders, "connect-a", manifest);
    manifest.write(recreated.io(), manifestLocation);
    recreated
        .newAppend()
        .set(COMMIT_ID_PROP, UUID.randomUUID().toString())
        .set(offsetsProp("connect-a"), "{\"0\":1}")
        .set(CHANGE_SET_ID_PROP, manifest.changeSetId().toString())
        .commit();
    StagedChangeFile orphan = stage(recreated, newOrders, "connect-a");

    scheduler("connect-a", 0L, () -> true, ofTheDroppedTable, drainState(recreated, newOrders))
        .maybeClean(pastTtl(), ImmutableList::of);

    assertThat(exists(recreated, live.location()))
        .as("a staged file of the new table's change set, named by the new table's pointer")
        .isTrue();
    assertThat(exists(recreated, manifestLocation))
        .as("the manifest the new table's pointer names")
        .isTrue();
    assertThat(exists(recreated, orphan.location()))
        .as("named by nothing: the new table's sweep did run")
        .isFalse();
  }

  @Test
  public void testTheStateOfAReplacedTableSweepsNothingUnderTheSharedPrefix() {
    // the same shared prefix, but this time the new table's change set is frozen and has not
    // committed a slice yet: no snapshot names it, so the only thing that reaches its manifest,
    // its staged files and the normalized file being rewritten is the memory of the state the new
    // table opened. The state of the table that is gone reaches none of them, and a pass that ran
    // it against the shared prefix would delete the lot
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("db"), "orders");
    Table dropped = table("orders");
    TableReference oldOrders = TableReference.of("catalog", identifier, dropped.uuid());
    TableDrainState ofTheDroppedTable = drainState(dropped, oldOrders);
    tables.dropTable(dropped.location());

    Table recreated = table("orders");
    TableReference newOrders = TableReference.of("catalog", identifier, recreated.uuid());
    StagedChangeFile live = stage(recreated, newOrders, "connect-a");
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            recreated,
            ID_FIELDS,
            ImmutableList.of(live),
            ImmutableSet.of("orders"),
            ImmutableMap.of(),
            null);
    String manifestLocation = manifestLocation(newOrders, "connect-a", manifest);
    manifest.write(recreated.io(), manifestLocation);
    String inFlight = normalized(newOrders, "connect-a", manifest.changeSetId(), 1);
    write(recreated, inFlight);
    StagedChangeFile orphan = stage(recreated, newOrders, "connect-a");

    TableDrainState ofTheNewTable = drainState(recreated, newOrders);
    ofTheNewTable.manifest = manifest;
    ofTheNewTable.manifestLocation = manifestLocation;
    ofTheNewTable.normalizedLocation = inFlight;

    // the state of the dropped table goes first, as the coordinator has held it the longer
    scheduler("connect-a", 0L, () -> true, ofTheDroppedTable, ofTheNewTable)
        .maybeClean(pastTtl(), ImmutableList::of);

    assertThat(exists(recreated, manifestLocation))
        .as("the manifest of a change set only the new table's memory names")
        .isTrue();
    assertThat(exists(recreated, live.location()))
        .as("its staged files, named by that same memory")
        .isTrue();
    assertThat(exists(recreated, inFlight))
        .as("the normalized file the workers are rewriting for it")
        .isTrue();
    assertThat(exists(recreated, orphan.location()))
        .as("named by nothing: the new table's own sweep did run")
        .isFalse();
  }

  @Test
  public void testAConfiguredStagingLocationLosesItsTrailingSlash() {
    Table table = table("orders");

    assertThat(StagedChangeFileWriter.stagingLocation(table, "s3://bucket/staging/"))
        .isEqualTo("s3://bucket/staging");
    assertThat(StagedChangeFileWriter.stagingLocation(table, "s3://bucket/staging"))
        .isEqualTo("s3://bucket/staging");
    assertThat(StagedChangeFileWriter.stagingLocation(table, null))
        .isEqualTo(table.location() + "/kc-copy-on-write-staging");
  }

  @Test
  public void testTheFirstSweepOfATableRunsAsSoonAsItIsLoaded() {
    // a coordinator is built anew by every rebalance that takes and returns the leader's partition.
    // A timer started at its birth means a connector rebalancing more often than the sweep interval
    // never sweeps at all, and staging grows without bound
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile orphan = stage(table, orders, "connect-a");

    scheduler("connect-a", SWEEP_INTERVAL_MS, () -> true, drainState(table, orders))
        .maybeClean(pastTtl(), ImmutableList::of);

    assertThat(exists(table, orphan.location()))
        .as("an orphan past the TTL, swept without waiting out an interval first")
        .isFalse();
  }

  @Test
  public void testATableLoadedAfterASweepDoesNotWaitOutTheIntervalOfAnother() {
    // the timer is per table: a table that joins the tables being swept between two sweeps has
    // never been swept
    TableReference orders = reference(Namespace.of("db"), "orders");
    TableReference items = reference(Namespace.of("db"), "items");
    Table ordersTable = table("orders");
    Table itemsTable = table("items");
    TableDrainState ordersState = drainState(ordersTable, orders);
    // deferred: no task has reported in yet, so no transition has loaded it
    TableDrainState itemsState = new TableDrainState(items);
    StagingCleanupScheduler scheduler =
        scheduler("connect-a", SWEEP_INTERVAL_MS, () -> true, ordersState, itemsState);

    long firstTick = pastTtl();
    scheduler.maybeClean(firstTick, ImmutableList::of);

    StagedChangeFile orphan = stage(itemsTable, items, "connect-a");
    itemsState.table = itemsTable;
    itemsState.stagingLocation = staging;
    scheduler.maybeClean(firstTick + 1, ImmutableList::of);

    assertThat(exists(itemsTable, orphan.location()))
        .as("the orphan of a table loaded after the first sweep, well within the interval")
        .isFalse();
  }

  @Test
  public void testASweptTableIsNotSweptAgainWithinTheInterval() {
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    TableDrainState state = drainState(table, orders);
    StagingCleanupScheduler scheduler =
        scheduler("connect-a", SWEEP_INTERVAL_MS, () -> true, state);

    long firstTick = pastTtl();
    scheduler.maybeClean(firstTick, ImmutableList::of);
    StagedChangeFile orphan = stage(table, orders, "connect-a");
    scheduler.maybeClean(firstTick + SWEEP_INTERVAL_MS - 1, ImmutableList::of);

    assertThat(exists(table, orphan.location()))
        .as("listing and deleting a whole prefix is not a per-tick cost")
        .isTrue();

    scheduler.maybeClean(firstTick + SWEEP_INTERVAL_MS, ImmutableList::of);

    assertThat(exists(table, orphan.location())).as("an interval later").isFalse();
  }

  @Test
  public void testNothingIsSweptUntilTheControlTopicIsCaughtUp() {
    // the commit buffer is one of the three sources of reachability. Until the control topic is
    // read to its end offsets the buffer holds only part of what the connector wrote, and the
    // staged files of the responses still on their way are named by nothing
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile orphan = stage(table, orders, "connect-a");
    AtomicBoolean caughtUp = new AtomicBoolean(false);
    StagingCleanupScheduler scheduler =
        scheduler("connect-a", SWEEP_INTERVAL_MS, caughtUp::get, drainState(table, orders));

    long tick = pastTtl();
    scheduler.maybeClean(tick, ImmutableList::of);

    assertThat(exists(table, orphan.location()))
        .as("the control topic is not read to the end offsets of its partitions yet")
        .isTrue();

    caughtUp.set(true);
    scheduler.maybeClean(tick + 1, ImmutableList::of);

    assertThat(exists(table, orphan.location())).as("read to the end, and swept").isFalse();
  }

  @Test
  public void testATableWhoseLockIsHeldIsSweptOnTheNextTick() throws Exception {
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile orphan = stage(table, orders, "connect-a");
    TableDrainState state = drainState(table, orders);
    StagingCleanupScheduler scheduler =
        scheduler("connect-a", SWEEP_INTERVAL_MS, () -> true, state);

    long firstTick = pastTtl();
    ExecutorService transition = Executors.newSingleThreadExecutor();
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch mayUnlock = new CountDownLatch(1);
    try {
      transition.execute(
          () -> {
            state.lock.lock();
            try {
              locked.countDown();
              mayUnlock.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              state.lock.unlock();
            }
          });
      assertThat(locked.await(30, TimeUnit.SECONDS)).as("the transition holds the lock").isTrue();

      scheduler.maybeClean(firstTick, ImmutableList::of);

      assertThat(exists(table, orphan.location()))
          .as("mid-transition: whatever the table is doing, its files are live")
          .isTrue();
    } finally {
      mayUnlock.countDown();
      transition.shutdown();
      assertThat(transition.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    // the skipped table did not spend its interval: the next tick is well inside it
    scheduler.maybeClean(firstTick + 1, ImmutableList::of);

    assertThat(exists(table, orphan.location())).as("swept once the lock is free").isFalse();
  }

  @Test
  public void testTheStopFlagIsCheckedBetweenTables() {
    // the coordinator is stopping mid-pass: the table already reached is swept, the rest is left
    // for the next coordinator: another may already be handing out its slices
    TableReference orders = reference(Namespace.of("db"), "orders");
    TableReference items = reference(Namespace.of("db"), "items");
    Table ordersTable = table("orders");
    Table itemsTable = table("items");
    StagedChangeFile ordersOrphan = stage(ordersTable, orders, "connect-a");
    StagedChangeFile itemsOrphan = stage(itemsTable, items, "connect-a");
    AtomicInteger checks = new AtomicInteger();
    // the flag is read both between tables and before each delete: false for the first
    // table's own two reads (between tables, then before its one delete), true from there on
    BooleanSupplier stopped = () -> checks.getAndIncrement() > 1;

    scheduler(
            "connect-a",
            0L,
            () -> true,
            stopped,
            drainState(ordersTable, orders),
            drainState(itemsTable, items))
        .maybeClean(pastTtl(), ImmutableList::of);

    assertThat(exists(ordersTable, ordersOrphan.location()))
        .as("the first table in the pass, before the flag was seen")
        .isFalse();
    assertThat(exists(itemsTable, itemsOrphan.location()))
        .as("the coordinator is stopping: the rest of the pass waits for the next one")
        .isTrue();
  }

  @Test
  public void testASecondSweepIsSkippedWhileThePreviousPassIsStillRunning() throws Exception {
    // one task per pass, not one per table: a slow pass must not queue a second one behind it on
    // the pool copy-on-write's slice transitions share. Two different tables, so the
    // first pass's own listing of its own prefix can never touch the second table's file: with
    // one shared prefix a delayed pass would sweep it too regardless of the guard, since the age
    // check uses the tick that scheduled the pass, not the wall-clock time it finally runs at
    TableReference orders = reference(Namespace.of("db"), "orders");
    TableReference items = reference(Namespace.of("db"), "items");
    Table ordersTable = table("orders");
    Table itemsTable = table("items");
    StagedChangeFile firstOrphan = stage(ordersTable, orders, "connect-a");
    TableDrainState ordersState = drainState(ordersTable, orders);
    // deferred: given a table only once the first pass is safely blocked mid-flight
    TableDrainState itemsState = new TableDrainState(items);

    ExecutorService pool = Executors.newSingleThreadExecutor();
    CountDownLatch runningFirstPass = new CountDownLatch(1);
    CountDownLatch mayFinishFirstPass = new CountDownLatch(1);
    Executor gate =
        command ->
            pool.execute(
                () -> {
                  runningFirstPass.countDown();
                  try {
                    mayFinishFirstPass.await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  command.run();
                });

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn("connect-a");
    when(config.copyOnWriteStagingSweepIntervalMs()).thenReturn(0L);
    when(config.copyOnWriteStagingOrphanTtlMs()).thenReturn(ORPHAN_TTL_MS);
    StagingCleanupScheduler scheduler =
        new StagingCleanupScheduler(
            config,
            new ChangeSetStore(config),
            gate,
            ImmutableList.of(ordersState, itemsState),
            () -> true,
            () -> false,
            identifier -> tables.load(locationOf(identifier, ordersState, itemsState)));

    StagedChangeFile secondOrphan;
    try {
      scheduler.maybeClean(pastTtl(), ImmutableList::of);
      assertThat(runningFirstPass.await(30, TimeUnit.SECONDS))
          .as("the first pass has started and is blocked mid-flight")
          .isTrue();

      // a second table joins the tables being swept while the first pass is still running: whether
      // it is swept now or waits for the next tick is exactly what the guard decides
      itemsState.table = itemsTable;
      itemsState.stagingLocation = staging;
      secondOrphan = stage(itemsTable, items, "connect-a");
      scheduler.maybeClean(pastTtl() + 1, ImmutableList::of);
    } finally {
      mayFinishFirstPass.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(exists(ordersTable, firstOrphan.location()))
        .as("the first pass ran to completion once unblocked")
        .isFalse();
    assertThat(exists(itemsTable, secondOrphan.location()))
        .as("a pass was already running: the second table was never queued")
        .isTrue();
  }

  @Test
  public void testCleanupFailureStaysWarnedUntilASuccessfulPassResetsIt() {
    // a hanging pointer or a missing manifest fails the same way every pass; only the first of a
    // run of failures is worth a WARN, and a fix is worth one again
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    StagedChangeFile live = stage(table, orders, "connect-a");
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            ImmutableList.of(live),
            ImmutableSet.of("orders"),
            ImmutableMap.of(),
            null);
    String manifestLocation = manifestLocation(orders, "connect-a", manifest);
    manifest.write(table.io(), manifestLocation);
    table
        .newAppend()
        .set(COMMIT_ID_PROP, UUID.randomUUID().toString())
        .set(offsetsProp("connect-a"), "{\"0\":1}")
        .set(CHANGE_SET_ID_PROP, manifest.changeSetId().toString())
        .commit();
    TableDrainState state = drainState(table, orders);
    StagingCleanupScheduler scheduler = scheduler("connect-a", 0L, () -> true, state);

    scheduler.maybeClean(pastTtl(), ImmutableList::of);
    assertThat(scheduler.hasWarnedOfCleanupFailure(orders))
        .as("the manifest the pointer names reads fine: nothing to warn about")
        .isFalse();

    // the pointer now names a manifest that was never written back
    table.io().deleteFile(manifestLocation);

    scheduler.maybeClean(pastTtl() + 1, ImmutableList::of);
    assertThat(scheduler.hasWarnedOfCleanupFailure(orders))
        .as("the first failed pass warns")
        .isTrue();

    scheduler.maybeClean(pastTtl() + 2, ImmutableList::of);
    assertThat(scheduler.hasWarnedOfCleanupFailure(orders))
        .as("still failing the same way: the next WARN stays suppressed")
        .isTrue();

    // fixed: the manifest is back
    manifest.write(table.io(), manifestLocation);
    scheduler.maybeClean(pastTtl() + 3, ImmutableList::of);

    assertThat(scheduler.hasWarnedOfCleanupFailure(orders))
        .as("a successful pass re-arms the WARN")
        .isFalse();
  }

  @Test
  public void testFileIoWithoutPrefixSupportNeverReadsTheSummaryOrTheManifest() {
    // checked before the summary or the manifest is read, and every pass, not only the first
    TableReference orders = reference(Namespace.of("db"), "orders");
    Table table = table("orders");
    Table unsupportedIo = spy(table);
    when(unsupportedIo.io()).thenReturn(new NonPrefixFileIO());

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn("connect-a");
    when(config.copyOnWriteStagingSweepIntervalMs()).thenReturn(0L);
    when(config.copyOnWriteStagingOrphanTtlMs()).thenReturn(ORPHAN_TTL_MS);
    AtomicInteger summaryReads = new AtomicInteger();
    ChangeSetStore store =
        new ChangeSetStore(config) {
          @Override
          Map<String, String> latestConnectorSummary(Table t, String branch) {
            summaryReads.incrementAndGet();
            return super.latestConnectorSummary(t, branch);
          }
        };
    TableDrainState state = drainState(unsupportedIo, orders);
    StagingCleanupScheduler scheduler =
        new StagingCleanupScheduler(
            config,
            store,
            Runnable::run,
            ImmutableList.of(state),
            () -> true,
            () -> false,
            identifier -> unsupportedIo);

    scheduler.maybeClean(pastTtl(), ImmutableList::of);
    scheduler.maybeClean(pastTtl() + 1, ImmutableList::of);

    assertThat(summaryReads.get())
        .as("an unsupported FileIO is caught before the summary is read, every pass")
        .isZero();
  }

  private Table table(String name) {
    return tables.create(SCHEMA, PartitionSpec.unpartitioned(), "file:" + dir.resolve(name));
  }

  private static TableReference reference(Namespace namespace, String name) {
    return TableReference.of("catalog", TableIdentifier.of(namespace, name));
  }

  private StagedChangeFile stage(Table table, TableReference tableReference, String groupId) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(table, tableReference, ID_FIELDS, staging, groupId, TASK_ID);
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", groupId);
    writer.write(writer.stagedRow(row, StagedChangeSchema.OP_UPDATE, "orders", 0, 0L));
    List<StagedChangeFile> files = writer.complete();
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private String freeze(
      Table table, TableReference tableReference, String groupId, List<StagedChangeFile> files) {
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table, ID_FIELDS, files, ImmutableSet.of("orders"), ImmutableMap.of(0, 1L), null);
    String location = manifestLocation(tableReference, groupId, manifest);
    manifest.write(table.io(), location);
    return location;
  }

  private String manifestLocation(
      TableReference tableReference, String groupId, ChangeSetManifest manifest) {
    return ChangeSetManifest.location(staging, tableReference, groupId, manifest.changeSetId());
  }

  /** A committed response of a cycle whose change set has not been frozen yet. */
  private static Envelope response(TableReference tableReference, StagedChangeFile... stagedFiles) {
    return new Envelope(
        new Event(
            "connect-a",
            new RowChangesWritten(
                UUID.randomUUID(),
                tableReference,
                TASK_ID,
                ImmutableList.of("orders"),
                ImmutableList.copyOf(stagedFiles))),
        0,
        0L);
  }

  /** The normalized file of one slice, spelled as the drain spells it. */
  private String normalized(
      TableReference tableReference, String groupId, UUID changeSetId, int sliceSeq) {
    return CopyOnWriteTableCommitter.normalizedLocation(
        staging, tableReference, groupId, changeSetId, sliceSeq);
  }

  private static void write(Table table, String location) {
    try (PositionOutputStream out = table.io().newOutputFile(location).create()) {
      out.write(new byte[] {1});
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String offsetsProp(String groupId) {
    return "kafka.connect.offsets." + CTL_TOPIC + "." + groupId;
  }

  private TableDrainState drainState(Table table, TableReference tableReference) {
    TableDrainState state = new TableDrainState(tableReference);
    state.table = table;
    state.stagingLocation = staging;
    return state;
  }

  /** One sweep of one table by the connector {@code groupId}, far enough past the orphan TTL. */
  private void sweep(TableDrainState state, String groupId) {
    scheduler(groupId, 0L, () -> true, state).maybeClean(pastTtl(), ImmutableList::of);
  }

  /**
   * A scheduler over the given tables, with the sweep interval and the catch-up mark of a test.
   *
   * <p>It loads a table by name the way a catalog does: whatever is at the location of the given
   * state's table now, read afresh.
   */
  private StagingCleanupScheduler scheduler(
      String groupId,
      long sweepIntervalMs,
      BooleanSupplier controlTopicCaughtUp,
      TableDrainState... states) {
    return scheduler(groupId, sweepIntervalMs, controlTopicCaughtUp, () -> false, states);
  }

  /** A scheduler whose stop flag a test controls, in addition to the sweep interval and gate. */
  private StagingCleanupScheduler scheduler(
      String groupId,
      long sweepIntervalMs,
      BooleanSupplier controlTopicCaughtUp,
      BooleanSupplier stopped,
      TableDrainState... states) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC);
    when(config.connectGroupId()).thenReturn(groupId);
    when(config.copyOnWriteStagingSweepIntervalMs()).thenReturn(sweepIntervalMs);
    when(config.copyOnWriteStagingOrphanTtlMs()).thenReturn(ORPHAN_TTL_MS);
    return new StagingCleanupScheduler(
        config,
        new ChangeSetStore(config),
        Runnable::run,
        ImmutableList.copyOf(states),
        controlTopicCaughtUp,
        stopped,
        identifier -> tables.load(locationOf(identifier, states)));
  }

  /** Where the table of that name lives: a state is given its table only once it is loaded. */
  private static String locationOf(TableIdentifier identifier, TableDrainState... states) {
    return Arrays.stream(states)
        .filter(state -> state.tableReference.identifier().equals(identifier))
        .filter(state -> state.table != null)
        .map(state -> state.table.location())
        .findFirst()
        .orElseThrow();
  }

  /** A tick far enough past the orphan TTL for anything already staged to be an orphan. */
  private static long pastTtl() {
    return System.currentTimeMillis() + 10 * ORPHAN_TTL_MS;
  }

  private static boolean exists(Table table, String location) {
    return table.io().newInputFile(location).exists();
  }

  /** A {@link FileIO} that cannot list a prefix, unlike the real one every table here uses. */
  private static final class NonPrefixFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}
