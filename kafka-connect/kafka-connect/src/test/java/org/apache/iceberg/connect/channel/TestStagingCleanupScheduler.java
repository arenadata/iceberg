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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.WarningCapture;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The staging cleanup of the coordinator on a source the test scripts: when a table is due, what it
 * reaches, and that nothing is deleted on a partial view or once the coordinator stops. The cleanup
 * of copy-on-write on its own source is {@link TestStagingLayout}'s.
 */
public class TestStagingCleanupScheduler {

  private static final String GROUP_ID = "connect-a";
  private static final String STAGING = "s3://bucket/staging";
  private static final long TTL_MS = 1_000L;
  private static final long INTERVAL_MS = 3_600_000L;
  private static final long NOW = 100_000_000L;
  private static final long OLD = NOW - TTL_MS - 1;
  private static final UUID ORDERS_UUID = UUID.randomUUID();
  private static final TableReference ORDERS =
      TableReference.of("catalog", TableIdentifier.of("db", "orders"), ORDERS_UUID);
  private static final String DIR =
      StagedChangeFileWriter.stagingDirectory(STAGING, ORDERS, GROUP_ID);

  private IcebergSinkConfig config;
  private ListableIO io;
  private StubSource source;
  private final AtomicBoolean caughtUp = new AtomicBoolean(true);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  @BeforeEach
  public void before() {
    config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn(GROUP_ID);
    when(config.stagingOrphanTtlMs()).thenReturn(TTL_MS);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(INTERVAL_MS);
    io = new ListableIO();
    source = new StubSource();
    source.offer(ORDERS, table(ORDERS_UUID), DIR);
  }

  @Test
  public void tableSourceCannotReadNowIsCleanedUpOnNextTickWithoutWaitingOutInterval() {
    String orphan = io.put(DIR + "/task-0/epoch/0.avro", OLD);
    source.busy.add(ORDERS);
    StagingCleanupScheduler scheduler = scheduler();

    scheduler.maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(orphan)).as("the source could not read the table: no pass").isTrue();
    assertThat(source.snapshots).hasValue(1);

    source.busy.clear();
    scheduler.maybeClean(NOW + 1, ImmutableList::of);

    assertThat(io.exists(orphan))
        .as("a snapshot of null did not spend the interval of the table")
        .isFalse();
  }

  @Test
  public void bufferIsProjectedOnlyWhenTableIsDue() {
    AtomicInteger projections = new AtomicInteger();
    Supplier<Collection<Envelope>> buffer =
        () -> {
          projections.incrementAndGet();
          return ImmutableList.of();
        };
    StagingCleanupScheduler scheduler = scheduler();

    caughtUp.set(false);
    scheduler.maybeClean(NOW, buffer);
    assertThat(projections).as("the control topic is not read to its end").hasValue(0);

    caughtUp.set(true);
    source.tables.clear();
    scheduler.maybeClean(NOW, buffer);
    assertThat(projections).as("the source offers no table").hasValue(0);

    source.tables.add(ORDERS);
    scheduler.maybeClean(NOW, buffer);
    assertThat(projections).as("the table is due").hasValue(1);

    scheduler.maybeClean(NOW + INTERVAL_MS - 1, buffer);
    assertThat(projections).as("cleaned up within the interval: no table is due").hasValue(1);

    scheduler.maybeClean(NOW + INTERVAL_MS, buffer);
    assertThat(projections).as("an interval later").hasValue(2);
  }

  @Test
  public void bufferIsNotProjectedWhileSourceCannotReadAnyDueTable() {
    // a drain holding the lock of a copy-on-write table for minutes keeps the table due on every
    // tick, and the buffer may hold hundreds of thousands of envelopes by then
    AtomicInteger projections = new AtomicInteger();
    Supplier<Collection<Envelope>> buffer =
        () -> {
          projections.incrementAndGet();
          return ImmutableList.of();
        };
    source.busy.add(ORDERS);
    StagingCleanupScheduler scheduler = scheduler();

    scheduler.maybeClean(NOW, buffer);
    scheduler.maybeClean(NOW + 1, buffer);

    assertThat(projections)
        .as("no table could be read: nothing to project the buffer for")
        .hasValue(0);
    assertThat(source.snapshots).hasValue(2);

    source.busy.clear();
    scheduler.maybeClean(NOW + 2, buffer);

    assertThat(projections).as("the table is read now").hasValue(1);
  }

  @Test
  public void tableSourceStopsOfferingTakesItsTimerWithIt() {
    // a table dropped and created again under the same name is cleaned up as soon as it is ready
    StagingCleanupScheduler scheduler = scheduler();
    scheduler.maybeClean(NOW, ImmutableList::of);
    String orphan = io.put(DIR + "/task-0/epoch/0.avro", OLD);

    source.tables.clear();
    scheduler.maybeClean(NOW + 1, ImmutableList::of);
    source.tables.add(ORDERS);
    scheduler.maybeClean(NOW + 2, ImmutableList::of);

    assertThat(io.exists(orphan)).as("cleaned up again well within the interval").isFalse();
  }

  @Test
  public void bufferReachesStagedFilesAsAnchorsAndPuffinFilesAsOtherLiveFiles() {
    String staged = DIR + "/task-0/epoch/0.avro";
    String ofRowChanges = DIR + "/stats/commit-task-0.puffin";
    String ofDataWritten = DIR + "/stats/commit-task-1.puffin";

    ReachableFileSet buffered =
        StagingCleanupScheduler.bufferedReachable(
            ImmutableList.of(
                envelope(rowChangesWritten(ImmutableList.of(staged), ofRowChanges)),
                envelope(dataWritten(ofDataWritten)),
                envelope(dataWritten(null)),
                envelope(rowChangesWritten(ImmutableList.of(), null)),
                envelope(new DataComplete(UUID.randomUUID(), ImmutableList.of()))));

    assertThat(buffered.anchors()).containsExactly(staged);
    assertThat(buffered.live()).containsExactlyInAnyOrder(staged, ofRowChanges, ofDataWritten);
  }

  @Test
  public void puffinFileOfBufferedResponseIsKept() {
    // a response in the buffer names its Puffin file from the moment it arrives
    String buffered = io.put(DIR + "/stats/commit-task-0.puffin", OLD);
    String orphan = io.put(DIR + "/stats/commit-task-2.puffin", OLD);

    scheduler().maybeClean(NOW, () -> ImmutableList.of(envelope(dataWritten(buffered))));

    assertThat(io.exists(buffered)).as("named by a response in the buffer").isTrue();
    assertThat(io.exists(orphan)).as("named by nothing: the pass ran").isFalse();
  }

  @Test
  public void bufferedPuffinFileItsStepDeletedCancelsNoPass() {
    // a Puffin file its step deleted after the buffer was read is not listed. It is no anchor,
    // and with no anchor under the prefix the pass goes on by the TTL
    String deletedByItsStep = DIR + "/stats/commit-task-1.puffin";
    String orphan = io.put(DIR + "/stats/commit-task-2.puffin", OLD);

    scheduler().maybeClean(NOW, () -> ImmutableList.of(envelope(dataWritten(deletedByItsStep))));

    assertThat(io.exists(orphan)).as("named by nothing: the pass ran").isFalse();
  }

  @Test
  public void anchorsInMemoryDecideWhetherListingAgrees() {
    // the storage spells the staged file differently: the listing names a live Puffin file, but
    // none of the anchors, and deleting by it would delete live data
    String anchor = DIR + "/task-0/epoch/0.avro";
    io.put(DIR + "//task-0/epoch/0.avro", OLD);
    String puffin = io.put(DIR + "/stats/commit-task-0.puffin", OLD);
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    source.inMemory.addAnchor(anchor);
    source.inMemory.addLive(puffin);

    scheduler().maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(orphan)).as("the pass is cancelled").isTrue();
    assertThat(io.exists(puffin)).isTrue();
  }

  @Test
  public void persistedStateOfSourceIsReadForPassAndKept() {
    String named = io.put(DIR + "/_changesets/" + UUID.randomUUID() + ".json", OLD);
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    source.persisted = into -> into.addAnchor(named);

    scheduler().maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(named)).as("named by the persisted state").isTrue();
    assertThat(io.exists(orphan)).isFalse();
    assertThat(source.persistedReads).hasValue(1);
  }

  @Test
  public void failedReadOfPersistedStateDeletesNothingAndWarnsOnce() {
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    when(config.stagingOrphanCleanupIntervalMs()).thenReturn(0L);
    source.persisted =
        into -> {
          throw new IllegalStateException("the manifest cannot be read");
        };
    StagingCleanupScheduler scheduler = scheduler();

    scheduler.maybeClean(NOW, ImmutableList::of);
    assertThat(io.exists(orphan)).as("never on a partial view of what is reachable").isTrue();
    assertThat(scheduler.hasWarnedOfCleanupFailure(ORDERS)).isTrue();

    scheduler.maybeClean(NOW + 1, ImmutableList::of);
    assertThat(io.exists(orphan)).isTrue();
    assertThat(scheduler.hasWarnedOfCleanupFailure(ORDERS))
        .as("still failing: the next WARN stays suppressed")
        .isTrue();

    source.persisted = into -> {};
    scheduler.maybeClean(NOW + 2, ImmutableList::of);
    assertThat(io.exists(orphan)).isFalse();
    assertThat(scheduler.hasWarnedOfCleanupFailure(ORDERS))
        .as("a successful pass re-arms the WARN")
        .isFalse();
  }

  @Test
  public void replacedTableIsNotCleanedUp() {
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    source.loaded.put(ORDERS.identifier(), table(UUID.randomUUID()));

    scheduler().maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(orphan)).as("the prefix is spelled from the name alone").isTrue();
    assertThat(source.persistedReads).as("nothing read for a table that is gone").hasValue(0);
  }

  @Test
  public void onlyCleanupPrefixOfSourceIsCleanedUp() {
    String inside = io.put(DIR + "/stats/commit-task-0.puffin", OLD);
    String outside = io.put(DIR + "/task-0/epoch/0.avro", OLD);
    source.cleanupPrefix = DIR + "/stats/";

    scheduler().maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(inside)).isFalse();
    assertThat(io.exists(outside)).as("outside the prefix the source cleans up").isTrue();
  }

  @Test
  public void cleanupPrefixOutsideStagingDirectoryOfConnectorCleansUpNothing() {
    String otherGroup = StagedChangeFileWriter.stagingDirectory(STAGING, ORDERS, GROUP_ID + "b");
    String ofTheOtherGroup = io.put(otherGroup + "/task-0/epoch/0.avro", OLD);
    source.cleanupPrefix = otherGroup;
    StagingCleanupScheduler scheduler = scheduler();

    scheduler.maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(ofTheOtherGroup)).as("the staging of another connector").isTrue();
    assertThat(scheduler.hasWarnedOfCleanupFailure(ORDERS)).isTrue();
  }

  @Test
  public void nothingIsDeletedOnceStopFlagIsUp() {
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    stopped.set(true);

    scheduler().maybeClean(NOW, ImmutableList::of);

    assertThat(io.exists(orphan)).isTrue();
  }

  @Test
  public void passDuringWhichStopFlagRisesDeletesNothingAfterIt() {
    // the flag rises while the pass reads the persisted state, after the check before the table
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    source.persisted = into -> stopped.set(true);

    scheduler().maybeClean(NOW, ImmutableList::of);

    assertThat(source.persistedReads).as("the pass reached the table").hasValue(1);
    assertThat(io.listed).as("and listed its prefix").hasValue(1);
    assertThat(io.exists(orphan)).as("no delete starts after the flag").isTrue();
  }

  @Test
  public void readCutShortByShutdownIsNoFailureOfPass() {
    // terminate() interrupts the pool, and the catalog or the storage answers with an exception
    // once the flag is up: the pass is given up on, not failed
    String orphan = io.put(DIR + "/task-0/epoch/1.avro", OLD);
    source.persisted =
        into -> {
          stopped.set(true);
          throw new UncheckedIOException(new InterruptedIOException("interrupted"));
        };
    StagingCleanupScheduler scheduler = scheduler();

    try (WarningCapture warnings = WarningCapture.capture()) {
      scheduler.maybeClean(NOW, ImmutableList::of);

      assertThat(source.persistedReads).as("the pass reached the read").hasValue(1);
      assertThat(warnings.lines()).as("a stop is no failure").isEmpty();
    }

    assertThat(io.exists(orphan)).isTrue();
    assertThat(scheduler.hasWarnedOfCleanupFailure(ORDERS))
        .as("the next failure of a running coordinator is still worth its WARN")
        .isFalse();
  }

  @Test
  public void readThatFailsWhileCoordinatorRunsIsWarnedOf() {
    // the control of the test above: the same exception with no stop is a WARN
    source.persisted =
        into -> {
          throw new UncheckedIOException(new InterruptedIOException("not a stop"));
        };
    StagingCleanupScheduler scheduler = scheduler();

    try (WarningCapture warnings = WarningCapture.capture()) {
      scheduler.maybeClean(NOW, ImmutableList::of);

      assertThat(warnings.lines())
          .anyMatch(line -> line.contains("Staging cleanup of") && line.contains("failed"));
    }

    assertThat(scheduler.hasWarnedOfCleanupFailure(ORDERS)).isTrue();
  }

  private StagingCleanupScheduler scheduler() {
    BooleanSupplier isStopped = stopped::get;
    return new StagingCleanupScheduler(config, Runnable::run, source, caughtUp::get, isStopped);
  }

  private Table table(UUID uuid) {
    Table table = mock(Table.class);
    when(table.uuid()).thenReturn(uuid);
    when(table.io()).thenReturn(io);
    when(table.name()).thenReturn("catalog.db.orders");
    return table;
  }

  private static Envelope envelope(Payload payload) {
    return new Envelope(new Event(GROUP_ID, payload), 0, 0L);
  }

  private static RowChangesWritten rowChangesWritten(List<String> staged, String puffin) {
    List<StagedChangeFile> files = Lists.newArrayList();
    for (String location : staged) {
      files.add(
          new StagedChangeFile(location, 10L, 1L, 0, 1, ImmutableMap.of(), ImmutableMap.of()));
    }
    return new RowChangesWritten(
        UUID.randomUUID(),
        ORDERS,
        "task-0",
        ImmutableList.of("src"),
        files,
        puffin == null ? null : new SketchFile(puffin, 100L, 10L));
  }

  private static DataWritten dataWritten(String puffin) {
    return new DataWritten(
        Types.StructType.of(),
        UUID.randomUUID(),
        ORDERS,
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of("src"),
        puffin == null ? null : new SketchFile(puffin, 100L, 10L));
  }

  /** A source whose one table, its entry and its persisted state the test scripts. */
  private final class StubSource implements StagingCleanupSource {
    private final List<TableReference> tables = Lists.newArrayList();
    private final Set<TableReference> busy = Sets.newHashSet();
    private final Map<TableIdentifier, Table> loaded = Maps.newHashMap();
    private final ReachableFileSet inMemory = new ReachableFileSet();
    private final AtomicInteger snapshots = new AtomicInteger();
    private final AtomicInteger persistedReads = new AtomicInteger();
    private Table table;
    private String cleanupPrefix;
    private Consumer<ReachableFileSet> persisted = into -> {};

    void offer(TableReference tableReference, Table offered, String prefix) {
      tables.add(tableReference);
      loaded.put(tableReference.identifier(), offered);
      this.table = offered;
      this.cleanupPrefix = prefix;
    }

    @Override
    public Collection<TableReference> tables() {
      return ImmutableList.copyOf(tables);
    }

    @Override
    public CleanupTarget snapshot(TableReference tableReference) {
      snapshots.incrementAndGet();
      if (busy.contains(tableReference)) {
        return null;
      }
      ReachableFileSet copy = new ReachableFileSet();
      copy.addAll(inMemory);
      return new CleanupTarget(
          table.name(), table.uuid(), null, tableReference, STAGING, cleanupPrefix, copy);
    }

    @Override
    public Table load(TableIdentifier identifier) {
      return loaded.get(identifier);
    }

    @Override
    public void addPersistedReachable(
        Table loadedTable, CleanupTarget target, ReachableFileSet into) {
      persistedReads.incrementAndGet();
      persisted.accept(into);
    }
  }

  /** An in-memory {@link FileIO} that can list a prefix and report an age per file. */
  private static final class ListableIO implements FileIO, SupportsPrefixOperations {
    private final Map<String, Long> createdAt = Maps.newLinkedHashMap();
    private final AtomicInteger listed = new AtomicInteger();

    String put(String location, long createdAtMillis) {
      createdAt.put(location, createdAtMillis);
      return location;
    }

    boolean exists(String location) {
      return createdAt.containsKey(location);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      listed.incrementAndGet();
      List<FileInfo> files = Lists.newArrayList();
      createdAt.forEach(
          (location, created) -> {
            if (location.startsWith(prefix)) {
              files.add(new FileInfo(location, 1L, created));
            }
          });
      return files;
    }

    @Override
    public void deletePrefix(String prefix) {
      createdAt.keySet().removeIf(location -> location.startsWith(prefix));
    }

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
      createdAt.remove(path);
    }
  }
}
