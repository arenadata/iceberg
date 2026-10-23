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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.iceberg.connect.WarningCapture;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestStagingOrphanCleaner {

  private static final String PREFIX = "s3://bucket/staging/db.tbl";
  private static final long TTL_MS = 1000L;
  private static final long NOW = 10_000L;

  @Test
  public void testUnreachableFilePastTtlIsDeleted() {
    ListableIO io = new ListableIO();
    io.put(PREFIX + "/task-0/epoch/0.avro", NOW - TTL_MS - 1);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io, PREFIX, ImmutableSet.of(), ImmutableSet.of(), TTL_MS, NOW, () -> false);

    assertThat(result.supported()).isTrue();
    assertThat(result.deleted()).isEqualTo(1);
    assertThat(io.locations()).isEmpty();
  }

  @Test
  public void testLiveFileIsKeptHoweverOld() {
    ListableIO io = new ListableIO();
    String live = PREFIX + "/task-0/epoch/0.avro";
    io.put(live, 0L);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io, PREFIX, ImmutableSet.of(live), ImmutableSet.of(live), TTL_MS, NOW, () -> false);

    assertThat(result.deleted()).isZero();
    assertThat(io.locations()).containsExactly(live);
  }

  @Test
  public void testUnreachableFileWithinTtlIsKept() {
    // a change set being frozen right now: its manifest names nothing yet, and deleting its staged
    // files would lose the rows outright
    ListableIO io = new ListableIO();
    String fresh = PREFIX + "/task-0/epoch/0.avro";
    io.put(fresh, NOW - TTL_MS + 1);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io, PREFIX, ImmutableSet.of(), ImmutableSet.of(), TTL_MS, NOW, () -> false);

    assertThat(result.deleted()).isZero();
    assertThat(io.locations()).containsExactly(fresh);
  }

  @Test
  public void testFilesOfUnknownAgeAreKeptAndWarnedOfOnce() {
    // a FileIO that cannot tell when a file was created reports 0 (GCSFileIO without a create
    // time): measured from the epoch every file is older than any TTL, and a Puffin file still
    // on its way in the control topic or a staged file of a change set being frozen would go
    ListableIO io = new ListableIO();
    String unknownFirst = PREFIX + "/task-0/epoch/0.avro";
    String unknownSecond = PREFIX + "/task-0/epoch/1.avro";
    String old = PREFIX + "/task-0/epoch/2.avro";
    io.put(unknownFirst, 0L);
    io.put(unknownSecond, -1L);
    io.put(old, NOW - TTL_MS - 1);

    try (WarningCapture warnings = WarningCapture.capture()) {
      StagingOrphanCleaner.CleanupResult result =
          StagingOrphanCleaner.cleanOrphans(
              io, PREFIX, ImmutableSet.of(), ImmutableSet.of(), TTL_MS, NOW, () -> false);

      assertThat(io.locations())
          .as("the files whose age is unknown are not deleted")
          .containsExactly(unknownFirst, unknownSecond);
      assertThat(result.deleted()).as("only the file of a known age").isEqualTo(1);
      assertThat(result.kept()).isEqualTo(2);
      assertThat(warnings.lines())
          .as("one WARN for the pass, not one per file")
          .filteredOn(line -> line.contains("age is unknown"))
          .hasSize(1);
    }
  }

  @Test
  public void testCleanupIsCancelledWhenListingNamesNotOneLiveFile() {
    // reachability is string equality between what the listing returns and what the writer, the
    // manifest and the change set were given. A storage that spells a listed location differently
    // (another scheme, a doubled separator, its own escaping) makes every live file look like
    // an orphan, and the drain loses the only copy of its rows
    ListableIO io = new ListableIO();
    String live = PREFIX + "/task-0/epoch/0.avro";
    io.put(PREFIX + "//task-0/epoch/0.avro", 0L);
    io.put(PREFIX + "/task-0/epoch/1.avro", NOW - TTL_MS - 1);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io, PREFIX, ImmutableSet.of(live), ImmutableSet.of(live), TTL_MS, NOW, () -> false);

    assertThat(result.cancelled()).isTrue();
    assertThat(result.deleted()).isZero();
    assertThat(io.locations())
        .as("nothing is deleted on a listing that agrees with nothing the connector holds")
        .containsExactly(PREFIX + "//task-0/epoch/0.avro", PREFIX + "/task-0/epoch/1.avro");
  }

  @Test
  public void testCleanupRunsWhenNothingLiveIsUnderTablePrefix() {
    // the live set is the whole connector's: the commit buffer names the staged files of every
    // table it drains. A table with no live file of its own is cleaned up, not cancelled
    ListableIO io = new ListableIO();
    String orphan = PREFIX + "/task-0/epoch/0.avro";
    io.put(orphan, NOW - TTL_MS - 1);

    String ofAnotherTable = "s3://bucket/staging/db.other/task-0/epoch/0.avro";
    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io,
            PREFIX,
            ImmutableSet.of(ofAnotherTable),
            ImmutableSet.of(ofAnotherTable),
            TTL_MS,
            NOW,
            () -> false);

    assertThat(result.cancelled()).isFalse();
    assertThat(result.deleted()).isEqualTo(1);
    assertThat(io.locations()).isEmpty();
  }

  @Test
  public void testLivePuffinFilePastTtlIsKept() {
    // an intermediate Puffin file a response or a step still names is live, though it is no
    // anchor. Deleting it would cost its step a recount of the gap it covers
    ListableIO io = new ListableIO();
    String puffin = PREFIX + "/stats/commit-task-0.puffin";
    String orphan = PREFIX + "/stats/commit-task-1.puffin";
    io.put(puffin, 0L);
    io.put(orphan, NOW - TTL_MS - 1);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io, PREFIX, ImmutableSet.of(puffin), ImmutableSet.of(), TTL_MS, NOW, () -> false);

    assertThat(result.cancelled()).isFalse();
    assertThat(result.deleted()).isEqualTo(1);
    assertThat(io.locations()).as("the live Puffin file, however old").containsExactly(puffin);
  }

  @Test
  public void testWithNoAnchorUnderPrefixPassRunsThoughNoLivePuffinFileIsListed() {
    // the live paths are read on the coordinator thread and the listing later on the pool; a step
    // that ends in between deletes its own Puffin files. A listing without them says nothing
    // about the storage: with no anchor to check it against, the pass goes on by the TTL
    ListableIO io = new ListableIO();
    String deletedByItsStep = PREFIX + "/stats/commit-task-0.puffin";
    String orphan = PREFIX + "/stats/commit-task-1.puffin";
    io.put(orphan, NOW - TTL_MS - 1);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io,
            PREFIX,
            ImmutableSet.of(deletedByItsStep),
            ImmutableSet.of(),
            TTL_MS,
            NOW,
            () -> false);

    assertThat(result.cancelled()).as("no anchor under the prefix: nothing to disagree").isFalse();
    assertThat(result.deleted()).isEqualTo(1);
    assertThat(io.locations()).isEmpty();
  }

  @Test
  public void testPassIsCancelledWhenListingNamesLivePuffinFileButNoAnchor() {
    // only the anchors say whether the listing is spelled as the connector wrote: a Puffin file
    // found by the listing does not save a pass whose anchors it misses
    ListableIO io = new ListableIO();
    String anchor = PREFIX + "/task-0/epoch/0.avro";
    String puffin = PREFIX + "/stats/commit-task-0.puffin";
    String orphan = PREFIX + "/task-0/epoch/1.avro";
    io.put(PREFIX + "//task-0/epoch/0.avro", 0L);
    io.put(puffin, 0L);
    io.put(orphan, NOW - TTL_MS - 1);

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io,
            PREFIX,
            ImmutableSet.of(anchor, puffin),
            ImmutableSet.of(anchor),
            TTL_MS,
            NOW,
            () -> false);

    assertThat(result.cancelled()).isTrue();
    assertThat(result.deleted()).isZero();
    assertThat(io.locations())
        .containsExactlyInAnyOrder(PREFIX + "//task-0/epoch/0.avro", puffin, orphan);
  }

  @Test
  public void testFileIoWithoutPrefixListingIsSkipped() {
    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            new InMemoryFileIO() {},
            PREFIX,
            ImmutableSet.of(),
            ImmutableSet.of(),
            TTL_MS,
            NOW,
            () -> false);

    assertThat(result.supported()).isFalse();
    assertThat(result.deleted()).isZero();
  }

  @Test
  public void testTheStoppedFlagIsCheckedBeforeEachDelete() {
    // the coordinator is stopping mid-pass: nothing already deleted comes back, but nothing past
    // that point starts either: another coordinator may already be handing out slices of the
    // same tables
    ListableIO io = new ListableIO();
    String first = PREFIX + "/task-0/epoch/0.avro";
    String second = PREFIX + "/task-0/epoch/1.avro";
    io.put(first, NOW - TTL_MS - 1);
    io.put(second, NOW - TTL_MS - 1);
    AtomicInteger checks = new AtomicInteger();
    // false for the first orphan, true from the second orphan on: exactly one delete gets through
    BooleanSupplier stopped = () -> checks.getAndIncrement() > 0;

    StagingOrphanCleaner.CleanupResult result =
        StagingOrphanCleaner.cleanOrphans(
            io, PREFIX, ImmutableSet.of(), ImmutableSet.of(), TTL_MS, NOW, stopped);

    assertThat(result.deleted()).as("one delete started before the flag was seen").isEqualTo(1);
    assertThat(io.locations())
        .as("the other orphan is left for the next coordinator's pass")
        .hasSize(1);
  }

  @Test
  public void testDeleteCutShortByShutdownIsNoFailureAndStartsNoOtherDelete() {
    // terminate() interrupts the pool and the storage answers with an exception, which is the
    // stop, not a failed delete
    ListableIO io = new ListableIO();
    io.put(PREFIX + "/task-0/epoch/0.avro", NOW - TTL_MS - 1);
    io.put(PREFIX + "/task-0/epoch/1.avro", NOW - TTL_MS - 1);
    AtomicBoolean stopped = new AtomicBoolean(false);
    AtomicInteger deletes = new AtomicInteger();
    io.onDelete =
        path -> {
          deletes.incrementAndGet();
          stopped.set(true);
          throw new UncheckedIOException(new InterruptedIOException("interrupted"));
        };

    try (WarningCapture warnings = WarningCapture.capture()) {
      StagingOrphanCleaner.CleanupResult result =
          StagingOrphanCleaner.cleanOrphans(
              io, PREFIX, ImmutableSet.of(), ImmutableSet.of(), TTL_MS, NOW, stopped::get);

      assertThat(result.deleted()).isZero();
      assertThat(result.kept()).as("both left for the next pass").isEqualTo(2);
      assertThat(deletes).as("no delete after the one that was cut short").hasValue(1);
      assertThat(warnings.lines()).as("a stop is no failure").isEmpty();
    }
  }

  @Test
  public void testFailedDeleteOfRunningCoordinatorIsStillWarnedOf() {
    // the control of the test above: the same exception with no stop is a WARN
    ListableIO io = new ListableIO();
    io.put(PREFIX + "/task-0/epoch/0.avro", NOW - TTL_MS - 1);
    io.onDelete =
        path -> {
          throw new UncheckedIOException(new InterruptedIOException("not a stop"));
        };

    try (WarningCapture warnings = WarningCapture.capture()) {
      StagingOrphanCleaner.cleanOrphans(
          io, PREFIX, ImmutableSet.of(), ImmutableSet.of(), TTL_MS, NOW, () -> false);

      assertThat(warnings.lines())
          .anyMatch(line -> line.contains("Failed to delete orphaned staging file"));
    }
  }

  /** An in-memory {@link FileIO} that can list a prefix and report an age per file. */
  private static final class ListableIO implements FileIO, SupportsPrefixOperations {
    private final Map<String, Long> createdAt = Maps.newLinkedHashMap();
    private Consumer<String> onDelete = path -> {};

    void put(String location, long createdAtMillis) {
      createdAt.put(location, createdAtMillis);
    }

    List<String> locations() {
      return Lists.newArrayList(createdAt.keySet());
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
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
      onDelete.accept(path);
      createdAt.remove(path);
    }
  }
}
