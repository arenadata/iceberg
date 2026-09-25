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

import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes copy-on-write staging files that no change set can reach any more.
 *
 * <p>The immediate half of cleanup belongs to the commit path, which deletes a change set's staged
 * files and manifest the moment it is drained. This is the other half: whatever an aborted Kafka
 * transaction, a lost coordinator or a cancelled slice left behind, and which therefore no manifest
 * will ever name.
 *
 * <p><b>Reachability, not age, decides what may be deleted</b>, which is why this is an orphan
 * cleaner and not a staging cleaner: age only decides <i>when</i> an already unreachable file goes.
 * A file the caller lists as live is never touched no matter how old, and a file younger than the
 * TTL is never touched even though nothing references it: it may belong to a change set being
 * frozen right now, whose manifest is not yet written.
 *
 * <p>Needs {@link SupportsPrefixOperations} to enumerate the staging prefix. A {@code FileIO}
 * without it (some Hadoop setups) cannot be cleaned: the pass is skipped with a WARN, and the
 * operator falls back to a storage-side lifecycle rule on the staging location.
 */
public final class StagingOrphanCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(StagingOrphanCleaner.class);

  private StagingOrphanCleaner() {}

  /** What one pass over one table's staging prefix did. */
  public static final class CleanupResult {
    private final int deleted;
    private final int kept;
    private final boolean supported;
    private final boolean cancelled;

    private CleanupResult(int deleted, int kept, boolean supported, boolean cancelled) {
      this.deleted = deleted;
      this.kept = kept;
      this.supported = supported;
      this.cancelled = cancelled;
    }

    public int deleted() {
      return deleted;
    }

    public int kept() {
      return kept;
    }

    /** False if the {@code FileIO} cannot list a prefix, so nothing was examined. */
    public boolean supported() {
      return supported;
    }

    /**
     * True if the pass was given up on because the listing agreed with nothing the connector holds
     * live, so that a storage spelling its locations differently cannot make live files look like
     * orphans. Nothing was deleted.
     */
    public boolean cancelled() {
      return cancelled;
    }
  }

  /**
   * The live locations a listing of this prefix is supposed to return.
   *
   * <p>The caller's set covers every table its connector drains (the commit buffer names the staged
   * files of all of them) and only the ones under this prefix say anything about whether this
   * listing agrees with what the connector holds.
   */
  private static Set<String> liveUnder(String directoryPrefix, Set<String> liveLocations) {
    Set<String> under = Sets.newHashSet();
    for (String location : liveLocations) {
      if (location.startsWith(directoryPrefix)) {
        under.add(location);
      }
    }
    return under;
  }

  /**
   * Deletes the orphans under one connector's staging prefix of one table.
   *
   * @param stagingPrefix the connector's staging directory of the table, {@code
   *     <staging>/<tableDir>/<groupDir>} as {@code StagedChangeFileWriter.stagingDirectory} spells
   *     it; a trailing separator is added if missing, because prefix listing matches a plain string
   *     and a table named {@code orders} would otherwise reach the files of {@code orders_archive}
   * @param liveLocations files an active change set still reaches: its staged files, its manifest
   *     and the normalized slices written for it
   * @param stopped the coordinator's stop flag, checked before every delete so that a shutdown mid
   *     pass starts no new delete; orphans not yet reached are left for the next coordinator's pass
   */
  public static CleanupResult cleanOrphans(
      FileIO io,
      String stagingPrefix,
      Set<String> liveLocations,
      long orphanTtlMs,
      long nowMs,
      BooleanSupplier stopped) {
    if (!(io instanceof SupportsPrefixOperations)) {
      LOG.warn(
          "FileIO {} does not support prefix listing, skipping copy-on-write orphan cleanup of {}; "
              + "configure a storage lifecycle rule on that location instead",
          io.getClass().getName(),
          stagingPrefix);
      return new CleanupResult(0, 0, false, false);
    }

    String directoryPrefix = stagingPrefix.endsWith("/") ? stagingPrefix : stagingPrefix + "/";
    Set<String> expected = liveUnder(directoryPrefix, liveLocations);

    // nothing is deleted while the listing is still being read: a listing that names none of the
    // live files is a listing spelled differently from what the connector holds, and every file
    // under the prefix then looks like an orphan
    boolean listingAgrees = expected.isEmpty();
    List<String> orphans = Lists.newArrayList();
    int kept = 0;
    for (FileInfo file : ((SupportsPrefixOperations) io).listPrefix(directoryPrefix)) {
      listingAgrees = listingAgrees || expected.contains(file.location());
      if (liveLocations.contains(file.location())
          || nowMs - file.createdAtMillis() <= orphanTtlMs) {
        kept += 1;
      } else {
        orphans.add(file.location());
      }
    }

    if (!listingAgrees) {
      LOG.warn(
          "Copy-on-write staging cleanup of {} cancelled: the listing returned {} file(s) and none "
              + "of the {} file(s) still in use, such as {}. The storage spells listed locations "
              + "differently from the ones this connector wrote, and deleting by them would delete "
              + "live data",
          stagingPrefix,
          kept + orphans.size(),
          expected.size(),
          expected.iterator().next());
      return new CleanupResult(0, kept + orphans.size(), true, true);
    }

    int deleted = 0;
    for (int i = 0; i < orphans.size(); i++) {
      if (stopped.getAsBoolean()) {
        // the coordinator is stopping: another may already be handing out slices of the same
        // tables, so no delete starts from here on. What is left is an orphan for the next pass
        kept += orphans.size() - i;
        break;
      }
      String location = orphans.get(i);
      try {
        io.deleteFile(location);
        deleted += 1;
      } catch (RuntimeException e) {
        // a concurrent cleanup or a retried delete: not worth failing the coordinator's cycle over
        LOG.warn("Failed to delete orphaned copy-on-write staging file: {}", location, e);
        kept += 1;
      }
    }

    if (deleted > 0) {
      LOG.info(
          "Deleted {} orphaned copy-on-write staging file(s) under {}, kept {}",
          deleted,
          stagingPrefix,
          kept);
    }
    return new CleanupResult(deleted, kept, true, false);
  }
}
