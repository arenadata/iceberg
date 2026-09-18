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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.ChangeSetManifest;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The persisted half of a change set: what is written to storage and read back from it.
 *
 * <p>Freezing responses into a manifest, recovering a half-applied change set from a snapshot
 * summary, and deleting a drained one. Nothing here knows about slices, workers or the control
 * channel: it is the IO the state machine sits on, and it is separate because a change set outlives
 * every coordinator that touches it: it belongs to the table, not to the control topic.
 *
 * <p>Also the one place the snapshot-summary keys are spelled, since reading them and writing them
 * happen in different classes ({@link SliceCommitter} writes).
 */
class ChangeSetStore {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeSetStore.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String COMMIT_ID_PROP = "kafka.connect.commit-id";
  static final String VALID_THROUGH_TS_PROP = "kafka.connect.valid-through-ts";
  static final String CHANGE_SET_ID_PROP = "kafka.connect.copy-on-write.change-set-id";
  static final String CHANGE_SET_CURSOR_PROP = "kafka.connect.copy-on-write.change-set-cursor";

  /**
   * Which slice of the drain produced a snapshot.
   *
   * <p>A drain's slices commit many snapshots, all carrying the commit id of the cycle that started
   * the drain, so {@code kafka.connect.commit-id} alone no longer identifies one. The pair is
   * unique: for anything outside the connector that keys on it, and for the connector itself, which
   * uses it to find the snapshot it just committed rather than whichever one heads the branch by
   * then.
   */
  static final String SLICE_SEQ_PROP = "kafka.connect.copy-on-write.slice-seq";

  private final String offsetsProp;
  private final String groupId;

  ChangeSetStore(IcebergSinkConfig config) {
    this.offsetsProp = offsetsProp(config);
    this.groupId = config.connectGroupId();
  }

  static String offsetsProp(IcebergSinkConfig config) {
    return String.format(
        "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
  }

  /**
   * What {@link #loadDrainState} found: a change set to resume, or files to fold into a new one.
   */
  static final class DrainStart {
    private final ChangeSetManifest manifest;
    private final StructLike cursor;
    private final String manifestLocation;
    // a change set whose identifier fields, or format version, no longer match: its files carry
    // over into the next one, and its manifest stays until that one first commits; see
    // adoptDroppedChangeSet
    private final ChangeSetManifest dropped;
    private final String droppedLocation;
    private final String droppedReason;

    private DrainStart(
        ChangeSetManifest manifest,
        StructLike cursor,
        String manifestLocation,
        ChangeSetManifest dropped,
        String droppedLocation,
        String droppedReason) {
      this.manifest = manifest;
      this.cursor = cursor;
      this.manifestLocation = manifestLocation;
      this.dropped = dropped;
      this.droppedLocation = droppedLocation;
      this.droppedReason = droppedReason;
    }

    private static DrainStart freshStart() {
      return new DrainStart(null, null, null, null, null, null);
    }

    ChangeSetManifest manifest() {
      return manifest;
    }

    StructLike cursor() {
      return cursor;
    }

    String manifestLocation() {
      return manifestLocation;
    }

    List<StagedChangeFile> carryOverFiles() {
      return dropped == null ? ImmutableList.of() : dropped.stagedFiles();
    }

    Set<String> carryOverTopics() {
      return dropped == null ? ImmutableSet.of() : dropped.sourceTopics();
    }
  }

  /** A change set that was just frozen, and where its manifest was written. */
  static final class FreezeResult {
    private final ChangeSetManifest manifest;
    private final String location;

    private FreezeResult(ChangeSetManifest manifest, String location) {
      this.manifest = manifest;
      this.location = location;
    }

    ChangeSetManifest manifest() {
      return manifest;
    }

    String location() {
      return location;
    }
  }

  /**
   * Reads the persisted draining state, if any, from the latest snapshot's summary.
   *
   * <p>A change set whose identifier fields no longer match the table's current ones is dropped
   * whole: the cursor is a key in the old identifier shape and cannot be compared against keys in
   * the new one, so its staged files are returned as {@code carryOverFiles} to fold into a freshly
   * frozen change set instead. Its manifest is not deleted here: see {@link
   * #adoptDroppedChangeSet}.
   *
   * <p>A manifest the pointer names that is missing, or is not a manifest, stops the table: see
   * {@link #readManifest}.
   */
  DrainStart loadDrainState(
      Table table,
      String branch,
      TableReference tableReference,
      String stagingLocation,
      Set<Integer> currentIdentifierFieldIds) {
    Map<String, String> summary = latestConnectorSummary(table, branch);
    String activeChangeSetId = summary.get(CHANGE_SET_ID_PROP);
    if (activeChangeSetId == null) {
      return DrainStart.freshStart();
    }

    UUID changeSetId = UUID.fromString(activeChangeSetId);
    String manifestLocation =
        ChangeSetManifest.location(stagingLocation, tableReference, groupId, changeSetId);
    ChangeSetManifest manifest =
        readManifest(table, branch, tableReference, changeSetId, manifestLocation);

    if (manifest.tableUuid() != null && !manifest.tableUuid().equals(table.uuid())) {
      // a staging location shared with another table, or a table dropped and recreated under the
      // same name: the manifest names staged files that have nothing to do with this table
      LOG.warn(
          "Change set {} at {} was frozen for table {}, not {}; ignoring it",
          changeSetId,
          manifestLocation,
          manifest.tableUuid(),
          table.uuid());
      return DrainStart.freshStart();
    }

    if (!manifest.formatVersionSupported()) {
      // whatever the newer format changed, this build cannot tell an additive change from one
      // that was not, so it cannot trust identifierFieldIds or anything else read above either
      String reason =
          String.format(
              Locale.ROOT,
              "manifest format version %d of change set %s is not one this build understands "
                  + "(it writes and reads version %d)",
              manifest.formatVersion(),
              changeSetId,
              ChangeSetManifest.CURRENT_FORMAT_VERSION);
      return new DrainStart(null, null, null, manifest, manifestLocation, reason);
    }

    if (!manifest.identifierFieldsMatch(currentIdentifierFieldIds)) {
      String reason =
          String.format(
              "identifier fields changed since change set %s froze (%s -> %s)",
              changeSetId, manifest.identifierFieldIds(), currentIdentifierFieldIds);
      return new DrainStart(null, null, null, manifest, manifestLocation, reason);
    }

    String cursorJson = summary.get(CHANGE_SET_CURSOR_PROP);
    StructType cursorType = manifest.cursorType(table.schema());
    StructLike cursor =
        cursorJson == null ? null : (StructLike) SingleValueParser.fromJson(cursorType, cursorJson);
    return new DrainStart(manifest, cursor, manifestLocation, null, null, null);
  }

  private ChangeSetManifest readManifest(
      Table table,
      String branch,
      TableReference tableReference,
      UUID changeSetId,
      String manifestLocation) {
    try {
      return ChangeSetManifest.read(table.io(), manifestLocation);
    } catch (NotFoundException e) {
      throw unusableManifest(
          branch, tableReference, changeSetId, manifestLocation, "does not exist");
    } catch (IllegalArgumentException e) {
      throw unusableManifest(
          branch, tableReference, changeSetId, manifestLocation, "is not a change-set manifest");
    }
  }

  private PermanentCopyOnWriteException unusableManifest(
      String branch,
      TableReference tableReference,
      UUID changeSetId,
      String manifestLocation,
      String problem) {
    return new PermanentCopyOnWriteException(
        String.format(
            "change set %s of table %s cannot be resumed: its manifest %s %s. Restore the manifest "
                + "(object versioning, a backup) and restart the connector. If it cannot be "
                + "restored, the part of the change set not yet applied is lost; to go on without "
                + "it, commit to %s a snapshot that carries %s, set to its value in the latest "
                + "snapshot that has it, and no %s, then restart the connector",
            changeSetId,
            tableReference.identifier(),
            manifestLocation,
            problem,
            branch == null ? "the main branch" : "branch " + branch,
            offsetsProp,
            CHANGE_SET_ID_PROP));
  }

  /**
   * Freezes a new change set from the currently buffered {@link RowChangesWritten} responses, plus
   * any staged files carried over from a dropped one. Returns {@code null} if there is nothing to
   * freeze.
   */
  FreezeResult freeze(
      Table table,
      String branch,
      TableReference tableReference,
      String stagingLocation,
      Set<Integer> identifierFieldIds,
      TableCommitRequest request,
      List<StagedChangeFile> carryOverFiles,
      Set<String> carryOverTopics) {
    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch);

    List<RowChangesWritten> payloads =
        request.envelopes().stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> envelope.event().payload())
            .filter(payload -> payload.type() == PayloadType.ROW_CHANGES_WRITTEN)
            .map(payload -> (RowChangesWritten) payload)
            .collect(Collectors.toList());

    List<StagedChangeFile> newFiles =
        payloads.stream().flatMap(p -> p.stagedFiles().stream()).collect(Collectors.toList());

    List<StagedChangeFile> allFiles =
        ImmutableList.<StagedChangeFile>builder().addAll(carryOverFiles).addAll(newFiles).build();
    if (allFiles.isEmpty()) {
      return null;
    }

    // before anything is written, so a rejected freeze leaves nothing behind to clean up
    checkIdentifierFieldsAccepted(tableReference, identifierFieldIds, allFiles);

    Set<String> sourceTopics = Sets.newLinkedHashSet(carryOverTopics);
    payloads.stream().flatMap(p -> p.sourceTopics().stream()).forEach(sourceTopics::add);

    Map<Integer, Long> frozenOffsets = mergeMax(committedOffsets, request.controlTopicOffsets());
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table,
            identifierFieldIds,
            allFiles,
            sourceTopics,
            frozenOffsets,
            request.validThroughTs());
    String location =
        ChangeSetManifest.location(
            stagingLocation, tableReference, groupId, manifest.changeSetId());
    // written before any planning starts: a coordinator lost right after this write leaves an
    // orphan manifest, not a change set whose files nobody can find
    manifest.write(table.io(), location);
    return new FreezeResult(manifest, location);
  }

  /**
   * Reports the change set {@link #loadDrainState} dropped, if it dropped one, once its staged
   * files are frozen into the next change set. Returns where its manifest is, or null.
   *
   * <p>The manifest outlives the freeze. The snapshot summary points at it until the change set
   * that adopted its files first commits, and the next attempt reads it from there: a drain that
   * fails before that commit, or a coordinator restarted in between, drops it again and adopts the
   * same files. Deleted any earlier, it would leave the pointer naming nothing, and no later cycle
   * could get past that. A freeze that rejects the files never gets here, and restoring the
   * identifier fields resumes the change set from the same manifest. See {@link
   * #deleteDroppedChangeSet}.
   */
  String adoptDroppedChangeSet(TableReference tableReference, DrainStart start) {
    if (start.dropped == null) {
      return null;
    }

    LOG.warn(
        "Table {}: {}; dropped the change set, its staged files joined the next one",
        tableReference.identifier(),
        start.droppedReason);
    return start.droppedLocation;
  }

  void deleteDroppedChangeSet(FileIO io, String droppedLocation) {
    if (droppedLocation != null) {
      CopyOnWriteFiles.deleteQuietly(io, droppedLocation);
    }
  }

  private static void checkIdentifierFieldsAccepted(
      TableReference tableReference,
      Set<Integer> currentIdentifierFieldIds,
      List<StagedChangeFile> files) {
    for (StagedChangeFile file : files) {
      Set<Integer> written = ImmutableSet.copyOf(file.identifierFieldIds());
      if (!written.containsAll(currentIdentifierFieldIds)) {
        throw new TableCommitRejectedException(
            String.format(
                "identifier fields %s of table %s are not a subset of %s, the identifier fields "
                    + "its staged change files were written with",
                Sets.newTreeSet(currentIdentifierFieldIds), tableReference.identifier(), written),
            String.format(
                "Staged file %s would be applied by a key it does not carry. Restore the previous "
                    + "identifier fields; to change them, wait for the drain to finish, stop the "
                    + "connector, change them, then start it",
                file.location()));
      }
    }
  }

  void cleanupChangeSet(FileIO io, ChangeSetManifest manifest, String manifestLocation) {
    List<String> locations =
        Stream.concat(
                manifest.stagedFiles().stream().map(StagedChangeFile::location),
                Stream.of(manifestLocation))
            .collect(Collectors.toList());
    CopyOnWriteFiles.deleteQuietly(io, locations);
  }

  /**
   * The summary of the most recent snapshot this connector wrote, walking back past anyone else's.
   *
   * <p>Absence of {@code kafka.connect.copy-on-write.change-set-id} is what says "no drain in
   * progress", and a summary belongs to the snapshot that produced it: Iceberg does not carry one
   * forward. So reading the branch's latest snapshot is only correct while the connector is the
   * table's only writer: one compaction, one manifest rewrite or one ad-hoc insert landing between
   * two slice commits would hide a live change set, and the tail of it would never be applied.
   * Walking back to the last snapshot the connector itself produced is what makes recovery
   * independent of other writers.
   *
   * <p>The connector's snapshot is the one carrying the offsets of its group, not any commit id:
   * another connector on the table writes a commit id too, and its snapshot on top would hide the
   * change set here and in the staging sweep just as a compaction would.
   */
  Map<String, String> latestConnectorSummary(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      if (summary.containsKey(offsetsProp)) {
        return summary;
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return ImmutableMap.of();
  }

  boolean pointsAtChangeSet(Table table, String branch) {
    return latestConnectorSummary(table, branch).containsKey(CHANGE_SET_ID_PROP);
  }

  Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(offsetsProp);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
        try {
          return MAPPER.readValue(value, typeRef);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return ImmutableMap.of();
  }

  static Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  static String offsetsToJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<Integer, Long> mergeMax(Map<Integer, Long> left, Map<Integer, Long> right) {
    return Stream.of(left, right)
        .flatMap(map -> map.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::max));
  }
}
