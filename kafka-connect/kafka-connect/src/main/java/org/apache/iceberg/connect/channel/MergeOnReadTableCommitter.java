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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits merge-on-read responses ({@link DataWritten}) through {@code AppendFiles} / {@code
 * RowDelta}.
 *
 * <p>A missing table or an empty commit returns {@link Outcome#COMMITTED} without touching the
 * table. Everything happens inside {@code commit()}, so a request it commits is always spent whole.
 *
 * <p>A table with copy-on-write work left over from before the connector switched to merge-on-read
 * is refused rather than committed, until copy-on-write is back and has finished with it.
 */
class MergeOnReadTableCommitter implements TableCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(MergeOnReadTableCommitter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final String COPY_ON_WRITE_RESPONSES_REJECTED =
      "copy-on-write responses are buffered for it, switch back to copy-on-write to apply them";
  private static final String UNFINISHED_CHANGE_SET_REJECTED =
      "unfinished change set, switch back to copy-on-write until it is drained";

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final MetadataEvents metadataEvents;
  private final Consumer<Event> eventSender;
  private final String snapshotOffsetsProp;

  /** Why each table's last commit was refused, so the refusal is reported at ERROR once. */
  private final Map<TableIdentifier, String> rejectedReasons = Maps.newConcurrentMap();

  MergeOnReadTableCommitter(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<Event> eventSender) {
    this.catalog = catalog;
    this.config = config;
    this.metadataEvents = metadataEvents;
    this.eventSender = eventSender;
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
  }

  @Override
  public CommitResult commit(TableCommitRequest request) {
    TableReference tableReference = request.tableReference();
    TableIdentifier tableIdentifier = tableReference.identifier();

    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      return CommitResult.committed(request.envelopes());
    }

    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    // a drain switched away from in the middle. Summaries are not inherited, so a merge-on-read
    // snapshot on top of this one drops the pointer to the keys the drain has not applied, and the
    // responses that carried them were spent when its first slice committed
    Map<String, String> connectorSummary = latestConnectorSummary(table, branch);
    Map<Integer, Long> committedOffsets = committedOffsets(connectorSummary);

    // the tail of copy-on-write: a RowChangesWritten not yet below the drain's committed offsets
    // cannot be applied here, and the worker that sent it has already committed its source offsets,
    // so reporting it spent would lose the records outright. One at or below the committed offset
    // was already applied by the drain before the switch, and is spent below together with the rest
    // of the request instead of being rejected forever
    if (request.envelopes().stream()
        .anyMatch(envelope -> isUnappliedRowChangesWritten(envelope, committedOffsets))) {
      return reject(
          tableIdentifier,
          COPY_ON_WRITE_RESPONSES_REJECTED,
          String.format(
              "Commit %s found RowChangesWritten from a copy-on-write cycle that was in flight "
                  + "when the connector switched to merge-on-read",
              request.commitId()));
    }

    String changeSetId = connectorSummary.get(ChangeSetStore.CHANGE_SET_ID_PROP);
    if (changeSetId != null) {
      return reject(
          tableIdentifier,
          UNFINISHED_CHANGE_SET_REJECTED,
          String.format(
              "The last snapshot of this connector carries change set %s, and a merge-on-read "
                  + "commit on top of it would lose the part not yet applied",
              changeSetId));
    }

    clearRejection(tableIdentifier);

    // Control topic partition offsets may include a subset of partition ids if there were no
    // records for other partitions.  Merge the updated topic partitions with the last committed
    // offsets.
    Map<Integer, Long> mergedOffsets =
        Stream.of(committedOffsets, request.controlTopicOffsets())
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::max));
    String offsetsJson = offsetsToJson(mergedOffsets);

    List<DataWritten> payloads =
        request.envelopes().stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> envelope.event().payload())
            .filter(payload -> payload.type() == PayloadType.DATA_WRITTEN)
            .map(payload -> (DataWritten) payload)
            .collect(Collectors.toList());

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
      return CommitResult.committed(request.envelopes());
    }

    OffsetDateTime validThroughTs = request.validThroughTs();
    if (deleteFiles.isEmpty()) {
      AppendFiles appendOp = table.newAppend();
      if (branch != null) {
        appendOp.toBranch(branch);
      }
      appendOp.set(snapshotOffsetsProp, offsetsJson);
      appendOp.set(COMMIT_ID_SNAPSHOT_PROP, request.commitId().toString());
      if (validThroughTs != null) {
        appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
      }
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();
    } else {
      RowDelta deltaOp = table.newRowDelta();
      if (branch != null) {
        deltaOp.toBranch(branch);
      }
      deltaOp.set(snapshotOffsetsProp, offsetsJson);
      deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, request.commitId().toString());
      if (validThroughTs != null) {
        deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
      }
      dataFiles.forEach(deltaOp::addRows);
      deleteFiles.forEach(deltaOp::addDeletes);
      deltaOp.commit();
    }

    Long snapshotId = latestSnapshot(table, branch).snapshotId();
    Event event =
        new Event(
            config.connectGroupId(),
            new CommitToTable(request.commitId(), tableReference, snapshotId, validThroughTs));
    eventSender.accept(event);

    Set<String> lineageTopics =
        payloads.stream()
            .flatMap(payload -> payload.sourceTopics().stream())
            .collect(Collectors.toUnmodifiableSet());
    metadataEvents.lineageCommit(lineageTopics, tableIdentifier, table.schema());

    LOG.info(
        "Commit complete to table {}, snapshot {}, commit ID {}, valid-through {}",
        tableIdentifier,
        snapshotId,
        request.commitId(),
        validThroughTs);

    return CommitResult.committed(request.envelopes());
  }

  /**
   * A {@code RowChangesWritten} not yet below the drain's committed offset for its partition: the
   * copy-on-write cycle that sent it has not applied it yet, so merge-on-read cannot spend it
   * either.
   */
  private boolean isUnappliedRowChangesWritten(
      Envelope envelope, Map<Integer, Long> committedOffsets) {
    if (envelope.event().payload().type() != PayloadType.ROW_CHANGES_WRITTEN) {
      return false;
    }
    Long minOffset = committedOffsets.get(envelope.partition());
    return minOffset == null || envelope.offset() >= minOffset;
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  /**
   * The summary of the latest snapshot on the branch that carries this connector's offsets, or an
   * empty map. Both the committed offsets and the change set pointer are read from it, not from the
   * branch head: summaries are not inherited, so another writer's commit on top would hide them.
   */
  private Map<String, String> latestConnectorSummary(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      if (summary.containsKey(snapshotOffsetsProp)) {
        return summary;
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return ImmutableMap.of();
  }

  private Map<Integer, Long> committedOffsets(Map<String, String> connectorSummary) {
    String value = connectorSummary.get(snapshotOffsetsProp);
    if (value == null) {
      return ImmutableMap.of();
    }
    TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
    try {
      return MAPPER.readValue(value, typeRef);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Refuses the table's commit without spending anything: its responses stay buffered and the
   * control topic offsets stay held, and every cycle checks again. Reported at ERROR once per table
   * and reason, every repeat at DEBUG.
   */
  private CommitResult reject(TableIdentifier tableIdentifier, String reason, String detail) {
    if (reason.equals(rejectedReasons.put(tableIdentifier, reason))) {
      LOG.debug(
          "Merge-on-read commit of table {} is still rejected: {}. {}",
          tableIdentifier,
          reason,
          detail);
    } else {
      LOG.error(
          "Merge-on-read commit of table {} is rejected: {}. {}. This table's responses stay in "
              + "the coordinator's buffer and its control topic offsets stay held; every commit "
              + "cycle checks again, and no restart is needed once the cause is undone",
          tableIdentifier,
          reason,
          detail);
    }
    return CommitResult.failed(ImmutableList.of());
  }

  /** Reports at INFO that the rejection {@link #reject} logged no longer holds. */
  private void clearRejection(TableIdentifier tableIdentifier) {
    String reason = rejectedReasons.remove(tableIdentifier);
    if (reason != null) {
      LOG.info(
          "Merge-on-read commit of table {} is no longer rejected ({})", tableIdentifier, reason);
    }
  }

  private String offsetsToJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
