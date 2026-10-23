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
import java.util.UUID;
import java.util.concurrent.Executor;
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
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.NdvChain;
import org.apache.iceberg.connect.data.StatisticsAnswer;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits merge-on-read responses ({@link DataWritten}) through {@code AppendFiles} / {@code
 * RowDelta}.
 *
 * <p>A missing table or an empty commit returns {@link Outcome#COMMITTED} without touching the
 * table. The data commit happens inside {@code commit()}, so a request it commits is always spent
 * whole.
 *
 * <p>For a table with statistics the data commit is marked, and its own snapshot gets a task of the
 * statistics step in {@link StatisticsStepQueue}, which runs it on the transition pool. Nothing of
 * it changes the answer of {@code commit()}: whatever fails after the data commit puts the table
 * into the statistics lag.
 *
 * <p>The standalone committer keeps that lag: {@link #pendingTables()} offers its tables a commit
 * without responses, which checks the lag ({@link StatisticsLagChecker}) and gets a step on the
 * head, a limited number per cycle ({@link StatisticsLagStepLimiter}). The tail of copy-on-write
 * has none of it.
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
  private final StatisticsHealth health;
  private final StatisticsLag lag;
  private final Mode mode;
  private final StatisticsStepQueue statisticsSteps;
  private final StatisticsLagStepLimiter lagStepLimiter;
  private final StatisticsLagChecker lagChecker;
  private final StatisticsLagChecker.Committer lagHooks = new LagHooks();
  private volatile boolean stopped = false;

  /** Why each table's last commit was refused, so the refusal is reported at ERROR once. */
  private final Map<TableIdentifier, String> rejectedReasons = Maps.newConcurrentMap();

  /** How the committer runs: on its own or as the merge-on-read tail of copy-on-write. */
  enum Mode {
    /**
     * The committer of a merge-on-read connector: its tasks take a token of the lag when they are
     * set and remove the lag by it.
     */
    STANDALONE,
    /**
     * The merge-on-read tail inside copy-on-write: it only puts tables into the lag of
     * copy-on-write and never removes it; its tasks need no token.
     */
    TAIL
  }

  MergeOnReadTableCommitter(
      Catalog catalog,
      IcebergSinkConfig config,
      MetadataEvents metadataEvents,
      Consumer<Event> eventSender,
      Executor transitionPool,
      StatisticsHealth health,
      StatisticsStep step,
      StatisticsLag lag,
      Mode mode) {
    this.catalog = catalog;
    this.config = config;
    this.metadataEvents = metadataEvents;
    this.eventSender = eventSender;
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
    this.health = health;
    this.lag = lag;
    this.mode = mode;
    this.lagStepLimiter = new StatisticsLagStepLimiter(config.commitThreads());
    this.lagChecker = new StatisticsLagChecker(catalog, config, lag, health);
    this.statisticsSteps =
        new StatisticsStepQueue(
            transitionPool,
            catalog,
            config,
            step,
            health,
            () -> stopped,
            mode == Mode.STANDALONE
                ? StatisticsStepQueue.standaloneOutcome(lag)
                : StatisticsStepQueue.tailOutcome(lag));
    if (mode == Mode.STANDALONE && config.statisticsInUse()) {
      // the static tables with statistics enter the lag, not verified until the quick check
      lag.enterAtStartup(
          config.configuredTables().stream()
              .filter(id -> config.tableConfig(id.toString()).statisticsEnabled())
              .collect(Collectors.toList()));
    }
  }

  /**
   * The tables of the statistics lag and those assigned a re-check of their health in this cycle,
   * under references without a UUID. Drops the admissions and re-checks left unspent from the
   * previous cycle and chooses this cycle's, so it is called once per cycle, before its commits.
   * Empty for the tail of copy-on-write and with statistics off.
   */
  @Override
  public Set<TableReference> pendingTables() {
    if (mode != Mode.STANDALONE) {
      return ImmutableSet.of();
    }

    Set<TableReference> pending = Sets.newLinkedHashSet();
    try {
      lag.admit(lagStepLimiter.freeSlots(), lagStepLimiter::inFlight);
      lag.tables().forEach(id -> pending.add(TableReference.of(catalog.name(), id)));
      health
          .assignRechecks(Math.max(1, config.commitThreads() / 2))
          .forEach(id -> pending.add(TableReference.of(catalog.name(), id)));
    } catch (RuntimeException e) {
      // nothing of statistics fails the cycle
      LOG.warn("Cannot list the tables of the statistics lag and of the health re-checks", e);
    }

    return pending;
  }

  @Override
  public void stop() {
    stopped = true;
    statisticsSteps.stop();
  }

  @Override
  public StagingCleanupSource stagingCleanupSource() {
    if (mode != Mode.STANDALONE || !config.statisticsInUse()) {
      return null;
    }

    return new MergeOnReadStagingCleanupSource(catalog, config, statisticsSteps);
  }

  boolean hasStatisticsStep(TableIdentifier tableIdentifier) {
    return statisticsSteps.hasTask(tableIdentifier);
  }

  /**
   * The intermediate Puffin files of the table's running and waiting statistics steps. Thread-safe,
   * never waits.
   */
  Set<String> inFlightSketchLocations(TableIdentifier tableIdentifier) {
    return statisticsSteps.inFlightSketchLocations(tableIdentifier);
  }

  @Override
  public CommitResult commit(TableCommitRequest request) {
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    // taken before the table is loaded. Whatever removes the lag below does it by this token,
    // and an evaluation by the head applies only at this version
    long lagToken = lag.token(tableIdentifier);
    long healthVersion = health.version(tableIdentifier);
    // only pendingTables() makes a request without responses, and its kind, not the lag,
    // chooses the branch: the lag may be gone since
    if (mode == Mode.STANDALONE && request.envelopes().isEmpty()) {
      return commitWithoutResponses(request, lagToken, healthVersion);
    }

    return commitResponses(request, lagToken, healthVersion);
  }

  private CommitResult commitResponses(
      TableCommitRequest request, long lagToken, long healthVersion) {
    TableReference tableReference = request.tableReference();
    TableIdentifier tableIdentifier = tableReference.identifier();

    TableSinkConfig tableConfig = config.tableConfig(tableIdentifier.toString());
    Table table = loadTable(tableIdentifier, tableConfig);
    if (table == null) {
      return CommitResult.committed(request.envelopes());
    }

    String branch = tableConfig.commitBranch();
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

    List<DataWritten> payloads = payloads(request, committedOffsets);

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
      return nothingToCommit(request, table, lagToken, healthVersion);
    }

    // only a table with statistics marks its data commit and looks for its own snapshot
    boolean statisticsEnabled = tableConfig.statisticsEnabled();
    Long beforeId = statisticsEnabled ? headId(table, branch) : null;
    long snapshotId;
    try {
      commitData(request, table, branch, offsetsJson, dataFiles, deleteFiles, statisticsEnabled);
      snapshotId = latestSnapshot(table, branch).snapshotId();
    } catch (RuntimeException e) {
      // the outcome is not confirmed, and a snapshot committed after all would get no step
      if (statisticsEnabled) {
        lag.enter(tableIdentifier);
      }

      throw e;
    }

    if (statisticsEnabled) {
      snapshotId = setStatisticsStep(request, table, branch, beforeId, payloads, snapshotId);
    }

    OffsetDateTime validThroughTs = request.validThroughTs();
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

  private Table loadTable(TableIdentifier tableIdentifier, TableSinkConfig tableConfig) {
    boolean inCleanup = mode == Mode.STANDALONE && config.statisticsInUse();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      if (inCleanup) {
        statisticsSteps.forgetTable(tableIdentifier);
      }

      return null;
    }

    if (inCleanup && tableConfig.statisticsEnabled()) {
      statisticsSteps.rememberTable(tableIdentifier, table);
    }

    return table;
  }

  private List<DataWritten> payloads(
      TableCommitRequest request, Map<Integer, Long> committedOffsets) {
    return request.envelopes().stream()
        .filter(
            envelope -> {
              Long minOffset = committedOffsets.get(envelope.partition());
              return minOffset == null || envelope.offset() >= minOffset;
            })
        .map(envelope -> envelope.event().payload())
        .filter(payload -> payload.type() == PayloadType.DATA_WRITTEN)
        .map(payload -> (DataWritten) payload)
        .collect(Collectors.toList());
  }

  /**
   * Every response of the request is below the committed offsets. A table in the standalone
   * committer's lag still goes through the lag check, with the table already loaded; the answer
   * does not change.
   */
  private CommitResult nothingToCommit(
      TableCommitRequest request, Table table, long lagToken, long healthVersion) {
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    if (mode == Mode.STANDALONE && lagToken != 0L) {
      lagChecker.visitLoaded(request, table, lagToken, healthVersion, lagHooks);
    }

    return CommitResult.committed(request.envelopes());
  }

  /** Always {@code COMMITTED} with nothing consumed: nothing of statistics fails the cycle. */
  private CommitResult commitWithoutResponses(
      TableCommitRequest request, long lagToken, long healthVersion) {
    lagChecker.visit(request, null, lagToken, healthVersion, lagHooks);
    return CommitResult.committed(ImmutableList.of());
  }

  /**
   * What merge-on-read adds to the lag check. A table whose last connector snapshot carries a
   * change set waits without a step; its refusal stays with the requests that have responses.
   */
  private final class LagHooks implements StatisticsLagChecker.Committer {
    @Override
    public void tableGone(TableIdentifier id) {
      statisticsSteps.forgetTable(id);
    }

    @Override
    public boolean mayCheck(TableIdentifier id, Table table) {
      statisticsSteps.rememberTable(id, table);
      String branch = config.tableConfig(id.toString()).commitBranch();
      String changeSetId =
          latestConnectorSummary(table, branch).get(ChangeSetStore.CHANGE_SET_ID_PROP);
      if (changeSetId != null) {
        LOG.debug(
            "The last snapshot of this connector on table {} carries change set {}, the table "
                + "stays in the statistics lag without a step",
            id,
            changeSetId);
        return false;
      }

      return true;
    }

    @Override
    public void setLagStep(TableCommitRequest request, Table table) {
      if (!statisticsSteps.enqueueLagStep(
          request.tableReference(), request.commitId(), lagStepLimiter, lag)) {
        LOG.debug(
            "Table {} has a statistics step already, it removes the lag instead of a lag step",
            request.tableReference().identifier());
      }
    }
  }

  private void commitData(
      TableCommitRequest request,
      Table table,
      String branch,
      String offsetsJson,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      boolean statisticsEnabled) {
    if (deleteFiles.isEmpty()) {
      AppendFiles appendOp = table.newAppend();
      applyCommitProperties(appendOp, request, branch, offsetsJson, statisticsEnabled);
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();
    } else {
      RowDelta deltaOp = table.newRowDelta();
      applyCommitProperties(deltaOp, request, branch, offsetsJson, statisticsEnabled);
      dataFiles.forEach(deltaOp::addRows);
      deleteFiles.forEach(deltaOp::addDeletes);
      deltaOp.commit();
    }
  }

  private void applyCommitProperties(
      SnapshotUpdate<?> operation,
      TableCommitRequest request,
      String branch,
      String offsetsJson,
      boolean statisticsEnabled) {
    if (branch != null) {
      operation.toBranch(branch);
    }
    operation.set(snapshotOffsetsProp, offsetsJson);
    operation.set(COMMIT_ID_SNAPSHOT_PROP, request.commitId().toString());
    if (request.validThroughTs() != null) {
      operation.set(VALID_THROUGH_TS_SNAPSHOT_PROP, request.validThroughTs().toString());
    }
    // the marker tells the gap of a failed start from history written with statistics off
    if (statisticsEnabled) {
      operation.set(NdvChain.STATISTICS_ENABLED_PROP, "true");
    }
  }

  /**
   * Sets the statistics step of the commit's own snapshot after a successful data commit. A
   * snapshot not found or a failure puts the table into the lag and changes nothing else of {@code
   * commit()}.
   *
   * @return the own snapshot, or the head of the branch if it is not found
   */
  private long setStatisticsStep(
      TableCommitRequest request,
      Table table,
      String branch,
      Long beforeId,
      List<DataWritten> payloads,
      long headId) {
    TableIdentifier tableIdentifier = request.tableReference().identifier();
    Long ownSnapshotId = null;
    try {
      ownSnapshotId = ownSnapshotId(table, branch, request.commitId(), beforeId);
      if (ownSnapshotId == null) {
        LOG.warn(
            "Snapshot of commit {} to table {} is not on the branch any more, so it gets no "
                + "statistics step; the table is in the statistics lag",
            request.commitId(),
            tableIdentifier);
        lag.enter(tableIdentifier);
        return headId;
      }

      StatisticsStepTask task =
          new StatisticsStepTask(
              request.tableReference(),
              table,
              ownSnapshotId,
              request.commitId(),
              ImmutableSet.of(ownSnapshotId),
              mode == Mode.STANDALONE ? lag.token(tableIdentifier) : 0L,
              answers(payloads, ownSnapshotId));
      if (mode == Mode.STANDALONE) {
        statisticsSteps.rememberTable(tableIdentifier, table);
      }

      statisticsSteps.enqueue(task);
      return ownSnapshotId;
    } catch (RuntimeException e) {
      LOG.warn(
          "Cannot set the statistics step of commit {} to table {}; the data commit stands and the "
              + "table is in the statistics lag",
          request.commitId(),
          tableIdentifier,
          e);
      lag.enter(tableIdentifier);
      return ownSnapshotId != null ? ownSnapshotId : headId;
    }
  }

  /**
   * The commit's own snapshot: the newest on the branch with this commit id and this connector's
   * offsets, searched down to {@code beforeId}, exclusive.
   *
   * @return the snapshot id, or {@code null} if it is not on the branch
   */
  private Long ownSnapshotId(Table table, String branch, UUID commitId, Long beforeId) {
    String expectedCommitId = commitId.toString();
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null && (beforeId == null || snapshot.snapshotId() != beforeId)) {
      Map<String, String> summary = snapshot.summary();
      if (expectedCommitId.equals(summary.get(COMMIT_ID_SNAPSHOT_PROP))
          && summary.containsKey(snapshotOffsetsProp)) {
        return snapshot.snapshotId();
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return null;
  }

  /**
   * The answers of the task: one per {@code DataWritten} with a data file left after the filters of
   * the data commit, which are applied here in the same order.
   */
  private List<StatisticsAnswer> answers(List<DataWritten> payloads, long snapshotId) {
    Predicate<DataFile> distinct = distinctByKey(ContentFile::location);
    List<StatisticsAnswer> answers = Lists.newArrayList();
    for (DataWritten payload : payloads) {
      if (payload.dataFiles() == null) {
        continue;
      }

      List<DataFile> files =
          payload.dataFiles().stream()
              .filter(dataFile -> dataFile.recordCount() > 0)
              .filter(distinct)
              .collect(Collectors.toList());
      if (!files.isEmpty()) {
        answers.add(StatisticsAnswer.ofDataFiles(snapshotId, files, payload.sketchFile()));
      }
    }

    return answers;
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

  private Long headId(Table table, String branch) {
    Snapshot head = latestSnapshot(table, branch);
    return head == null ? null : head.snapshotId();
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
