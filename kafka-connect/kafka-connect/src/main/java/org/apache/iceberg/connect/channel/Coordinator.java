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

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStep;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final ExecutorService exec;
  private final ExecutorService transitionExec;
  private final CommitState commitState;
  private final TableCommitter tableCommitter;
  private final StatisticsHealth statisticsHealth;
  private final StatisticsMetrics statisticsMetrics;
  private final StagingCleanupScheduler stagingCleanup;
  private volatile boolean terminating = false;
  private Duration terminationTimeout = Duration.ofMinutes(1);

  /** Builds the table committer from what only the coordinator has: senders, pools, statistics. */
  @FunctionalInterface
  interface TableCommitterFactory {
    TableCommitter create(
        Consumer<Event> send,
        Consumer<List<Event>> sendInOneTransaction,
        Executor transitionPool,
        StatisticsHealth health,
        StatisticsStep step);
  }

  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      MetadataEvents metadataEvents) {
    this(catalog, config, members, clientFactory, context, metadataEvents, new StatisticsStep());
  }

  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      MetadataEvents metadataEvents,
      StatisticsMetrics statisticsMetrics) {
    this(
        catalog,
        config,
        members,
        clientFactory,
        context,
        metadataEvents,
        new StatisticsStep(),
        statisticsMetrics);
  }

  @VisibleForTesting
  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      MetadataEvents metadataEvents,
      StatisticsStep statisticsStep) {
    this(catalog, config, members, clientFactory, context, metadataEvents, statisticsStep, null);
  }

  private Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      MetadataEvents metadataEvents,
      StatisticsStep statisticsStep,
      StatisticsMetrics statisticsMetrics) {
    this(
        config,
        members,
        clientFactory,
        context,
        statisticsStep,
        (send, sendInOneTransaction, transitionPool, health, step) ->
            config.isCopyOnWriteMode()
                ? new CopyOnWriteTableCommitter(
                    catalog,
                    config,
                    metadataEvents,
                    sendInOneTransaction,
                    transitionPool,
                    health,
                    step)
                : new MergeOnReadTableCommitter(
                    catalog,
                    config,
                    metadataEvents,
                    send,
                    transitionPool,
                    health,
                    step,
                    new StatisticsLag(),
                    MergeOnReadTableCommitter.Mode.STANDALONE),
        statisticsMetrics);
  }

  @VisibleForTesting
  Coordinator(
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      StatisticsStep statisticsStep,
      TableCommitterFactory tableCommitterFactory) {
    this(config, members, clientFactory, context, statisticsStep, tableCommitterFactory, null);
  }

  @VisibleForTesting
  Coordinator(
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      StatisticsStep statisticsStep,
      TableCommitterFactory tableCommitterFactory,
      StatisticsMetrics statisticsMetrics) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.connectGroupId() + "-coord", config, clientFactory, context);

    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.exec =
        new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            config.keepAliveTimeoutInMs(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-committer" + "-%d")
                .build());
    // table transitions (copy-on-write slices, statistics steps) get a pool of their own:
    // doCommit() joins the commit pool from the coordinator thread, so a planning or committing
    // transition queued in front of its tasks would become the coordinator's latency
    this.transitionExec =
        new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            config.keepAliveTimeoutInMs(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-table-transition" + "-%d")
                .build());
    this.commitState = new CommitState(config);
    this.statisticsMetrics = statisticsMetrics;
    this.statisticsHealth =
        config.statisticsInUse()
            ? new StatisticsHealth(config.statisticsHealthRecheckIntervalMs())
            : StatisticsHealth.NOOP;
    this.tableCommitter =
        tableCommitterFactory.create(
            this::send,
            events -> send(events, ImmutableMap.of()),
            transitionExec,
            statisticsHealth,
            statisticsStep);
    // one cleanup for the life of the coordinator, whichever committer supplies what it reaches:
    // two cleaners on one prefix, each with its own idea of what is live, would delete each other's
    StagingCleanupSource stagingCleanupSource = tableCommitter.stagingCleanupSource();
    this.stagingCleanup =
        stagingCleanupSource == null
            ? null
            : new StagingCleanupScheduler(
                config,
                transitionExec,
                stagingCleanupSource,
                this::isControlTopicCaughtUp,
                () -> terminating);
    // last, so that a constructor that fails leaves no health behind in the metrics of the task
    if (statisticsMetrics != null) {
      statisticsMetrics.attach(statisticsHealth);
    }
  }

  void process() {
    if (!isHeldAtUndecodableEvent() && commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Commit {} initiated", commitState.currentCommitId());
    }

    consumeAvailable(POLL_DURATION);

    // reading is held at an undecodable event of this connector that may carry table files, see
    // holdsAtUndecodableTableFiles(). Nothing commits until a restart reads it again: no cycle,
    // whose responses could not be read anyway, and no slice of a drain
    if (isHeldAtUndecodableEvent()) {
      return;
    }

    // copy-on-write drives its slices from here, not from the commit cycle: a rewrite answer can
    // arrive at any tick, and tying slice progress to commit() would stretch a K-slice drain over
    // K commit intervals. Must not block: anything slow it triggers goes to the commit pool
    long nowMs = System.currentTimeMillis();
    tableCommitter.process(nowMs, commitState::bufferedResponses);
    // outside doCommit, and so never between a commit taking the files of a response into its
    // queue or manifest and the buffer letting the response go: every live file is in one of the
    // two when the cleanup reads them
    if (stagingCleanup != null) {
      stagingCleanup.maybeClean(nowMs, commitState::bufferedResponses);
    }

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  @VisibleForTesting
  boolean hasStagingCleanup() {
    return stagingCleanup != null;
  }

  /**
   * A skipped response loses records: its worker committed their source offsets along with it, and
   * readiness counts only DataComplete, so the cycle would commit without it. Holding back the
   * consumer offset alone does not save it (every table committing a later cycle records control
   * topic offsets past it, and would filter it out after a restart) and which table it belongs to
   * cannot be read. So the coordinator holds at it instead, see {@link #process()}.
   */
  @Override
  protected boolean holdsAtUndecodableTableFiles() {
    return true;
  }

  @Override
  protected boolean receive(Envelope envelope) {
    switch (envelope.event().payload().type()) {
      case DATA_WRITTEN:
      case ROW_CHANGES_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
      case REWRITE_COMPLETE:
        return tableCommitter.receive(envelope);
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableReference, List<Envelope>> commitMap = commitState.tableCommitMap();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);
    Map<Integer, Long> controlTopicOffsets = controlTopicOffsets();

    Set<TableReference> committedTables = Sets.newConcurrentHashSet();
    // what the committers are done with, which in copy-on-write is not the same as what committed:
    // a drain spends the envelopes it froze several cycles before it finishes
    List<Envelope> consumedResponses = Collections.synchronizedList(Lists.newArrayList());
    Map<String, List<TopicPartitionRef>> activeTasks = commitState.activeTasks();

    // a table with a half-applied change set is visited even with an empty buffer: its responses
    // were reported spent when the first slice committed, so it would otherwise fall out of the
    // cycle entirely and wait for the next record written to it before anyone resumed the drain
    Set<TableReference> tables = cycleTables(commitMap.keySet(), tableCommitter.pendingTables());

    // one table must not take the cycle down with it, or a single broken table stalls commits for
    // every other table and the response buffer grows without bound. A PENDING outcome
    // (copy-on-write slice still in flight) counts like a failure: the table is not added.
    Tasks.foreach(tables)
        .executeWith(exec)
        .suppressFailureWhenFinished()
        .onFailure(
            (tableReference, exc) ->
                LOG.warn(
                    "Commit failed for table {}, will try again next cycle",
                    tableReference.identifier(),
                    exc))
        .run(
            tableReference -> {
              List<Envelope> envelopeList =
                  commitMap.getOrDefault(tableReference, ImmutableList.of());
              checkSingleMode(
                  tableReference.identifier(), commitState.currentCommitId(), envelopeList);

              TableCommitRequest request =
                  new TableCommitRequest(
                      tableReference,
                      envelopeList,
                      controlTopicOffsets,
                      commitState.currentCommitId(),
                      validThroughTs,
                      activeTasks);
              CommitResult result = tableCommitter.commit(request);
              consumedResponses.addAll(result.consumed());
              if (result.outcome() == TableCommitter.Outcome.COMMITTED) {
                committedTables.add(tableReference);
              }
            });

    // responses of the tables that failed stay buffered for the next cycle, and the control topic
    // offsets are held back to the oldest of them: committing past an event that has not reached
    // its table would lose it outright, since the source offsets are already committed by the
    // worker that sent it. And to the replay anchors, spent envelopes a restarted coordinator
    // must read again to find a table with a change set left to drain
    List<Envelope> pendingResponses = commitState.clearResponses(consumedResponses);
    commitConsumerOffsets(
        offsetsHeldBackBy(Iterables.concat(pendingResponses, tableCommitter.replayAnchors())));

    // valid-through is a claim about every table, so it holds only if every table committed. A
    // copy-on-write table answers PENDING while its drain, spanning many cycles, is in flight, so
    // a busy one suppresses valid-through in most cycles, and rightly: it has not applied
    // everything up to that timestamp yet. Its kafka.connect.valid-through-ts is likewise written
    // only on the exhausting commit of a drain
    boolean allCommitted = committedTables.size() == tables.size();
    OffsetDateTime sentValidThroughTs = allCommitted ? validThroughTs : null;
    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), sentValidThroughTs));
    send(event);

    // logs the value actually sent and says "partial" outright: a monitor matching this line by
    // commitId alone must not mistake a partial cycle's "committed to 0 of 0" for a drained table
    LOG.info(
        "Commit {} complete, committed to {} of {} table(s), valid-through {}{}",
        commitState.currentCommitId(),
        committedTables.size(),
        tables.size(),
        sentValidThroughTs,
        allCommitted ? "" : " (partial)");
  }

  /**
   * The buffered and pending tables, minus a pending reference without a UUID when the cycle has a
   * reference of the same table with one, so that table gets one {@code commit()}. Buffered
   * references of one name with different UUIDs (a re-created table) are all kept.
   */
  private static Set<TableReference> cycleTables(
      Set<TableReference> buffered, Set<TableReference> pending) {
    Set<TableReference> tables = Sets.newLinkedHashSet(buffered);
    for (TableReference reference : pending) {
      boolean namedWithUuid =
          reference.uuid() == null
              && Stream.concat(buffered.stream(), pending.stream())
                  .anyMatch(other -> other.uuid() != null && sameTable(other, reference));
      if (!namedWithUuid) {
        tables.add(reference);
      }
    }

    return tables;
  }

  private static boolean sameTable(TableReference one, TableReference other) {
    return Objects.equals(one.catalog(), other.catalog())
        && one.identifier().equals(other.identifier());
  }

  /**
   * Returns the consumed control topic offsets, each capped at the oldest envelope that holds its
   * partition back.
   */
  private Map<Integer, Long> offsetsHeldBackBy(Iterable<Envelope> holding) {
    Map<Integer, Long> offsets = Maps.newHashMap(controlTopicOffsets());
    holding.forEach(envelope -> offsets.merge(envelope.partition(), envelope.offset(), Math::min));
    return offsets;
  }

  /**
   * Rejects a copy-on-write commit the committer cannot apply in full.
   *
   * <p>Both cases fail the table's cycle, not the connector, and concern a {@code DataWritten}
   * under a commit id this coordinator issued. Mixed with {@code RowChangesWritten}: a delete file
   * and a rewrite of the same data file have no defined order within one snapshot. Alone: it would
   * be applied as merge-on-read, delete files and all, to a table copy-on-write keeps free of them;
   * configuration validation refuses what produces it, so it has to stop the table loudly. The
   * envelopes stay buffered and the offsets held back, so nothing is lost, at the cost of a buffer
   * that grows while the table is stuck, which {@code CommitState} reports past its threshold.
   *
   * <p>A {@code DataWritten} under a commit id this coordinator never issued is the tail of
   * merge-on-read, written before the switch to copy-on-write. It is not rejected: the committer
   * applies it the merge-on-read way, in order of age, and refusing it would stall the table for
   * good, since nothing ever changes a buffered response.
   *
   * <p>Merge-on-read checks nothing here: {@code MergeOnReadTableCommitter} refuses copy-on-write
   * responses itself, since it reads the table anyway to tell whether a drain was left unfinished.
   */
  @VisibleForTesting
  void checkSingleMode(
      TableIdentifier tableIdentifier, UUID commitId, List<Envelope> envelopeList) {
    if (!config.isCopyOnWriteMode()) {
      return;
    }

    Set<PayloadType> kinds =
        envelopeList.stream()
            .filter(envelope -> !isMergeOnReadTail(envelope))
            .map(envelope -> envelope.event().payload().type())
            .collect(Collectors.toSet());

    if (kinds.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "Table %s received both merge-on-read and copy-on-write responses in commit %s; "
                  + "the two cannot be applied in one snapshot",
              tableIdentifier, commitId));
    }

    if (kinds.contains(PayloadType.DATA_WRITTEN)) {
      throw new IllegalStateException(
          String.format(
              "Table %s received a merge-on-read response in commit %s while the connector is in "
                  + "copy-on-write mode; the copy-on-write committer cannot apply it",
              tableIdentifier, commitId));
    }
  }

  private boolean isMergeOnReadTail(Envelope envelope) {
    return envelope.event().payload().type() == PayloadType.DATA_WRITTEN
        && !commitState.isIssued(CommitState.commitId(envelope));
  }

  @VisibleForTesting
  void terminationTimeout(Duration timeout) {
    this.terminationTimeout = timeout;
  }

  void terminate() {
    try {
      // first, while the pools are still up: a transition or a pass of the staging cleanup that
      // ignores shutdownNow()'s interrupt must still start no Iceberg commit, delete or send once
      // another coordinator may be elected
      terminating = true;
      tableCommitter.stop();
      transitionExec.shutdownNow();
      exec.shutdownNow();
      // wait for coordinator termination, else cause the sink task to fail
      try {
        if (!exec.awaitTermination(terminationTimeout.toMillis(), TimeUnit.MILLISECONDS)
            || !transitionExec.awaitTermination(
                terminationTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
          throw new ConnectException("Timed out waiting for coordinator shutdown");
        }
      } catch (InterruptedException e) {
        throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
      }
    } finally {
      // only once the steps of this coordinator are done with the health: the metrics of the task
      // read it until then. Another coordinator may have attached its own since (the task got the
      // leader partition back while this one was still in here): its health stays
      if (statisticsMetrics != null) {
        statisticsMetrics.detach(statisticsHealth);
      }
    }
  }

  @Override
  public void stop() {
    // a transition stuck in transitionExec must not keep the channel's consumer, producer and
    // admin open: an unclosed "-coord" consumer sends no LeaveGroup, so the group's rebalance
    // waits out max.poll.interval.ms before a new coordinator on another task can take over
    try {
      terminate();
    } finally {
      super.stop();
    }
  }
}
