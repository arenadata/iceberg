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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class TestCoordinator extends ChannelTestBase {

  private static final String COPY_ON_WRITE_CHANGE_SET_ID_PROP =
      "kafka.connect.copy-on-write.change-set-id";
  // a payload type a later version might add
  private static final int UNKNOWN_TYPE_ID = 99;

  @Test
  public void testCommitAppend() {
    assertThat(table.snapshots()).isEmpty();

    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(1);

    Snapshot snapshot = snapshots.get(0);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();

    assertThat(snapshot.summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}")
        .containsEntry(VALID_THROUGH_TS_SNAPSHOT_PROP, ts.toString());
  }

  @Test
  public void testCommitDelta() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(EventTestUtil.createDeleteFile()),
            ts);

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(1);

    Snapshot snapshot = snapshots.get(0);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshot.addedDeleteFiles(table.io())).hasSize(1);

    assertThat(snapshot.summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}")
        .containsEntry(VALID_THROUGH_TS_SNAPSHOT_PROP, ts.toString());
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, ts);

    assertThat(table.snapshots()).isEmpty();
  }

  @Test
  public void testCommitError() {
    // this spec isn't registered with the table
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();

    UUID commitId = coordinatorTest(ImmutableList.of(badDataFile), ImmutableList.of(), null);

    // the table did not commit, so no CommitToTable was sent; the cycle itself still completes
    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, null);

    assertThat(table.snapshots()).isEmpty();
  }

  @Test
  public void testReportsLineageForWrittenSourceTopic() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    runCommitCyclesForMetadata(reporter, 2);

    verify(reporter, times(2)).report(any(LineageEdge.class));
  }

  @Test
  public void testFailedTableDoesNotBlockOtherTables() {
    TableIdentifier otherIdentifier = TableIdentifier.of(NAMESPACE, "tbl2");
    Table otherTable = catalog.createTable(otherIdentifier, SCHEMA);

    // this spec isn't registered with either table, so committing it fails
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();

    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(), clientFactory, context, MetadataEvents.NOOP);
    coordinator.start();
    initConsumer();

    coordinator.process();
    UUID commitId =
        ((StartCommit) AvroUtil.decode(producer.history().get(0).value()).payload()).commitId();

    // the healthy table at control topic offset 1, the failing one at offset 2
    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);
    addDataWritten(commitId, "tbl2", ImmutableList.of(badDataFile), 2);

    OffsetDateTime ts = EventTestUtil.now();
    Event commitReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 3, "key", AvroUtil.encode(commitReady)));

    coordinator.process();

    // the healthy table committed even though the other one failed
    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    otherTable.refresh();
    assertThat(otherTable.snapshots()).isEmpty();

    // StartCommit, CommitToTable for the healthy table, CommitComplete
    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    // one table did not commit, so the cycle claims no valid-through timestamp
    assertCommitComplete(2, commitId, null);

    // control topic offsets are held back to the response that has not reached its table, so a
    // restarted coordinator re-reads it
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(2L));
  }

  @Test
  public void testCopyOnWriteRejectsAMergeOnReadResponseInsteadOfDroppingIt() {
    // configuration validation refuses the setup that produces this, so anything arriving here came
    // another way. The copy-on-write committer consumes RowChangesWritten and nothing else, and a
    // DataWritten reaching it would be reported spent without ever being committed: the records
    // acknowledged and gone. It has to stop the table loudly instead
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.rowLevelMode()).thenReturn(RowLevelOperationMode.COPY_ON_WRITE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(), clientFactory, context, MetadataEvents.NOOP);
    coordinator.start();
    initConsumer();

    coordinator.process();
    UUID commitId =
        ((StartCommit) AvroUtil.decode(producer.history().get(0).value()).payload()).commitId();

    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);

    OffsetDateTime ts = EventTestUtil.now();
    Event commitReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                "task-0"));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(commitReady)));

    coordinator.process();

    // nothing committed, and the response is still buffered: the offsets are held back to it, so a
    // restarted coordinator sees it again rather than losing it
    table.refresh();
    assertThat(table.snapshots()).isEmpty();
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(1L));
  }

  @Test
  public void testCopyOnWriteCommitsTheTailOfMergeOnReadInsteadOfRejectingIt() {
    // merge-on-read -> copy-on-write with a cycle in flight: the DataWritten carries a commit id
    // this
    // coordinator never issued. Refused, it would stall the table and pin the offsets for good
    catalog.dropTable(TABLE_IDENTIFIER);
    table = catalog.createTable(TABLE_IDENTIFIER, new Schema(SCHEMA.columns(), ImmutableSet.of(1)));
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.rowLevelMode()).thenReturn(RowLevelOperationMode.COPY_ON_WRITE);
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    addDataWritten(
        UUID.randomUUID(), TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);
    OffsetDateTime ts = EventTestUtil.now();
    Event commitReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                "task-0"));
    addRecord(AvroUtil.encode(commitReady), 2);

    coordinator.process();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}");
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(3L));
    // StartCommit, CommitToTable, CommitComplete: the table committed, so with the timestamp
    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);
  }

  @Test
  public void testMergeOnReadRejectsACopyOnWriteResponseInsteadOfDroppingIt() {
    // the tail of a copy-on-write cycle that was in flight when the connector switched to
    // merge-on-read. Its worker committed the source offsets together with it, so reporting it
    // spent without applying it loses the records outright
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    addRowChangesWritten(UUID.randomUUID(), TABLE_NAME, 1);
    addDataComplete(commitId, EventTestUtil.now(), 2);

    coordinator.process();

    // the response is still buffered: the offsets are held back to it, so a restarted coordinator
    // sees it again rather than losing it
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(1L));
    table.refresh();
    assertThat(table.snapshots()).isEmpty();
    // StartCommit, CommitComplete: the table did not commit, so no valid-through timestamp
    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, null);
  }

  @Test
  public void testMergeOnReadSpendsACopyOnWriteResponseTheDrainAlreadyCommitted() {
    // unlike the previous test, this RowChangesWritten's control-topic offset is already below
    // the drain's last committed offset for its partition: the copy-on-write cycle applied it
    // before the switch, so merge-on-read can spend it here without losing anything
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":6}")
        .commit();

    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    OffsetDateTime ts = EventTestUtil.now();
    addRowChangesWritten(UUID.randomUUID(), TABLE_NAME, 5);
    addDataComplete(commitId, ts, 6);

    coordinator.process();

    // spent together with the rest of the request: nothing holds the offsets back
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(7L));
    table.refresh();
    // nothing to apply, so no new snapshot on top of the one from setup
    assertThat(table.snapshots()).hasSize(1);
    assertThat(producer.history()).hasSize(2);
    // the only table committed, so the cycle is valid-through in full
    assertCommitComplete(1, commitId, ts);
  }

  @Test
  public void testMergeOnReadRefusesToCommitOverAnUnfinishedChangeSet() {
    // the connector switched to merge-on-read in the middle of a copy-on-write drain. Summaries
    // are not inherited, so a merge-on-read snapshot on top of this one would drop the pointer to
    // the keys the drain has not applied yet, and their responses are already spent
    UUID changeSetId = UUID.randomUUID();
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":1}")
        .set(COPY_ON_WRITE_CHANGE_SET_ID_PROP, changeSetId.toString())
        .commit();

    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);
    addDataComplete(commitId, EventTestUtil.now(), 2);

    coordinator.process();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(COPY_ON_WRITE_CHANGE_SET_ID_PROP, changeSetId.toString());
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(1L));
    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, null);

    // the drain finishes (its exhausting commit carries no change set id) and the held response
    // applies in the next cycle, with no restart
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":1}")
        .commit();

    coordinator.process();
    UUID nextCommitId = startCommitId(2);
    OffsetDateTime ts = EventTestUtil.now();
    addDataComplete(nextCommitId, ts, 3);

    coordinator.process();

    table.refresh();
    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, nextCommitId.toString())
        .doesNotContainKey(COPY_ON_WRITE_CHANGE_SET_ID_PROP);
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(4L));
  }

  @Test
  public void testMergeOnReadRefusesAMixedCommitAsTheTailOfCopyOnWrite() {
    // the tail of a copy-on-write cycle next to fresh merge-on-read responses. Refused by the
    // merge-on-read committer, not by the mixed-mode check: only that refusal is reported at ERROR
    // once, with the remedy: switch back to copy-on-write
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    Envelope dataWritten =
        addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);
    Envelope rowChangesWritten = addRowChangesWritten(UUID.randomUUID(), TABLE_NAME, 2);
    addDataComplete(commitId, EventTestUtil.now(), 3);

    coordinator.process();

    table.refresh();
    assertThat(table.snapshots()).isEmpty();
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(1L));
    assertCommitComplete(1, commitId, null);

    assertThatCode(
            () ->
                coordinator.checkSingleMode(
                    TABLE_IDENTIFIER, commitId, ImmutableList.of(dataWritten, rowChangesWritten)))
        .doesNotThrowAnyException();
  }

  @Test
  public void testCopyOnWriteRefusesAMixedCommitAsMixed() {
    // in copy-on-write the DataWritten alone would be refused too, so only the message tells the
    // mixed-mode check from the plain-append one
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.rowLevelMode()).thenReturn(RowLevelOperationMode.COPY_ON_WRITE);
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    Envelope dataWritten =
        addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);
    Envelope rowChangesWritten = addRowChangesWritten(commitId, TABLE_NAME, 2);
    addDataComplete(commitId, EventTestUtil.now(), 3);

    coordinator.process();

    table.refresh();
    assertThat(table.snapshots()).isEmpty();
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(1L));
    assertCommitComplete(1, commitId, null);

    assertThatThrownBy(
            () ->
                coordinator.checkSingleMode(
                    TABLE_IDENTIFIER, commitId, ImmutableList.of(dataWritten, rowChangesWritten)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("both merge-on-read and copy-on-write responses")
        .hasMessageContaining(TABLE_IDENTIFIER.toString())
        .hasMessageContaining(commitId.toString());
  }

  @Test
  public void testAnUndecodableEventOfAnotherGroupIsSkipped() {
    // two connectors share the default control topic, and the other one already runs a version with
    // a payload type this one does not know. Its events are none of this coordinator's business, so
    // they must not take its thread down, and every task of this connector with it
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    addRecord(
        EventTestUtil.withPayloadTypeId(
            new Event("other-connector", new StartCommit(UUID.randomUUID())), UNKNOWN_TYPE_ID),
        1);
    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 2);
    OffsetDateTime ts = EventTestUtil.now();
    addDataComplete(commitId, ts, 3);

    assertThatCode(coordinator::process).doesNotThrowAnyException();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertCommitComplete(2, commitId, ts);
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(4L));
  }

  @Test
  public void testAnUndecodableEventOfItsOwnGroupWithoutFilesIsSkipped() {
    // no table's files ride on it, so skipping it loses nothing: a DataComplete that is not read
    // leaves the cycle to a partial commit, a RewriteComplete leaves its slice to time out
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    addRecord(EventTestUtil.withTruncatedPayload(dataComplete(commitId, EventTestUtil.now())), 1);
    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 2);
    OffsetDateTime ts = EventTestUtil.now();
    addDataComplete(commitId, ts, 3);

    assertThatCode(coordinator::process).doesNotThrowAnyException();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertCommitComplete(2, commitId, ts);
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(4L));
  }

  @Test
  public void testAnUndecodableResponseOfItsOwnGroupStopsCommitsButNotTheCoordinator() {
    // a worker on a later version reports files this coordinator cannot read. Its source offsets
    // went out with the report, so a cycle that skipped it would commit without those records, and
    // a table committing a later cycle would filter the report out after a restart
    assertCommitsStopAt(
        commitId ->
            EventTestUtil.withTruncatedPayload(
                dataWritten(
                    commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()))));
  }

  @Test
  public void testAnEventOfItsOwnGroupWithAnUnknownPayloadTypeStopsCommits() {
    // whether an unknown payload carries files cannot be read, so it is held like one that does
    assertCommitsStopAt(
        commitId ->
            EventTestUtil.withPayloadTypeId(
                new Event(config.connectGroupId(), new StartCommit(commitId)), UNKNOWN_TYPE_ID));
  }

  @Test
  public void testWhatATableHasNotSpentStaysBufferedAndHoldsBackItsPartition() {
    // a copy-on-write drain spans cycles: the table answers PENDING while its slices are in flight,
    // and spends envelopes when a first slice commits, not when the drain ends. What it has not
    // spent must stay buffered and hold back the offsets of its own partition, and a cycle with
    // such a table claims no valid-through timestamp: the table has not applied all up to it
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);
    UUID firstCommitId = startCommitId(0);

    // "tbl" is draining and spends one of its two envelopes, "tbl3" commits outright
    addRowChangesWritten(firstCommitId, TABLE_NAME, 0, 1);
    addRowChangesWritten(firstCommitId, TABLE_NAME, 0, 2);
    addRowChangesWritten(firstCommitId, "tbl3", 0, 3);
    addDataComplete(firstCommitId, "task-0", 1, EventTestUtil.now(), 4);
    committer.answer(TABLE_NAME, envelopes -> CommitResult.pending(envelopes.subList(0, 1)));
    committer.answer("tbl3", CommitResult::committed);

    coordinator.process();

    assertCommitComplete(1, firstCommitId, null);
    assertThat(committedControlTopicOffsets())
        .as("held back to the envelope the draining table has not spent")
        .containsEntry(controlTopicPartition(0), new OffsetAndMetadata(2L));
    TableCommitRequest drain = committer.lastRequest(TABLE_NAME);
    assertThat(positions(drain.envelopes())).containsExactly("0:1", "0:2");
    assertThat(drain.commitId()).isEqualTo(firstCommitId);
    assertThat(drain.controlTopicOffsets())
        .as("the position of the cycle that is committing")
        .isEqualTo(ImmutableMap.of(0, 5L));
    assertThat(drain.activeTasks()).containsOnlyKeys("task-0");
    assertThat(drain.activeTasks().get("task-0"))
        .extracting(TopicPartitionRef::topic, TopicPartitionRef::partition)
        .containsExactly(tuple("topic", 1));

    // the next cycle: the drain spends the rest, and a table on the other partition fails having
    // spent one of its three
    coordinator.process();
    UUID secondCommitId = startCommitId(2);
    addRowChangesWritten(secondCommitId, "tbl2", 1, 5);
    addRowChangesWritten(secondCommitId, "tbl2", 1, 6);
    addRowChangesWritten(secondCommitId, "tbl2", 1, 7);
    // read on a tick of their own: the order of two partitions within one poll is not defined
    coordinator.process();
    addDataComplete(secondCommitId, "task-1", 2, EventTestUtil.now(), 5);
    committer.answer(TABLE_NAME, CommitResult::committed);
    committer.answer("tbl2", envelopes -> CommitResult.failed(envelopes.subList(0, 1)));

    coordinator.process();

    assertCommitComplete(3, secondCommitId, null);
    assertThat(positions(committer.lastRequest(TABLE_NAME).envelopes())).containsExactly("0:2");
    assertThat(committer.lastRequest("tbl2").activeTasks()).containsOnlyKeys("task-1");
    assertThat(committedControlTopicOffsets())
        .as("partition 0 moves past the cycle, partition 1 stays at the older of the two left")
        .containsEntry(controlTopicPartition(0), new OffsetAndMetadata(6L))
        .containsEntry(controlTopicPartition(1), new OffsetAndMetadata(6L));

    // every table of the next cycle commits: the timestamp is claimed, and nothing holds offsets
    coordinator.process();
    UUID thirdCommitId = startCommitId(4);
    OffsetDateTime ts = EventTestUtil.now();
    addDataComplete(thirdCommitId, "task-1", 2, ts, 6);
    committer.answer("tbl2", CommitResult::committed);

    coordinator.process();

    assertCommitComplete(5, thirdCommitId, ts);
    assertThat(positions(committer.lastRequest("tbl2").envelopes())).containsExactly("1:6", "1:7");
    assertThat(committedControlTopicOffsets())
        .containsEntry(controlTopicPartition(0), new OffsetAndMetadata(7L))
        .containsEntry(controlTopicPartition(1), new OffsetAndMetadata(8L));
  }

  @Test
  public void testATableWithADrainInFlightIsOfferedACommitWithNothingBuffered() {
    // its envelopes were spent when the first slice committed, and nothing new was written to it.
    // Left out of the cycle, the rest of the change set would wait for the next record written to
    // the table
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    committer.drainInFlight(TABLE_NAME);
    committer.answer(TABLE_NAME, CommitResult::pending);
    Coordinator coordinator = startCoordinator(committer);
    UUID commitId = startCommitId(0);
    addDataComplete(commitId, "task-0", 1, EventTestUtil.now(), 1);

    coordinator.process();

    assertThat(committer.requests(TABLE_NAME)).as("commits offered to the table").hasSize(1);
    assertThat(committer.lastRequest(TABLE_NAME).envelopes()).isEmpty();
    // a table of the cycle like any other: it has not applied everything up to the timestamp
    assertCommitComplete(1, commitId, null);
  }

  @Test
  public void testARewriteCompleteReachesTheCommitterAsItArrives() {
    // an answer to a slice that does not reach the committer leaves every slice to time out
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);

    Event rewriteComplete =
        new Event(
            config.connectGroupId(),
            new RewriteComplete(
                StructType.of(),
                UUID.randomUUID(),
                tableReference(TABLE_NAME),
                UUID.randomUUID(),
                0,
                "task-0",
                RewriteComplete.STATUS_OK,
                ImmutableList.of(),
                0,
                1));
    addRecord(AvroUtil.encode(rewriteComplete), 1);

    coordinator.process();

    assertThat(committer.received)
        .extracting(envelope -> envelope.event().type(), Envelope::offset)
        .containsExactly(tuple(PayloadType.REWRITE_COMPLETE, 1L));
  }

  @Test
  public void testTheCommitterIsDrivenOnEveryTickAfterItsPoll() {
    // slice timeouts and cancellations are driven from here rather than from the cycle, or a drain
    // of K slices would stretch over K commit intervals
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);
    UUID commitId = startCommitId(0);

    addRowChangesWritten(commitId, TABLE_NAME, 0, 1);
    // no DataComplete, so no cycle completes on these ticks
    coordinator.process();
    coordinator.process();

    // the size of the buffer the committer saw on each tick, the one that started the coordinator
    // included: the response read on a tick is already there
    assertThat(committer.bufferSeenOnEachTick).containsExactly(0, 1, 1);
  }

  @Test
  public void testATickEndsWhileTheControlTopicKeepsBringingEvents() {
    // copy-on-write drains feed the control topic themselves: an answer read here lets a
    // transition assign the next slice, whose answer arrives before the poll is over. A tick that
    // read until the topic fell silent would time no slice out, start no cycle and close no
    // partial commit until every drain had finished
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);

    Event rewriteComplete =
        new Event(
            config.connectGroupId(),
            new RewriteComplete(
                StructType.of(),
                UUID.randomUUID(),
                tableReference(TABLE_NAME),
                UUID.randomUUID(),
                0,
                "task-0",
                RewriteComplete.STATUS_OK,
                ImmutableList.of(),
                0,
                1));
    AtomicBoolean floodOver = floodControlTopic(AvroUtil.encode(rewriteComplete), 1, 10_000);
    coordinator.process();

    assertThat(floodOver).as("the tick ended while events kept arriving").isFalse();
    assertThat(committer.received).as("the tick read what had arrived").isNotEmpty();
    assertThat(committer.bufferSeenOnEachTick)
        .as("the committer was driven on this tick too, the one that started it included")
        .hasSize(2);
  }

  @Test
  public void testATransitionInFlightDoesNotHoldUpTheNextCycle() {
    // the coordinator thread waits for the commit pool. A slice transition (planning, committing
    // a slice) queued in that pool would become the coordinator's latency, and a long one a
    // missed max.poll.interval.ms. The pools here have one thread each
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);
    UUID firstCommitId = startCommitId(0);
    addRowChangesWritten(firstCommitId, TABLE_NAME, 0, 1);
    addDataComplete(firstCommitId, "task-0", 1, EventTestUtil.now(), 2);

    CountDownLatch transitionMayFinish = new CountDownLatch(1);
    AtomicBoolean transitionFinished = new AtomicBoolean(false);
    committer.answer(
        TABLE_NAME,
        envelopes -> {
          committer.transitionExec.execute(
              () -> {
                try {
                  // bounded, so that a transition sharing the commit pool fails the test rather
                  // than hanging it
                  transitionMayFinish.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                transitionFinished.set(true);
              });
          return CommitResult.pending(ImmutableList.of());
        });

    AtomicBoolean committedWhileTransitionRan = new AtomicBoolean(false);
    try {
      coordinator.process();

      committer.answer(
          TABLE_NAME,
          envelopes -> {
            committedWhileTransitionRan.set(!transitionFinished.get());
            return CommitResult.pending(ImmutableList.of());
          });
      coordinator.process();
      UUID secondCommitId = startCommitId(2);
      addDataComplete(secondCommitId, "task-0", 1, EventTestUtil.now(), 3);

      coordinator.process();
    } finally {
      transitionMayFinish.countDown();
    }

    assertThat(committer.requests(TABLE_NAME)).hasSize(2);
    assertThat(committedWhileTransitionRan)
        .as("the next cycle committed while the transition was still running")
        .isTrue();
  }

  @Test
  public void testStopClosesTheChannelEvenWhenATransitionOutlivesTermination()
      throws InterruptedException {
    // a slice transition blocked on I/O ignores shutdownNow()'s interrupt, so terminate() can
    // time out. stop() must still close the consumer, producer and admin: otherwise the "-coord"
    // consumer never sends a LeaveGroup, and the coordinator elected
    // after a leadership move waits out max.poll.interval.ms before it can read the control topic
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);
    coordinator.terminationTimeout(Duration.ofMillis(200));

    CountDownLatch transitionStarted = new CountDownLatch(1);
    CountDownLatch releaseTransition = new CountDownLatch(1);
    committer.transitionExec.execute(
        () -> {
          transitionStarted.countDown();
          while (releaseTransition.getCount() > 0) {
            try {
              releaseTransition.await();
            } catch (InterruptedException e) {
              // a blocking read from object storage would not honor the interrupt either
            }
          }
        });

    try {
      assertThat(transitionStarted.await(10, TimeUnit.SECONDS)).isTrue();

      assertThatThrownBy(coordinator::stop)
          .isInstanceOf(ConnectException.class)
          .hasMessage("Timed out waiting for coordinator shutdown");

      assertThat(producer.closed()).as("producer closed despite the timed-out transition").isTrue();
      assertThat(consumer.closed()).as("consumer closed despite the timed-out transition").isTrue();
    } finally {
      releaseTransition.countDown();
    }
  }

  @Test
  public void testStopTellsTheCommitterBeforeItsTransitionsAreShutDown() {
    // a transition still running after the leader moved must not start an Iceberg commit, a
    // delete or a send; shutdownNow() alone does not stop one that ignores its interrupt, so the
    // committer is told first, while its pool is still up
    copyOnWrite();
    ScriptedTableCommitter committer = new ScriptedTableCommitter();
    Coordinator coordinator = startCoordinator(committer);

    coordinator.stop();

    assertThat(committer.transitionPoolShutDownAtStop)
        .as("the committer was told to stop, before its pool was shut down")
        .isFalse();
  }

  @Test
  public void testTheControlTopicIsCaughtUpOnlyAtTheEndOffsetsOfAssignment() {
    // consumeAvailable() stops at the first empty poll, which says nothing about how much of the
    // topic is behind it. The copy-on-write staging cleanup reads the commit buffer as one of its
    // three sources of reachability, so it may not run until what the topic held when its
    // partitions were assigned has been consumed
    Coordinator coordinator = startCoordinator();
    consumer.updateEndOffsets(ImmutableMap.of(new TopicPartition(CTL_TOPIC_NAME, 0), 2L));

    assertThat(coordinator.isControlTopicCaughtUp())
        .as("the responses of a cycle in flight are still ahead of the consumer")
        .isFalse();

    UUID commitId = startCommitId(0);
    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 0);
    addDataComplete(commitId, EventTestUtil.now(), 1);
    coordinator.process();

    assertThat(coordinator.isControlTopicCaughtUp())
        .as("read to where the control topic ended when the partition was assigned")
        .isTrue();
  }

  @Test
  public void testAnEndOffsetFetchThatFailsOnlyDelaysTheCatchUpMark() {
    // the mark gates nothing that may fail a poll of the coordinator: it is asked again next tick
    Coordinator coordinator = startCoordinator();

    assertThat(coordinator.isControlTopicCaughtUp())
        .as("the broker did not answer where the topic ends")
        .isFalse();

    consumer.updateEndOffsets(ImmutableMap.of(new TopicPartition(CTL_TOPIC_NAME, 0), 0L));

    assertThat(coordinator.isControlTopicCaughtUp()).as("asked again, and answered").isTrue();
  }

  private void assertCommitsStopAt(Function<UUID, byte[]> undecodable) {
    Coordinator coordinator = startCoordinator();
    UUID commitId = startCommitId(0);

    // a cycle that commits before the event is reached, and a response behind it
    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 1);
    addDataComplete(commitId, EventTestUtil.now(), 2);
    addRecord(undecodable.apply(commitId), 3);
    addDataWritten(commitId, TABLE_NAME, ImmutableList.of(EventTestUtil.createDataFile()), 4);

    assertThatCode(coordinator::process).doesNotThrowAnyException();
    // no commit is in progress any more, so a coordinator that went on would start the next cycle
    assertThatCode(coordinator::process).doesNotThrowAnyException();

    table.refresh();
    assertThat(table.snapshots()).as("only the cycle before the event committed").hasSize(1);
    assertThat(producer.history())
        .as("StartCommit, CommitToTable, CommitComplete, and no StartCommit after the event")
        .hasSize(3);
    assertThat(coordinator.controlTopicOffsets())
        .as("the control topic is not read past the event")
        .containsEntry(0, 3L);
    // nothing past the event is committed, so a restarted coordinator reads it again
    assertThat(consumer.committed(ImmutableSet.of(new TopicPartition(CTL_TOPIC_NAME, 0))))
        .containsEntry(new TopicPartition(CTL_TOPIC_NAME, 0), new OffsetAndMetadata(3L));
  }

  private void addRecord(byte[] bytes, long ctlOffset) {
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset, "key", bytes));
  }

  private Coordinator startCoordinator() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(), clientFactory, context, MetadataEvents.NOOP);
    coordinator.start();
    initConsumer();

    coordinator.process();
    return coordinator;
  }

  private UUID startCommitId(int idx) {
    Event event = AvroUtil.decode(producer.history().get(idx).value());
    assertThat(event.type()).isEqualTo(PayloadType.START_COMMIT);
    return ((StartCommit) event.payload()).commitId();
  }

  private void copyOnWrite() {
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.rowLevelMode()).thenReturn(RowLevelOperationMode.COPY_ON_WRITE);
  }

  /** Starts a coordinator on {@code committer}, reading two partitions of the control topic. */
  private Coordinator startCoordinator(ScriptedTableCommitter committer) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator =
        new Coordinator(
            config,
            ImmutableList.of(),
            clientFactory,
            mock(SinkTaskContext.class),
            (send, sendInOneTransaction, transitionExec, controlTopicCaughtUp) ->
                committer.startedWith(transitionExec));
    coordinator.start();
    consumer.rebalance(ImmutableList.of(controlTopicPartition(0), controlTopicPartition(1)));
    consumer.updateBeginningOffsets(
        ImmutableMap.of(controlTopicPartition(0), 0L, controlTopicPartition(1), 0L));

    coordinator.process();
    return coordinator;
  }

  private static TopicPartition controlTopicPartition(int partition) {
    return new TopicPartition(CTL_TOPIC_NAME, partition);
  }

  private Map<TopicPartition, OffsetAndMetadata> committedControlTopicOffsets() {
    return consumer.committed(ImmutableSet.of(controlTopicPartition(0), controlTopicPartition(1)));
  }

  private static TableReference tableReference(String tableName) {
    return new TableReference("catalog", ImmutableList.of("db"), tableName);
  }

  /** Envelopes as {@code partition:offset}, which is what tells them apart. */
  private static List<String> positions(List<Envelope> envelopes) {
    return envelopes.stream()
        .map(envelope -> envelope.partition() + ":" + envelope.offset())
        .collect(Collectors.toList());
  }

  private void addDataComplete(
      UUID commitId, String taskId, int sourcePartition, OffsetDateTime ts, long ctlOffset) {
    Event dataComplete =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(new TopicPartitionOffset("topic", sourcePartition, 1L, ts)),
                taskId));
    addRecord(AvroUtil.encode(dataComplete), ctlOffset);
  }

  private Envelope addRowChangesWritten(UUID commitId, String tableName, long ctlOffset) {
    return addRowChangesWritten(commitId, tableName, 0, ctlOffset);
  }

  private Envelope addRowChangesWritten(
      UUID commitId, String tableName, int ctlPartition, long ctlOffset) {
    StagedChangeFile staged =
        new StagedChangeFile(
            "s3://warehouse/db/" + tableName + "/staging/task-0/epoch/0.avro",
            10L,
            1L,
            0,
            1,
            ImmutableMap.of(),
            ImmutableMap.of());
    Event rowChangesWritten =
        new Event(
            config.connectGroupId(),
            new RowChangesWritten(
                commitId,
                new TableReference("catalog", ImmutableList.of("db"), tableName),
                "task-0",
                ImmutableList.of(SRC_TOPIC_NAME),
                ImmutableList.of(staged)));
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME, ctlPartition, ctlOffset, "key", AvroUtil.encode(rowChangesWritten)));
    return new Envelope(rowChangesWritten, ctlPartition, ctlOffset);
  }

  private void addDataComplete(UUID commitId, OffsetDateTime ts, long ctlOffset) {
    addRecord(AvroUtil.encode(dataComplete(commitId, ts)), ctlOffset);
  }

  private Event dataComplete(UUID commitId, OffsetDateTime ts) {
    return new Event(
        config.connectGroupId(),
        new DataComplete(commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
  }

  private Envelope addDataWritten(
      UUID commitId, String tableName, List<DataFile> dataFiles, long ctlOffset) {
    Event dataWritten = dataWritten(commitId, tableName, dataFiles);
    addRecord(AvroUtil.encode(dataWritten), ctlOffset);
    return new Envelope(dataWritten, 0, ctlOffset);
  }

  private Event dataWritten(UUID commitId, String tableName, List<DataFile> dataFiles) {
    return new Event(
        config.connectGroupId(),
        new DataWritten(
            StructType.of(),
            commitId,
            new TableReference("catalog", ImmutableList.of("db"), tableName),
            dataFiles,
            ImmutableList.of()));
  }

  private void assertCommitTable(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitTable = AvroUtil.decode(bytes);
    assertThat(commitTable.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    CommitToTable commitToTablePayload = (CommitToTable) commitTable.payload();
    assertThat(commitToTablePayload.commitId()).isEqualTo(commitId);
    assertThat(commitToTablePayload.tableReference().identifier().toString())
        .isEqualTo(TABLE_IDENTIFIER.toString());
    assertThat(commitToTablePayload.validThroughTs()).isEqualTo(ts);
  }

  private void assertCommitComplete(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = AvroUtil.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    CommitComplete commitCompletePayload = (CommitComplete) commitComplete.payload();
    assertThat(commitCompletePayload.commitId()).isEqualTo(commitId);
    assertThat(commitCompletePayload.validThroughTs()).isEqualTo(ts);
  }

  private UUID coordinatorTest(
      List<DataFile> dataFiles, List<DeleteFile> deleteFiles, OffsetDateTime ts) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(), clientFactory, context, MetadataEvents.NOOP);
    coordinator.start();

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = AvroUtil.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(PayloadType.START_COMMIT);

    UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    Event commitResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                dataFiles,
                deleteFiles));
    bytes = AvroUtil.encode(commitResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event commitReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
    bytes = AvroUtil.encode(commitReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    when(config.commitIntervalMs()).thenReturn(0);

    coordinator.process();

    return commitId;
  }

  @Test
  public void testCoordinatorRunning() {
    TopicPartition tp0 = new TopicPartition(SRC_TOPIC_NAME, 0);
    TopicPartition tp1 = new TopicPartition(SRC_TOPIC_NAME, 1);
    TopicPartition tp2 = new TopicPartition(SRC_TOPIC_NAME, 2);

    // elected leader for holding tp0
    sourceConsumer.rebalance(Lists.newArrayList(tp0, tp1, tp2));
    assertThat(mockIcebergSinkTask.isCoordinatorRunning()).isTrue();

    // still holds tp0, so still leader
    sourceConsumer.rebalance(Lists.newArrayList(tp0, tp1));
    assertThat(mockIcebergSinkTask.isCoordinatorRunning()).isTrue();

    // tp0 revoked, so the coordinator closes
    sourceConsumer.rebalance(ImmutableList.of(tp1));
    assertThat(mockIcebergSinkTask.isCoordinatorRunning()).isFalse();
  }

  @Test
  public void testCoordinatorCommittedOffsetMerging() {
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"1\":7}")
        .commit();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"1\":7}");

    coordinatorTest(
        ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), EventTestUtil.now());

    // the new commit carries partition 0's offset merged with partition 1's, already on the table
    table.refresh();
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3,\"1\":7}");
  }

  private void runCommitCyclesForMetadata(MetadataReporter reporter, int cycles) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    MemberAssignment assignment = mock(MemberAssignment.class);
    when(assignment.topicPartitions())
        .thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));
    MemberDescription member = mock(MemberDescription.class);
    when(member.assignment()).thenReturn(assignment);

    MetadataEvents metadataEvents =
        new MetadataEvents(reporter, "iceberg", "default", "test-pipeline", "kafka");

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(member), clientFactory, context, metadataEvents);
    coordinator.start();
    initConsumer();

    long ctlOffset = 1;
    for (int i = 0; i < cycles; i++) {
      coordinator.process();

      // the coordinator picks the commitId, so read its own StartCommit back for it
      int lastIdx = producer.history().size() - 1;
      Event startEvent = AvroUtil.decode(producer.history().get(lastIdx).value());
      assertThat(startEvent.type()).isEqualTo(PayloadType.START_COMMIT);
      UUID commitId = ((StartCommit) startEvent.payload()).commitId();

      Event dataWritten =
          new Event(
              config.connectGroupId(),
              new DataWritten(
                  StructType.of(),
                  commitId,
                  new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                  ImmutableList.of(EventTestUtil.createDataFile()),
                  ImmutableList.of(),
                  ImmutableList.of(SRC_TOPIC_NAME)));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "k", AvroUtil.encode(dataWritten)));

      OffsetDateTime ts = EventTestUtil.now();
      Event dataComplete =
          new Event(
              config.connectGroupId(),
              new DataComplete(
                  commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "k", AvroUtil.encode(dataComplete)));

      coordinator.process();
    }
  }

  /**
   * Stands in for the committer of either mode, to test the coordinator's side of the seam: answers
   * each table as the test scripts it, and records what the coordinator handed it.
   */
  private static class ScriptedTableCommitter implements TableCommitter {
    private final Map<String, Function<List<Envelope>, CommitResult>> answers =
        Maps.newConcurrentMap();
    private final List<TableCommitRequest> requests =
        Collections.synchronizedList(Lists.newArrayList());
    private final Set<TableReference> pendingTables = Sets.newConcurrentHashSet();
    // the coordinator thread only
    private final List<Envelope> received = Lists.newArrayList();
    private final List<Integer> bufferSeenOnEachTick = Lists.newArrayList();
    private Executor transitionExec;
    // null until stop()
    private volatile Boolean transitionPoolShutDownAtStop;

    private TableCommitter startedWith(Executor exec) {
      this.transitionExec = exec;
      return this;
    }

    private void answer(String tableName, Function<List<Envelope>, CommitResult> answer) {
      answers.put(tableName, answer);
    }

    private void drainInFlight(String tableName) {
      pendingTables.add(tableReference(tableName));
    }

    private List<TableCommitRequest> requests(String tableName) {
      synchronized (requests) {
        return requests.stream()
            .filter(request -> request.tableReference().identifier().name().equals(tableName))
            .collect(Collectors.toList());
      }
    }

    private TableCommitRequest lastRequest(String tableName) {
      List<TableCommitRequest> ofTable = requests(tableName);
      assertThat(ofTable).as("commits offered to %s", tableName).isNotEmpty();
      return ofTable.get(ofTable.size() - 1);
    }

    @Override
    public CommitResult commit(TableCommitRequest request) {
      // the offsets are the channel's live map, so they are recorded as they are at the call
      requests.add(
          new TableCommitRequest(
              request.tableReference(),
              ImmutableList.copyOf(request.envelopes()),
              ImmutableMap.copyOf(request.controlTopicOffsets()),
              request.commitId(),
              request.validThroughTs(),
              ImmutableMap.copyOf(request.activeTasks())));
      return answers
          .getOrDefault(request.tableReference().identifier().name(), CommitResult::committed)
          .apply(request.envelopes());
    }

    @Override
    public boolean receive(Envelope envelope) {
      received.add(envelope);
      return true;
    }

    @Override
    public void process(long nowMs, Supplier<Collection<Envelope>> bufferedResponses) {
      bufferSeenOnEachTick.add(bufferedResponses.get().size());
    }

    @Override
    public Set<TableReference> pendingTables() {
      return ImmutableSet.copyOf(pendingTables);
    }

    @Override
    public void stop() {
      transitionPoolShutDownAtStop = ((ExecutorService) transitionExec).isShutdown();
    }
  }
}
