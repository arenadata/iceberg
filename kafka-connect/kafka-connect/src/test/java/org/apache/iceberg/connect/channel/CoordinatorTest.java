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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.metadata.TableCreated;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoordinatorTest extends ChannelTestBase {

  @Test
  public void testCommitAppend() {
    Assertions.assertEquals(0, ImmutableList.copyOf(table.snapshots().iterator()).size());

    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(ts.toString(), summary.get(VALID_THROUGH_TS_SNAPSHOT_PROP));
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
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(ts.toString(), summary.get(VALID_THROUGH_TS_SNAPSHOT_PROP));
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
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

    coordinatorTest(ImmutableList.of(badDataFile), ImmutableList.of(), null);

    // no commit messages sent
    assertThat(producer.history()).hasSize(1);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testRepublishesTableCreatedOnEveryCommitWithinWindow() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    runCommitCyclesForMetadata(reporter, /* windowMs */ 60_000L, /* force */ false, /* cycles */ 2);

    // Both commits sit inside the 60 s window → two TableCreated events.
    verify(reporter, times(2)).report(any(TableCreated.class));
    // Two cycles × one consumed topic → two LineageEdge events.
    verify(reporter, times(2)).report(any(LineageEdge.class));
  }

  @Test
  public void testStopsTableCreatedAfterWindowElapses() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    // window=0 means (now - firstSeen) < 0 is false on the very first call →
    // TableCreated never fires for this Coordinator instance.
    runCommitCyclesForMetadata(reporter, /* windowMs */ 0L, /* force */ false, /* cycles */ 2);

    verify(reporter, never()).report(any(TableCreated.class));
    // Lineage edges still fire — they're the load-bearing signal post-bootstrap.
    verify(reporter, times(2)).report(any(LineageEdge.class));
  }

  @Test
  public void testForceOnStartRepublishesOnceRegardlessOfWindow() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    runCommitCyclesForMetadata(reporter, /* windowMs */ 0L, /* force */ true, /* cycles */ 3);

    // Force fires exactly once across the Coordinator's lifetime, on cycle 1.
    // Cycles 2 and 3 see republishedOnStart=true so the flag is "spent".
    verify(reporter, times(1)).report(any(TableCreated.class));
    // Lineage continues every cycle.
    verify(reporter, times(3)).report(any(LineageEdge.class));
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

  private void runCommitCyclesForMetadata(
      MetadataReporter reporter, long republishWindowMs, boolean forceOnStart, int cycles) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    when(config.catalogName()).thenReturn("iceberg");
    when(config.connectorName()).thenReturn("test-pipeline");
    when(config.metadataLineageRepublishWindowMs()).thenReturn(republishWindowMs);
    when(config.metadataLineageForceRepublishOnStart()).thenReturn(forceOnStart);

    // One fake group member assigned to src-topic-0 →
    // Coordinator.consumedTopics = {SRC_TOPIC_NAME}, giving one LineageEdge per commit.
    MemberAssignment assignment = mock(MemberAssignment.class);
    when(assignment.topicPartitions())
        .thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));
    MemberDescription member = mock(MemberDescription.class);
    when(member.assignment()).thenReturn(assignment);

    MetadataEvents metadataEvents = new MetadataEvents(reporter, "iceberg", "test-pipeline");

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(member), clientFactory, context, metadataEvents);
    coordinator.start();
    initConsumer();

    long ctlOffset = 1;
    for (int i = 0; i < cycles; i++) {
      // process() #1 of the cycle: emits StartCommit to the control topic.
      coordinator.process();

      // Read back the StartCommit we just produced to get the commitId.
      int lastIdx = producer.history().size() - 1;
      Event startEvent = AvroUtil.decode(producer.history().get(lastIdx).value());
      Assertions.assertEquals(PayloadType.START_COMMIT, startEvent.type());
      UUID commitId = ((StartCommit) startEvent.payload()).commitId();

      // Inject the (fake) Worker's DataWritten reply.
      Event dataWritten =
          new Event(
              config.connectGroupId(),
              new DataWritten(
                  StructType.of(),
                  commitId,
                  new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                  ImmutableList.of(EventTestUtil.createDataFile()),
                  ImmutableList.of()));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "k", AvroUtil.encode(dataWritten)));

      // Inject the (fake) Worker's DataComplete reply.
      OffsetDateTime ts = EventTestUtil.now();
      Event dataComplete =
          new Event(
              config.connectGroupId(),
              new DataComplete(
                  commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, ctlOffset++, "k", AvroUtil.encode(dataComplete)));

      // process() #2 of the cycle: consumes the data events, commits to iceberg,
      // fires CommitToTable + CommitComplete, and runs republishMetadata().
      coordinator.process();
    }
  }
}
