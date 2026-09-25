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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.RecordWriteResult;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.data.StagedChangesResult;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestWorker extends ChannelTestBase {

  @Test
  public void testSave() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      when(context.assignment()).thenReturn(ImmutableSet.of(topicPartition));

      IcebergWriterResult writeResult =
          new IcebergWriterResult(
              TableIdentifier.parse(TABLE_NAME),
              ImmutableList.of(EventTestUtil.createDataFile()),
              ImmutableList.of(),
              StructType.of());

      Map<TopicPartition, Offset> offsets =
          ImmutableMap.of(topicPartition, new Offset(1L, EventTestUtil.now()));

      SinkWriterResult sinkWriterResult =
          new SinkWriterResult(ImmutableList.<RecordWriteResult>of(writeResult), offsets);
      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite()).thenReturn(sinkWriterResult);

      Worker worker = new Worker(catalog, config, clientFactory, sinkWriter, context);
      worker.start();

      // init consumer after subscribe()
      initConsumer();

      // save a record
      Map<String, Object> value = ImmutableMap.of();
      SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
      worker.save(ImmutableList.of(rec));

      UUID commitId = UUID.randomUUID();
      Event commitRequest = new Event(config.connectGroupId(), new StartCommit(commitId));
      byte[] bytes = AvroUtil.encode(commitRequest);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

      worker.process();

      assertThat(producer.history()).hasSize(2);

      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.payload().type()).isEqualTo(PayloadType.DATA_WRITTEN);
      DataWritten dataWritten = (DataWritten) event.payload();
      assertThat(dataWritten.commitId()).isEqualTo(commitId);

      event = AvroUtil.decode(producer.history().get(1).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
      DataComplete dataComplete = (DataComplete) event.payload();
      assertThat(dataComplete.commitId()).isEqualTo(commitId);
      assertThat(dataComplete.assignments()).hasSize(1);
      assertThat(dataComplete.assignments().get(0).offset()).isEqualTo(1L);
    }
  }

  @Test
  public void testACopyOnWriteCycleReportsItsStagedFilesInTheTransactionOfTheirSourceOffsets() {
    // the coordinator learns of a cycle's changes only from RowChangesWritten, and the offsets of
    // their records are released by the same transaction: a report without the files loses the
    // changes, offsets outside the transaction replay records already committed
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.taskId()).thenReturn("0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      when(consumerGroupMetadata.groupId()).thenReturn(CONNECT_CONSUMER_GROUP_ID);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      TopicPartition partition0 = new TopicPartition(SRC_TOPIC_NAME, 0);
      TopicPartition partition1 = new TopicPartition(SRC_TOPIC_NAME, 1);
      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of(partition0, partition1));

      TableReference tableReference =
          TableReference.of("catalog", TableIdentifier.parse(TABLE_NAME));
      List<StagedChangeFile> stagedFiles =
          ImmutableList.of(
              stagedFile("s3://warehouse/staging/0/epoch/0.avro", 3L),
              stagedFile("s3://warehouse/staging/0/epoch/1.avro", 2L));
      StagedChangesResult stagedResult =
          new StagedChangesResult(
              tableReference, "0", stagedFiles, ImmutableSet.of(SRC_TOPIC_NAME));
      Map<TopicPartition, Offset> offsets =
          ImmutableMap.of(
              partition0, new Offset(6L, EventTestUtil.now()),
              partition1, new Offset(4L, EventTestUtil.now()));

      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(
              new SinkWriterResult(ImmutableList.<RecordWriteResult>of(stagedResult), offsets));

      Worker worker = new Worker(catalog, config, clientFactory, sinkWriter, context);
      worker.start();
      initConsumer();

      UUID commitId = UUID.randomUUID();
      Event commitRequest = new Event(config.connectGroupId(), new StartCommit(commitId));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(commitRequest)));
      worker.process();

      assertThat(producer.history()).hasSize(2);

      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.type()).isEqualTo(PayloadType.ROW_CHANGES_WRITTEN);
      RowChangesWritten rowChanges = (RowChangesWritten) event.payload();
      assertThat(rowChanges.commitId()).isEqualTo(commitId);
      assertThat(rowChanges.tableReference()).isEqualTo(tableReference);
      assertThat(rowChanges.taskId()).isEqualTo("0");
      assertThat(rowChanges.sourceTopics()).containsExactly(SRC_TOPIC_NAME);
      assertThat(rowChanges.stagedFiles())
          .as("every staged file of the cycle is reported")
          .extracting(StagedChangeFile::location, StagedChangeFile::recordCount)
          .containsExactly(
              tuple("s3://warehouse/staging/0/epoch/0.avro", 3L),
              tuple("s3://warehouse/staging/0/epoch/1.avro", 2L));

      event = AvroUtil.decode(producer.history().get(1).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
      DataComplete dataComplete = (DataComplete) event.payload();
      assertThat(dataComplete.commitId()).isEqualTo(commitId);
      assertThat(dataComplete.assignments())
          .extracting(tpo -> tuple(tpo.partition(), tpo.offset()))
          .containsExactlyInAnyOrder(tuple(0, 6L), tuple(1, 4L));

      assertThat(producer.commitCount()).as("the events go in one transaction").isEqualTo(1L);
      assertThat(producer.consumerGroupOffsetsHistory())
          .as("the source offsets are committed by the transaction that carries the events")
          .containsExactly(
              ImmutableMap.of(
                  CONNECT_CONSUMER_GROUP_ID,
                  ImmutableMap.of(
                      partition0, new OffsetAndMetadata(6L),
                      partition1, new OffsetAndMetadata(4L))));
    }
  }

  private static StagedChangeFile stagedFile(String location, long recordCount) {
    return new StagedChangeFile(
        location, 100L, recordCount, 0, 1, ImmutableMap.of(), ImmutableMap.of());
  }

  @Test
  public void testDataCompleteCarriesNoTaskIdInMergeOnRead() {
    // a coordinator one version behind cannot decode a DataComplete with task_id, so merge-on-read,
    // which has no use for it, must not send it during a rolling upgrade
    when(config.taskId()).thenReturn("0");

    assertThat(dataCompleteOfACycleWithoutRows().taskId()).isNull();
  }

  @Test
  public void testDataCompleteCarriesTheTaskIdInCopyOnWrite() {
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.taskId()).thenReturn("0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);

    assertThat(dataCompleteOfACycleWithoutRows().taskId()).isEqualTo("0");
  }

  private DataComplete dataCompleteOfACycleWithoutRows() {
    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(
              new SinkWriterResult(ImmutableList.<RecordWriteResult>of(), ImmutableMap.of()));

      Worker worker = new Worker(catalog, config, clientFactory, sinkWriter, context);
      worker.start();
      initConsumer();

      Event commitRequest = new Event(config.connectGroupId(), new StartCommit(UUID.randomUUID()));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(commitRequest)));
      worker.process();

      // a task that landed no rows still reports
      assertThat(producer.history()).hasSize(1);
      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
      return (DataComplete) event.payload();
    }
  }

  @Test
  public void testAnUndecodableEventIsSkipped() {
    // a worker takes no other task's files from the control topic, so skipping loses nothing: a
    // StartCommit or a rewrite assignment it did not read is timed out by the coordinator. Throwing
    // instead fails put(), and the task with it
    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(
              new SinkWriterResult(ImmutableList.<RecordWriteResult>of(), ImmutableMap.of()));

      Worker worker = new Worker(catalog, config, clientFactory, sinkWriter, context);
      worker.start();
      initConsumer();

      Event undecodable =
          new Event(
              config.connectGroupId(),
              new DataWritten(
                  StructType.of(),
                  UUID.randomUUID(),
                  TableReference.of("catalog", TableIdentifier.parse(TABLE_NAME)),
                  ImmutableList.of(EventTestUtil.createDataFile()),
                  ImmutableList.of()));
      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME, 0, 1, "key", EventTestUtil.withTruncatedPayload(undecodable)));
      UUID commitId = UUID.randomUUID();
      Event commitRequest = new Event(config.connectGroupId(), new StartCommit(commitId));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(commitRequest)));

      assertThatCode(worker::process).doesNotThrowAnyException();

      // the StartCommit behind it is still answered
      assertThat(producer.history()).hasSize(1);
      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
      assertThat(((DataComplete) event.payload()).commitId()).isEqualTo(commitId);
    }
  }

  @Test
  public void testARewriteAssignmentIsDroppedWhenThisTaskWasRebalanced() {
    // task ids are stable across restarts of the same task index, so without expectedPartitions a
    // task that came back with a different set of partitions would happily execute work planned for
    // the shape it used to have: rewriting files another task may now hold as well
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.taskId()).thenReturn("0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 1)));

    Worker worker = new Worker(catalog, config, clientFactory, mock(SinkWriter.class), context);
    worker.start();
    initConsumer();

    // planned when this task owned partition 0; it owns partition 1 now
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(rewriteAssigned("0", 0))));
    worker.process();

    assertThat(producer.history()).as("no answer, and no rewrite").isEmpty();
  }

  @Test
  public void testAPollOfTheControlTopicEndsWhileEventsKeepArriving() {
    // put() polls the control topic: reading until the topic fell silent would hold the task for
    // as long as the drains of copy-on-write, or the connectors sharing the topic, keep it busy
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.taskId()).thenReturn("0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

    Worker worker = new Worker(catalog, config, clientFactory, mock(SinkWriter.class), context);
    worker.start();
    initConsumer();

    AtomicBoolean floodOver =
        floodControlTopic(AvroUtil.encode(rewriteAssigned("1", 0)), 1, 10_000);
    worker.process();

    assertThat(floodOver).as("the poll ended while events kept arriving").isFalse();
  }

  @Test
  public void testARewriteAssignmentForAnotherTaskIsIgnored() {
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.taskId()).thenReturn("0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

    Worker worker = new Worker(catalog, config, clientFactory, mock(SinkWriter.class), context);
    worker.start();
    initConsumer();

    // one message per task: this one is addressed to task 1
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(rewriteAssigned("1", 0))));
    worker.process();

    assertThat(producer.history()).isEmpty();
  }

  @Test
  public void testASliceHandedOnlyToOtherTasksStillSupersedesTheRewriteThisTaskRuns()
      throws InterruptedException {
    when(config.isCopyOnWriteMode()).thenReturn(true);
    when(config.taskId()).thenReturn("task-0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
    when(config.controlMessageMaxBytes()).thenReturn(1024 * 1024);

    TableIdentifier copyOnWriteIdentifier = TableIdentifier.of(NAMESPACE, "cow");
    TableReference copyOnWriteReference = TableReference.of("catalog", copyOnWriteIdentifier);
    Table cow =
        catalog.createTable(
            copyOnWriteIdentifier,
            TestRewriteAssignmentRunner.SCHEMA,
            PartitionSpec.unpartitioned());
    TestRewriteAssignmentRunner.appendRows(
        cow, TestRewriteAssignmentRunner.row(1L, "a"), TestRewriteAssignmentRunner.row(2L, "b"));
    RewriteAssigned sliceZero =
        TestRewriteAssignmentRunner.assignment(
            cow,
            copyOnWriteReference,
            TestRewriteAssignmentRunner.staged(
                cow,
                copyOnWriteReference,
                StagedChangeSchema.OP_UPDATE,
                TestRewriteAssignmentRunner.row(1L, "a2")),
            UUID.randomUUID(),
            0);
    // slice 0 timed out, and this task's DataComplete missed the next cycle: slice 1 went to that
    // cycle's active tasks, with nothing in it for this one
    RewriteAssigned sliceOne =
        new RewriteAssigned(
            StructType.of(),
            sliceZero.commitId(),
            copyOnWriteReference,
            sliceZero.changeSetId(),
            1,
            sliceZero.baseSnapshotId(),
            sliceZero.normalizedRef(),
            ImmutableList.of(
                new Assignment(
                    "task-1", ImmutableList.of(), sliceZero.assignments().get(0).files())),
            0,
            1,
            sliceZero.identifierFieldIds());
    TableIdentifier otherIdentifier = TableIdentifier.of(NAMESPACE, "other");
    RewriteAssigned otherTable =
        new RewriteAssigned(
            StructType.of(),
            UUID.randomUUID(),
            TableReference.of("catalog", otherIdentifier),
            UUID.randomUUID(),
            0,
            1L,
            sliceZero.normalizedRef(),
            ImmutableList.of(new Assignment("task-0", ImmutableList.of(), ImmutableList.of())),
            0,
            1,
            ImmutableList.of(1));

    List<String> steps = Lists.newCopyOnWriteArrayList();
    TestRewriteAssignmentRunner.DataFileTrail trail =
        new TestRewriteAssignmentRunner.DataFileTrail(cow.io(), cow.location() + "/data/", steps);
    Table trailed = TestRewriteAssignmentRunner.probedBy(cow, trail);
    CountDownLatch otherTableStarted = new CountDownLatch(1);
    Catalog loading = mock(Catalog.class);
    when(loading.loadTable(copyOnWriteIdentifier)).thenReturn(trailed);
    when(loading.loadTable(otherIdentifier))
        .thenAnswer(
            invocation -> {
              otherTableStarted.countDown();
              throw new NoSuchTableException("Table does not exist: %s", otherIdentifier);
            });

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of());
    Worker worker = new Worker(loading, config, clientFactory, mock(SinkWriter.class), context);
    worker.start();
    initConsumer();

    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            1,
            "key",
            AvroUtil.encode(new Event(config.connectGroupId(), sliceZero))));
    worker.process();
    trail.awaitFirstRead();

    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            2,
            "key",
            AvroUtil.encode(new Event(config.connectGroupId(), sliceOne))));
    // queued behind slice 0 on the task's only rewrite thread: once it loads its table, slice 0 is
    // over, answered or not
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            3,
            "key",
            AvroUtil.encode(new Event(config.connectGroupId(), otherTable))));
    worker.process();
    trail.release();
    assertThat(otherTableStarted.await(30, TimeUnit.SECONDS)).isTrue();

    List<RewriteComplete> answers =
        producer.history().stream()
            .map(record -> AvroUtil.decode(record.value()).payload())
            .filter(RewriteComplete.class::isInstance)
            .map(RewriteComplete.class::cast)
            .filter(answer -> answer.tableReference().identifier().equals(copyOnWriteIdentifier))
            .collect(Collectors.toList());
    assertThat(answers)
        .as("answers to slice 0, which the coordinator replaced while it ran and throws away")
        .isEmpty();
    InMemoryFileIO storage = (InMemoryFileIO) cow.io();
    assertThat(
            steps.stream()
                .filter(step -> step.startsWith("write "))
                .map(step -> step.substring("write ".length()))
                .filter(storage::fileExists)
                .collect(Collectors.toList()))
        .as("replacement files of slice 0 still in the table's data location")
        .isEmpty();
  }

  @Test
  public void testAnAssignmentSentAsChunksStartsOnlyOnceAllOfThemHaveArrived() throws Exception {
    Fixture fixture = new Fixture();
    List<FileScanTaskDescriptor> files = fixture.whole.assignments().get(0).files();
    assertThat(files).as("a file of the plan per key, to have something to split").hasSize(2);

    fixture.worker.start();
    initConsumer();
    // half an assignment is not an assignment: rewriting the files of one chunk and answering for
    // them would have the coordinator commit the slice with the keys of the other chunk's files
    // left unreplaced beside their replacements
    fixture.deliver(1, fixture.chunk(files.subList(0, 1), 0, 2));
    fixture.worker.process();
    assertThat(answers()).as("nothing runs on part of an assignment").isEmpty();

    fixture.deliver(2, fixture.chunk(files.subList(1, 2), 1, 2));
    fixture.worker.process();

    List<RewriteComplete> answered = awaitAnswers(1);
    assertThat(answered).hasSize(1);
    assertThat(answered.get(0).status()).isEqualTo(RewriteComplete.STATUS_OK);
    assertThat(answered.get(0).dataFiles().stream().mapToLong(file -> file.recordCount()).sum())
        .as("both keys of the slice rewritten, by one run over both files")
        .isEqualTo(2L);
  }

  @Test
  public void testAnAssignmentMissingAChunkIsDroppedWhenANewerSliceArrives() throws Exception {
    Fixture fixture = new Fixture();
    RewriteAssigned replaced = fixture.slice(0, 1L, 2L);
    List<FileScanTaskDescriptor> replacedFiles = replaced.assignments().get(0).files();

    fixture.worker.start();
    initConsumer();
    // one chunk of the first attempt arrives and the other is still on its way
    fixture.deliver(1, fixture.chunk(replaced, replacedFiles.subList(1, 2), 1, 2));
    fixture.worker.process();

    // the coordinator timed that attempt out and handed out another slice, of other keys and so of
    // other files. The chunk left over from the attempt it gave up on belongs to no assignment any
    // more: mixed into this one it would rewrite a file this slice never planned
    RewriteAssigned replacement = fixture.slice(1, 3L, 4L);
    List<FileScanTaskDescriptor> files = replacement.assignments().get(0).files();
    fixture.deliver(2, fixture.chunk(replacement, files.subList(0, 1), 0, 2));
    fixture.deliver(3, fixture.chunk(replacement, files.subList(1, 2), 1, 2));
    fixture.worker.process();

    List<RewriteComplete> answered = awaitAnswers(1);
    assertThat(answered).as("the replaced attempt is never answered").hasSize(1);
    assertThat(answered.get(0).sliceSeq()).isEqualTo(1);
    assertThat(answered.get(0).dataFiles().stream().mapToLong(file -> file.recordCount()).sum())
        .as("the rows of this slice's own two files, and no row of the file left over")
        .isEqualTo(2L);
  }

  @Test
  public void testALateChunkOfASupersededSliceDoesNotDropTheBufferOfTheCurrentOne()
      throws Exception {
    // a message of a slice this task has already been told is superseded (a redelivery, or one
    // that overtook the current slice's own messages on the way in). Checked against the pending
    // buffer before the supersession check, it would look like a stale, unrelated slice and evict
    // the current slice's buffer, and the current slice would never complete
    Fixture fixture = new Fixture();

    fixture.worker.start();
    initConsumer();

    RewriteAssigned current = fixture.slice(1, 3L, 4L);
    List<FileScanTaskDescriptor> files = current.assignments().get(0).files();
    fixture.deliver(1, fixture.chunk(current, files.subList(0, 1), 0, 2));
    fixture.worker.process();

    // slice 0 of the very same attempt this task has already moved past to slice 1: it must not
    // be compared against the partial buffer slice 1 is building
    RewriteAssigned staleSliceZero = fixture.asEarlierSlice(current, 0);
    fixture.deliver(2, fixture.chunk(staleSliceZero, files.subList(0, 1), 0, 1));
    fixture.worker.process();

    fixture.deliver(3, fixture.chunk(current, files.subList(1, 2), 1, 2));
    fixture.worker.process();

    List<RewriteComplete> answered = awaitAnswers(1);
    assertThat(answered).as("the current slice still completes and answers once").hasSize(1);
    assertThat(answered.get(0).sliceSeq()).isEqualTo(1);
    assertThat(answered.get(0).dataFiles().stream().mapToLong(file -> file.recordCount()).sum())
        .as("both keys of the current slice rewritten, by one run over both its files")
        .isEqualTo(2L);
  }

  /** A copy-on-write table with one data file per key, and a worker rewriting slices of it. */
  private final class Fixture {
    private final TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "cow");
    private final TableReference reference = TableReference.of("catalog", identifier);
    private final Table cow;
    private final RewriteAssigned whole;
    private final Worker worker;

    private Fixture() {
      when(config.isCopyOnWriteMode()).thenReturn(true);
      when(config.taskId()).thenReturn("task-0");
      when(config.copyOnWriteRewriteThreads()).thenReturn(1);
      when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(200);
      when(config.copyOnWriteRewriteTimeoutMs()).thenReturn(60_000L);
      when(config.controlMessageMaxBytes()).thenReturn(1024 * 1024);

      this.cow =
          catalog.createTable(
              identifier, TestRewriteAssignmentRunner.SCHEMA, PartitionSpec.unpartitioned());
      for (long key = 1L; key <= 4L; key++) {
        TestRewriteAssignmentRunner.appendRows(cow, TestRewriteAssignmentRunner.row(key, "a"));
      }
      this.whole = slice(0, 1L, 2L);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of());
      this.worker = new Worker(catalog, config, clientFactory, mock(SinkWriter.class), context);
    }

    /** A slice updating {@code keys}, whose plan is the one file each of them sits in. */
    private RewriteAssigned slice(int sliceSeq, long... keys) {
      Record[] rows = new Record[keys.length];
      for (int index = 0; index < keys.length; index++) {
        rows[index] = TestRewriteAssignmentRunner.row(keys[index], "v2");
      }
      return TestRewriteAssignmentRunner.assignment(
          cow,
          reference,
          TestRewriteAssignmentRunner.staged(cow, reference, StagedChangeSchema.OP_UPDATE, rows),
          UUID.randomUUID(),
          sliceSeq);
    }

    private RewriteAssigned chunk(
        List<FileScanTaskDescriptor> files, int chunkIndex, int chunkCount) {
      return chunk(whole, files, chunkIndex, chunkCount);
    }

    /** The same attempt, renumbered as an earlier slice: what a stale, superseded message names. */
    private RewriteAssigned asEarlierSlice(RewriteAssigned assignment, int sliceSeq) {
      return new RewriteAssigned(
          cow.spec().partitionType(),
          assignment.commitId(),
          reference,
          assignment.changeSetId(),
          sliceSeq,
          assignment.baseSnapshotId(),
          assignment.normalizedRef(),
          assignment.assignments(),
          assignment.ownerIndex(),
          assignment.ownerCount(),
          assignment.identifierFieldIds());
    }

    /** One message of {@code assignment}, carrying only {@code files}. */
    private RewriteAssigned chunk(
        RewriteAssigned assignment,
        List<FileScanTaskDescriptor> files,
        int chunkIndex,
        int chunkCount) {
      return new RewriteAssigned(
          cow.spec().partitionType(),
          assignment.commitId(),
          reference,
          assignment.changeSetId(),
          assignment.sliceSeq(),
          assignment.baseSnapshotId(),
          assignment.normalizedRef(),
          ImmutableList.of(new Assignment("task-0", ImmutableList.of(), files)),
          0,
          1,
          assignment.identifierFieldIds(),
          chunkIndex,
          chunkCount);
    }

    private void deliver(int offset, RewriteAssigned payload) {
      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME,
              0,
              offset,
              "key",
              AvroUtil.encode(new Event(config.connectGroupId(), payload))));
    }
  }

  private List<RewriteComplete> answers() {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()).payload())
        .filter(RewriteComplete.class::isInstance)
        .map(RewriteComplete.class::cast)
        .collect(Collectors.toList());
  }

  private List<RewriteComplete> awaitAnswers(int count) throws InterruptedException {
    for (int waited = 0; waited < 300 && answers().size() < count; waited++) {
      Thread.sleep(100);
    }
    return answers();
  }

  private Event rewriteAssigned(String taskId, int partition) {
    return new Event(
        config.connectGroupId(),
        new RewriteAssigned(
            StructType.of(),
            UUID.randomUUID(),
            TableReference.of("catalog", TableIdentifier.parse(TABLE_NAME)),
            UUID.randomUUID(),
            0,
            1L,
            new StagedChangeFile(
                "s3://warehouse/normalized-0.avro",
                10L,
                1L,
                0,
                1,
                ImmutableMap.of(),
                ImmutableMap.of()),
            ImmutableList.of(
                new Assignment(
                    taskId,
                    ImmutableList.of(new TopicPartitionRef(SRC_TOPIC_NAME, partition)),
                    ImmutableList.of())),
            0,
            1,
            ImmutableList.of(1)));
  }
}
