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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.data.RecordRoutingStrategy;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class TestCommitterImpl extends ChannelTestBase {

  private static final TopicPartition SOURCE_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 0);
  private static final TopicPartition CONTROL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);

  private final List<MockProducer<String, byte[]>> producers = Lists.newArrayList();
  private final List<MockConsumer<String, byte[]>> consumers = Lists.newArrayList();
  private final Set<TopicPartition> assignment = Sets.newHashSet();
  private SinkTaskContext context;
  private long controlTopicOffset = 0L;

  @BeforeEach
  public void beforeTask() {
    when(config.taskId()).thenReturn("0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.routingStrategy()).thenReturn(RecordRoutingStrategy.ALL_TABLES);

    // a stopped worker closes its clients, so every worker gets its own
    when(clientFactory.createProducer(any())).thenAnswer(invocation -> newProducer());
    when(clientFactory.createConsumer(any())).thenAnswer(invocation -> newConsumer());

    context = mock(SinkTaskContext.class);
    when(context.assignment()).thenAnswer(invocation -> ImmutableSet.copyOf(assignment));
  }

  @Test
  public void testIsLeader() {
    CommitterImpl committer = new CommitterImpl();

    MemberAssignment assignment1 =
        new MemberAssignment(
            ImmutableSet.of(new TopicPartition("topic1", 0), new TopicPartition("topic2", 1)));
    MemberDescription member1 =
        new MemberDescription(null, Optional.empty(), null, null, assignment1);

    MemberAssignment assignment2 =
        new MemberAssignment(
            ImmutableSet.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1)));
    MemberDescription member2 =
        new MemberDescription(null, Optional.empty(), null, null, assignment2);

    List<MemberDescription> members = ImmutableList.of(member1, member2);

    List<TopicPartition> assignments =
        ImmutableList.of(new TopicPartition("topic2", 1), new TopicPartition("topic1", 0));
    assertThat(committer.containsFirstPartition(members, assignments)).isTrue();

    assignments =
        ImmutableList.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1));
    assertThat(committer.containsFirstPartition(members, assignments)).isFalse();
  }

  @Test
  public void testACopyOnWriteTaskAnswersEveryStartCommitWithoutIncomingRecords() {
    when(config.isCopyOnWriteMode()).thenReturn(true);

    try (MockedStatic<KafkaUtils> ignored = mockKafkaUtils()) {
      CommitterImpl committer = new CommitterImpl(icebergSinkConfig -> clientFactory);

      // a restarted task: every record was reported before the restart, so its input is empty
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      committer.save(ImmutableList.of());
      UUID firstCommitId = UUID.randomUUID();
      publishStartCommit(firstCommitId);
      committer.save(ImmutableList.of());

      // a rebalance stops the worker, and the partition comes back without records
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      committer.save(ImmutableList.of());
      UUID secondCommitId = UUID.randomUUID();
      publishStartCommit(secondCommitId);
      committer.save(ImmutableList.of());

      assertThat(dataCompletes())
          .as("a copy-on-write task that owns partitions must join the active tasks of every cycle")
          .extracting(DataComplete::commitId, DataComplete::taskId, this::partitions)
          .containsExactly(
              tuple(firstCommitId, "0", ImmutableList.of(SOURCE_PARTITION)),
              tuple(secondCommitId, "0", ImmutableList.of(SOURCE_PARTITION)));
    }
  }

  @Test
  public void testACopyOnWriteTaskBoundsEveryPollOfItsIdleInput() {
    when(config.isCopyOnWriteMode()).thenReturn(true);

    try (MockedStatic<KafkaUtils> ignored = mockKafkaUtils()) {
      CommitterImpl committer = new CommitterImpl(icebergSinkConfig -> clientFactory);
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));

      // idle or paused input: Connect calls put() with an empty batch after each poll, and
      // forgets the timeout once that poll has used it
      committer.save(ImmutableList.of());
      committer.save(ImmutableList.of());

      ArgumentCaptor<Long> timeouts = ArgumentCaptor.forClass(Long.class);
      verify(context, times(2)).timeout(timeouts.capture());
      assertThat(timeouts.getAllValues())
          .allSatisfy(timeout -> assertThat(timeout).isBetween(1L, 1_000L));
    }
  }

  @Test
  public void testAMergeOnReadTaskWithoutRecordsStartsNoWorkerAndLeavesThePollAlone() {
    try (MockedStatic<KafkaUtils> ignored = mockKafkaUtils()) {
      CommitterImpl committer = new CommitterImpl(icebergSinkConfig -> clientFactory);
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));

      committer.save(ImmutableList.of());
      committer.save(null);

      assertThat(consumers).isEmpty();
      verify(context, never()).timeout(anyLong());
    }
  }

  @Test
  public void testACopyOnWriteTaskWithoutPartitionsStartsNoWorker() {
    when(config.isCopyOnWriteMode()).thenReturn(true);

    try (MockedStatic<KafkaUtils> ignored = mockKafkaUtils()) {
      CommitterImpl committer = new CommitterImpl(icebergSinkConfig -> clientFactory);

      // Connect does not call open() for an empty assignment, but still calls put()
      committer.save(ImmutableList.of());

      // every partition revoked
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      assignment.clear();
      committer.save(ImmutableList.of());

      assertThat(consumers).isEmpty();
    }
  }

  private MockedStatic<KafkaUtils> mockKafkaUtils() {
    MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class);
    // not a stable group, so this task is never the leader
    kafkaUtils
        .when(() -> KafkaUtils.consumerGroupDescription(any(), any()))
        .thenReturn(mock(ConsumerGroupDescription.class));
    return kafkaUtils;
  }

  private MockProducer<String, byte[]> newProducer() {
    MockProducer<String, byte[]> newProducer =
        new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
    newProducer.initTransactions();
    producers.add(newProducer);
    return newProducer;
  }

  private MockConsumer<String, byte[]> newConsumer() {
    MockConsumer<String, byte[]> newConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumers.add(newConsumer);
    return newConsumer;
  }

  /** Delivers the event to the control topic reader of the current worker, if there is one. */
  private void publishStartCommit(UUID commitId) {
    if (consumers.isEmpty()) {
      return;
    }

    MockConsumer<String, byte[]> current = consumers.get(consumers.size() - 1);
    if (current.assignment().isEmpty()) {
      current.rebalance(ImmutableList.of(CONTROL_PARTITION));
      current.updateBeginningOffsets(ImmutableMap.of(CONTROL_PARTITION, 0L));
    }

    Event event = new Event(CONNECT_CONSUMER_GROUP_ID, new StartCommit(commitId));
    controlTopicOffset += 1;
    current.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, controlTopicOffset, "key", AvroUtil.encode(event)));
  }

  private List<DataComplete> dataCompletes() {
    return producers.stream()
        .flatMap(sent -> sent.history().stream())
        .map(record -> AvroUtil.decode(record.value()))
        .filter(event -> event.payload().type() == PayloadType.DATA_COMPLETE)
        .map(event -> (DataComplete) event.payload())
        .collect(Collectors.toList());
  }

  private List<TopicPartition> partitions(DataComplete dataComplete) {
    return dataComplete.assignments().stream()
        .map(offset -> new TopicPartition(offset.topic(), offset.partition()))
        .collect(Collectors.toList());
  }
}
