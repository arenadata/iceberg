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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.StatisticsAssertions;
import org.apache.iceberg.connect.data.RecordRoutingStrategy;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.connect.data.StatisticsStep;
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
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.Logger;

public class TestCommitterImpl extends ChannelTestBase {

  private static final TopicPartition SOURCE_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 0);
  private static final TopicPartition CONTROL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);

  private final List<MockProducer<String, byte[]>> producers = Lists.newArrayList();
  private final List<MockConsumer<String, byte[]>> consumers = Lists.newArrayList();
  private final Set<TopicPartition> assignment = Sets.newHashSet();
  // one per coordinator started, in start order
  private final List<StubTableCommitter> tableCommitters =
      Collections.synchronizedList(Lists.newArrayList());
  private final FakePluginMetrics pluginMetrics = new FakePluginMetrics();
  private final Logger metricsLog = mock(Logger.class);
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

  @Test
  public void
      testTheLeaderRegistersTheThreeMetricsBeforeItsCoordinatorIsBuiltAndTakesThemOffOnClose() {
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      List<String> registeredWhenBuilt = Collections.synchronizedList(Lists.newArrayList());
      CommitterImpl committer = committer(() -> registeredWhenBuilt.addAll(pluginMetrics.names()));
      assignment.add(SOURCE_PARTITION);

      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));

      assertThat(registeredWhenBuilt)
          .as("the metrics are there before the coordinator attaches its health")
          .containsExactlyInAnyOrder(
              "statistics-tables-gap-over-limit",
              "statistics-tables-missing-baseline",
              "statistics-tables-stale");
      // the health of the coordinator is what the metrics read
      StubTableCommitter first = tableCommitters.get(0);
      first.health.update(TABLE_IDENTIFIER, flags -> new Flags(true, false, true));
      assertThat(pluginMetrics.metricValue("statistics-tables-gap-over-limit")).isEqualTo(1.0);
      assertThat(pluginMetrics.metricValue("statistics-tables-missing-baseline")).isEqualTo(0.0);
      assertThat(pluginMetrics.metricValue("statistics-tables-stale")).isEqualTo(1.0);
      verifyNoInteractions(metricsLog);

      committer.close(ImmutableList.of(SOURCE_PARTITION));

      assertThat(pluginMetrics.names()).as("taken off when the coordinator is stopped").isEmpty();
      awaitCoordinatorsStopped(1);
    }
  }

  @Test
  public void testNextLeadershipRegistersMetricsAgainWithoutClashOfNames() {
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);

      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      assertThat(pluginMetrics.names()).isEmpty();
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));

      assertThat(pluginMetrics.names()).hasSize(3);
      verifyNoInteractions(metricsLog);
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      awaitCoordinatorsStopped(2);
    }
  }

  @Test
  public void testLeaderPartitionBackOnSameTaskWhileOldCoordinatorIsInTerminate() {
    // eager rebalance gives partition 0 back to the same task, and the new coordinator is built
    // while the old one still waits for its pools in terminate()
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      StubTableCommitter oldOne = tableCommitters.get(0);
      oldOne.holdStop();
      oldOne.health.update(TABLE_IDENTIFIER, flags -> new Flags(true, true, true));

      committer.close(ImmutableList.of(SOURCE_PARTITION));
      StatisticsAssertions.await(
          "the old coordinator in terminate()", () -> oldOne.stopEntered.getCount() == 0);
      assertThatCode(
              () -> committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION)))
          .as("names taken by the old coordinator must not fail open()")
          .doesNotThrowAnyException();

      assertThat(tableCommitters).hasSize(2);
      StubTableCommitter newOne = tableCommitters.get(1);
      newOne.health.update(
          TableIdentifier.of("db", "other"), flags -> new Flags(false, true, true));
      assertThat(pluginMetrics.names()).hasSize(3);
      assertThat(pluginMetrics.metricValue("statistics-tables-gap-over-limit"))
          .as("the health of the new coordinator, not the old one's")
          .isEqualTo(0.0);
      assertThat(pluginMetrics.metricValue("statistics-tables-missing-baseline")).isEqualTo(1.0);
      assertThat(pluginMetrics.metricValue("statistics-tables-stale")).isEqualTo(1.0);

      // the old one finishes terminate() and detaches
      oldOne.releaseStop();
      awaitCoordinatorsStopped(1);

      assertThat(pluginMetrics.names()).as("the metrics of the new one stay").hasSize(3);
      assertThat(pluginMetrics.metricValue("statistics-tables-gap-over-limit")).isEqualTo(0.0);
      assertThat(pluginMetrics.metricValue("statistics-tables-missing-baseline")).isEqualTo(1.0);
      assertThat(pluginMetrics.metricValue("statistics-tables-stale")).isEqualTo(1.0);
      // and the health of the old one no longer moves them
      oldOne.health.update(TABLE_IDENTIFIER, flags -> new Flags(true, true, true));
      assertThat(pluginMetrics.metricValue("statistics-tables-gap-over-limit")).isEqualTo(0.0);

      committer.close(ImmutableList.of(SOURCE_PARTITION));
      awaitCoordinatorsStopped(2);
      verifyNoInteractions(metricsLog);
    }
  }

  @Test
  public void testCoordinatorThatFailsToBeBuiltLeavesNoMetricsAndNextStartWarnsOfNothing() {
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      AtomicBoolean fail = new AtomicBoolean(true);
      CommitterImpl committer =
          committer(
              () -> {
                if (fail.get()) {
                  throw new IllegalStateException("no consumer");
                }
              });
      assignment.add(SOURCE_PARTITION);

      assertThatThrownBy(
              () -> committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("no consumer");
      assertThat(pluginMetrics.names()).as("nothing stopCoordinator() could take off").isEmpty();

      fail.set(false);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));

      assertThat(pluginMetrics.names()).hasSize(3);
      verifyNoInteractions(metricsLog);
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      awaitCoordinatorsStopped(1);
    }
  }

  @Test
  public void testWithStatisticsOffNoMetricsAreAskedForNorRegistered() {
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);

      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      committer.close(ImmutableList.of(SOURCE_PARTITION));

      assertThat(tableCommitters).hasSize(1);
      assertThat(tableCommitters.get(0).health).isSameAs(StatisticsHealth.NOOP);
      verify(context, never()).pluginMetrics();
      assertThat(pluginMetrics.metricsAdded()).isEqualTo(0);
      verifyNoInteractions(metricsLog);
      awaitCoordinatorsStopped(1);
    }
  }

  @Test
  public void testContextWithoutPluginMetricsMeansNoMetricsAndNoLog() {
    // a test double of the context: pluginMetrics() is null
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(null);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);

      assertThatCode(
              () -> committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();
      assertThatCode(() -> committer.close(ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();

      assertThat(tableCommitters).as("the coordinator starts all the same").hasSize(1);
      assertThat(pluginMetrics.metricsAdded()).isEqualTo(0);
      verifyNoInteractions(metricsLog);
      awaitCoordinatorsStopped(1);
    }
  }

  @Test
  public void testRuntimeBelow41WarnsOncePerInstanceAndCoordinatorRunsWithoutMetrics() {
    enableStatistics();
    when(context.pluginMetrics()).thenThrow(new NoSuchMethodError("pluginMetrics"));

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);

      assertThatCode(
              () -> committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();
      // the task is not brought down: the poll still finds the coordinator alive
      assertThatCode(() -> committer.save(ImmutableList.of())).doesNotThrowAnyException();
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      assertThatCode(
              () -> committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();
      committer.close(ImmutableList.of(SOURCE_PARTITION));

      assertThat(tableCommitters).as("a coordinator per leadership").hasSize(2);
      verify(metricsLog, times(1)).warn(contains("Kafka Connect 4.1 or later"));
      verify(metricsLog, times(1)).warn(anyString());
      assertThat(pluginMetrics.metricsAdded()).isEqualTo(0);
      awaitCoordinatorsStopped(2);
    }
  }

  @Test
  public void testNameTakenWarnsAndCoordinatorRunsWithoutMetricsUntilNextStart() {
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);
    pluginMetrics.reserve("statistics-tables-stale");

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);

      assertThatCode(
              () -> committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();

      assertThat(tableCommitters).as("the coordinator starts all the same").hasSize(1);
      assertThat(pluginMetrics.names()).as("none of the three, not two of them").isEmpty();
      verify(metricsLog, times(1)).warn(anyString(), any(IllegalArgumentException.class));

      committer.close(ImmutableList.of(SOURCE_PARTITION));
      pluginMetrics.unreserve("statistics-tables-stale");
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));

      assertThat(pluginMetrics.names()).hasSize(3);
      verify(metricsLog, times(1)).warn(anyString(), any(IllegalArgumentException.class));
      committer.close(ImmutableList.of(SOURCE_PARTITION));
      awaitCoordinatorsStopped(2);
    }
  }

  @Test
  public void testStoppingCoordinatorOnClosedPluginMetricsIsLoggedAtDebugAndNothingMore() {
    // the runtime closes the PluginMetrics of a task that is stopping, before its close() call
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      pluginMetrics.close();

      assertThatCode(() -> committer.close(ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();

      verify(metricsLog, times(1)).debug(anyString(), any(IllegalStateException.class));
      verify(metricsLog, never()).warn(anyString());
      verify(metricsLog, never()).warn(anyString(), any(Throwable.class));
      awaitCoordinatorsStopped(1);
    }
  }

  @Test
  public void
      testStoppingTheCoordinatorWhenAMetricIsAlreadyGoneIsLoggedAtDebugAndTheRestIsRemoved() {
    enableStatistics();
    when(context.pluginMetrics()).thenReturn(pluginMetrics);

    try (MockedStatic<KafkaUtils> ignored = mockLeaderKafkaUtils()) {
      CommitterImpl committer = committer(() -> {});
      assignment.add(SOURCE_PARTITION);
      committer.open(catalog, config, context, ImmutableList.of(SOURCE_PARTITION));
      pluginMetrics.removeMetric(pluginMetrics.metric("statistics-tables-stale"));

      assertThatCode(() -> committer.close(ImmutableList.of(SOURCE_PARTITION)))
          .doesNotThrowAnyException();

      verify(metricsLog, times(1)).debug(anyString(), any(IllegalArgumentException.class));
      assertThat(pluginMetrics.names()).isEmpty();
      awaitCoordinatorsStopped(1);
    }
  }

  private void enableStatistics() {
    when(config.statisticsInUse()).thenReturn(true);
    when(config.statisticsHealthRecheckIntervalMs()).thenReturn(60_000L);
  }

  /**
   * A committer whose coordinators are real ones over a stub table committer; {@code
   * whenCoordinatorIsBuilt} runs right before each is built.
   */
  private CommitterImpl committer(Runnable whenCoordinatorIsBuilt) {
    return new CommitterImpl(icebergSinkConfig -> clientFactory, metricsLog) {
      @Override
      Coordinator newCoordinator(StatisticsMetrics metrics) {
        whenCoordinatorIsBuilt.run();
        StubTableCommitter tableCommitter = new StubTableCommitter();
        tableCommitters.add(tableCommitter);
        return new Coordinator(
            config,
            ImmutableList.of(),
            clientFactory,
            context,
            new StatisticsStep(),
            (send, sendInOneTransaction, transitionPool, health, step) ->
                tableCommitter.startedWith(health),
            metrics) {
          @Override
          void process() {
            // the consumer of a test does not block for the poll, and there is nothing to read
            try {
              Thread.sleep(5);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        };
      }
    };
  }

  private void awaitCoordinatorsStopped(int count) {
    StatisticsAssertions.await(
        "the coordinators stopped",
        () ->
            consumers.size() >= count
                && consumers.subList(0, count).stream().allMatch(MockConsumer::closed));
  }

  private MockedStatic<KafkaUtils> mockLeaderKafkaUtils() {
    ConsumerGroupDescription group = mock(ConsumerGroupDescription.class);
    when(group.state()).thenReturn(ConsumerGroupState.STABLE);
    Collection<MemberDescription> members =
        ImmutableList.of(
            new MemberDescription(
                null,
                Optional.empty(),
                null,
                null,
                new MemberAssignment(ImmutableSet.of(SOURCE_PARTITION))));
    when(group.members()).thenReturn(members);

    MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class);
    kafkaUtils.when(() -> KafkaUtils.consumerGroupDescription(any(), any())).thenReturn(group);
    return kafkaUtils;
  }

  /**
   * The table committer of a coordinator that has nothing to commit; its {@code stop()} is where
   * the test holds the coordinator in {@code terminate()}.
   */
  private static class StubTableCommitter implements TableCommitter {
    private final CountDownLatch stopEntered = new CountDownLatch(1);
    private final CountDownLatch stopReleased = new CountDownLatch(1);
    private volatile boolean holdStop = false;
    private volatile StatisticsHealth health;

    private StubTableCommitter startedWith(StatisticsHealth statisticsHealth) {
      this.health = statisticsHealth;
      return this;
    }

    private void holdStop() {
      this.holdStop = true;
    }

    private void releaseStop() {
      stopReleased.countDown();
    }

    @Override
    public CommitResult commit(TableCommitRequest request) {
      return CommitResult.committed(request.envelopes());
    }

    @Override
    public void stop() {
      stopEntered.countDown();
      if (holdStop) {
        try {
          if (!stopReleased.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("terminate() was not released");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
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
