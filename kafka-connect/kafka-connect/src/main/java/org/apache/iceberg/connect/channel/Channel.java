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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.EventHeader;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

  // no table's files ride on these, so an undecodable one of them can be skipped by anyone
  private static final Set<PayloadType> EVENTS_WITHOUT_TABLE_FILES =
      Sets.immutableEnumSet(
          PayloadType.START_COMMIT,
          PayloadType.DATA_COMPLETE,
          PayloadType.COMMIT_TO_TABLE,
          PayloadType.COMMIT_COMPLETE,
          PayloadType.REWRITE_ASSIGNED,
          PayloadType.REWRITE_COMPLETE);

  // how long one call reads a control topic that keeps bringing events, see consumeAvailable()
  private static final Duration READ_QUOTA = Duration.ofSeconds(1);

  private final String controlTopic;
  private final String connectGroupId;
  private final Producer<String, byte[]> producer;
  private final Consumer<String, byte[]> consumer;
  private final SinkTaskContext context;
  private final Admin admin;
  private final Map<Integer, Long> controlTopicOffsets = Maps.newHashMap();
  private final String producerId;
  // group id and payload type id of the undecodable events already skipped with a WARN
  private final Set<Pair<String, Integer>> skippedUndecodableEvents = Sets.newHashSet();
  private String heldAt = null;
  // where the control topic ended when each partition was assigned, see isControlTopicCaughtUp()
  private final Map<TopicPartition, Long> controlTopicEndOffsets = Maps.newHashMap();
  private boolean controlTopicCaughtUp = false;
  private boolean endOffsetFetchWarned = false;

  Channel(
      String name,
      String consumerGroupId,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    this.controlTopic = config.controlTopic();
    this.connectGroupId = config.connectGroupId();
    this.context = context;

    String transactionalId = config.transactionalPrefix() + name + config.transactionalSuffix();
    this.producer = clientFactory.createProducer(transactionalId);
    this.consumer = clientFactory.createConsumer(consumerGroupId);
    this.admin = clientFactory.createAdmin();

    this.producerId = UUID.randomUUID().toString();
  }

  protected void send(Event event) {
    send(ImmutableList.of(event), ImmutableMap.of());
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  protected void send(List<Event> events, Map<TopicPartition, Offset> sourceOffsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    sourceOffsets.forEach((k, v) -> offsetsToCommit.put(k, new OffsetAndMetadata(v.offset())));

    List<ProducerRecord<String, byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.type().name());
                  byte[] data = AvroUtil.encode(event);
                  // key by producer ID to keep event order
                  return new ProducerRecord<>(controlTopic, producerId, data);
                })
            .collect(Collectors.toList());

    synchronized (producer) {
      producer.beginTransaction();
      try {
        // NOTE: we shouldn't call get() on the future in a transactional context,
        // see docs for org.apache.kafka.clients.producer.KafkaProducer
        recordList.forEach(producer::send);
        if (!sourceOffsets.isEmpty()) {
          producer.sendOffsetsToTransaction(
              offsetsToCommit, KafkaUtils.consumerGroupMetadata(context));
        }
        producer.commitTransaction();
      } catch (Exception e) {
        try {
          producer.abortTransaction();
        } catch (Exception ex) {
          LOG.warn("Error aborting producer transaction", ex);
        }
        throw e;
      }
    }
  }

  protected abstract boolean receive(Envelope envelope);

  /**
   * Whether an undecodable event of this channel's group that may carry table files holds reading
   * at it, rather than being skipped. A channel that does not act on other tasks' files loses
   * nothing by skipping one.
   */
  protected boolean holdsAtUndecodableTableFiles() {
    return false;
  }

  /**
   * Whether reading is held at an undecodable event, see {@link #holdsAtUndecodableTableFiles()}.
   * Only a restart lifts it: the control topic is read again from the committed offsets, which are
   * not past the event.
   */
  protected boolean isHeldAtUndecodableEvent() {
    return heldAt != null;
  }

  /**
   * Reads the control topic and hands each event of this channel's group to {@link
   * #receive(Envelope)}.
   *
   * <p>An event that cannot be decoded (written by another version of the connector, typically)
   * never fails the call: it is skipped by its group and payload type, which precede the payload.
   * Only an event of this group that may carry table files, on a channel that {@link
   * #holdsAtUndecodableTableFiles() holds at one}, stops reading at it, in every partition.
   *
   * <p>Returns at the first empty poll, or once this call has been reading for the longer of {@code
   * pollDuration} and {@link #READ_QUOTA}: the caller's own work waits for the return, and a topic
   * kept busy (by copy-on-write drains, whose answers lead to the next assignment, or by the
   * connectors sharing it) need never fall silent. What is left is read by the next call.
   */
  protected void consumeAvailable(Duration pollDuration) {
    if (heldAt != null) {
      LOG.debug("Not reading the control topic past the undecodable event at {}", heldAt);
      // the poll keeps the consumer in its group. A partition handed over by a rebalance is not
      // paused yet, and whatever it returns is dropped: nothing may be read past the event
      consumer.pause(consumer.assignment());
      consumer.poll(pollDuration);
      return;
    }

    long deadlineNanos = System.nanoTime() + Math.max(pollDuration.toNanos(), READ_QUOTA.toNanos());
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      for (ConsumerRecord<String, byte[]> record : records) {
        Event event = decode(record);
        if (heldAt != null) {
          consumer.pause(consumer.assignment());
          return;
        }

        // the consumer stores the offsets that corresponds to the next record to consume,
        // so increment the record offset by one
        controlTopicOffsets.put(record.partition(), record.offset() + 1);

        if (event != null && event.groupId().equals(connectGroupId)) {
          LOG.debug("Received event of type: {}", event.type().name());
          if (receive(new Envelope(event, record.partition(), record.offset()))) {
            LOG.info("Handled event of type: {}", event.type().name());
          }
        }
      }

      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        LOG.debug("Control topic still has events after the read budget, reading on next call");
        return;
      }
      records = consumer.poll(Duration.ofNanos(Math.min(pollDuration.toNanos(), remainingNanos)));
    }
  }

  /** Returns the record's event, or null if it cannot be decoded. */
  private Event decode(ConsumerRecord<String, byte[]> record) {
    try {
      return AvroUtil.decode(record.value());
    } catch (RuntimeException e) {
      skipOrHoldAt(record, e);
      return null;
    }
  }

  private void skipOrHoldAt(ConsumerRecord<String, byte[]> record, RuntimeException cause) {
    EventHeader header;
    try {
      header = AvroUtil.decodeHeader(record.value());
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping control topic record at partition {}, offset {}: it is not a readable event",
          record.partition(),
          record.offset(),
          cause);
      return;
    }

    if (holdsAtUndecodableTableFiles()
        && header.groupId().equals(connectGroupId)
        && !EVENTS_WITHOUT_TABLE_FILES.contains(header.type())) {
      this.heldAt = String.format("partition %s, offset %s", record.partition(), record.offset());
      LOG.error(
          "Cannot decode the event at control topic partition {}, offset {} of group {} with "
              + "payload type id {}. It may carry table files whose source offsets are already "
              + "committed, so no commits are made and the control topic is not read past it "
              + "until restart. The event was likely written by another version of the "
              + "connector: bring the task running the coordinator to the version of the workers "
              + "and restart it",
          record.partition(),
          record.offset(),
          header.groupId(),
          header.typeId(),
          cause);
      return;
    }

    if (skippedUndecodableEvents.add(Pair.of(header.groupId(), header.typeId()))) {
      LOG.warn(
          "Skipping undecodable events of group {} with payload type id {}, first at control topic "
              + "partition {}, offset {}; they were likely written by another version of the "
              + "connector",
          header.groupId(),
          header.typeId(),
          record.partition(),
          record.offset(),
          cause);
    } else {
      LOG.debug(
          "Skipping undecodable event of group {} with payload type id {} at control topic "
              + "partition {}, offset {}",
          header.groupId(),
          header.typeId(),
          record.partition(),
          record.offset());
    }
  }

  /**
   * Whether this channel has read the control topic up to where it ended when its partitions were
   * assigned.
   *
   * <p>{@link #consumeAvailable} stops at the first empty poll or at its read budget, which says
   * nothing about how much of the topic is behind it: right after a start or a rebalance the events
   * of a cycle already in flight are still ahead of the consumer. The copy-on-write staging cleanup
   * waits for this: the commit buffer is one of its sources of reachability, and a sweep on a
   * half-read topic would delete the staged files of responses still on their way.
   *
   * <p>Latches once true: the end offsets are a start-up mark, not a watermark to keep chasing. A
   * coordinator recreated by a rebalance does not inherit it: it gets a channel of its own.
   */
  protected boolean isControlTopicCaughtUp() {
    if (controlTopicCaughtUp) {
      return true;
    }

    Set<TopicPartition> assignment = consumer.assignment();
    if (assignment.isEmpty()) {
      return false;
    }

    try {
      List<TopicPartition> unmarked =
          assignment.stream()
              .filter(partition -> !controlTopicEndOffsets.containsKey(partition))
              .collect(Collectors.toList());
      if (!unmarked.isEmpty()) {
        controlTopicEndOffsets.putAll(consumer.endOffsets(unmarked));
      }

      for (TopicPartition partition : assignment) {
        Long end = controlTopicEndOffsets.get(partition);
        if (end == null || consumer.position(partition) < end) {
          return false;
        }
      }
    } catch (RuntimeException e) {
      // nothing this mark gates is urgent enough to fail a poll of the coordinator over, and the
      // next call asks again
      if (!endOffsetFetchWarned) {
        this.endOffsetFetchWarned = true;
        LOG.warn(
            "Cannot tell whether the control topic has been read to the end offsets its "
                + "partitions had when they were assigned. Whatever waits for that -- the "
                + "copy-on-write staging cleanup does -- waits until this succeeds",
            e);
      }
      return false;
    }

    this.controlTopicCaughtUp = true;
    LOG.info("Control topic read up to the end offsets of the partitions of this channel");
    return true;
  }

  protected Map<Integer, Long> controlTopicOffsets() {
    return controlTopicOffsets;
  }

  protected void commitConsumerOffsets() {
    commitConsumerOffsets(controlTopicOffsets());
  }

  /**
   * Commits control topic offsets that may lag {@link #controlTopicOffsets()}: a caller that has
   * consumed an event but not yet acted on it holds its offset back, or a restart loses the event.
   */
  protected void commitConsumerOffsets(Map<Integer, Long> offsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    offsets.forEach(
        (k, v) ->
            offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  void start() {
    consumer.subscribe(ImmutableList.of(controlTopic));

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofSeconds(1));
  }

  void stop() {
    LOG.info("Channel stopping");
    producer.close();
    consumer.close();
    admin.close();
  }
}
