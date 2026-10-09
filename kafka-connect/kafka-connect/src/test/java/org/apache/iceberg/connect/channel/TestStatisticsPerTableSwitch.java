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

import static org.apache.iceberg.connect.StatisticsAssertions.assertNoFilesUnder;
import static org.apache.iceberg.connect.StatisticsAssertions.assertSomeFilesUnder;
import static org.apache.iceberg.connect.StatisticsAssertions.awaitStatistics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.MetadataEvents;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.connect.data.NdvChain;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

/**
 * The switch {@code iceberg.table.<name>.statistics.enabled} from the connector properties to the
 * table files with nothing replaced: a real {@link IcebergSinkConfig}, a {@link Worker} with a real
 * {@link SinkWriter} and a {@link Coordinator} committing merge-on-read to an in-memory catalog;
 * only the Kafka clients are played. The other merge-on-read statistics tests take the table config
 * from a mock, so a writer and a coordinator reading the switch under different names pass them.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class TestStatisticsPerTableSwitch extends ChannelTestBase {
  private static final TableIdentifier ON = TableIdentifier.of(NAMESPACE, "switch_on");
  private static final TableIdentifier OFF = TableIdentifier.of(NAMESPACE, "switch_off");

  private MockProducer<String, byte[]> workerProducer;
  private MockConsumer<String, byte[]> workerConsumer;
  private Worker worker;
  private Coordinator coordinator;
  private long coordinatorOffset = 1L;

  /**
   * The catalog of the base has no warehouse, so its tables lie under {@code /db/...}, which is not
   * a key of the in-memory {@code FileIO} the writer writes to: an emptiness check on such a
   * location would pass whatever is staged.
   */
  @BeforeEach
  public void useInMemoryWarehouse() throws IOException {
    catalog.close();
    catalog = new InMemoryCatalog();
    catalog.initialize(
        "mem",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION, "mem://warehouse/" + UUID.randomUUID()));
    catalog.createNamespace(NAMESPACE);
  }

  @AfterEach
  public void stopParticipants() {
    if (worker != null) {
      worker.stop();
    }

    if (coordinator != null) {
      coordinator.terminate();
    }
  }

  @Test
  public void tableSwitchedOffBesideTableSwitchedOnIsWrittenAndCommittedAsBefore() {
    catalog.createTable(ON, SCHEMA);
    catalog.createTable(OFF, SCHEMA);
    IcebergSinkConfig sinkConfig =
        sinkConfig(
            "iceberg.tables.statistics.enabled",
            "true",
            "iceberg.table." + OFF + ".statistics.enabled",
            "false");
    assertThat(sinkConfig.statisticsInUse()).isTrue();

    Map<TableIdentifier, DataWritten> answers = cycle(sinkConfig);

    Table on = catalog.loadTable(ON);
    Table off = catalog.loadTable(OFF);
    DataWritten onAnswer = answers.get(ON);
    assertThat(onAnswer.sketchFile()).as("the worker's answer of the table on").isNotNull();
    assertThat(onAnswer.getSchema().getField("sketch_file")).isNotNull();
    assertThat(onAnswer.sketchFile().location())
        .contains("/" + ColumnSketchCollector.STATS_DIR + "/");

    DataWritten offAnswer = answers.get(OFF);
    assertThat(offAnswer.dataFiles()).hasSize(1);
    assertThat(offAnswer.sketchFile()).as("the worker's answer of the table off").isNull();
    assertThat(offAnswer.getSchema().getField("sketch_file"))
        .as("the schema of the released version")
        .isNull();

    Snapshot onSnapshot = on.currentSnapshot();
    assertThat(onSnapshot.summary()).containsEntry(NdvChain.STATISTICS_ENABLED_PROP, "true");
    awaitStatistics(on, onSnapshot.snapshotId(), stagingDirectory(sinkConfig, on, ON));
    assertThat(on.statisticsFiles())
        .extracting(file -> file.snapshotId())
        .contains(onSnapshot.snapshotId());

    off.refresh();
    assertThat(off.currentSnapshot().summary()).doesNotContainKey(NdvChain.STATISTICS_ENABLED_PROP);
    assertThat(off.statisticsFiles()).isEmpty();
    assertThat(off.partitionStatisticsFiles()).isEmpty();
    assertStagingEmpty(off, sinkConfig);
  }

  @Test
  public void withStatisticsSwitchedOffEverywhereNothingIsStagedAnywhere() {
    catalog.createTable(ON, SCHEMA);
    catalog.createTable(OFF, SCHEMA);
    IcebergSinkConfig sinkConfig = sinkConfig();
    assertThat(sinkConfig.statisticsInUse()).isFalse();

    Map<TableIdentifier, DataWritten> answers = cycle(sinkConfig);

    for (TableIdentifier identifier : ImmutableList.of(ON, OFF)) {
      Table loaded = catalog.loadTable(identifier);
      DataWritten answer = answers.get(identifier);
      assertThat(answer.dataFiles()).hasSize(1);
      assertThat(answer.sketchFile()).isNull();
      assertThat(answer.getSchema().getField("sketch_file")).isNull();
      assertThat(loaded.currentSnapshot().summary())
          .doesNotContainKey(NdvChain.STATISTICS_ENABLED_PROP);
      assertThat(loaded.statisticsFiles()).isEmpty();
      assertThat(loaded.partitionStatisticsFiles()).isEmpty();
      assertStagingEmpty(loaded, sinkConfig);
    }
  }

  /** Nothing under the staging location; as a control, the table location around it has files. */
  private static void assertStagingEmpty(Table loaded, IcebergSinkConfig sinkConfig) {
    String stagingLocation =
        StagedChangeFileWriter.stagingLocation(loaded, sinkConfig.stagingLocation());
    assertThat(stagingLocation).startsWith(loaded.location() + "/");
    assertSomeFilesUnder(loaded.location());
    assertNoFilesUnder(stagingLocation);
  }

  /** One cycle through the worker and the coordinator; returns the worker's answers by table. */
  private Map<TableIdentifier, DataWritten> cycle(IcebergSinkConfig sinkConfig) {
    MetadataEvents metadataEvents = mock(MetadataEvents.class);
    config = sinkConfig;
    coordinator =
        new Coordinator(
            catalog,
            sinkConfig,
            ImmutableList.of(),
            clientFactory,
            mock(SinkTaskContext.class),
            metadataEvents);
    coordinator.start();
    initConsumer();

    coordinator.process();
    byte[] commitRequestBytes = producer.history().get(producer.history().size() - 1).value();
    Event commitRequest = AvroUtil.decode(commitRequestBytes);
    assertThat(commitRequest.type()).isEqualTo(PayloadType.START_COMMIT);
    UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    List<byte[]> workerEvents = workerCycle(sinkConfig, commitRequestBytes);
    Map<TableIdentifier, DataWritten> answers = Maps.newHashMap();
    for (byte[] bytes : workerEvents) {
      Event event = AvroUtil.decode(bytes);
      if (event.type() == PayloadType.DATA_WRITTEN) {
        DataWritten dataWritten = (DataWritten) event.payload();
        assertThat(dataWritten.commitId()).isEqualTo(commitId);
        TableReference reference = dataWritten.tableReference();
        assertThat(answers.put(reference.identifier(), dataWritten)).isNull();
      }

      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, coordinatorOffset++, "key", bytes));
    }

    assertThat(answers.keySet()).containsExactlyInAnyOrder(ON, OFF);
    coordinator.process();
    return answers;
  }

  /**
   * The events a started worker answers to {@code commitRequest}, as sent, after a record for each
   * table.
   */
  private List<byte[]> workerCycle(IcebergSinkConfig sinkConfig, byte[] commitRequest) {
    workerProducer =
        new MockProducer<>(false, null, new StringSerializer(), new ByteArraySerializer());
    workerProducer.initTransactions();
    workerConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    KafkaClientFactory workerFactory = mock(KafkaClientFactory.class);
    when(workerFactory.createProducer(any())).thenReturn(workerProducer);
    when(workerFactory.createConsumer(any())).thenReturn(workerConsumer);
    when(workerFactory.createAdmin()).thenReturn(admin);

    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(mock(ConsumerGroupMetadata.class));
      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

      worker =
          new Worker(
              catalog, sinkConfig, workerFactory, new SinkWriter(catalog, sinkConfig), context);
      worker.start();
      TopicPartition controlPartition = new TopicPartition(CTL_TOPIC_NAME, 0);
      workerConsumer.rebalance(ImmutableList.of(controlPartition));
      workerConsumer.updateBeginningOffsets(ImmutableMap.of(controlPartition, 0L));

      worker.save(
          ImmutableList.of(
              new SinkRecord(
                  SRC_TOPIC_NAME,
                  0,
                  null,
                  "key",
                  null,
                  ImmutableMap.of("id", 1L, "data", "a", "date", "d"),
                  0L)));
      workerConsumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", commitRequest));
      worker.process();
    }

    List<byte[]> events = Lists.newArrayList();
    workerProducer.history().forEach(record -> events.add(record.value()));
    return events;
  }

  private IcebergSinkConfig sinkConfig(String... extra) {
    Map<String, String> props = Maps.newHashMap();
    props.put("name", "statistics-switch");
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", SRC_TOPIC_NAME);
    props.put("iceberg.tables", ON + "," + OFF);
    props.put("iceberg.control.topic", CTL_TOPIC_NAME);
    props.put("iceberg.connect.group-id", CONNECT_CONSUMER_GROUP_ID);
    props.put("iceberg.control.commit.interval-ms", "0");
    props.put("task.id", "0");
    for (int i = 0; i < extra.length; i += 2) {
      props.put(extra[i], extra[i + 1]);
    }

    return new IcebergSinkConfig(props);
  }

  private String stagingDirectory(IcebergSinkConfig sinkConfig, Table loaded, TableIdentifier id) {
    return StagedChangeFileWriter.stagingDirectory(
        StagedChangeFileWriter.stagingLocation(loaded, sinkConfig.stagingLocation()),
        TableReference.of(catalog.name(), id, loaded.uuid()),
        sinkConfig.connectGroupId());
  }
}
