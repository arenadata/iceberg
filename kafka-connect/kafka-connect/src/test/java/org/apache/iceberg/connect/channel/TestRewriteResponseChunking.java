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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.PermanentCopyOnWriteException;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How a task's rewrite answer is cut into control messages.
 *
 * <p>The file count is only the first cut; what has to hold is that no message outgrows the
 * producer's limit, whatever the width of the table. A data file descriptor carries per-column
 * metrics, so the same count of files is a small message on a narrow table and an oversized one on
 * a wide table, which is exactly what a count-only split got wrong.
 */
public class TestRewriteResponseChunking {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final int MAX_BYTES = 1024 * 1024;

  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private RewriteAssignmentRunner assignmentRunner;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, wideSchema(300), PartitionSpec.unpartitioned());

    config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("cg-connect");
    when(config.taskId()).thenReturn("task-0");
    when(config.copyOnWriteRewriteThreads()).thenReturn(1);
    when(config.controlMessageMaxBytes()).thenReturn(MAX_BYTES);

    assignmentRunner = new RewriteAssignmentRunner(catalog, config, event -> {});
  }

  @AfterEach
  public void after() {
    assignmentRunner.stop();
  }

  @Test
  public void testAWideTableIsSplitBySizeEvenWithinTheFileCount() {
    // 300 columns of metrics per descriptor: 200 of them already outgrow a 1 MB message, so the
    // configured count on its own would produce a message the producer rejects
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(500);

    List<List<DataFile>> chunks = split(dataFiles(200));

    assertThat(chunks.size()).isGreaterThan(1);
    assertEveryChunkFits(chunks);
    assertNothingLostOrDuplicated(chunks, 200);
  }

  @Test
  public void testANarrowTableAnswersInOneMessage() {
    table = catalog.createTable(TableIdentifier.of(NAMESPACE, "narrow"), wideSchema(5));
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(500);

    List<List<DataFile>> chunks = split(dataFiles(400));

    assertThat(chunks).hasSize(1);
    assertEveryChunkFits(chunks);
  }

  @Test
  public void testTheFileCountStillCapsTheMessage() {
    // well inside the byte budget, so only the count can be doing the splitting here
    table = catalog.createTable(TableIdentifier.of(NAMESPACE, "narrow2"), wideSchema(5));
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(50);

    List<List<DataFile>> chunks = split(dataFiles(120));

    assertThat(chunks).hasSize(3);
    assertThat(chunks.get(0)).hasSize(50);
    assertThat(chunks.get(2)).hasSize(20);
    assertNothingLostOrDuplicated(chunks, 120);
  }

  @Test
  public void testATaskThatWroteNothingStillAnswersOnce() {
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(500);

    List<List<DataFile>> chunks = split(ImmutableList.of());

    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0)).isEmpty();
  }

  @Test
  public void testASingleOversizedDescriptorIsAPermanentFailure() {
    // nothing can be split further, and the next cycle rewrites the same rows into a descriptor of
    // the same size: sending it anyway has the producer reject the response every time, forever
    when(config.copyOnWriteRewriteResponseChunkFiles()).thenReturn(500);
    when(config.controlMessageMaxBytes()).thenReturn(1024);

    assertThatThrownBy(() -> split(dataFiles(3)))
        .isInstanceOf(PermanentCopyOnWriteException.class)
        .hasMessageContaining("db.tbl")
        .hasMessageContaining("1024 byte limit")
        .hasMessageContaining(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)
        .hasMessageContaining(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
        .hasMessageContaining("write.metadata.metrics");
  }

  private List<List<DataFile>> split(List<DataFile> written) {
    return assignmentRunner.splitToFit(assignment(), table, written);
  }

  private void assertEveryChunkFits(List<List<DataFile>> chunks) {
    for (List<DataFile> chunk : chunks) {
      int encoded =
          AvroUtil.encode(assignmentRunner.event(assignment(), table, chunk, 0, 1)).length;
      assertThat(encoded).isLessThanOrEqualTo(MAX_BYTES);
    }
  }

  private void assertNothingLostOrDuplicated(List<List<DataFile>> chunks, int expected) {
    List<String> locations = Lists.newArrayList();
    chunks.forEach(chunk -> chunk.forEach(file -> locations.add(file.location())));
    assertThat(locations).hasSize(expected).doesNotHaveDuplicates();
  }

  private RewriteAssigned assignment() {
    return new RewriteAssigned(
        PartitionSpec.unpartitioned().partitionType(),
        UUID.randomUUID(),
        TABLE_REFERENCE,
        UUID.randomUUID(),
        0,
        1L,
        new StagedChangeFile(
            "s3://warehouse/db/tbl/staging/normalized-0.avro",
            10L,
            1L,
            0,
            1,
            ImmutableMap.of(),
            ImmutableMap.of()),
        ImmutableList.of(),
        0,
        1,
        ImmutableList.of(1));
  }

  private static Schema wideSchema(int columns) {
    List<NestedField> fields = Lists.newArrayList();
    fields.add(NestedField.required(1, "id", Types.LongType.get()));
    for (int i = 2; i <= columns; i++) {
      fields.add(NestedField.optional(i, "c" + i, Types.LongType.get()));
    }
    return new Schema(fields);
  }

  private List<DataFile> dataFiles(int count) {
    List<DataFile> files = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      files.add(dataFile(i));
    }
    return files;
  }

  private DataFile dataFile(int index) {
    Map<Integer, Long> sizes = Maps.newHashMap();
    Map<Integer, Long> counts = Maps.newHashMap();
    Map<Integer, Long> nulls = Maps.newHashMap();
    Map<Integer, ByteBuffer> lower = Maps.newHashMap();
    Map<Integer, ByteBuffer> upper = Maps.newHashMap();
    for (NestedField field : table.schema().columns()) {
      int id = field.fieldId();
      sizes.put(id, 1024L * id);
      counts.put(id, 5000L);
      nulls.put(id, 3L);
      lower.put(id, Conversions.toByteBuffer(Types.LongType.get(), (long) id));
      upper.put(id, Conversions.toByteBuffer(Types.LongType.get(), id * 1000L));
    }
    Metrics metrics = new Metrics(5000L, sizes, counts, nulls, Maps.newHashMap(), lower, upper);
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/0000" + index + "-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(268_435_456L)
        .withRecordCount(5000)
        .withMetrics(metrics)
        .build();
  }
}
