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
package org.apache.iceberg.connect.data.copyonwrite;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The rewrite over a table whose columns are more than {@code (long, string)}.
 *
 * <p>Every other test in this package uses a two-column schema, and that is how a projection
 * sharing one nested view across a whole block of emitted rows went unnoticed. Both phases of the
 * rewrite are exercised here over the types a real CDC table carries: the untouched row travels the
 * carry-over path, the changed and inserted ones the emit path, and every value has to come back
 * exactly.
 */
public class TestWideSchemaRewrite {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "wide");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);

  // a composite key, so the ordering and the pruning predicate see more than one column
  private static final Set<Integer> ID_FIELDS = ImmutableSet.of(1, 2);

  private static final Types.StructType NESTED =
      Types.StructType.of(
          optional(11, "label", Types.StringType.get()),
          optional(12, "score", Types.IntegerType.get()));

  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()),
              required(2, "region", Types.StringType.get()),
              optional(3, "amount", Types.DecimalType.of(9, 2)),
              optional(4, "day", Types.DateType.get()),
              optional(5, "seen_at", Types.TimestampType.withZone()),
              optional(6, "blob", Types.BinaryType.get()),
              optional(7, "tags", Types.ListType.ofOptional(8, Types.StringType.get())),
              optional(
                  9,
                  "props",
                  Types.MapType.ofOptional(13, 14, Types.StringType.get(), Types.StringType.get())),
              optional(10, "meta", NESTED)),
          ID_FIELDS);

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testEveryColumnTypeSurvivesBothPhasesOfTheRewrite() {
    append(
        row(1L, "eu", "1.25", "2026-01-31", "one", 1),
        row(2L, "us", "2.50", "2026-02-01", "two", 2));
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table,
            TABLE_REFERENCE,
            ID_FIELDS,
            table.location() + "/_staging",
            "cg-connect",
            "task-0");
    // key 1 is updated in every column, key 3 is new: both go through the emit phase; key 2 is
    // untouched and has to survive the carry-over phase byte for byte
    writer.write(
        writer.stagedRow(
            row(1L, "eu", "9.99", "2026-03-15", "one-updated", 42),
            StagedChangeSchema.OP_UPDATE,
            "t",
            0,
            0L));
    writer.write(
        writer.stagedRow(
            row(3L, "ap", "3.75", "2026-04-01", "three", 3),
            StagedChangeSchema.OP_INSERT,
            "t",
            0,
            1L));
    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(table, ID_FIELDS, writer.complete(), null, 1000);

    List<DataFile> written =
        new CopyOnWriteRewriter(table, 1).rewrite(baseSnapshotId, planFiles(), slice);

    assertThat(readBack(written))
        .containsExactlyInAnyOrder(
            described(1L, "eu", "9.99", "2026-03-15", "one-updated", 42),
            described(2L, "us", "2.50", "2026-02-01", "two", 2),
            described(3L, "ap", "3.75", "2026-04-01", "three", 3));
  }

  /** A row's every value flattened to strings, so one mismatch names the column it happened in. */
  private Map<String, String> described(
      long id, String region, String amount, String day, String label, int score) {
    return ImmutableMap.<String, String>builder()
        .put("id", Long.toString(id))
        .put("region", region)
        .put("amount", amount)
        .put("day", day)
        .put("seen_at", "2026-05-06T07:08:09Z")
        .put("blob", label)
        .put("tags", "[" + label + "]")
        .put("props", "{k=" + label + "}")
        .put("meta", label + "/" + score)
        .build();
  }

  private Record row(long id, String region, String amount, String day, String label, int score) {
    GenericRecord meta = GenericRecord.create(NESTED);
    meta.setField("label", label);
    meta.setField("score", score);

    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("region", region);
    record.setField("amount", new BigDecimal(amount));
    record.setField("day", LocalDate.parse(day));
    record.setField("seen_at", OffsetDateTime.parse("2026-05-06T07:08:09Z"));
    record.setField("blob", ByteBuffer.wrap(label.getBytes(StandardCharsets.UTF_8)));
    record.setField("tags", ImmutableList.of(label));
    record.setField("props", ImmutableMap.of("k", label));
    record.setField("meta", meta);
    return record;
  }

  private List<Map<String, String>> readBack(List<DataFile> files) {
    List<Map<String, String>> rows = Lists.newArrayList();
    for (DataFile file : files) {
      FileScanTask task = mock(FileScanTask.class);
      when(task.file()).thenReturn(file);
      when(task.deletes()).thenReturn(ImmutableList.of());
      when(task.spec()).thenReturn(table.spec());
      when(task.start()).thenReturn(0L);
      when(task.length()).thenReturn(file.fileSizeInBytes());
      try (CloseableIterable<Record> records =
          AffectedFileReader.open(table.io(), task, table.schema(), table.schema())) {
        for (Record record : records) {
          rows.add(describe(record));
        }
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
    return rows;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> describe(Record record) {
    ByteBuffer blob = ((ByteBuffer) record.get(5, Object.class)).duplicate();
    byte[] bytes = new byte[blob.remaining()];
    blob.get(bytes);
    Record meta = (Record) record.get(8, Object.class);
    return ImmutableMap.<String, String>builder()
        .put("id", record.get(0, Object.class).toString())
        .put("region", record.get(1, Object.class).toString())
        .put("amount", record.get(2, Object.class).toString())
        .put("day", record.get(3, Object.class).toString())
        .put("seen_at", record.get(4, Object.class).toString())
        .put("blob", new String(bytes, StandardCharsets.UTF_8))
        .put("tags", ((List<String>) record.get(6, Object.class)).toString())
        .put("props", ((Map<String, String>) record.get(7, Object.class)).toString())
        .put("meta", meta.get(0, Object.class) + "/" + meta.get(1, Object.class))
        .build();
  }

  private List<FileScanTask> planFiles() {
    List<FileScanTask> plan = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      planned.forEach(plan::add);
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return plan;
  }

  private void append(Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).build(),
            OutputFileFactory.builderFor(table, 0, 0L)
                .operationId(UUID.randomUUID().toString())
                .build(),
            table.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        PartitionKey key = new PartitionKey(table.spec(), table.schema());
        key.partition(row);
        writer.write(row, table.spec(), key);
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
    AppendFiles append = table.newAppend();
    writer.result().dataFiles().forEach(append::appendFile);
    append.commit();
  }
}
