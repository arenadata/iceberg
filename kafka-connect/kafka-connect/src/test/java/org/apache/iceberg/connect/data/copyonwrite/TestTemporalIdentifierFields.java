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

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Copy-on-write on a table whose identifier fields are a date and a timestamp.
 *
 * <p>Rows arrive in Iceberg's generic in-memory model, where a date is a {@code LocalDate} and a
 * timestamp an {@code OffsetDateTime}. Everything a key is subsequently handed to speaks the
 * internal representation instead: {@code Conversions} for the staged file's bounds, {@code
 * Literals} for the planner's pruning predicate, {@code SingleValueParser} for the cursor persisted
 * in the snapshot summary. All three throw on a generic value, and none of them is reached by a
 * table keyed on a {@code long} or a {@code string}, which is every other test here.
 *
 * <p>So this walks one change set through all three: stage it, normalize it, plan it, and take its
 * cursor to JSON and back.
 */
public class TestTemporalIdentifierFields {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "events");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);

  private static final Set<Integer> ID_FIELDS = ImmutableSet.of(1, 2);
  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "event_date", Types.DateType.get()),
              required(2, "seen_at", Types.TimestampType.withZone()),
              optional(3, "data", Types.StringType.get())),
          ID_FIELDS);

  private static final LocalDate DAY_ONE = LocalDate.of(2026, 3, 1);
  private static final LocalDate DAY_TWO = LocalDate.of(2026, 3, 2);
  private static final LocalDate DAY_THREE = LocalDate.of(2026, 3, 3);

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
  public void testStagingNormalizingAndPlanningATemporalKey() {
    appendRows(row(DAY_ONE, "old"), row(DAY_TWO, "old"), row(DAY_THREE, "old"));

    StagedChangeFile staged =
        writeStagedFile(
            DAY_ONE, StagedChangeSchema.OP_UPDATE, DAY_TWO, StagedChangeSchema.OP_DELETE);

    // the bounds are written through Conversions, which only accepts internal values
    assertThat(staged.lowerBounds()).containsKey(1);
    assertThat(staged.upperBounds()).containsKey(1);

    ChangeSetSlice slice =
        ChangeSetNormalizer.normalize(table, ID_FIELDS, ImmutableList.of(staged), null, 100);

    assertThat(slice.size()).isEqualTo(2);
    assertThat(slice.changes().firstKey().get(0, Object.class))
        .as("keys hold days since the epoch, not a LocalDate")
        .isEqualTo(daysOf(DAY_ONE));

    // the pruning predicate turns every key value into an expression literal
    PlanResult plan =
        AffectedFilePlanner.plan(
            table, table.currentSnapshot().snapshotId(), slice, 200, Long.MAX_VALUE);

    assertThat(plan.fileScanTasks()).isNotEmpty();
    assertThat(plan.conflictDetectionFilter()).isNotNull();
  }

  @Test
  public void testTheCursorOfATemporalKeySurvivesTheSnapshotSummary() {
    StagedChangeFile staged =
        writeStagedFile(
            DAY_ONE, StagedChangeSchema.OP_UPDATE, DAY_TWO, StagedChangeSchema.OP_UPDATE);

    ChangeSetSlice full =
        ChangeSetNormalizer.normalize(table, ID_FIELDS, ImmutableList.of(staged), null, 100);
    ChangeSetSlice firstSlice =
        ChangeSetNormalizer.normalize(table, ID_FIELDS, ImmutableList.of(staged), null, 1);

    assertThat(firstSlice.truncated()).isTrue();

    // the round trip the coordinator makes between two slice commits
    Types.StructType cursorType =
        ChangeSetManifest.freeze(
                table,
                ID_FIELDS,
                ImmutableList.of(staged),
                ImmutableSet.of("src"),
                ImmutableMap.of(),
                null)
            .cursorType(table.schema());
    String json = SingleValueParser.toJson(cursorType, firstSlice.lastKey().orElseThrow());
    StructLike restored = (StructLike) SingleValueParser.fromJson(cursorType, json);

    ChangeSetSlice secondSlice =
        ChangeSetNormalizer.normalize(table, ID_FIELDS, ImmutableList.of(staged), restored, 100);

    assertThat(secondSlice.size()).isEqualTo(1);
    assertThat(secondSlice.changes().keySet()).containsExactly(full.changes().lastKey());
  }

  private int daysOf(LocalDate date) {
    return (int) date.toEpochDay();
  }

  private StagedChangeFile writeStagedFile(LocalDate first, int firstOp, LocalDate second, int op) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table,
            TABLE_REFERENCE,
            ID_FIELDS,
            table.location() + "/_staging",
            "cg-connect",
            "task-0");
    writer.write(writer.stagedRow(row(first, "new"), firstOp, "src", 0, 0L));
    writer.write(writer.stagedRow(row(second, "new"), op, "src", 0, 1L));
    List<StagedChangeFile> files = writer.complete();
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private Record row(LocalDate day, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("event_date", day);
    record.setField("seen_at", day.atStartOfDay().atOffset(ZoneOffset.UTC));
    record.setField("data", data);
    return record;
  }

  private void appendRows(Record... rows) {
    ClusteredDataWriter<Record> writer =
        new ClusteredDataWriter<>(
            new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).build(),
            OutputFileFactory.builderFor(table, 0, 0L).operationId("test").build(),
            table.io(),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    try {
      for (Record row : rows) {
        writer.write(row, table.spec(), null);
      }
      writer.close();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    AppendFiles append = table.newAppend();
    writer.result().dataFiles().forEach(append::appendFile);
    append.commit();
    table.refresh();
  }
}
