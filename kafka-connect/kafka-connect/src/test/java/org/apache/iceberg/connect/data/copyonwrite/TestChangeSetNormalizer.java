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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestChangeSetNormalizer {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final java.util.Set<Integer> ID_FIELDS = java.util.Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final TableReference COMPOSITE_TABLE_REFERENCE =
      TableReference.of("catalog", TableIdentifier.of(NAMESPACE, "composite"));
  private static final java.util.Set<Integer> COMPOSITE_ID_FIELDS = java.util.Set.of(1, 2);
  private static final Schema COMPOSITE_SCHEMA =
      new Schema(
          required(1, "region", Types.StringType.get()),
          required(2, "id", Types.LongType.get()),
          optional(3, "data", Types.StringType.get()));

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
  public void testInsertThenUpdateKeepsFinalRow() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 1L, "b", StagedChangeSchema.OP_UPDATE, "t", 0, 1L));
            });

    ChangeSetSlice slice = normalize(ImmutableList.of(file), null, 100);

    assertThat(slice.size()).isEqualTo(1);
    ChangeRecord record = onlyRecord(slice);
    assertThat(record.state()).isEqualTo(ChangeRecord.State.PRESENT);
    assertThat(record.row().getField("data")).isEqualTo("b");
  }

  @Test
  public void testInsertThenDeleteIsInsertedThenDeleted() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 1L, null, StagedChangeSchema.OP_DELETE, "t", 0, 1L));
            });

    ChangeSetSlice slice = normalize(ImmutableList.of(file), null, 100);

    assertThat(onlyRecord(slice).state()).isEqualTo(ChangeRecord.State.INSERTED_THEN_DELETED);
  }

  @Test
  public void testUpdateThenDeleteIsAbsent() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_UPDATE, "t", 0, 0L));
              writer.write(row(writer, 1L, null, StagedChangeSchema.OP_DELETE, "t", 0, 1L));
            });

    ChangeSetSlice slice = normalize(ImmutableList.of(file), null, 100);

    assertThat(onlyRecord(slice).state()).isEqualTo(ChangeRecord.State.ABSENT);
  }

  @Test
  public void testDeleteThenInsertReplacesWithFinalRow() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, null, StagedChangeSchema.OP_DELETE, "t", 0, 0L));
              writer.write(row(writer, 1L, "new", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
            });

    ChangeSetSlice slice = normalize(ImmutableList.of(file), null, 100);

    ChangeRecord record = onlyRecord(slice);
    assertThat(record.state()).isEqualTo(ChangeRecord.State.PRESENT);
    assertThat(record.row().getField("data")).isEqualTo("new");
  }

  @Test
  public void testUpdateThenUpdateKeepsLastVersionRegardlessOfFileOrder() {
    // two workers writing the same key: the earlier op (by offset) lands in the file visited
    // second, proving the fold does not depend on file iteration order
    StagedChangeFile fileWithLaterOp =
        writeOneFile(
            "task-0",
            writer ->
                writer.write(row(writer, 1L, "v2", StagedChangeSchema.OP_UPDATE, "t", 0, 5L)));
    StagedChangeFile fileWithEarlierOp =
        writeOneFile(
            "task-1",
            writer ->
                writer.write(row(writer, 1L, "v1", StagedChangeSchema.OP_UPDATE, "t", 0, 2L)));

    ChangeSetSlice slice =
        normalize(ImmutableList.of(fileWithLaterOp, fileWithEarlierOp), null, 100);

    ChangeRecord record = onlyRecord(slice);
    assertThat(record.state()).isEqualTo(ChangeRecord.State.PRESENT);
    assertThat(record.row().getField("data")).isEqualTo("v2");
  }

  @Test
  public void testCursorSkipsAlreadyAppliedKeys() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 2L, "b", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
              writer.write(row(writer, 3L, "c", StagedChangeSchema.OP_INSERT, "t", 0, 2L));
            });

    StructLike cursor = key(1L);
    ChangeSetSlice slice = normalize(ImmutableList.of(file), cursor, 100);

    assertThat(slice.size()).isEqualTo(2);
    assertThat(slice.changes().keySet()).containsExactly(key(2L), key(3L));
  }

  @Test
  public void testRecordQuotaKeepsSmallestKeysAndMarksTruncated() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 3L, "c", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
              writer.write(row(writer, 2L, "b", StagedChangeSchema.OP_INSERT, "t", 0, 2L));
            });

    ChangeSetSlice slice = normalize(ImmutableList.of(file), null, 2);

    assertThat(slice.truncated()).isTrue();
    assertThat(slice.changes().keySet()).containsExactly(key(1L), key(2L));
  }

  @Test
  public void testAHeadOfASliceIsWhatASmallerRecordQuotaNormalizesTo() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 3L, "c", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
              writer.write(row(writer, 4L, "d", StagedChangeSchema.OP_INSERT, "t", 0, 2L));
              writer.write(row(writer, 2L, "b", StagedChangeSchema.OP_INSERT, "t", 0, 3L));
            });
    ChangeSetSlice slice = normalize(ImmutableList.of(file), null, 10);

    for (int k = 1; k < 4; k++) {
      ChangeSetSlice head = slice.head(k);
      ChangeSetSlice normalized = normalize(ImmutableList.of(file), null, k);
      assertThat(head.changes().keySet())
          .containsExactlyElementsOf(normalized.changes().keySet())
          .containsExactlyElementsOf(ImmutableList.of(key(1L), key(2L), key(3L)).subList(0, k));
      // truncated, or the commit of a cut slice would end a drain with keys still to apply
      assertThat(head.truncated()).isTrue();
    }

    assertThat(slice.head(4)).isSameAs(slice);
    assertThat(slice.head(4).truncated()).isFalse();
    assertThat(normalize(ImmutableList.of(file), null, 3).head(3).truncated()).isTrue();
    assertThatThrownBy(() -> slice.head(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("A prefix holds at least one key: 0");
  }

  @Test
  public void testAnIdentifierFieldMissingFromTheSchemaIsRejected() {
    // silently skipping it would build a key one column short, which compares equal for rows that
    // differ: distinct rows merged into one, invisible until the data is wrong
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_UPDATE, "t", 0, 0L)));

    assertThatThrownBy(
            () ->
                ChangeSetNormalizer.normalize(
                    table, java.util.Set.of(1, 999), ImmutableList.of(file), null, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("999");
  }

  @Test
  public void testFilesWrittenUnderTwoSchemasNormalizeAsOneChangeSet() {
    // schema evolution mid-cycle ends the staged file in progress, so one change set can hold files
    // written under two schemas. Avro resolution is by field id, so the older file reads back with
    // the column it never had as null rather than failing or shifting every value
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table,
            TABLE_REFERENCE,
            ID_FIELDS,
            table.location() + "/_staging",
            "cg-connect",
            "task-0");
    writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_UPDATE, "t", 0, 0L));

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    writer.refreshSchema();

    Record evolved = GenericRecord.create(table.schema());
    evolved.setField("id", 2L);
    evolved.setField("data", "b");
    evolved.setField("extra", "x");
    writer.write(writer.stagedRow(evolved, StagedChangeSchema.OP_UPDATE, "t", 0, 1L));

    List<StagedChangeFile> files = writer.complete();
    assertThat(files).as("one file per schema").hasSize(2);

    ChangeSetSlice slice = normalize(files, null, 100);

    assertThat(slice.size()).isEqualTo(2);
    List<Object> extras = Lists.newArrayList();
    slice.changes().values().forEach(change -> extras.add(change.row().getField("extra")));
    assertThat(extras).containsExactly(null, "x");
  }

  @Test
  public void testANonPositiveRecordQuotaIsRejected() {
    // a zero quota admits no key, so every slice is empty and truncated, and the committer's
    // "not drained, start the next slice" path would spin between startSlice and finishSlice
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_UPDATE, "t", 0, 0L)));
    List<StagedChangeFile> files = ImmutableList.of(file);

    assertThatThrownBy(() -> ChangeSetNormalizer.normalize(table, ID_FIELDS, files, null, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Record quota must be positive");
  }

  @Test
  public void testAStagedFileEntirelyBehindTheCursorIsNotOpened() {
    StagedChangeFile behind =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 2L, "b", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
            });
    StagedChangeFile ahead =
        writeOneFile(
            "task-1",
            writer -> {
              writer.write(row(writer, 3L, "c", StagedChangeSchema.OP_INSERT, "t", 0, 2L));
              writer.write(row(writer, 4L, "d", StagedChangeSchema.OP_INSERT, "t", 0, 3L));
            });

    // deleted, so the only way through is the bounds the writer recorded: a change set drained
    // over many slices re-reads every staged file for every slice without this
    table.io().deleteFile(behind.location());

    ChangeSetSlice slice = normalize(ImmutableList.of(behind, ahead), key(2L), 100);

    assertThat(slice.changes().keySet()).containsExactly(key(3L), key(4L));
  }

  @Test
  public void testAStagedFileEntirelyPastAFullSliceIsNotOpened() {
    StagedChangeFile low =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 2L, "b", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
            });
    StagedChangeFile high =
        writeOneFile(
            "task-1",
            writer -> {
              writer.write(row(writer, 8L, "h", StagedChangeSchema.OP_INSERT, "t", 0, 2L));
              writer.write(row(writer, 9L, "i", StagedChangeSchema.OP_INSERT, "t", 0, 3L));
            });

    table.io().deleteFile(high.location());

    ChangeSetSlice slice = normalize(ImmutableList.of(low, high), null, 2);

    assertThat(slice.truncated()).isTrue();
    assertThat(slice.changes().keySet()).containsExactly(key(1L), key(2L));
  }

  @Test
  public void testACompositeKeyFileWhoseUpperLeadingBoundEqualsTheCursorIsStillOpened() {
    // with a composite key an upper bound equal to the cursor's leading value says nothing about
    // the
    // rest of the key: ("eu", 7) sorts after the cursor ("eu", 5), and skipping its file would end
    // the drain with that update never applied
    Table composite =
        catalog.createTable(
            COMPOSITE_TABLE_REFERENCE.identifier(),
            COMPOSITE_SCHEMA,
            PartitionSpec.unpartitioned());
    StagedChangeFile first =
        writeOneFile(
            composite,
            COMPOSITE_TABLE_REFERENCE,
            COMPOSITE_ID_FIELDS,
            "task-0",
            writer -> writer.write(compositeUpdate(writer, "eu", 5L, 0L)));
    StagedChangeFile second =
        writeOneFile(
            composite,
            COMPOSITE_TABLE_REFERENCE,
            COMPOSITE_ID_FIELDS,
            "task-1",
            writer -> writer.write(compositeUpdate(writer, "eu", 7L, 1L)));
    List<StagedChangeFile> files = ImmutableList.of(first, second);

    ChangeSetSlice firstSlice =
        ChangeSetNormalizer.normalize(composite, COMPOSITE_ID_FIELDS, files, null, 1);

    assertThat(firstSlice.truncated()).isTrue();
    assertThat(onlyRecord(firstSlice).row().getField("id")).isEqualTo(5L);

    ChangeSetSlice secondSlice =
        ChangeSetNormalizer.normalize(
            composite, COMPOSITE_ID_FIELDS, files, firstSlice.lastKey().orElseThrow(), 1);

    assertThat(secondSlice.size())
        .as("the key sharing the cursor's leading value is still in the change set")
        .isEqualTo(1);
    assertThat(onlyRecord(secondSlice).row().getField("data")).isEqualTo("v7");
    assertThat(secondSlice.truncated()).isFalse();
  }

  @Test
  public void testAFileWhoseLowerBoundEqualsTheLastKeyOfAFullSliceIsStillOpened() {
    // the second file holds a later record of the slice's own last key: skipping it would close the
    // slice with key 2 still present, and the next slice starts past key 2: the delete never
    // lands
    StagedChangeFile low =
        writeOneFile(
            "task-0",
            writer -> {
              writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L));
              writer.write(row(writer, 2L, "b", StagedChangeSchema.OP_INSERT, "t", 0, 1L));
            });
    StagedChangeFile high =
        writeOneFile(
            "task-1",
            writer -> {
              writer.write(row(writer, 2L, null, StagedChangeSchema.OP_DELETE, "t", 0, 2L));
              writer.write(row(writer, 3L, "c", StagedChangeSchema.OP_INSERT, "t", 0, 3L));
            });
    List<StagedChangeFile> files = ImmutableList.of(low, high);

    ChangeSetSlice firstSlice = normalize(files, null, 2);

    assertThat(firstSlice.changes().get(key(2L)).state())
        .as("key 2 folds the delete from the second file")
        .isEqualTo(ChangeRecord.State.INSERTED_THEN_DELETED);
    assertThat(firstSlice.changes().keySet()).containsExactly(key(1L), key(2L));
    assertThat(firstSlice.truncated()).isTrue();

    ChangeSetSlice secondSlice = normalize(files, firstSlice.lastKey().orElseThrow(), 2);

    assertThat(secondSlice.changes().keySet()).containsExactly(key(3L));
    assertThat(secondSlice.truncated()).isFalse();
  }

  @Test
  public void testRecordsOfOneKeyFromTwoPartitionsFoldByPartitionBeforeOffset() {
    // (_topic, _partition, _offset) orders across partitions, not the offset alone: offset 3 of
    // partition 1 is later than offset 7 of partition 0. The later record is in the file read
    // first,
    // so read order would get it wrong too
    StagedChangeFile deleteFile =
        writeOneFile(
            "task-0",
            writer ->
                writer.write(row(writer, 1L, null, StagedChangeSchema.OP_DELETE, "orders", 1, 3L)));
    StagedChangeFile updateFile =
        writeOneFile(
            "task-1",
            writer ->
                writer.write(row(writer, 1L, "v1", StagedChangeSchema.OP_UPDATE, "orders", 0, 7L)));

    ChangeSetSlice slice = normalize(ImmutableList.of(deleteFile, updateFile), null, 100);

    assertThat(onlyRecord(slice).state())
        .as("the delete from partition 1 is the key's last record")
        .isEqualTo(ChangeRecord.State.ABSENT);
  }

  @Test
  public void testRecordsOfOneKeyFromTwoTopicsFoldByTopicBeforePartition() {
    // the topic decides before the partition: ("a", 1) is earlier than ("b", 0)
    StagedChangeFile deleteFile =
        writeOneFile(
            "task-0",
            writer ->
                writer.write(row(writer, 1L, null, StagedChangeSchema.OP_DELETE, "b", 0, 0L)));
    StagedChangeFile updateFile =
        writeOneFile(
            "task-1",
            writer ->
                writer.write(row(writer, 1L, "v1", StagedChangeSchema.OP_UPDATE, "a", 1, 0L)));

    ChangeSetSlice slice = normalize(ImmutableList.of(deleteFile, updateFile), null, 100);

    assertThat(onlyRecord(slice).state())
        .as("the delete from topic b is the key's last record")
        .isEqualTo(ChangeRecord.State.ABSENT);
  }

  private ChangeRecord onlyRecord(ChangeSetSlice slice) {
    assertThat(slice.size()).isEqualTo(1);
    return slice.changes().firstEntry().getValue();
  }

  private StructLike key(long id) {
    return IdentifierKeys.internalKey(id);
  }

  @Test
  public void testAStagedFileThatIsGoneIsToldApartFromAStorageThatCannotServeIt() {
    StagedChangeFile file =
        writeOneFile(
            "task-0",
            writer -> writer.write(row(writer, 1L, "a", StagedChangeSchema.OP_INSERT, "t", 0, 0L)));
    List<StagedChangeFile> files = ImmutableList.of(file);

    // storage failing to serve the file may pass on its own: it reaches the caller as it is, and
    // the drain retries it on the next cycle
    assertThatThrownBy(
            () ->
                ChangeSetNormalizer.normalize(
                    openingWith(
                        file, new UncheckedIOException(new IOException("connection reset"))),
                    ID_FIELDS,
                    files,
                    null,
                    100))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("connection reset");

    // the same wrapper around a FileNotFoundException is the file being gone, which no later cycle
    // undoes: the caller has to be able to tell the two apart
    assertThatThrownBy(
            () ->
                ChangeSetNormalizer.normalize(
                    openingWith(
                        file, new UncheckedIOException(new FileNotFoundException(file.location()))),
                    ID_FIELDS,
                    files,
                    null,
                    100))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(file.location());

    // and the plain spelling, from storage that answers a missing file itself
    table.io().deleteFile(file.location());
    assertThatThrownBy(() -> normalize(files, null, 100))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(file.location());
  }

  /** The table, except that opening {@code file} fails with {@code failure}. */
  private Table openingWith(StagedChangeFile file, RuntimeException failure) {
    Table unreachable = spy(table);
    FileIO io = mock(FileIO.class);
    doThrow(failure).when(io).newInputFile(file.location());
    doReturn(io).when(unreachable).io();
    return unreachable;
  }

  private ChangeSetSlice normalize(
      List<StagedChangeFile> files, StructLike cursor, long maxRecords) {
    return ChangeSetNormalizer.normalize(table, ID_FIELDS, files, cursor, maxRecords);
  }

  private Record row(
      StagedChangeFileWriter writer,
      long id,
      String data,
      int op,
      String topic,
      int partition,
      long offset) {
    Record tableRow = GenericRecord.create(SCHEMA);
    tableRow.setField("id", id);
    tableRow.setField("data", data);
    return writer.stagedRow(tableRow, op, topic, partition, offset);
  }

  private Record compositeUpdate(
      StagedChangeFileWriter writer, String region, long id, long offset) {
    Record tableRow = GenericRecord.create(COMPOSITE_SCHEMA);
    tableRow.setField("region", region);
    tableRow.setField("id", id);
    tableRow.setField("data", "v" + id);
    return writer.stagedRow(tableRow, StagedChangeSchema.OP_UPDATE, "t", 0, offset);
  }

  private StagedChangeFile writeOneFile(
      String taskId, java.util.function.Consumer<StagedChangeFileWriter> writes) {
    return writeOneFile(table, TABLE_REFERENCE, ID_FIELDS, taskId, writes);
  }

  private StagedChangeFile writeOneFile(
      Table target,
      TableReference reference,
      java.util.Set<Integer> idFields,
      String taskId,
      java.util.function.Consumer<StagedChangeFileWriter> writes) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            target, reference, idFields, target.location() + "/_staging", "cg-connect", taskId);
    writes.accept(writer);
    List<StagedChangeFile> files = writer.complete();
    assertThat(files).hasSize(1);
    return files.get(0);
  }
}
