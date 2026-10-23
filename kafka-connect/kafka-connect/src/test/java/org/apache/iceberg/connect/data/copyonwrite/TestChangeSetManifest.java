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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestChangeSetManifest {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = Set.of(1);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

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
  public void testWriteReadRoundTrip() {
    List<StagedChangeFile> stagedFiles = writeOneStagedFile();

    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table,
            ID_FIELDS,
            stagedFiles,
            Set.of("topic-a", "topic-b"),
            Map.of(0, 100L, 1, 200L),
            OffsetDateTime.parse("2026-09-15T01:02:03.456Z"));

    String location =
        ChangeSetManifest.location(
            table.location() + "/_staging", TABLE_REFERENCE, "cg-connect", manifest.changeSetId());
    manifest.write(table.io(), location);

    ChangeSetManifest read = ChangeSetManifest.read(table.io(), location);

    assertThat(read.formatVersion()).isEqualTo(ChangeSetManifest.CURRENT_FORMAT_VERSION);
    assertThat(read.changeSetId()).isEqualTo(manifest.changeSetId());
    assertThat(read.tableUuid()).isEqualTo(table.uuid());
    assertThat(read.createdAtMs()).isEqualTo(manifest.createdAtMs());
    assertThat(read.schemaIdAtFreeze()).isEqualTo(table.schema().schemaId());
    assertThat(read.identifierFieldIds()).isEqualTo(ID_FIELDS);
    assertThat(read.sourceTopics()).containsExactlyInAnyOrder("topic-a", "topic-b");
    assertThat(read.controlOffsets()).isEqualTo(Map.of(0, 100L, 1, 200L));
    assertThat(read.validThroughTs()).isAtSameInstantAs(manifest.validThroughTs());

    assertThat(read.stagedFiles()).hasSize(1);
    StagedChangeFile original = stagedFiles.get(0);
    StagedChangeFile roundTripped = read.stagedFiles().get(0);
    assertThat(roundTripped.location()).isEqualTo(original.location());
    assertThat(roundTripped.fileSizeBytes()).isEqualTo(original.fileSizeBytes());
    assertThat(roundTripped.recordCount()).isEqualTo(original.recordCount());
    assertThat(roundTripped.schemaId()).isEqualTo(original.schemaId());
    assertThat(roundTripped.formatVersion()).isEqualTo(original.formatVersion());
    assertThat(roundTripped.lowerBounds()).isEqualTo(original.lowerBounds());
    assertThat(roundTripped.upperBounds()).isEqualTo(original.upperBounds());
    assertThat(roundTripped.identifierFieldIds())
        .isNotEmpty()
        .isEqualTo(original.identifierFieldIds());
  }

  /**
   * A change set frozen in a partial cycle has no {@code validThroughTs} to state, and the field is
   * left out of the file rather than written as null: a manifest written before the field existed
   * reads back the same way, as a change set whose commits state no timestamp.
   */
  @Test
  public void testAManifestFrozenWithoutAValidThroughTsReadsBackWithoutOne() {
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(
            table, ID_FIELDS, writeOneStagedFile(), Set.of("topic-a"), Map.of(0, 100L), null);
    String location =
        ChangeSetManifest.location(
            table.location() + "/_staging", TABLE_REFERENCE, "cg-connect", manifest.changeSetId());
    manifest.write(table.io(), location);

    assertThat(ChangeSetManifest.read(table.io(), location).validThroughTs()).isNull();
  }

  @Test
  public void testIdentifierFieldsMatch() {
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(table, ID_FIELDS, List.of(), Set.of(), Map.of(), null);

    assertThat(manifest.identifierFieldsMatch(ID_FIELDS)).isTrue();
    assertThat(manifest.identifierFieldsMatch(ImmutableSet.of(2))).isFalse();
  }

  @Test
  public void testLocationIncludesTableAndChangeSetId() {
    java.util.UUID changeSetId = java.util.UUID.randomUUID();
    String location =
        ChangeSetManifest.location(
            "s3://bucket/staging", TABLE_REFERENCE, "cg-connect", changeSetId);

    assertThat(location)
        .isEqualTo(
            "s3://bucket/staging/"
                + TABLE_IDENTIFIER
                + "/cg-connect/_changesets/"
                + changeSetId
                + ".json");
  }

  @Test
  public void testFormatVersionSupported() {
    ChangeSetManifest manifest =
        ChangeSetManifest.freeze(table, ID_FIELDS, List.of(), Set.of(), Map.of(), null);

    assertThat(manifest.formatVersion()).isEqualTo(ChangeSetManifest.CURRENT_FORMAT_VERSION);
    assertThat(manifest.formatVersionSupported()).isTrue();
  }

  /**
   * One pair of keys, {@code first < second}, for every type copy-on-write accepts as an identifier
   * field, in the generic representation rows arrive in. The bytes start above {@code 0x7f}, so an
   * order that read them as signed would put {@code second} first.
   *
   * <p>Geometry and geography pass the identifier field checks too, but no key of theirs reaches a
   * change set: {@code RecordConverter} converts no value of either type, in either mode.
   */
  private static Stream<Arguments> keyTypes() {
    return Stream.of(
        Arguments.of(Types.BooleanType.get(), false, true),
        Arguments.of(Types.IntegerType.get(), 1, 2),
        Arguments.of(Types.LongType.get(), 1L, 2L),
        Arguments.of(Types.DateType.get(), LocalDate.of(2026, 3, 1), LocalDate.of(2026, 3, 2)),
        Arguments.of(Types.TimeType.get(), LocalTime.of(10, 0), LocalTime.of(10, 0, 0, 1_000)),
        Arguments.of(
            Types.TimestampType.withoutZone(),
            LocalDateTime.of(2026, 3, 1, 10, 0),
            LocalDateTime.of(2026, 3, 1, 10, 0, 0, 1_000)),
        Arguments.of(
            Types.TimestampType.withZone(),
            OffsetDateTime.of(2026, 3, 1, 10, 0, 0, 0, ZoneOffset.UTC),
            OffsetDateTime.of(2026, 3, 1, 10, 0, 0, 1_000, ZoneOffset.UTC)),
        Arguments.of(Types.StringType.get(), "a", "b"),
        Arguments.of(
            Types.UUIDType.get(),
            UUID.fromString("00000000-0000-0000-0000-000000000001"),
            UUID.fromString("00000000-0000-0001-0000-000000000000")),
        Arguments.of(
            Types.FixedType.ofLength(2), new byte[] {0x01, 0x02}, new byte[] {(byte) 0xf0, 0x00}),
        Arguments.of(
            Types.BinaryType.get(),
            ByteBuffer.wrap(new byte[] {0x01}),
            ByteBuffer.wrap(new byte[] {(byte) 0xf0})),
        Arguments.of(Types.DecimalType.of(9, 2), new BigDecimal("1.50"), new BigDecimal("12.25")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("keyTypes")
  public void testTheCursorOfEveryKeyTypeSurvivesTheSnapshotSummary(
      Type.PrimitiveType type, Object first, Object second) {
    Schema schema =
        new Schema(
            ImmutableList.of(required(1, "key", type), optional(2, "data", Types.StringType.get())),
            ID_FIELDS);
    Table keyed =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "keyed"), schema, PartitionSpec.unpartitioned());

    // one file wholly at the cursor, skipped by its bounds; one across it, read row by row
    List<StagedChangeFile> stagedFiles =
        ImmutableList.<StagedChangeFile>builder()
            .addAll(stage(keyed, ID_FIELDS, keyedRow(schema, first)))
            .addAll(stage(keyed, ID_FIELDS, keyedRow(schema, first), keyedRow(schema, second)))
            .build();

    assertCursorSurvivesTheSnapshotSummary(keyed, ID_FIELDS, stagedFiles);
  }

  @Test
  public void testTheCursorOfACompositeKeyOfEveryTypeSurvivesTheSnapshotSummary() {
    // not the leading columns, and the two keys differ only in the last field: every other field of
    // the cursor has to compare equal to the key of the row it was taken from
    List<Arguments> keyTypes = keyTypes().collect(Collectors.toList());
    List<Types.NestedField> columns = Lists.newArrayList();
    columns.add(optional(1, "data", Types.StringType.get()));
    Set<Integer> keyFieldIds = Sets.newHashSet();
    for (int i = 0; i < keyTypes.size(); i++) {
      columns.add(required(i + 2, "key_" + i, (Type) keyTypes.get(i).get()[0]));
      keyFieldIds.add(i + 2);
    }
    Schema schema = new Schema(columns, keyFieldIds);
    Table keyed =
        catalog.createTable(
            TableIdentifier.of(NAMESPACE, "keyed"), schema, PartitionSpec.unpartitioned());

    GenericRecord lower = GenericRecord.create(schema);
    GenericRecord upper = GenericRecord.create(schema);
    for (int i = 0; i < keyTypes.size(); i++) {
      Object[] pair = keyTypes.get(i).get();
      lower.setField("key_" + i, pair[1]);
      upper.setField("key_" + i, i == keyTypes.size() - 1 ? pair[2] : pair[1]);
    }

    List<StagedChangeFile> stagedFiles =
        ImmutableList.<StagedChangeFile>builder()
            .addAll(stage(keyed, keyFieldIds, lower))
            .addAll(stage(keyed, keyFieldIds, lower, upper))
            .build();

    assertCursorSurvivesTheSnapshotSummary(keyed, keyFieldIds, stagedFiles);
  }

  /**
   * The round trip a cursor makes between two slices of a change set applied by different
   * coordinators: the first slice's last key is written against the frozen manifest, and read back
   * against the manifest a new coordinator reads from storage. The second slice has to start right
   * after it: skipping the file wholly behind it by its bounds, and the key itself in the file that
   * goes past it.
   */
  private void assertCursorSurvivesTheSnapshotSummary(
      Table keyed, Set<Integer> keyFieldIds, List<StagedChangeFile> stagedFiles) {
    ChangeSetSlice whole =
        ChangeSetNormalizer.normalize(keyed, keyFieldIds, stagedFiles, null, 100);
    ChangeSetSlice firstSlice =
        ChangeSetNormalizer.normalize(keyed, keyFieldIds, stagedFiles, null, 1);
    assertThat(whole.size()).isEqualTo(2);
    assertThat(firstSlice.truncated()).isTrue();
    StructLike lastApplied = firstSlice.lastKey().orElseThrow();

    ChangeSetManifest frozen =
        ChangeSetManifest.freeze(keyed, keyFieldIds, stagedFiles, Set.of("src"), Map.of(), null);
    String location =
        ChangeSetManifest.location(
            keyed.location() + "/_staging", TABLE_REFERENCE, "cg-connect", frozen.changeSetId());
    frozen.write(keyed.io(), location);
    String json = SingleValueParser.toJson(frozen.cursorType(keyed.schema()), lastApplied);

    ChangeSetManifest read = ChangeSetManifest.read(keyed.io(), location);
    StructLike cursor =
        (StructLike) SingleValueParser.fromJson(read.cursorType(keyed.schema()), json);

    assertThat(
            IdentifierKeys.comparator(read.identifierFields(keyed.schema()))
                .compare(cursor, lastApplied))
        .as("the cursor read back is the key it was written from")
        .isZero();

    ChangeSetSlice secondSlice =
        ChangeSetNormalizer.normalize(keyed, keyFieldIds, stagedFiles, cursor, 100);
    assertThat(secondSlice.truncated()).isFalse();
    assertThat(secondSlice.changes().keySet()).containsExactly(whole.changes().lastKey());
  }

  private static Record keyedRow(Schema schema, Object key) {
    GenericRecord row = GenericRecord.create(schema);
    row.setField("key", key);
    row.setField("data", "changed");
    return row;
  }

  private List<StagedChangeFile> stage(Table target, Set<Integer> keyFieldIds, Record... rows) {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            target,
            TABLE_REFERENCE,
            keyFieldIds,
            target.location() + "/_staging",
            "cg-connect",
            "task-0");
    long offset = 0;
    for (Record row : rows) {
      writer.write(writer.stagedRow(row, StagedChangeSchema.OP_UPDATE, "src", 0, offset++));
    }
    return writer.complete();
  }

  private List<StagedChangeFile> writeOneStagedFile() {
    StagedChangeFileWriter writer =
        new StagedChangeFileWriter(
            table,
            TABLE_REFERENCE,
            ID_FIELDS,
            table.location() + "/_staging",
            "cg-connect",
            "task-0");
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", "a");
    writer.write(writer.stagedRow(row, StagedChangeSchema.OP_INSERT, "t", 0, 0L));
    return writer.complete();
  }
}
