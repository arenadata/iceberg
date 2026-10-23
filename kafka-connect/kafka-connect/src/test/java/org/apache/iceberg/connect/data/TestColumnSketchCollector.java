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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestColumnSketchCollector {
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  private static final String STAGING = "mem://bucket/kc-staging/db.tbl/group";
  private static final UUID COMMIT_ID = UUID.randomUUID();
  private static final int LG_K = 12;
  private static final String UUID_PATTERN =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private InMemoryFileIO fileIO;
  private Table table;

  @BeforeEach
  public void before() {
    fileIO = new InMemoryFileIO();
    table = mock(Table.class);
    when(table.io()).thenReturn(fileIO);
  }

  @Test
  public void writesBlobPerColumnWithFileProperties() throws IOException {
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.update(row(SCHEMA, 2, "b"));
    sketches.update(row(SCHEMA, 2, null));

    SketchFile file = sketches.write(COMMIT_ID);

    assertThat(file).isNotNull();
    assertThat(ColumnSketchCollector.STATS_DIR).isEqualTo("stats");
    assertThat(file.location())
        .matches(quote(STAGING + "/stats/" + COMMIT_ID + "-7-") + UUID_PATTERN + quote(".puffin"));
    assertThat(file.fileSizeBytes()).isEqualTo(fileIO.newInputFile(file.location()).getLength());

    PuffinFile puffin = read(file);
    assertThat(puffin.properties)
        .containsEntry("last-column-id", "2")
        .containsEntry("row-count", "3")
        .containsEntry("created-by", "iceberg-kafka-connect " + IcebergBuild.version());
    assertThat(puffin.blobs).hasSize(2);
    assertBlob(puffin.blobs.get(0), 1, "int");
    assertBlob(puffin.blobs.get(1), 2, "string");
    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K, 1, 2, 2));
    assertThat(puffin.data.get(2)).isEqualTo(expected(Types.StringType.get(), LG_K, "a", "b"));
    assertThat(estimate(puffin.data.get(1))).isEqualTo(2.0);

    assertThat(new String(bytes(file.location()), StandardCharsets.UTF_8))
        .contains("\"last-column-id\"");
  }

  @Test
  public void fileNameWithoutTaskIdAndUniquePerWrite() {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(TABLE, table, STAGING, null, LG_K, null, false, SCHEMA);
    sketches.update(row(SCHEMA, 1, "a"));

    SketchFile first = sketches.write(COMMIT_ID);
    SketchFile second = sketches.write(COMMIT_ID);

    assertThat(first).isNotNull();
    assertThat(second).isNotNull();
    assertThat(first.location())
        .matches(quote(STAGING + "/stats/" + COMMIT_ID + "-") + UUID_PATTERN + quote(".puffin"));
    assertThat(second.location()).isNotEqualTo(first.location());
  }

  @Test
  public void columnWithoutValuesIsEmptyBlob() throws IOException {
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, null));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1, 2);
    assertThat(puffin.data.get(2)).isEqualTo(expected(Types.StringType.get(), LG_K));
    assertThat(estimate(puffin.data.get(2))).isEqualTo(0.0);
    assertThat(puffin.blobs.get(1).properties()).containsEntry("type", "string");
  }

  @Test
  public void noRowsGiveEmptyBlobs() throws IOException {
    PuffinFile puffin = read(sketches(SCHEMA, null).write(COMMIT_ID));

    assertThat(puffin.properties).containsEntry("row-count", "0");
    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1, 2);
    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K));
  }

  @Test
  public void sketchIsBuiltWithConfiguredLgK() throws IOException {
    int lgK = 5;
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(TABLE, table, STAGING, "7", lgK, null, false, SCHEMA);
    Object[] ids = new Object[300];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
      sketches.update(row(SCHEMA, i, "x"));
    }

    byte[] blob = read(sketches.write(COMMIT_ID)).data.get(1);

    assertThat(blob).isEqualTo(expected(Types.IntegerType.get(), lgK, ids));
    assertThat(blob).isNotEqualTo(expected(Types.IntegerType.get(), LG_K, ids));
    assertThat(CompactSketch.heapify(Memory.wrap(blob)).isEstimationMode()).isTrue();
  }

  @Test
  public void columnOfSameTypeStaysValidAtItsNewPosition() throws IOException {
    Schema reordered =
        new Schema(
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.schemaChanged(reordered);
    sketches.update(row(reordered, "b", 2));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K, 1, 2));
    assertThat(puffin.data.get(2)).isEqualTo(expected(Types.StringType.get(), LG_K, "a", "b"));
    assertThat(puffin.properties).containsEntry("last-column-id", "2");
  }

  @Test
  public void columnOfAnotherTypeIsInvalid() throws IOException {
    Schema widened =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.schemaChanged(widened);
    sketches.update(row(widened, 2L, "b"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(2);
    assertThat(puffin.properties).containsEntry("last-column-id", "2");
  }

  @Test
  public void columnThatLeavesSetButStaysInSchemaIsInvalid() throws IOException {
    Schema renamed =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, ImmutableList.of("id", "data"));
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.schemaChanged(renamed);
    sketches.update(row(renamed, 2, "b"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1);
    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K, 1, 2));
  }

  @Test
  public void columnDroppedFromSchemaStaysValid() throws IOException {
    Schema dropped = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.schemaChanged(dropped);
    sketches.update(row(dropped, 2));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1, 2);
    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K, 1, 2));
    assertThat(puffin.data.get(2)).isEqualTo(expected(Types.StringType.get(), LG_K, "a"));
    assertThat(puffin.properties)
        .containsEntry("last-column-id", "2")
        .containsEntry("row-count", "2");
  }

  @Test
  public void newColumnGetsEmptySketch() throws IOException {
    Schema added =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()),
            Types.NestedField.optional(4, "unused", Types.LongType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.schemaChanged(added);
    sketches.update(row(added, 2, "b", "x", null));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1, 2, 3, 4);
    assertThat(puffin.data.get(3)).isEqualTo(expected(Types.StringType.get(), LG_K, "x"));
    assertThat(puffin.data.get(4)).isEqualTo(expected(Types.LongType.get(), LG_K));
    assertThat(puffin.properties).containsEntry("last-column-id", "4");
  }

  @Test
  public void knownColumnThatEntersSetIsInvalid() throws IOException {
    Schema renamed =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, ImmutableList.of("id", "payload"));
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.schemaChanged(renamed);
    sketches.update(row(renamed, 2, "b"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1);
  }

  @Test
  public void columnKnownToEarlierSchemaIsInvalid() throws IOException {
    // id 3 is not in the set of the first schema, the second one lacks it (highest id 2), and the
    // third one brings it into the set: rows of the first schema had values of id 3
    Schema first =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));
    Schema third =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "payload", Types.StringType.get()));
    ColumnSketchCollector sketches = sketches(first, ImmutableList.of("id", "payload"));
    sketches.update(row(first, 1, "a", "x"));
    sketches.schemaChanged(SCHEMA);
    sketches.update(row(SCHEMA, 2, "b"));
    sketches.schemaChanged(third);
    sketches.update(row(third, 3, "c", "y"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1);
    assertThat(puffin.properties).containsEntry("last-column-id", "3");
  }

  @Test
  public void columnPushedOutOfDefaultSetByNewFirstColumnIsInvalid() throws IOException {
    int max = StatisticsColumns.DEFAULT_MAX_COLUMNS;
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int id = 1; id <= max; id++) {
      fields.add(Types.NestedField.optional(id, "c" + id, Types.IntegerType.get()));
    }

    Schema first = new Schema(fields);
    List<Types.NestedField> withNew = Lists.newArrayList();
    withNew.add(Types.NestedField.optional(max + 1, "first", Types.IntegerType.get()));
    withNew.addAll(fields);
    Schema second = new Schema(withNew);

    Object[] firstValues = new Object[max];
    Arrays.fill(firstValues, 1);
    Object[] secondValues = new Object[max + 1];
    Arrays.fill(secondValues, 2);
    ColumnSketchCollector sketches = sketches(first, null);
    sketches.update(row(first, firstValues));
    sketches.schemaChanged(second);
    secondValues[0] = null;
    sketches.update(row(second, secondValues));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    // c100 left the default set but stays in the schema: rows of the first schema had its values
    assertThat(puffin.blobs).hasSize(max);
    assertThat(puffin.data).doesNotContainKey(max).containsKey(max + 1).containsKey(1);
    assertThat(puffin.data.get(max + 1)).isEqualTo(expected(Types.IntegerType.get(), LG_K));
    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K, 1, 2));
    assertThat(puffin.properties).containsEntry("last-column-id", String.valueOf(max + 1));
  }

  @Test
  public void invalidColumnStaysInvalid() throws IOException {
    Schema widened =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    Schema added =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, 5));
    sketches.schemaChanged(widened);
    sketches.update(row(widened, 2L, "b"));
    sketches.schemaChanged(added);
    sketches.update(row(added, 3L, "c", "x"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    // id: int -> long, then the same long; data: a value that could not be converted
    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(3);
    assertThat(puffin.properties).containsEntry("last-column-id", "3");
  }

  @Test
  public void lastColumnIdIsMaximumOverSchemas() throws IOException {
    Schema first =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));
    Schema added =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(4, "other", Types.StringType.get()));

    ColumnSketchCollector dropped = sketches(first, null);
    dropped.schemaChanged(SCHEMA);
    assertThat(read(dropped.write(COMMIT_ID)).properties).containsEntry("last-column-id", "3");

    ColumnSketchCollector droppedThenAdded = sketches(first, null);
    droppedThenAdded.schemaChanged(SCHEMA);
    droppedThenAdded.schemaChanged(added);
    PuffinFile puffin = read(droppedThenAdded.write(COMMIT_ID));
    assertThat(puffin.properties).containsEntry("last-column-id", "4");
    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1, 2, 4, 3);
  }

  @Test
  public void valueThatCannotBeConvertedInvalidatesColumnForCycle() throws IOException {
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));
    sketches.update(row(SCHEMA, 2, 5));
    sketches.update(row(SCHEMA, 3, "c"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));

    assertThat(puffin.blobs).extracting(this::fieldId).containsExactly(1);
    assertThat(puffin.data.get(1)).isEqualTo(expected(Types.IntegerType.get(), LG_K, 1, 2, 3));
    assertThat(puffin.properties).containsEntry("row-count", "3");
  }

  @Test
  public void recordThatThrowsInvalidatesColumns() throws IOException {
    Record broken = mock(Record.class);
    when(broken.get(anyInt())).thenThrow(new IllegalStateException("broken"));
    when(broken.get(anyInt(), any())).thenThrow(new IllegalStateException("broken"));
    ColumnSketchCollector sketches = sketches(SCHEMA, null);

    assertThatCode(() -> sketches.update(broken)).doesNotThrowAnyException();
    sketches.update(row(SCHEMA, 2, "b"));

    PuffinFile puffin = read(sketches.write(COMMIT_ID));
    assertThat(puffin.blobs).isEmpty();
    assertThat(puffin.properties)
        .containsEntry("last-column-id", "2")
        .containsEntry("row-count", "2");
  }

  @Test
  public void failuresOfValuesAndOfFileDoNotEscape() {
    FileIO failing = mock(FileIO.class);
    when(failing.newOutputFile(anyString())).thenThrow(new UncheckedIOException(new IOException()));
    when(table.io()).thenReturn(failing);
    ColumnSketchCollector sketches = sketches(SCHEMA, null);

    assertThatCode(
            () -> {
              sketches.update(row(SCHEMA, "not an int", 1));
              sketches.update(row(SCHEMA, "still not an int", 2));
            })
        .doesNotThrowAnyException();
    assertThat(sketches.write(COMMIT_ID)).isNull();
    verify(failing).newOutputFile(anyString());
  }

  @Test
  public void failedWriteDeletesFileWithoutGuarantee() {
    FileIO failing = mock(FileIO.class);
    OutputFile out = mock(OutputFile.class);
    when(failing.newOutputFile(anyString())).thenReturn(out);
    when(out.create()).thenReturn(new FailingStream());
    doThrow(new UncheckedIOException(new IOException())).when(failing).deleteFile(anyString());
    when(table.io()).thenReturn(failing);
    ColumnSketchCollector sketches = sketches(SCHEMA, null);
    sketches.update(row(SCHEMA, 1, "a"));

    assertThat(sketches.write(COMMIT_ID)).isNull();

    ArgumentCaptor<String> path = ArgumentCaptor.forClass(String.class);
    verify(failing).newOutputFile(path.capture());
    verify(failing).deleteFile(path.getValue());
    assertThat(path.getValue()).startsWith(STAGING + "/stats/" + COMMIT_ID + "-7-");
  }

  private ColumnSketchCollector sketches(Schema schema, List<String> explicit) {
    return new ColumnSketchCollector(TABLE, table, STAGING, "7", LG_K, explicit, false, schema);
  }

  private static Record row(Schema schema, Object... values) {
    GenericRecord record = GenericRecord.create(schema);
    for (int i = 0; i < values.length; i++) {
      record.set(i, values[i]);
    }

    return record;
  }

  private static byte[] expected(Type.PrimitiveType type, int lgK, Object... values) {
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(1 << lgK).build();
    for (Object value : values) {
      sketch.update(ByteBuffers.toByteArray(Conversions.toByteBuffer(type, value)));
    }

    return sketch.compact().toByteArray();
  }

  private static double estimate(byte[] blob) {
    return CompactSketch.heapify(Memory.wrap(blob)).getEstimate();
  }

  private static String quote(String text) {
    return Pattern.quote(text);
  }

  private int fieldId(BlobMetadata blob) {
    assertThat(blob.inputFields()).hasSize(1);
    return blob.inputFields().get(0);
  }

  private static void assertBlob(BlobMetadata blob, int fieldId, String type) {
    assertThat(blob.type()).isEqualTo(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);
    assertThat(blob.inputFields()).containsExactly(fieldId);
    assertThat(blob.snapshotId()).isEqualTo(-1L);
    assertThat(blob.sequenceNumber()).isEqualTo(-1L);
    assertThat(blob.compressionCodec()).isNull();
    assertThat(blob.properties()).isEqualTo(ImmutableMap.of("type", type));
  }

  private byte[] bytes(String location) throws IOException {
    try (InputStream in = fileIO.newInputFile(location).newStream()) {
      return in.readAllBytes();
    }
  }

  private PuffinFile read(SketchFile file) throws IOException {
    assertThat(file).isNotNull();
    try (PuffinReader reader =
        Puffin.read(fileIO.newInputFile(file.location()))
            .withFileSize(file.fileSizeBytes())
            .withFooterSize(file.footerSizeBytes())
            .build()) {
      PuffinFile puffin = new PuffinFile();
      puffin.properties = reader.fileMetadata().properties();
      puffin.blobs = reader.fileMetadata().blobs();
      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(puffin.blobs)) {
        puffin.data.put(blob.first().inputFields().get(0), ByteBuffers.toByteArray(blob.second()));
      }

      return puffin;
    }
  }

  private static class PuffinFile {
    private Map<String, String> properties;
    private List<BlobMetadata> blobs;
    private final Map<Integer, byte[]> data = Maps.newHashMap();
  }

  private static class FailingStream extends PositionOutputStream {
    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public void write(int value) throws IOException {
      throw new IOException("disk is full");
    }
  }
}
