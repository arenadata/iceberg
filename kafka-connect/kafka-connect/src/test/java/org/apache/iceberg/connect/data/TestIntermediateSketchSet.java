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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IntermediateSketchSet.Coverage;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/** Coverage of the answers by the intermediate Puffin files, unreadable files included. */
public class TestIntermediateSketchSet {
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  private static final String STAGING = "mem://bucket/kc-staging/db.tbl/group";
  private static final int LG_K = 12;
  private static final BooleanSupplier RUNNING = () -> false;

  private static final Schema WRITER =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
  // column 3 was added by another task after this writer loaded its schema
  private static final Schema WITH_NEW_COLUMN =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "extra", Types.StringType.get()));
  // an external writer widened id after the worker sketched it as int
  private static final Schema WIDENED =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private ReadFileIO io;
  private Table table;
  private int fileCount = 0;

  @BeforeEach
  public void before() {
    io = new ReadFileIO();
    table = mock(Table.class);
    when(table.io()).thenReturn(io);
    when(table.name()).thenReturn("mem." + TABLE);
  }

  @Test
  public void blobOfColumnCoversAnswer() {
    SketchFile file = writerFile(WRITER, row(1, "a"), row(2, "b"), row(3, null));
    StatisticsAnswer answer = answer(file);

    IntermediateSketchSet sketches = read(WRITER, answer);

    assertThat(sketches.stopped()).isFalse();
    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coverage(answer, 2)).isEqualTo(Coverage.COVERED);
    assertThat(estimate(sketches.sketch(1))).isEqualTo(3.0);
    assertThat(estimate(sketches.sketch(2))).isEqualTo(2.0);
    assertThat(sketches.coveredRows(1)).isEqualTo(3L);
    assertThat(sketches.coveredRows(2)).isEqualTo(3L);
  }

  @Test
  public void columnUnknownToWriterIsEmpty() {
    SketchFile file = writerFile(WRITER, row(1, "a"));
    StatisticsAnswer answer = answer(file);

    IntermediateSketchSet sketches = read(WITH_NEW_COLUMN, answer);

    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coverage(answer, 3)).isEqualTo(Coverage.EMPTY);
    assertThat(sketches.sketch(3)).isNull();
    assertThat(sketches.coveredRows(3)).isZero();
  }

  @Test
  public void blobOfAnotherTypeDoesNotCover() {
    SketchFile file = writerFile(WRITER, row(1, "a"), row(2, "b"));
    StatisticsAnswer answer = answer(file);

    IntermediateSketchSet sketches = read(WIDENED, answer);

    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(answer, 2)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.sketch(1)).isNull();
    assertThat(sketches.coveredRows(1)).isZero();
  }

  @Test
  public void knownColumnWithoutBlobDoesNotCover() {
    // a value the sketch cannot hash makes the column INVALID: the writer knew it, but has no blob
    SketchFile file = writerFile(WRITER, row(1, "a"), row(2, 42));
    StatisticsAnswer answer = answer(file);

    IntermediateSketchSet sketches = read(WRITER, answer);

    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coverage(answer, 2)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.sketch(2)).isNull();
  }

  @Test
  public void answerWithoutReferenceIsNotCovered() {
    io.refuseAll();
    StatisticsAnswer answer = StatisticsAnswer.ofDataFiles(1L, ImmutableList.of(), null);

    IntermediateSketchSet sketches = read(WITH_NEW_COLUMN, answer);

    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(answer, 2)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(answer, 3)).isEqualTo(Coverage.UNCOVERED);
  }

  @Test
  public void deletedPuffinLeavesItsAnswersUncovered() {
    SketchFile deleted = writerFile(WRITER, row(1, "a"), row(2, "b"));
    SketchFile kept = writerFile(WRITER, row(5, "e"));
    io.deleteFile(deleted.location());
    StatisticsAnswer first = answer(deleted);
    StatisticsAnswer second = answer(deleted);
    StatisticsAnswer other = answer(kept);
    List<String> logs = Lists.newArrayList();

    IntermediateSketchSet sketches =
        IntermediateSketchSet.read(
            NdvTestFiles.recordingLogger(logs),
            table,
            ImmutableList.of(first, second, other),
            WITH_NEW_COLUMN,
            WITH_NEW_COLUMN.columns(),
            LG_K,
            RUNNING);

    for (int fieldId = 1; fieldId <= 3; fieldId += 1) {
      assertThat(sketches.coverage(first, fieldId)).isEqualTo(Coverage.UNCOVERED);
      assertThat(sketches.coverage(second, fieldId)).isEqualTo(Coverage.UNCOVERED);
    }

    assertThat(sketches.coverage(other, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coverage(other, 3)).isEqualTo(Coverage.EMPTY);
    assertThat(estimate(sketches.sketch(1))).isEqualTo(1.0);
    assertThat(sketches.coveredRows(1)).isEqualTo(1L);
    assertThat(logs)
        .as("one WARN per file")
        .singleElement()
        .asString()
        .startsWith("WARN ")
        .contains(TABLE.toString(), deleted.location());
  }

  @Test
  public void puffinDeletedUnderRangeReadsLeavesItsAnswersUncovered() {
    SketchFile deleted = writerFile(WRITER, row(1, "a"), row(2, "b"));
    SketchFile kept = writerFile(WRITER, row(5, "e"));
    // a range read of S3 throws the raw NoSuchKeyException of its client, not NotFoundException
    io.deleteUnderRangeReads(
        deleted.location(), () -> new RuntimeException("The specified key does not exist"));
    StatisticsAnswer answer = answer(deleted);
    StatisticsAnswer other = answer(kept);
    List<String> logs = Lists.newArrayList();

    IntermediateSketchSet sketches =
        IntermediateSketchSet.read(
            NdvTestFiles.recordingLogger(logs),
            table,
            ImmutableList.of(answer, other),
            WRITER,
            WRITER.columns(),
            LG_K,
            RUNNING);

    assertThat(sketches.failure()).as("a deleted file is not a failure").isNull();
    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(answer, 2)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(other, 1)).isEqualTo(Coverage.COVERED);
    assertThat(estimate(sketches.sketch(1))).isEqualTo(1.0);
    assertThat(logs)
        .singleElement()
        .asString()
        .startsWith("WARN ")
        .contains(TABLE.toString(), deleted.location());
  }

  @Test
  public void failureOfExistenceCheckIsFailure() {
    SketchFile file = writerFile(WRITER, row(1, "a"));
    RuntimeException existsFailure = new RuntimeException("HEAD timed out");
    io.deleteUnderRangeReads(file.location(), () -> new RuntimeException("GET timed out"))
        .failExists(file.location(), () -> existsFailure);

    IntermediateSketchSet sketches = read(WRITER, answer(file));

    assertThat(sketches.failure()).as("a failure a retry may fix").isNotNull();
    assertThat(sketches.failure().getSuppressed()).containsExactly(existsFailure);
    assertThat(sketches.failure()).hasMessage("GET timed out");
  }

  @Test
  public void corruptPuffinLeavesItsAnswersUncovered() throws IOException {
    SketchFile corrupt = writerFile(WRITER, row(1, "a"));
    byte[] garbage = new byte[(int) corrupt.fileSizeBytes()];
    Arrays.fill(garbage, (byte) 3);
    try (OutputStream stream = io.newOutputFile(corrupt.location()).createOrOverwrite()) {
      stream.write(garbage);
    }

    SketchFile valid = writerFile(WRITER, row(1, "a"));
    SketchFile wrongSize =
        new SketchFile(valid.location(), valid.fileSizeBytes() + 10, valid.footerSizeBytes());
    StatisticsAnswer corruptAnswer = answer(corrupt);
    StatisticsAnswer wrongSizeAnswer = answer(wrongSize);

    IntermediateSketchSet sketches = read(WRITER, corruptAnswer, wrongSizeAnswer);

    assertThat(sketches.coverage(corruptAnswer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(wrongSizeAnswer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.sketch(1)).isNull();
  }

  @Test
  public void fileWithoutItsPropertiesIsUnavailable() {
    byte[] blob = sketch(1L, 2L);
    SketchFile noRowCount = manualFile(ImmutableMap.of("last-column-id", "2"), 1, "int", blob);
    SketchFile noLastColumnId = manualFile(ImmutableMap.of("row-count", "2"), 1, "int", blob);
    SketchFile notANumber =
        manualFile(ImmutableMap.of("last-column-id", "two", "row-count", "2"), 1, "int", blob);
    SketchFile complete =
        manualFile(ImmutableMap.of("last-column-id", "2", "row-count", "2"), 1, "int", blob);
    StatisticsAnswer withoutRowCount = answer(noRowCount);
    StatisticsAnswer withoutLastColumnId = answer(noLastColumnId);
    StatisticsAnswer unparsable = answer(notANumber);
    StatisticsAnswer readable = answer(complete);

    IntermediateSketchSet sketches =
        read(WRITER, withoutRowCount, withoutLastColumnId, unparsable, readable);

    assertThat(sketches.coverage(withoutRowCount, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(withoutLastColumnId, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(unparsable, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.coverage(readable, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coveredRows(1)).isEqualTo(2L);
  }

  @Test
  public void blobWithUnparsableTypeIsLikeNoBlob() {
    Map<String, String> properties = ImmutableMap.of("last-column-id", "2", "row-count", "2");
    SketchFile unknownType = manualFile(properties, 1, "no-such-type", sketch(1L, 2L));
    StatisticsAnswer answer = answer(unknownType);

    IntermediateSketchSet sketches = read(WRITER, answer);

    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.sketch(1)).isNull();
  }

  @Test
  public void answersSharingFileReadItOnce() {
    SketchFile file = writerFile(WRITER, row(1, "a"), row(2, "b"));
    StatisticsAnswer first = answer(file);
    StatisticsAnswer second = answer(file);

    IntermediateSketchSet sketches = read(WRITER, first, second);

    assertThat(io.opened(file.location())).isEqualTo(1);
    assertThat(sketches.coverage(first, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coverage(second, 1)).isEqualTo(Coverage.COVERED);
    assertThat(sketches.coveredRows(1)).as("row-count once per file").isEqualTo(2L);
  }

  @Test
  public void sketchIsUnionOfCoveringFiles() {
    SketchFile first = writerFile(WRITER, row(1, "a"), row(2, "a"), row(3, "a"));
    SketchFile second = writerFile(WRITER, row(3, "b"), row(4, "b"));

    IntermediateSketchSet sketches = read(WRITER, answer(first), answer(second));

    assertThat(estimate(sketches.sketch(1))).isEqualTo(4.0);
    assertThat(estimate(sketches.sketch(2))).isEqualTo(2.0);
    assertThat(sketches.coveredRows(1)).isEqualTo(5L);
  }

  @Test
  public void stoppedBeforeFileReadsNothing() {
    SketchFile file = writerFile(WRITER, row(1, "a"));

    IntermediateSketchSet sketches =
        IntermediateSketchSet.read(
            table, ImmutableList.of(answer(file)), WRITER, WRITER.columns(), LG_K, () -> true);

    assertThat(sketches.stopped()).isTrue();
    assertThat(io.openedTotal()).isZero();
  }

  @Test
  public void ioExceptionOfFileIsFailureNotUnavailableFile() {
    SketchFile first = writerFile(WRITER, row(1, "a"));
    SketchFile second = writerFile(WRITER, row(2, "b"));
    StatisticsAnswer answer = answer(first);
    io.failReads(first.location(), () -> new IOException("connection reset"));
    List<String> logs = Lists.newArrayList();

    IntermediateSketchSet sketches =
        IntermediateSketchSet.read(
            NdvTestFiles.recordingLogger(logs),
            table,
            ImmutableList.of(answer, answer(second)),
            WRITER,
            WRITER.columns(),
            LG_K,
            RUNNING);

    assertThat(sketches.failure()).as("a failure a retry may fix").hasMessage("connection reset");
    assertThat(sketches.stopped()).isFalse();
    assertThat(sketches.coverage(answer, 1)).isEqualTo(Coverage.UNCOVERED);
    assertThat(sketches.sketch(1)).isNull();
    assertThat(io.opened(second.location())).as("nothing is read after the failure").isZero();
    assertThat(logs).isEmpty();
  }

  @Test
  public void failureOfStorageClientIsFailureNotUnavailableFile() {
    SketchFile file = writerFile(WRITER, row(1, "a"));
    io.failReads(file.location(), () -> new IllegalStateException("Connection pool shut down"));
    SketchFile other = writerFile(WRITER, row(1, "a"));
    io.failReads(
        other.location(), () -> new UncheckedIOException(new IOException("503 Slow Down")));

    IntermediateSketchSet sketches = read(WRITER, answer(file));
    IntermediateSketchSet wrapped = read(WRITER, answer(other));

    assertThat(sketches.failure()).hasMessage("Connection pool shut down");
    assertThat(wrapped.failure()).hasRootCauseMessage("503 Slow Down");
  }

  @Test
  public void goneOrBrokenFilesAreNotFailure() {
    SketchFile gone = writerFile(WRITER, row(1, "a"));
    io.deleteFile(gone.location());
    SketchFile noProperties = manualFile(ImmutableMap.of(), 1, "int", sketch(1L));
    SketchFile unparsableBlob =
        manualFile(
            ImmutableMap.of("last-column-id", "2", "row-count", "1"), 1, "int", new byte[] {1, 2});

    IntermediateSketchSet sketches =
        read(WRITER, answer(gone), answer(noProperties), answer(unparsableBlob));

    assertThat(sketches.failure()).isNull();
    assertThat(sketches.stopped()).isFalse();
  }

  @Test
  public void failureAfterStopIsStopWithoutWarning() {
    SketchFile file = writerFile(WRITER, row(1, "a"));
    AtomicBoolean stopped = new AtomicBoolean(false);
    io.failReads(
        file.location(),
        () -> {
          stopped.set(true);
          return new IOException("closed by interrupt");
        });
    List<String> logs = Lists.newArrayList();

    IntermediateSketchSet sketches =
        IntermediateSketchSet.read(
            NdvTestFiles.recordingLogger(logs),
            table,
            ImmutableList.of(answer(file)),
            WRITER,
            WRITER.columns(),
            LG_K,
            stopped::get);

    assertThat(sketches.stopped()).isTrue();
    assertThat(logs).isEmpty();
  }

  @Test
  public void interruptedThreadStopsBeforeFile() {
    SketchFile file = writerFile(WRITER, row(1, "a"));
    ImmutableList<StatisticsAnswer> answers = ImmutableList.of(answer(file));

    Thread.currentThread().interrupt();
    try {
      IntermediateSketchSet sketches =
          IntermediateSketchSet.read(table, answers, WRITER, WRITER.columns(), LG_K, RUNNING);

      assertThat(sketches.stopped()).isTrue();
      assertThat(io.openedTotal()).isZero();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void failureOfInterruptedThreadIsStopWithoutWarning() {
    SketchFile file = writerFile(WRITER, row(1, "a"));
    io.failReads(
        file.location(),
        () -> {
          Thread.currentThread().interrupt();
          return new IOException("closed by interrupt");
        });
    List<String> logs = Lists.newArrayList();

    try {
      IntermediateSketchSet sketches =
          IntermediateSketchSet.read(
              NdvTestFiles.recordingLogger(logs),
              table,
              ImmutableList.of(answer(file)),
              WRITER,
              WRITER.columns(),
              LG_K,
              RUNNING);

      assertThat(sketches.stopped()).isTrue();
      assertThat(logs).isEmpty();
    } finally {
      Thread.interrupted();
    }
  }

  private IntermediateSketchSet read(Schema schemaS, StatisticsAnswer... answers) {
    Logger logger = NdvTestFiles.recordingLogger(Lists.newArrayList());
    return IntermediateSketchSet.read(
        logger, table, ImmutableList.copyOf(answers), schemaS, schemaS.columns(), LG_K, RUNNING);
  }

  private static double estimate(CompactSketch sketch) {
    assertThat(sketch).as("the union of the covering blobs").isNotNull();
    return sketch.getEstimate();
  }

  private static StatisticsAnswer answer(SketchFile file) {
    return StatisticsAnswer.ofDataFiles(1L, ImmutableList.of(), file);
  }

  private SketchFile writerFile(Schema schema, Record... rows) {
    ColumnSketchCollector sketches =
        new ColumnSketchCollector(TABLE, table, STAGING, "0", LG_K, null, false, schema);
    for (Record row : rows) {
      sketches.update(row);
    }

    SketchFile file = sketches.write(UUID.randomUUID());
    assertThat(file).as("the writer's sketch file").isNotNull();
    return file;
  }

  private static Record row(Object id, Object data) {
    Record record = GenericRecord.create(WRITER);
    record.set(0, id);
    record.set(1, data);
    return record;
  }

  private SketchFile manualFile(
      Map<String, String> properties, int fieldId, String type, byte[] data) {
    fileCount += 1;
    String path = STAGING + "/stats/manual-" + fileCount + "-" + UUID.randomUUID() + ".puffin";
    try (PuffinWriter writer =
        Puffin.write(io.newOutputFile(path)).createdBy("test").setAll(properties).build()) {
      writer.add(
          new Blob(
              StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
              ImmutableList.of(fieldId),
              -1L,
              -1L,
              ByteBuffer.wrap(data),
              PuffinCompressionCodec.NONE,
              ImmutableMap.of("type", type)));
      writer.finish();
      return new SketchFile(path, writer.fileSize(), writer.footerSize());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] sketch(long... values) {
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(1 << LG_K).build();
    for (long value : values) {
      sketch.update(value);
    }

    return sketch.compact().toByteArray();
  }
}
