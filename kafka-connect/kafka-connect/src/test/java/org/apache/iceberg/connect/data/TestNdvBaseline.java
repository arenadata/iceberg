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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.NdvBaseline.Kind;
import org.apache.iceberg.connect.data.NdvBaseline.Reason;
import org.apache.iceberg.connect.data.NdvTestFiles.ReadFileIO;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Initial sketches from the base of the NDV and how a failure to read the base is classified. */
public class TestNdvBaseline {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
  private static final int LG_K = 12;
  private static final BooleanSupplier RUNNING = () -> false;

  private InMemoryCatalog catalog;
  private Table table;
  private ReadFileIO io;
  private long baseSnapshotId;
  private int fileCount = 0;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned());
    io = new ReadFileIO();
    baseSnapshotId = append();
  }

  @AfterEach
  public void after() throws Exception {
    catalog.close();
  }

  @Test
  public void blobsOfBaseAreInitialSketches() {
    StatisticsFile base =
        writeBase(
            ImmutableMap.of(1, sketch(1, 2, 3), 2, sketch(10, 20, 30, 40, 50)),
            PuffinCompressionCodec.ZSTD); // as Spark writes them
    append();

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches().keySet()).containsExactly(1, 2);
    assertThat(sketches.sketches().get(1).getEstimate()).isEqualTo(3.0);
    assertThat(sketches.sketches().get(2).getEstimate()).isEqualTo(5.0);
    assertThat(sketches.undeserializable()).isEmpty();
    assertThat(io.opened(base.path())).isEqualTo(1);
  }

  @Test
  public void emptyBaseStartsEveryColumnEmpty() {
    io.refuseAll();

    NdvBaseline sketches = load(null, table.schema());

    assertThat(NdvBaseline.columnsWithoutBaseline(null, null, SCHEMA, SCHEMA.columns())).isEmpty();
    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches().keySet()).containsExactly(1, 2);
    assertThat(sketches.sketches().values()).allSatisfy(sketch -> assertEmpty(sketch));
  }

  @Test
  public void newColumnStartsEmpty() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2), 2, sketch(7)));
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    append();
    Schema schemaS = table.schema();

    NdvBaseline sketches = load(base, schemaS);

    assertThat(NdvBaseline.columnsWithoutBaseline(base, SCHEMA, schemaS, schemaS.columns()))
        .isEmpty();
    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches().keySet()).containsExactly(1, 2, 3);
    assertThat(sketches.sketches().get(1).getEstimate()).isEqualTo(2.0);
    assertEmpty(sketches.sketches().get(3));
  }

  @Test
  public void columnWhoseTypeChangedHasNoBaseline() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2), 2, sketch(7)));
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    append();
    Schema schemaS = table.schema();

    Map<Integer, Reason> withoutBaseline =
        NdvBaseline.columnsWithoutBaseline(base, SCHEMA, schemaS, schemaS.columns());
    NdvBaseline sketches = load(base, schemaS);

    assertThat(withoutBaseline).containsExactly(Map.entry(1, Reason.TYPE_CHANGED));
    assertThat(withoutBaseline.get(1)).hasToString("type changed");
    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches().keySet()).containsExactly(2);
  }

  @Test
  public void partialSparkBaseLeavesOtherColumnsWithoutBlob() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2, 3)));
    append();

    Map<Integer, Reason> withoutBaseline =
        NdvBaseline.columnsWithoutBaseline(base, SCHEMA, SCHEMA, SCHEMA.columns());
    NdvBaseline sketches = load(base, table.schema());

    assertThat(withoutBaseline).containsExactly(Map.entry(2, Reason.NO_BLOB));
    assertThat(withoutBaseline.get(2)).hasToString("no blob");
    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches().keySet()).containsExactly(1);
    assertThat(sketches.sketches().get(1).getEstimate()).isEqualTo(3.0);
  }

  @Test
  public void baseWithoutBlobsOfSetIsNotRead() {
    StatisticsFile base = writeBase(ImmutableMap.of());
    append();
    io.refuseAll();

    NdvBaseline sketches = load(base, table.schema());

    assertThat(NdvBaseline.columnsWithoutBaseline(base, SCHEMA, SCHEMA, SCHEMA.columns()))
        .containsExactly(Map.entry(1, Reason.NO_BLOB), Map.entry(2, Reason.NO_BLOB));
    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches()).isEmpty();
  }

  @Test
  public void blobThatDoesNotDeserializeHasNoBlobAndItsOwnWarning() {
    UpdateSketch otherSeedSketch = UpdateSketch.builder().setSeed(42L).build();
    otherSeedSketch.update(1L);
    byte[] otherSeed = otherSeedSketch.compact().toByteArray();
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2), 2, otherSeed));
    append();
    List<String> logs = Lists.newArrayList();

    NdvBaseline sketches =
        NdvBaseline.load(
            NdvTestFiles.recordingLogger(logs),
            reading(),
            base,
            SCHEMA,
            table.schema(),
            table.schema().columns(),
            RUNNING);

    assertThat(NdvBaseline.columnsWithoutBaseline(base, SCHEMA, SCHEMA, SCHEMA.columns()))
        .isEmpty();
    assertThat(sketches.kind()).isEqualTo(Kind.LOADED);
    assertThat(sketches.sketches().keySet()).containsExactly(1);
    assertThat(sketches.undeserializable()).containsExactly(2);
    assertThat(logs)
        .singleElement()
        .asString()
        .startsWith("WARN ")
        .contains(TABLE.toString(), "data", base.path());
  }

  @Test
  public void ioExceptionReadingFileIsError() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    IOException failure = new IOException("connection reset");
    io.failReads(base.path(), () -> failure);

    NdvBaseline failed = load(base, table.schema());
    ReadFileIO healthy = new ReadFileIO();
    this.io = healthy;
    NdvBaseline retried = load(base, table.schema());

    assertThat(failed.kind()).isEqualTo(Kind.FAILED);
    assertThat(failed.failure()).isSameAs(failure);
    assertThat(retried.kind()).isEqualTo(Kind.LOADED);
    assertThat(retried.sketches().get(1).getEstimate()).isEqualTo(2.0);
  }

  @Test
  public void storageClientErrorsAreErrors() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    RuntimeException throttled = new RuntimeException("503 Slow Down");
    io.failReads(base.path(), () -> throttled);

    NdvBaseline client = load(base, table.schema());
    this.io = new ReadFileIO();
    UncheckedIOException unchecked = new UncheckedIOException(new IOException("timeout"));
    io.failReads(base.path(), () -> unchecked);
    NdvBaseline wrapped = load(base, table.schema());

    assertThat(client.kind()).isEqualTo(Kind.FAILED);
    assertThat(client.failure()).isSameAs(throttled);
    assertThat(wrapped.kind()).isEqualTo(Kind.FAILED);
    assertThat(wrapped.failure()).isSameAs(unchecked);
  }

  @Test
  public void stateErrorOfStorageClientIsErrorNotBrokenFile() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    IllegalStateException poolShutDown = new IllegalStateException("Connection pool shut down");
    io.failReads(base.path(), () -> poolShutDown);

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.FAILED);
    assertThat(sketches.failure()).isSameAs(poolShutDown);

    this.io = new ReadFileIO();
    IllegalArgumentException badRequest = new IllegalArgumentException("bad request");
    io.failReads(base.path(), () -> badRequest);

    assertThat(load(base, table.schema()).kind()).isEqualTo(Kind.FAILED);
  }

  @Test
  public void deletedFileWithRecordInPlaceHasNoBaseline() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    io.deleteFile(base.path());

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
  }

  @Test
  public void deletedFileWithRecordRemovedIsError() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    Table stale = reading();
    // a parallel step of the same table drops the record by N and deletes the file
    table.updateStatistics().removeStatistics(baseSnapshotId).commit();
    io.deleteFile(base.path());

    NdvBaseline sketches = NdvBaseline.load(stale, base, SCHEMA, SCHEMA, SCHEMA.columns(), RUNNING);

    assertThat(sketches.kind()).isEqualTo(Kind.FAILED);
    assertThat(stale.statisticsFiles()).as("the table was refreshed").isEmpty();
  }

  @Test
  public void fileDeletedUnderRangeReadsWithRecordInPlaceHasNoBaseline() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    // a range read of S3 throws the raw NoSuchKeyException of its client, not NotFoundException
    io.deleteUnderRangeReads(
        base.path(), () -> new RuntimeException("The specified key does not exist"));

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
  }

  @Test
  public void fileDeletedUnderRangeReadsWithRecordRemovedIsError() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    Table stale = reading();
    table.updateStatistics().removeStatistics(baseSnapshotId).commit();
    io.deleteUnderRangeReads(
        base.path(), () -> new RuntimeException("The specified key does not exist"));

    NdvBaseline sketches = NdvBaseline.load(stale, base, SCHEMA, SCHEMA, SCHEMA.columns(), RUNNING);

    assertThat(stale.statisticsFiles()).as("the table was refreshed").isEmpty();
    assertThat(sketches.kind()).isEqualTo(Kind.FAILED);
  }

  @Test
  public void failureOfExistenceCheckIsError() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    RuntimeException readFailure = new RuntimeException("GET timed out");
    RuntimeException existsFailure = new RuntimeException("HEAD timed out");
    io.deleteUnderRangeReads(base.path(), () -> readFailure)
        .failExists(base.path(), () -> existsFailure);

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.failure()).isSameAs(readFailure);
    assertThat(readFailure.getSuppressed()).containsExactly(existsFailure);
    assertThat(sketches.kind()).isEqualTo(Kind.FAILED);
  }

  @Test
  public void fileWithoutMagicHasNoBaseline() throws IOException {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    byte[] garbage = new byte[(int) base.fileSizeInBytes()];
    Arrays.fill(garbage, (byte) 7);
    overwrite(base.path(), garbage);

    assertThat(load(base, table.schema()).kind()).isEqualTo(Kind.NO_BASELINE);
  }

  @Test
  public void fileShorterThanRecordHasNoBaseline() {
    StatisticsFile written = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    StatisticsFile longer =
        new GenericStatisticsFile(
            written.snapshotId(),
            written.path(),
            written.fileSizeInBytes() + 1,
            written.fileFooterSizeInBytes(),
            written.blobMetadata());

    NdvBaseline sketches = load(longer, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(sketches.failure()).isInstanceOf(EOFException.class);
  }

  @Test
  public void invalidFooterSizeHasNoBaseline() {
    StatisticsFile written = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    StatisticsFile wrongFooter =
        new GenericStatisticsFile(
            written.snapshotId(),
            written.path(),
            written.fileSizeInBytes(),
            written.fileSizeInBytes() + 100,
            written.blobMetadata());

    NdvBaseline sketches = load(wrongFooter, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(sketches.failure()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void footerThatDoesNotParseHasNoBaseline() throws IOException {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    byte[] bytes = read(base.path());
    int json = indexOf(bytes, "{\"blobs\"".getBytes(StandardCharsets.UTF_8));
    assertThat(json).as("the footer JSON").isPositive();
    bytes[json] = '!';
    overwrite(base.path(), bytes);

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
  }

  @Test
  public void footerWithoutRequiredFieldHasNoBaseline() throws IOException {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    byte[] bytes = read(base.path());
    byte[] field = "\"fields\"".getBytes(StandardCharsets.UTF_8);
    int json = indexOf(bytes, field);
    assertThat(json).as("the field of the blob").isPositive();
    // valid JSON, the key is renamed (same length): the parser of the footer finds no "fields"
    bytes[json + 1] = 'g';
    overwrite(base.path(), bytes);

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(sketches.failure()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void footerWithFieldOfWrongTypeHasNoBaseline() throws IOException {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    byte[] bytes = read(base.path());
    byte[] field = "\"fields\":[1]".getBytes(StandardCharsets.UTF_8);
    int json = indexOf(bytes, field);
    assertThat(json).as("the fields of the blob").isPositive();
    // valid JSON, [1] becomes "1" (same length): the array of ids is a string
    bytes[json + 9] = '"';
    bytes[json + 11] = '"';
    overwrite(base.path(), bytes);

    NdvBaseline sketches = load(base, table.schema());

    assertThat(sketches.kind()).isEqualTo(Kind.NO_BASELINE);
    assertThat(sketches.failure()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void stoppedBeforeOpeningFile() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    io.refuseAll();

    NdvBaseline sketches =
        NdvBaseline.load(reading(), base, SCHEMA, SCHEMA, SCHEMA.columns(), () -> true);

    assertThat(sketches.kind()).isEqualTo(Kind.STOPPED);
  }

  @Test
  public void failureAfterStopIsStop() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    AtomicBoolean stopped = new AtomicBoolean(false);
    io.failReads(
        base.path(),
        () -> {
          stopped.set(true);
          return new IOException("closed by interrupt");
        });

    NdvBaseline sketches =
        NdvBaseline.load(reading(), base, SCHEMA, SCHEMA, SCHEMA.columns(), stopped::get);

    assertThat(sketches.kind()).isEqualTo(Kind.STOPPED);
  }

  @Test
  public void interruptedThreadStopsBeforeOpeningFile() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    io.refuseAll();
    Table reading = reading();

    Thread.currentThread().interrupt();
    try {
      NdvBaseline sketches =
          NdvBaseline.load(reading, base, SCHEMA, SCHEMA, SCHEMA.columns(), RUNNING);

      assertThat(sketches.kind()).isEqualTo(Kind.STOPPED);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void failureOfInterruptedThreadIsStop() {
    StatisticsFile base = writeBase(ImmutableMap.of(1, sketch(1, 2)));
    append();
    io.failReads(
        base.path(),
        () -> {
          Thread.currentThread().interrupt();
          return new IOException("closed by interrupt");
        });
    Table reading = reading();

    try {
      NdvBaseline sketches =
          NdvBaseline.load(reading, base, SCHEMA, SCHEMA, SCHEMA.columns(), RUNNING);

      assertThat(sketches.kind()).isEqualTo(Kind.STOPPED);
    } finally {
      Thread.interrupted();
    }
  }

  private NdvBaseline load(StatisticsFile base, Schema schemaS) {
    return NdvBaseline.load(reading(), base, SCHEMA, schemaS, schemaS.columns(), RUNNING);
  }

  private Table reading() {
    Table reading = spy(catalog.loadTable(TABLE));
    doReturn(io).when(reading).io();
    return reading;
  }

  private StatisticsFile writeBase(Map<Integer, byte[]> blobs) {
    return writeBase(blobs, PuffinCompressionCodec.NONE);
  }

  private StatisticsFile writeBase(Map<Integer, byte[]> blobs, PuffinCompressionCodec codec) {
    String path = table.location() + "/metadata/stats-" + baseSnapshotId + ".puffin";
    long sequenceNumber = table.snapshot(baseSnapshotId).sequenceNumber();
    try (PuffinWriter writer =
        Puffin.write(io.newOutputFile(path)).createdBy("Trino version 479").build()) {
      blobs.forEach(
          (fieldId, data) ->
              writer.add(
                  new Blob(
                      StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                      ImmutableList.of(fieldId),
                      baseSnapshotId,
                      sequenceNumber,
                      ByteBuffer.wrap(data),
                      codec,
                      ImmutableMap.of("ndv", "1"))));
      writer.finish();
      StatisticsFile file =
          new GenericStatisticsFile(
              baseSnapshotId,
              path,
              writer.fileSize(),
              writer.footerSize(),
              GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
      table.updateStatistics().setStatistics(file).commit();
      return file;
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

  private static void assertEmpty(Sketch sketch) {
    assertThat(sketch).isNotNull();
    assertThat(sketch.isEmpty()).isTrue();
    assertThat(sketch.getEstimate()).isEqualTo(0.0);
  }

  private long append() {
    fileCount += 1;
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("mem://data/file-" + fileCount + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100L)
                .withRecordCount(10L)
                .build())
        .commit();
    return table.currentSnapshot().snapshotId();
  }

  private byte[] read(String path) throws IOException {
    InputFile file = io.newInputFile(path);
    byte[] bytes = new byte[(int) file.getLength()];
    try (SeekableInputStream stream = file.newStream()) {
      ByteStreams.readFully(stream, bytes);
    }

    return bytes;
  }

  private void overwrite(String path, byte[] bytes) throws IOException {
    try (OutputStream stream = io.newOutputFile(path).createOrOverwrite()) {
      stream.write(bytes);
    }
  }

  private static int indexOf(byte[] bytes, byte[] part) {
    for (int start = 0; start <= bytes.length - part.length; start += 1) {
      if (Arrays.equals(Arrays.copyOfRange(bytes, start, start + part.length), part)) {
        return start;
      }
    }

    return -1;
  }
}
