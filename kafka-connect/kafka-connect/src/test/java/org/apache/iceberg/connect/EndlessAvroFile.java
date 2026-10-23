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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * A data file that does not end, for the tests of a stop during the read of a recount: an Avro file
 * of a few rows written for real, whose blocks the storage serves again and again after the header
 * once the file is {@link #arm armed}. Nothing but a stop ends the read; after a give-up time the
 * file ends anyway, at the end of its blocks, so that a test that fails leaves no thread reading.
 *
 * <p>The storage of a test serves it through {@link #serve}; until armed, the file is what was
 * written, so that whatever reads it before the step (a rewrite of copy-on-write) reads it whole.
 */
public final class EndlessAvroFile {
  /** The size in the record: above any limit of a test, so that no split reader stops at it. */
  public static final long DECLARED_SIZE = 1L << 40;

  private static final long GIVE_UP_NANOS = TimeUnit.MINUTES.toNanos(1);

  private final String location;
  private final DataFile dataFile;
  private final byte[] header;
  private final byte[] blocks;
  private final int rowsPerRound;
  // the rounds of the blocks served so far, over all streams
  private final AtomicLong rounds = new AtomicLong();
  private final List<Map.Entry<Long, Runnable>> atRows = new CopyOnWriteArrayList<>();
  private volatile boolean armed = false;
  private volatile boolean interruptible = false;
  private volatile Thread reader = null;

  private EndlessAvroFile(String location, DataFile dataFile, byte[] bytes, int rowsPerRound) {
    this.location = location;
    this.dataFile = dataFile;
    this.rowsPerRound = rowsPerRound;
    // every block ends with the sync marker of the header, which ends the file, too
    byte[] sync = Arrays.copyOfRange(bytes, bytes.length - 16, bytes.length);
    int headerEnd = indexOf(bytes, sync) + sync.length;
    assertThat(headerEnd).as("the header of %s ends with its sync marker", location).isPositive();
    this.header = Arrays.copyOfRange(bytes, 0, headerEnd);
    this.blocks = Arrays.copyOfRange(bytes, headerEnd, bytes.length);
    assertThat(blocks).as("the blocks of %s", location).isNotEmpty();
  }

  /** Writes the rows into an Avro data file of the table through its {@code FileIO}. */
  public static EndlessAvroFile write(Table table, Schema schema, List<Record> rows) {
    String location = table.location() + "/data/endless-" + UUID.randomUUID() + ".avro";
    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.AVRO)
            .dataSchema(schema)
            .build()
            .newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(location)),
                PartitionSpec.unpartitioned(),
                null);
    try {
      try {
        rows.forEach(writer::write);
      } finally {
        writer.close();
      }

      byte[] bytes;
      try (InputStream stream = table.io().newInputFile(location).newStream()) {
        bytes = ByteStreams.toByteArray(stream);
      }

      DataFile file = writer.toDataFile();
      DataFile written =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .copy(file)
              .withFileSizeInBytes(DECLARED_SIZE)
              .withMetrics(
                  new Metrics(
                      file.recordCount(),
                      file.columnSizes(),
                      file.valueCounts(),
                      file.nullValueCounts(),
                      file.nanValueCounts(),
                      bounds(schema, rows, -1),
                      bounds(schema, rows, 1)))
              .build();
      return new EndlessAvroFile(location, written, bytes, rows.size());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The record of the file for an append: its size is {@link #DECLARED_SIZE}. */
  public DataFile dataFile() {
    return dataFile;
  }

  public String location() {
    return location;
  }

  /** From now on the file does not end; a read of it ignores an interrupt of its thread. */
  public EndlessAvroFile arm() {
    this.armed = true;
    return this;
  }

  /**
   * From now on the file does not end, and a read of it on an interrupted thread throws an {@link
   * InterruptedIOException}, as a client of an object store does.
   */
  public EndlessAvroFile armInterruptible() {
    this.interruptible = true;
    return arm();
  }

  /** The file ends again: a read after this reads it whole. */
  public void disarm() {
    this.armed = false;
  }

  /**
   * Runs the action once, on the thread that reads the file, as soon as {@code rows} rows of it
   * were served.
   */
  public void whenServed(long rows, Runnable action) {
    atRows.add(Maps.immutableEntry(rows, action));
  }

  /** The rows served so far, counted by the whole rounds of the blocks started. */
  public long rowsServed() {
    return rounds.get() * rowsPerRound;
  }

  /** The thread that read the file last, or {@code null}. */
  public Thread reader() {
    return reader;
  }

  public void awaitServed(long rows) {
    StatisticsAssertions.await(
        "the read of " + rows + " rows of the endless file", () -> rowsServed() >= rows);
  }

  /** The file the storage hands out for {@code path}: this one, if armed, else {@code file}. */
  public InputFile serve(String path, InputFile file) {
    return armed && location.equals(path) ? new Endless() : file;
  }

  private void served(long round) {
    long now = rounds.accumulateAndGet(round, Math::max);
    for (Map.Entry<Long, Runnable> entry : atRows) {
      if (entry.getKey() <= now * rowsPerRound && atRows.remove(entry)) {
        entry.getValue().run();
      }
    }
  }

  /**
   * The lower ({@code sign} -1) or upper bounds of the long columns: Avro keeps none, and a
   * copy-on-write rewrite prunes the file by them rather than read it.
   */
  private static Map<Integer, ByteBuffer> bounds(Schema schema, List<Record> rows, int sign) {
    Map<Integer, ByteBuffer> bounds = Maps.newHashMap();
    for (Types.NestedField column : schema.columns()) {
      if (column.type().typeId() != Type.TypeID.LONG) {
        continue;
      }

      rows.stream()
          .map(row -> (Long) row.getField(column.name()))
          .filter(Objects::nonNull)
          .reduce((one, other) -> Long.compare(one, other) * sign >= 0 ? one : other)
          .ifPresent(
              bound ->
                  bounds.put(column.fieldId(), Conversions.toByteBuffer(column.type(), bound)));
    }

    return bounds;
  }

  private static int indexOf(byte[] bytes, byte[] part) {
    for (int start = 0; start + part.length <= bytes.length; start++) {
      if (Arrays.equals(bytes, start, start + part.length, part, 0, part.length)) {
        return start;
      }
    }

    return -1;
  }

  private final class Endless implements InputFile {
    @Override
    public long getLength() {
      return Long.MAX_VALUE;
    }

    @Override
    public SeekableInputStream newStream() {
      return new EndlessStream();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }

  private final class EndlessStream extends SeekableInputStream {
    private final long start = System.nanoTime();
    private long pos = 0L;

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      this.pos = newPos;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int read = read(one, 0, 1);
      return read < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }

      if (interruptible && Thread.currentThread().isInterrupted()) {
        throw new InterruptedIOException("the read of " + location + " is interrupted");
      }

      reader = Thread.currentThread();
      int copied;
      if (pos < header.length) {
        copied = (int) Math.min(len, header.length - pos);
        System.arraycopy(header, (int) pos, bytes, off, copied);
      } else {
        long inBlocks = pos - header.length;
        int at = (int) (inBlocks % blocks.length);
        if (at == 0 && System.nanoTime() - start > GIVE_UP_NANOS) {
          return -1;
        }

        copied = Math.min(len, blocks.length - at);
        System.arraycopy(blocks, at, bytes, off, copied);
        served(inBlocks / blocks.length + 1);
      }

      pos += copied;
      return copied;
    }
  }
}
