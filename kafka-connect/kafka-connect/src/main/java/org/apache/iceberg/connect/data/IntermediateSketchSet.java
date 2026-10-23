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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The intermediate Puffin files of the answers in the chain of snapshot S, and the coverage of each
 * answer by column.
 *
 * <p>Each file is read once, however many answers refer to it. The blobs that cover a column are
 * united into one sketch of the column as each file is read, so no blob is kept after its file. A
 * file that is gone or broken leaves all its answers uncovered. Any other failure to read a file
 * that still exists is kept as {@link #failure()}: a retry may fix it, and reading the files of the
 * answers instead could exceed the limit for good.
 */
public class IntermediateSketchSet {
  private static final Logger LOG = LoggerFactory.getLogger(IntermediateSketchSet.class);

  public enum Coverage {
    /** The blob of the column in the answer's file is in {@link #sketch(int)}. */
    COVERED,
    /** The writer did not know the column: the rows of the answer have only its initial default. */
    EMPTY,
    /** The values of the column in the answer's rows are in no sketch. */
    UNCOVERED
  }

  private static final FileContent UNAVAILABLE =
      new FileContent(false, 0, 0L, ImmutableSet.of(), ImmutableSet.of());

  private final boolean stopped;
  private final Exception failure;
  private final Map<String, FileContent> files;
  private final Map<Integer, Union> unions;
  private final Map<Integer, Long> coveredRows;

  private IntermediateSketchSet(
      boolean stopped,
      Exception failure,
      Map<String, FileContent> files,
      Map<Integer, Union> unions,
      Map<Integer, Long> coveredRows) {
    this.stopped = stopped;
    this.failure = failure;
    this.files = files;
    this.unions = unions;
    this.coveredRows = coveredRows;
  }

  /**
   * Reads the intermediate Puffin files of the answers, checking {@code stopped} before each.
   *
   * @param columns the columns of the set, resolved against {@code schemaS}
   * @param lgK log2 of the nominal entries of the union of each column
   */
  public static IntermediateSketchSet read(
      Table table,
      List<StatisticsAnswer> answers,
      Schema schemaS,
      List<Types.NestedField> columns,
      int lgK,
      BooleanSupplier stopped) {
    return read(LOG, table, answers, schemaS, columns, lgK, stopped);
  }

  static IntermediateSketchSet read(
      Logger logger,
      Table table,
      List<StatisticsAnswer> answers,
      Schema schemaS,
      List<Types.NestedField> columns,
      int lgK,
      BooleanSupplier stopped) {
    Map<Integer, Type> types = Maps.newHashMap();
    columns.forEach(column -> types.put(column.fieldId(), schemaS.findType(column.fieldId())));

    Map<String, List<StatisticsAnswer>> byLocation = Maps.newLinkedHashMap();
    for (StatisticsAnswer answer : answers) {
      if (answer.sketchFile() != null) {
        byLocation
            .computeIfAbsent(answer.sketchFile().location(), location -> Lists.newArrayList())
            .add(answer);
      }
    }

    Map<String, FileContent> files = Maps.newHashMap();
    Map<Integer, Union> unions = Maps.newHashMap();
    Map<Integer, Long> coveredRows = Maps.newHashMap();
    for (Map.Entry<String, List<StatisticsAnswer>> entry : byLocation.entrySet()) {
      if (isStopped(stopped)) {
        return stoppedBeforeEnd();
      }

      String location = entry.getKey();
      try {
        FileRead read = readFile(table.io(), entry.getValue().get(0).sketchFile(), types);
        read.sketches.forEach(
            (fieldId, sketch) ->
                unions
                    .computeIfAbsent(
                        fieldId,
                        id -> SetOperation.builder().setNominalEntries(1 << lgK).buildUnion())
                    .union(sketch));
        read.content.covered.forEach(
            fieldId -> coveredRows.merge(fieldId, read.content.rowCount, Long::sum));
        files.put(location, read.content);
      } catch (IOException | RuntimeException e) {
        if (isStopped(stopped)) {
          return stoppedBeforeEnd();
        }

        if (!isUnavailable(e) && !isDeleted(table.io(), location, e)) {
          return failed(e);
        }

        files.put(location, UNAVAILABLE);
        logger.warn(
            "Intermediate statistics file {} of table {} cannot be read: the files of its {} "
                + "answers are read instead",
            location,
            table.name(),
            entry.getValue().size(),
            e);
      }
    }

    return new IntermediateSketchSet(false, null, files, unions, coveredRows);
  }

  private static IntermediateSketchSet stoppedBeforeEnd() {
    return new IntermediateSketchSet(
        true, null, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());
  }

  private static IntermediateSketchSet failed(Exception failure) {
    return new IntermediateSketchSet(
        false, failure, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());
  }

  private static boolean isUnavailable(Exception failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof NotFoundException || cause instanceof FileNotFoundException) {
        return true;
      }
    }

    return failure instanceof BrokenContentException || NdvBaseline.isCorrupt(failure);
  }

  private static boolean isDeleted(FileIO io, String location, Exception failure) {
    try {
      return !io.newInputFile(location).exists();
    } catch (RuntimeException e) {
      failure.addSuppressed(e);
      return false;
    }
  }

  private static FileRead readFile(FileIO io, SketchFile file, Map<Integer, Type> types)
      throws IOException {
    try (PuffinReader reader =
        Puffin.read(io.newInputFile(file.location(), file.fileSizeBytes()))
            .withFileSize(file.fileSizeBytes())
            .withFooterSize(file.footerSizeBytes())
            .build()) {
      FileMetadata metadata = reader.fileMetadata();
      long lastColumnId = number(metadata, ColumnSketchCollector.LAST_COLUMN_ID_PROPERTY);
      long rowCount = number(metadata, ColumnSketchCollector.ROW_COUNT_PROPERTY);
      if (rowCount < 0 || lastColumnId > Integer.MAX_VALUE) {
        throw new BrokenContentException(
            "Invalid file properties: last column id " + lastColumnId + ", row count " + rowCount,
            null);
      }

      Set<Integer> withBlob = Sets.newHashSet();
      Map<Integer, BlobMetadata> covering = Maps.newHashMap();
      for (BlobMetadata blob : metadata.blobs()) {
        Integer fieldId = NdvBaseline.thetaFieldId(blob.type(), blob.inputFields());
        Type.PrimitiveType type = blobType(blob);
        if (fieldId == null || !types.containsKey(fieldId) || type == null) {
          continue; // a blob whose type does not parse is as good as none
        }

        withBlob.add(fieldId);
        if (type.equals(types.get(fieldId))) {
          covering.putIfAbsent(fieldId, blob);
        }
      }

      Map<Integer, Sketch> sketches = Maps.newHashMap();
      for (Pair<BlobMetadata, ByteBuffer> blob :
          reader.readAll(ImmutableList.copyOf(covering.values()))) {
        byte[] data = ByteBuffers.toByteArray(blob.second());
        sketches.put(blob.first().inputFields().get(0), wrapSketch(data));
      }

      // a copy of the ids: the sketches are dropped once united
      FileContent content =
          new FileContent(
              true, (int) lastColumnId, rowCount, ImmutableSet.copyOf(sketches.keySet()), withBlob);
      return new FileRead(content, sketches);
    }
  }

  private static long number(FileMetadata metadata, String name) {
    String value = metadata.properties().get(name);
    if (value == null) {
      throw new BrokenContentException("Missing file property " + name, null);
    }

    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new BrokenContentException("Invalid file property " + name + ": " + value, e);
    }
  }

  private static Sketch wrapSketch(byte[] data) {
    try {
      return Sketches.wrapSketch(Memory.wrap(data), ThetaUtil.DEFAULT_UPDATE_SEED);
    } catch (RuntimeException e) {
      throw new BrokenContentException("Blob is not a theta sketch", e);
    }
  }

  private static Type.PrimitiveType blobType(BlobMetadata blob) {
    String type = blob.properties().get(ColumnSketchCollector.TYPE_PROPERTY);
    if (type == null) {
      return null;
    }

    try {
      return Types.fromPrimitiveString(type);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static boolean isStopped(BooleanSupplier stopped) {
    return stopped.getAsBoolean() || Thread.currentThread().isInterrupted();
  }

  /** Deletes the intermediate Puffin files at the locations, best effort, until stopped. */
  public static void delete(
      FileIO io,
      Iterable<String> locations,
      TableIdentifier identifier,
      BooleanSupplier stopped,
      Logger log) {
    for (String location : locations) {
      if (isStopped(stopped)) {
        return;
      }

      try {
        io.deleteFile(location);
      } catch (NotFoundException e) {
        log.debug("Intermediate statistics file {} is already gone", location, e);
      } catch (RuntimeException e) {
        log.warn(
            "Cannot delete the intermediate statistics file {} of table {}",
            location,
            identifier,
            e);
      }
    }
  }

  public boolean stopped() {
    return stopped;
  }

  public Exception failure() {
    return failure;
  }

  public Coverage coverage(StatisticsAnswer answer, int fieldId) {
    SketchFile file = answer.sketchFile();
    FileContent content = file == null ? null : files.get(file.location());
    if (content == null || !content.available) {
      return Coverage.UNCOVERED;
    } else if (content.covered.contains(fieldId)) {
      return Coverage.COVERED;
    } else if (content.lacks(fieldId)) {
      return Coverage.EMPTY;
    }

    return Coverage.UNCOVERED;
  }

  public boolean hasRowsWithout(int fieldId) {
    return files.values().stream()
        .anyMatch(content -> content.available && content.rowCount > 0 && content.lacks(fieldId));
  }

  /** The union of the covering blobs of the column, or {@code null} when no file covers it. */
  public CompactSketch sketch(int fieldId) {
    Union union = unions.get(fieldId);
    return union == null ? null : union.getResult();
  }

  /** The sum of {@code row-count} of the files that cover an answer by the column, once a file. */
  public long coveredRows(int fieldId) {
    return coveredRows.getOrDefault(fieldId, 0L);
  }

  /** The file was read, but what it holds is not what the writer produces. */
  private static class BrokenContentException extends RuntimeException {
    private BrokenContentException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class FileContent {
    private final boolean available;
    private final int lastColumnId;
    private final long rowCount;
    // columns whose blob has the type of the column in the schema of S
    private final Set<Integer> covered;
    // columns with a blob of a type that parses, whether it is the type in S or not
    private final Set<Integer> withBlob;

    private FileContent(
        boolean available,
        int lastColumnId,
        long rowCount,
        Set<Integer> covered,
        Set<Integer> withBlob) {
      this.available = available;
      this.lastColumnId = lastColumnId;
      this.rowCount = rowCount;
      this.covered = covered;
      this.withBlob = withBlob;
    }

    private boolean lacks(int fieldId) {
      return !withBlob.contains(fieldId) && fieldId > lastColumnId;
    }
  }

  private static class FileRead {
    private final FileContent content;
    private final Map<Integer, Sketch> sketches;

    private FileRead(FileContent content, Map<Integer, Sketch> sketches) {
      this.content = content;
      this.sketches = sketches;
    }
  }
}
