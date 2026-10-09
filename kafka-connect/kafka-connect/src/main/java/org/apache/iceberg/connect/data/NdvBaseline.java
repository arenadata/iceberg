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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The initial sketches of the NDV columns from the base: the blob of the base when the column has
 * one of the same type, an empty sketch for an empty base or a new column, and no sketch at all for
 * a column without a baseline.
 */
public class NdvBaseline {
  private static final Logger LOG = LoggerFactory.getLogger(NdvBaseline.class);

  private static final Sketch EMPTY = UpdateSketch.builder().build().compact();

  /** Why a column of the set has no baseline in the base. */
  public enum Reason {
    NO_BLOB("no blob"),
    TYPE_CHANGED("type changed");

    private final String label;

    Reason(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return label;
    }
  }

  public enum Kind {
    /** The initial sketches are in {@link #sketches()}. */
    LOADED,
    /** The base file cannot be read for good: no statistics to build on. */
    NO_BASELINE,
    /** The base file could not be read this time: a retry may succeed. */
    FAILED,
    /** The step was stopped before or while reading the base file. */
    STOPPED
  }

  private final Kind kind;
  private final Map<Integer, Sketch> sketches;
  private final Set<Integer> undeserializable;
  private final Throwable failure;

  private NdvBaseline(
      Kind kind, Map<Integer, Sketch> sketches, Set<Integer> undeserializable, Throwable failure) {
    this.kind = kind;
    this.sketches = sketches;
    this.undeserializable = undeserializable;
    this.failure = failure;
  }

  /**
   * The columns of the set that are in the schema of the base but have no usable blob in it, by
   * field id in the order of the set; from the metadata only, the base file is not read.
   *
   * @param base the statistics record of the base, {@code null} for an empty base
   */
  public static Map<Integer, Reason> columnsWithoutBaseline(
      StatisticsFile base, Schema baseSchema, Schema schemaS, List<Types.NestedField> columns) {
    Map<Integer, Reason> result = Maps.newLinkedHashMap();
    if (base == null) {
      return result;
    }

    Set<Integer> withBlob = Sets.newHashSet();
    for (BlobMetadata blob : base.blobMetadata()) {
      Integer fieldId = thetaFieldId(blob.type(), blob.fields());
      if (fieldId != null) {
        withBlob.add(fieldId);
      }
    }

    for (Types.NestedField column : columns) {
      int fieldId = column.fieldId();
      Types.NestedField inBase = baseSchema.findField(fieldId);
      if (inBase == null) {
        continue; // a new column starts empty
      }

      Type typeInS = schemaS.findType(fieldId);
      if (!inBase.type().equals(typeInS)) {
        result.put(fieldId, Reason.TYPE_CHANGED);
      } else if (!withBlob.contains(fieldId)) {
        result.put(fieldId, Reason.NO_BLOB);
      }
    }

    return result;
  }

  /**
   * Loads the initial sketches of the columns of the set, checking {@code stopped} before the base
   * file is opened.
   *
   * @param base the statistics record of the base, {@code null} for an empty base
   * @param baseSchema the schema of the base snapshot, ignored for an empty base
   */
  public static NdvBaseline load(
      Table table,
      StatisticsFile base,
      Schema baseSchema,
      Schema schemaS,
      List<Types.NestedField> columns,
      BooleanSupplier stopped) {
    return load(LOG, table, base, baseSchema, schemaS, columns, stopped);
  }

  static NdvBaseline load(
      Logger logger,
      Table table,
      StatisticsFile base,
      Schema baseSchema,
      Schema schemaS,
      List<Types.NestedField> columns,
      BooleanSupplier stopped) {
    Map<Integer, Sketch> sketches = Maps.newLinkedHashMap();
    if (base == null) {
      columns.forEach(column -> sketches.put(column.fieldId(), EMPTY));
      return new NdvBaseline(Kind.LOADED, sketches, ImmutableSet.of(), null);
    }

    Preconditions.checkArgument(baseSchema != null, "Invalid base schema: null");
    Map<Integer, Reason> withoutBaseline =
        columnsWithoutBaseline(base, baseSchema, schemaS, columns);
    Set<Integer> fromBlobs = Sets.newHashSet();
    for (Types.NestedField column : columns) {
      int fieldId = column.fieldId();
      if (!withoutBaseline.containsKey(fieldId) && baseSchema.findField(fieldId) != null) {
        fromBlobs.add(fieldId);
      }
    }

    Map<Integer, byte[]> blobs = ImmutableMap.of();
    if (!fromBlobs.isEmpty()) {
      if (isStopped(stopped)) {
        return new NdvBaseline(Kind.STOPPED, ImmutableMap.of(), ImmutableSet.of(), null);
      }

      try {
        blobs = readBlobs(table.io(), base, fromBlobs);
      } catch (IOException | RuntimeException e) {
        return failure(table, base, e, stopped);
      }
    }

    Set<Integer> undeserializable = Sets.newLinkedHashSet();
    for (Types.NestedField column : columns) {
      int fieldId = column.fieldId();
      if (withoutBaseline.containsKey(fieldId)) {
        continue;
      } else if (!fromBlobs.contains(fieldId)) {
        sketches.put(fieldId, EMPTY);
        continue;
      }

      try {
        byte[] data = blobs.get(fieldId);
        Preconditions.checkState(data != null, "No theta sketch blob of field id %s", fieldId);
        // with the seed: a sketch of another seed fails here, not in the union
        sketches.put(
            fieldId, Sketches.wrapSketch(Memory.wrap(data), ThetaUtil.DEFAULT_UPDATE_SEED));
      } catch (RuntimeException e) {
        undeserializable.add(fieldId);
        logger.warn(
            "Column {} (field id {}) of table {} gets no NDV blob: its blob in base statistics "
                + "file {} cannot be read as a theta sketch",
            column.name(),
            fieldId,
            table.name(),
            base.path(),
            e);
      }
    }

    return new NdvBaseline(Kind.LOADED, sketches, undeserializable, null);
  }

  /** The field id of a theta sketch blob of one column, {@code null} for any other blob. */
  static Integer thetaFieldId(String type, List<Integer> fields) {
    if (StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(type)
        && fields != null
        && fields.size() == 1) {
      return fields.get(0);
    }

    return null;
  }

  private static Map<Integer, byte[]> readBlobs(
      FileIO io, StatisticsFile base, Set<Integer> fieldIds) throws IOException {
    Map<Integer, byte[]> blobs = Maps.newHashMap();
    try (PuffinReader reader =
        Puffin.read(io.newInputFile(base.path()))
            .withFileSize(base.fileSizeInBytes())
            .withFooterSize(base.fileFooterSizeInBytes())
            .build()) {
      Map<Integer, org.apache.iceberg.puffin.BlobMetadata> selected = Maps.newHashMap();
      for (org.apache.iceberg.puffin.BlobMetadata blob : reader.fileMetadata().blobs()) {
        Integer fieldId = thetaFieldId(blob.type(), blob.inputFields());
        if (fieldId != null && fieldIds.contains(fieldId)) {
          selected.putIfAbsent(fieldId, blob);
        }
      }

      for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> blob :
          reader.readAll(ImmutableList.copyOf(selected.values()))) {
        blobs.put(blob.first().inputFields().get(0), ByteBuffers.toByteArray(blob.second()));
      }
    }

    return blobs;
  }

  /** Classifies a failure to read the base file: stopped, no baseline, or failed for now. */
  private static NdvBaseline failure(
      Table table, StatisticsFile base, Exception failure, BooleanSupplier stopped) {
    if (isStopped(stopped)) {
      return new NdvBaseline(Kind.STOPPED, ImmutableMap.of(), ImmutableSet.of(), failure);
    }

    Kind kind;
    if (isCorrupt(failure)) {
      kind = Kind.NO_BASELINE;
    } else if (failure instanceof NotFoundException || isDeleted(table.io(), base, failure)) {
      // gone for good only if the record still points to it
      try {
        table.refresh();
        kind = isRecorded(table, base) ? Kind.NO_BASELINE : Kind.FAILED;
      } catch (RuntimeException e) {
        failure.addSuppressed(e);
        kind = Kind.FAILED;
      }
    } else {
      kind = Kind.FAILED;
    }

    return new NdvBaseline(kind, ImmutableMap.of(), ImmutableSet.of(), failure);
  }

  /** Whether the storage says the file is gone: an S3 range read throws a raw client exception. */
  private static boolean isDeleted(FileIO io, StatisticsFile base, Exception failure) {
    try {
      return !io.newInputFile(base.path()).exists();
    } catch (RuntimeException e) {
      failure.addSuppressed(e);
      return false;
    }
  }

  private static boolean isRecorded(Table table, StatisticsFile base) {
    return table.statisticsFiles().stream()
        .anyMatch(
            file -> file.snapshotId() == base.snapshotId() && base.path().equals(file.path()));
  }

  /**
   * A broken file: another read gives the same result. An {@code IllegalStateException} or {@code
   * IllegalArgumentException} counts only when Puffin itself threw it (a check of the format), not
   * a storage client (a closed connection pool is {@code IllegalStateException} too).
   */
  static boolean isCorrupt(Exception failure) {
    Throwable cause = failure instanceof UncheckedIOException ? failure.getCause() : failure;
    return cause instanceof EOFException
        || cause instanceof JsonProcessingException
        || ((cause instanceof IllegalStateException || cause instanceof IllegalArgumentException)
            && thrownByPuffin(cause));
  }

  /**
   * Whether the first frame of the stack beyond the precondition checks and the JSON helpers (the
   * parser of the footer reads it through them) is Puffin code.
   */
  private static boolean thrownByPuffin(Throwable failure) {
    for (StackTraceElement frame : failure.getStackTrace()) {
      String className = frame.getClassName();
      if (className.startsWith("org.apache.iceberg.relocated.")
          || className.startsWith("org.apache.iceberg.util.JsonUtil")
          || className.startsWith("java.")
          || className.startsWith("jdk.")) {
        continue;
      }

      return className.startsWith("org.apache.iceberg.puffin.");
    }

    return false;
  }

  private static boolean isStopped(BooleanSupplier stopped) {
    return stopped.getAsBoolean() || Thread.currentThread().isInterrupted();
  }

  public Kind kind() {
    return kind;
  }

  /** By field id, in the order of the set; empty unless {@link Kind#LOADED}. */
  public Map<Integer, Sketch> sketches() {
    return sketches;
  }

  /** Columns whose blob is in the base record but cannot be read as a theta sketch. */
  public Set<Integer> undeserializable() {
    return undeserializable;
  }

  /** What made the base file unreadable, {@code null} when it was read. */
  public Throwable failure() {
    return failure;
  }
}
