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
package org.apache.iceberg.connect.events;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * Element describing a copy-on-write change file staged by a worker.
 *
 * <p>Only the descriptor travels on the control topic; the rows themselves stay in the file. The
 * bounds let the coordinator drop a file from consideration without reading it, and the identifier
 * fields let it accept or refuse the file at freeze without opening it.
 */
public class StagedChangeFile implements IndexedRecord {

  private String location;
  private Long fileSizeBytes;
  private Long recordCount;
  private Integer schemaId;
  private Integer formatVersion;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;
  private List<Integer> identifierFieldIds;
  private final Schema avroSchema;

  static final int LOCATION = 10_810;
  static final int FILE_SIZE_BYTES = 10_811;
  static final int RECORD_COUNT = 10_812;
  static final int SCHEMA_ID = 10_813;
  static final int FORMAT_VERSION = 10_814;
  static final int LOWER_BOUNDS = 10_815;
  static final int LOWER_BOUNDS_KEY = 10_816;
  static final int LOWER_BOUNDS_VALUE = 10_817;
  static final int UPPER_BOUNDS = 10_818;
  static final int UPPER_BOUNDS_KEY = 10_819;
  static final int UPPER_BOUNDS_VALUE = 10_820;
  static final int IDENTIFIER_FIELD_IDS = 10_821;
  static final int IDENTIFIER_FIELD_IDS_ELEMENT = 10_822;

  public static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(LOCATION, "location", StringType.get()),
          NestedField.required(FILE_SIZE_BYTES, "file_size_bytes", LongType.get()),
          NestedField.required(RECORD_COUNT, "record_count", LongType.get()),
          NestedField.required(SCHEMA_ID, "schema_id", IntegerType.get()),
          NestedField.required(FORMAT_VERSION, "format_version", IntegerType.get()),
          NestedField.optional(
              LOWER_BOUNDS,
              "lower_bounds",
              MapType.ofRequired(
                  LOWER_BOUNDS_KEY, LOWER_BOUNDS_VALUE, IntegerType.get(), BinaryType.get())),
          NestedField.optional(
              UPPER_BOUNDS,
              "upper_bounds",
              MapType.ofRequired(
                  UPPER_BOUNDS_KEY, UPPER_BOUNDS_VALUE, IntegerType.get(), BinaryType.get())),
          NestedField.optional(
              IDENTIFIER_FIELD_IDS,
              "identifier_field_ids",
              ListType.ofRequired(IDENTIFIER_FIELD_IDS_ELEMENT, IntegerType.get())));
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(ICEBERG_SCHEMA, StagedChangeFile.class);

  // Used by Avro reflection to instantiate this class when reading events
  public StagedChangeFile(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  /** A descriptor that does not say which identifier fields its file was written with. */
  public StagedChangeFile(
      String location,
      long fileSizeBytes,
      long recordCount,
      int schemaId,
      int formatVersion,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    this(
        location,
        fileSizeBytes,
        recordCount,
        schemaId,
        formatVersion,
        lowerBounds,
        upperBounds,
        null);
  }

  public StagedChangeFile(
      String location,
      long fileSizeBytes,
      long recordCount,
      int schemaId,
      int formatVersion,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds,
      Collection<Integer> identifierFieldIds) {
    Preconditions.checkNotNull(location, "Location cannot be null");
    this.location = location;
    this.fileSizeBytes = fileSizeBytes;
    this.recordCount = recordCount;
    this.schemaId = schemaId;
    this.formatVersion = formatVersion;
    this.lowerBounds = lowerBounds;
    this.upperBounds = upperBounds;
    this.identifierFieldIds =
        identifierFieldIds == null ? null : ImmutableList.sortedCopyOf(identifierFieldIds);
    this.avroSchema = AVRO_SCHEMA;
  }

  public String location() {
    return location;
  }

  public long fileSizeBytes() {
    return fileSizeBytes;
  }

  public long recordCount() {
    return recordCount;
  }

  public int schemaId() {
    return schemaId;
  }

  public int formatVersion() {
    return formatVersion;
  }

  public Map<Integer, ByteBuffer> lowerBounds() {
    return lowerBounds;
  }

  public Map<Integer, ByteBuffer> upperBounds() {
    return upperBounds;
  }

  /**
   * The identifier fields the file was written with, as the file's {@code identifier-field-ids}
   * metadata records them. Empty when the descriptor does not say: no table's set is a subset of
   * that, so such a file is never accepted.
   */
  public List<Integer> identifierFieldIds() {
    return identifierFieldIds == null ? ImmutableList.of() : identifierFieldIds;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case LOCATION:
        this.location = v == null ? null : v.toString();
        return;
      case FILE_SIZE_BYTES:
        this.fileSizeBytes = (Long) v;
        return;
      case RECORD_COUNT:
        this.recordCount = (Long) v;
        return;
      case SCHEMA_ID:
        this.schemaId = (Integer) v;
        return;
      case FORMAT_VERSION:
        this.formatVersion = (Integer) v;
        return;
      case LOWER_BOUNDS:
        this.lowerBounds = (Map<Integer, ByteBuffer>) v;
        return;
      case UPPER_BOUNDS:
        this.upperBounds = (Map<Integer, ByteBuffer>) v;
        return;
      case IDENTIFIER_FIELD_IDS:
        this.identifierFieldIds = (List<Integer>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case LOCATION:
        return location;
      case FILE_SIZE_BYTES:
        return fileSizeBytes;
      case RECORD_COUNT:
        return recordCount;
      case SCHEMA_ID:
        return schemaId;
      case FORMAT_VERSION:
        return formatVersion;
      case LOWER_BOUNDS:
        return lowerBounds;
      case UPPER_BOUNDS:
        return upperBounds;
      case IDENTIFIER_FIELD_IDS:
        return identifierFieldIds;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
