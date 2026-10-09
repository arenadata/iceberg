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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * Reference to the intermediate Puffin file with the column sketches a worker wrote for a commit.
 * The file and footer sizes let a Puffin reader open the file without listing or probing it.
 */
public class SketchFile implements IndexedRecord {

  private String location;
  private Long fileSizeBytes;
  private Long footerSizeBytes;
  private final Schema avroSchema;

  static final int LOCATION = 10_960;
  static final int FILE_SIZE_BYTES = 10_961;
  static final int FOOTER_SIZE_BYTES = 10_962;

  public static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(LOCATION, "location", StringType.get()),
          NestedField.required(FILE_SIZE_BYTES, "file_size_bytes", LongType.get()),
          NestedField.required(FOOTER_SIZE_BYTES, "footer_size_bytes", LongType.get()));
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, SketchFile.class);

  // Used by Avro reflection to instantiate this class when reading events
  public SketchFile(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public SketchFile(String location, long fileSizeBytes, long footerSizeBytes) {
    Preconditions.checkNotNull(location, "Location cannot be null");
    this.location = location;
    this.fileSizeBytes = fileSizeBytes;
    this.footerSizeBytes = footerSizeBytes;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String location() {
    return location;
  }

  public long fileSizeBytes() {
    return fileSizeBytes;
  }

  public long footerSizeBytes() {
    return footerSizeBytes;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case LOCATION:
        this.location = v == null ? null : v.toString();
        return;
      case FILE_SIZE_BYTES:
        this.fileSizeBytes = (Long) v;
        return;
      case FOOTER_SIZE_BYTES:
        this.footerSizeBytes = (Long) v;
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
      case FOOTER_SIZE_BYTES:
        return footerSizeBytes;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
