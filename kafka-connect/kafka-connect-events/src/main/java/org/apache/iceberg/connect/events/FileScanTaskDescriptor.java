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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
 * Element describing one data file a worker has to rewrite, together with the delete files that
 * apply to it.
 *
 * <p>This is the part of a {@code FileScanTask} a worker actually needs: the file is read whole, so
 * neither the residual nor the scan expression travels with it.
 */
public class FileScanTaskDescriptor implements IndexedRecord {

  private DataFile dataFile;
  private List<DeleteFile> deleteFiles;
  private Integer specId;
  private final Schema avroSchema;

  static final int DATA_FILE = 10_930;
  static final int DELETE_FILES = 10_931;
  static final int DELETE_FILES_ELEMENT = 10_932;
  static final int SPEC_ID = 10_933;

  public static StructType icebergSchema(StructType partitionType) {
    StructType dataFileStruct = DataFile.getType(partitionType);
    return StructType.of(
        NestedField.required(DATA_FILE, "data_file", dataFileStruct),
        NestedField.optional(
            DELETE_FILES,
            "delete_files",
            ListType.ofRequired(DELETE_FILES_ELEMENT, dataFileStruct)),
        NestedField.required(SPEC_ID, "spec_id", IntegerType.get()));
  }

  // the partition type changes the nested data file struct but not this element's own fields, so a
  // placeholder is enough for the position-to-id mapping put() and get() rely on
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(icebergSchema(StructType.of()), FileScanTaskDescriptor.class);

  // Used by Avro reflection to instantiate this class when reading events
  public FileScanTaskDescriptor(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public FileScanTaskDescriptor(
      DataFile dataFile, List<DeleteFile> deleteFiles, int specId, StructType wirePartitionType) {
    Preconditions.checkNotNull(dataFile, "Data file cannot be null");
    WirePartitions.checkPartitionTypeFits(wirePartitionType, dataFile, "data file");
    if (deleteFiles != null) {
      deleteFiles.forEach(
          file -> WirePartitions.checkPartitionTypeFits(wirePartitionType, file, "delete file"));
    }
    this.dataFile = dataFile;
    this.deleteFiles = deleteFiles;
    this.specId = specId;
    this.avroSchema = AVRO_SCHEMA;
  }

  public DataFile dataFile() {
    return dataFile;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public int specId() {
    return specId;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case DATA_FILE:
        this.dataFile = (DataFile) v;
        return;
      case DELETE_FILES:
        this.deleteFiles = (List<DeleteFile>) v;
        return;
      case SPEC_ID:
        this.specId = (Integer) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case DATA_FILE:
        return dataFile;
      case DELETE_FILES:
        return deleteFiles;
      case SPEC_ID:
        return specId;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
