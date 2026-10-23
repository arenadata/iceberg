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
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.types.Types.StructType;

/**
 * The reading side of {@link DataWritten} as released, renamed: the coordinator of a connector one
 * version behind. Keep it as it is; it must not follow changes to {@link DataWritten}.
 *
 * <p>Its {@code get} throws on an unknown field, and the Avro reader calls {@code get} for every
 * field of the writer schema, so an event with a field unknown here cannot be read at all.
 */
public class LegacyDataWritten implements Payload {

  private UUID commitId;
  private TableReference tableReference;
  private List<DataFile> dataFiles;
  private List<DeleteFile> deleteFiles;
  private List<String> sourceTopics;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_300;
  static final int TABLE_REFERENCE = 10_301;
  static final int DATA_FILES = 10_302;
  static final int DELETE_FILES = 10_304;
  static final int SOURCE_TOPICS = 10_306;

  // Used by Avro reflection to instantiate this class when reading events
  public LegacyDataWritten(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  @Override
  public PayloadType type() {
    return PayloadType.DATA_WRITTEN;
  }

  public UUID commitId() {
    return commitId;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  public List<DataFile> dataFiles() {
    return dataFiles;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public List<String> sourceTopics() {
    return sourceTopics == null ? List.of() : sourceTopics;
  }

  @Override
  public StructType writeSchema() {
    throw new UnsupportedOperationException("A reader does not write");
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        this.commitId = (UUID) v;
        return;
      case TABLE_REFERENCE:
        this.tableReference = (TableReference) v;
        return;
      case DATA_FILES:
        this.dataFiles = (List<DataFile>) v;
        return;
      case DELETE_FILES:
        this.deleteFiles = (List<DeleteFile>) v;
        return;
      case SOURCE_TOPICS:
        this.sourceTopics =
            v == null
                ? List.of()
                : ((List<?>) v).stream().map(Object::toString).collect(Collectors.toList());
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        return commitId;
      case TABLE_REFERENCE:
        return tableReference;
      case DATA_FILES:
        return dataFiles;
      case DELETE_FILES:
        return deleteFiles;
      case SOURCE_TOPICS:
        return sourceTopics;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
