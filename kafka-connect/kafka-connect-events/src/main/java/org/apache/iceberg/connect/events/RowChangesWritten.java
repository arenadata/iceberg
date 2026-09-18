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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a worker in copy-on-write mode, pointing at the change
 * files it has staged for a commit.
 *
 * <p>Distinct from {@link DataWritten}, which means "files are written, all that is left is to
 * commit them". Staged changes are neither data files nor delete files, and nothing can be
 * committed from them until the coordinator has planned the rewrite.
 */
public class RowChangesWritten implements Payload {

  private UUID commitId;
  private TableReference tableReference;
  private String taskId;
  private List<String> sourceTopics;
  private List<StagedChangeFile> stagedFiles;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_800;
  static final int TABLE_REFERENCE = 10_801;
  static final int TASK_ID = 10_802;
  static final int SOURCE_TOPICS = 10_803;
  static final int SOURCE_TOPICS_ELEMENT = 10_804;
  static final int STAGED_FILES = 10_805;
  static final int STAGED_FILES_ELEMENT = 10_806;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(COMMIT_ID, "commit_id", UUIDType.get()),
          NestedField.required(TABLE_REFERENCE, "table_reference", TableReference.ICEBERG_SCHEMA),
          NestedField.required(TASK_ID, "task_id", StringType.get()),
          NestedField.optional(
              SOURCE_TOPICS,
              "source_topics",
              ListType.ofRequired(SOURCE_TOPICS_ELEMENT, StringType.get())),
          NestedField.optional(
              STAGED_FILES,
              "staged_files",
              ListType.ofRequired(STAGED_FILES_ELEMENT, StagedChangeFile.ICEBERG_SCHEMA)));
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(ICEBERG_SCHEMA, RowChangesWritten.class);

  // Used by Avro reflection to instantiate this class when reading events
  public RowChangesWritten(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RowChangesWritten(
      UUID commitId,
      TableReference tableReference,
      String taskId,
      List<String> sourceTopics,
      List<StagedChangeFile> stagedFiles) {
    Preconditions.checkNotNull(commitId, "Commit ID cannot be null");
    Preconditions.checkNotNull(tableReference, "Table reference cannot be null");
    Preconditions.checkNotNull(taskId, "Task ID cannot be null");
    this.commitId = commitId;
    this.tableReference = tableReference;
    this.taskId = taskId;
    this.sourceTopics = sourceTopics;
    this.stagedFiles = stagedFiles;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.ROW_CHANGES_WRITTEN;
  }

  public UUID commitId() {
    return commitId;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  public String taskId() {
    return taskId;
  }

  public List<String> sourceTopics() {
    return sourceTopics == null ? ImmutableList.of() : sourceTopics;
  }

  public List<StagedChangeFile> stagedFiles() {
    return stagedFiles == null ? ImmutableList.of() : stagedFiles;
  }

  @Override
  public StructType writeSchema() {
    return ICEBERG_SCHEMA;
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
      case TASK_ID:
        this.taskId = v == null ? null : v.toString();
        return;
      case SOURCE_TOPICS:
        this.sourceTopics =
            v == null
                ? ImmutableList.of()
                : ((List<?>) v).stream().map(Object::toString).collect(Collectors.toList());
        return;
      case STAGED_FILES:
        this.stagedFiles = (List<StagedChangeFile>) v;
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
      case TASK_ID:
        return taskId;
      case SOURCE_TOPICS:
        return sourceTopics;
      case STAGED_FILES:
        return stagedFiles;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
