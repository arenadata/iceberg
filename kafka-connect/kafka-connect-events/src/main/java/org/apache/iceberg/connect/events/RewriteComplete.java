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
import org.apache.avro.Schema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a worker that has finished, or failed, its part of a
 * copy-on-write slice. A failure is reported explicitly: a worker that died on its first file would
 * otherwise cost the slice a full timeout every cycle.
 *
 * <p>A worker may split its answer over several messages; every chunk carries the same slice, its
 * own {@link #chunkIndex()} and the shared {@link #chunkCount()}. The coordinator treats a task as
 * having answered only once it has seen every distinct index, so a lost chunk is a timeout rather
 * than a commit missing data files. Each chunk is its own producer transaction and can be lost
 * alone: a flag on the final chunk would not notice that, and a bare count would accept one chunk
 * delivered twice in place of a missing one.
 */
public class RewriteComplete implements Payload {

  /** Rewrite finished, {@link #dataFiles()} holds the replacement files. */
  public static final int STATUS_OK = 0;

  /**
   * Rewrite failed, and {@link #dataFiles()} is empty. What the coordinator does depends on {@link
   * #failureKind()}: a permanent failure stops the table's drain until the connector is restarted,
   * a retryable one (or one that does not say) cancels the whole slice and hands it out again. A
   * failure that arrives after the task's answer was already complete is ignored.
   */
  public static final int STATUS_FAILED = 1;

  /** A failed rewrite that another attempt may get through: the slice is handed out again. */
  public static final int FAILURE_RETRYABLE = 0;

  /**
   * A failed rewrite that every attempt will fail the same way until an operator applies the remedy
   * the worker logged ({@code PermanentCopyOnWriteException}): the coordinator stops the table
   * rather than retrying it.
   */
  public static final int FAILURE_PERMANENT = 1;

  private StructType partitionType;

  private UUID commitId;
  private TableReference tableReference;
  private UUID changeSetId;
  private Integer sliceSeq;
  private String taskId;
  private Integer status;
  private List<DataFile> dataFiles;
  private Integer chunkCount;
  private Integer chunkIndex;
  private Integer failureKind;
  private StructType icebergSchema;
  private final Schema avroSchema;

  static final int COMMIT_ID = 11_000;
  static final int TABLE_REFERENCE = 11_001;
  static final int CHANGE_SET_ID = 11_002;
  static final int SLICE_SEQ = 11_003;
  static final int TASK_ID = 11_004;
  static final int STATUS = 11_005;
  static final int DATA_FILES = 11_006;
  static final int DATA_FILES_ELEMENT = 11_007;
  static final int CHUNK_COUNT = 11_008;
  static final int CHUNK_INDEX = 11_009;
  static final int FAILURE_KIND = 11_010;

  // Used by Avro reflection to instantiate this class when reading events, note that this does not
  // set the partition type so the instance cannot be re-serialized
  public RewriteComplete(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RewriteComplete(
      StructType partitionType,
      UUID commitId,
      TableReference tableReference,
      UUID changeSetId,
      int sliceSeq,
      String taskId,
      int status,
      List<DataFile> dataFiles,
      int chunkIndex,
      int chunkCount) {
    this(
        partitionType,
        commitId,
        tableReference,
        changeSetId,
        sliceSeq,
        taskId,
        status,
        dataFiles,
        chunkIndex,
        chunkCount,
        null);
  }

  /**
   * An answer that also says whether a failure is worth retrying.
   *
   * @param failureKind {@link #FAILURE_RETRYABLE} or {@link #FAILURE_PERMANENT} for a failed
   *     rewrite, null otherwise
   */
  public RewriteComplete(
      StructType partitionType,
      UUID commitId,
      TableReference tableReference,
      UUID changeSetId,
      int sliceSeq,
      String taskId,
      int status,
      List<DataFile> dataFiles,
      int chunkIndex,
      int chunkCount,
      Integer failureKind) {
    Preconditions.checkNotNull(commitId, "Commit ID cannot be null");
    Preconditions.checkNotNull(tableReference, "Table reference cannot be null");
    Preconditions.checkNotNull(changeSetId, "Change set ID cannot be null");
    Preconditions.checkNotNull(taskId, "Task ID cannot be null");
    Preconditions.checkArgument(chunkCount > 0, "Chunk count must be positive: %s", chunkCount);
    Preconditions.checkArgument(
        chunkIndex >= 0 && chunkIndex < chunkCount,
        "Chunk index %s out of range for %s chunk(s)",
        chunkIndex,
        chunkCount);
    this.partitionType = partitionType;
    this.commitId = commitId;
    this.tableReference = tableReference;
    this.changeSetId = changeSetId;
    this.sliceSeq = sliceSeq;
    this.taskId = taskId;
    this.status = status;
    this.dataFiles = dataFiles;
    this.chunkCount = chunkCount;
    this.chunkIndex = chunkIndex;
    this.failureKind = failureKind;
    if (dataFiles != null) {
      dataFiles.forEach(
          file -> WirePartitions.checkPartitionTypeFits(partitionType, file, "data file"));
    }
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  @Override
  public PayloadType type() {
    return PayloadType.REWRITE_COMPLETE;
  }

  public UUID commitId() {
    return commitId;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  public UUID changeSetId() {
    return changeSetId;
  }

  public int sliceSeq() {
    return sliceSeq;
  }

  public String taskId() {
    return taskId;
  }

  public int status() {
    return status;
  }

  public List<DataFile> dataFiles() {
    return dataFiles == null ? ImmutableList.of() : dataFiles;
  }

  /** How many messages this task's answer is split over, the same on every one of them. */
  public int chunkCount() {
    return chunkCount == null ? 1 : chunkCount;
  }

  /** Which chunk of {@link #chunkCount()} this is, counted from zero. */
  public int chunkIndex() {
    return chunkIndex == null ? 0 : chunkIndex;
  }

  /**
   * Whether a failed rewrite is worth retrying; meaningful only with {@link #STATUS_FAILED}. A
   * worker of an earlier version does not say, and its failures read as {@link #FAILURE_RETRYABLE}.
   */
  public int failureKind() {
    return failureKind == null ? FAILURE_RETRYABLE : failureKind;
  }

  @Override
  public StructType writeSchema() {
    if (icebergSchema == null) {
      this.icebergSchema =
          StructType.of(
              NestedField.required(COMMIT_ID, "commit_id", UUIDType.get()),
              NestedField.required(
                  TABLE_REFERENCE, "table_reference", TableReference.ICEBERG_SCHEMA),
              NestedField.required(CHANGE_SET_ID, "change_set_id", UUIDType.get()),
              NestedField.required(SLICE_SEQ, "slice_seq", IntegerType.get()),
              NestedField.required(TASK_ID, "task_id", StringType.get()),
              NestedField.required(STATUS, "status", IntegerType.get()),
              NestedField.optional(
                  DATA_FILES,
                  "data_files",
                  ListType.ofRequired(DATA_FILES_ELEMENT, DataFile.getType(partitionType))),
              NestedField.required(CHUNK_COUNT, "chunk_count", IntegerType.get()),
              NestedField.required(CHUNK_INDEX, "chunk_index", IntegerType.get()),
              NestedField.optional(FAILURE_KIND, "failure_kind", IntegerType.get()));
    }

    return icebergSchema;
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
      case CHANGE_SET_ID:
        this.changeSetId = (UUID) v;
        return;
      case SLICE_SEQ:
        this.sliceSeq = (Integer) v;
        return;
      case TASK_ID:
        this.taskId = v == null ? null : v.toString();
        return;
      case STATUS:
        this.status = (Integer) v;
        return;
      case DATA_FILES:
        this.dataFiles = (List<DataFile>) v;
        return;
      case CHUNK_COUNT:
        this.chunkCount = (Integer) v;
        return;
      case CHUNK_INDEX:
        this.chunkIndex = (Integer) v;
        return;
      case FAILURE_KIND:
        this.failureKind = (Integer) v;
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
      case CHANGE_SET_ID:
        return changeSetId;
      case SLICE_SEQ:
        return sliceSeq;
      case TASK_ID:
        return taskId;
      case STATUS:
        return status;
      case DATA_FILES:
        return dataFiles;
      case CHUNK_INDEX:
        return chunkIndex;
      case CHUNK_COUNT:
        return chunkCount;
      case FAILURE_KIND:
        return failureKind;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
