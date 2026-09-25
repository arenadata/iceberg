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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by the coordinator to hand one slice of a copy-on-write
 * rewrite to the workers. One message per task, carrying only that task's {@link Assignment}, so
 * the round's size is spread over {@code tasks.max}; with a descriptor per data file this is still
 * the largest payload in the protocol.
 *
 * <p>An assignment too large for one message goes out as several, each with its own {@link
 * #chunkIndex()} and the shared {@link #chunkCount()}, all in the transaction the round is sent in.
 * Every other field repeats in every chunk, and the addressee starts the rewrite only once it holds
 * them all: the slice is never shrunk to fit a message. A single data file descriptor past the
 * limit cannot be split and stops the table instead, since replanning would produce it again every
 * cycle.
 */
public class RewriteAssigned implements Payload {

  private StructType partitionType;

  private UUID commitId;
  private TableReference tableReference;
  private UUID changeSetId;
  private Integer sliceSeq;
  private Long baseSnapshotId;
  private StagedChangeFile normalizedRef;
  private List<Assignment> assignments;
  private Integer ownerIndex;
  private Integer ownerCount;
  private List<Integer> identifierFieldIds;
  private Integer chunkIndex;
  private Integer chunkCount;
  private StructType icebergSchema;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_900;
  static final int TABLE_REFERENCE = 10_901;
  static final int CHANGE_SET_ID = 10_902;
  static final int SLICE_SEQ = 10_903;
  static final int BASE_SNAPSHOT_ID = 10_904;
  static final int NORMALIZED_REF = 10_905;
  static final int ASSIGNMENTS = 10_906;
  static final int ASSIGNMENTS_ELEMENT = 10_907;
  static final int OWNER_INDEX = 10_908;
  static final int OWNER_COUNT = 10_909;
  static final int IDENTIFIER_FIELD_IDS = 10_950;
  static final int IDENTIFIER_FIELD_IDS_ELEMENT = 10_951;
  static final int CHUNK_INDEX = 10_952;
  static final int CHUNK_COUNT = 10_953;

  // Used by Avro reflection to instantiate this class when reading events, note that this does not
  // set the partition type so the instance cannot be re-serialized
  public RewriteAssigned(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  /** An assignment that fits one message: the whole of it, chunk {@code 0} of {@code 1}. */
  public RewriteAssigned(
      StructType partitionType,
      UUID commitId,
      TableReference tableReference,
      UUID changeSetId,
      int sliceSeq,
      long baseSnapshotId,
      StagedChangeFile normalizedRef,
      List<Assignment> assignments,
      int ownerIndex,
      int ownerCount,
      List<Integer> identifierFieldIds) {
    this(
        partitionType,
        commitId,
        tableReference,
        changeSetId,
        sliceSeq,
        baseSnapshotId,
        normalizedRef,
        assignments,
        ownerIndex,
        ownerCount,
        identifierFieldIds,
        0,
        1);
  }

  public RewriteAssigned(
      StructType partitionType,
      UUID commitId,
      TableReference tableReference,
      UUID changeSetId,
      int sliceSeq,
      long baseSnapshotId,
      StagedChangeFile normalizedRef,
      List<Assignment> assignments,
      int ownerIndex,
      int ownerCount,
      List<Integer> identifierFieldIds,
      int chunkIndex,
      int chunkCount) {
    Preconditions.checkNotNull(commitId, "Commit ID cannot be null");
    Preconditions.checkNotNull(tableReference, "Table reference cannot be null");
    Preconditions.checkNotNull(changeSetId, "Change set ID cannot be null");
    Preconditions.checkArgument(ownerCount > 0, "Owner count must be positive: %s", ownerCount);
    Preconditions.checkArgument(
        ownerIndex >= 0 && ownerIndex < ownerCount,
        "Owner index %s out of range for %s owner(s)",
        ownerIndex,
        ownerCount);
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
    this.baseSnapshotId = baseSnapshotId;
    this.normalizedRef = normalizedRef;
    this.assignments = assignments;
    this.ownerIndex = ownerIndex;
    this.ownerCount = ownerCount;
    this.identifierFieldIds = identifierFieldIds;
    this.chunkIndex = chunkIndex;
    this.chunkCount = chunkCount;
    // each descriptor checked its files against the type it was built with, but the wire schema is
    // built from this event's partition type, and the two come from different callers
    if (assignments != null) {
      for (Assignment assignment : assignments) {
        if (assignment.files() != null) {
          assignment.files().forEach(file -> checkFileFits(partitionType, file));
        }
      }
    }
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  private static void checkFileFits(StructType partitionType, FileScanTaskDescriptor file) {
    WirePartitions.checkPartitionTypeFits(partitionType, file.dataFile(), "data file");
    if (file.deleteFiles() != null) {
      for (DeleteFile deleteFile : file.deleteFiles()) {
        WirePartitions.checkDeleteFileFits(partitionType, deleteFile, file.specId());
      }
    }
  }

  @Override
  public PayloadType type() {
    return PayloadType.REWRITE_ASSIGNED;
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

  public long baseSnapshotId() {
    return baseSnapshotId;
  }

  public StagedChangeFile normalizedRef() {
    return normalizedRef;
  }

  public List<Assignment> assignments() {
    return assignments == null ? ImmutableList.of() : assignments;
  }

  /**
   * Which block of the slice's keys the addressee owns, and how many blocks there are. Carried
   * rather than derived: {@link #assignments()} holds only the addressee's own assignment.
   */
  public int ownerIndex() {
    return ownerIndex;
  }

  public int ownerCount() {
    return ownerCount;
  }

  /**
   * The identifier fields this change set was frozen with.
   *
   * <p>The coordinator's, not the worker's. Key order decides which keys each owner writes, so two
   * workers resolving identifier fields independently (against table handles that need not be at
   * the same schema) could partition the slice differently and silently duplicate or drop keys.
   */
  public List<Integer> identifierFieldIds() {
    return identifierFieldIds == null ? ImmutableList.of() : identifierFieldIds;
  }

  /**
   * How many messages this task's assignment was split over. Paired with {@link #chunkIndex()} for
   * the same reason as in {@link RewriteComplete}: a rewrite started on part of an assignment would
   * leave the missing files' keys unreplaced while the slice commits as if they were rewritten.
   */
  public int chunkCount() {
    return chunkCount == null ? 1 : chunkCount;
  }

  /** Which chunk of {@link #chunkCount()} this is, counted from zero. */
  public int chunkIndex() {
    return chunkIndex == null ? 0 : chunkIndex;
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
              NestedField.required(BASE_SNAPSHOT_ID, "base_snapshot_id", LongType.get()),
              NestedField.required(
                  NORMALIZED_REF, "normalized_ref", StagedChangeFile.ICEBERG_SCHEMA),
              NestedField.optional(
                  ASSIGNMENTS,
                  "assignments",
                  ListType.ofRequired(
                      ASSIGNMENTS_ELEMENT, Assignment.icebergSchema(partitionType))),
              NestedField.required(OWNER_INDEX, "owner_index", IntegerType.get()),
              NestedField.required(OWNER_COUNT, "owner_count", IntegerType.get()),
              NestedField.required(
                  IDENTIFIER_FIELD_IDS,
                  "identifier_field_ids",
                  ListType.ofRequired(IDENTIFIER_FIELD_IDS_ELEMENT, IntegerType.get())),
              NestedField.required(CHUNK_INDEX, "chunk_index", IntegerType.get()),
              NestedField.required(CHUNK_COUNT, "chunk_count", IntegerType.get()));
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
      case BASE_SNAPSHOT_ID:
        this.baseSnapshotId = (Long) v;
        return;
      case NORMALIZED_REF:
        this.normalizedRef = (StagedChangeFile) v;
        return;
      case ASSIGNMENTS:
        this.assignments = (List<Assignment>) v;
        return;
      case OWNER_INDEX:
        this.ownerIndex = (Integer) v;
        return;
      case OWNER_COUNT:
        this.ownerCount = (Integer) v;
        return;
      case IDENTIFIER_FIELD_IDS:
        this.identifierFieldIds = (List<Integer>) v;
        return;
      case CHUNK_INDEX:
        this.chunkIndex = (Integer) v;
        return;
      case CHUNK_COUNT:
        this.chunkCount = (Integer) v;
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
      case BASE_SNAPSHOT_ID:
        return baseSnapshotId;
      case NORMALIZED_REF:
        return normalizedRef;
      case ASSIGNMENTS:
        return assignments;
      case OWNER_INDEX:
        return ownerIndex;
      case OWNER_COUNT:
        return ownerCount;
      case IDENTIFIER_FIELD_IDS:
        return identifierFieldIds;
      case CHUNK_INDEX:
        return chunkIndex;
      case CHUNK_COUNT:
        return chunkCount;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
