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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a worker that indicates it has finished sending all
 * data for a commit request.
 */
public class DataComplete implements Payload {

  private UUID commitId;
  private List<TopicPartitionOffset> assignments;
  private String taskId;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_100;
  static final int ASSIGNMENTS = 10_101;
  static final int ASSIGNMENTS_ELEMENT = 10_102;
  static final int TASK_ID = 10_103;

  // the schema from before task_id: an older reader throws on any field it does not know, because
  // the Avro reader calls get() on the record for every field of the writer's schema
  private static final StructType ICEBERG_SCHEMA_WITHOUT_TASK_ID =
      StructType.of(
          NestedField.required(COMMIT_ID, "commit_id", UUIDType.get()),
          NestedField.optional(
              ASSIGNMENTS,
              "assignments",
              ListType.ofRequired(ASSIGNMENTS_ELEMENT, TopicPartitionOffset.ICEBERG_SCHEMA)));
  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          ImmutableList.<NestedField>builder()
              .addAll(ICEBERG_SCHEMA_WITHOUT_TASK_ID.fields())
              .add(NestedField.optional(TASK_ID, "task_id", StringType.get()))
              .build());
  private static final Schema AVRO_SCHEMA_WITHOUT_TASK_ID =
      AvroUtil.convert(ICEBERG_SCHEMA_WITHOUT_TASK_ID, DataComplete.class);
  private static final Schema AVRO_SCHEMA = AvroUtil.convert(ICEBERG_SCHEMA, DataComplete.class);

  // Used by Avro reflection to instantiate this class when reading events
  public DataComplete(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataComplete(UUID commitId, List<TopicPartitionOffset> assignments) {
    this(commitId, assignments, null);
  }

  /**
   * @param taskId the sending task, which copy-on-write needs to list rewrite executors, or null.
   *     Without it the event is written in the schema without the field, which a connector one
   *     version behind still reads; with it, that connector cannot decode the event.
   */
  public DataComplete(UUID commitId, List<TopicPartitionOffset> assignments, String taskId) {
    Preconditions.checkNotNull(commitId, "Commit ID cannot be null");
    this.commitId = commitId;
    this.assignments = assignments;
    this.taskId = taskId;
    this.avroSchema = taskId == null ? AVRO_SCHEMA_WITHOUT_TASK_ID : AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.DATA_COMPLETE;
  }

  public UUID commitId() {
    return commitId;
  }

  public List<TopicPartitionOffset> assignments() {
    return assignments;
  }

  public String taskId() {
    return taskId;
  }

  @Override
  public StructType writeSchema() {
    return taskId == null ? ICEBERG_SCHEMA_WITHOUT_TASK_ID : ICEBERG_SCHEMA;
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
      case ASSIGNMENTS:
        this.assignments = (List<TopicPartitionOffset>) v;
        return;
      case TASK_ID:
        this.taskId = v == null ? null : v.toString();
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
      case ASSIGNMENTS:
        return assignments;
      case TASK_ID:
        return taskId;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
