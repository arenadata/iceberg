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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * Element assigning a set of data files to one task for rewriting.
 *
 * <p>An empty file list is a legal assignment: every active task is assigned, so that a task with
 * nothing to do is still distinguishable from a task that never answered.
 *
 * <p>{@code expectedPartitions} records what the task owned when the assignment was planned. A task
 * id is stable across restarts of the same task index, so without this a restarted task would
 * happily execute work planned for its predecessor.
 */
public class Assignment implements IndexedRecord {

  private String taskId;
  private List<TopicPartitionRef> expectedPartitions;
  private List<FileScanTaskDescriptor> files;
  private final Schema avroSchema;

  static final int TASK_ID = 10_910;
  static final int EXPECTED_PARTITIONS = 10_911;
  static final int EXPECTED_PARTITIONS_ELEMENT = 10_912;
  static final int FILES = 10_913;
  static final int FILES_ELEMENT = 10_914;

  public static StructType icebergSchema(StructType partitionType) {
    return StructType.of(
        NestedField.required(TASK_ID, "task_id", StringType.get()),
        NestedField.required(
            EXPECTED_PARTITIONS,
            "expected_partitions",
            ListType.ofRequired(EXPECTED_PARTITIONS_ELEMENT, TopicPartitionRef.ICEBERG_SCHEMA)),
        NestedField.required(
            FILES,
            "files",
            ListType.ofRequired(
                FILES_ELEMENT, FileScanTaskDescriptor.icebergSchema(partitionType))));
  }

  // see FileScanTaskDescriptor: the partition type does not reach this element's own fields
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(icebergSchema(StructType.of()), Assignment.class);

  // Used by Avro reflection to instantiate this class when reading events
  public Assignment(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Assignment(
      String taskId,
      List<TopicPartitionRef> expectedPartitions,
      List<FileScanTaskDescriptor> files) {
    Preconditions.checkNotNull(taskId, "Task ID cannot be null");
    this.taskId = taskId;
    this.expectedPartitions = expectedPartitions;
    this.files = files;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String taskId() {
    return taskId;
  }

  public List<TopicPartitionRef> expectedPartitions() {
    return expectedPartitions;
  }

  public List<FileScanTaskDescriptor> files() {
    return files;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TASK_ID:
        this.taskId = v == null ? null : v.toString();
        return;
      case EXPECTED_PARTITIONS:
        this.expectedPartitions = (List<TopicPartitionRef>) v;
        return;
      case FILES:
        this.files = (List<FileScanTaskDescriptor>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TASK_ID:
        return taskId;
      case EXPECTED_PARTITIONS:
        return expectedPartitions;
      case FILES:
        return files;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
