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
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * Element naming a topic partition, without an offset.
 *
 * <p>Used to record the partitions a task owned when work was assigned to it, so that a rebalance
 * invalidates the assignment on its own.
 */
public class TopicPartitionRef implements IndexedRecord {

  private String topic;
  private Integer partition;
  private final Schema avroSchema;

  static final int TOPIC = 10_920;
  static final int PARTITION = 10_921;

  public static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(TOPIC, "topic", StringType.get()),
          NestedField.required(PARTITION, "partition", IntegerType.get()));
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(ICEBERG_SCHEMA, TopicPartitionRef.class);

  // Used by Avro reflection to instantiate this class when reading events
  public TopicPartitionRef(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TopicPartitionRef(String topic, int partition) {
    Preconditions.checkNotNull(topic, "Topic cannot be null");
    this.topic = topic;
    this.partition = partition;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TOPIC:
        this.topic = v == null ? null : v.toString();
        return;
      case PARTITION:
        this.partition = (Integer) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TOPIC:
        return topic;
      case PARTITION:
        return partition;
      default:
        // a newer version's field: the reader gets it for reuse before put() ignores it
        return null;
    }
  }
}
