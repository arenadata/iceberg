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
package org.apache.iceberg.connect.data.copyonwrite;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * {@code (_topic, _partition, _offset)} of one staged change record.
 *
 * <p>The only order that survives across workers: position within a staged file orders one worker's
 * own records, and the control topic orders batches, not individual records. Two records for the
 * same identifier key are folded in this order regardless of which staged file, or which position
 * within the coordinator's file iteration, they were read from.
 */
final class OrderKey implements Comparable<OrderKey> {

  private final String topic;
  private final int partition;
  private final long offset;

  OrderKey(String topic, int partition, long offset) {
    this.topic = Preconditions.checkNotNull(topic, "Topic cannot be null");
    this.partition = partition;
    this.offset = offset;
  }

  @Override
  public int compareTo(OrderKey other) {
    int cmp = topic.compareTo(other.topic);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Integer.compare(partition, other.partition);
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(offset, other.offset);
  }
}
