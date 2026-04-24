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
package org.apache.iceberg.connect.data;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopicToTableRecordRouter implements RecordRouter {

  private static final Logger LOG = LoggerFactory.getLogger(TopicToTableRecordRouter.class);

  private final Map<String, String> topicToTableMapping;

  TopicToTableRecordRouter(Map<String, String> topicToTableMapping) {
    this.topicToTableMapping = topicToTableMapping;
  }

  @Override
  public List<String> route(SinkRecord record) {
    String tableName = topicToTableMapping.get(record.topic());
    if (tableName == null) {
      LOG.debug("No table mapping found for topic {}, skipping record", record.topic());
      return Collections.emptyList();
    }

    LOG.debug("Routing record from topic {} to table {}", record.topic(), tableName);
    return Collections.singletonList(tableName);
  }
}
