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

import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.common.config.ConfigException;

class RecordRouterFactory {

  private RecordRouterFactory() {}

  static RecordRouter create(IcebergSinkConfig config) {
    switch (config.routingStrategy()) {
      case DYNAMIC_FIELD:
        return new DynamicFieldRecordRouter(config.tablesRouteField());
      case ALL_TABLES:
        return new AllTablesRecordRouter(config.tables());
      case REGEX:
        return new RegexRecordRouter(config, config.tablesRouteField(), config.tables());
      case TOPIC_TO_TABLE:
        return new TopicToTableRecordRouter(config.topicToTableMapping());
      default:
        throw new ConfigException(
            "Unsupported record routing strategy: " + config.routingStrategy());
    }
  }
}
