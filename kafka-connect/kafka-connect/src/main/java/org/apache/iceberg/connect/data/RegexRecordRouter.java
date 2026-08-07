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
import java.util.regex.Pattern;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.connect.sink.SinkRecord;

class RegexRecordRouter implements RecordRouter {

  private final IcebergSinkConfig config;
  private final String routeField;
  private final List<String> tables;

  RegexRecordRouter(IcebergSinkConfig config, String routeField, List<String> tables) {
    this.config = config;
    this.routeField = routeField;
    this.tables = tables;
  }

  @Override
  public List<String> route(SinkRecord record) {
    if (record.value() == null) {
      return Collections.emptyList();
    }

    Object routeValue = RecordUtils.extractFromRecordValue(record.value(), routeField);
    if (routeValue == null) {
      return Collections.emptyList();
    }

    List<String> matchedTables = Lists.newArrayList();
    for (String tableName : tables) {
      Pattern regex = config.tableConfig(tableName).routeRegex();
      if (regex != null && regex.matcher(routeValue.toString()).matches()) {
        matchedTables.add(tableName);
      }
    }

    return matchedTables;
  }
}
