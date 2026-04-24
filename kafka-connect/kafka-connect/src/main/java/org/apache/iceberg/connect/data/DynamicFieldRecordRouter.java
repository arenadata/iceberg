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
import java.util.Locale;
import org.apache.kafka.connect.sink.SinkRecord;

class DynamicFieldRecordRouter implements RecordRouter {

  private final String routeField;

  DynamicFieldRecordRouter(String routeField) {
    this.routeField = routeField;
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

    return Collections.singletonList(routeValue.toString().toLowerCase(Locale.ROOT));
  }

  @Override
  public boolean ignoreMissingTable() {
    return true;
  }
}
