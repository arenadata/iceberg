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
package org.apache.iceberg.connect.service;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.connect.v3.dto.UserEvent;
import org.apache.iceberg.data.GenericRecord;

public class KafkaBaseEventsService {
  public static final List<UserEvent> KAFKA_BASE_EVENTS =
      List.of(new UserEvent(1, "Sam"), new UserEvent(2, "Susan"));

  private KafkaBaseEventsService() {}

  public static List<org.apache.iceberg.data.Record> castKafkaBaseEventsToRecords(Schema schema) {
    return KAFKA_BASE_EVENTS.stream()
        .map(
            event -> {
              org.apache.iceberg.data.Record record = GenericRecord.create(schema);
              record.setField("id", event.id());
              record.setField("username", event.username());
              return record;
            })
        .toList();
  }
}
