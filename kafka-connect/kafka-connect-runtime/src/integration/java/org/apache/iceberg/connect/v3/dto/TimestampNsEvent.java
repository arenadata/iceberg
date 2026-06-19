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
package org.apache.iceberg.connect.v3.dto;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Stream;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.connect.TestContext;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class TimestampNsEvent extends Event {
  private Object eventTime;
  private TimestampNsFinishTime userStats;
  private TimestampNsEventTime checkStats;

  public TimestampNsEvent(
      long id,
      String username,
      Object eventTime,
      TimestampNsFinishTime userStats,
      TimestampNsEventTime checkStats) {
    super(id, username);
    this.eventTime = eventTime;
    this.userStats = userStats;
    this.checkStats = checkStats;
  }

  public Object eventTime() {
    return eventTime;
  }

  public TimestampNsFinishTime userStats() {
    return userStats;
  }

  public TimestampNsEventTime checkStats() {
    return checkStats;
  }

  public static final org.apache.kafka.connect.data.Schema TIMESTAMP_NS_EVENT_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("username", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("event_time", SchemaBuilder.int64().name("io.debezium.time.NanoTimestamp"))
          .field(
              "user_stats",
              SchemaBuilder.struct()
                  .field(
                      "finish_time", SchemaBuilder.int64().name("io.debezium.time.NanoTimestamp")))
          .field(
              "check_stats",
              SchemaBuilder.struct()
                  .field(
                      "event_time", SchemaBuilder.int64().name("io.debezium.time.NanoTimestamp")));

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value =
          new Struct(TIMESTAMP_NS_EVENT_CONNECT_SCHEMA)
              .put("id", id())
              .put("username", username())
              .put("event_time", eventTime)
              .put(
                  "user_stats",
                  new Struct(TIMESTAMP_NS_EVENT_CONNECT_SCHEMA.field("user_stats").schema())
                      .put("finish_time", userStats.finishTime()))
              .put(
                  "check_stats",
                  new Struct(TIMESTAMP_NS_EVENT_CONNECT_SCHEMA.field("check_stats").schema())
                      .put("event_time", checkStats.eventTime()));
      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(TIMESTAMP_NS_EVENT_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public String castToString() {
    return Stream.of(
            String.valueOf(id()),
            username(),
            String.valueOf(eventTime()),
            format("Record(%s)", userStats().finishTime()),
            format("Record(%s)", checkStats().eventTime()))
        .collect(joining("|"))
        .toString();
  }

  public static class TimestampNsFinishTime {
    private Object finishTime;

    public TimestampNsFinishTime(Object finishTime) {
      this.finishTime = finishTime;
    }

    public Object finishTime() {
      return finishTime;
    }
  }

  public static class TimestampNsEventTime {
    private Object eventTime;

    public TimestampNsEventTime(Object eventTime) {
      this.eventTime = eventTime;
    }

    public Object eventTime() {
      return eventTime;
    }
  }
}
