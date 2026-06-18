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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Stream;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.connect.BaseTestEvent;
import org.apache.iceberg.connect.TestContext;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import static java.util.stream.Collectors.joining;

public class Event extends BaseTestEvent {
  private final String username;

  public static final org.apache.kafka.connect.data.Schema EVENT_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("username", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);

  public Event(long id, String username) {
    super(id);
    this.username = username;
  }

  public String castToString() {
    return Stream.of(String.valueOf(id()), username())
            .collect(joining("|")).toString();
  }

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value = new Struct(EVENT_CONNECT_SCHEMA).put("id", id()).put("username", username);

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(Event.EVENT_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public String username() {
    return username;
  }
}
