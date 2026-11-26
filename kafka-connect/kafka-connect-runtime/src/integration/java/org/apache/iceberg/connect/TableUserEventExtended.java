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
package org.apache.iceberg.connect;

import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.common.DynMethods;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class TableUserEventExtended extends TableUserEvent {

  private final int year;

  public TableUserEventExtended(long id, String username, String table, int year) {
    super(id, username, table);
    this.year = year;
  }

  public TableUserEventExtended(long id, String username, int year) {
    super(id, username);
    this.year = year;
  }

  public static final org.apache.kafka.connect.data.Schema
      TABLE_USER_EVENT_EXTENDED_CONNECT_SCHEMA =
          SchemaBuilder.struct()
              .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
              .field("username", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
              .field("table", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
              .field("year", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA);

  public String castToString() {
    String tableUserEventString = String.join("|", String.valueOf(id()), username());
    return table() == null
        ? format("%s|%s", tableUserEventString, year())
        : format("%s|%s|%s", tableUserEventString, table(), year());
  }

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value;
      value =
          new Struct(TABLE_USER_EVENT_EXTENDED_CONNECT_SCHEMA)
              .put("id", id())
              .put("username", username())
              .put("table", table())
              .put("year", year());

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(TABLE_USER_EVENT_EXTENDED_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public int year() {
    return year;
  }
}
