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
package org.apache.iceberg.connect.routingstrategy.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.connect.BaseTestEvent;
import org.apache.iceberg.connect.TestContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class UserEvent extends BaseTestEvent {
  private final String username;
  private final String table;

  public static final org.apache.kafka.connect.data.Schema USER_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("username", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
          .field("table", org.apache.kafka.connect.data.Schema.STRING_SCHEMA);

  public static final org.apache.iceberg.Schema USER_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "username", Types.StringType.get())),
          ImmutableSet.of(1));

  public static final PartitionSpec USER_SPEC = PartitionSpec.builderFor(USER_SCHEMA).build();

  public UserEvent(long id, String username, String table) {
    super(id);
    this.username = username;
    this.table = table;
  }

  public String castToString() {
    return String.join("|", String.valueOf(id()), username());
  }

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value =
          new Struct(USER_CONNECT_SCHEMA)
              .put("id", id())
              .put("username", username)
              .put("table", table());

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(UserEvent.USER_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public String username() {
    return username;
  }

  public String table() {
    return table;
  }
}
