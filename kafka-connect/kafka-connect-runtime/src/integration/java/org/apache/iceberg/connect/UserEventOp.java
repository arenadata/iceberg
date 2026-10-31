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
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class UserEventOp extends UserEvent {
  private String op;

  public UserEventOp(long id, String username, String op) {
    super(id, username);
    this.op = op;
  }

  public UserEventOp(long id, String username) {
    super(id, username);
  }

  public UserEventOp(long id) {
    super(id);
  }

  public UserEventOp setOp(String op) {
    this.op = op;
    return this;
  }

  public static final org.apache.iceberg.Schema USER_EVENT_OP_TABLE_SCHEMA =
      new org.apache.iceberg.Schema(
          List.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "username", Types.StringType.get()),
              Types.NestedField.optional(3, "op", Types.StringType.get())),
          ImmutableSet.of(1));

  public static final PartitionSpec USER_EVENT_OP_SPEC =
      PartitionSpec.builderFor(USER_EVENT_OP_TABLE_SCHEMA).build();

  public static final Schema USER_EVENT_OP_SCHEMA =
      SchemaBuilder.struct()
          .field("id", Schema.INT64_SCHEMA)
          .field("username", Schema.OPTIONAL_STRING_SCHEMA)
          .field("op", Schema.STRING_SCHEMA);

  public String castToString() {
    return String.join("|", String.valueOf(id()), username(), op());
  }

  public String castForInsert() {
    return format(
        "(%s)",
        String.join(", ", Long.toString(id()), format("'%s'", username()), format("'%s'", op())));
  }

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value =
          new Struct(USER_EVENT_OP_SCHEMA)
              .put("id", id())
              .put("username", username())
              .put("op", op());

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(JsonConverter.class, Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(USER_EVENT_OP_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public String op() {
    return op;
  }
}
