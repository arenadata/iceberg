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
import static org.apache.iceberg.connect.v3.dto.EventExtended.INFO_WRITE_DEFAULT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.connect.TestContext;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class StructEvent extends Event {
  private final Info info;

  public StructEvent(long id, String username, Info info) {
    super(id, username);
    this.info = info;
  }

  public Info info() {
    return info;
  }

  public static final org.apache.kafka.connect.data.Schema EVENT_STRUCT_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("username", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field(
              "info",
              SchemaBuilder.struct()
                  .field("age", org.apache.kafka.connect.data.Schema.INT32_SCHEMA));

  public static final Schema EVENT_STRUCT_TABLE_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "username", Types.StringType.get()),
              Types.NestedField.optional(
                  3,
                  "info",
                  Types.StructType.of(
                      Types.NestedField.optional(4, "age", Types.IntegerType.get()),
                      Types.NestedField.optional("status")
                          .withId(5)
                          .ofType(Types.StringType.get())
                          .withInitialDefault(Literal.of("non-active"))
                          .withWriteDefault(Literal.of(INFO_WRITE_DEFAULT))
                          .build()))),
          ImmutableSet.of(1));

  public static final PartitionSpec TEST_STRUCT_SPEC =
      PartitionSpec.builderFor(EVENT_STRUCT_TABLE_SCHEMA).build();

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value =
          new Struct(EVENT_STRUCT_CONNECT_SCHEMA)
              .put("id", id())
              .put("username", username())
              .put(
                  "info",
                  new Struct(EVENT_STRUCT_CONNECT_SCHEMA.field("info").schema())
                      .put("age", info.age()));

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(
                  JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(StructEvent.EVENT_STRUCT_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String castToString() {
    return Stream.of(String.valueOf(id()), username(), format("Record(%s)", info().age))
        .collect(joining("|"))
        .toString();
  }

  public static class Info {
    private final int age;

    public Info(int age) {
      this.age = age;
    }

    public int age() {
      return age;
    }
  }
}
