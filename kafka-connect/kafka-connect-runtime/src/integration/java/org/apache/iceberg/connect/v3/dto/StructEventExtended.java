/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.connect.v3.dto;

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

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.connect.v3.dto.EventExtended.INFO_WRITE_DEFAULT;

public class StructEventExtended extends Event {
    private final InfoExtended info;

    public StructEventExtended(long id, String username, InfoExtended info) {
        super(id, username);
        this.info = info;
    }

    public InfoExtended info() {
        return info;
    }

    public static final org.apache.kafka.connect.data.Schema EVENT_STRUCT_EXTENDED_CONNECT_SCHEMA =
            SchemaBuilder.struct()
                    .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                    .field("username", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                    .field(
                            "info",
                            SchemaBuilder.struct()
                                    .field("age", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                                    .field("status", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA));


    @Override
    protected String serialize(boolean useSchema) {
        try {
            Struct value =
                    new Struct(EVENT_STRUCT_EXTENDED_CONNECT_SCHEMA)
                            .put("id", id())
                            .put("username", username())
                            .put(
                                    "info",
                                    new Struct(EVENT_STRUCT_EXTENDED_CONNECT_SCHEMA.field("info").schema())
                                            .put("age", info.age())
                                            .put("status", info().status()));

            String convertMethod =
                    useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
            JsonNode json =
                    DynMethods.builder(convertMethod)
                            .hiddenImpl(
                                    JsonConverter.class, org.apache.kafka.connect.data.Schema.class, Object.class)
                            .build(JSON_CONVERTER)
                            .invoke(EVENT_STRUCT_EXTENDED_CONNECT_SCHEMA, value);
            return TestContext.MAPPER.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public String castToString() {
        return Stream.of(String.valueOf(id()), username(), format("Record(%s, %s)", info().age(), info().status()))
                .collect(joining("|")).toString();
    }

    public static class InfoExtended extends StructEvent.Info {
        private final String status;

        public InfoExtended(int age, String status) {
            super(age);
            this.status = status;
        }

        public String status() {
            return status;
        }

    }

}
