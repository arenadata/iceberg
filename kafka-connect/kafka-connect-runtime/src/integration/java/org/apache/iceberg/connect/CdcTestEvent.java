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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Date;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.Record;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;

/**
 * A change event for {@link TestEvent#TEST_SCHEMA} tables. Every field except {@code id} is
 * optional, so a key-only delete carries just the key and {@code op}.
 */
public class CdcTestEvent extends BaseTestEvent {

  public static final Schema CDC_CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("id", Schema.INT64_SCHEMA)
          .field("type", Schema.OPTIONAL_STRING_SCHEMA)
          .field("ts", Timestamp.builder().optional().build())
          .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
          .field("op", Schema.OPTIONAL_STRING_SCHEMA);

  private final String type;
  private final Instant ts;
  private final String payload;
  private final String op;

  private CdcTestEvent(long id, String type, Instant ts, String payload, String op) {
    super(id);
    this.type = type;
    this.ts = ts;
    this.payload = payload;
    this.op = op;
  }

  public static CdcTestEvent insert(long id, String type, Instant ts, String payload) {
    return new CdcTestEvent(id, type, ts, payload, "c");
  }

  public static CdcTestEvent update(long id, String type, Instant ts, String payload) {
    return new CdcTestEvent(id, type, ts, payload, "u");
  }

  public static CdcTestEvent delete(CdcTestEvent row) {
    return new CdcTestEvent(row.id(), row.type, row.ts, row.payload, "d");
  }

  public static CdcTestEvent deleteKey(long id) {
    return new CdcTestEvent(id, null, null, null, "d");
  }

  public String row() {
    return row(id(), type, ts, payload);
  }

  public static String row(Record record) {
    OffsetDateTime recordTs = (OffsetDateTime) record.getField("ts");
    return row(
        (Long) record.getField("id"),
        (String) record.getField("type"),
        recordTs == null ? null : recordTs.toInstant(),
        (String) record.getField("payload"));
  }

  private static String row(long id, String type, Instant ts, String payload) {
    return id + "|" + type + "|" + ts + "|" + payload;
  }

  @Override
  protected String serialize(boolean useSchema) {
    try {
      Struct value =
          new Struct(CDC_CONNECT_SCHEMA)
              .put("id", id())
              .put("type", type)
              .put("ts", ts == null ? null : Date.from(ts))
              .put("payload", payload)
              .put("op", op);

      String convertMethod =
          useSchema ? "convertToJsonWithEnvelope" : "convertToJsonWithoutEnvelope";
      JsonNode json =
          DynMethods.builder(convertMethod)
              .hiddenImpl(JsonConverter.class, Schema.class, Object.class)
              .build(JSON_CONVERTER)
              .invoke(CDC_CONNECT_SCHEMA, value);
      return TestContext.MAPPER.writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
