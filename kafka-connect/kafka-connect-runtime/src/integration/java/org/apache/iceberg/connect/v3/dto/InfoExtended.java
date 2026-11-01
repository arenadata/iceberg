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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class InfoExtended extends Info {

  private final Schema infoSchema =
      SchemaBuilder.struct()
          .field("age", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .field("status", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private final String status;

  public String status() {
    return status;
  }

  public InfoExtended(int age, String status) {
    super(age);
    this.status = status;
  }

  @Override
  public Schema schema() {
    return infoSchema;
  }

  @Override
  public Struct struct() {
    return new Struct(schema()).put("age", age()).put("status", status);
  }

  @Override
  public String castToString() {
    return format("Record(%s, %s)", age(), status);
  }
}
