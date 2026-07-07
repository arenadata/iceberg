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
package org.apache.iceberg.connect.data;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public final class IcebergSchemaUtil {

  private IcebergSchemaUtil() {}

  public static org.apache.kafka.connect.data.Schema toConnectSchema(Schema icebergSchema) {
    if (icebergSchema == null) {
      return null;
    }
    SchemaBuilder builder = SchemaBuilder.struct().name("iceberg.table").optional();
    for (Types.NestedField column : icebergSchema.columns()) {
      builder.field(column.name(), toConnect(column.type(), column.isOptional()));
    }
    return builder.build();
  }

  private static org.apache.kafka.connect.data.Schema toConnect(Type type, boolean optional) {
    SchemaBuilder b;
    switch (type.typeId()) {
      case BOOLEAN:
        b = SchemaBuilder.bool();
        break;
      case INTEGER:
        b = SchemaBuilder.int32();
        break;
      case LONG:
        b = SchemaBuilder.int64();
        break;
      case FLOAT:
        b = SchemaBuilder.float32();
        break;
      case DOUBLE:
        b = SchemaBuilder.float64();
        break;
      case DATE:
        b = Date.builder();
        break;
      case TIME:
        b = Time.builder();
        break;
      case TIMESTAMP:
        b = Timestamp.builder();
        break;
      case STRING:
      case UUID:
        b = SchemaBuilder.string();
        break;
      case FIXED:
      case BINARY:
        b = SchemaBuilder.bytes();
        break;
      case DECIMAL:
        Types.DecimalType dec = (Types.DecimalType) type;
        b =
            Decimal.builder(dec.scale())
                .parameter("connect.decimal.precision", String.valueOf(dec.precision()));
        break;
      case STRUCT:
        b = SchemaBuilder.struct();
        for (Types.NestedField f : type.asStructType().fields()) {
          b.field(f.name(), toConnect(f.type(), f.isOptional()));
        }
        break;
      case LIST:
        Types.ListType list = type.asListType();
        b = SchemaBuilder.array(toConnect(list.elementType(), list.isElementOptional()));
        break;
      case MAP:
        Types.MapType map = type.asMapType();
        b =
            SchemaBuilder.map(
                toConnect(map.keyType(), false), toConnect(map.valueType(), map.isValueOptional()));
        break;
      default:
        b = SchemaBuilder.string();
    }
    return optional ? b.optional().build() : b.required().build();
  }
}
