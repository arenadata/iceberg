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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SchemaUpdate.AddColumn;
import org.apache.iceberg.connect.data.SchemaUpdate.MakeOptional;
import org.apache.iceberg.connect.data.SchemaUpdate.UpdateType;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");
  private static final String DEBEZIUM_NANO_TIMESTAMP_CLASS = "io.debezium.time.NanoTimestamp";

  static PrimitiveType needsDataTypeUpdate(Type currentIcebergType, Schema valueSchema) {
    if (currentIcebergType.typeId() == TypeID.FLOAT && valueSchema.type() == Schema.Type.FLOAT64) {
      return DoubleType.get();
    }
    if (currentIcebergType.typeId() == TypeID.INTEGER && valueSchema.type() == Schema.Type.INT64) {
      return LongType.get();
    }
    return null;
  }

  static void applySchemaUpdates(Table table, SchemaUpdate.Consumer updates) {
    if (updates == null || updates.empty()) {
      // no updates to apply
      return;
    }

    Tasks.range(1)
        .retry(IcebergSinkConfig.SCHEMA_UPDATE_RETRIES)
        .run(notUsed -> commitSchemaUpdates(table, updates));
  }

  private static void commitSchemaUpdates(Table table, SchemaUpdate.Consumer updates) {
    // get the latest schema in case another process updated it
    table.refresh();

    // filter out columns that have already been added
    List<AddColumn> addColumns =
        updates.addColumns().stream()
            .filter(addCol -> !columnExists(table.schema(), addCol))
            .collect(Collectors.toList());

    // filter out columns that have the updated type
    List<UpdateType> updateTypes =
        updates.updateTypes().stream()
            .filter(updateType -> !typeMatches(table.schema(), updateType))
            .collect(Collectors.toList());

    // filter out columns that have already been made optional
    List<MakeOptional> makeOptionals =
        updates.makeOptionals().stream()
            .filter(makeOptional -> !isOptional(table.schema(), makeOptional))
            .collect(Collectors.toList());

    if (addColumns.isEmpty() && updateTypes.isEmpty() && makeOptionals.isEmpty()) {
      // no updates to apply
      LOG.info("Schema for table {} already up-to-date", table.name());
      return;
    }

    // apply the updates
    UpdateSchema updateSchema = table.updateSchema();
    addColumns.forEach(
        update -> {
          if (update.defaultValue() != null) {
            updateSchema.addColumn(
                update.parentName(), update.name(), update.type(), update.defaultValue());
          } else {
            updateSchema.addColumn(update.parentName(), update.name(), update.type());
          }
        });
    updateTypes.forEach(update -> updateSchema.updateColumn(update.name(), update.type()));
    makeOptionals.forEach(update -> updateSchema.makeColumnOptional(update.name()));
    updateSchema.commit();
    LOG.info("Schema for table {} updated with new columns", table.name());
  }

  private static boolean columnExists(org.apache.iceberg.Schema schema, AddColumn update) {
    return schema.findType(update.key()) != null;
  }

  private static boolean typeMatches(org.apache.iceberg.Schema schema, UpdateType update) {
    Type type = schema.findType(update.name());
    if (type == null) {
      throw new IllegalArgumentException("Invalid column: " + update.name());
    }
    return type.typeId() == update.type().typeId();
  }

  private static boolean isOptional(org.apache.iceberg.Schema schema, MakeOptional update) {
    NestedField field = schema.findField(update.name());
    if (field == null) {
      throw new IllegalArgumentException("Invalid column: " + update.name());
    }
    return field.isOptional();
  }

  static PartitionSpec createPartitionSpec(
      org.apache.iceberg.Schema schema, List<String> partitionBy) {
    if (partitionBy.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    partitionBy.forEach(
        partitionField -> {
          Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
          if (matcher.matches()) {
            String transform = matcher.group(1);
            switch (transform) {
              case "year":
              case "years":
                specBuilder.year(matcher.group(2));
                break;
              case "month":
              case "months":
                specBuilder.month(matcher.group(2));
                break;
              case "day":
              case "days":
                specBuilder.day(matcher.group(2));
                break;
              case "hour":
              case "hours":
                specBuilder.hour(matcher.group(2));
                break;
              case "bucket":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.bucket(args.first(), args.second());
                  break;
                }
              case "truncate":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.truncate(args.first(), args.second());
                  break;
                }
              default:
                throw new UnsupportedOperationException("Unsupported transform: " + transform);
            }
          } else {
            specBuilder.identity(partitionField);
          }
        });
    return specBuilder.build();
  }

  private static Pair<String, Integer> transformArgPair(String argsStr) {
    List<String> parts = Splitter.on(',').splitToList(argsStr);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
    }
    return Pair.of(parts.get(0).trim(), Integer.parseInt(parts.get(1).trim()));
  }

  static Type toIcebergType(Schema valueSchema, IcebergSinkConfig config, String fieldPath) {
    return new SchemaGenerator(config).toIcebergType(valueSchema, fieldPath);
  }

  static Type toIcebergType(Schema valueSchema, IcebergSinkConfig config) {
    return new SchemaGenerator(config).toIcebergType(valueSchema, null);
  }

  static Type inferIcebergType(Object value, IcebergSinkConfig config) {
    return new SchemaGenerator(config).inferIcebergType(value, null);
  }

  static Type inferIcebergType(Object value, IcebergSinkConfig config, String fieldPath) {
    return new SchemaGenerator(config).inferIcebergType(value, fieldPath);
  }

  static class SchemaGenerator {

    private int fieldId = 1;
    private final IcebergSinkConfig config;

    SchemaGenerator(IcebergSinkConfig config) {
      this.config = config;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Type toIcebergType(Schema valueSchema, String fieldPath) {
      if (isVariantField(fieldPath)) {
        return Types.VariantType.get();
      }

      switch (valueSchema.type()) {
        case BOOLEAN:
          return BooleanType.get();
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(valueSchema.name())) {
            int scale = Integer.parseInt(valueSchema.parameters().get(Decimal.SCALE_FIELD));
            return DecimalType.of(38, scale);
          }
          return BinaryType.get();
        case INT8:
        case INT16:
          return IntegerType.get();
        case INT32:
          if (Date.LOGICAL_NAME.equals(valueSchema.name())) {
            return DateType.get();
          } else if (Time.LOGICAL_NAME.equals(valueSchema.name())) {
            return TimeType.get();
          }
          return IntegerType.get();
        case INT64:
          if (Timestamp.LOGICAL_NAME.equals(valueSchema.name())) {
            return TimestampType.withZone();
          } else if (DEBEZIUM_NANO_TIMESTAMP_CLASS.equals(valueSchema.name())) {
            return isTimestampNsField(fieldPath)
                ? Types.TimestampNanoType.withoutZone()
                : Types.TimestampNanoType.withZone();
          }
          return LongType.get();
        case FLOAT32:
          return FloatType.get();
        case FLOAT64:
          return DoubleType.get();
        case ARRAY:
          Type elementType = toIcebergType(valueSchema.valueSchema(), childPath(fieldPath, "[]"));
          if (config.schemaForceOptional() || valueSchema.valueSchema().isOptional()) {
            return ListType.ofOptional(nextId(), elementType);
          } else {
            return ListType.ofRequired(nextId(), elementType);
          }
        case MAP:
          Type keyType = toIcebergType(valueSchema.keySchema(), childPath(fieldPath, "<key>"));
          Type valueType =
              toIcebergType(valueSchema.valueSchema(), childPath(fieldPath, "<value>"));
          if (config.schemaForceOptional() || valueSchema.valueSchema().isOptional()) {
            return MapType.ofOptional(nextId(), nextId(), keyType, valueType);
          } else {
            return MapType.ofRequired(nextId(), nextId(), keyType, valueType);
          }
        case STRUCT:
          List<NestedField> structFields =
              valueSchema.fields().stream()
                  .map(
                      field -> {
                        String child = childPath(fieldPath, field.name());
                        Type fieldType = toIcebergType(field.schema(), child);

                        NestedField.Builder builder =
                            NestedField.builder()
                                .isOptional(
                                    config.schemaForceOptional() || field.schema().isOptional())
                                .withId(nextId())
                                .ofType(fieldType)
                                .withName(field.name());

                        if (config.tableDefaultsEnabled()
                            && field.schema().defaultValue() != null
                            && SchemaUtils.isPrimitiveDefaultSupported(fieldType)) {
                          Literal<?> lit =
                              SchemaUtils.toIcebergLiteral(
                                  fieldType, field.schema().defaultValue());
                          if (lit != null) {
                            builder.withWriteDefault(lit);
                          }
                        }
                        return builder.build();
                      })
                  .collect(Collectors.toList());
          return StructType.of(structFields);
        case STRING:
        default:
          return StringType.get();
      }
    }

    private String childPath(String parent, String child) {
      if (parent == null || parent.isEmpty()) {
        return child;
      }
      return parent + "." + child;
    }

    private boolean isVariantField(String fieldPath) {
      if (fieldPath == null) {
        return false;
      }

      Collection<String> variantFieldPaths = config.schemaVariantFieldPaths();
      return variantFieldPaths != null && variantFieldPaths.contains(fieldPath);
    }

    private boolean isTimestampNsField(String fieldPath) {
      if (fieldPath == null) {
        return false;
      }

      Collection<String> patterns = config.schemaTimestampNsFieldPaths();
      if (patterns == null || patterns.isEmpty()) {
        return false;
      }

      return patterns.stream().anyMatch(pattern -> fieldPathMatches(pattern, fieldPath));
    }

    private boolean fieldPathMatches(String pattern, String fieldPath) {
      if ("*".equals(pattern)) {
        return true;
      }

      if (pattern.contains(".")) {
        return pattern.equals(fieldPath);
      }

      List<String> fieldParts = Splitter.on('.').splitToList(fieldPath);
      return pattern.equals(fieldParts.get(fieldParts.size() - 1));
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Type inferIcebergType(Object value, String fieldPath) {
      if (value == null) {
        return unknownOrNull();
      } else if (isVariantField(fieldPath)) {
        return Types.VariantType.get();
      } else if (value instanceof String) {
        return StringType.get();
      } else if (value instanceof Boolean) {
        return BooleanType.get();
      } else if (value instanceof BigDecimal) {
        BigDecimal bigDecimal = (BigDecimal) value;
        return DecimalType.of(bigDecimal.precision(), bigDecimal.scale());
      } else if (value instanceof Integer || value instanceof Long) {
        return LongType.get();
      } else if (value instanceof Float || value instanceof Double) {
        return DoubleType.get();
      } else if (value instanceof LocalDate) {
        return DateType.get();
      } else if (value instanceof LocalTime) {
        return TimeType.get();
      } else if (value instanceof java.util.Date || value instanceof OffsetDateTime) {
        return TimestampType.withZone();
      } else if (value instanceof LocalDateTime) {
        return TimestampType.withoutZone();
      } else if (value instanceof List) {
        List<?> list = (List<?>) value;
        if (list.isEmpty()) {
          return unknownOrNull();
        }
        Type elementType = inferIcebergType(list.get(0), childPath(fieldPath, "[]"));
        return elementType == null ? unknownOrNull() : ListType.ofOptional(nextId(), elementType);
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        List<NestedField> structFields =
            map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .map(
                    entry -> {
                      String name = entry.getKey().toString();
                      String childPath = childPath(fieldPath, name);
                      Type valueType = inferIcebergType(entry.getValue(), childPath);
                      return valueType == null
                          ? null
                          : NestedField.optional(nextId(), entry.getKey().toString(), valueType);
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (structFields.isEmpty()) {
          return unknownOrNull();
        }
        return StructType.of(structFields);
      } else {
        return unknownOrNull();
      }
    }

    private Type unknownOrNull() {
      return config.evolveUnknownTypeEnabled() ? Types.UnknownType.get() : null;
    }

    private int nextId() {
      return fieldId++;
    }
  }

  static boolean isPrimitiveDefaultSupported(Type type) {
    if (type == null || !type.isPrimitiveType()) {
      return false;
    }
    Type.TypeID id = type.typeId();
    return id != TypeID.VARIANT && id != TypeID.UNKNOWN;
  }

  static Literal<?> toIcebergLiteral(Type icebergType, Object connectDefault) {
    if (connectDefault == null || !isPrimitiveDefaultSupported(icebergType)) {
      return null;
    }
    switch (icebergType.typeId()) {
      case DATE:
        int days = DateTimeUtil.daysFromInstant(((java.util.Date) connectDefault).toInstant());
        return Literal.of(days).to(icebergType);
      case TIME:
        long timeMicros =
            DateTimeUtil.microsFromTime(
                ((java.util.Date) connectDefault)
                    .toInstant()
                    .atOffset(ZoneOffset.UTC)
                    .toLocalTime());
        return Literal.of(timeMicros).to(icebergType);
      case TIMESTAMP:
        long micros = DateTimeUtil.microsFromInstant(((java.util.Date) connectDefault).toInstant());
        return Literal.of(micros).to(icebergType);
      case TIMESTAMP_NANO:
        long nanos = (Long) connectDefault;
        return Literal.of(nanos).to(icebergType);
      default:
        return Expressions.lit(connectDefault).to(icebergType);
    }
  }

  private SchemaUtils() {}
}
