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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

/**
 * Turns values of a {@code Record} into the bytes a theta sketch hashes, and builds and updates the
 * sketches, so that the sketches of the same rows are equal wherever they are built and can be
 * united with those of Spark and Trino.
 */
public class SketchValues {
  private static final int UUID_LENGTH = 16;

  private SketchValues() {}

  /**
   * Returns the bytes of {@link Conversions#toByteBuffer(Type, Object)} for the internal
   * representation of the value, or {@code null} for a {@code null} value. For binary and fixed the
   * result is the very buffer of the value (no copy), so it must be read absolutely.
   *
   * @throws IllegalArgumentException if the class of the value does not fit the type, or the type
   *     is not supported for sketches
   */
  public static ByteBuffer toByteBuffer(Type.PrimitiveType type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
        return Conversions.toByteBuffer(type, cast(type, value, Boolean.class));
      case INTEGER:
        return Conversions.toByteBuffer(type, cast(type, value, Integer.class));
      case LONG:
        return Conversions.toByteBuffer(type, cast(type, value, Long.class));
      case FLOAT:
        return Conversions.toByteBuffer(type, cast(type, value, Float.class));
      case DOUBLE:
        return Conversions.toByteBuffer(type, cast(type, value, Double.class));
      case DECIMAL:
        return Conversions.toByteBuffer(type, cast(type, value, BigDecimal.class));
      case DATE:
        return Conversions.toByteBuffer(type, days(type, value));
      case TIME:
        return Conversions.toByteBuffer(type, time(type, value));
      case TIMESTAMP:
        return Conversions.toByteBuffer(type, timestamp(type, value, false));
      case TIMESTAMP_NANO:
        return Conversions.toByteBuffer(type, timestamp(type, value, true));
      case STRING:
        return string(type, value);
      case UUID:
        return uuid(value);
      case FIXED:
      case BINARY:
        return binary(type, value);
      default:
        throw new IllegalArgumentException("Type is not supported for sketches: " + type);
    }
  }

  /** A theta sketch with the family and seed of Spark and Trino, {@code 2^lgK} nominal entries. */
  public static UpdateSketch newSketch(int lgK) {
    return UpdateSketch.builder().setNominalEntries(1 << lgK).build();
  }

  /** Adds the remaining bytes of the buffer without moving its position; skips {@code null}. */
  public static void update(UpdateSketch sketch, ByteBuffer bytes) {
    if (bytes == null) {
      return;
    }

    byte[] copy = new byte[bytes.remaining()];
    bytes.duplicate().get(copy);
    sketch.update(copy);
  }

  private static <T> T cast(Type type, Object value, Class<T> expected) {
    if (!expected.isInstance(value)) {
      throw unexpected(type, value);
    }

    return expected.cast(value);
  }

  private static IllegalArgumentException unexpected(Type type, Object value) {
    return new IllegalArgumentException(
        String.format("Unexpected value class %s for type %s", value.getClass().getName(), type));
  }

  private static Integer days(Type type, Object value) {
    if (value instanceof LocalDate) {
      return DateTimeUtil.daysFromDate((LocalDate) value);
    }

    return cast(type, value, Integer.class);
  }

  private static Long time(Type type, Object value) {
    if (value instanceof LocalTime) {
      return DateTimeUtil.microsFromTime((LocalTime) value);
    }

    return cast(type, value, Long.class);
  }

  private static Long timestamp(Type type, Object value, boolean nanos) {
    boolean adjustToUtc =
        nanos
            ? ((Types.TimestampNanoType) type).shouldAdjustToUTC()
            : ((Types.TimestampType) type).shouldAdjustToUTC();

    if (value instanceof Long) {
      return (Long) value;
    } else if (!adjustToUtc && value instanceof LocalDateTime) {
      LocalDateTime dateTime = (LocalDateTime) value;
      return nanos
          ? DateTimeUtil.nanosFromTimestamp(dateTime)
          : DateTimeUtil.microsFromTimestamp(dateTime);
    } else if (adjustToUtc && value instanceof OffsetDateTime) {
      OffsetDateTime dateTime = (OffsetDateTime) value;
      return nanos
          ? DateTimeUtil.nanosFromTimestamptz(dateTime)
          : DateTimeUtil.microsFromTimestamptz(dateTime);
    }

    throw unexpected(type, value);
  }

  private static ByteBuffer string(Type type, Object value) {
    // Conversions throws on an unpaired surrogate, while the data file writers replace it
    String string = cast(type, value, CharSequence.class).toString();
    return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
  }

  private static ByteBuffer uuid(Object value) {
    if (value instanceof UUID) {
      return UUIDUtil.convertToByteBuffer((UUID) value);
    } else if (value instanceof byte[] && ((byte[]) value).length == UUID_LENGTH) {
      return ByteBuffer.wrap((byte[]) value);
    }

    throw unexpected(Types.UUIDType.get(), value);
  }

  private static ByteBuffer binary(Type type, Object value) {
    if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    }

    throw unexpected(type, value);
  }
}
