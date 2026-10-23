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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSketchValues {
  private static final LocalDate DATE = LocalDate.of(2024, 3, 15);
  private static final LocalTime TIME = LocalTime.of(13, 45, 12, 123_456_789);
  private static final LocalDateTime TS = LocalDateTime.of(2024, 3, 15, 13, 45, 12, 123_456_789);
  private static final OffsetDateTime TSTZ =
      OffsetDateTime.of(2024, 3, 15, 13, 45, 12, 123_456_789, ZoneOffset.ofHours(3));
  private static final UUID ID = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");

  private static Arguments arg(Type.PrimitiveType type, Object record, Object internal) {
    return Arguments.of(type.toString(), type, record, internal);
  }

  static Stream<Arguments> values() {
    byte[] bytes = new byte[] {1, 2, 3, 4, 5};
    return Stream.of(
        arg(Types.BooleanType.get(), true, true),
        arg(Types.BooleanType.get(), false, false),
        arg(Types.IntegerType.get(), 42, 42),
        arg(Types.LongType.get(), 1L << 40, 1L << 40),
        arg(Types.FloatType.get(), 1.5f, 1.5f),
        arg(Types.DoubleType.get(), -2.25d, -2.25d),
        arg(Types.DecimalType.of(10, 2), new BigDecimal("-123.45"), new BigDecimal("-123.45")),
        arg(
            Types.DecimalType.of(38, 0),
            new BigDecimal("123456789012345678"),
            new BigDecimal("123456789012345678")),
        arg(Types.DateType.get(), DATE, DateTimeUtil.daysFromDate(DATE)),
        arg(Types.DateType.get(), 19797, 19797),
        arg(Types.TimeType.get(), TIME, DateTimeUtil.microsFromTime(TIME)),
        arg(Types.TimeType.get(), 49_512_123_456L, 49_512_123_456L),
        arg(Types.TimestampType.withoutZone(), TS, DateTimeUtil.microsFromTimestamp(TS)),
        arg(Types.TimestampType.withoutZone(), 1_710_510_312_123_456L, 1_710_510_312_123_456L),
        arg(Types.TimestampType.withZone(), TSTZ, DateTimeUtil.microsFromTimestamptz(TSTZ)),
        arg(Types.TimestampType.withZone(), 1_710_510_312_123_456L, 1_710_510_312_123_456L),
        arg(Types.TimestampNanoType.withoutZone(), TS, DateTimeUtil.nanosFromTimestamp(TS)),
        arg(
            Types.TimestampNanoType.withoutZone(),
            1_710_510_312_123_456_789L,
            1_710_510_312_123_456_789L),
        arg(Types.TimestampNanoType.withZone(), TSTZ, DateTimeUtil.nanosFromTimestamptz(TSTZ)),
        arg(
            Types.TimestampNanoType.withZone(),
            1_710_510_312_123_456_789L,
            1_710_510_312_123_456_789L),
        arg(Types.StringType.get(), "héllo, мир", "héllo, мир"),
        arg(Types.StringType.get(), new StringBuilder("abc"), "abc"),
        arg(Types.UUIDType.get(), ID, ID),
        arg(Types.UUIDType.get(), UUIDUtil.convert(ID), ID),
        arg(Types.BinaryType.get(), bytes, ByteBuffer.wrap(bytes)),
        arg(Types.BinaryType.get(), ByteBuffer.wrap(bytes), ByteBuffer.wrap(bytes)),
        arg(Types.FixedType.ofLength(5), bytes, ByteBuffer.wrap(bytes)),
        arg(Types.FixedType.ofLength(5), ByteBuffer.wrap(bytes), ByteBuffer.wrap(bytes)));
  }

  @ParameterizedTest(name = "{0}: {2}")
  @MethodSource("values")
  public void bytesEqualConversionsOfInternalValue(
      String name, Type.PrimitiveType type, Object record, Object internal) {
    ByteBuffer expected = Conversions.toByteBuffer(type, internal);

    ByteBuffer actual = SketchValues.toByteBuffer(type, record);

    assertThat(actual).isNotNull();
    assertThat(actual).isEqualTo(expected);
  }

  @ParameterizedTest(name = "{0}: {2}")
  @MethodSource("values")
  public void sketchIsByteEqualToSparkStyleReferenceAndUnionDeduplicates(
      String name, Type.PrimitiveType type, Object record, Object internal) {
    // Spark's ThetaSketchAgg hashes Conversions.toByteBuffer(type, value) through
    // UpdateSketch.update(ByteBuffer); the worker copies the bytes and uses update(byte[])
    UpdateSketch worker = SketchValues.newSketch(12);
    SketchValues.update(worker, SketchValues.toByteBuffer(type, record));
    UpdateSketch reference = UpdateSketch.builder().setNominalEntries(1 << 12).build();
    reference.update(Conversions.toByteBuffer(type, internal));

    assertThat(worker.compact().toByteArray()).isEqualTo(reference.compact().toByteArray());

    Union union = SetOperation.builder().buildUnion();
    union.union(worker.compact());
    union.union(reference.compact());
    assertThat(union.getResult().getEstimate()).isEqualTo(1.0d);
  }

  @Test
  public void uuidAsUuidAndAsByteArrayGiveSixteenBytes() {
    ByteBuffer fromUuid = SketchValues.toByteBuffer(Types.UUIDType.get(), ID);
    ByteBuffer fromBytes = SketchValues.toByteBuffer(Types.UUIDType.get(), UUIDUtil.convert(ID));

    assertThat(fromUuid.remaining()).isEqualTo(16);
    assertThat(fromBytes).isEqualTo(fromUuid);
  }

  @Test
  public void uuidByteArrayOfWrongLengthIsRejected() {
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.UUIDType.get(), new byte[15]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class [B for type uuid");
  }

  private static Stream<String> unpairedSurrogates() {
    char high = (char) 0xD800;
    char low = (char) 0xDC00;
    return Stream.of("a" + high + "b", String.valueOf(low), "tail" + high);
  }

  @ParameterizedTest
  @MethodSource("unpairedSurrogates")
  public void stringWithUnpairedSurrogateIsEncodedLikeDataFileWriters(String value) {
    // the writers store such a string with String#getBytes, which replaces the surrogate
    ByteBuffer[] actual = new ByteBuffer[1];

    assertThatCode(() -> actual[0] = SketchValues.toByteBuffer(Types.StringType.get(), value))
        .doesNotThrowAnyException();

    assertThat(actual[0]).isEqualTo(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void nullYieldsNoBufferAndIsNotAddedToSketch() {
    UpdateSketch sketch = SketchValues.newSketch(12);

    ByteBuffer buffer = SketchValues.toByteBuffer(Types.StringType.get(), null);
    SketchValues.update(sketch, buffer);

    assertThat(buffer).isNull();
    assertThat(sketch.getRetainedEntries()).isZero();
    assertThat(sketch.getEstimate()).isZero();
  }

  @Test
  public void unexpectedClassIsRejected() {
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.IntegerType.get(), "42"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class java.lang.String for type int");
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.LongType.get(), 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class java.lang.Integer for type long");
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.StringType.get(), 5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class java.lang.Integer for type string");
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.DateType.get(), TS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class java.time.LocalDateTime for type date");
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.TimestampType.withZone(), TS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Unexpected value class java.time.LocalDateTime for type timestamptz");
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.UUIDType.get(), "abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class java.lang.String for type uuid");
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.BinaryType.get(), "abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unexpected value class java.lang.String for type binary");
  }

  @Test
  public void unsuitableTypesAreRejected() {
    assertThatThrownBy(() -> SketchValues.toByteBuffer(Types.UnknownType.get(), "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Type is not supported for sketches: unknown");
    assertThatThrownBy(
            () -> SketchValues.toByteBuffer(Types.GeometryType.crs84(), ByteBuffer.allocate(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Type is not supported for sketches: geometry");
  }

  @Test
  public void uuidSketchIsByteEqualToReference() {
    UUID[] ids = {ID, UUID.randomUUID(), new UUID(0L, 0L), UUID.randomUUID()};

    UpdateSketch asUuid = SketchValues.newSketch(14);
    UpdateSketch asBytes = SketchValues.newSketch(14);
    UpdateSketch reference = UpdateSketch.builder().setNominalEntries(1 << 14).build();
    for (UUID id : ids) {
      SketchValues.update(asUuid, SketchValues.toByteBuffer(Types.UUIDType.get(), id));
      SketchValues.update(
          asBytes, SketchValues.toByteBuffer(Types.UUIDType.get(), UUIDUtil.convert(id)));
      ByteBuffer expected = Conversions.toByteBuffer(Types.UUIDType.get(), id);
      byte[] raw = new byte[expected.remaining()];
      expected.duplicate().get(raw);
      reference.update(raw);
    }

    byte[] expected = reference.compact().toByteArray();
    assertThat(asUuid.compact().toByteArray()).isEqualTo(expected);
    assertThat(asBytes.compact().toByteArray()).isEqualTo(expected);
    assertThat(asUuid.getEstimate()).isEqualTo(4.0d);
  }

  @Test
  public void newSketchUsesQuickSelectAndDefaultSeed() {
    UpdateSketch sketch = SketchValues.newSketch(10);

    assertThat(sketch.getFamily()).isEqualTo(Family.QUICKSELECT);
    for (int i = 0; i < 100_000; i++) {
      sketch.update(i);
    }
    // nominal entries 2^10: the sketch is in estimation mode and holds at most 2 * 2^10 entries
    assertThat(sketch.isEstimationMode()).isTrue();
    assertThat(sketch.getRetainedEntries(false)).isLessThanOrEqualTo(2 * 1024);
    assertThat(sketch.getLgNomLongs()).isEqualTo(10);
  }

  @Test
  public void sketchMatchesSketchOfSameBytesAndDeduplicates() {
    UpdateSketch sketch = SketchValues.newSketch(12);
    UpdateSketch reference = UpdateSketch.builder().setNominalEntries(1 << 12).build();
    for (String value : new String[] {"a", "b", "a", "c"}) {
      SketchValues.update(sketch, SketchValues.toByteBuffer(Types.StringType.get(), value));
      reference.update(value.getBytes(StandardCharsets.UTF_8));
    }

    CompactSketch compact = sketch.compact();
    assertThat(compact.getEstimate()).isEqualTo(3.0d);
    assertThat(compact.toByteArray()).isEqualTo(reference.compact().toByteArray());
  }

  @ParameterizedTest
  @MethodSource("binaryTypes")
  public void updateKeepsPositionAndLimitOfBinaryAndFixed(Type.PrimitiveType type) {
    byte[] backing = new byte[] {9, 9, 1, 2, 3, 4, 9, 9};
    ByteBuffer value = ByteBuffer.wrap(backing);
    value.position(2).limit(6);

    ByteBuffer buffer = SketchValues.toByteBuffer(type, value);
    UpdateSketch sketch = SketchValues.newSketch(12);
    SketchValues.update(sketch, buffer);

    assertThat(buffer.position()).isEqualTo(2);
    assertThat(buffer.limit()).isEqualTo(6);
    assertThat(value.position()).isEqualTo(2);
    assertThat(value.limit()).isEqualTo(6);
    UpdateSketch reference = UpdateSketch.builder().setNominalEntries(1 << 12).build();
    reference.update(new byte[] {1, 2, 3, 4});
    assertThat(sketch.compact().toByteArray()).isEqualTo(reference.compact().toByteArray());
  }

  static Stream<Type.PrimitiveType> binaryTypes() {
    return Stream.of(Types.BinaryType.get(), Types.FixedType.ofLength(4));
  }

  @Test
  public void updateReadsSliceWithArrayOffsetAbsolutely() {
    byte[] backing = new byte[] {9, 9, 1, 2, 3, 4, 9, 9};
    ByteBuffer slice = ByteBuffer.wrap(backing, 2, 4).slice();
    slice.position(1);

    UpdateSketch sketch = SketchValues.newSketch(12);
    SketchValues.update(sketch, slice);

    assertThat(slice.position()).isEqualTo(1);
    assertThat(slice.limit()).isEqualTo(4);
    UpdateSketch reference = UpdateSketch.builder().setNominalEntries(1 << 12).build();
    reference.update(new byte[] {2, 3, 4});
    assertThat(sketch.compact().toByteArray()).isEqualTo(reference.compact().toByteArray());
  }
}
