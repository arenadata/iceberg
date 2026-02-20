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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

public class TestVariantConverter {

  private static VariantConverter newConverter() {
    return new VariantConverter(10_000, Duration.ofMinutes(10));
  }

  @Test
  public void testPrimitiveConversions() {
    VariantConverter converter = newConverter();

    VariantValue vNull = converter.toVariantValue(null);
    assertThat(vNull).isNotNull();
    assertThat(vNull.toString()).isEqualTo(Variants.ofNull().toString());

    VariantValue vBool = converter.toVariantValue(true);
    assertThat(vBool.toString()).isEqualTo(Variants.of(true).toString());

    VariantValue vInt = converter.toVariantValue(12);
    assertThat(vInt.toString()).isEqualTo(Variants.of(12).toString());

    VariantValue vLong = converter.toVariantValue(12L);
    assertThat(vLong.toString()).isEqualTo(Variants.of(12L).toString());

    VariantValue vFloat = converter.toVariantValue(1.5f);
    assertThat(vFloat.toString()).isEqualTo(Variants.of(1.5f).toString());

    VariantValue vDouble = converter.toVariantValue(1.5d);
    assertThat(vDouble.toString()).isEqualTo(Variants.of(1.5d).toString());

    BigDecimal dec = new BigDecimal("123.45");
    VariantValue vDec = converter.toVariantValue(dec);
    assertThat(vDec.toString()).isEqualTo(Variants.of(dec).toString());

    VariantValue vStr = converter.toVariantValue("x");
    assertThat(vStr.toString()).isEqualTo(Variants.of("x").toString());
  }

  @Test
  public void testMapConversionProducesVariantObject() {
    VariantConverter converter = newConverter();

    Map<String, Object> payload = Maps.newHashMap();
    payload.put("a", 1);
    payload.put("b", true);
    payload.put("c", "v");

    Variant variant = converter.fromMap(payload);
    assertThat(variant).isNotNull();
    assertThat(variant.metadata()).isNotNull();
    assertThat(variant.value()).isNotNull();
    assertThat(variant.value()).isInstanceOf(ShreddedObject.class);
  }

  @Test
  public void testNestedMapAndListConversion() {
    VariantConverter converter = newConverter();

    Map<String, Object> nested = Maps.newHashMap();
    nested.put("k", "v");

    List<Object> arr = List.of(true, "x", 1);

    Map<String, Object> payload = Maps.newHashMap();
    payload.put("obj", nested);
    payload.put("arr", arr);

    Variant variant = converter.fromMap(payload);
    assertThat(variant.value()).isInstanceOf(ShreddedObject.class);

    ShreddedObject root = (ShreddedObject) variant.value();
    VariantValue objVal = root.get("obj");
    VariantValue arrVal = root.get("arr");

    assertThat(objVal).isNotNull();
    assertThat(objVal).isInstanceOf(ShreddedObject.class);

    assertThat(arrVal).isNotNull();
    assertThat(arrVal).isInstanceOf(ValueArray.class);
  }

  @Test
  public void testMetadataCacheSameKeySetSameInstance() {
    VariantConverter converter = newConverter();

    Map<String, Object> map = Maps.newHashMap();
    map.put("b", 2);
    map.put("a", 1);
    map.put("c", 3);

    Map<String, Object> map2 = Maps.newHashMap();
    map2.put("c", 30);
    map2.put("b", 20);
    map2.put("a", 10);

    Variant variant = converter.fromMap(map);
    Variant variant2 = converter.fromMap(map2);

    VariantMetadata variantMetadata = variant.metadata();
    VariantMetadata variantMetadata2 = variant2.metadata();

    assertThat(variantMetadata).isSameAs(variantMetadata2);
  }

  @Test
  public void testMetadataCacheDifferentKeySetDifferentInstance() {
    VariantConverter converter = newConverter();

    Map<String, Object> map = Maps.newHashMap();
    map.put("a", 1);
    map.put("b", 2);

    Map<String, Object> map2 = Maps.newHashMap();
    map2.put("a", 1);
    map2.put("c", 3);

    Variant variant = converter.fromMap(map);
    Variant variant2 = converter.fromMap(map2);

    VariantMetadata variantMetadata = variant.metadata();
    VariantMetadata variantMetadata2 = variant2.metadata();

    assertThat(variantMetadata).isNotNull();
    assertThat(variantMetadata2).isNotNull();

    assertThat(variantMetadata).isNotSameAs(variantMetadata2);
  }
}
