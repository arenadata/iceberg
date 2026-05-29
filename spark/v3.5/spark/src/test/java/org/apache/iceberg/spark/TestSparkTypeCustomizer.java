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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.junit.jupiter.api.Test;

public class TestSparkTypeCustomizer {

  @Test
  public void testServiceLoaderDiscovery() {
    List<SparkTypeCustomizer> customizers = SparkTypeCustomizer.loadAll();
    assertThat(customizers).isNotEmpty();
    assertThat(customizers).anyMatch(c -> c instanceof TestGeometrySparkTypeCustomizer);
  }

  @Test
  public void testCustomizerToSparkType() {
    TestGeometrySparkTypeCustomizer customizer = new TestGeometrySparkTypeCustomizer();

    // Handles GEOMETRY and GEOGRAPHY
    assertThat(customizer.toSparkType(Types.GeometryType.crs84())).isNotNull();
    assertThat(customizer.toSparkType(Types.GeographyType.crs84())).isNotNull();

    // Returns null for other types (delegates to default)
    assertThat(customizer.toSparkType(Types.BinaryType.get())).isNull();
    assertThat(customizer.toSparkType(Types.StringType.get())).isNull();
  }

  @Test
  public void testCustomizerIntegrationWithTypeToSparkType() {
    // Verify that TypeToSparkType produces correct output when customizer is on classpath
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()));

    StructType sparkType = SparkSchemaUtil.convert(schema);

    // The test customizer returns BinaryType (same as default), so this always holds
    assertThat(sparkType.apply("geom").dataType()).isEqualTo(BinaryType$.MODULE$);

    // Metadata should still be present for roundtrip
    assertThat(sparkType.apply("geom").metadata().contains("iceberg.original-type")).isTrue();
  }

  @Test
  public void testCustomizerIntegrationWithSparkTypeToType() {
    // Verify that SparkTypeToType restores geometry type via metadata (not just via SPI)
    Schema schema = new Schema(Types.NestedField.optional(1, "geom", Types.GeometryType.crs84()));

    // Iceberg → Spark → Iceberg roundtrip
    StructType sparkType = SparkSchemaUtil.convert(schema);
    Schema restored = SparkSchemaUtil.convert(sparkType);

    assertThat(restored.findField("geom").type()).isEqualTo(Types.GeometryType.crs84());
  }

  @Test
  public void testLoadAllIsCached() {
    // Multiple calls should return the same list instance (cached via Holder)
    List<SparkTypeCustomizer> first = SparkTypeCustomizer.loadAll();
    List<SparkTypeCustomizer> second = SparkTypeCustomizer.loadAll();
    assertThat(first).isSameAs(second);
  }

  /**
   * Test implementation of {@link SparkTypeCustomizer} registered via META-INF/services. Returns
   * BinaryType for geometry types to avoid breaking other tests that expect BinaryType.
   */
  public static class TestGeometrySparkTypeCustomizer implements SparkTypeCustomizer {

    @Override
    public DataType toSparkType(Type.PrimitiveType icebergType) {
      switch (icebergType.typeId()) {
        case GEOMETRY:
        case GEOGRAPHY:
          return BinaryType$.MODULE$;
        default:
          return null;
      }
    }

    @Override
    public Type toIcebergType(UserDefinedType<?> sparkUdt) {
      return null;
    }

    @Override
    public Type toIcebergType(StructField field) {
      return null;
    }
  }
}
