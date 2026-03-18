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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.MetadataAttribute;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestSparkSchemaUtil {
  private static final Schema TEST_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final Schema TEST_SCHEMA_WITH_METADATA_COLS =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          MetadataColumns.FILE_PATH,
          MetadataColumns.ROW_POSITION);

  private static final Schema TEST_SCHEMA_WITH_GEOMETRY =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "geom", Types.GeometryType.crs84()),
          optional(3, "geog", Types.GeographyType.crs84()));

  private static final Schema TEST_SCHEMA_WITH_CUSTOM_CRS =
      new Schema(
          optional(1, "geom", Types.GeometryType.of("EPSG:32637")));

  @Test
  public void testGeometrySchemaConversion() {
    StructType sparkType = SparkSchemaUtil.convert(TEST_SCHEMA_WITH_GEOMETRY);

    assertThat(sparkType.fields()).hasSize(3);

    // GEOMETRY/GEOGRAPHY map to BinaryType with iceberg.original-type metadata
    assertThat(sparkType.apply("geom").dataType()).isEqualTo(BinaryType$.MODULE$);
    assertThat(sparkType.apply("geom").metadata().getString("iceberg.original-type"))
        .isEqualTo("geometry");

    assertThat(sparkType.apply("geog").dataType()).isEqualTo(BinaryType$.MODULE$);
    assertThat(sparkType.apply("geog").metadata().getString("iceberg.original-type"))
        .isEqualTo("geography");

    // Regular fields should not have iceberg.original-type metadata
    assertThat(sparkType.apply("id").metadata().contains("iceberg.original-type")).isFalse();
  }

  @Test
  public void testGeometryMetadataRoundTrip() {
    StructType sparkType = SparkSchemaUtil.convert(TEST_SCHEMA_WITH_GEOMETRY);
    Schema roundTripped = SparkSchemaUtil.convert(sparkType);

    assertThat(roundTripped.findField("geom").type())
        .isEqualTo(Types.GeometryType.crs84());
    assertThat(roundTripped.findField("geog").type())
        .isEqualTo(Types.GeographyType.crs84());
  }

  @Test
  public void testGeometryCustomCrsRoundTrip() {
    StructType sparkType = SparkSchemaUtil.convert(TEST_SCHEMA_WITH_CUSTOM_CRS);

    // Metadata should preserve the full type string including CRS
    assertThat(sparkType.apply("geom").metadata().getString("iceberg.original-type"))
        .isEqualTo("geometry(EPSG:32637)");

    // Round-trip should preserve the CRS
    Schema roundTripped = SparkSchemaUtil.convert(sparkType);
    Types.GeometryType geomType = (Types.GeometryType) roundTripped.findField("geom").type();
    assertThat(geomType.crs()).isEqualTo("EPSG:32637");
  }

  @Test
  public void testEstimateSizeMaxValue() {
    assertThat(SparkSchemaUtil.estimateSize(null, Long.MAX_VALUE))
        .as("estimateSize returns Long max value")
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testEstimateSizeWithOverflow() {
    long tableSize =
        SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), Long.MAX_VALUE - 1);
    assertThat(tableSize).as("estimateSize handles overflow").isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testEstimateSize() {
    long tableSize = SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), 1);
    assertThat(tableSize).as("estimateSize matches with expected approximation").isEqualTo(24);
  }

  @Test
  public void testSchemaConversionWithMetaDataColumnSchema() {
    StructType structType = SparkSchemaUtil.convert(TEST_SCHEMA_WITH_METADATA_COLS);
    List<AttributeReference> attrRefs =
        scala.collection.JavaConverters.seqAsJavaList(DataTypeUtils.toAttributes(structType));
    for (AttributeReference attrRef : attrRefs) {
      if (MetadataColumns.isMetadataColumn(attrRef.name())) {
        assertThat(MetadataAttribute.unapply(attrRef).isDefined())
            .as("metadata columns should have __metadata_col in attribute metadata")
            .isTrue();
      } else {
        assertThat(MetadataAttribute.unapply(attrRef).isDefined())
            .as("non metadata columns should not have __metadata_col in attribute metadata")
            .isFalse();
      }
    }
  }
}
