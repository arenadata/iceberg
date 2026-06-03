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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_REST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestAlterTable extends CatalogTestBase {
  private final TableIdentifier renamedIdent =
      TableIdentifier.of(Namespace.of("default"), "table2");

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s2", tableName);
  }

  @TestTemplate
  public void testAddColumnNotNull() {
    assertThatThrownBy(() -> sql("ALTER TABLE %s ADD COLUMN c3 INT NOT NULL", tableName))
        .isInstanceOf(SparkException.class)
        .hasMessage(
            "Unsupported table change: Incompatible change: cannot add required column: c3");
  }

  @TestTemplate
  public void testAddColumn() {
    sql(
        "ALTER TABLE %s ADD COLUMN point struct<x: double NOT NULL, y: double NOT NULL> AFTER id",
        tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(
                3,
                "point",
                Types.StructType.of(
                    NestedField.required(4, "x", Types.DoubleType.get()),
                    NestedField.required(5, "y", Types.DoubleType.get()))),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);

    sql("ALTER TABLE %s ADD COLUMN point.z double COMMENT 'May be null' FIRST", tableName);

    Types.StructType expectedSchema2 =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(
                3,
                "point",
                Types.StructType.of(
                    NestedField.optional(6, "z", Types.DoubleType.get(), "May be null"),
                    NestedField.required(4, "x", Types.DoubleType.get()),
                    NestedField.required(5, "y", Types.DoubleType.get()))),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema2);
  }

  @TestTemplate
  public void testAddColumnWithArray() {
    sql("ALTER TABLE %s ADD COLUMN data2 array<struct<a:INT,b:INT,c:int>>", tableName);
    // use the implicit column name 'element' to access member of array and add column d to struct.
    sql("ALTER TABLE %s ADD COLUMN data2.element.d int", tableName);
    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()),
            NestedField.optional(
                3,
                "data2",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        NestedField.optional(5, "a", Types.IntegerType.get()),
                        NestedField.optional(6, "b", Types.IntegerType.get()),
                        NestedField.optional(7, "c", Types.IntegerType.get()),
                        NestedField.optional(8, "d", Types.IntegerType.get())))));
    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testAddColumnWithMap() {
    sql("ALTER TABLE %s ADD COLUMN data2 map<struct<x:INT>, struct<a:INT,b:INT>>", tableName);
    // use the implicit column name 'key' and 'value' to access member of map.
    // add column to value struct column
    sql("ALTER TABLE %s ADD COLUMN data2.value.c int", tableName);
    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()),
            NestedField.optional(
                3,
                "data2",
                Types.MapType.ofOptional(
                    4,
                    5,
                    Types.StructType.of(NestedField.optional(6, "x", Types.IntegerType.get())),
                    Types.StructType.of(
                        NestedField.optional(7, "a", Types.IntegerType.get()),
                        NestedField.optional(8, "b", Types.IntegerType.get()),
                        NestedField.optional(9, "c", Types.IntegerType.get())))));
    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);

    // should not allow changing map key column
    assertThatThrownBy(() -> sql("ALTER TABLE %s ADD COLUMN data2.key.y int", tableName))
        .isInstanceOf(SparkException.class)
        .hasMessageStartingWith("Unsupported table change: Cannot add fields to map keys:");
  }

  @TestTemplate
  public void testAddColumnWithDefaultValuesUnsupported() throws InterruptedException {
    assumeThat(catalogName).isNotEqualTo("spark_catalog");
    assertThatThrownBy(
            () -> sql("ALTER TABLE %s ADD COLUMN col_with_default int DEFAULT 123", tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith(
            "Cannot add column col_with_default since setting default values in Spark is currently unsupported");
  }

  @TestTemplate
  public void testDropColumn() {
    sql("ALTER TABLE %s DROP COLUMN data", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(NestedField.required(1, "id", Types.LongType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testRenameColumn() {
    sql("ALTER TABLE %s RENAME COLUMN id TO row_id", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "row_id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testAlterColumnComment() {
    sql("ALTER TABLE %s ALTER COLUMN id COMMENT 'Record id'", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get(), "Record id"),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testAlterColumnType() {
    sql("ALTER TABLE %s ADD COLUMN count int", tableName);
    sql("ALTER TABLE %s ALTER COLUMN count TYPE bigint", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()),
            NestedField.optional(3, "count", Types.LongType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testAlterColumnDropNotNull() {
    sql("ALTER TABLE %s ALTER COLUMN id DROP NOT NULL", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.optional(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testAlterColumnSetNotNull() {
    // no-op changes are allowed
    sql("ALTER TABLE %s ALTER COLUMN id SET NOT NULL", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);

    assertThatThrownBy(() -> sql("ALTER TABLE %s ALTER COLUMN data SET NOT NULL", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Cannot change nullable column to non-nullable: data");
  }

  @TestTemplate
  public void testAlterColumnPositionAfter() {
    sql("ALTER TABLE %s ADD COLUMN count int", tableName);
    sql("ALTER TABLE %s ALTER COLUMN count AFTER id", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(3, "count", Types.IntegerType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testAlterColumnPositionFirst() {
    sql("ALTER TABLE %s ADD COLUMN count int", tableName);
    sql("ALTER TABLE %s ALTER COLUMN count FIRST", tableName);

    Types.StructType expectedSchema =
        Types.StructType.of(
            NestedField.optional(3, "count", Types.IntegerType.get()),
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema);
  }

  @TestTemplate
  public void testTableRename() {
    assumeThat(catalogConfig.get(ICEBERG_CATALOG_TYPE))
        .as(
            "need to fix https://github.com/apache/iceberg/issues/11154 before enabling this for the REST catalog")
        .isNotEqualTo(ICEBERG_CATALOG_TYPE_REST);
    assumeThat(validationCatalog)
        .as("Hadoop catalog does not support rename")
        .isNotInstanceOf(HadoopCatalog.class);

    assertThat(validationCatalog.tableExists(tableIdent)).as("Initial name should exist").isTrue();
    assertThat(validationCatalog.tableExists(renamedIdent))
        .as("New name should not exist")
        .isFalse();

    sql("ALTER TABLE %s RENAME TO %s2", tableName, tableName);

    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Initial name should not exist")
        .isFalse();
    assertThat(validationCatalog.tableExists(renamedIdent)).as("New name should exist").isTrue();
  }

  /**
   * Verifies the data-safety property of {@link CatalogProperties#RENAME_UPDATE_LOCATION} that is
   * specific to directory-level purges. Iceberg's per-file {@code DROP PURGE} is already safe
   * even when two tables share a directory, so the asserted property only matters when {@code
   * drop.base-directory.enabled} is in play. Only writes made
   * after the rename are protected — pre-rename data files keep absolute paths under the old
   * directory and are not moved.
   */
  @TestTemplate
  public void testRenameProtectsAgainstBaseDirectoryDrop() {
    assumeThat(catalogConfig.get(ICEBERG_CATALOG_TYPE))
        .as("rename.metadata.location.update is only implemented for the Hive catalog")
        .isEqualTo(ICEBERG_CATALOG_TYPE_HIVE);

    String renameCatalog = "rename_loc_cat";
    spark.conf().set("spark.sql.catalog." + renameCatalog, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + renameCatalog + ".type", ICEBERG_CATALOG_TYPE_HIVE);
    spark
        .conf()
        .set(
            "spark.sql.catalog." + renameCatalog + "." + CatalogProperties.RENAME_UPDATE_LOCATION,
            "true");

    String src = renameCatalog + ".default.rename_drop_src";
    String dst = renameCatalog + ".default.rename_drop_dst";

    try {
      sql("CREATE NAMESPACE IF NOT EXISTS %s.default", renameCatalog);
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING iceberg TBLPROPERTIES "
              + "('drop.base-directory.enabled' = 'true')",
          src);

      sql("ALTER TABLE %s RENAME TO %s", src, dst);

      // Write to the renamed table AFTER the rename; with rename.metadata.location.update these files
      // are placed under the new default directory, not the old src directory.
      sql("INSERT INTO %s VALUES (1, 'post-rename')", dst);

      // Re-create the old name; it takes the now-vacated default directory.
      sql(
          "CREATE TABLE %s (id INT, name STRING) USING iceberg TBLPROPERTIES "
              + "('drop.base-directory.enabled' = 'true')",
          src);
      sql("INSERT INTO %s VALUES (2, 'new-src-data')", src);

      // DROP ... PURGE on the new src triggers io.deletePrefix on its base directory.
      // The renamed table's post-rename data lives elsewhere and must survive.
      sql("DROP TABLE %s PURGE", src);

      assertEquals(
          "Renamed table data written after the rename must survive base-dir purge of the old name",
          ImmutableList.of(row(1, "post-rename")),
          sql("SELECT * FROM %s ORDER BY id", dst));
    } finally {
      sql("DROP TABLE IF EXISTS %s", src);
      sql("DROP TABLE IF EXISTS %s", dst);
    }
  }

  @TestTemplate
  public void testSetTableProperties() {
    sql("ALTER TABLE %s SET TBLPROPERTIES ('prop'='value')", tableName);

    assertThat(validationCatalog.loadTable(tableIdent).properties())
        .as("Should have the new table property")
        .containsEntry("prop", "value");

    sql("ALTER TABLE %s UNSET TBLPROPERTIES ('prop')", tableName);

    assertThat(validationCatalog.loadTable(tableIdent).properties())
        .as("Should not have the removed table property")
        .doesNotContainKey("prop");

    String[] reservedProperties = new String[] {"sort-order", "identifier-fields"};
    for (String reservedProp : reservedProperties) {
      assertThatThrownBy(
              () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('%s'='value')", tableName, reservedProp))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageStartingWith(
              "Cannot specify the '%s' because it's a reserved table property", reservedProp);
    }
  }
}
