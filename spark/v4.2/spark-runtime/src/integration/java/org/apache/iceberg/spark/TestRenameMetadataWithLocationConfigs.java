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

import static java.lang.String.format;
import static org.apache.iceberg.spark.IcebergCatalogProperties.BASE_COLUMN_SCHEMA;
import static org.apache.iceberg.spark.IcebergCatalogProperties.CATALOG_TEST_TABLE;
import static org.apache.iceberg.spark.IcebergCatalogProperties.CATALOG_TEST_TABLE_NEW;
import static org.apache.iceberg.spark.IcebergCatalogProperties.RECORDS;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TABLE_IDENTIFIER;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TABLE_IDENTIFIER_NEW;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_DB;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE_NEW;
import static org.apache.iceberg.spark.IcebergCatalogProperties.WAREHOUSE_LOCATION;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRenameMetadataWithLocationConfigs extends IntegrationTestBase {

  @TestTemplate
  public void testRenameMetadataLocationUpdateNonDefaultLocation(Map<String, String> locationConfig)
      throws TableAlreadyExistsException,
          NoSuchNamespaceException,
          IOException,
          NoSuchTableException {
    initSpark(IcebergCatalogType.HIVE, Map.of("rename.metadata.location.update", "true"));
    sparkCatalog()
        .createTable(TABLE_IDENTIFIER, BASE_COLUMN_SCHEMA, new Transform[0], locationConfig);
    insertData(CATALOG_TEST_TABLE, RECORDS.subList(0, 2));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TEST_TABLE, TEST_TABLE_NEW));
    insertData(CATALOG_TEST_TABLE_NEW, RECORDS.subList(2, 4));
    assertRecords(CATALOG_TEST_TABLE_NEW, MAIN_COLUMNS, RECORDS);
    AssertionsForClassTypes.assertThat(
            loadCatalogTableLocation(sparkCatalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFileSystemContents(IcebergCatalogType.HIVE.getNamespacePath(), false))
        .doesNotContain(format("%s/%s", IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW));
  }

  @Parameters(name = "locationConfig={0}")
  private static List<Object[]> renameMetadataLocationUpdateNonDefaultLocationProvider() {
    return Arrays.asList(
        new Object[] {
          Map.of(
              "write.data.path",
              format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")),
          format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")
        },
        new Object[] {
          Map.of(
              "write.metadata.path",
              format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "metadata-storage")),
          format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "metadata-storage")
        },
        new Object[] {
          Map.of(
              "write.object-storage.enabled",
              "true",
              "write.data.path",
              format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")),
          format("%s/%s.db/%s", WAREHOUSE_LOCATION, TEST_DB, "data-storage")
        });
  }
}
