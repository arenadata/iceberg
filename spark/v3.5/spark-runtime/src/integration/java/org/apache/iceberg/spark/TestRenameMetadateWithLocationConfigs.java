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
// @formatter:on
package org.apache.iceberg.spark;

import static java.lang.String.format;
import static org.apache.iceberg.spark.TestContext.TEST_CATALOG;
import static org.apache.iceberg.spark.TestContext.TEST_DB;
import static org.apache.iceberg.spark.TestContext.WAREHOUSE_LOCATION;
import static org.apache.iceberg.spark.service.IcebergTableClient.loadCatalogTableLocation;
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
public class TestRenameMetadateWithLocationConfigs extends RenameIcebergTableCatalogTestBase {

  @TestTemplate
  public void testRenameMetadataLocationUpdateNonDefaultLocation(Map<String, String> locationConfig)
      throws TableAlreadyExistsException,
          NoSuchNamespaceException,
          IOException,
          NoSuchTableException {
    initSpark(
        TestContext.IcebergCatalogType.HIVE, Map.of("rename.metadata.location.update", "true"));
    spark().sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
    catalog().createTable(TABLE_IDENTIFIER, BASE_TABLE_SCHEMA, new Transform[0], locationConfig);
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    spark().sql(format("ALTER TABLE %s RENAME TO %s", CATALOG_TABLE_NAME, TEST_TABLE_NEW));
    spark()
        .sql(
            format(
                "INSERT INTO %s VALUES %s",
                CATALOG_TABLE_NEW_NAME, String.join(", ", RECORDS.subList(2, 4))));
    assertThat(extractTableRecords(CATALOG_TABLE_NEW_NAME))
        .containsExactlyInAnyOrderElementsOf(RECORDS.subList(0, 4));
    AssertionsForClassTypes.assertThat(
            loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER_NEW)))
        .isEqualTo(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    assertThat(extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
        .doesNotContain(
            format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE_NEW));
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