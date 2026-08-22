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
import static org.apache.iceberg.spark.service.IcebergTableClient.loadCatalogTableLocation;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDropBaseDirectory extends RenameIcebergTableCatalogTestBase {

  @TestTemplate
  public void testDropBaseDirectoryEnabled(
    boolean isDropBaseDirectoryEnabled, List<String> namespaceContents)
    throws IOException,
    TableAlreadyExistsException,
    NoSuchNamespaceException,
    NoSuchTableException {
    catalog()
      .createTable(
        TABLE_IDENTIFIER,
        BASE_TABLE_SCHEMA,
        new Transform[0],
        Map.of("drop.base-directory.enabled", String.valueOf(isDropBaseDirectoryEnabled)));
    spark()
      .sql(
        format(
          "INSERT INTO %s VALUES %s",
          CATALOG_TABLE_NAME, String.join(", ", RECORDS.subList(0, 2))));
    assertThat(loadCatalogTableLocation(catalog().loadTable(TABLE_IDENTIFIER)))
      .isEqualTo(
        format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE));
    AssertionsForInterfaceTypes.assertThat(
        extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
      .containsExactlyInAnyOrderElementsOf(
        List.of(
          format(
            "%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE)));
    catalog().purgeTable(TABLE_IDENTIFIER);
    AssertionsForInterfaceTypes.assertThat(
        extractFsContents(TestContext.IcebergCatalogType.HIVE, false))
      .containsExactlyInAnyOrderElementsOf(namespaceContents);
  }

  @Parameters(name = "isDropBaseDirectoryEnabled={0}, namespaceContents={1}")
  private static List<Object[]> dropBaseDirectoryArgsProvider() {
    return Arrays.asList(
      new Object[]{true, List.of()},
      new Object[]{
        false,
        List.of(
          format("%s/%s", TestContext.IcebergCatalogType.HIVE.getNamespaceDir(), TEST_TABLE))
      });
  }
}
