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
import static org.apache.iceberg.spark.TestContext.TEST_CATALOG;
import static org.apache.iceberg.spark.TestContext.TEST_DB;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;

public class RenameIcebergTableCatalogTestBase extends AbstractTestBase {
  protected static final String TEST_TABLE = "test_table";

  protected static final String TEST_TABLE_NEW = "test_table_new";

  protected static final Identifier TABLE_IDENTIFIER =
      Identifier.of(new String[] {TEST_DB}, TEST_TABLE);

  protected static final Identifier TABLE_IDENTIFIER_NEW =
      Identifier.of(new String[] {TEST_DB}, TEST_TABLE_NEW);

  protected static final String CATALOG_TABLE_NAME =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE);

  protected static final String CATALOG_TABLE_NEW_NAME =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE_NEW);

  protected static final Column[] BASE_TABLE_SCHEMA =
      new Column[] {
        Column.create("id", DataTypes.IntegerType, false),
        Column.create("username", DataTypes.StringType, true)
      };

  protected static final List<String> RECORDS =
      List.of("(1, 'Sam')", "(2, 'Bob')", "(3, 'Sue')", "(4, 'Ann')", "(1, 'Tom')", "(2, 'Brian')");

  protected List<String> extractTableFiles(String tableName) throws NoSuchTableException {
    List<String> tableDataFiles =
        spark()
            .sql(format("SELECT path FROM %s.%s.%s.manifests", TEST_CATALOG, TEST_DB, tableName))
            .collectAsList()
            .stream()
            .map(Row::mkString)
            .collect(Collectors.toList());
    tableDataFiles.addAll(
        spark()
            .sql(format("SELECT file_path FROM %s.%s.%s.files", TEST_CATALOG, TEST_DB, tableName))
            .collectAsList()
            .stream()
            .map(Row::mkString)
            .collect(Collectors.toList()));
    tableDataFiles.addAll(
        ReachableFileUtil.metadataFileLocations(
            ((SparkTable) catalog().loadTable(Identifier.of(new String[] {TEST_DB}, tableName)))
                .table(),
            true));
    return tableDataFiles;
  }

  protected List<String> extractFsContents(
      TestContext.IcebergCatalogType catalogType, boolean isRecursive) throws IOException {
    return isRecursive
        ? RemoteIterators.toList((fs().listFiles(catalogType.getNamespacePath(), true))).stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList())
        : Arrays.stream(fs().listStatus(catalogType.getNamespacePath()))
            .collect(Collectors.toList())
            .stream()
            .map(fs -> fs.getPath().toString())
            .collect(Collectors.toList());
  }

  protected List<String> extractTableRecords(String catalogTableName) {
    return spark().sql(format("SELECT * FROM %s", catalogTableName)).collectAsList().stream()
        .map(row -> format("(%s, '%s')", row.get(0), row.get(1)))
        .collect(Collectors.toList());
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
    catalog().dropTable(TABLE_IDENTIFIER_NEW);
  }
}
