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
import static org.apache.iceberg.spark.IcebergCatalogProperties.RECORDS;
import static org.apache.iceberg.spark.IcebergCatalogProperties.SPARK_CATALOG;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TABLE_IDENTIFIER;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_DB;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_TABLE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

public class TestOrcTableMigration extends IntegrationTestBase {

  private static final String TEST_TABLE_BACKUP = "test_table_backup";
  private static final String DB_TEST_TABLE = format("%s.%s", TEST_DB, TEST_TABLE);
  private static final String SPARK_CATALOG_TEST_TABLE =
      format("%s.%s.%s", SPARK_CATALOG, TEST_DB, TEST_TABLE);
  private static final LocalDateTime CURRENT_TIME = LocalDateTime.now();
  private static final String FORMATTED_TIME =
      CURRENT_TIME.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));

  @Test
  public void testOrcMigrationTableWithTimestampColumn() throws IOException, NoSuchTableException {
    spark()
        .sql(
            format(
                "CREATE TABLE %s (id INTEGER, username STRING, create_time TIMESTAMP) USING orc",
                SPARK_CATALOG_TEST_TABLE));
    insertData(SPARK_CATALOG_TEST_TABLE, getInsertTimestampRecords(RECORDS.subList(0, 2)));
    assertRecords(SPARK_CATALOG_TEST_TABLE, castRecords(RECORDS.subList(0, 2), false));
    String tableLocation =
        loadCatalogTableLocation(sparkSessionCatalog().loadTable(TABLE_IDENTIFIER));
    assertThat(
            extractFileSystemContents(new Path(tableLocation), true).stream()
                .anyMatch(s -> s.contains("orc")))
        .isTrue();
    assertThat(
            extractFileSystemContents(new Path(tableLocation), false).stream()
                .anyMatch(s -> s.contains("parquet")))
        .isFalse();
    spark()
        .sql(
            format(
                "CALL %s.system.migrate(table => '%s', backup_table_name => '%s', properties => map('write.format.default', 'parquet'))",
                SPARK_CATALOG, DB_TEST_TABLE, TEST_TABLE_BACKUP));
    insertData(SPARK_CATALOG_TEST_TABLE, getInsertTimestampRecords(RECORDS.subList(2, 4)));
    assertRecords(SPARK_CATALOG_TEST_TABLE, castRecords(RECORDS, true));
    assertThat(
            extractFileSystemContents(new Path(tableLocation), true).stream()
                .anyMatch(s -> s.contains("parquet")))
        .isTrue();
  }

  @Test
  public void testOrcMigrationTableWithTimestampColumnWithNTZ() {
    spark()
        .sql(
            format(
                "CREATE TABLE %s (id INTEGER, username STRING, create_time TIMESTAMP) USING orc",
                SPARK_CATALOG_TEST_TABLE));
    insertData(SPARK_CATALOG_TEST_TABLE, getInsertTimestampRecords(RECORDS.subList(0, 2)));
    spark().sql("SET spark.sql.timestampType = TIMESTAMP_NTZ");
    spark()
        .sql(
            format(
                "CALL %s.system.migrate(table => '%s', backup_table_name => '%s', properties => map('write.format.default', 'parquet'))",
                SPARK_CATALOG, DB_TEST_TABLE, TEST_TABLE_BACKUP));
    insertData(SPARK_CATALOG_TEST_TABLE, getInsertTimestampRecords(RECORDS.subList(2, 4)));
    assertRecords(SPARK_CATALOG_TEST_TABLE, castRecords(RECORDS, true));
  }

  @Override
  public void clearTables() {
    sparkSessionCatalog().dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE_BACKUP));
    sparkSessionCatalog().dropTable(Identifier.of(new String[] {TEST_DB}, TEST_TABLE));
  }

  private List<String> castRecords(List<String> records, boolean isIceberg) {
    return records.stream()
        .map(
            row ->
                format(
                    "%s, %s",
                    row,
                    isIceberg
                        ? FORMATTED_TIME
                        : CURRENT_TIME.format(
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))))
        .collect(Collectors.toList());
  }

  private List<String> getInsertTimestampRecords(List<String> records) {
    return records.stream()
        .map(row -> format("%s, TIMESTAMP '%s'", row, FORMATTED_TIME))
        .collect(Collectors.toList());
  }
}
