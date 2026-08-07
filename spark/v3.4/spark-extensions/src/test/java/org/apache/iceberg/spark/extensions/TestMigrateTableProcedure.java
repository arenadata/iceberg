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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMigrateTableProcedure extends ExtensionsTestBase {
  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
    sql("DROP TABLE IF EXISTS default.orc_timestamp_backup");
  }

  @TestTemplate
  public void testMigrate() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    String tableLocation = createdTable.location().replace("file:", "");
    assertThat(tableLocation).as("Table should have original location").isEqualTo(location);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName + "_BACKUP_");
  }

  @TestTemplate
  public void testMigrateOrcTableWithTimestamp() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    File location = Files.createTempDirectory(temp, "junit").toFile();
    writeOrcTimestampFile(location);
    sql(
        "CREATE EXTERNAL TABLE %s (id INT, region STRING, count INT, last_update TIMESTAMP) "
            + "STORED AS ORC LOCATION '%s'",
        tableName, location);

    Object result =
        scalarSql(
            "CALL %s.system.migrate("
                + "table => '%s', "
                + "backup_table_name => 'orc_timestamp_backup', "
                + "properties => map('write.format.default', 'parquet'))",
            catalogName, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);
    Types.TimestampType lastUpdateType =
        (Types.TimestampType) createdTable.schema().findType("last_update");
    assertThat(lastUpdateType.shouldAdjustToUTC()).isFalse();

    assertThat(sql("SELECT * FROM %s", tableName)).hasSize(5);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, "moscow", 150, "2026-03-25 14:30:00"),
            row(2, "kazan", 89, "2026-03-25 15:15:00"),
            row(3, "spb", 234, "2026-03-23 10:45:00"),
            row(4, "ekaterinburg", 67, "2025-03-02 13:20:00"),
            row(5, "novosibirsk", 192, "2026-03-25 17:10:00")),
        sql(
            "SELECT id, region, count, CAST(last_update AS STRING) FROM %s ORDER BY id",
            tableName));
  }

  @TestTemplate
  public void testMigrateWithOptions() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql("CALL %s.system.migrate('%s', map('foo', 'bar'))", catalogName, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    Map<String, String> props = createdTable.properties();
    assertThat(props).containsEntry("foo", "bar");

    String tableLocation = createdTable.location().replace("file:", "");
    assertThat(tableLocation).as("Table should have original location").isEqualTo(location);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName + "_BACKUP_");
  }

  @TestTemplate
  public void testMigrateWithDropBackup() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql(
            "CALL %s.system.migrate(table => '%s', drop_backup => true)", catalogName, tableName);
    assertThat(result).as("Should have added one file").isEqualTo(1L);
    assertThat(spark.catalog().tableExists(tableName + "_BACKUP_")).isFalse();
  }

  @TestTemplate
  public void testMigrateWithBackupTableName() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    String backupTableName = "backup_table";
    Object result =
        scalarSql(
            "CALL %s.system.migrate(table => '%s', backup_table_name => '%s')",
            catalogName, tableName, backupTableName);

    assertThat(result).isEqualTo(1L);
    String dbName = tableName.split("\\.")[0];
    assertThat(spark.catalog().tableExists(dbName + "." + backupTableName)).isTrue();
  }

  @TestTemplate
  public void testMigrateWithInvalidMetricsConfig() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);

    assertThatThrownBy(
            () -> {
              String props = "map('write.metadata.metrics.column.x', 'X')";
              sql("CALL %s.system.migrate('%s', %s)", catalogName, tableName, props);
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Invalid metrics config");
  }

  @TestTemplate
  public void testMigrateWithConflictingProps() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql("CALL %s.system.migrate('%s', map('migrated', 'false'))", catalogName, tableName);
    assertThat(result).as("Should have added one file").isEqualTo(1L);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.properties()).containsEntry("migrated", "true");
  }

  @TestTemplate
  public void testInvalidMigrateCases() {
    assertThatThrownBy(() -> sql("CALL %s.system.migrate()", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    assertThatThrownBy(() -> sql("CALL %s.system.migrate(map('foo','bar'))", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Wrong arg type for table");

    assertThatThrownBy(() -> sql("CALL %s.system.migrate('')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }

  @TestTemplate
  public void testMigratePartitionWithSpecialCharacter() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string, dt date) USING parquet "
            + "PARTITIONED BY (data, dt) LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, '2023/05/30', date '2023-05-30')", tableName);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "2023/05/30", java.sql.Date.valueOf("2023-05-30"))),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testMigrateEmptyPartitionedTable() throws Exception {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet PARTITIONED BY (id) LOCATION '%s'",
        tableName, location);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);
    assertThat(result).isEqualTo(0L);
  }

  @TestTemplate
  public void testMigrateEmptyTable() throws Exception {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);
    assertThat(result).isEqualTo(0L);
  }

  @TestTemplate
  public void testMigrateWithParallelism() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    List<Object[]> result =
        sql("CALL %s.system.migrate(table => '%s', parallelism => %d)", catalogName, tableName, 2);
    assertEquals("Procedure output must match", ImmutableList.of(row(2L)), result);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testMigrateWithInvalidParallelism() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.migrate(table => '%s', parallelism => %d)",
                    catalogName, tableName, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Parallelism should be larger than 0");
  }

  @TestTemplate
  public void testMigratePartitionedWithParallelism() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet PARTITIONED BY (id) LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s (id, data) VALUES (1, 'a'), (2, 'b')", tableName);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(2L)),
        sql("CALL %s.system.migrate(table => '%s', parallelism => %d)", catalogName, tableName, 2));
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row("a", 1L), row("b", 2L)),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  private void writeOrcTimestampFile(File location) throws IOException {
    TypeDescription schema =
        TypeDescription.fromString("struct<id:int,region:string,count:int,last_update:timestamp>");
    Path path = new Path(new File(location, "data.orc").toURI());

    try (Writer writer =
        OrcFile.createWriter(
            path, OrcFile.writerOptions(spark.sessionState().newHadoopConf()).setSchema(schema))) {
      Object batch = TypeDescription.class.getMethod("createRowBatch").invoke(schema);
      Object[] columns = (Object[]) batch.getClass().getField("cols").get(batch);

      addOrcTimestampRow(
          batch,
          columns[0],
          columns[1],
          columns[2],
          columns[3],
          1,
          "moscow",
          150,
          "2026-03-25 14:30:00");
      addOrcTimestampRow(
          batch,
          columns[0],
          columns[1],
          columns[2],
          columns[3],
          2,
          "kazan",
          89,
          "2026-03-25 15:15:00");
      addOrcTimestampRow(
          batch,
          columns[0],
          columns[1],
          columns[2],
          columns[3],
          3,
          "spb",
          234,
          "2026-03-23 10:45:00");
      addOrcTimestampRow(
          batch,
          columns[0],
          columns[1],
          columns[2],
          columns[3],
          4,
          "ekaterinburg",
          67,
          "2025-03-02 13:20:00");
      addOrcTimestampRow(
          batch,
          columns[0],
          columns[1],
          columns[2],
          columns[3],
          5,
          "novosibirsk",
          192,
          "2026-03-25 17:10:00");
      writer.getClass().getMethod("addRowBatch", batch.getClass()).invoke(writer, batch);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to write ORC test file", e);
    }
  }

  private static void addOrcTimestampRow(
      Object batch,
      Object idVector,
      Object regionVector,
      Object countVector,
      Object timestampVector,
      int id,
      String region,
      int count,
      String timestamp)
      throws ReflectiveOperationException {
    int row = batch.getClass().getField("size").getInt(batch);
    batch.getClass().getField("size").setInt(batch, row + 1);

    ((long[]) idVector.getClass().getField("vector").get(idVector))[row] = id;
    byte[] regionBytes = region.getBytes(StandardCharsets.UTF_8);
    regionVector
        .getClass()
        .getMethod("setVal", int.class, byte[].class)
        .invoke(regionVector, row, regionBytes);
    ((long[]) countVector.getClass().getField("vector").get(countVector))[row] = count;
    Timestamp parsedTimestamp = Timestamp.valueOf(timestamp);
    ((long[]) timestampVector.getClass().getField("time").get(timestampVector))[row] =
        parsedTimestamp.getTime();
    ((int[]) timestampVector.getClass().getField("nanos").get(timestampVector))[row] =
        parsedTimestamp.getNanos();
  }
}
