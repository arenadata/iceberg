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
package org.apache.iceberg.connect;

import static java.lang.String.format;
import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.apache.iceberg.connect.UserEventOp.USER_EVENT_OP_TABLE_SCHEMA;
import static org.apache.iceberg.connect.service.IcebergTableClient.S3_CLIENT;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableFiles;
import static org.apache.iceberg.connect.service.IcebergTableClient.extractTableRecords;
import static org.apache.iceberg.connect.service.IcebergTableClient.getTablePath;
import static org.apache.iceberg.connect.service.IcebergTableClient.loadCatalogTable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.api.client.util.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.service.RestCatalogSparkClient;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class TestIcebergTableWriteModes extends SchemaTestBase {
  private static final String TEST_TABLE_BASE = "test_table";
  private static final UserEventOp USER_EVENT_OP_1 = new UserEventOp(1, "Tom", "I");
  private static final UserEventOp USER_EVENT_OP_2 = new UserEventOp(2, "Hue", "I");
  private static final UserEventOp USER_EVENT_OP_1_U = new UserEventOp(1, "Nolan", "U");
  private static final UserEventOp USER_EVENT_OP_1_D = new UserEventOp(1, "Nolan", "D");
  private static final UserEventOp USER_EVENT_OP_MERGE_ON_READ_REC = new UserEventOp(1);
  private static final List<UserEventOp> USER_EVENT_OP_LIST_INITIAL =
      List.of(USER_EVENT_OP_1, USER_EVENT_OP_2);
  private static final List<UserEventOp> USER_EVENT_OP_LIST_UPDATE =
      List.of(USER_EVENT_OP_1_U, USER_EVENT_OP_2);
  private TableIdentifier tableIdentifier;

  @BeforeEach
  public void beforeEach() throws InterruptedException {
    String suffix = UUID.randomUUID().toString().substring(0, 4);
    String testTable = TEST_TABLE_BASE + "_" + suffix;
    tableIdentifier = TableIdentifier.of(TEST_DB, testTable);
    createIcebergTable(
        Map.of(
            "write.update.mode", COPY_ON_WRITE.modeName(),
            "write.delete.mode", COPY_ON_WRITE.modeName(),
            "write.merge.mode", COPY_ON_WRITE.modeName()));
    insertTableRows(USER_EVENT_OP_LIST_INITIAL);
  }

  @ParameterizedTest
  @MethodSource("testModeArgsProvider")
  public void testRowLevelModeCopyOnWrite(
      boolean useSchema,
      RowLevelOperationMode mode,
      UserEventOp sentUserEventOp,
      List<Record> expectedTableRecords,
      List<String> expectedAddedTableParquetFilesContents) {
    Table tableBefore = loadCatalogTable(catalog(), tableIdentifier);
    assertThat(extractTableRecords(tableBefore))
        .containsExactlyInAnyOrderElementsOf(
            castUserEventOpListToRecords(USER_EVENT_OP_LIST_INITIAL));
    List<S3Object> tableParquetFilesBefore =
        extractTableFiles(getTablePath(catalog(), tableIdentifier));
    Map<String, String> tableParquetDataBefore =
        tableParquetFilesBefore.stream()
            .map(s3Object -> Map.entry(s3Object.key(), extractParquetFileData(s3Object)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    assertThat(tableParquetFilesBefore.size()).isEqualTo(2);
    launchConnector(useSchema, Map.of("iceberg.tables.row-level-mode", mode.modeName()));
    send(testTopic(), sentUserEventOp, useSchema);
    checkTableAfterConnectorStart(expectedTableRecords);
    List<S3Object> tableDataAfter = extractTableFiles(getTablePath(catalog(), tableIdentifier));
    Map<String, String> tableParquetFilesDataAfter =
        tableDataAfter.stream()
            .map(s3Object -> Map.entry(s3Object.key(), extractParquetFileData(s3Object)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    assertThat(tableParquetFilesDataAfter.entrySet())
        .containsAll(tableParquetDataBefore.entrySet());
    assertThat(tableParquetFilesDataAfter.values())
        .containsExactlyInAnyOrderElementsOf(
            Stream.concat(
                    tableParquetDataBefore.values().stream(),
                    expectedAddedTableParquetFilesContents.stream())
                .toList());
  }

  private static Stream<Arguments> testModeArgsProvider() {
    return Stream.of(
        Arguments.of(
            true,
            MERGE_ON_READ,
            USER_EVENT_OP_1_D,
            castUserEventOpListToRecords(List.of(USER_EVENT_OP_2)),
            List.of(castUserEventOpToRecord(USER_EVENT_OP_MERGE_ON_READ_REC).toString())),
        Arguments.of(
            false,
            MERGE_ON_READ,
            USER_EVENT_OP_1_D,
            castUserEventOpListToRecords(List.of(USER_EVENT_OP_2)),
            List.of(castUserEventOpToRecord(USER_EVENT_OP_MERGE_ON_READ_REC).toString())),
        Arguments.of(
            true,
            COPY_ON_WRITE,
            USER_EVENT_OP_1_D,
            castUserEventOpListToRecords(List.of(USER_EVENT_OP_2)),
            List.of()),
        Arguments.of(
            false,
            COPY_ON_WRITE,
            USER_EVENT_OP_1_D,
            castUserEventOpListToRecords(List.of(USER_EVENT_OP_2)),
            List.of()),
        Arguments.of(
            true,
            MERGE_ON_READ,
            USER_EVENT_OP_1_U,
            castUserEventOpListToRecords(USER_EVENT_OP_LIST_UPDATE),
            List.of(
                castUserEventOpToRecord(USER_EVENT_OP_1_U).toString(),
                castUserEventOpToRecord(USER_EVENT_OP_MERGE_ON_READ_REC).toString())),
        Arguments.of(
            false,
            MERGE_ON_READ,
            USER_EVENT_OP_1_U,
            castUserEventOpListToRecords(USER_EVENT_OP_LIST_UPDATE),
            List.of(
                castUserEventOpToRecord(USER_EVENT_OP_1_U).toString(),
                castUserEventOpToRecord(USER_EVENT_OP_MERGE_ON_READ_REC).toString())),
        Arguments.of(
            true,
            COPY_ON_WRITE,
            USER_EVENT_OP_1_U,
            castUserEventOpListToRecords(USER_EVENT_OP_LIST_UPDATE),
            List.of(castUserEventOpToRecord(USER_EVENT_OP_1_U).toString())),
        Arguments.of(
            false,
            COPY_ON_WRITE,
            USER_EVENT_OP_1_U,
            castUserEventOpListToRecords(USER_EVENT_OP_LIST_UPDATE),
            List.of(castUserEventOpToRecord(USER_EVENT_OP_1_U).toString())));
  }

  private void createIcebergTable(Map<String, String> tableConfig) {
    catalog()
        .createTable(
            tableIdentifier,
            USER_EVENT_OP_TABLE_SCHEMA,
            UserEventOp.USER_EVENT_OP_SPEC,
            tableConfig);
  }

  private void insertTableRows(List<UserEventOp> userEventOpList) throws InterruptedException {
    RestCatalogSparkClient.runSparkSqlQuery(
        format(
            "INSERT INTO spark_catalog.%s VALUES %s",
            tableIdentifier,
            String.join(", ", userEventOpList.stream().map(UserEventOp::castForInsert).toList())));
  }

  private static List<Record> castUserEventOpListToRecords(List<UserEventOp> userEventOpList) {
    return userEventOpList.stream()
        .map(TestIcebergTableWriteModes::castUserEventOpToRecord)
        .toList();
  }

  private static Record castUserEventOpToRecord(UserEventOp userEventOp) {
    Record record = GenericRecord.create(USER_EVENT_OP_TABLE_SCHEMA);
    record.setField("id", userEventOp.id());
    record.setField("username", userEventOp.username());
    record.setField("op", userEventOp.op());
    return record;
  }

  private String extractParquetFileData(S3Object s3Object) {
    ResponseInputStream<GetObjectResponse> s3Stream =
        S3_CLIENT.getObject(b -> b.bucket("bucket").key(s3Object.key()));

    List<Object> parquetRows = Lists.newArrayList();
    CloseableIterable<Object> parquetIterable;
    try {
      parquetIterable =
          Parquet.read(new InMemoryInputFile(s3Stream.readAllBytes()))
              .project(loadCatalogTable(catalog(), tableIdentifier).schema())
              .createReaderFunc(
                  fileSchema ->
                      GenericParquetReaders.buildReader(
                          loadCatalogTable(catalog(), tableIdentifier).schema(), fileSchema))
              .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    parquetIterable.forEach(parquetRows::add);
    return parquetRows.get(0).toString();
  }

  private Table checkTableAfterConnectorStart(List<Record> expectedRecords) {
    AtomicReference<Table> tableAfterRef = new AtomicReference<>();
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Table tableAfter = loadCatalogTable(catalog(), tableIdentifier);
              assertThat(extractTableRecords(tableAfter))
                  .containsExactlyInAnyOrderElementsOf(expectedRecords);
              tableAfterRef.set(tableAfter);
            });
    return tableAfterRef.get();
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", tableIdentifier.toString())
        .config("iceberg.tables.default-id-columns", "id")
        .config("iceberg.tables.upsert-mode-enabled", true)
        .config("iceberg.tables.cdc-field", "op")
        .config("iceberg.tables.staging-orphan-ttl-ms", 2160000000L)
        .config("tasks.max", 1);
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(tableIdentifier);
  }
}
