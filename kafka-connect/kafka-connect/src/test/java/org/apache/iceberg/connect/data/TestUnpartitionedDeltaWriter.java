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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestUnpartitionedDeltaWriter extends TestBaseWriter {

  @Override
  @BeforeEach
  public void before() {
    super.before();
    // Override the schema for CDC tests
    when(table.schema()).thenReturn(SCHEMA);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testCDCInsertOperations(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    Record row1 = wrappedRecord(1L, "insert1", Operation.INSERT);
    Record row2 = wrappedRecord(2L, "insert2", Operation.INSERT);
    Record row3 = wrappedRecord(3L, "insert3", Operation.INSERT);

    WriteResult result =
        writeTest(ImmutableList.of(row1, row2, row3), config, UnpartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(0);

    Arrays.asList(result.dataFiles())
        .forEach(
            file -> {
              assertThat(file.format()).isEqualTo(FileFormat.fromString(format));
              assertThat(file.recordCount()).isEqualTo(3);
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testCDCUpdateOperations(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    // INSERT followed by UPDATE
    Record row1Insert = wrappedRecord(100L, "original", Operation.INSERT);
    Record row1Update = wrappedRecord(100L, "updated", Operation.UPDATE);

    WriteResult result =
        writeTest(ImmutableList.of(row1Insert, row1Update), config, UnpartitionedDeltaWriter.class);

    // In upsert mode, UPDATE = delete by key + insert
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
    assertPuffinDeleteFile(result.deleteFiles()[0]);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testCDCDeleteOperations(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    // INSERT followed by DELETE
    Record row1Insert = wrappedRecord(200L, "to-be-deleted", Operation.INSERT);
    Record row1Delete = wrappedRecord(200L, "to-be-deleted", Operation.DELETE);

    WriteResult result =
        writeTest(ImmutableList.of(row1Insert, row1Delete), config, UnpartitionedDeltaWriter.class);

    // In upsert mode, DELETE = delete by key (no insert)
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testCDCMixedOperations(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    // Mix of INSERT, UPDATE, DELETE in single batch
    Record insert1 = wrappedRecord(1L, "row1", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "row2", Operation.INSERT);
    Record update1 = wrappedRecord(1L, "row1-updated", Operation.UPDATE);
    Record delete2 = wrappedRecord(2L, "row2", Operation.DELETE);
    Record insert3 = wrappedRecord(3L, "row3", Operation.INSERT);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, delete2, insert3),
            config,
            UnpartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testNonUpsertMode(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(false); // Non-upsert mode
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    Record insert = wrappedRecord(1L, "row1", Operation.INSERT);
    Record update = wrappedRecord(1L, "row1-updated", Operation.UPDATE);
    Record delete = wrappedRecord(1L, "row1-updated", Operation.DELETE);

    WriteResult result =
        writeTest(ImmutableList.of(insert, update, delete), config, UnpartitionedDeltaWriter.class);

    // In non-upsert mode, deletes use full row comparison
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testDataCorrectnessDVMode(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    Record insert1 = wrappedRecord(1L, "data1", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "data2", Operation.INSERT);
    Record update1 = wrappedRecord(1L, "data1-updated", Operation.UPDATE);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1), config, UnpartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    Arrays.asList(result.dataFiles())
        .forEach(
            file -> {
              assertThat(file.format()).isEqualTo(FileFormat.fromString(format));
              assertThat(file.recordCount()).isEqualTo(3);
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testEmptyWriteResult(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    WriteResult result = writeTest(ImmutableList.of(), config, UnpartitionedDeltaWriter.class);

    // Empty write should produce no files
    assertThat(result.dataFiles()).isEmpty();
    assertThat(result.deleteFiles()).isEmpty();
  }
}
