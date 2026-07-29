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

public class TestPartitionedDeltaWriter extends TestBaseWriter {

  @Override
  @BeforeEach
  public void before() {
    super.before();
    // Override the schema for Partitioned CDC tests
    when(table.schema()).thenReturn(SCHEMA);
    when(table.spec()).thenReturn(SPEC);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testPartitionedDeltaWriter(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    Record row1 = wrappedRecord(123L, "partition1", Operation.INSERT);
    Record row2 = wrappedRecord(234L, "partition2", Operation.INSERT);
    Record row3 = wrappedRecord(345L, "partition1", Operation.INSERT);

    WriteResult result =
        writeTest(ImmutableList.of(row1, row2, row3), config, PartitionedDeltaWriter.class);

    // no delete files should be created
    assertThat(result.dataFiles()).hasSize(2); // 2 partitions
    assertThat(result.dataFiles()).allMatch(file -> file.format() == FileFormat.fromString(format));
    assertThat(result.deleteFiles()).hasSize(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testCDCOperationsAcrossPartitions(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Different operations in different partitions
    Record insert1 = wrappedRecord(1L, "partition-a", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "partition-b", Operation.INSERT);
    Record update1 = wrappedRecord(1L, "partition-a", Operation.UPDATE);
    Record delete2 = wrappedRecord(2L, "partition-b", Operation.DELETE);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // 2 partitions with data files and delete files
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testMultipleUpdatesPerPartition(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Multiple operations in the same partition
    Record insert1 = wrappedRecord(1L, "same-partition", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "same-partition", Operation.INSERT);
    Record update1 = wrappedRecord(1L, "same-partition", Operation.UPDATE);
    Record update2 = wrappedRecord(2L, "same-partition", Operation.UPDATE);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, update2),
            config,
            PartitionedDeltaWriter.class);

    // Single partition with all operations
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
    assertPuffinDeleteFile(result.deleteFiles()[0]);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testPartitionedInsertOnly(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    Record insert1 = wrappedRecord(1L, "p1", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "p2", Operation.INSERT);
    Record insert3 = wrappedRecord(3L, "p3", Operation.INSERT);
    Record insert4 = wrappedRecord(4L, "p1", Operation.INSERT);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, insert3, insert4),
            config,
            PartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(3);
    assertThat(result.deleteFiles()).hasSize(0);

    Arrays.asList(result.dataFiles())
        .forEach(
            file -> {
              assertThat(file.format()).isEqualTo(FileFormat.fromString(format));
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testPartitionedDeleteOnly(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Inserts followed by deletes across partitions
    Record insert1 = wrappedRecord(1L, "pa", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "pb", Operation.INSERT);
    Record delete1 = wrappedRecord(1L, "pa", Operation.DELETE);
    Record delete2 = wrappedRecord(2L, "pb", Operation.DELETE);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, delete1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // 2 partitions with data and delete files
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testMixedOperationsSinglePartition(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Mix of INSERT, UPDATE, DELETE in single partition
    Record insert1 = wrappedRecord(1L, "partition-x", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "partition-x", Operation.INSERT);
    Record insert3 = wrappedRecord(3L, "partition-x", Operation.INSERT);
    Record update1 = wrappedRecord(1L, "partition-x", Operation.UPDATE);
    Record delete2 = wrappedRecord(2L, "partition-x", Operation.DELETE);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, insert3, update1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // Single partition with all operations
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testPartitionedNonUpsertMode(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(false); // Non-upsert mode
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    Record insert1 = wrappedRecord(1L, "part-a", Operation.INSERT);
    Record insert2 = wrappedRecord(2L, "part-b", Operation.INSERT);
    Record update1 = wrappedRecord(1L, "part-a", Operation.UPDATE);
    Record delete2 = wrappedRecord(2L, "part-b", Operation.DELETE);

    WriteResult result =
        writeTest(
            ImmutableList.of(insert1, insert2, update1, delete2),
            config,
            PartitionedDeltaWriter.class);

    // 2 partitions with data and delete files
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testEmptyPartitions(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");

    when(table.spec()).thenReturn(SPEC);

    // Empty write should produce no files
    WriteResult result = writeTest(ImmutableList.of(), config, PartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).isEmpty();
    assertThat(result.deleteFiles()).isEmpty();
  }
}
