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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class PartitionedDeltaWriterTest extends BaseWriterTest {

  @Test
  public void testPartitionedDeltaWriter() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tableConfig(table.name()))
        .thenReturn(new TableSinkConfig(Pattern.compile(""), Arrays.asList(), Arrays.asList(), ""));

    when(table.spec()).thenReturn(SPEC);

    Record row1 = GenericRecord.create(SCHEMA);
    row1.setField("id", 123L);
    row1.setField("data", "hello world!");
    row1.setField("id2", 123L);

    Record row2 = GenericRecord.create(SCHEMA);
    row2.setField("id", 234L);
    row2.setField("data", "foobar");
    row2.setField("id2", 234L);

    WriteResult result =
        writeTest(ImmutableList.of(row1, row2), config, PartitionedDeltaWriter.class);

    // in upsert mode, each write is a delete + append, so we'll have 1 data file
    // and 1 delete file for each partition (2 total)
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(2);
  }

  @Test
  public void testPartitionedDeltaWriterWithWrappedRows() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tableConfig(table.name()))
        .thenReturn(new TableSinkConfig(Pattern.compile(""), Arrays.asList(), Arrays.asList(), ""));

    when(table.spec()).thenReturn(SPEC);

    List<Record> records =
        ImmutableList.of(
            wrappedRecord(123L, "part1", Operation.INSERT),
            wrappedRecord(234L, "part1", Operation.UPDATE),
            wrappedRecord(123L, "part1", Operation.DELETE),
            wrappedRecord(456L, "part2", Operation.UPDATE));

    WriteResult result = writeTest(records, config, PartitionedDeltaWriter.class);

    // 2 append files because of 2 UPDATES and INSERT (1 for each partition)
    // 3 delete files because of 2 UPDATES (equality deletes) + 1 positional for DELETE
    assertThat(result.dataFiles()).hasSize(2);
    assertThat(result.deleteFiles()).hasSize(3);
  }
}
