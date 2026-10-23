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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Schema evolution on the copy-on-write staging path.
 *
 * <p>Merge-on-read has evolved schemas since long before this mode existed, and a connector that
 * did so must keep doing it when its mode changes: otherwise a new source field reaches neither the
 * table nor the staged file, and nothing says so.
 */
public class TestCopyOnWriteSchemaEvolution {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TABLE_IDENTIFIER);
  private static final Set<Integer> ID_FIELDS = ImmutableSet.of(1);
  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ID_FIELDS);

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testANewFieldEvolvesTheTableAndRollsTheStagedFile() {
    CopyOnWriteStagingWriter writer =
        new CopyOnWriteStagingWriter(table, TABLE_REFERENCE, config(true), ID_FIELDS);

    writer.write(record(ImmutableMap.of("id", 1L, "data", "a"), 0L));
    // the second record carries a field the table does not have yet
    writer.write(record(ImmutableMap.of("id", 2L, "data", "b", "extra", "x"), 1L));

    table.refresh();
    assertThat(table.schema().findField("extra")).as("table evolved").isNotNull();

    List<StagedChangeFile> staged = stagedFiles(writer);
    // the appender is bound to its schema, so evolving mid-cycle has to end the file in progress
    assertThat(staged).as("one file per schema").hasSize(2);
    assertThat(staged).extracting(StagedChangeFile::schemaId).doesNotHaveDuplicates();
    assertThat(staged).extracting(StagedChangeFile::recordCount).containsExactly(1L, 1L);
    // that the two are read back as one change set is asserted where the normalizer lives:
    // TestChangeSetNormalizer#testFilesWrittenUnderTwoSchemasNormalizeAsOneChangeSet
  }

  @Test
  public void testEvolutionStaysOffWhenItIsNotEnabled() {
    CopyOnWriteStagingWriter writer =
        new CopyOnWriteStagingWriter(table, TABLE_REFERENCE, config(false), ID_FIELDS);

    writer.write(record(ImmutableMap.of("id", 1L, "data", "a", "extra", "x"), 0L));

    table.refresh();
    assertThat(table.schema().findField("extra")).isNull();
    assertThat(stagedFiles(writer)).hasSize(1);
  }

  private List<StagedChangeFile> stagedFiles(CopyOnWriteStagingWriter writer) {
    List<StagedChangeFile> files = Lists.newArrayList();
    writer.complete().forEach(result -> files.addAll(((StagedChangesResult) result).stagedFiles()));
    return files;
  }

  private SinkRecord record(Map<String, Object> value, long offset) {
    return new SinkRecord(
        "topic", 0, null, "key", null, value, offset, 0L, TimestampType.LOG_APPEND_TIME);
  }

  private IcebergSinkConfig config(boolean evolveSchema) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("cg-connect");
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.taskId()).thenReturn("task-0");
    when(config.isUpsertMode()).thenReturn(true);
    when(config.evolveSchemaEnabled()).thenReturn(evolveSchema);
    when(config.copyOnWriteStagingLocation()).thenReturn(null);
    return config;
  }
}
