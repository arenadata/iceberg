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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class TestIcebergWriterFactory {

  private static final UUID COMMIT_ID = UUID.randomUUID();
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier KEYED = TableIdentifier.of(NAMESPACE, "keyed");
  private static final TableIdentifier UNKEYED = TableIdentifier.of(NAMESPACE, "unkeyed");
  private static final List<NestedField> COLUMNS =
      ImmutableList.of(required(1, "id", LongType.get()), optional(2, "data", StringType.get()));

  private InMemoryCatalog inMemoryCatalog;

  @BeforeEach
  public void before() {
    inMemoryCatalog = new InMemoryCatalog();
    inMemoryCatalog.initialize(null, ImmutableMap.of());
    inMemoryCatalog.createNamespace(NAMESPACE);
    inMemoryCatalog.createTable(KEYED, new Schema(COLUMNS, ImmutableSet.of(1)));
    inMemoryCatalog.createTable(UNKEYED, new Schema(COLUMNS));
  }

  @AfterEach
  public void after() throws IOException {
    inMemoryCatalog.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @SuppressWarnings("unchecked")
  public void testAutoCreateTable(boolean partitioned) {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    if (partitioned) {
      when(tableConfig.partitionBy()).thenReturn(ImmutableList.of("data"));
    }

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of("test-prop", "foo1"));
    when(config.tableConfig(any())).thenReturn(tableConfig);

    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(ImmutableMap.of("id", 123, "data", "foo2"));

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);
    factory.autoCreateTable("foo1.foo2.foo3.bar", record);

    ArgumentCaptor<TableIdentifier> identCaptor = ArgumentCaptor.forClass(TableIdentifier.class);
    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<PartitionSpec> specCaptor = ArgumentCaptor.forClass(PartitionSpec.class);
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(catalog)
        .createTable(
            identCaptor.capture(),
            schemaCaptor.capture(),
            specCaptor.capture(),
            propsCaptor.capture());

    assertThat(identCaptor.getValue())
        .isEqualTo(TableIdentifier.of(Namespace.of("foo1", "foo2", "foo3"), "bar"));
    assertThat(schemaCaptor.getValue().findField("id").type()).isEqualTo(LongType.get());
    assertThat(schemaCaptor.getValue().findField("data").type()).isEqualTo(StringType.get());
    assertThat(specCaptor.getValue().isPartitioned()).isEqualTo(partitioned);
    assertThat(propsCaptor.getValue()).containsKey("test-prop");

    ArgumentCaptor<Namespace> namespaceCaptor = ArgumentCaptor.forClass(Namespace.class);
    verify((SupportsNamespaces) catalog, times(3)).createNamespace(namespaceCaptor.capture());
    List<Namespace> capturedArguments = namespaceCaptor.getAllValues();
    assertThat(capturedArguments.get(0)).isEqualTo(Namespace.of("foo1"));
    assertThat(capturedArguments.get(1)).isEqualTo(Namespace.of("foo1", "foo2"));
    assertThat(capturedArguments.get(2)).isEqualTo(Namespace.of("foo1", "foo2", "foo3"));
  }

  @Test
  public void testCopyOnWriteRefusesATableWithoutIdentifierFieldsNamingIt() {
    // a connector fanning out to many tables fails the whole task on this; a message that does not
    // say which table leaves the operator to find it by elimination
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.COPY_ON_WRITE, List.of()));

    assertThatThrownBy(() -> factory.createWriter(UNKEYED.toString(), sample(), false))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(UNKEYED.toString())
        .hasMessageContaining("id-columns");
  }

  @Test
  public void testCopyOnWriteRefusalFailsCreateWriterRatherThanDroppingRecords() {
    // an optional key would reach the normalizer, the pruning predicate and the cursor as null. The
    // refusal has to leave createWriter as an exception even when missing tables are ignored: a
    // NoOpWriter would discard every record for the table without a word
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.COPY_ON_WRITE, ImmutableList.of("data")));

    assertThatThrownBy(() -> factory.createWriter(KEYED.toString(), sample(), true))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(KEYED.toString())
        .hasMessageContaining("data");
  }

  @Test
  public void testCopyOnWriteTableModeMismatchFailsCreateWriter() {
    inMemoryCatalog
        .loadTable(KEYED)
        .updateProperties()
        .set(TableProperties.DELETE_MODE, "merge-on-read")
        .commit();
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.COPY_ON_WRITE, List.of()));

    assertThatThrownBy(() -> factory.createWriter(KEYED.toString(), sample(), true))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(KEYED.toString())
        .hasMessageContaining(TableProperties.DELETE_MODE);
  }

  @Test
  public void testCopyOnWriteStagesATableThatPassesTheChecks() {
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.COPY_ON_WRITE, List.of()));

    assertThat(factory.createWriter(KEYED.toString(), sample(), false))
        .isInstanceOf(CopyOnWriteStagingWriter.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  public void testCopyOnWriteWithoutAChangeStreamFailsCreateWriterRatherThanWritingAppends(
      String cdcField) {
    // the config refuses this combination, but a plain append writer here would report DataWritten,
    // which the copy-on-write committer does not consume: the records would be acknowledged and
    // never land. The factory has to fail on its own should that check ever be weakened or moved
    IcebergSinkConfig config = config(RowLevelOperationMode.COPY_ON_WRITE, List.of());
    when(config.isUpsertMode()).thenReturn(false);
    when(config.tablesCdcField()).thenReturn(cdcField);
    IcebergWriterFactory factory = new IcebergWriterFactory(inMemoryCatalog, config);

    assertThatThrownBy(() -> factory.createWriter(KEYED.toString(), sample(), true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(KEYED.toString())
        .hasMessageContaining("iceberg.tables.cdc-field")
        .hasMessageContaining("iceberg.tables.upsert-mode-enabled");
  }

  @ParameterizedTest
  @ValueSource(strings = {"_op", "_topic", "_partition", "_offset"})
  public void testCopyOnWriteRefusesATableWithAColumnNamedLikeAServiceColumn(String column) {
    // a merge-on-read table enriched with, say, InsertField$Value offset.field=_offset keeps
    // writing there; in copy-on-write the staged file schema would hold two fields of that name.
    // The refusal has to be a ConfigException naming the table and the column, not a schema
    // validation failure from inside the writer on every restart
    TableIdentifier enriched = TableIdentifier.of(NAMESPACE, "enriched");
    inMemoryCatalog.createTable(
        enriched,
        new Schema(
            ImmutableList.<NestedField>builder()
                .addAll(COLUMNS)
                .add(optional(3, column, LongType.get()))
                .build(),
            ImmutableSet.of(1)));
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.COPY_ON_WRITE, List.of()));

    assertThatThrownBy(() -> factory.createWriter(enriched.toString(), sample(), true))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(enriched.toString())
        .hasMessageContaining(column)
        .hasMessageContaining("[_op, _topic, _partition, _offset]");
  }

  @Test
  public void testCopyOnWriteStagesATableWithAServiceColumnNameNestedInAStruct() {
    // a nested field's full name is kafka._offset, which the staged schema's _offset does not clash
    // with; refusing it would turn away a table copy-on-write can stage
    TableIdentifier nested = TableIdentifier.of(NAMESPACE, "nested");
    inMemoryCatalog.createTable(
        nested,
        new Schema(
            ImmutableList.<NestedField>builder()
                .addAll(COLUMNS)
                .add(optional(3, "kafka", StructType.of(optional(4, "_offset", LongType.get()))))
                .build(),
            ImmutableSet.of(1)));
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.COPY_ON_WRITE, List.of()));

    assertThat(factory.createWriter(nested.toString(), sample(), false))
        .isInstanceOf(CopyOnWriteStagingWriter.class);
  }

  @Test
  public void testMergeOnReadKeepsTheIdColumnsCopyOnWriteRefuses() {
    // the identifier checks belong to copy-on-write alone: an installation that never enabled it
    // has to go on writing after the upgrade exactly as it did before
    IcebergWriterFactory factory =
        new IcebergWriterFactory(
            inMemoryCatalog, config(RowLevelOperationMode.MERGE_ON_READ, ImmutableList.of("data")));

    assertThat(factory.createWriter(KEYED.toString(), sample(), false))
        .isInstanceOf(IcebergWriter.class);
    assertThat(factory.createWriter(UNKEYED.toString(), sample(), false))
        .isInstanceOf(IcebergWriter.class);
  }

  @ParameterizedTest
  @EnumSource(RowLevelOperationMode.class)
  public void testStatisticsOnForTableGiveWriterSketches(RowLevelOperationMode mode) {
    IcebergSinkConfig config = config(mode, List.of());
    statistics(config, true);
    IcebergWriterFactory factory = new IcebergWriterFactory(inMemoryCatalog, config);

    List<RecordWriteResult> results = writeTwoRows(factory);

    Table table = inMemoryCatalog.loadTable(KEYED);
    SketchFile sketchFile = sketchFileOf(results.get(0));
    assertThat(sketchFile).as("a file for the rows").isNotNull();
    String stagingDirectory =
        StagedChangeFileWriter.stagingDirectory(
            StagedChangeFileWriter.stagingLocation(table, null),
            TableReference.of("catalog", KEYED),
            "cg-connect");
    assertThat(sketchFile.location()).startsWith(stagingDirectory + "/stats/" + COMMIT_ID + "-0-");
    SketchFiles.Content content = SketchFiles.read(table.io(), sketchFile);
    assertThat(content.properties()).containsEntry("row-count", "2");
    assertThat(content.ndv()).containsEntry(1, 2.0).containsEntry(2, 2.0);
  }

  @ParameterizedTest
  @EnumSource(RowLevelOperationMode.class)
  public void testStatisticsOffForTableGiveWriterNoSketches(RowLevelOperationMode mode) {
    IcebergSinkConfig config = config(mode, List.of());
    statistics(config, false);
    SketchFiles.RecordingFileIO io = new SketchFiles.RecordingFileIO();
    IcebergWriterFactory factory = new IcebergWriterFactory(catalogWithFileIo(io), config);

    List<RecordWriteResult> results = writeTwoRows(factory);

    assertThat(sketchFileOf(results.get(0))).isNull();
    assertThat(io.otherFiles()).as("the rows were written").isNotEmpty();
    assertThat(io.statsFilesAttempted()).as("no file asked for in stats/").isEmpty();
    assertThat(io.statsFiles()).isEmpty();
  }

  @Test
  public void testStatisticsThatCannotBeSetUpDoNotFailWriter() {
    IcebergSinkConfig config = config(RowLevelOperationMode.MERGE_ON_READ, List.of());
    statistics(config, true);
    // the staging directory cannot be named without the group id
    when(config.connectGroupId()).thenReturn(null);
    IcebergWriterFactory factory = new IcebergWriterFactory(inMemoryCatalog, config);

    List<RecordWriteResult> results = writeTwoRows(factory);

    assertThat(results).hasSize(1);
    assertThat(((IcebergWriterResult) results.get(0)).dataFiles()).isNotEmpty();
    assertThat(sketchFileOf(results.get(0))).isNull();
  }

  private Catalog catalogWithFileIo(SketchFiles.RecordingFileIO io) {
    Catalog wrapped = mock(Catalog.class, delegatesTo(inMemoryCatalog));
    doAnswer(
            invocation -> {
              // the writer reads the format version through the table operations
              Table table =
                  mock(
                      Table.class,
                      withSettings()
                          .extraInterfaces(HasTableOperations.class)
                          .defaultAnswer(delegatesTo(inMemoryCatalog.loadTable(KEYED))));
              doReturn(io).when(table).io();
              return table;
            })
        .when(wrapped)
        .loadTable(any());
    return wrapped;
  }

  private List<RecordWriteResult> writeTwoRows(IcebergWriterFactory factory) {
    RecordWriter writer = factory.createWriter(KEYED.toString(), sample(), false);
    writer.write(row(1L, "a"));
    writer.write(row(2L, "b"));
    List<RecordWriteResult> results = writer.complete(COMMIT_ID);
    writer.close();
    assertThat(results).hasSize(1);
    return results;
  }

  private static SketchFile sketchFileOf(RecordWriteResult result) {
    return result.kind() == RecordWriteResult.Kind.DATA_FILES
        ? ((IcebergWriterResult) result).sketchFile()
        : ((StagedChangesResult) result).sketchFile();
  }

  private static SinkRecord row(long id, String data) {
    return new SinkRecord(
        "topic",
        0,
        null,
        "key",
        null,
        ImmutableMap.of("id", id, "data", data),
        id,
        0L,
        TimestampType.LOG_APPEND_TIME);
  }

  private static void statistics(IcebergSinkConfig config, boolean enabled) {
    TableSinkConfig tableConfig = config.tableConfig(KEYED.toString());
    when(tableConfig.statisticsEnabled()).thenReturn(enabled);
    // the mock answers an empty list, which is an explicit list without a column
    when(tableConfig.statisticsColumns()).thenReturn(null);
    when(config.statisticsLgNominalEntries()).thenReturn(12);
  }

  private static SinkRecord sample() {
    return mock(SinkRecord.class);
  }

  private static IcebergSinkConfig config(RowLevelOperationMode mode, List<String> idColumns) {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.idColumns()).thenReturn(idColumns);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("cg-connect");
    when(config.rowLevelMode()).thenReturn(mode);
    when(config.isCopyOnWriteMode()).thenReturn(mode == RowLevelOperationMode.COPY_ON_WRITE);
    // copy-on-write requires a change stream; upsert is the one that needs no record field
    when(config.isUpsertMode()).thenReturn(true);
    when(config.taskId()).thenReturn("0");
    when(config.tableConfig(any())).thenReturn(tableConfig);
    return config;
  }
}
