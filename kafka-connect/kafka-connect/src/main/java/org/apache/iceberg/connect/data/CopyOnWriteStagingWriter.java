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

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The copy-on-write counterpart of {@link IcebergWriter}: it does not touch the table, it stages
 * the changes for the coordinator to plan a rewrite from.
 */
class CopyOnWriteStagingWriter implements RecordWriter {

  private final TableReference tableReference;
  private final Table table;
  private final IcebergSinkConfig config;
  private final Set<Integer> identifierFieldIds;
  private final CdcOperations cdcOperations;
  private final StagedChangeFileWriter fileWriter;
  private final Set<String> sourceTopics;
  private RecordConverter recordConverter;

  CopyOnWriteStagingWriter(
      Table table,
      TableReference tableReference,
      IcebergSinkConfig config,
      Set<Integer> identifierFieldIds) {
    this.table = table;
    this.tableReference = tableReference;
    this.config = config;
    this.identifierFieldIds = identifierFieldIds;
    this.cdcOperations = new CdcOperations(config);
    this.recordConverter = new RecordConverter(table, config);
    this.sourceTopics = Sets.newHashSet();
    this.fileWriter =
        new StagedChangeFileWriter(
            table,
            tableReference,
            identifierFieldIds,
            config.copyOnWriteStagingLocation(),
            config.connectGroupId(),
            config.taskId());
  }

  @Override
  public void write(SinkRecord record) {
    if (record.value() == null) {
      // ignore tombstones...
      return;
    }

    Optional<String> rawOperation;
    Record tableRow;
    try {
      rawOperation = cdcOperations.rawOperation(record);
      if (rawOperation.filter(cdcOperations::isIgnored).isPresent()) {
        return;
      }

      tableRow = convertToRow(record);
    } catch (Exception e) {
      throw conversionFailure(record, e);
    }

    // outside the conversion's catch, so the message itself names the field
    checkIdentifierValues(record, tableRow);

    try {
      fileWriter.write(
          fileWriter.stagedRow(
              tableRow,
              opCode(rawOperation),
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset()));
      sourceTopics.add(record.topic());
    } catch (Exception e) {
      throw conversionFailure(record, e);
    }
  }

  private static DataException conversionFailure(SinkRecord record, Exception cause) {
    return new DataException(
        String.format(
            Locale.ROOT,
            "An error occurred converting record, topic: %s, partition, %d, offset: %d",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset()),
        cause);
  }

  /**
   * Fails a record whose identifier fields are not all set.
   *
   * <p>A change row is applied by its key, so there is nothing to skip to: the task stops. Left to
   * the appender, the null fails on the required column with an error that does not say which field
   * is empty: on a composite key the operator would have to guess.
   */
  private void checkIdentifierValues(SinkRecord record, Record tableRow) {
    for (int fieldId : identifierFieldIds) {
      if (tableRow.getField(tableRow.struct().field(fieldId).name()) == null) {
        throw new DataException(
            String.format(
                Locale.ROOT,
                "Identifier fields %s have no value, topic: %s, partition: %d, offset: %d",
                emptyIdentifierFields(tableRow),
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset()));
      }
    }
  }

  private List<String> emptyIdentifierFields(Record tableRow) {
    List<String> names = Lists.newArrayList();
    for (NestedField field : tableRow.struct().fields()) {
      if (identifierFieldIds.contains(field.fieldId()) && tableRow.getField(field.name()) == null) {
        names.add(field.name());
      }
    }
    return names;
  }

  /**
   * Converts a record, evolving the table's schema first if the record carries something new.
   *
   * <p>The same contract as {@link IcebergWriter}'s: without it a connector switched to
   * copy-on-write would silently stop evolving schemas, and a new field would reach neither the
   * table nor the staged file.
   */
  private Record convertToRow(SinkRecord record) {
    if (!config.evolveSchemaEnabled()) {
      return recordConverter.convert(record.value());
    }

    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    Record row = recordConverter.convert(record.value(), updates);
    if (updates.empty()) {
      return row;
    }

    SchemaUtils.applySchemaUpdates(table, updates);
    fileWriter.refreshSchema();
    this.recordConverter = new RecordConverter(table, config);
    return recordConverter.convert(record.value(), null);
  }

  /**
   * Maps a record to the operation stored in the staged file.
   *
   * <p>Without a CDC field, upsert mode makes every record an update: the connector is told the
   * stream is keyed, and the row either replaces an existing one or is inserted.
   */
  private int opCode(Optional<String> rawOperation) {
    Operation operation = rawOperation.map(cdcOperations::operation).orElse(null);
    if (operation == null) {
      return config.isUpsertMode() ? StagedChangeSchema.OP_UPDATE : StagedChangeSchema.OP_INSERT;
    }

    switch (operation) {
      case DELETE:
        return StagedChangeSchema.OP_DELETE;
      case UPDATE:
        return StagedChangeSchema.OP_UPDATE;
      default:
        return StagedChangeSchema.OP_INSERT;
    }
  }

  @Override
  public List<RecordWriteResult> complete() {
    List<StagedChangeFile> stagedFiles = fileWriter.complete();
    if (stagedFiles.isEmpty()) {
      sourceTopics.clear();
      return ImmutableList.of();
    }

    RecordWriteResult result =
        new StagedChangesResult(
            tableReference, config.taskId(), stagedFiles, Set.copyOf(sourceTopics));
    sourceTopics.clear();
    return ImmutableList.of(result);
  }

  @Override
  public void close() {
    fileWriter.close();
  }
}
