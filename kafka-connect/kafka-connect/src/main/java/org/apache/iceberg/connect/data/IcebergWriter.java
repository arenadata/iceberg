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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergWriter implements RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

  private final Table table;
  private final TableReference tableReference;
  private final IcebergSinkConfig config;
  private final List<IcebergWriterResult> writerResults;
  private final CdcOperations cdcOperations;
  private final Set<String> sourceTopics;
  private ColumnSketchCollector sketches;

  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;

  IcebergWriter(Table table, TableReference tableReference, IcebergSinkConfig config) {
    this(table, tableReference, config, (ColumnSketchCollector) null);
  }

  IcebergWriter(
      Table table,
      TableReference tableReference,
      IcebergSinkConfig config,
      ColumnSketchCollector sketches) {
    this(
        table,
        tableReference,
        config,
        RecordUtils.createTableWriter(table, tableReference, config),
        sketches);
  }

  IcebergWriter(
      Table table,
      TableReference tableReference,
      IcebergSinkConfig config,
      TaskWriter<Record> writer) {
    this(table, tableReference, config, writer, null);
  }

  IcebergWriter(
      Table table,
      TableReference tableReference,
      IcebergSinkConfig config,
      TaskWriter<Record> writer,
      ColumnSketchCollector sketches) {
    this.table = table;
    this.tableReference = tableReference;
    this.config = config;
    this.writerResults = Lists.newArrayList();
    this.cdcOperations = new CdcOperations(config);
    this.sourceTopics = Sets.newHashSet();
    this.sketches = sketches;
    this.writer = writer;
    this.recordConverter = new RecordConverter(table, config);
  }

  @Override
  public void write(SinkRecord record) {
    try {
      if (record.value() == null) {
        // ignore tombstones...
        return;
      }

      Optional<String> rawOperation = cdcOperations.rawOperation(record);
      if (rawOperation.filter(cdcOperations::isIgnored).isPresent()) {
        // skip ignored operation
        return;
      }

      // We enrich a record with an operation only if we have a mapping for it.
      // Otherwise, we send a raw record to the downstream writer, allowing him to decide
      // what type of operation to generate, based on the mode (upsert/append).
      Record row =
          rawOperation
              .flatMap(operation -> convertToRowWithOp(record, operation))
              .orElseGet(() -> convertToRow(record));
      writer.write(row);
      updateSketches(row);
      sourceTopics.add(record.topic());
    } catch (Exception e) {
      throw new DataException(
          String.format(
              Locale.ROOT,
              "An error occurred converting record, topic: %s, partition, %d, offset: %d",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset()),
          e);
    }
  }

  /**
   * Must follow the write: a sketch that saw a row the file lacks overstates. A delete is skipped,
   * it writes only the key. Best effort: a failure never fails the write; the sketches would then
   * understate, so they are dropped for the rest of the cycle.
   */
  private void updateSketches(Record row) {
    if (sketches == null || isDelete(row)) {
      return;
    }

    try {
      sketches.update(row);
    } catch (RuntimeException e) {
      sketches = null;
      LOG.warn(
          "Statistics of table {} cannot take a row, they are dropped for this commit and the "
              + "write goes on",
          tableReference,
          e);
    }
  }

  private static boolean isDelete(Record row) {
    return row instanceof RecordWrapper && ((RecordWrapper) row).op() == Operation.DELETE;
  }

  private Optional<Record> convertToRowWithOp(SinkRecord record, String rawOperation) {
    return Optional.ofNullable(cdcOperations.operation(rawOperation))
        .map(operation -> new RecordWrapper(convertToRow(record), operation));
  }

  private Record convertToRow(SinkRecord record) {
    if (!config.evolveSchemaEnabled()) {
      return recordConverter.convert(record.value());
    }

    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    Record row = recordConverter.convert(record.value(), updates);

    if (!updates.empty()) {
      // complete the current file
      flush();
      // apply the schema updates, this will refresh the table
      SchemaUtils.applySchemaUpdates(table, updates);
      // initialize a new writer with the new schema
      initNewWriter();
      if (sketches != null) {
        sketches.schemaChanged(table.schema());
      }
      // convert the row again, this time using the new table schema
      row = recordConverter.convert(record.value(), null);
    }

    return row;
  }

  private void flush() {
    WriteResult writeResult;
    try {
      writeResult = writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    writerResults.add(
        new IcebergWriterResult(
            tableReference,
            Arrays.asList(writeResult.dataFiles()),
            Arrays.asList(writeResult.deleteFiles()),
            table.spec().partitionType(),
            Set.copyOf(sourceTopics)));
    sourceTopics.clear();
  }

  private void initNewWriter() {
    writer = RecordUtils.createTableWriter(table, tableReference, config);
    recordConverter = new RecordConverter(table, config);
  }

  @Override
  public List<RecordWriteResult> complete(UUID commitId) {
    flush();

    List<IcebergWriterResult> completed = Lists.newArrayList(writerResults);
    writerResults.clear();

    // without a data file there is nothing to cover; a delete file carries keys, not values
    SketchFile sketchFile =
        sketches != null && completed.stream().anyMatch(result -> !result.dataFiles().isEmpty())
            ? sketches.write(commitId)
            : null;

    List<RecordWriteResult> result = Lists.newArrayList();
    for (IcebergWriterResult completedResult : completed) {
      result.add(completedResult.withSketchFile(sketchFile));
    }

    return result;
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
