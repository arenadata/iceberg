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
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

class IcebergWriter implements RecordWriter {
  private final Table table;
  private final TableReference tableReference;
  private final IcebergSinkConfig config;
  private final List<IcebergWriterResult> writerResults;
  private final CdcOperations cdcOperations;
  private final Set<String> sourceTopics;

  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;

  IcebergWriter(Table table, TableReference tableReference, IcebergSinkConfig config) {
    this(
        table,
        tableReference,
        config,
        RecordUtils.createTableWriter(table, tableReference, config));
  }

  IcebergWriter(
      Table table,
      TableReference tableReference,
      IcebergSinkConfig config,
      TaskWriter<Record> writer) {
    this.table = table;
    this.tableReference = tableReference;
    this.config = config;
    this.writerResults = Lists.newArrayList();
    this.cdcOperations = new CdcOperations(config);
    this.sourceTopics = Sets.newHashSet();
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
  public List<RecordWriteResult> complete() {
    flush();

    List<RecordWriteResult> result = Lists.newArrayList(writerResults);
    writerResults.clear();

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
