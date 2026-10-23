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
package org.apache.iceberg.connect.data.copyonwrite;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.LocationUtil;

/**
 * Writes copy-on-write change rows to staged Avro files.
 *
 * <p>Avro rather than Parquet: the file is written once and read once, whole and in row order, so
 * nothing pays for column layout, and the writing happens on the worker's poll thread.
 *
 * <p>One file per commit cycle. The path carries the task id and a writer epoch rather than the
 * commit id: rows are written during {@code put()}, and the commit id only arrives later with
 * {@code StartCommit}. Grouping by commit moves to the coordinator's change set manifest.
 *
 * <p>Knows nothing about Kafka Connect or the control topic; {@code CopyOnWriteStagingWriter} is
 * what plugs it into the sink.
 */
public class StagedChangeFileWriter {

  private static final String STAGING_DIR = "kc-copy-on-write-staging";

  private final Table table;
  private final Set<Integer> identifierFieldIds;
  private final String directory;
  private Schema stagedSchema;
  private final List<StagedChangeFile> completedFiles;
  private List<BoundsColumn> boundsColumns;
  private InternalRecordWrapper internalView;

  private FileAppender<Record> appender;
  private OutputFile currentFile;
  private long recordCount;
  private int seq;
  // captured when the file is opened, not read at close: a schema evolution closes the file in
  // progress, and by then the table already carries the new schema, which this file is not
  // written with
  private int fileSchemaId;
  private final Map<Integer, Object> lowerBounds;
  private final Map<Integer, Object> upperBounds;

  public StagedChangeFileWriter(
      Table table,
      TableReference tableReference,
      Set<Integer> identifierFieldIds,
      String stagingLocation,
      String groupId,
      String taskId) {
    this.table = table;
    this.identifierFieldIds = identifierFieldIds;
    this.completedFiles = Lists.newArrayList();
    this.lowerBounds = Maps.newHashMap();
    this.upperBounds = Maps.newHashMap();
    adoptTableSchema();

    // a writer is recreated every commit cycle, so the epoch separates the files of one writer
    // from those of its predecessor, the way OutputFileFactory uses an operation id
    String writerEpoch = UUID.randomUUID().toString();
    // table / group / task / epoch, in that order: the staging location may be shared by a fan-out
    // connector, several connectors may write one table and each sweeps only its own group, the
    // task id gives sweeping a narrow prefix, and the epoch separates writers
    this.directory =
        String.format(
            "%s/%s/%s",
            stagingDirectory(stagingLocation(table, stagingLocation), tableReference, groupId),
            taskId,
            writerEpoch);
  }

  /**
   * Resolves the configured staging location, or the table-relative default if none is set.
   *
   * <p>Public so the coordinator locates a change set's manifest by the same rule the worker used
   * to place its staged files.
   *
   * <p>A trailing slash of the configured value is dropped: every path is joined under it with a
   * separator of its own, and the sweep compares the locations it lists with the written ones as
   * plain strings.
   */
  public static String stagingLocation(Table table, String configured) {
    return configured != null
        ? LocationUtil.stripTrailingSlash(configured)
        : table.location() + "/" + STAGING_DIR;
  }

  /**
   * One connector's staging directory for one table, {@code <staging>/<tableDir>/<groupDir>}:
   * staged files, change set manifests, normalized files and the sweep prefix all live under it,
   * and this is the only place that spells it.
   *
   * <p>The table part is the namespace levels and the table name joined with {@code .}; the group
   * part is the connector's group id, so the sweep of one connector never lists another's files. In
   * every part {@code .}, {@code /} and {@code %} are escaped as {@code %XX}: otherwise table
   * {@code c} in namespace {@code a.b} and table {@code b.c} in namespace {@code a} share a
   * directory, and the sweep of one deletes the files of the other. Names without those characters
   * keep the directory they print as.
   */
  public static String stagingDirectory(
      String stagingLocation, TableReference tableReference, String groupId) {
    Preconditions.checkArgument(groupId != null, "Invalid group id: null");
    TableIdentifier identifier = tableReference.identifier();
    StringBuilder directory = new StringBuilder(stagingLocation).append('/');
    for (String level : identifier.namespace().levels()) {
      appendEscaped(directory, level).append('.');
    }
    appendEscaped(directory, identifier.name()).append('/');
    return appendEscaped(directory, groupId).toString();
  }

  private static StringBuilder appendEscaped(StringBuilder directory, String part) {
    for (int i = 0; i < part.length(); i += 1) {
      char ch = part.charAt(i);
      if (ch == '.') {
        directory.append("%2E");
      } else if (ch == '/') {
        directory.append("%2F");
      } else if (ch == '%') {
        directory.append("%25");
      } else {
        directory.append(ch);
      }
    }
    return directory;
  }

  /**
   * Picks up the table's current schema, closing the file in progress first.
   *
   * <p>Called after the sink evolves the table. The appender is bound to the schema it was opened
   * with, so the file has to end here; it joins {@link #complete()}'s result with its own schema
   * id, and the change set that adopts it reads it back by field id: a set whose files were written
   * under two schemas normalizes as one.
   */
  public void refreshSchema() {
    closeFile();
    adoptTableSchema();
  }

  private void adoptTableSchema() {
    this.stagedSchema = StagedChangeSchema.stagedSchema(table.schema(), identifierFieldIds);
    this.boundsColumns = boundsColumns(stagedSchema, identifierFieldIds);
    this.internalView = new InternalRecordWrapper(stagedSchema.asStruct());
  }

  public void write(Record stagedRow) {
    if (appender == null) {
      openFile();
    }

    appender.add(stagedRow);
    recordCount += 1;
    trackBounds(stagedRow);
  }

  public Record stagedRow(Record tableRow, int op, String topic, int partition, long offset) {
    GenericRecord row = GenericRecord.create(stagedSchema);
    for (NestedField field : table.schema().columns()) {
      row.setField(field.name(), tableRow.getField(field.name()));
    }
    row.setField(StagedChangeSchema.OP, op);
    row.setField(StagedChangeSchema.TOPIC, topic);
    row.setField(StagedChangeSchema.PARTITION, partition);
    row.setField(StagedChangeSchema.OFFSET, offset);
    return row;
  }

  public List<StagedChangeFile> complete() {
    closeFile();
    List<StagedChangeFile> result = ImmutableList.copyOf(completedFiles);
    completedFiles.clear();
    return result;
  }

  public void close() {
    closeFile();
  }

  public Schema stagedSchema() {
    return stagedSchema;
  }

  private void openFile() {
    this.currentFile = table.io().newOutputFile(directory + "/" + seq + ".avro");
    this.fileSchemaId = table.schema().schemaId();
    seq += 1;
    try {
      this.appender =
          Avro.write(currentFile)
              .schema(stagedSchema)
              .createWriterFunc(DataWriter::create)
              .named("staged_change")
              .meta(
                  StagedChangeSchema.FORMAT_VERSION_META,
                  Integer.toString(StagedChangeSchema.FORMAT_VERSION))
              .meta(StagedChangeSchema.SCHEMA_ID_META, Integer.toString(fileSchemaId))
              .meta(
                  StagedChangeSchema.IDENTIFIER_FIELD_IDS_META,
                  StagedChangeSchema.identifierFieldIdsMeta(identifierFieldIds))
              .overwrite()
              .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void closeFile() {
    if (appender == null) {
      return;
    }

    try {
      appender.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    completedFiles.add(
        new StagedChangeFile(
            currentFile.location(),
            appender.length(),
            recordCount,
            fileSchemaId,
            StagedChangeSchema.FORMAT_VERSION,
            serializeBounds(lowerBounds),
            serializeBounds(upperBounds),
            identifierFieldIds));

    this.appender = null;
    this.currentFile = null;
    this.recordCount = 0;
    lowerBounds.clear();
    upperBounds.clear();
  }

  /**
   * Tracks per-identifier-column bounds.
   *
   * <p>Avro carries no column statistics, so they are collected here. {@code ChangeSetNormalizer}
   * uses the leading identifier column's bounds to skip a staged file whole (either already behind
   * the cursor or past the end of a full slice), which is what stops a change set that drains over
   * many slices from re-reading every staged file for every one of them.
   *
   * <p>Values are read through {@link InternalRecordWrapper}: a bound is serialized with {@code
   * Conversions}, which accepts only the internal representation ({@code Integer} days for a date,
   * {@code Long} micros for a timestamp), while the row itself holds the generic one. Positions and
   * comparators are resolved once per schema: this runs on the poll thread, for every record.
   */
  private void trackBounds(Record row) {
    if (boundsColumns.isEmpty()) {
      return;
    }

    StructLike internal = internalView.wrap(row);
    for (BoundsColumn column : boundsColumns) {
      Object value = internal.get(column.position, Object.class);
      if (value == null) {
        continue;
      }

      lowerBounds.merge(
          column.fieldId, value, (a, b) -> column.comparator.compare(a, b) <= 0 ? a : b);
      upperBounds.merge(
          column.fieldId, value, (a, b) -> column.comparator.compare(a, b) >= 0 ? a : b);
    }
  }

  private Map<Integer, ByteBuffer> serializeBounds(Map<Integer, Object> bounds) {
    Map<Integer, ByteBuffer> result = Maps.newHashMap();
    bounds.forEach(
        (fieldId, value) -> {
          Type type = stagedSchema.findField(fieldId).type();
          result.put(fieldId, Conversions.toByteBuffer(type, value));
        });
    return result;
  }

  private static List<BoundsColumn> boundsColumns(
      Schema stagedSchema, Set<Integer> identifierFieldIds) {
    List<NestedField> columns = stagedSchema.columns();
    List<BoundsColumn> result = Lists.newArrayList();
    for (int pos = 0; pos < columns.size(); pos++) {
      NestedField field = columns.get(pos);
      if (identifierFieldIds.contains(field.fieldId())) {
        result.add(
            new BoundsColumn(
                field.fieldId(), pos, Comparators.forType(field.type().asPrimitiveType())));
      }
    }
    return ImmutableList.copyOf(result);
  }

  /** One identifier column, resolved once so the per-record path stays a positional read. */
  private static final class BoundsColumn {
    private final int fieldId;
    private final int position;
    private final Comparator<Object> comparator;

    private BoundsColumn(int fieldId, int position, Comparator<Object> comparator) {
      this.fieldId = fieldId;
      this.position = position;
      this.comparator = comparator;
    }
  }
}
