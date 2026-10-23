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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects theta sketches of the statistics columns over the rows a table writer writes in one
 * commit cycle, and writes them to the intermediate Puffin file of the writer.
 *
 * <p>A column keeps its sketch (VALID) only while the sketch has seen every row written with the
 * column in the writer's schema; otherwise it is INVALID until the end of the cycle and gets no
 * blob. The file property {@link #LAST_COLUMN_ID_PROPERTY} tells the coordinator which missing
 * blobs mean "not covered" and which mean "the writer did not know the column".
 *
 * <p>No method throws: statistics never stop writing data.
 */
public class ColumnSketchCollector {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnSketchCollector.class);

  /** Subdirectory of the staging directory with the intermediate Puffin files. */
  public static final String STATS_DIR = "stats";

  /** File property: the highest field id over the schemas the writer had in the cycle. */
  public static final String LAST_COLUMN_ID_PROPERTY = "last-column-id";

  /** File property: the number of rows passed to {@link #update(Record)} in the cycle. */
  public static final String ROW_COUNT_PROPERTY = "row-count";

  /** Blob property: the Iceberg type the values of the blob were serialized by. */
  public static final String TYPE_PROPERTY = "type";

  private static final String CREATED_BY = "iceberg-kafka-connect " + IcebergBuild.version();

  private final TableIdentifier identifier;
  private final Table table;
  private final String stagingDirectory;
  private final String taskId;
  private final int lgK;
  private final List<String> explicitColumns;
  private final boolean caseInsensitive;

  // valid columns in the order of the set; columns dropped from the schema go last
  private List<Column> valid = Lists.newArrayList();
  private final Set<Integer> invalid = Sets.newHashSet();
  private int lastColumnId;
  private long rowCount = 0L;
  private boolean failed = false;

  /**
   * @param taskId the task id for the file name, or {@code null} when the task has none
   * @param explicitColumns the configured statistics columns, or {@code null} for the default set
   */
  public ColumnSketchCollector(
      TableIdentifier identifier,
      Table table,
      String stagingDirectory,
      String taskId,
      int lgK,
      List<String> explicitColumns,
      boolean caseInsensitive,
      Schema schema) {
    this.identifier = identifier;
    this.table = table;
    this.stagingDirectory = stagingDirectory;
    this.taskId = taskId;
    this.lgK = lgK;
    this.explicitColumns = explicitColumns;
    this.caseInsensitive = caseInsensitive;

    try {
      this.lastColumnId = schema.highestFieldId();
      Map<Integer, Integer> positions = positions(schema);
      for (Types.NestedField field : resolve(schema)) {
        valid.add(new Column(field, SketchValues.newSketch(lgK), positions.get(field.fieldId())));
      }
    } catch (RuntimeException e) {
      fail(e);
    }
  }

  /** Adds the values of a row that was written successfully; {@code null} values are skipped. */
  public void update(Record row) {
    if (failed) {
      return;
    }

    try {
      rowCount += 1;
      for (Iterator<Column> iterator = valid.iterator(); iterator.hasNext(); ) {
        Column column = iterator.next();
        if (column.position < 0) {
          continue; // dropped from the schema: rows have no values of it
        }

        try {
          ByteBuffer bytes = SketchValues.toByteBuffer(column.type, row.get(column.position));
          SketchValues.update(column.sketch, bytes);
        } catch (RuntimeException e) {
          iterator.remove();
          invalid.add(column.fieldId);
          LOG.warn(
              "Statistics of column {} (field id {}) of table {} are dropped for this commit: "
                  + "cannot read or hash a value of type {}",
              column.name,
              column.fieldId,
              identifier,
              column.type,
              e);
        }
      }
    } catch (RuntimeException e) {
      fail(e);
    }
  }

  /** Moves the sketches to a new schema of the writer. */
  public void schemaChanged(Schema schema) {
    if (failed) {
      return;
    }

    try {
      Map<Integer, Column> byId = Maps.newHashMap();
      valid.forEach(column -> byId.put(column.fieldId, column));
      Map<Integer, Integer> positions = positions(schema);
      Set<Integer> inSet = Sets.newHashSet();
      List<Column> next = Lists.newArrayList();

      for (Types.NestedField field : resolve(schema)) {
        int fieldId = field.fieldId();
        inSet.add(fieldId);
        Column column = byId.get(fieldId);
        if (invalid.contains(fieldId)) {
          continue;
        } else if (column != null && column.type.equals(field.type())) {
          column.position = positions.get(fieldId);
          next.add(column);
        } else if (column == null && fieldId > lastColumnId) {
          // a new column: rows written before had no values of it
          next.add(new Column(field, SketchValues.newSketch(lgK), positions.get(fieldId)));
        } else {
          // another type, or a column rows had values of without the sketch seeing them
          invalid.add(fieldId);
        }
      }

      for (Column column : valid) {
        if (inSet.contains(column.fieldId)) {
          continue;
        } else if (schema.findField(column.fieldId) == null) {
          // dropped: later rows have no values of it, so the sketch stays complete
          column.position = -1;
          next.add(column);
        } else {
          invalid.add(column.fieldId);
        }
      }

      this.valid = next;
      this.lastColumnId = Math.max(lastColumnId, schema.highestFieldId());
    } catch (RuntimeException e) {
      fail(e);
    }
  }

  /**
   * Writes the intermediate Puffin file with a blob per VALID column.
   *
   * @return the reference to the file, or {@code null} if it could not be written
   */
  public SketchFile write(UUID commitId) {
    if (failed) {
      return null;
    }

    String path = null;
    try {
      path = path(commitId);
      SketchFile file;
      try (PuffinWriter writer =
          Puffin.write(table.io().newOutputFile(path))
              .createdBy(CREATED_BY)
              .set(LAST_COLUMN_ID_PROPERTY, String.valueOf(lastColumnId))
              .set(ROW_COUNT_PROPERTY, String.valueOf(rowCount))
              .compressBlobs(PuffinCompressionCodec.NONE)
              .build()) {
        for (Column column : valid) {
          writer.add(
              new Blob(
                  StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                  ImmutableList.of(column.fieldId),
                  -1L,
                  -1L,
                  ByteBuffer.wrap(column.sketch.compact().toByteArray()),
                  PuffinCompressionCodec.NONE,
                  ImmutableMap.of(TYPE_PROPERTY, column.type.toString())));
        }

        writer.finish();
        file = new SketchFile(path, writer.fileSize(), writer.footerSize());
      }

      LOG.debug(
          "Wrote intermediate statistics file {} of table {} with {} blobs",
          path,
          identifier,
          valid.size());
      return file;
    } catch (Exception e) {
      LOG.warn(
          "Failed to write intermediate statistics file {} of table {}, its data files will be "
              + "read by the coordinator instead",
          path,
          identifier,
          e);
      deleteQuietly(path);
      return null;
    }
  }

  private String path(UUID commitId) {
    StringBuilder name = new StringBuilder().append(commitId).append('-');
    if (taskId != null) {
      name.append(taskId).append('-');
    }

    name.append(UUID.randomUUID()).append(".puffin");
    return stagingDirectory + "/" + STATS_DIR + "/" + name;
  }

  private void deleteQuietly(String path) {
    if (path == null) {
      return;
    }

    try {
      table.io().deleteFile(path);
    } catch (RuntimeException e) {
      LOG.debug("Failed to delete intermediate statistics file {}", path, e);
    }
  }

  private List<Types.NestedField> resolve(Schema schema) {
    return StatisticsColumns.resolve(identifier, schema, explicitColumns, caseInsensitive);
  }

  private static Map<Integer, Integer> positions(Schema schema) {
    Map<Integer, Integer> positions = Maps.newHashMap();
    List<Types.NestedField> columns = schema.columns();
    for (int pos = 0; pos < columns.size(); pos += 1) {
      positions.put(columns.get(pos).fieldId(), pos);
    }

    return positions;
  }

  private void fail(RuntimeException cause) {
    this.failed = true;
    this.valid = Lists.newArrayList();
    LOG.warn(
        "Statistics of table {} are dropped for this commit: failed to collect sketches",
        identifier,
        cause);
  }

  private static class Column {
    private final int fieldId;
    private final String name;
    private final Type.PrimitiveType type;
    private final UpdateSketch sketch;
    // index in schema.columns() of the writer's schema, -1 once the column is dropped from it
    private int position;

    private Column(Types.NestedField field, UpdateSketch sketch, int position) {
      this.fieldId = field.fieldId();
      this.name = field.name();
      this.type = field.type().asPrimitiveType();
      this.sketch = sketch;
      this.position = position;
    }
  }
}
