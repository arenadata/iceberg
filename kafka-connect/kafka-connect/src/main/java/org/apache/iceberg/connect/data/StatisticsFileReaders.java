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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileReader;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PartitionUtil;

/**
 * Reads the rows of the files the statistics step recounts: data files of the gap and of
 * merge-on-read answers, staged change files of copy-on-write answers.
 *
 * <p>Rows come back as the file holds them: deletes are not applied, since a sketch counts the
 * values ever inserted, not the live ones.
 */
public class StatisticsFileReaders {

  private StatisticsFileReaders() {}

  /** The name mapping of the table, or {@code null}; it resolves files without field ids. */
  public static NameMapping nameMapping(Table table) {
    String json = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    return json != null ? NameMappingParser.fromJson(json) : null;
  }

  /**
   * Reads all rows of a data file, projected onto {@code projection}; columns the file does not
   * have come back {@code null} or as their {@code initial-default}.
   *
   * @param spec the spec of the file: identity partition values come from its partition tuple
   */
  public static CloseableIterable<Record> dataFile(
      FileIO io, DataFile file, PartitionSpec spec, Schema projection, NameMapping nameMapping) {
    return dataFile(io, new WholeFile(file, spec), projection, nameMapping);
  }

  /**
   * Reads the split of its data file a scan task covers, projected onto {@code projection}, with no
   * deletes applied.
   */
  public static CloseableIterable<Record> dataFile(
      FileIO io, ContentScanTask<DataFile> task, Schema projection, NameMapping nameMapping) {
    InputFile input = io.newInputFile(task.file());
    Map<Integer, ?> partition =
        PartitionUtil.constantsMap(task, StatisticsFileReaders::genericValue);

    switch (task.file().format()) {
      case AVRO:
        return Avro.read(input)
            .project(projection)
            .createResolvingReader(schema -> PlannedDataReader.create(schema, partition))
            .withNameMapping(nameMapping)
            .split(task.start(), task.length())
            .build();

      case PARQUET:
        return Parquet.read(input)
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema, partition))
            .withNameMapping(nameMapping)
            .split(task.start(), task.length())
            .build();

      case ORC:
        Schema withoutConstants =
            TypeUtil.selectNot(
                projection, Sets.union(partition.keySet(), MetadataColumns.metadataFieldIds()));
        return ORC.read(input)
            .project(withoutConstants)
            .createReaderFunc(
                fileSchema -> GenericOrcReader.buildReader(projection, fileSchema, partition))
            .withNameMapping(nameMapping)
            .split(task.start(), task.length())
            .build();

      default:
        throw new UnsupportedOperationException(
            "Cannot read " + task.file().format() + " file: " + task.file().location());
    }
  }

  /**
   * Reads the inserted and updated rows of a staged change file, projected onto {@code projection}:
   * a delete adds no value to a column. Rows are valid until the next one is read.
   */
  public static CloseableIterable<Record> stagedFile(
      FileIO io, StagedChangeFile file, Schema projection) {
    Schema readSchema = withOp(projection);
    int opPosition = readSchema.columns().size() - 1;
    CloseableIterable<Record> rows =
        CloseableIterable.filter(
            stagedFileWithOp(io, file, projection),
            row -> addsValues(row.get(opPosition, Integer.class)));
    RecordProjection projector = RecordProjection.create(readSchema, projection);
    return CloseableIterable.transform(rows, projector::wrap);
  }

  /**
   * Reads all rows of a staged change file, deletes too, projected onto {@code projection} and
   * followed by {@code _op}.
   */
  static CloseableIterable<Record> stagedFileWithOp(
      FileIO io, StagedChangeFile file, Schema projection) {
    return StagedChangeFileReader.open(io, file, withOp(projection));
  }

  /** Whether a staged row of the operation adds values to its columns: an insert or an update. */
  static boolean addsValues(Integer op) {
    return op != null && (op == StagedChangeSchema.OP_INSERT || op == StagedChangeSchema.OP_UPDATE);
  }

  private static Schema withOp(Schema projection) {
    List<Types.NestedField> columns = Lists.newArrayList(projection.columns());
    columns.add(
        Types.NestedField.required(
            StagedChangeSchema.OP_ID, StagedChangeSchema.OP, Types.IntegerType.get()));
    return new Schema(columns);
  }

  /**
   * An identity partition value as a generic row holds it: readers put these constants in place of
   * their columns, so a date must be a {@code LocalDate} and a {@code fixed} a {@code byte[]}, not
   * the day count or the {@code ByteBuffer} of the partition tuple.
   */
  private static Object genericValue(Type type, Object value) {
    if (type.typeId() == Type.TypeID.FIXED && value instanceof ByteBuffer) {
      return ByteBuffers.toByteArray((ByteBuffer) value);
    }
    return IdentityPartitionConverters.convertConstant(type, value);
  }

  /** A whole data file as a scan task, for its partition constants and split. */
  private static class WholeFile implements ContentScanTask<DataFile> {
    private final DataFile file;
    private final PartitionSpec spec;

    private WholeFile(DataFile file, PartitionSpec spec) {
      this.file = file;
      this.spec = spec;
    }

    @Override
    public DataFile file() {
      return file;
    }

    @Override
    public PartitionSpec spec() {
      return spec;
    }

    @Override
    public long start() {
      return 0L;
    }

    @Override
    public long length() {
      return file.fileSizeInBytes();
    }

    @Override
    public Expression residual() {
      return Expressions.alwaysTrue();
    }
  }
}
