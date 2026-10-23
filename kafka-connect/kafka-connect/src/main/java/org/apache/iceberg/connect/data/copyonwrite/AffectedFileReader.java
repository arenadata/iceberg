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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.connect.data.RecordProjection;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PartitionUtil;

/**
 * Reads a data file's surviving rows: existing equality deletes, position deletes and DVs applied,
 * nothing pruned by the scan's residual.
 *
 * <p>{@code org.apache.iceberg.data.GenericReader}, which does exactly this, is package-private, so
 * this is that class's minimal read path, built on the same public pieces ({@link
 * GenericDeleteFilter}, {@code Parquet}/{@code ORC}/{@code Avro} read builders). Two differences
 * are deliberate:
 *
 * <ul>
 *   <li>the residual is never applied. Iceberg hands a scan task the part of the scan filter it
 *       could not prove from metadata (the residual), expecting the reader to evaluate it per row.
 *       That is wrong here: the scan's predicate is a coarse, over-inclusive stand-in for "might
 *       contain a changed key" (an AND over identifier columns, with a range fallback past the
 *       cardinality quota), chosen to select files, not rows. Evaluating it per row would silently
 *       drop rows the change set never touched from the rewritten remainder, which is to say,
 *       delete them from the table.
 *   <li>rows come back projected onto {@code projection}, not {@link
 *       GenericDeleteFilter#requiredSchema()}. That schema is wider (it adds {@code _pos}, {@code
 *       _deleted} and the columns equality deletes need) and a caller writing rows back out must
 *       not carry those along.
 * </ul>
 */
final class AffectedFileReader {

  private AffectedFileReader() {}

  static CloseableIterable<Record> open(
      FileIO io, FileScanTask task, Schema tableSchema, Schema projection) {
    GenericDeleteFilter deletes = new GenericDeleteFilter(io, task, tableSchema, projection);
    Schema readSchema = deletes.requiredSchema();
    CloseableIterable<Record> records = deletes.filter(openFile(io, task, readSchema));
    RecordProjection projector = RecordProjection.create(readSchema, projection);
    return CloseableIterable.transform(records, projector::wrap);
  }

  private static CloseableIterable<Record> openFile(
      FileIO io, FileScanTask task, Schema readSchema) {
    InputFile input = io.newInputFile(task.file());
    Map<Integer, ?> partition = PartitionUtil.constantsMap(task, AffectedFileReader::genericValue);

    switch (task.file().format()) {
      case AVRO:
        return Avro.read(input)
            .project(readSchema)
            .createResolvingReader(schema -> PlannedDataReader.create(schema, partition))
            .split(task.start(), task.length())
            .build();

      case PARQUET:
        return Parquet.read(input)
            .project(readSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema, partition))
            .split(task.start(), task.length())
            .build();

      case ORC:
        Schema withoutConstants =
            TypeUtil.selectNot(
                readSchema, Sets.union(partition.keySet(), MetadataColumns.metadataFieldIds()));
        return ORC.read(input)
            .project(withoutConstants)
            .createReaderFunc(
                fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema, partition))
            .split(task.start(), task.length())
            .build();

      default:
        throw new UnsupportedOperationException(
            "Cannot read " + task.file().format() + " file: " + task.file().location());
    }
  }

  /**
   * An identity partition value as a generic row holds it.
   *
   * <p>Every reader puts these constants in place of their columns, even where the file has the
   * column, and the rows go straight back to a generic writer: a date must arrive as a {@code
   * LocalDate}, not as the day count the partition tuple stores. {@link
   * IdentityPartitionConverters} converts dates and times, as {@code GenericReader} does; a {@code
   * fixed} value read from a manifest is a {@code ByteBuffer}, which it passes through, while a
   * generic row holds {@code byte[]}.
   */
  private static Object genericValue(Type type, Object value) {
    if (type.typeId() == Type.TypeID.FIXED && value instanceof ByteBuffer) {
      return ByteBuffers.toByteArray((ByteBuffer) value);
    }
    return IdentityPartitionConverters.convertConstant(type, value);
  }
}
