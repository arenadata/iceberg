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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.connect.data.RecordProjection;
import org.apache.iceberg.connect.data.StatisticsFileReaders;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMapping;

/**
 * Reads a data file's surviving rows: existing equality deletes, position deletes and DVs applied,
 * nothing pruned by the scan's residual.
 *
 * <p>{@code org.apache.iceberg.data.GenericReader}, which does exactly this, is package-private, so
 * this is that class's minimal read path, built on the same public pieces ({@link
 * GenericDeleteFilter}, and the {@code Parquet}/{@code ORC}/{@code Avro} read builders behind
 * {@link StatisticsFileReaders}). Two differences are deliberate:
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
      FileIO io,
      FileScanTask task,
      Schema tableSchema,
      Schema projection,
      NameMapping nameMapping) {
    GenericDeleteFilter deletes = new GenericDeleteFilter(io, task, tableSchema, projection);
    Schema readSchema = deletes.requiredSchema();
    CloseableIterable<Record> records =
        deletes.filter(StatisticsFileReaders.dataFile(io, task, readSchema, nameMapping));
    RecordProjection projector = RecordProjection.create(readSchema, projection);
    return CloseableIterable.transform(records, projector::wrap);
  }
}
