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

import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

/**
 * Reads a staged change file back, projected onto a target schema.
 *
 * <p>Staged files are always Avro (there is exactly one writer, {@code StagedChangeFileWriter}), so
 * this needs none of {@link org.apache.iceberg.data.GenericReader}'s per-format branching.
 * Iceberg's Avro resolution matches by field id, not name, so projecting an older staged file onto
 * the current staged schema is safe across schema evolution between commit cycles: added columns
 * come back null, renamed columns still resolve.
 */
final class StagedChangeFileReader {

  private StagedChangeFileReader() {}

  static CloseableIterable<Record> open(FileIO io, StagedChangeFile file, Schema projection) {
    InputFile input = io.newInputFile(file.location());
    return Avro.read(input)
        .project(projection)
        // spelled out, not left to the default: ChangeSetNormalizer.foldFile keeps every row this
        // iterator yields for the rest of the slice, and a reused container would silently collapse
        // all rows of a file onto the last one read
        .reuseContainers(false)
        .createResolvingReader(PlannedDataReader::create)
        .build();
  }
}
