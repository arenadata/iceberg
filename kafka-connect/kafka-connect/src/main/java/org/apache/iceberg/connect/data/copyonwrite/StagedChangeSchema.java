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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;

/**
 * The schema of a copy-on-write staged change file: the table schema plus the service columns the
 * coordinator needs to normalize the change set.
 */
public class StagedChangeSchema {

  /** Format version of the staged file, written to the file metadata. */
  public static final int FORMAT_VERSION = 1;

  public static final String FORMAT_VERSION_META = "format-version";
  public static final String SCHEMA_ID_META = "schema-id";

  /**
   * The identifier fields a staged file was written with: field ids, ascending, comma-separated.
   *
   * <p>A file is frozen into a change set only while the table's identifier fields are a subset of
   * these: its rows carry values for this set alone, and read by a key with a column more they
   * would match nothing.
   */
  public static final String IDENTIFIER_FIELD_IDS_META = "identifier-field-ids";

  /**
   * Field ids of the service columns.
   *
   * <p>Iceberg reserves {@code MAX_VALUE - (1..100)} for metadata columns and {@code -(101..200)}
   * for reserved columns ({@code MetadataColumns}); the connector takes a band well clear of both,
   * and of any table schema.
   */
  public static final int OP_ID = Integer.MAX_VALUE - 501;

  public static final int TOPIC_ID = Integer.MAX_VALUE - 502;
  public static final int PARTITION_ID = Integer.MAX_VALUE - 503;
  public static final int OFFSET_ID = Integer.MAX_VALUE - 504;

  public static final String OP = "_op";
  public static final String TOPIC = "_topic";
  public static final String PARTITION = "_partition";
  public static final String OFFSET = "_offset";

  /** Values of the {@link #OP} column. */
  public static final int OP_INSERT = 0;

  public static final int OP_UPDATE = 1;
  public static final int OP_DELETE = 2;

  /**
   * Builds the staged file schema for a table.
   *
   * <p>Every non-identifier column becomes optional: a delete, and a CDC event without a full row
   * image, carry the identifier fields and nothing else. Leaving a required table column required
   * would make the Avro union non-nullable and fail on the first such record.
   */
  public static Schema stagedSchema(Schema tableSchema, Set<Integer> identifierFieldIds) {
    List<NestedField> fields = Lists.newArrayList();
    for (NestedField field : tableSchema.columns()) {
      fields.add(
          identifierFieldIds.contains(field.fieldId()) ? field.asRequired() : field.asOptional());
    }

    // (_topic, _partition, _offset) is the only ordering that survives across workers: position in
    // a file orders one worker's records, and the control topic orders batches, not records
    fields.add(NestedField.required(OP_ID, OP, IntegerType.get()));
    fields.add(NestedField.required(TOPIC_ID, TOPIC, StringType.get()));
    fields.add(NestedField.required(PARTITION_ID, PARTITION, IntegerType.get()));
    fields.add(NestedField.required(OFFSET_ID, OFFSET, LongType.get()));

    return new Schema(fields, identifierFieldIds);
  }

  public static String identifierFieldIdsMeta(Set<Integer> identifierFieldIds) {
    return identifierFieldIds.stream()
        .sorted()
        .map(String::valueOf)
        .collect(Collectors.joining(","));
  }

  private StagedChangeSchema() {}
}
