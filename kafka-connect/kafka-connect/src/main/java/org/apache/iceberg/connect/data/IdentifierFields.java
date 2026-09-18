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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.common.config.ConfigException;

public class IdentifierFields {

  /**
   * Returns the field ids that identify a row in the given table: {@code id-columns} if configured,
   * else the table's own identifier fields, which may be none.
   *
   * <p>Checks nothing beyond the names. Copy-on-write resolves through {@link
   * #resolveForCopyOnWrite} instead.
   *
   * @throws IllegalArgumentException if a configured id column is not present in the table schema
   */
  public static Set<Integer> resolve(
      Table table, TableReference tableReference, IcebergSinkConfig config) {
    List<String> idCols = config.tableConfig(tableReference.identifier().toString()).idColumns();
    if (idCols.isEmpty()) {
      return table.schema().identifierFieldIds();
    }

    return idCols.stream()
        .map(
            colName -> {
              NestedField field = table.schema().findField(colName);
              if (field == null) {
                throw new IllegalArgumentException("ID column not found: " + colName);
              }
              return field.fieldId();
            })
        .collect(Collectors.toSet());
  }

  public static Set<Integer> resolveForCopyOnWrite(
      Table table, TableReference tableReference, IcebergSinkConfig config) {
    TableIdentifier identifier = tableReference.identifier();

    List<String> idColumns = config.tableConfig(identifier.toString()).idColumns();
    Set<Integer> identifierFieldIds;
    if (idColumns.isEmpty()) {
      // the table's own identifier fields were validated when the schema was built
      identifierFieldIds = table.schema().identifierFieldIds();
      if (identifierFieldIds.isEmpty()) {
        throw new ConfigException(
            String.format(
                "Table %s has no identifier fields, which copy-on-write needs to find the rows a "
                    + "change replaces; set identifier fields on the table, or id-columns for it in "
                    + "the connector",
                identifier));
      }
    } else {
      try {
        identifierFieldIds = resolve(table, tableReference, config);
        // building a schema applies the very rules the id-columns path skips, so they cannot drift
        new Schema(table.schema().asStruct().fields(), identifierFieldIds);
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
            String.format(
                "Table %s: id-columns %s cannot be used as identifier fields in copy-on-write: %s",
                identifier, idColumns, e.getMessage()));
      }
    }

    // Copy-on-write addresses an identifier field three ways: by name in the pruning predicate,
    // by top-level position when building a key out of a row, and by field id in the persisted
    // cursor. Only a top-level column answers to all three: a field nested in a struct has a dotted
    // path rather than a bare name, and no position among the schema's columns at all. Iceberg
    // itself allows nested identifier fields, so the refusal has to be ours, and it belongs here
    // rather than in a stack trace from the middle of a drain.
    Set<Integer> topLevelIds = Sets.newHashSet();
    table.schema().columns().forEach(column -> topLevelIds.add(column.fieldId()));
    if (!topLevelIds.containsAll(identifierFieldIds)) {
      throw new ConfigException(
          String.format(
              "Table %s has nested identifier fields %s, which copy-on-write does not support; "
                  + "use top-level columns, or route this table to a merge-on-read connector",
              identifier, Sets.difference(identifierFieldIds, topLevelIds)));
    }

    // The pruning predicate hands a key value to Iceberg as a bare object and lets it infer the
    // literal. For a nanosecond timestamp that literal is a long, which binding to the column reads
    // as microseconds and multiplies by a thousand: a long overflow on every plan of a recent key,
    // or a bound a thousand times too high that prunes away the file holding an older one: the
    // old row then stays beside the new, a silent duplicate. Iceberg accepts such identifier
    // fields, so, as with nesting, the refusal has to be ours.
    List<String> nanosecondTimestampColumns =
        identifierFieldIds.stream()
            .map(id -> table.schema().findField(id))
            .filter(field -> field.type().typeId() == Type.TypeID.TIMESTAMP_NANO)
            .map(NestedField::name)
            .sorted()
            .collect(Collectors.toList());
    if (!nanosecondTimestampColumns.isEmpty()) {
      throw new ConfigException(
          String.format(
              "Table %s has identifier fields %s of type timestamp_ns or timestamptz_ns, which "
                  + "copy-on-write does not support; key the table by columns of other types, or "
                  + "route this table to a merge-on-read connector",
              identifier, nanosecondTimestampColumns));
    }

    return identifierFieldIds;
  }

  private IdentifierFields() {}
}
