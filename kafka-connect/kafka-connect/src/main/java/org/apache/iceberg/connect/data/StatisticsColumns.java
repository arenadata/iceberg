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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves the set of columns that get NDV statistics against a schema. */
public class StatisticsColumns {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsColumns.class);

  public static final int DEFAULT_MAX_COLUMNS = 100;

  // (table, schema id, name) keys already warned about, process-wide
  private static final Set<String> WARNED_KEYS = Sets.newConcurrentHashSet();

  private StatisticsColumns() {}

  /**
   * Returns the statistics columns in the order of the set.
   *
   * @param explicit the configured column names; {@code null} means "no list": top-level columns of
   *     a suitable type in schema order, at most {@link #DEFAULT_MAX_COLUMNS}
   */
  public static List<Types.NestedField> resolve(
      TableIdentifier table, Schema schema, List<String> explicit, boolean caseInsensitive) {
    if (explicit == null) {
      List<Types.NestedField> columns = Lists.newArrayList();
      for (Types.NestedField field : schema.columns()) {
        if (suitable(field.type())) {
          columns.add(field);
          if (columns.size() == DEFAULT_MAX_COLUMNS) {
            break;
          }
        }
      }
      return columns;
    }

    Map<Integer, Types.NestedField> columns = Maps.newLinkedHashMap();
    for (String name : explicit) {
      Types.NestedField field =
          caseInsensitive ? schema.caseInsensitiveFindField(name) : schema.findField(name);
      if (field == null) {
        warnOnce(table, schema, name, "there is no such column");
      } else if (schema.columns().stream().noneMatch(top -> top.fieldId() == field.fieldId())) {
        warnOnce(table, schema, name, "it is not a top-level column");
      } else if (!suitable(field.type())) {
        warnOnce(table, schema, name, "its type " + field.type() + " is not supported");
      } else {
        columns.putIfAbsent(field.fieldId(), field);
      }
    }

    return Lists.newArrayList(columns.values());
  }

  private static boolean suitable(Type type) {
    if (!type.isPrimitiveType()) {
      return false;
    }

    switch (type.typeId()) {
      case UNKNOWN:
      case GEOMETRY:
      case GEOGRAPHY:
        return false;
      default:
        return true;
    }
  }

  private static void warnOnce(TableIdentifier table, Schema schema, String name, String reason) {
    String key = table + "\u0000" + schema.schemaId() + "\u0000" + name;
    if (WARNED_KEYS.add(key)) {
      LOG.warn(
          "Skipping statistics column '{}' of table {} (schema {}): {}",
          name,
          table,
          schema.schemaId(),
          reason);
    }
  }

  @VisibleForTesting
  static int warnedKeyCount() {
    return WARNED_KEYS.size();
  }
}
