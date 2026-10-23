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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
 * Builds identifier-only key structs from staged rows, in a fixed field-id order.
 *
 * <p>The order only has to be the same wherever a key is built; ascending field id makes it
 * deterministic.
 *
 * <p>Key values are held in Iceberg's <b>internal</b> representation, not the generic one the rows
 * arrive in. The rows come from {@code org.apache.iceberg.data.Record}, where a date is a {@code
 * LocalDate} and a timestamp an {@code OffsetDateTime}; every API a key is later handed to ({@code
 * Expressions}/{@code Literals} for the pruning predicate, {@code SingleValueParser} for the
 * persisted cursor, {@code Conversions} for staged-file bounds) accepts only the internal form
 * ({@code Integer} days, {@code Long} micros, {@code ByteBuffer} for {@code fixed}) and throws on
 * the generic one. Converting once, here, is what keeps date and timestamp identifier fields
 * working. {@link InternalRecordWrapper} carries Iceberg's own conversion table, but not {@code
 * timestamp_ns}: such a key would stay generic, so copy-on-write rejects the type.
 */
final class IdentifierKeys {

  private IdentifierKeys() {}

  static List<NestedField> orderedFields(Schema schema, Set<Integer> identifierFieldIds) {
    List<NestedField> fields = Lists.newArrayList();
    identifierFieldIds.stream()
        .sorted()
        .forEach(
            id -> {
              NestedField field = schema.findField(id);
              // never skipped: a key one column short compares equal for rows that differ, which
              // merges distinct rows into one and is invisible until the data is wrong
              Preconditions.checkArgument(
                  field != null, "Identifier field id %s is not in the schema", id);
              fields.add(field);
            });
    return ImmutableList.copyOf(fields);
  }

  static StructType keyType(List<NestedField> orderedIdentifierFields) {
    return StructType.of(orderedIdentifierFields);
  }

  static Comparator<StructLike> comparator(List<NestedField> orderedIdentifierFields) {
    return Comparators.forType(keyType(orderedIdentifierFields));
  }

  /**
   * The position of each identifier field within {@code rowSchema}, in the same order as {@code
   * orderedIdentifierFields}.
   *
   * <p>Positional, not name-based, because a row read back through {@code RecordProjection} (as
   * every row {@link CopyOnWriteRewriter} sees is) does not support {@code getField(String)}: only
   * positional {@link StructLike#get}. A staged change file's row layout and the table's own share
   * the same prefix (see {@link StagedChangeSchema#stagedSchema}), so one identifier field has the
   * same position in both.
   */
  static int[] positions(Schema rowSchema, List<NestedField> orderedIdentifierFields) {
    List<NestedField> columns = rowSchema.columns();
    int[] positions = new int[orderedIdentifierFields.size()];
    for (int i = 0; i < positions.length; i++) {
      int fieldId = orderedIdentifierFields.get(i).fieldId();
      positions[i] = -1;
      for (int pos = 0; pos < columns.size(); pos++) {
        if (columns.get(pos).fieldId() == fieldId) {
          positions[i] = pos;
          break;
        }
      }
      if (positions[i] < 0) {
        throw new IllegalArgumentException("Field id not found in schema: " + fieldId);
      }
    }
    return positions;
  }

  /**
   * A reusable key builder for rows of one schema.
   *
   * <p>Stateful and not thread safe: {@link InternalRecordWrapper} wraps by mutating a single view,
   * so every thread that reads rows builds its own.
   */
  static KeyBuilder keyBuilder(Schema rowSchema, List<NestedField> orderedIdentifierFields) {
    return new KeyBuilder(
        new InternalRecordWrapper(rowSchema.asStruct()),
        positions(rowSchema, orderedIdentifierFields));
  }

  static final class KeyBuilder {
    private final InternalRecordWrapper wrapper;
    private final int[] positions;

    private KeyBuilder(InternalRecordWrapper wrapper, int[] positions) {
      this.wrapper = wrapper;
      this.positions = positions;
    }

    /** The identifier key of one row, converted to the internal representation. */
    StructLike keyOf(StructLike row) {
      StructLike internal = wrapper.wrap(row);
      Object[] values = new Object[positions.length];
      for (int i = 0; i < positions.length; i++) {
        values[i] = internal.get(positions[i], Object.class);
      }
      return new KeyStruct(values);
    }
  }

  /**
   * A key built from values that are already in the internal representation.
   *
   * <p>Deliberately not called {@code keyOf}: an overload of that name would quietly accept a
   * {@code (row, positions)} call as two varargs and build a nonsense key out of them.
   */
  @VisibleForTesting
  static StructLike internalKey(Object... internalValues) {
    return new KeyStruct(internalValues.clone());
  }

  /**
   * {@code equals}/{@code hashCode} are structural, unlike most {@link StructLike} implementations.
   *
   * <p>The rewrite path orders keys by the {@link Comparator} ({@link ChangeSetSlice}'s map is a
   * {@code TreeMap}), but a key is a value: a fresh {@code KeyStruct} is built per row read, so two
   * keys that are the same key must be equal, and the tests that assert on cursors and blocks rely
   * on it.
   */
  private static final class KeyStruct implements StructLike {
    private final Object[] values;

    private KeyStruct(Object[] values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof KeyStruct)) {
        return false;
      }
      return Arrays.equals(values, ((KeyStruct) other).values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }
}
