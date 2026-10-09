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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStatisticsColumns {
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "t");

  private static TableIdentifier uniqueTable(String test) {
    return TableIdentifier.of("db", "t_" + test);
  }

  private static List<String> names(List<Types.NestedField> fields) {
    return fields.stream().map(Types.NestedField::name).collect(Collectors.toList());
  }

  @Test
  public void defaultSetCapsPrimitiveColumnsAtHundred() {
    Schema schema =
        new Schema(
            IntStream.rangeClosed(1, 120)
                .mapToObj(i -> Types.NestedField.optional(i, "c" + i, Types.IntegerType.get()))
                .collect(Collectors.toList()));

    List<Types.NestedField> columns =
        StatisticsColumns.resolve(uniqueTable("cap"), schema, null, false);

    assertThat(StatisticsColumns.DEFAULT_MAX_COLUMNS).isEqualTo(100);
    assertThat(columns).hasSize(100);
    assertThat(columns.get(0).name()).isEqualTo("c1");
    assertThat(columns.get(99).name()).isEqualTo("c100");
  }

  @Test
  public void defaultSetSkipsUnsuitableTypesKeepingSchemaOrder() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "unk", Types.UnknownType.get()),
            Types.NestedField.optional(
                3,
                "nested",
                Types.StructType.of(
                    Types.NestedField.optional(4, "inner", Types.StringType.get()))),
            Types.NestedField.optional(5, "geom", Types.GeometryType.crs84()),
            Types.NestedField.optional(6, "geog", Types.GeographyType.crs84()),
            Types.NestedField.optional(
                7, "l", Types.ListType.ofOptional(8, Types.IntegerType.get())),
            Types.NestedField.optional(
                9,
                "m",
                Types.MapType.ofOptional(10, 11, Types.StringType.get(), Types.StringType.get())),
            Types.NestedField.optional(12, "name", Types.StringType.get()),
            Types.NestedField.optional(13, "u", Types.UUIDType.get()),
            Types.NestedField.optional(14, "ts", Types.TimestampNanoType.withZone()));

    List<Types.NestedField> columns =
        StatisticsColumns.resolve(uniqueTable("types"), schema, null, false);

    assertThat(names(columns)).containsExactly("id", "name", "u", "ts");
  }

  @Test
  public void explicitListReplacesSetWithoutLimitAndKeepsListOrder() {
    Schema schema =
        new Schema(
            IntStream.rangeClosed(1, 120)
                .mapToObj(i -> Types.NestedField.optional(i, "c" + i, Types.IntegerType.get()))
                .collect(Collectors.toList()));
    List<String> explicit =
        IntStream.rangeClosed(1, 120).mapToObj(i -> "c" + (121 - i)).collect(Collectors.toList());

    List<Types.NestedField> columns =
        StatisticsColumns.resolve(uniqueTable("explicit120"), schema, explicit, false);

    assertThat(names(columns)).containsExactlyElementsOf(explicit);
  }

  @Test
  public void explicitListRespectsCase() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "Id", Types.LongType.get()),
            Types.NestedField.optional(2, "Name", Types.StringType.get()));

    assertThat(
            names(
                StatisticsColumns.resolve(
                    uniqueTable("case_sensitive"), schema, List.of("id", "Name"), false)))
        .containsExactly("Name");
    assertThat(
            names(
                StatisticsColumns.resolve(
                    uniqueTable("case_insensitive"), schema, List.of("id", "NAME"), true)))
        .containsExactly("Id", "Name");
  }

  @Test
  public void explicitListSkipsMissingNestedAndUnsuitableColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.optional(3, "inner", Types.StringType.get()))),
            Types.NestedField.optional(4, "unk", Types.UnknownType.get()),
            Types.NestedField.optional(5, "geom", Types.GeometryType.crs84()),
            Types.NestedField.optional(6, "geog", Types.GeographyType.crs84()),
            Types.NestedField.optional(7, "name", Types.StringType.get()));

    List<Types.NestedField> columns =
        StatisticsColumns.resolve(
            uniqueTable("skips"),
            schema,
            List.of("id", "missing", "nested", "nested.inner", "unk", "geom", "geog", "name"),
            false);

    assertThat(names(columns)).containsExactly("id", "name");
  }

  @Test
  public void warnsOncePerTableSchemaAndName() {
    TableIdentifier table = uniqueTable("warn_once");
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "unk", Types.UnknownType.get()));
    List<String> explicit = List.of("id", "missing", "unk");

    int before = StatisticsColumns.warnedKeyCount();
    StatisticsColumns.resolve(table, schema, explicit, false);
    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before + 2);

    // the same (table, schema id, name) again, e.g. from the step or fromHead: no new WARN
    StatisticsColumns.resolve(table, schema, explicit, false);
    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before + 2);

    // another table, another schema id and another name each warn again
    StatisticsColumns.resolve(uniqueTable("warn_once_other"), schema, explicit, false);
    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before + 4);

    Schema evolved =
        new Schema(
            1,
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "unk", Types.UnknownType.get())));
    StatisticsColumns.resolve(table, evolved, explicit, false);
    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before + 6);

    StatisticsColumns.resolve(table, schema, List.of("id", "other_missing"), false);
    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before + 7);
  }

  @Test
  public void noWarningForValidColumns() {
    TableIdentifier table = uniqueTable("no_warn");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    int before = StatisticsColumns.warnedKeyCount();
    StatisticsColumns.resolve(table, schema, null, false);
    StatisticsColumns.resolve(table, schema, List.of("id"), false);

    assertThat(StatisticsColumns.warnedKeyCount()).isEqualTo(before);
  }

  @Test
  public void emptySetWhenNothingSuitable() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "unk", Types.UnknownType.get()),
            Types.NestedField.optional(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.optional(3, "inner", Types.StringType.get()))));

    assertThat(StatisticsColumns.resolve(uniqueTable("empty_default"), schema, null, false))
        .isEmpty();
    assertThat(
            StatisticsColumns.resolve(
                uniqueTable("empty_explicit"), schema, List.of("unk", "nested", "zzz"), false))
        .isEmpty();
    assertThat(StatisticsColumns.resolve(TABLE, new Schema(), null, false)).isEmpty();
  }

  @Test
  public void repeatedNamesGiveOneColumn() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    assertThat(
            names(
                StatisticsColumns.resolve(uniqueTable("dupes"), schema, List.of("id", "ID"), true)))
        .containsExactly("id");
  }
}
