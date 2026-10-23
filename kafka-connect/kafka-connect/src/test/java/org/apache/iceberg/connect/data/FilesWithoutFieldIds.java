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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

/**
 * Data files as a tool other than Iceberg writes them and {@code add_files} adds them: columns
 * known by name alone, without field ids.
 */
public final class FilesWithoutFieldIds {
  private FilesWithoutFieldIds() {}

  /**
   * The name mapping of a flat schema, as JSON, in which one column is also known by {@code alias}:
   * the name a file written before a rename has for it.
   */
  public static String mappingWithAlias(Schema schema, String column, String alias) {
    List<MappedField> fields = Lists.newArrayList();
    for (Types.NestedField field : schema.columns()) {
      List<String> names =
          field.name().equals(column) ? List.of(field.name(), alias) : List.of(field.name());
      fields.add(MappedField.of(field.fieldId(), names));
    }

    return NameMappingParser.toJson(NameMapping.of(fields));
  }

  /**
   * A row as the tests write it: {@code id}, then {@code note}, a column the table does not have,
   * then {@code payload}, the name the table's {@code data} had when the file was written. Neither
   * the position nor the current name of {@code data} finds it in the file.
   */
  public static Map<String, Object> row(long id, String note, String payload) {
    Map<String, Object> columns = Maps.newLinkedHashMap();
    columns.put("id", id);
    columns.put("note", note);
    columns.put("payload", payload);
    return columns;
  }

  /**
   * Writes the rows to a new unpartitioned data file of the table, which is not appended. Every row
   * has the same columns in the same order; the first row decides their types: a {@code Long} is a
   * required long, a {@code String} an optional string.
   *
   * @param dir a local directory the file is written to before it is copied into the table's IO
   */
  public static DataFile write(
      Table table, File dir, FileFormat format, List<Map<String, Object>> rows) throws IOException {
    File local = new File(dir, UUID.randomUUID() + "." + format.name().toLowerCase(Locale.ROOT));
    Path path = new Path(local.toURI());
    Map<String, Object> first = rows.get(0);

    switch (format) {
      case PARQUET:
        writeParquet(path, avroSchema(first), rows);
        break;
      case ORC:
        writeOrc(path, first, rows);
        break;
      case AVRO:
        writeAvro(local, avroSchema(first), rows);
        break;
      default:
        throw new IllegalArgumentException("Unexpected format: " + format);
    }

    String location = table.locationProvider().newDataLocation(local.getName());
    try (PositionOutputStream out = table.io().newOutputFile(location).create()) {
      out.write(Files.readAllBytes(local.toPath()));
    }

    return DataFiles.builder(table.spec())
        .withPath(location)
        .withFormat(format)
        .withFileSizeInBytes(local.length())
        .withRecordCount(rows.size())
        .build();
  }

  private static org.apache.avro.Schema avroSchema(Map<String, Object> first) {
    SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields =
        SchemaBuilder.record("row").fields();
    for (Map.Entry<String, Object> column : first.entrySet()) {
      fields =
          column.getValue() instanceof Long
              ? fields.requiredLong(column.getKey())
              : fields.optionalString(column.getKey());
    }

    return fields.endRecord();
  }

  private static List<GenericData.Record> avroRows(
      org.apache.avro.Schema schema, List<Map<String, Object>> rows) {
    List<GenericData.Record> records = Lists.newArrayList();
    for (Map<String, Object> row : rows) {
      GenericData.Record record = new GenericData.Record(schema);
      row.forEach(record::put);
      records.add(record);
    }

    return records;
  }

  private static void writeParquet(
      Path path, org.apache.avro.Schema schema, List<Map<String, Object>> rows) throws IOException {
    try (ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(
                HadoopOutputFile.fromPath(path, new Configuration()))
            .withSchema(schema)
            .build()) {
      for (GenericData.Record record : avroRows(schema, rows)) {
        writer.write(record);
      }
    }
  }

  private static void writeAvro(
      File local, org.apache.avro.Schema schema, List<Map<String, Object>> rows)
      throws IOException {
    try (DataFileWriter<GenericData.Record> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.create(schema, local);
      for (GenericData.Record record : avroRows(schema, rows)) {
        writer.append(record);
      }
    }
  }

  private static void writeOrc(Path path, Map<String, Object> first, List<Map<String, Object>> rows)
      throws IOException {
    TypeDescription schema = TypeDescription.createStruct();
    List<String> names = Lists.newArrayList(first.keySet());
    for (String name : names) {
      schema.addField(
          name,
          first.get(name) instanceof Long
              ? TypeDescription.createLong()
              : TypeDescription.createString());
    }

    try (Writer writer =
        OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration()).setSchema(schema))) {
      VectorizedRowBatch batch = schema.createRowBatch(rows.size());
      for (int row = 0; row < rows.size(); row += 1) {
        for (int col = 0; col < names.size(); col += 1) {
          Object value = rows.get(row).get(names.get(col));
          if (value == null) {
            batch.cols[col].noNulls = false;
            batch.cols[col].isNull[row] = true;
          } else if (value instanceof Long) {
            ((LongColumnVector) batch.cols[col]).vector[row] = (Long) value;
          } else {
            ((BytesColumnVector) batch.cols[col])
                .setVal(row, ((String) value).getBytes(StandardCharsets.UTF_8));
          }
        }
      }

      batch.size = rows.size();
      writer.addRowBatch(batch);
    }
  }
}
