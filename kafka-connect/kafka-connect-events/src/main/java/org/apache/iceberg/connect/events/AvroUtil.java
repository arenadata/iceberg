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
package org.apache.iceberg.connect.events;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.data.avro.DecoderResolver;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Class for Avro-related utility methods. */
public class AvroUtil {
  private static final String GENERIC_DATA_FILE = "org.apache.iceberg.GenericDataFile";
  private static final String GENERIC_DELETE_FILE = "org.apache.iceberg.GenericDeleteFile";

  // the header AvroEncoderUtil.encode writes before the schema
  private static final byte[] HEADER_BYTES = new byte[] {(byte) 0xC2, (byte) 0x01};

  static {
    // decode parses the writer's schema itself, and the logical types of Iceberg's Avro schemas
    // are registered when AvroEncoderUtil is initialized
    try {
      Class.forName(AvroEncoderUtil.class.getName());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Cannot initialize " + AvroEncoderUtil.class.getName(), e);
    }
  }

  static final Map<Integer, String> FIELD_ID_TO_CLASS =
      ImmutableMap.<Integer, String>builder()
          .put(DataComplete.ASSIGNMENTS_ELEMENT, TopicPartitionOffset.class.getName())
          .put(DataFile.PARTITION_ID, PartitionData.class.getName())
          .put(DataWritten.TABLE_REFERENCE, TableReference.class.getName())
          .put(DataWritten.DATA_FILES_ELEMENT, GENERIC_DATA_FILE)
          .put(DataWritten.DELETE_FILES_ELEMENT, GENERIC_DELETE_FILE)
          .put(CommitToTable.TABLE_REFERENCE, TableReference.class.getName())
          .put(RowChangesWritten.TABLE_REFERENCE, TableReference.class.getName())
          .put(RowChangesWritten.STAGED_FILES_ELEMENT, StagedChangeFile.class.getName())
          .put(RewriteAssigned.TABLE_REFERENCE, TableReference.class.getName())
          .put(RewriteAssigned.NORMALIZED_REF, StagedChangeFile.class.getName())
          .put(RewriteAssigned.ASSIGNMENTS_ELEMENT, Assignment.class.getName())
          .put(Assignment.EXPECTED_PARTITIONS_ELEMENT, TopicPartitionRef.class.getName())
          .put(Assignment.FILES_ELEMENT, FileScanTaskDescriptor.class.getName())
          .put(FileScanTaskDescriptor.DATA_FILE, GENERIC_DATA_FILE)
          .put(FileScanTaskDescriptor.DELETE_FILES_ELEMENT, GENERIC_DELETE_FILE)
          .put(RewriteComplete.TABLE_REFERENCE, TableReference.class.getName())
          .put(RewriteComplete.DATA_FILES_ELEMENT, GENERIC_DATA_FILE)
          .build();

  // ids of the fields and elements that hold a data or delete file
  private static final Set<Integer> CONTENT_FILE_IDS =
      FIELD_ID_TO_CLASS.entrySet().stream()
          .filter(
              entry ->
                  entry.getValue().equals(GENERIC_DATA_FILE)
                      || entry.getValue().equals(GENERIC_DELETE_FILE))
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());

  // ids of the content file fields this version of Iceberg core can read
  private static final Set<Integer> KNOWN_CONTENT_FILE_FIELD_IDS =
      DataFile.getType(Types.StructType.of()).fields().stream()
          .map(Types.NestedField::fieldId)
          .collect(Collectors.toSet());

  public static byte[] encode(Event event) {
    try {
      return AvroEncoderUtil.encode(event, event.getSchema());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Event decode(byte[] bytes) {
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      Schema writeSchema = readWriteSchema(in);
      DatumReader<Event> reader = GenericAvroReader.create(readSchema(writeSchema));
      reader.setSchema(writeSchema);
      Event event = reader.read(null, DecoderFactory.get().binaryDecoder(in, null));
      // clear the cache to avoid memory leak
      DecoderResolver.clearCache();
      return event;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static EventHeader decodeHeader(byte[] bytes) {
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
      Schema writeSchema = readWriteSchema(in);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
      Integer typeId = null;
      String groupId = null;
      for (Schema.Field field : writeSchema.getFields()) {
        if (typeId != null && groupId != null) {
          break;
        }

        Object value = new GenericDatumReader<>(field.schema()).read(null, decoder);
        int fieldId = positionToId(field.pos(), writeSchema);
        if (fieldId == Event.TYPE) {
          typeId = (Integer) value;
        } else if (fieldId == Event.GROUP_ID) {
          groupId = value == null ? null : value.toString();
        }
      }

      Preconditions.checkState(
          typeId != null && groupId != null, "Event has no type or group id before its payload");
      return new EventHeader(groupId, typeId);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Schema readWriteSchema(ByteArrayInputStream in) throws IOException {
    DataInputStream dataInput = new DataInputStream(in);
    byte header0 = dataInput.readByte();
    byte header1 = dataInput.readByte();
    Preconditions.checkState(
        header0 == HEADER_BYTES[0] && header1 == HEADER_BYTES[1],
        "Unrecognized header bytes: %s %s",
        header0,
        header1);

    return new Schema.Parser().parse(dataInput.readUTF());
  }

  private static Schema readSchema(Schema writeSchema) {
    Type writeType = AvroSchemaUtil.convert(writeSchema);
    Type readType = withoutUnknownContentFileFields(writeType);
    if (readType == writeType) {
      return writeSchema;
    }

    return AvroSchemaUtil.convert(readType.asStructType(), writeSchema.getFullName());
  }

  private static Type withoutUnknownContentFileFields(Type type) {
    switch (type.typeId()) {
      case STRUCT:
        List<Types.NestedField> fields = Lists.newArrayList();
        boolean changed = false;
        for (Types.NestedField field : type.asStructType().fields()) {
          Type fieldType = readType(field.fieldId(), field.type());
          changed |= fieldType != field.type();
          fields.add(
              fieldType == field.type()
                  ? field
                  : Types.NestedField.from(field).ofType(fieldType).build());
        }
        return changed ? Types.StructType.of(fields) : type;

      case LIST:
        Types.ListType list = type.asListType();
        Type elementType = readType(list.elementId(), list.elementType());
        if (elementType == list.elementType()) {
          return type;
        }
        return list.isElementOptional()
            ? Types.ListType.ofOptional(list.elementId(), elementType)
            : Types.ListType.ofRequired(list.elementId(), elementType);

      case MAP:
        Types.MapType map = type.asMapType();
        Type valueType = readType(map.valueId(), map.valueType());
        if (valueType == map.valueType()) {
          return type;
        }
        return map.isValueOptional()
            ? Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueType)
            : Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueType);

      default:
        return type;
    }
  }

  private static Type readType(int id, Type type) {
    if (!CONTENT_FILE_IDS.contains(id) || !type.isStructType()) {
      return withoutUnknownContentFileFields(type);
    }

    List<Types.NestedField> fields = type.asStructType().fields();
    List<Types.NestedField> known =
        fields.stream()
            .filter(
                field ->
                    KNOWN_CONTENT_FILE_FIELD_IDS.contains(field.fieldId()) || field.isRequired())
            .collect(Collectors.toList());
    return known.size() == fields.size() ? type : Types.StructType.of(known);
  }

  static Schema convert(Types.StructType icebergSchema, Class<? extends IndexedRecord> javaClass) {
    return convert(icebergSchema, javaClass, FIELD_ID_TO_CLASS);
  }

  static Schema convert(
      Types.StructType icebergSchema,
      Class<? extends IndexedRecord> javaClass,
      Map<Integer, String> typeMap) {
    return AvroSchemaUtil.convert(
        icebergSchema,
        (fieldId, struct) ->
            struct.equals(icebergSchema) ? javaClass.getName() : typeMap.get(fieldId));
  }

  static int positionToId(int position, Schema avroSchema) {
    List<Schema.Field> fields = avroSchema.getFields();
    Preconditions.checkArgument(
        position >= 0 && position < fields.size(), "Invalid field position: " + position);
    Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
    return val == null ? -1 : (int) val;
  }

  private AvroUtil() {}
}
