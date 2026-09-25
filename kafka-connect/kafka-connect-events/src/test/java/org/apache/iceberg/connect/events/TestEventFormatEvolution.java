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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Guards the evolution rule of the control protocol: a later version may add an optional field with
 * a new id to any record, and a reader of this version decodes the event and keeps the fields it
 * knows.
 *
 * <p>Compatibility is checked by reading bytes of a later schema with the current classes, not by a
 * round trip: the Avro reader asks the record for the value of every field of the writer's schema
 * before setting it, so a record whose {@code get} throws on an unknown id cannot be read at all.
 *
 * <p>Data and delete files travel as records of Iceberg core, whose schema grows with core itself:
 * a later version of the connector built on a later core writes content files with fields this
 * version does not know. The reader skips such a field when it is optional and refuses it when it
 * is required.
 */
public class TestEventFormatEvolution {

  private static final String ADDED_FIELD = "added_in_a_later_version";
  private static final String ADDED_VALUE = "a value this version does not know";

  // an id DataFile.getType of this core does not use; core, not the connector, picks it
  private static final int CORE_FIELD_ID = 190;
  private static final List<String> CORE_CONTENT_FILES =
      ImmutableList.of(
          "org.apache.iceberg.GenericDataFile", "org.apache.iceberg.GenericDeleteFile");
  private static final String CORE_CONTENT_FILE_FIELDS = "content-file-fields.txt";

  @ParameterizedTest(name = "{0} in {1}")
  @MethodSource("protocolRecordsOfSampleEvents")
  public void testFieldAddedByALaterVersionIsIgnored(
      Class<?> record, PayloadType payloadType, Event event) throws IOException {
    // a new id from the band of the record, as a later version would take it
    int fieldId = TestEventFieldIds.PROTOCOL_CLASSES.get(record) + TestEventFieldIds.BAND_SIZE - 1;
    byte[] encoded = AvroUtil.encode(event);

    byte[] later = withFieldAddedTo(encoded, record.getName(), fieldId, true, ADDED_VALUE);

    assertDecodedAs(AvroUtil.decode(later), event);
  }

  @ParameterizedTest(name = "{0} in {1}, {2}")
  @MethodSource("coreContentFilesWithAddedValuesOfSampleEvents")
  public void testFieldAddedToACoreContentFileByALaterVersionIsIgnored(
      String record, PayloadType payloadType, Object addedValue, Event event) throws IOException {
    byte[] encoded = AvroUtil.encode(event);

    byte[] later = withFieldAddedTo(encoded, record, CORE_FIELD_ID, true, addedValue);

    assertDecodedAs(AvroUtil.decode(later), event);
  }

  @ParameterizedTest(name = "{0} in {1}")
  @MethodSource("coreContentFilesOfSampleEvents")
  public void testRequiredFieldAddedToACoreContentFileIsRefused(
      String record, PayloadType payloadType, Event event) throws IOException {
    byte[] encoded = AvroUtil.encode(event);

    // the writer requires the value, so skipping it is not safe
    byte[] later = withFieldAddedTo(encoded, record, CORE_FIELD_ID, false, ADDED_VALUE);

    assertThatThrownBy(() -> AvroUtil.decode(later))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find projected field: " + CORE_FIELD_ID);
  }

  @Test
  public void testCoreContentFileTypeIsTheOneTheProtocolWasCheckedAgainst() throws IOException {
    List<String> fields =
        DataFile.getType(Types.StructType.of()).fields().stream()
            .map(
                field ->
                    String.format(
                        "%d %s %s %s",
                        field.fieldId(),
                        field.name(),
                        field.isOptional() ? "optional" : "required",
                        field.type()))
            .collect(Collectors.toList());

    assertThat(fields)
        .as(
            "DataFile.getType of Iceberg core changed: a reader of the previous version skips a new "
                + "optional field of a data or delete file and refuses a new required one. Check "
                + "that losing the field is safe during a rolling upgrade, then update %s",
            CORE_CONTENT_FILE_FIELDS)
        .containsExactlyElementsOf(resourceLines(CORE_CONTENT_FILE_FIELDS));
  }

  @Test
  public void testEveryProtocolClassIsInASampleEvent() {
    Set<Class<?>> covered = Sets.newHashSet();
    protocolRecordsOfSampleEvents().forEach(arguments -> covered.add(classOf(arguments)));

    assertThat(covered)
        .containsExactlyInAnyOrderElementsOf(TestEventFieldIds.PROTOCOL_CLASSES.keySet());
  }

  static Stream<Arguments> protocolRecordsOfSampleEvents() {
    Map<String, Class<?>> protocolClasses = Maps.newHashMap();
    TestEventFieldIds.PROTOCOL_CLASSES
        .keySet()
        .forEach(type -> protocolClasses.put(type.getName(), type));

    List<Arguments> cases = Lists.newArrayList();
    for (Event event : sampleEvents()) {
      for (String recordName : recordNames(event.getSchema(), Sets.newTreeSet())) {
        Class<?> type = protocolClasses.get(recordName);
        if (type != null) {
          cases.add(Arguments.of(Named.of(type.getSimpleName(), type), event.type(), event));
        }
      }
    }

    return cases.stream();
  }

  static Stream<Arguments> coreContentFilesOfSampleEvents() {
    List<Arguments> cases = Lists.newArrayList();
    for (Event event : sampleEvents()) {
      Set<String> recordNames = recordNames(event.getSchema(), Sets.newTreeSet());
      for (String recordName : CORE_CONTENT_FILES) {
        if (recordNames.contains(recordName)) {
          String simpleName = recordName.substring(recordName.lastIndexOf('.') + 1);
          cases.add(Arguments.of(Named.of(simpleName, recordName), event.type(), event));
        }
      }
    }

    return cases.stream();
  }

  static Stream<Arguments> coreContentFilesWithAddedValuesOfSampleEvents() {
    return coreContentFilesOfSampleEvents()
        .flatMap(
            arguments -> {
              Object[] args = arguments.get();
              return Stream.of(
                  Arguments.of(args[0], args[1], Named.of("null", null), args[2]),
                  Arguments.of(args[0], args[1], Named.of("with a value", ADDED_VALUE), args[2]));
            });
  }

  private static void assertDecodedAs(Event result, Event event) {
    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(
            "payload\\.partitionType",
            ".*avroSchema",
            ".*icebergSchema",
            ".*schema",
            ".*fromProjectionPos")
        .isEqualTo(event);
  }

  private static Class<?> classOf(Arguments arguments) {
    return (Class<?>) ((Named<?>) arguments.get()[0]).getPayload();
  }

  private static List<String> resourceLines(String name) throws IOException {
    try (InputStream in = TestEventFormatEvolution.class.getResourceAsStream(name)) {
      assertThat(in).as("test resource %s", name).isNotNull();
      return new String(in.readAllBytes(), StandardCharsets.UTF_8)
          .lines()
          .filter(line -> !line.isBlank() && !line.startsWith("#"))
          .collect(Collectors.toList());
    }
  }

  private static List<Event> sampleEvents() {
    TableReference table =
        TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID());
    return ImmutableList.of(
        new Event("cg-connector", new StartCommit(UUID.randomUUID())),
        new Event(
            "cg-connector",
            new DataWritten(
                EventTestUtil.SPEC.partitionType(),
                UUID.randomUUID(),
                table,
                Arrays.asList(EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile()))),
        new Event(
            "cg-connector",
            new DataComplete(
                UUID.randomUUID(),
                Arrays.asList(new TopicPartitionOffset("topic", 1, 1L, EventTestUtil.now())),
                "3")),
        new Event(
            "cg-connector", new CommitToTable(UUID.randomUUID(), table, 1L, EventTestUtil.now())),
        new Event("cg-connector", new CommitComplete(UUID.randomUUID(), EventTestUtil.now())),
        new Event(
            "cg-connector",
            new RowChangesWritten(
                UUID.randomUUID(),
                table,
                "0",
                Arrays.asList("topic"),
                Arrays.asList(EventTestUtil.createStagedChangeFile()))),
        new Event(
            "cg-connector",
            new RewriteAssigned(
                EventTestUtil.SPEC.partitionType(),
                UUID.randomUUID(),
                table,
                UUID.randomUUID(),
                2,
                123L,
                EventTestUtil.createStagedChangeFile(),
                Arrays.asList(
                    new Assignment(
                        "0",
                        Arrays.asList(new TopicPartitionRef("topic", 0)),
                        Arrays.asList(
                            new FileScanTaskDescriptor(
                                EventTestUtil.createDataFile(),
                                Arrays.asList(EventTestUtil.createDeleteFile()),
                                0,
                                EventTestUtil.SPEC.partitionType())))),
                0,
                1,
                Arrays.asList(1))),
        new Event(
            "cg-connector",
            new RewriteComplete(
                EventTestUtil.SPEC.partitionType(),
                UUID.randomUUID(),
                table,
                UUID.randomUUID(),
                2,
                "0",
                RewriteComplete.STATUS_OK,
                Arrays.asList(EventTestUtil.createDataFile()),
                0,
                1)));
  }

  /**
   * Returns the encoded event as a later version would write it: every record named {@code
   * recordName} carries one more field, optional or required, set to {@code addedValue}.
   */
  private static byte[] withFieldAddedTo(
      byte[] encoded, String recordName, int fieldId, boolean optional, Object addedValue)
      throws IOException {
    byte[] header = new byte[2];
    Schema schema;
    byte[] datum;
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
      in.readFully(header);
      schema = new Schema.Parser().parse(in.readUTF());
      datum = in.readAllBytes();
    }

    // data and delete files of one event repeat the ids of the core file type, so ids are collected
    // rather than indexed
    assertThat(fieldIds(schema, Sets.newHashSet()))
        .as("field id %s is free in the event", fieldId)
        .doesNotContain(fieldId);

    Schema laterSchema = withFieldAddedTo(schema, recordName, fieldId, optional, Maps.newHashMap());
    assertThat(fieldIds(laterSchema, Sets.newHashSet())).contains(fieldId);

    Object value =
        new GenericDatumReader<>(schema, laterSchema)
            .read(null, DecoderFactory.get().binaryDecoder(datum, null));
    assertThat(setAddedField(value, recordName, addedValue))
        .as("records %s in the event", recordName)
        .isPositive();

    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes)) {
      out.write(header);
      out.writeUTF(laterSchema.toString());
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      new GenericDatumWriter<>(laterSchema).write(value, encoder);
      encoder.flush();
      return bytes.toByteArray();
    }
  }

  private static Schema withFieldAddedTo(
      Schema schema, String recordName, int fieldId, boolean optional, Map<String, Schema> copies) {
    switch (schema.getType()) {
      case RECORD:
        Schema copied = copies.get(schema.getFullName());
        if (copied != null) {
          return copied;
        }

        Schema copy =
            Schema.createRecord(
                schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
        schema.getObjectProps().forEach(copy::addProp);
        copies.put(schema.getFullName(), copy);

        List<Schema.Field> fields = Lists.newArrayList();
        for (Schema.Field field : schema.getFields()) {
          fields.add(
              new Schema.Field(
                  field, withFieldAddedTo(field.schema(), recordName, fieldId, optional, copies)));
        }

        if (schema.getFullName().equals(recordName)) {
          Schema.Field added =
              optional
                  ? new Schema.Field(
                      ADDED_FIELD,
                      Schema.createUnion(
                          Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)),
                      null,
                      Schema.Field.NULL_DEFAULT_VALUE)
                  : new Schema.Field(ADDED_FIELD, Schema.create(Schema.Type.STRING), null, "");
          added.addProp(AvroSchemaUtil.FIELD_ID_PROP, fieldId);
          fields.add(added);
        }

        copy.setFields(fields);
        return copy;

      case ARRAY:
        Schema array =
            Schema.createArray(
                withFieldAddedTo(schema.getElementType(), recordName, fieldId, optional, copies));
        schema.getObjectProps().forEach(array::addProp);
        return array;

      case MAP:
        Schema map =
            Schema.createMap(
                withFieldAddedTo(schema.getValueType(), recordName, fieldId, optional, copies));
        schema.getObjectProps().forEach(map::addProp);
        return map;

      case UNION:
        List<Schema> types = Lists.newArrayList();
        for (Schema type : schema.getTypes()) {
          types.add(withFieldAddedTo(type, recordName, fieldId, optional, copies));
        }
        return Schema.createUnion(types);

      default:
        return schema;
    }
  }

  /**
   * Sets the added field of every record named {@code recordName} to {@code addedValue}; returns
   * how many there were.
   */
  private static int setAddedField(Object datum, String recordName, Object addedValue) {
    int count = 0;
    if (datum instanceof GenericData.Record) {
      GenericData.Record record = (GenericData.Record) datum;
      if (record.getSchema().getFullName().equals(recordName)) {
        record.put(ADDED_FIELD, addedValue);
        count += 1;
      }

      for (Schema.Field field : record.getSchema().getFields()) {
        count += setAddedField(record.get(field.pos()), recordName, addedValue);
      }
    } else if (datum instanceof Collection) {
      for (Object element : (Collection<?>) datum) {
        count += setAddedField(element, recordName, addedValue);
      }
    } else if (datum instanceof Map) {
      for (Object element : ((Map<?, ?>) datum).values()) {
        count += setAddedField(element, recordName, addedValue);
      }
    }

    return count;
  }

  private static Set<Integer> fieldIds(Schema schema, Set<Integer> ids) {
    switch (schema.getType()) {
      case RECORD:
        for (Schema.Field field : schema.getFields()) {
          Object id = field.getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
          if (id != null) {
            ids.add((Integer) id);
          }
          fieldIds(field.schema(), ids);
        }
        break;
      case ARRAY:
        addId(schema, AvroSchemaUtil.ELEMENT_ID_PROP, ids);
        fieldIds(schema.getElementType(), ids);
        break;
      case MAP:
        addId(schema, AvroSchemaUtil.KEY_ID_PROP, ids);
        addId(schema, AvroSchemaUtil.VALUE_ID_PROP, ids);
        fieldIds(schema.getValueType(), ids);
        break;
      case UNION:
        schema.getTypes().forEach(type -> fieldIds(type, ids));
        break;
      default:
    }

    return ids;
  }

  private static void addId(Schema schema, String prop, Set<Integer> ids) {
    Object id = schema.getObjectProp(prop);
    if (id != null) {
      ids.add((Integer) id);
    }
  }

  private static Set<String> recordNames(Schema schema, Set<String> names) {
    switch (schema.getType()) {
      case RECORD:
        if (names.add(schema.getFullName())) {
          schema.getFields().forEach(field -> recordNames(field.schema(), names));
        }
        break;
      case ARRAY:
        recordNames(schema.getElementType(), names);
        break;
      case MAP:
        recordNames(schema.getValueType(), names);
        break;
      case UNION:
        schema.getTypes().forEach(type -> recordNames(type, names));
        break;
      default:
    }

    return names;
  }
}
