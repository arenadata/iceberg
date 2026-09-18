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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

/**
 * Guards the field id allocation of the control protocol.
 *
 * <p>{@code AvroUtil.FIELD_ID_TO_CLASS} is keyed by field id and {@link Event} copies it to build
 * its own schema, so registering a class on an id that already belongs to another payload changes
 * how that payload's field is converted, and breaks every event on the topic, not only the new
 * ones. A duplicate is therefore not a cosmetic problem.
 */
public class TestEventFieldIds {

  // field ids start here; smaller int constants (statuses) are not field ids
  private static final int FIRST_FIELD_ID = 10_000;
  static final int BAND_SIZE = 100;

  // every class of the protocol that declares field ids, with the band it takes them from
  static final Map<Class<?>, Integer> PROTOCOL_CLASSES =
      ImmutableMap.<Class<?>, Integer>builder()
          .put(CommitComplete.class, 10_000)
          .put(DataComplete.class, 10_100)
          .put(StartCommit.class, 10_200)
          .put(DataWritten.class, 10_300)
          .put(CommitToTable.class, 10_400)
          .put(Event.class, 10_500)
          .put(TableReference.class, 10_600)
          .put(TopicPartitionOffset.class, 10_700)
          .put(RowChangesWritten.class, 10_800)
          .put(StagedChangeFile.class, 10_800)
          .put(RewriteAssigned.class, 10_900)
          .put(Assignment.class, 10_900)
          .put(TopicPartitionRef.class, 10_900)
          .put(FileScanTaskDescriptor.class, 10_900)
          .put(RewriteComplete.class, 11_000)
          .build();

  @Test
  public void testDeclaredFieldIdsAreUnique() {
    Map<Integer, String> owners = Maps.newHashMap();
    Map<Integer, String> duplicates = Maps.newHashMap();

    for (Class<?> type : PROTOCOL_CLASSES.keySet()) {
      fieldIds(type)
          .forEach(
              (name, fieldId) -> {
                String owner = type.getSimpleName() + "." + name;
                String previous = owners.put(fieldId, owner);
                if (previous != null) {
                  duplicates.put(fieldId, previous + " and " + owner);
                }
              });
    }

    assertThat(duplicates).as("field ids claimed by more than one element").isEmpty();
  }

  @Test
  public void testDeclaredFieldIdsStayInTheBandOfTheirClass() {
    Map<String, Integer> outside = Maps.newTreeMap();

    PROTOCOL_CLASSES.forEach(
        (type, band) ->
            fieldIds(type)
                .forEach(
                    (name, fieldId) -> {
                      if (fieldId < band || fieldId >= band + BAND_SIZE) {
                        outside.put(type.getSimpleName() + "." + name, fieldId);
                      }
                    }));

    assertThat(outside).as("field ids outside the band of their class").isEmpty();
    assertThat(DataComplete.TASK_ID).isEqualTo(10_103);
  }

  @Test
  public void testRecordClassesRegisteredForConversionAreChecked() throws ClassNotFoundException {
    // a nested record missing from PROTOCOL_CLASSES would pass the checks above with any id
    String eventsPackage = Event.class.getPackage().getName() + ".";
    Set<String> unchecked = Sets.newTreeSet();
    for (String className : AvroUtil.FIELD_ID_TO_CLASS.values()) {
      if (className.startsWith(eventsPackage)
          && !PROTOCOL_CLASSES.containsKey(Class.forName(className))) {
        unchecked.add(className);
      }
    }

    assertThat(unchecked).as("classes of AvroUtil.FIELD_ID_TO_CLASS not checked here").isEmpty();
  }

  @Test
  public void testPayloadTypeIdsAreUnique() {
    Set<Integer> ids = Sets.newHashSet();
    for (PayloadType type : PayloadType.values()) {
      assertThat(ids.add(type.id())).as("duplicate payload type id %s", type.id()).isTrue();
    }
  }

  @Test
  public void testTableReferenceNamespaceElementIdIsAccountedFor() {
    // TableReference derives its list element id as NAMESPACE + 1 rather than declaring it, so the
    // scan above cannot see it; assert nothing else took that id
    int namespaceElementId = 10_601 + 1;
    for (Class<?> type : PROTOCOL_CLASSES.keySet()) {
      if (fieldIds(type).containsValue(namespaceElementId)) {
        assertThat(type).isEqualTo(TableReference.class);
      }
    }
  }

  /** Static int constants of a class that are field ids, by constant name. */
  private static Map<String, Integer> fieldIds(Class<?> type) {
    Map<String, Integer> fieldIds = Maps.newTreeMap();
    for (Field field : type.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())
          && Modifier.isFinal(field.getModifiers())
          && field.getType() == int.class) {
        int value = readInt(field);
        if (value >= FIRST_FIELD_ID) {
          fieldIds.put(field.getName(), value);
        }
      }
    }

    return fieldIds;
  }

  private static int readInt(Field field) {
    try {
      field.setAccessible(true);
      return field.getInt(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Cannot read " + field, e);
    }
  }
}
