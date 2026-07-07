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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataEvent;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.metadata.SchemaEvolved;
import org.apache.kafka.connect.metadata.TableCreated;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MetadataEventsTest {

  private final TableIdentifier ident = TableIdentifier.of(Namespace.of("db", "schema"), "events");
  private final Schema schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();

  @Test
  void noopIsNoOpEvenWithEvents() {
    MetadataEvents events = MetadataEvents.NOOP;
    assertThatNoException()
        .isThrownBy(
            () -> {
              events.tableCreated(ident, schema);
              events.schemaEvolved(ident, null, schema);
              events.lineageCommit(ImmutableSet.of("topic-a"), ident);
            });
    assertThat(events.enabled()).isFalse();
  }

  @Test
  void tableCreatedReportsCatalogDbAndTable() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    MetadataEvents events = new MetadataEvents(reporter, "iceberg", "my-pipe");

    events.tableCreated(ident, schema);

    ArgumentCaptor<MetadataEvent> captor = ArgumentCaptor.forClass(MetadataEvent.class);
    verify(reporter).report(captor.capture());
    TableCreated tc = (TableCreated) captor.getValue();
    assertThat(tc.catalogName()).isEqualTo("iceberg");
    assertThat(tc.databaseName()).isEqualTo("default.db.schema");
    assertThat(tc.tableName()).isEqualTo("events");
    assertThat(tc.schema()).isSameAs(schema);
  }

  @Test
  void tableCreatedSkipsWhenSchemaIsNull() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    new MetadataEvents(reporter, "iceberg", "my-pipe").tableCreated(ident, null);
    verifyNoInteractions(reporter);
  }

  @Test
  void schemaEvolvedUsesFqnAndAllowsNullOldSchema() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    MetadataEvents events = new MetadataEvents(reporter, "iceberg", "my-pipe");

    events.schemaEvolved(ident, null, schema);

    ArgumentCaptor<MetadataEvent> captor = ArgumentCaptor.forClass(MetadataEvent.class);
    verify(reporter).report(captor.capture());
    SchemaEvolved se = (SchemaEvolved) captor.getValue();
    assertThat(se.tableFqn()).isEqualTo("iceberg.default.db.schema.events");
    assertThat(se.oldSchema()).isNull();
    assertThat(se.newSchema()).isSameAs(schema);
  }

  @Test
  void lineageCommitEmitsOneEdgePerTopic() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    MetadataEvents events = new MetadataEvents(reporter, "iceberg", "my-pipe");
    Set<String> topics = ImmutableSet.of("a", "b", "c");

    events.lineageCommit(topics, ident);

    ArgumentCaptor<MetadataEvent> captor = ArgumentCaptor.forClass(MetadataEvent.class);
    verify(reporter, org.mockito.Mockito.times(3)).report(captor.capture());
    assertThat(captor.getAllValues())
        .allSatisfy(
            e -> {
              LineageEdge edge = (LineageEdge) e;
              assertThat(edge.target().name()).isEqualTo("iceberg.default.db.schema.events");
              assertThat(edge.pipelineName()).isEqualTo("my-pipe");
            });
  }

  @Test
  void lineageCommitNoopOnEmptyTopics() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    new MetadataEvents(reporter, "iceberg", "my-pipe").lineageCommit(ImmutableSet.of(), ident);
    verify(reporter, never()).report(any());
  }

  @Test
  void reporterErrorsDoNotPropagate() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    doThrow(new RuntimeException("kaboom")).when(reporter).report(any());
    MetadataEvents events = new MetadataEvents(reporter, "iceberg", "my-pipe");

    assertThatNoException()
        .isThrownBy(
            () -> {
              events.tableCreated(ident, schema);
              events.schemaEvolved(ident, null, schema);
              events.lineageCommit(ImmutableSet.of("topic"), ident);
            });
  }
}
