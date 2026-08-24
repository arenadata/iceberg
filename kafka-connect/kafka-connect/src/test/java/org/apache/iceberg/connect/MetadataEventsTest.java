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

import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataEvent;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MetadataEventsTest {

  private final TableIdentifier ident = TableIdentifier.of(Namespace.of("db", "schema"), "events");
  private final Schema schema =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Test
  void noopIsNoOpEvenWithEvents() {
    MetadataEvents events = MetadataEvents.NOOP;
    assertThatNoException()
        .isThrownBy(
            () -> events.lineageCommit(ImmutableSet.of("topic-a"), ident, schema));
    assertThat(events.enabled()).isFalse();
  }

  @Test
  void lineageCommitEmitsOneEdgePerTopic() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    MetadataEvents events =
        new MetadataEvents(reporter, "iceberg", "warehouse", "my-pipe", "kafka");
    Set<String> topics = ImmutableSet.of("a", "b", "c");

    events.lineageCommit(topics, ident, schema);

    ArgumentCaptor<MetadataEvent> captor = ArgumentCaptor.forClass(MetadataEvent.class);
    verify(reporter, org.mockito.Mockito.times(3)).report(captor.capture());
    assertThat(captor.getAllValues())
        .allSatisfy(
            e -> {
              LineageEdge edge = (LineageEdge) e;
              assertThat(edge.source().name()).startsWith("kafka.");
              assertThat(edge.target().name()).isEqualTo("iceberg.warehouse.db.schema.events");
              assertThat(edge.pipelineName()).isEqualTo("my-pipe");
              assertThat(edge.columnsLineage()).hasSize(1);
            });
  }

  @Test
  void lineageCommitNoopOnEmptyTopics() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    new MetadataEvents(reporter, "iceberg", "default", "my-pipe", "kafka")
        .lineageCommit(ImmutableSet.of(), ident, schema);
    verify(reporter, never()).report(any());
  }

  @Test
  void reporterErrorsDoNotPropagate() {
    MetadataReporter reporter = mock(MetadataReporter.class);
    doThrow(new RuntimeException("kaboom")).when(reporter).report(any());
    MetadataEvents events =
        new MetadataEvents(reporter, "iceberg", "default", "my-pipe", "kafka");

    assertThatNoException()
        .isThrownBy(() -> events.lineageCommit(ImmutableSet.of("topic"), ident, schema));
  }
}
