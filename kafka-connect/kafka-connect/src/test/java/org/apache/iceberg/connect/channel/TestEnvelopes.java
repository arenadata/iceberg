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
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestEnvelopes {

  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TableIdentifier.parse("db.tbl"), UUID.randomUUID());

  private static final Event SHARED_EVENT = rowChanges();

  @Test
  public void testLeadingOnTheirPartitionIsCutOffOnlyByTheOtherTypeBelowOnTheSamePartition() {
    Envelope rowChangesBelowTail = rowChangesAt(0, 3);
    Envelope tail = dataWrittenAt(0, 5);
    Envelope rowChangesAboveTail = rowChangesAt(0, 7);
    // above the tail's offset, but on a partition the tail is not on
    Envelope rowChangesElsewhere = rowChangesAt(1, 10);
    List<Envelope> envelopes =
        List.of(rowChangesBelowTail, tail, rowChangesAboveTail, rowChangesElsewhere);

    assertThat(Envelopes.leadingOnTheirPartition(envelopes, PayloadType.ROW_CHANGES_WRITTEN))
        .containsExactly(rowChangesBelowTail, rowChangesElsewhere);
    assertThat(Envelopes.leadingOnTheirPartition(envelopes, PayloadType.DATA_WRITTEN)).isEmpty();
  }

  @Test
  public void testNarrowedToHoldsOffsetsBackToTheOldestWithheldEnvelope() {
    OffsetDateTime validThroughTs = OffsetDateTime.now();
    Map<String, List<TopicPartitionRef>> activeTasks = ImmutableMap.of("task-0", List.of());
    TableCommitRequest request =
        new TableCommitRequest(
            TABLE_REFERENCE,
            List.of(),
            ImmutableMap.of(0, 20L, 1, 30L),
            UUID.randomUUID(),
            validThroughTs,
            activeTasks);
    Envelope kept = rowChangesAt(0, 2);
    List<Envelope> withheld = List.of(dataWrittenAt(0, 7), rowChangesAt(0, 4));

    TableCommitRequest narrowed = Envelopes.narrowedTo(request, List.of(kept), withheld);

    assertThat(narrowed.envelopes()).containsExactly(kept);
    assertThat(narrowed.controlTopicOffsets()).isEqualTo(Map.of(0, 4L, 1, 30L));
    assertThat(narrowed.validThroughTs()).isNull();
    assertThat(narrowed.tableReference()).isSameAs(TABLE_REFERENCE);
    assertThat(narrowed.commitId()).isEqualTo(request.commitId());
    assertThat(narrowed.activeTasks()).isEqualTo(activeTasks);
    assertThat(request.controlTopicOffsets()).isEqualTo(Map.of(0, 20L, 1, 30L));
  }

  @Test
  public void testNarrowedToWithNothingWithheldKeepsOffsetsAndValidThrough() {
    OffsetDateTime validThroughTs = OffsetDateTime.now();
    TableCommitRequest request =
        new TableCommitRequest(
            TABLE_REFERENCE, List.of(), ImmutableMap.of(0, 20L), UUID.randomUUID(), validThroughTs);
    Envelope kept = rowChangesAt(0, 2);

    TableCommitRequest narrowed = Envelopes.narrowedTo(request, List.of(kept), List.of());

    assertThat(narrowed.envelopes()).containsExactly(kept);
    assertThat(narrowed.controlTopicOffsets()).isEqualTo(Map.of(0, 20L));
    assertThat(narrowed.validThroughTs()).isEqualTo(validThroughTs);
  }

  @Test
  public void testWithoutRemovesByIdentity() {
    Envelope removed = equalByContent(0, 1);
    Envelope equalToRemoved = equalByContent(0, 1);
    Envelope other = rowChangesAt(0, 2);

    assertThat(Envelopes.without(List.of(removed, equalToRemoved, other), List.of(removed)))
        .hasSize(2)
        .first()
        .isSameAs(equalToRemoved);
  }

  @Test
  public void testConcatCollapsesTheSameObjectOnly() {
    Envelope spent = equalByContent(0, 1);
    Envelope equalToSpent = equalByContent(0, 1);
    Envelope offered = rowChangesAt(0, 2);

    List<Envelope> all = Envelopes.concat(List.of(spent), List.of(spent, equalToSpent, offered));

    assertThat(all).hasSize(3);
    assertThat(all.get(0)).isSameAs(spent);
    assertThat(all.get(1)).isSameAs(equalToSpent);
    assertThat(all.get(2)).isSameAs(offered);
  }

  private static Envelope rowChangesAt(int partition, long offset) {
    return new Envelope(rowChanges(), partition, offset);
  }

  private static Envelope dataWrittenAt(int partition, long offset) {
    DataWritten payload =
        new DataWritten(StructType.of(), UUID.randomUUID(), TABLE_REFERENCE, List.of(), List.of());
    return new Envelope(new Event("group", payload), partition, offset);
  }

  /** Equal to any envelope at the same position: only identity tells two of them apart. */
  private static Envelope equalByContent(int partition, long offset) {
    return new Envelope(SHARED_EVENT, partition, offset) {
      @Override
      public boolean equals(Object other) {
        return other instanceof Envelope
            && ((Envelope) other).partition() == partition()
            && ((Envelope) other).offset() == offset();
      }

      @Override
      public int hashCode() {
        return Objects.hash(partition(), offset());
      }
    };
  }

  private static Event rowChanges() {
    RowChangesWritten payload =
        new RowChangesWritten(
            UUID.randomUUID(), TABLE_REFERENCE, "task-0", List.of("src-topic"), List.of());
    return new Event("group", payload);
  }
}
