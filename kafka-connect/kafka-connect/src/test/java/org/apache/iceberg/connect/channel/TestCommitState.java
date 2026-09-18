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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCommitState {
  @Test
  public void testIsCommitReady() {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    DataComplete payload1 = mock(DataComplete.class);
    when(payload1.commitId()).thenReturn(commitState.currentCommitId());
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp, tp));

    DataComplete payload2 = mock(DataComplete.class);
    when(payload2.commitId()).thenReturn(commitState.currentCommitId());
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp));

    DataComplete payload3 = mock(DataComplete.class);
    when(payload3.commitId()).thenReturn(UUID.randomUUID());
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp));

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));
    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.isCommitReady(3)).isTrue();
    assertThat(commitState.isCommitReady(4)).isFalse();
  }

  @Test
  public void testGetValidThroughTs() {
    DataComplete payload1 = mock(DataComplete.class);
    TopicPartitionOffset tp1 = mock(TopicPartitionOffset.class);
    OffsetDateTime ts1 = EventTestUtil.now();
    when(tp1.timestamp()).thenReturn(ts1);

    TopicPartitionOffset tp2 = mock(TopicPartitionOffset.class);
    OffsetDateTime ts2 = ts1.plusSeconds(1);
    when(tp2.timestamp()).thenReturn(ts2);
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp1, tp2));

    DataComplete payload2 = mock(DataComplete.class);
    TopicPartitionOffset tp3 = mock(TopicPartitionOffset.class);
    OffsetDateTime ts3 = ts1.plusSeconds(2);
    when(tp3.timestamp()).thenReturn(ts3);
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp3));

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));

    assertThat(commitState.validThroughTs(false)).isEqualTo(ts1);
    assertThat(commitState.validThroughTs(true)).isNull();

    // null timestamp for one, so should not set a valid-through timestamp
    DataComplete payload3 = mock(DataComplete.class);
    TopicPartitionOffset tp4 = mock(TopicPartitionOffset.class);
    when(tp4.timestamp()).thenReturn(null);
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp4));

    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.validThroughTs(false)).isNull();
    assertThat(commitState.validThroughTs(true)).isNull();
  }

  @Test
  public void testBufferedResponsesNameTheStagedFilesOfADrainInProgress() {
    // the staging cleanup decides reachability from three places, and this is the third: a response
    // that arrived while a drain was running belongs to no change set yet, so nothing in the table
    // points at its staged files. A drain outlasting the orphan TTL would have them deleted
    // before the freeze that adopts them. CommitState owns the buffer; reading staged files out of
    // it is the copy-on-write cleanup's business, so the projection lives there
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    StagedChangeFile staged =
        new StagedChangeFile(
            "s3://warehouse/db/tbl/staging/task-0/epoch/0.avro",
            10L,
            1L,
            0,
            1,
            ImmutableMap.of(),
            ImmutableMap.of());
    Envelope rowChanges =
        wrapInEnvelope(
            new RowChangesWritten(
                UUID.randomUUID(),
                TableReference.of("catalog", TableIdentifier.of("db", "tbl")),
                "task-0",
                ImmutableList.of("src"),
                ImmutableList.of(staged)));
    commitState.addResponse(rowChanges);

    assertThat(StagingCleanupScheduler.stagedFileLocations(commitState.bufferedResponses()))
        .containsExactly(staged.location());

    // once the response is spent it stops protecting anything
    commitState.clearResponses(ImmutableList.of(rowChanges));
    assertThat(StagingCleanupScheduler.stagedFileLocations(commitState.bufferedResponses()))
        .isEmpty();
  }

  @Test
  public void testTheActiveTasksAreTheTasksThatReportedInForTheCurrentCommit() {
    // the executors of a copy-on-write rewrite. A task that landed no rows is one of them. A
    // DataComplete of an earlier cycle, re-read after a restart, names partitions its task may no
    // longer own, and one without a task id comes from a merge-on-read version of the task
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();
    UUID commitId = commitState.currentCommitId();
    OffsetDateTime ts = EventTestUtil.now();

    commitState.addReady(
        wrapInEnvelope(
            new DataComplete(
                UUID.randomUUID(),
                ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)),
                "task-1")));
    commitState.addReady(
        wrapInEnvelope(
            new DataComplete(
                commitId,
                ImmutableList.of(
                    new TopicPartitionOffset("topic", 0, 1L, ts),
                    new TopicPartitionOffset("topic", 2, 1L, ts)),
                "task-0")));
    commitState.addReady(
        wrapInEnvelope(
            new DataComplete(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 3, 1L, ts)))));

    // no response is buffered at all: task-0 wrote nothing this cycle
    Map<String, List<TopicPartitionRef>> activeTasks = commitState.activeTasks();

    assertThat(activeTasks).containsOnlyKeys("task-0");
    assertThat(activeTasks.get("task-0"))
        .extracting(TopicPartitionRef::topic, TopicPartitionRef::partition)
        .containsExactly(tuple("topic", 0), tuple("topic", 2));
  }

  private Envelope wrapInEnvelope(Payload payload) {
    Event event = mock(Event.class);
    when(event.payload()).thenReturn(payload);
    return new Envelope(event, 0, 0);
  }
}
