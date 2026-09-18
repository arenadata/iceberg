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

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.channel.SliceAttempt.ChunkOutcome;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestSliceAttempt {

  private static final TableReference TABLE_REFERENCE =
      TableReference.of("catalog", TableIdentifier.parse("db.tbl"), UUID.randomUUID());
  private static final UUID COMMIT_ID = UUID.randomUUID();
  private static final UUID CHANGE_SET_ID = UUID.randomUUID();

  @Test
  public void testAnIncompleteSetOfChunkIndexesDoesNotComplete() {
    SliceAttempt attempt = attemptFor("task-1");

    assertThat(attempt.acceptChunk(chunk("task-1", 0, 3)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    assertThat(attempt.acceptChunk(chunk("task-1", 2, 3)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    assertThat(attempt.missingTasks()).containsExactly("task-1");

    assertThat(attempt.acceptChunk(chunk("task-1", 1, 3)).outcome())
        .isEqualTo(ChunkOutcome.COMPLETE);
    assertThat(attempt.missingTasks()).isEmpty();
  }

  @Test
  public void testADuplicateIndexIsNotCountedAndAddsNoFiles() {
    SliceAttempt attempt = attemptFor("task-1");
    DataFile first = dataFile();
    DataFile redelivered = dataFile();

    assertThat(attempt.acceptChunk(chunk("task-1", 0, 2, first)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    // two arrivals of index 0 are not two of the two chunks announced
    assertThat(attempt.acceptChunk(chunk("task-1", 0, 2, redelivered)).outcome())
        .isEqualTo(ChunkOutcome.DUPLICATE);
    assertThat(attempt.collected()).containsExactly(first);
    assertThat(attempt.missingTasks()).containsExactly("task-1");
  }

  @Test
  public void testAChunkIndexAtTheAnnouncedCountIsAChunkError() {
    SliceAttempt attempt = attemptFor("task-1");
    // the constructor refuses such a chunk, so it is set the way decoding one off the wire sets it
    RewriteComplete outOfRange = chunk("task-1", 1, 2, dataFile());
    outOfRange.put(outOfRange.getSchema().getField("chunk_index").pos(), 2);

    SliceAttempt.ChunkResult result = attempt.acceptChunk(outOfRange);

    assertThat(result.outcome()).isEqualTo(ChunkOutcome.CHUNK_ERROR);
    assertThat(result.reason()).isEqualTo("task task-1 sent chunk 2 of an announced 2");
    assertThat(attempt.collected()).isEmpty();
    // the task shows among those heard from, with no chunk: the timeout reason names it that way
    assertThat(attempt.receivedChunks()).hasToString("{task-1=[]}");
  }

  @Test
  public void testAChangedAnnouncedChunkCountIsAChunkError() {
    SliceAttempt attempt = attemptFor("task-1");
    DataFile announcedTwo = dataFile();

    assertThat(attempt.acceptChunk(chunk("task-1", 0, 2, announcedTwo)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    SliceAttempt.ChunkResult result = attempt.acceptChunk(chunk("task-1", 1, 3, dataFile()));

    assertThat(result.outcome()).isEqualTo(ChunkOutcome.CHUNK_ERROR);
    assertThat(result.reason()).isEqualTo("task task-1 announced 2 chunks and then 3");
    assertThat(attempt.collected()).containsExactly(announcedTwo);
  }

  @Test
  public void testAnnouncedChunkCountsCompareByValue() {
    // past the range of cached Integer instances, so comparing references would tell two equal
    // counts apart
    SliceAttempt attempt = attemptFor("task-1");

    assertThat(attempt.acceptChunk(chunk("task-1", 0, 200)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    assertThat(attempt.acceptChunk(chunk("task-1", 1, 200)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
  }

  @Test
  public void testTheAnswerIsCompleteOnlyOnceEveryAssignedTaskHasAnsweredInFull() {
    SliceAttempt attempt = attemptFor("task-1", "task-2");

    assertThat(attempt.isAssigned("task-2")).isTrue();
    assertThat(attempt.isAssigned("task-3")).isFalse();
    assertThat(attempt.assignedTaskCount()).isEqualTo(2);

    assertThat(attempt.acceptChunk(chunk("task-1", 0, 1)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    assertThat(attempt.acceptChunk(chunk("task-2", 0, 2)).outcome())
        .isEqualTo(ChunkOutcome.PARTIAL);
    assertThat(attempt.missingTasks()).containsExactly("task-2");
    assertThat(attempt.acceptChunk(chunk("task-2", 1, 2)).outcome())
        .isEqualTo(ChunkOutcome.COMPLETE);
  }

  @Test
  public void testTakeCollectedEmptiesTheAttempt() {
    SliceAttempt attempt = attemptFor("task-1", "task-2");
    DataFile first = dataFile();
    DataFile second = dataFile();
    attempt.acceptChunk(chunk("task-1", 0, 1, first));
    attempt.acceptChunk(chunk("task-2", 0, 1, second));

    assertThat(attempt.takeCollected()).containsExactly(first, second);
    assertThat(attempt.collected()).isEmpty();
    assertThat(attempt.takeCollected()).isEmpty();
  }

  private static SliceAttempt attemptFor(String... taskIds) {
    return new SliceAttempt(Sets.newHashSet(taskIds), 1_000L);
  }

  private static RewriteComplete chunk(
      String taskId, int chunkIndex, int chunkCount, DataFile... dataFiles) {
    List<DataFile> files = ImmutableList.copyOf(dataFiles);
    return new RewriteComplete(
        StructType.of(),
        COMMIT_ID,
        TABLE_REFERENCE,
        CHANGE_SET_ID,
        0,
        taskId,
        RewriteComplete.STATUS_OK,
        files,
        chunkIndex,
        chunkCount);
  }

  private static DataFile dataFile() {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("s3://bucket/data/" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(1)
        .build();
  }
}
