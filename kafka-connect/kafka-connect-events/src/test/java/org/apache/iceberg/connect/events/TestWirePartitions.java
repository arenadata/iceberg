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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * The payloads that carry a content file build their {@code DataFile} schema from one partition
 * type and encode positionally, so a file of another spec has to be refused rather than quietly
 * re-shaped. Without the check this is not an exception but a wrong value in a manifest: an arity
 * mismatch pads with nulls or truncates, and equal arity with different field order writes one
 * partition column's value under another's name.
 */
public class TestWirePartitions {

  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "cat", Types.StringType.get())));

  private static final PartitionSpec ONE_FIELD =
      PartitionSpec.builderFor(SCHEMA).identity("id").build();
  private static final PartitionSpec TWO_FIELDS =
      PartitionSpec.builderFor(SCHEMA).identity("id").identity("cat").withSpecId(1).build();

  private static DataFile fileUnder(PartitionSpec spec) {
    PartitionData partition = new PartitionData(spec.partitionType());
    if (spec.isPartitioned()) {
      partition.set(0, 1L);
    }
    if (spec.fields().size() > 1) {
      partition.set(1, "a");
    }
    return DataFiles.builder(spec)
        .withPath("path/to/file.parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(1L)
        .withPartition(partition)
        .build();
  }

  private static DeleteFile deleteUnder(PartitionSpec spec) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath("path/to/deletes.parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10L)
        .withRecordCount(1L)
        .withPartition(fileUnder(spec).partition())
        .build();
  }

  private static RewriteAssigned rewriteAssigned(
      Types.StructType partitionType, FileScanTaskDescriptor file) {
    return new RewriteAssigned(
        partitionType,
        UUID.randomUUID(),
        new TableReference("catalog", ImmutableList.of("db"), "tbl"),
        UUID.randomUUID(),
        0,
        1L,
        EventTestUtil.createStagedChangeFile(),
        ImmutableList.of(new Assignment("0", ImmutableList.of(), ImmutableList.of(file))),
        0,
        1,
        ImmutableList.of(1));
  }

  @Test
  public void testRewriteAssignedRefusesAFileOfAnotherSpec() {
    // the descriptor checks its files against the type it was handed, the event builds the wire
    // schema from its own: two arguments from two callers, so the event has to check as well
    FileScanTaskDescriptor oneFieldDataFile =
        new FileScanTaskDescriptor(
            fileUnder(ONE_FIELD),
            ImmutableList.of(),
            ONE_FIELD.specId(),
            ONE_FIELD.partitionType());
    assertThatThrownBy(() -> rewriteAssigned(TWO_FIELDS.partitionType(), oneFieldDataFile))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("data file path/to/file.parquet")
        .hasMessageContaining("partition tuple has 1 field(s)")
        .hasMessageContaining("partition type has 2");

    // the descriptor's constructor holds its delete files to its data file's type, so a delete file
    // of another spec only gets this far in a descriptor filled in the way the reader fills it
    FileScanTaskDescriptor oneFieldDeleteFile =
        new FileScanTaskDescriptor(oneFieldDataFile.getSchema());
    oneFieldDeleteFile.put(0, fileUnder(TWO_FIELDS));
    oneFieldDeleteFile.put(1, ImmutableList.of(deleteUnder(ONE_FIELD)));
    oneFieldDeleteFile.put(2, TWO_FIELDS.specId());
    assertThatThrownBy(() -> rewriteAssigned(TWO_FIELDS.partitionType(), oneFieldDeleteFile))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("delete file path/to/deletes.parquet of partition spec 0")
        .hasMessageContaining("with its partition tuple");
  }

  @Test
  public void testADeleteFileOfAnotherSpecTravelsWithoutItsPartitionTuple() {
    // an unpartitioned spec's delete applies to the data files of every spec. It keeps its own spec
    // id, and with no tuple of its own nothing lands under the data file's partition fields
    PartitionSpec unpartitioned = PartitionSpec.builderFor(SCHEMA).withSpecId(2).build();
    FileScanTaskDescriptor withGlobalDelete =
        new FileScanTaskDescriptor(
            fileUnder(TWO_FIELDS),
            ImmutableList.of(deleteUnder(unpartitioned)),
            TWO_FIELDS.specId(),
            TWO_FIELDS.partitionType());
    rewriteAssigned(TWO_FIELDS.partitionType(), withGlobalDelete);

    assertThatThrownBy(
            () ->
                new FileScanTaskDescriptor(
                    fileUnder(TWO_FIELDS),
                    ImmutableList.of(deleteUnder(ONE_FIELD)),
                    TWO_FIELDS.specId(),
                    TWO_FIELDS.partitionType()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("delete file path/to/deletes.parquet of partition spec 0")
        .hasMessageContaining("with its partition tuple");
  }

  @Test
  public void testRewriteCompleteRefusesAFileOfAnotherSpec() {
    assertThatThrownBy(
            () ->
                new RewriteComplete(
                    TWO_FIELDS.partitionType(),
                    UUID.randomUUID(),
                    new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                    UUID.randomUUID(),
                    0,
                    "0",
                    RewriteComplete.STATUS_OK,
                    ImmutableList.of(fileUnder(ONE_FIELD)),
                    0,
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition tuple has 1 field(s)")
        .hasMessageContaining("partition type has 2");
  }

  @Test
  public void testFileScanTaskDescriptorRefusesAFileOfAnotherSpec() {
    assertThatThrownBy(
            () ->
                new FileScanTaskDescriptor(
                    fileUnder(TWO_FIELDS),
                    ImmutableList.of(),
                    TWO_FIELDS.specId(),
                    ONE_FIELD.partitionType()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition tuple has 2 field(s)")
        .hasMessageContaining("partition type has 1");
  }

  @Test
  public void testAFileOfTheEventsOwnSpecIsAccepted() {
    new RewriteComplete(
        TWO_FIELDS.partitionType(),
        UUID.randomUUID(),
        new TableReference("catalog", ImmutableList.of("db"), "tbl"),
        UUID.randomUUID(),
        0,
        "0",
        RewriteComplete.STATUS_OK,
        ImmutableList.of(fileUnder(TWO_FIELDS)),
        0,
        1);
    FileScanTaskDescriptor oneFieldFile =
        new FileScanTaskDescriptor(
            fileUnder(ONE_FIELD),
            ImmutableList.of(deleteUnder(ONE_FIELD)),
            ONE_FIELD.specId(),
            ONE_FIELD.partitionType());
    rewriteAssigned(ONE_FIELD.partitionType(), oneFieldFile);
  }
}
