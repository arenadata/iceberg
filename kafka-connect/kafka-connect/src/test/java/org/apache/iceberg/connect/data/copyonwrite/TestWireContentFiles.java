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
package org.apache.iceberg.connect.data.copyonwrite;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.RewriteAssigned;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A file read from a manifest answers {@code get(pos)} in the order of the scan projection, so it
 * reaches the control topic intact only after {@link WireContentFiles#forWire} rebuilds it. The
 * delete-file rebuild has to keep what the builder's {@code copy()} drops: the deletion-vector
 * fields, the reference of a file-scoped position delete and the equality field ids.
 */
public class TestWireContentFiles {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "region", Types.StringType.get()),
          required(3, "day", Types.IntegerType.get()));
  // two partition fields of different types: a value shifted by a position does not pass for its
  // neighbour
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("region").identity("day").build();
  private static final String US_PARTITION = "region=us/day=20240102";
  // not 0: an offset or size that is lost and defaulted does not pass for the real one
  private static final long DV_OFFSET = 4L;
  private static final long DV_SIZE = 40L;

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = createTable(TABLE_IDENTIFIER, 3);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testScannedDataFileSurvivesTheWire() {
    FileScanTask task = scannedTask("us");

    FileScanTaskDescriptor decoded = Iterables.getOnlyElement(roundTrip(forWire(task)).files());

    assertSameFile(decoded, task);
  }

  @Test
  public void testAssignedScannedDataFileSurvivesTheWire() {
    FileScanTask task = scannedTask("us");

    FileScanTaskDescriptor decoded = Iterables.getOnlyElement(roundTrip(assign(task)).files());

    assertSameFile(decoded, task);
  }

  @Test
  public void testScannedDeletionVectorSurvivesTheWire() {
    table.newRowDelta().addDeletes(deletionVector()).commit();
    FileScanTask task = scannedTask("us");

    FileScanTaskDescriptor decoded = Iterables.getOnlyElement(roundTrip(forWire(task)).files());

    assertSameDeletionVector(decoded, task);
  }

  @Test
  public void testAssignedScannedDeletionVectorSurvivesTheWire() {
    table.newRowDelta().addDeletes(deletionVector()).commit();
    FileScanTask task = scannedTask("us");

    FileScanTaskDescriptor decoded = Iterables.getOnlyElement(roundTrip(assign(task)).files());

    assertSameDeletionVector(decoded, task);
  }

  @Test
  public void testScannedFileScopedPositionDeleteSurvivesTheWire() {
    // v3 accepts only deletion vectors for position deletes
    table = createTable(TableIdentifier.of(NAMESPACE, "tbl_v2"), 2);
    table
        .newRowDelta()
        .addDeletes(
            FileMetadata.deleteFileBuilder(SPEC)
                .ofPositionDeletes()
                .withPath(table.location() + "/data/us-pos-deletes.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(50L)
                .withRecordCount(2L)
                .withPartitionPath(US_PARTITION)
                .withReferencedDataFile(dataPath("us", 20_240_102))
                .build())
        .commit();
    FileScanTask task = scannedTask("us");

    DeleteFile decoded = onlyDeleteFile(roundTrip(forWire(task)));

    DeleteFile expected = Iterables.getOnlyElement(task.deletes());
    assertThat(decoded.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(decoded.format()).isEqualTo(FileFormat.PARQUET);
    assertThat(decoded.location()).isEqualTo(expected.location());
    assertThat(decoded.referencedDataFile()).isEqualTo(dataPath("us", 20_240_102));
    assertThat(decoded.contentOffset()).isNull();
    assertThat(decoded.contentSizeInBytes()).isNull();
  }

  @Test
  public void testScannedEqualityDeleteSurvivesTheWire() {
    table
        .newRowDelta()
        .addDeletes(
            FileMetadata.deleteFileBuilder(SPEC)
                .ofEqualityDeletes(1)
                .withPath(table.location() + "/data/us-eq-deletes.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(50L)
                .withRecordCount(2L)
                .withPartitionPath(US_PARTITION)
                .build())
        .commit();
    FileScanTask task = scannedTask("us");

    DeleteFile decoded = onlyDeleteFile(roundTrip(forWire(task)));

    DeleteFile expected = Iterables.getOnlyElement(task.deletes());
    assertThat(decoded.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(decoded.location()).isEqualTo(expected.location());
    assertThat(decoded.equalityFieldIds()).containsExactly(1);
    assertThat(decoded.referencedDataFile()).isNull();
  }

  private static void assertSameFile(FileScanTaskDescriptor decoded, FileScanTask task) {
    DataFile expected = task.file();
    DataFile actual = decoded.dataFile();

    assertThat(decoded.specId()).isEqualTo(task.spec().specId());
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.format()).isEqualTo(FileFormat.PARQUET);
    assertThat(actual.recordCount()).isEqualTo(11L);
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    StructLike partition = actual.partition();
    assertThat(partition.get(0, CharSequence.class)).hasToString("us");
    assertThat(partition.get(1, Integer.class)).isEqualTo(20_240_102);
    assertThat(actual.firstRowId()).isNotNull().isEqualTo(expected.firstRowId());
  }

  private void assertSameDeletionVector(FileScanTaskDescriptor decoded, FileScanTask task) {
    DeleteFile expected = Iterables.getOnlyElement(task.deletes());
    DeleteFile actual = Iterables.getOnlyElement(decoded.deleteFiles());

    assertThat(decoded.dataFile().location()).isEqualTo(task.file().location());
    assertThat(actual.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(actual.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.recordCount()).isEqualTo(2L);
    assertThat(actual.referencedDataFile()).isEqualTo(dataPath("us", 20_240_102));
    assertThat(actual.contentOffset()).isEqualTo(DV_OFFSET);
    assertThat(actual.contentSizeInBytes()).isEqualTo(DV_SIZE);
  }

  private static DeleteFile onlyDeleteFile(Assignment assignment) {
    return Iterables.getOnlyElement(Iterables.getOnlyElement(assignment.files()).deleteFiles());
  }

  private static Assignment forWire(FileScanTask task) {
    FileScanTaskDescriptor descriptor =
        new FileScanTaskDescriptor(
            WireContentFiles.forWire(task.spec(), task.file()),
            task.deletes().stream()
                .map(delete -> WireContentFiles.forWire(task.spec(), delete))
                .collect(ImmutableList.toImmutableList()),
            task.spec().specId(),
            SPEC.partitionType());
    return new Assignment(
        "0", ImmutableList.of(new TopicPartitionRef("topic", 0)), ImmutableList.of(descriptor));
  }

  private static Assignment assign(FileScanTask task) {
    return Iterables.getOnlyElement(
        RewriteAssigner.assign(
            ImmutableList.of(task),
            ImmutableMap.of("0", ImmutableList.of(new TopicPartitionRef("topic", 0))),
            SPEC.partitionType()));
  }

  private Assignment roundTrip(Assignment assignment) {
    Event event =
        new Event(
            "cg-connector",
            new RewriteAssigned(
                SPEC.partitionType(),
                UUID.randomUUID(),
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                UUID.randomUUID(),
                0,
                table.currentSnapshot().snapshotId(),
                new StagedChangeFile(
                    "normalized.parquet", 1L, 1L, 0, 1, ImmutableMap.of(), ImmutableMap.of()),
                ImmutableList.of(assignment),
                0,
                1,
                ImmutableList.of(1)));

    RewriteAssigned decoded = (RewriteAssigned) AvroUtil.decode(AvroUtil.encode(event)).payload();
    return Iterables.getOnlyElement(decoded.assignments());
  }

  private FileScanTask scannedTask(String region) {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      planned.forEach(tasks::add);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Iterables.getOnlyElement(
        tasks.stream()
            .filter(task -> region.equals(task.file().partition().get(0, String.class)))
            .toList());
  }

  private Table createTable(TableIdentifier identifier, int formatVersion) {
    Table created =
        catalog.createTable(
            identifier,
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
    // the second file is the one checked: its first row id is not 0, so a lost one shows
    created
        .newAppend()
        .appendFile(file(created, "eu", 20_240_101, 7L))
        .appendFile(file(created, "us", 20_240_102, 11L))
        .commit();
    return created;
  }

  private DeleteFile deletionVector() {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(table.location() + "/data/us-dv.puffin")
        .withFormat(FileFormat.PUFFIN)
        .withFileSizeInBytes(100L)
        .withRecordCount(2L)
        .withPartitionPath(US_PARTITION)
        .withReferencedDataFile(dataPath("us", 20_240_102))
        .withContentOffset(DV_OFFSET)
        .withContentSizeInBytes(DV_SIZE)
        .build();
  }

  private String dataPath(String region, int day) {
    return dataPath(table, region, day);
  }

  private static String dataPath(Table target, String region, int day) {
    return target.location() + "/data/" + region + "-" + day + ".parquet";
  }

  private static DataFile file(Table target, String region, int day, long recordCount) {
    return DataFiles.builder(SPEC)
        .withPath(dataPath(target, region, day))
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L * recordCount)
        .withRecordCount(recordCount)
        .withPartitionPath("region=" + region + "/day=" + day)
        .build();
  }
}
