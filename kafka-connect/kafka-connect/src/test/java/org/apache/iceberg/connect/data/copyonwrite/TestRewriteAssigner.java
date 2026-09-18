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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.Assignment;
import org.apache.iceberg.connect.events.FileScanTaskDescriptor;
import org.apache.iceberg.connect.events.TopicPartitionRef;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRewriteAssigner {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "bucket", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("bucket").build();

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testAssignmentCoversPlanExactlyOnceAndBalancesLoad() {
    DataFile big = file("p1", 1, 500L);
    List<DataFile> small = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      small.add(file("p" + (i + 2), i + 2, 100L));
    }
    org.apache.iceberg.AppendFiles append = table.newAppend().appendFile(big);
    small.forEach(append::appendFile);
    append.commit();

    List<FileScanTask> plan = planFiles();
    assertThat(plan).hasSize(6);

    Map<String, List<TopicPartitionRef>> activeTasks =
        ImmutableMap.of(
            "task-0", ImmutableList.of(new TopicPartitionRef("t", 0)),
            "task-1", ImmutableList.of(new TopicPartitionRef("t", 1)));

    List<Assignment> assignments = RewriteAssigner.assign(plan, activeTasks, SPEC.partitionType());

    assertThat(assignments).hasSize(2);
    // every file assigned, none twice
    List<String> allAssignedPaths =
        assignments.stream()
            .flatMap(a -> a.files().stream())
            .map(d -> d.dataFile().location())
            .toList();
    assertThat(allAssignedPaths)
        .hasSize(6)
        .containsExactlyInAnyOrderElementsOf(plan.stream().map(t -> t.file().location()).toList());

    // LPT: the 500-byte file goes alone against the five 100-byte files: both sides land at 500
    for (Assignment assignment : assignments) {
      long total = assignment.files().stream().mapToLong(d -> d.dataFile().fileSizeInBytes()).sum();
      assertThat(total).isEqualTo(500L);
    }
  }

  @Test
  public void testEveryActiveTaskGetsAnAssignmentEvenWhenEmpty() {
    DataFile f1 = file("p1", 1, 100L);
    DataFile f2 = file("p2", 2, 100L);
    table.newAppend().appendFile(f1).appendFile(f2).commit();

    List<FileScanTask> plan = planFiles();

    Map<String, List<TopicPartitionRef>> activeTasks =
        ImmutableMap.of(
            "task-0", ImmutableList.of(),
            "task-1", ImmutableList.of(),
            "task-2", ImmutableList.of());

    List<Assignment> assignments = RewriteAssigner.assign(plan, activeTasks, SPEC.partitionType());

    assertThat(assignments).hasSize(3);
    assertThat(assignments.stream().map(Assignment::taskId))
        .containsExactlyInAnyOrder("task-0", "task-1", "task-2");
    long tasksWithFiles = assignments.stream().filter(a -> !a.files().isEmpty()).count();
    assertThat(tasksWithFiles).isEqualTo(2);
  }

  @Test
  public void testFilesWithinOneAssignmentAreClusteredByPartition() {
    for (int i = 5; i >= 1; i--) {
      table.newAppend().appendFile(file("p" + i, i, 10L)).commit();
    }

    List<FileScanTask> plan = planFiles();
    Map<String, List<TopicPartitionRef>> activeTasks =
        ImmutableMap.of("task-0", ImmutableList.of(new TopicPartitionRef("t", 0)));

    List<Assignment> assignments = RewriteAssigner.assign(plan, activeTasks, SPEC.partitionType());

    List<FileScanTaskDescriptor> files = assignments.get(0).files();
    List<String> partitionValues =
        files.stream().map(d -> d.dataFile().partition().get(0, String.class).toString()).toList();
    assertThat(partitionValues).isSorted();
  }

  private List<FileScanTask> planFiles() {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      planned.forEach(tasks::add);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return tasks;
  }

  private DataFile file(String partitionValue, int seq, long sizeBytes) {
    GenericRecord partitionRecord = GenericRecord.create(SCHEMA);
    org.apache.iceberg.PartitionKey key = new org.apache.iceberg.PartitionKey(SPEC, SCHEMA);
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", (long) seq);
    row.setField("bucket", partitionValue);
    key.partition(row);

    return DataFiles.builder(SPEC)
        .withPath(table.location() + "/data/" + partitionValue + "-" + seq + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(1)
        .withPartition(key)
        .build();
  }
}
