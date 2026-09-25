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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.awaitility.Awaitility;

/**
 * Base for tests of CDC streams applied as row-level operations. Unlike {@link
 * IntegrationTestBase#runTest}, which waits for exactly one snapshot, it waits for the table rows:
 * a copy-on-write drain may commit several snapshots.
 */
public abstract class IntegrationTestBaseRowLevel extends AbstractTestBase {

  protected static final String COPY_ON_WRITE = "copy-on-write";
  protected static final String MERGE_ON_READ = "merge-on-read";

  // the common Integer.MAX_VALUE fails the copy-on-write check that the staging orphan TTL
  // exceeds the commit interval plus the commit timeout
  private static final int COMMIT_TIMEOUT_MS = 30_000;

  /**
   * CDC connector config with the given row-level mode; a null mode leaves the connector default.
   */
  protected KafkaConnectUtils.Config rowLevelConfig(
      boolean useSchema, String rowLevelMode, String branch, Map<String, Object> extraConfig) {
    KafkaConnectUtils.Config connectorConfig = createConfig(useSchema);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    connectorConfig
        .config("iceberg.tables.cdc-field", "op")
        .config("iceberg.control.commit.timeout-ms", COMMIT_TIMEOUT_MS);
    if (rowLevelMode != null) {
      connectorConfig.config("iceberg.tables.row-level-mode", rowLevelMode);
    }
    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }
    extraConfig.forEach(connectorConfig::config);
    return connectorConfig;
  }

  /** Sends the events and waits until the rows of the table (branch) are the expected ones. */
  protected void sendAndAwait(
      TableIdentifier tableIdentifier,
      String branch,
      boolean useSchema,
      List<CdcTestEvent> events,
      List<CdcTestEvent> expectedRows) {
    events.forEach(event -> send(testTopic(), event, useSchema));
    flush();
    awaitRows(tableIdentifier, branch, expectedRows);
  }

  /** Waits until the rows of the table (branch) are the expected ones. */
  protected void awaitRows(
      TableIdentifier tableIdentifier, String branch, List<CdcTestEvent> expectedRows) {
    List<String> expected = expectedRows.stream().map(CdcTestEvent::row).toList();
    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () ->
                assertThat(rows(tableIdentifier, branch))
                    .containsExactlyInAnyOrderElementsOf(expected));
  }

  /** Rows of the table (branch) in the form of {@link CdcTestEvent#row()}. */
  protected List<String> rows(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog().loadTable(tableIdentifier);
    Snapshot snapshot = snapshot(table, branch);
    if (snapshot == null) {
      return List.of();
    }
    return Lists.newArrayList(
            IcebergGenerics.read(table).useSnapshot(snapshot.snapshotId()).build())
        .stream()
        .map(CdcTestEvent::row)
        .toList();
  }

  protected Snapshot snapshot(Table table, String branch) {
    return branch == null ? table.currentSnapshot() : table.snapshot(branch);
  }

  /** Locations of the live data files of the snapshot that may hold rows matching the filter. */
  protected Set<String> dataFileLocations(Table table, long snapshotId, Expression filter) {
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).filter(filter).planFiles()) {
      Set<String> locations = Sets.newHashSet();
      tasks.forEach(task -> locations.add(task.file().location()));
      return locations;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Asserts that no snapshot descending from the given one added delete files (AC-02). {@link
   * #deleteFiles} looks at the latest snapshot only.
   */
  protected void assertNoConnectorDeleteFiles(Table table, long fromSnapshotId) {
    table.refresh();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.snapshotId() != fromSnapshotId
          && SnapshotUtil.isAncestorOf(table, snapshot.snapshotId(), fromSnapshotId)) {
        assertThat(snapshot.addedDeleteFiles(table.io()))
            .as("delete files added by snapshot %s", snapshot.snapshotId())
            .isEmpty();
      }
    }
  }
}
