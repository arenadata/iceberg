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

import java.util.Collection;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.channel.StatisticsStepQueue.CleanupTable;
import org.apache.iceberg.connect.data.ColumnSketchCollector;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeFileWriter;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The staging cleanup's view of merge-on-read with statistics: the tables of the queue of
 * statistics steps, and in each the intermediate Puffin files of its running and waiting tasks.
 * Only {@code <stagingDirectory>/stats/} is cleaned up: the files of a change set that
 * copy-on-write left unfinished before the switch stay where they are, and nothing in memory or
 * persisted reaches them from here.
 *
 * <p>No anchors, and nothing persisted: a Puffin file of a response is reachable from the buffer
 * until the task that takes it is set, and from the queue until the task has tried to delete it.
 */
final class MergeOnReadStagingCleanupSource implements StagingCleanupSource {

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final StatisticsStepQueue queue;

  MergeOnReadStagingCleanupSource(
      Catalog catalog, IcebergSinkConfig config, StatisticsStepQueue queue) {
    this.catalog = catalog;
    this.config = config;
    this.queue = queue;
  }

  @Override
  public Collection<TableReference> tables() {
    List<TableReference> tables = Lists.newArrayList();
    queue
        .cleanupTables()
        .forEach((id, table) -> tables.add(TableReference.of(catalog.name(), id, table.uuid())));
    return tables;
  }

  @Override
  public CleanupTarget snapshot(TableReference tableReference) {
    TableIdentifier identifier = tableReference.identifier();
    CleanupTable table = queue.cleanupTable(identifier);
    if (table == null) {
      // left the set since tables(): gone, and nothing to clean up with
      return null;
    }

    ReachableFileSet inMemory = new ReachableFileSet();
    queue.inFlightSketchLocations(identifier).forEach(inMemory::addLive);
    String stagingLocation =
        StagedChangeFileWriter.stagingLocation(table.location(), config.stagingLocation());
    String cleanupPrefix =
        StagedChangeFileWriter.stagingDirectory(
                stagingLocation, tableReference, config.connectGroupId())
            + "/"
            + ColumnSketchCollector.STATS_DIR
            + "/";
    return new CleanupTarget(
        table.name(), table.uuid(), null, tableReference, stagingLocation, cleanupPrefix, inMemory);
  }

  @Override
  public Table load(TableIdentifier identifier) {
    return catalog.loadTable(identifier);
  }

  @Override
  public void addPersistedReachable(Table table, CleanupTarget target, ReachableFileSet into) {}
}
