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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.TableReference;

/**
 * What a table committer tells the staging cleanup of its coordinator: which tables to clean up and
 * what is reachable in each. When to clean up and what to delete is {@link
 * StagingCleanupScheduler}'s.
 */
interface StagingCleanupSource {

  /**
   * The tables ready to be cleaned up: with a loaded table and a staging location. Called on the
   * coordinator thread on every tick a cleanup may be due, so it never waits.
   */
  Collection<TableReference> tables();

  /**
   * The entry of {@code tableReference} to a pass, with what it reaches in memory read in full, or
   * {@code null} if it cannot be read now without waiting. Called on the coordinator thread.
   */
  CleanupTarget snapshot(TableReference tableReference);

  /** The table loaded afresh for the pass. Called on the pool of table transitions. */
  Table load(TableIdentifier identifier);

  /**
   * Adds what the persisted state of {@code table} reaches. Called on the pool of table
   * transitions, and may read files.
   *
   * @throws RuntimeException if it cannot be read: the table is not cleaned up in this pass
   */
  void addPersistedReachable(Table table, CleanupTarget target, ReachableFileSet into);
}
