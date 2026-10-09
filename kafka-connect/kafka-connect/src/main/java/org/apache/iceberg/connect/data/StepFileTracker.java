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
package org.apache.iceberg.connect.data;

import java.util.Collection;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;

/**
 * The files a statistics step created and has not handed to {@code commitTransaction()}: the
 * catalog never saw them, so the step deletes them on any exit but a stop. A stopped step deletes
 * nothing and leaves its files to the orphan cleanup.
 */
final class StepFileTracker {
  private final Table table;
  private final TableIdentifier identifier;
  private final BooleanSupplier stopped;
  private final Logger log;
  private final Set<String> created = Sets.newLinkedHashSet();

  StepFileTracker(Table table, TableIdentifier identifier, BooleanSupplier stopped, Logger log) {
    this.table = table;
    this.identifier = identifier;
    this.stopped = stopped;
    this.log = log;
  }

  void add(String path) {
    created.add(path);
  }

  void handOver(Collection<String> paths) {
    created.removeAll(paths);
  }

  void deleteCreated() {
    for (String path : created) {
      delete(path, "the uncommitted statistics file");
    }
  }

  void delete(String path, String what) {
    if (stopped.getAsBoolean()) {
      log.debug("Not deleting {} {} of table {}: the step is stopped", what, path, identifier);
      return;
    }

    try {
      table.io().deleteFile(path);
    } catch (RuntimeException e) {
      log.warn("Cannot delete {} {} of table {}", what, path, identifier, e);
    }
  }
}
