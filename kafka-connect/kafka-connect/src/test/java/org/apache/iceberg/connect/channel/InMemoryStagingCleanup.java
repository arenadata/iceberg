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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * The staging cleanup of a coordinator over an in-memory catalog, whose {@code FileIO} cannot list
 * a prefix: the tables the cleanup loads list the in-memory files of the JVM instead. Under a TTL
 * of 0 a pass deletes everything under the prefix that the source, the buffer and the persisted
 * state do not reach. The pass runs inline, on the calling thread.
 */
final class InMemoryStagingCleanup {

  private InMemoryStagingCleanup() {}

  static StagingCleanupScheduler over(
      IcebergSinkConfig config, StagingCleanupSource source, BooleanSupplier stopped) {
    return new StagingCleanupScheduler(config, Runnable::run, listing(source), () -> true, stopped);
  }

  /** {@code source}, except that the tables it loads for a pass can list a prefix. */
  static StagingCleanupSource listing(StagingCleanupSource source) {
    return new StagingCleanupSource() {
      @Override
      public Collection<TableReference> tables() {
        return source.tables();
      }

      @Override
      public CleanupTarget snapshot(TableReference tableReference) {
        return source.snapshot(tableReference);
      }

      @Override
      public Table load(TableIdentifier identifier) {
        Table table = spy(source.load(identifier));
        doReturn(new ListingInMemoryFileIO()).when(table).io();
        return table;
      }

      @Override
      public void addPersistedReachable(Table table, CleanupTarget target, ReachableFileSet into) {
        source.addPersistedReachable(table, target, into);
      }
    };
  }

  /** {@link InMemoryFileIO} that lists the files of the JVM under a prefix, all created at 1 ms. */
  static class ListingInMemoryFileIO extends InMemoryFileIO implements SupportsPrefixOperations {

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      return locationsUnder(prefix).stream()
          .map(location -> new FileInfo(location, 1L, 1L))
          .collect(Collectors.toList());
    }

    @Override
    public void deletePrefix(String prefix) {
      locationsUnder(prefix).forEach(this::deleteFile);
    }

    private static List<String> locationsUnder(String prefix) {
      return inMemoryFiles().keySet().stream()
          .filter(location -> location.startsWith(prefix))
          .sorted()
          .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, byte[]> inMemoryFiles() {
      try {
        Field files = InMemoryFileIO.class.getDeclaredField("IN_MEMORY_FILES");
        files.setAccessible(true);
        return (Map<String, byte[]>) files.get(null);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Cannot list the in-memory files", e);
      }
    }
  }
}
