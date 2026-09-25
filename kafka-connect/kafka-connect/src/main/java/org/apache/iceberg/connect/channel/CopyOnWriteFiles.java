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
import java.util.Collections;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one way the copy-on-write commit path deletes a file it owns.
 *
 * <p>Never throws. Every caller is already cleaning up after something (a cancelled slice, a
 * drained change set, a normalized file nobody will read again) and a failed delete is worth a line
 * in the log and nothing more: it leaves an orphan, which the staging cleanup will collect, whereas
 * unwinding out of a cleanup handler would abandon the rest of it.
 *
 * <p>This is a file-deletion primitive, not the ownership boundary. What may be deleted and when is
 * decided in {@code CopyOnWriteTableCommitter.discardSliceFiles} and nowhere else.
 */
final class CopyOnWriteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteFiles.class);

  private CopyOnWriteFiles() {}

  static void deleteQuietly(FileIO io, String location) {
    deleteQuietly(io, Collections.singletonList(location));
  }

  /**
   * Deletes every location, one {@code deleteFile} call each so a failure on one does not stop the
   * rest. A cleanup after a large change set can name hundreds of staged files; logging a stack
   * trace for each would bury the reason storage is unavailable under the noise of saying so
   * hundreds of times. Each failure is worth a line at DEBUG, and the whole call at most one at
   * WARN: the count, the first path, and the first exception.
   */
  static void deleteQuietly(FileIO io, Collection<String> locations) {
    int failures = 0;
    String firstFailedLocation = null;
    RuntimeException firstFailure = null;
    for (String location : locations) {
      try {
        io.deleteFile(location);
      } catch (RuntimeException e) {
        LOG.debug("Failed to delete copy-on-write file: {}", location, e);
        failures += 1;
        if (firstFailure == null) {
          firstFailedLocation = location;
          firstFailure = e;
        }
      }
    }
    if (failures > 0) {
      LOG.warn(
          "Failed to delete {} of {} copy-on-write file(s), first: {}",
          failures,
          locations.size(),
          firstFailedLocation,
          firstFailure);
    }
  }
}
