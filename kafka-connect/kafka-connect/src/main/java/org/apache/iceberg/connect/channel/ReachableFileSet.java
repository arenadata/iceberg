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

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * What a staging cleanup must leave alone, in two kinds.
 *
 * <p><b>Anchors</b> are the files whose loss would lose data: staged files, change set manifests,
 * normalized files. They also tell whether a listing agrees with what the connector holds: a
 * listing that names none of the anchors under its prefix is spelled differently from the locations
 * the connector wrote. <b>Other live files</b> are the intermediate Puffin files: kept like anchors
 * however old, but left out of that check, since their owner, a statistics step, deletes them at
 * any moment; losing one costs only a recount of its gap.
 *
 * <p>Not thread safe: filled by one thread, and handed to another only through the pool that runs
 * the pass.
 */
final class ReachableFileSet {

  private final Set<String> anchors = Sets.newHashSet();
  private final Set<String> others = Sets.newHashSet();

  void addAnchor(String location) {
    anchors.add(location);
  }

  void addLive(String location) {
    others.add(location);
  }

  Set<String> anchors() {
    return Collections.unmodifiableSet(anchors);
  }

  Set<String> live() {
    return Sets.union(anchors, others);
  }

  void addAll(ReachableFileSet other) {
    anchors.addAll(other.anchors);
    others.addAll(other.others);
  }
}
