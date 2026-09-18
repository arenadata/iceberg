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

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.StructType;

/** The one assumption every payload carrying a content file makes about partition specs. */
class WirePartitions {

  private WirePartitions() {}

  /**
   * Rejects a content file whose partition tuple is not the length this payload's partition type
   * expects.
   *
   * <p>The {@code DataFile} struct in these payloads is derived from a single {@code partitionType}
   * and encoded positionally, so a file of another spec does not fail: a short tuple is padded with
   * nulls, a long one truncated. Only arity is checked here. Same-arity specs with fields reordered
   * or renamed (a v2 {@code DROP PARTITION FIELD} followed by an {@code ADD}) pass and would encode
   * one column's value under another's name; spec id checks catch that case instead, in {@code
   * AffectedFilePlanner} (no slice spans spec ids) and {@code RewriteAssignmentRunner} (no answer
   * with a file of another spec than the table's).
   */
  static void checkPartitionTypeFits(StructType partitionType, ContentFile<?> file, String what) {
    int expected = partitionType.fields().size();
    int actual = file.partition().size();
    Preconditions.checkArgument(
        expected == actual,
        "Cannot put %s %s on the wire: its partition tuple has %s field(s), the event's partition "
            + "type has %s. A file of another partition spec cannot travel in this payload",
        what,
        file.location(),
        actual,
        expected);
  }
}
