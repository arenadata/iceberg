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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RowChangesWritten;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Lists of envelopes as the copy-on-write committer takes them from a request and reports them
 * spent. Envelopes are compared by identity throughout, as the coordinator removes them from its
 * buffer.
 */
final class Envelopes {

  private Envelopes() {}

  /** {@code request} with only {@code envelopes}, held back to {@code withheld}. */
  static TableCommitRequest narrowedTo(
      TableCommitRequest request, List<Envelope> envelopes, List<Envelope> withheld) {
    Map<Integer, Long> offsets = Maps.newHashMap(request.controlTopicOffsets());
    withheld.forEach(envelope -> offsets.merge(envelope.partition(), envelope.offset(), Math::min));
    return new TableCommitRequest(
        request.tableReference(),
        envelopes,
        offsets,
        request.commitId(),
        withheld.isEmpty() ? request.validThroughTs() : null,
        request.activeTasks());
  }

  /** The envelopes of {@code type} with nothing of the other type below them on their partition. */
  static List<Envelope> leadingOnTheirPartition(List<Envelope> envelopes, PayloadType type) {
    Map<Integer, Long> firstOfOtherType = Maps.newHashMap();
    envelopes.stream()
        .filter(envelope -> envelope.event().payload().type() != type)
        .forEach(
            envelope -> firstOfOtherType.merge(envelope.partition(), envelope.offset(), Math::min));
    return envelopes.stream()
        .filter(envelope -> envelope.event().payload().type() == type)
        .filter(
            envelope -> {
              Long other = firstOfOtherType.get(envelope.partition());
              return other == null || envelope.offset() < other;
            })
        .collect(Collectors.toList());
  }

  /** By identity, as the coordinator removes them from its buffer. */
  static List<Envelope> without(List<Envelope> envelopes, Collection<Envelope> removed) {
    Set<Envelope> gone = Collections.newSetFromMap(new IdentityHashMap<>());
    gone.addAll(removed);
    return envelopes.stream()
        .filter(envelope -> !gone.contains(envelope))
        .collect(Collectors.toList());
  }

  /** Union by identity: a request often re-offers the very envelopes a drain already spent. */
  static List<Envelope> concat(List<Envelope> spent, Collection<Envelope> offered) {
    Set<Envelope> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    List<Envelope> all = Lists.newArrayList();
    Stream.concat(spent.stream(), offered.stream())
        .forEach(
            envelope -> {
              if (seen.add(envelope)) {
                all.add(envelope);
              }
            });
    return ImmutableList.copyOf(all);
  }

  /**
   * The envelope read last: the last one listed, or a later one on its partition. {@code null} for
   * none.
   */
  static Envelope latest(List<Envelope> envelopes) {
    if (envelopes.isEmpty()) {
      return null;
    }
    Envelope latest = envelopes.get(envelopes.size() - 1);
    for (Envelope envelope : envelopes) {
      if (envelope.partition() == latest.partition() && envelope.offset() > latest.offset()) {
        latest = envelope;
      }
    }
    return latest;
  }

  static boolean isDataWritten(Envelope envelope) {
    return envelope.event().payload().type() == PayloadType.DATA_WRITTEN;
  }

  static long countStagedFiles(Collection<Envelope> envelopes) {
    return envelopes.stream()
        .map(envelope -> envelope.event().payload())
        .filter(payload -> payload.type() == PayloadType.ROW_CHANGES_WRITTEN)
        .flatMap(payload -> ((RowChangesWritten) payload).stagedFiles().stream())
        .count();
  }
}
