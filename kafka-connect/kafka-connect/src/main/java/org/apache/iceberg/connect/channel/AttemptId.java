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

import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.connect.events.RewriteComplete;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Which attempt at a slice an answer or a queued transition belongs to.
 *
 * <p>An attempt is the triple, not the pair: a resumed drain numbers its slices from zero again
 * under the commit id of the cycle that resumed it, so only the commit id tells slice 0 of one
 * drain from slice 0 of the next. An answer to a drain that is gone was rewritten against another
 * plan, and counting it would commit files that do not cover this slice's keys.
 *
 * <p>Taken from the state when it is needed, never kept in it: a transition queued before a slice
 * is handed out has to be told apart as well.
 */
final class AttemptId {

  private final UUID commitId;
  private final UUID changeSetId;
  private final int sliceSeq;

  private AttemptId(UUID commitId, UUID changeSetId, int sliceSeq) {
    this.commitId = commitId;
    this.changeSetId = changeSetId;
    this.sliceSeq = sliceSeq;
  }

  /** The attempt {@code state} is at. Called with its lock held, while a change set drains. */
  static AttemptId of(TableDrainState state) {
    return new AttemptId(state.commitId, state.manifest.changeSetId(), state.sliceSeq);
  }

  /** The attempt a worker's answer was rewritten for. */
  static AttemptId of(RewriteComplete payload) {
    return new AttemptId(payload.commitId(), payload.changeSetId(), payload.sliceSeq());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AttemptId)) {
      return false;
    }
    AttemptId that = (AttemptId) other;
    return sliceSeq == that.sliceSeq
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(changeSetId, that.changeSetId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitId, changeSetId, sliceSeq);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("commitId", commitId)
        .add("changeSetId", changeSetId)
        .add("sliceSeq", sliceSeq)
        .toString();
  }
}
