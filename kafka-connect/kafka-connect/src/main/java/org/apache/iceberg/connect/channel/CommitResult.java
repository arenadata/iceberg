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

import java.util.List;
import org.apache.iceberg.connect.channel.TableCommitter.Outcome;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * What one table's commit did this cycle: how it ended, and which buffered responses it used up.
 *
 * <p>The two are not the same question, and copy-on-write is where they come apart. A drain spans
 * several cycles, so responses keep arriving while it runs; dropping them all when the drain
 * finally ends would silently discard the ones that never entered a change set. So the committer
 * names the envelopes it is done with, and {@link CommitState#clearResponses} drops exactly those.
 *
 * <p>An envelope is spent once the first slice of its change set has committed: at that point
 * either the {@code change-set-id} pointer is stored in a snapshot summary (an intermediate slice),
 * or the data itself is applied and the offsets advanced (a single-slice change set). Before that
 * the frozen manifest is referenced by nothing, and releasing the offsets would orphan its staged
 * files if the coordinator restarted.
 */
final class CommitResult {

  private final Outcome outcome;
  private final List<Envelope> consumed;

  private CommitResult(Outcome outcome, List<Envelope> consumed) {
    this.outcome = outcome;
    this.consumed = consumed;
  }

  static CommitResult of(Outcome outcome, List<Envelope> consumed) {
    return new CommitResult(outcome, ImmutableList.copyOf(consumed));
  }

  static CommitResult committed(List<Envelope> consumed) {
    return of(Outcome.COMMITTED, consumed);
  }

  static CommitResult pending(List<Envelope> consumed) {
    return of(Outcome.PENDING, consumed);
  }

  static CommitResult failed(List<Envelope> consumed) {
    return of(Outcome.FAILED, consumed);
  }

  Outcome outcome() {
    return outcome;
  }

  List<Envelope> consumed() {
    return consumed;
  }
}
