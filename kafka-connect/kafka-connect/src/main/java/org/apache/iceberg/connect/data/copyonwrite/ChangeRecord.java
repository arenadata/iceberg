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
package org.apache.iceberg.connect.data.copyonwrite;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The fold of every staged operation seen so far for one identifier key.
 *
 * <p>Only two facts about the whole chain of operations for a key ever matter: the operation with
 * the smallest {@link OrderKey} (was the key born inside this change set, i.e. did it start with an
 * {@code INSERT}?) and the operation with the largest one (what is its state now?). Every
 * transition (INSERT-&gt;UPDATE, INSERT-&gt;DELETE, UPDATE-&gt;UPDATE, UPDATE-&gt;DELETE,
 * DELETE-&gt;INSERT, DELETE-&gt;DELETE) is a special case of that: intermediate operations never
 * change the answer. {@link #combine} is therefore commutative and associative, which is what makes
 * it safe to fold with {@link java.util.TreeMap#merge} regardless of the order staged files are
 * visited in.
 */
final class ChangeRecord {

  enum State {
    /** The key's final row must be present in the rewritten output. */
    PRESENT,
    /** The key must not appear in the rewritten output; it never existed before this change set. */
    INSERTED_THEN_DELETED,
    /** The key must not appear in the rewritten output. */
    ABSENT
  }

  private final int firstOp;
  private final OrderKey firstOrder;
  private final int lastOp;
  private final OrderKey lastOrder;
  private final Record lastRow;

  private ChangeRecord(
      int firstOp, OrderKey firstOrder, int lastOp, OrderKey lastOrder, Record lastRow) {
    this.firstOp = firstOp;
    this.firstOrder = firstOrder;
    this.lastOp = lastOp;
    this.lastOrder = lastOrder;
    this.lastRow = lastRow;
  }

  static ChangeRecord initial(int op, Record row, OrderKey order) {
    Preconditions.checkNotNull(order, "Order key cannot be null");
    return new ChangeRecord(op, order, op, order, row);
  }

  static ChangeRecord combine(ChangeRecord left, ChangeRecord right) {
    ChangeRecord earlier = left.firstOrder.compareTo(right.firstOrder) <= 0 ? left : right;
    ChangeRecord later = left.lastOrder.compareTo(right.lastOrder) >= 0 ? left : right;
    return new ChangeRecord(
        earlier.firstOp, earlier.firstOrder, later.lastOp, later.lastOrder, later.lastRow);
  }

  State state() {
    if (lastOp == StagedChangeSchema.OP_DELETE) {
      return firstOp == StagedChangeSchema.OP_INSERT ? State.INSERTED_THEN_DELETED : State.ABSENT;
    }
    return State.PRESENT;
  }

  /** The row to write, valid only when {@link #state()} is {@link State#PRESENT}. */
  Record row() {
    return lastRow;
  }
}
