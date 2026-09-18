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

/**
 * A copy-on-write commit of one table refused until the operator undoes its cause.
 *
 * <p>Unlike {@code PermanentCopyOnWriteException} the table is not stopped: every cycle checks
 * again, and once the cause is undone the held responses apply as they are, with no restart. Like
 * it, the rejection is reported at ERROR once per table and reason rather than every commit
 * interval, and the table's responses stay buffered with the control topic offsets held.
 */
class TableCommitRejectedException extends RuntimeException {

  private final String reason;

  /**
   * @param reason what is wrong, the same every cycle until it is undone
   * @param detail where it was found and what to do about it
   */
  TableCommitRejectedException(String reason, String detail) {
    super(reason + ". " + detail);
    this.reason = reason;
  }

  String reason() {
    return reason;
  }
}
