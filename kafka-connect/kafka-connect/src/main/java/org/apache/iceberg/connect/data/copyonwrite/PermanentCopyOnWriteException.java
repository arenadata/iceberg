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

/**
 * A copy-on-write failure that the next cycle will hit again, identically, forever.
 *
 * <p>Transient failures (a conflicting writer, an object store hiccup, a producer that lost its
 * broker) keep the change set and retry on the next commit cycle. A plan spanning partition specs
 * cannot resolve itself that way: retried, it prints the same warning every cycle while the
 * response buffer grows and the control-topic offsets stay pinned, until the heap runs out. So the
 * coordinator reports this type once, at ERROR, with the remedy in the message, and stops the
 * table's drain; the operator fixes what the message names (compaction, configuration) and restarts
 * the connector.
 *
 * <p><b>Only for failures with a named remedy.</b> If a retry could ever succeed without anyone
 * doing anything, the failure is transient and belongs in the ordinary path.
 */
public class PermanentCopyOnWriteException extends RuntimeException {

  public PermanentCopyOnWriteException(String message) {
    super(message);
  }
}
