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
 * Every scenario of {@link TestCopyOnWriteTableCommitter}, rewritten by three tasks instead of one.
 *
 * <p>The acceptance criterion for distributing the rewrite: {@code tasks.max > 1} must produce the
 * same table as a single task. Here that is not a similar test but literally the same assertions,
 * with the plan split across owners, and the slice's keys emitted by whichever owner holds them
 * rather than by whoever found the old row.
 */
public class TestCopyOnWriteTableCommitterMultiTask extends TestCopyOnWriteTableCommitter {

  @Override
  protected int taskCount() {
    return 3;
  }
}
