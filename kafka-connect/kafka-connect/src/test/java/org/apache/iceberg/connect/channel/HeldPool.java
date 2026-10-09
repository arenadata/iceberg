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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * The transition pool: runs its jobs inline, unless holding them; the test then runs or starts
 * them, oldest first.
 */
final class HeldPool implements Executor {
  private final List<Runnable> jobs = Lists.newArrayList();
  private boolean holding = false;

  @Override
  public void execute(Runnable command) {
    synchronized (this) {
      if (holding) {
        jobs.add(command);
        return;
      }
    }

    command.run();
  }

  synchronized void hold() {
    holding = true;
  }

  synchronized void runInline() {
    holding = false;
  }

  synchronized int held() {
    return jobs.size();
  }

  synchronized Runnable take() {
    assertThat(jobs).as("a held job").isNotEmpty();
    return jobs.remove(0);
  }

  void runHeld() {
    while (held() > 0) {
      take().run();
    }
  }

  Thread startHeld() {
    Thread thread = new Thread(take(), "held-transition");
    thread.start();
    return thread;
  }
}
