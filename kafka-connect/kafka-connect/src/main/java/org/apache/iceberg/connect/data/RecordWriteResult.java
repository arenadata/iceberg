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
package org.apache.iceberg.connect.data;

import java.util.Set;
import org.apache.iceberg.connect.events.TableReference;

/**
 * What a {@link RecordWriter} produced for one commit cycle.
 *
 * <p>Merge-on-read produces data and delete files, ready to be committed. Copy-on-write produces
 * staged change files, which are neither, and cannot be committed until the coordinator has planned
 * the rewrite. The discriminator keeps the two apart at the seam where the worker turns a result
 * into a control event.
 */
public interface RecordWriteResult {

  enum Kind {
    DATA_FILES,
    STAGED_CHANGES
  }

  Kind kind();

  TableReference tableReference();

  Set<String> sourceTopics();
}
