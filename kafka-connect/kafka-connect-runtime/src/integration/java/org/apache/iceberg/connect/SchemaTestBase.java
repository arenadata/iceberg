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
package org.apache.iceberg.connect;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.awaitility.Awaitility;

public abstract class SchemaTestBase extends AbstractTestBase {

  protected void runTest(
      boolean useSchema,
      Map<String, Object> extraConfig,
      List<TableIdentifier> tableIdentifiers,
      List<? extends BaseTestEvent> events) {
    KafkaConnectUtils.Config connectorConfig =
        initConnectorConfig(createConfig(useSchema), extraConfig);
    events.forEach(event -> send(testTopic(), event, useSchema));

    context().startConnector(connectorConfig);

    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(tableIdentifiers));
  }
}
