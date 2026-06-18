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

public abstract class IntegrationTestBase extends AbstractTestBase {

  abstract void sendEvents(boolean useSchema);

  protected void runTest(
      String branch,
      boolean useSchema,
      Map<String, String> extraConfig,
      List<TableIdentifier> tableIdentifiers) {
    KafkaConnectUtils.Config connectorConfig = createConfig(useSchema);

    context().connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }

    extraConfig.forEach(connectorConfig::config);

    context().startConnector(connectorConfig);

    sendEvents(useSchema);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(tableIdentifiers));
  }
}
