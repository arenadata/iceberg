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
package org.apache.iceberg.connect.v3;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.KafkaConnectUtils;
import org.apache.iceberg.connect.SchemaTestBase;

public abstract class IntegrationTestBaseV3 extends SchemaTestBase {
  protected static final String TEST_TABLE_V3 = "tb1";
  protected static final TableIdentifier TABLE_IDENTIFIER_V3 =
      TableIdentifier.of(TEST_DB, TEST_TABLE_V3);

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", TABLE_IDENTIFIER_V3.toString())
        .config("iceberg.tables.evolve-schema-enabled", "true")
        .config("tasks.max", "1");
  }

  @Override
  protected void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER_V3);
  }
}
