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
package org.apache.iceberg.spark;

import static java.lang.String.format;
import static java.util.Map.entry;
import static org.apache.iceberg.spark.IcebergCatalogProperties.BASE_CATALOG_CONFIGS;
import static org.apache.iceberg.spark.IcebergCatalogProperties.HIVE_METASTORE_PORT;
import static org.apache.iceberg.spark.IcebergCatalogProperties.TEST_DB;
import static org.apache.iceberg.spark.IcebergCatalogProperties.WAREHOUSE_LOCATION;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;

public enum IcebergCatalogType {
  HIVE(
      Stream.concat(
              BASE_CATALOG_CONFIGS.entrySet().stream(),
              Map.ofEntries(
                  entry("type", "hive"),
                  entry(CatalogProperties.URI, "thrift://localhost:" + HIVE_METASTORE_PORT),
                  entry("hive.metastore.uris", "thrift://localhost:" + HIVE_METASTORE_PORT),
                  entry("hive.metastore.schema.verification", "false"),
                  entry("hive.metastore.authorization.storage.checks", "false"),
                  entry("hive.metastore.client.capability.check", "false"),
                  entry("hive.metastore.skip.type.validation", "true"))
                  .entrySet()
                  .stream())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
      format("%s%s.db", WAREHOUSE_LOCATION, TEST_DB)),
  REST(
      Stream.concat(
              BASE_CATALOG_CONFIGS.entrySet().stream(),
              Map.of("type", "rest", "uri", "http://localhost:8181").entrySet().stream())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
      format("%s%s", WAREHOUSE_LOCATION, TEST_DB));

  private final Map<String, String> catalogTypeBaseConfigs;

  private final String namespaceDir;

  private final Path namespacePath;

  IcebergCatalogType(Map<String, String> catalogTypeBaseConfigs, String namespaceDir) {
    this.catalogTypeBaseConfigs = catalogTypeBaseConfigs;
    this.namespaceDir = namespaceDir;
    this.namespacePath = new Path(namespaceDir);
  }

  public Map<String, String> getCatalogTypeBaseConfigs() {
    return this.catalogTypeBaseConfigs;
  }

  public String getNamespaceDir() {
    return this.namespaceDir;
  }

  public Path getNamespacePath() {
    return this.namespacePath;
  }
}
