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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;

public class IcebergCatalogProperties {
  public static final String HIVE_METASTORE_PORT = "9083";
  public static final String WAREHOUSE_LOCATION = "s3a://warehouse/";
  public static final String TEST_DB = "test";
  public static final String TEST_CATALOG = "test_catalog";
  public static final String SPARK_CATALOG = "spark_catalog";
  public static final int MINIO_PORT = 9000;
  public static final String AWS_ACCESS_KEY = "minioadmin";
  public static final String AWS_SECRET_KEY = "minioadmin";
  public static final String AWS_REGION = "us-east-1";
  public static final String TEST_TABLE = "test_table";
  public static final String TEST_TABLE_NEW = "test_table_new";
  public static final String CATALOG_TEST_TABLE =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE);
  public static final String CATALOG_TEST_TABLE_NEW =
      format("%s.%s.%s", TEST_CATALOG, TEST_DB, TEST_TABLE_NEW);
  public static final List<String> RECORDS =
      List.of("1, 'Sam'", "2, 'Bob'", "3, 'Sue'", "4, 'Ann'");
  public static final List<String> ALT_RECORDS = List.of("1, 'Tom'", "2, 'Brian'");
  public static final Identifier TABLE_IDENTIFIER =
      Identifier.of(new String[] {TEST_DB}, TEST_TABLE);
  public static final Identifier TABLE_IDENTIFIER_NEW =
      Identifier.of(new String[] {TEST_DB}, TEST_TABLE_NEW);
  public static final Column[] BASE_COLUMN_SCHEMA =
      new Column[] {
        Column.create("id", DataTypes.IntegerType, false),
        Column.create("username", DataTypes.StringType, true)
      };

  public static final Map<String, String> BASE_CATALOG_CONFIGS =
      Map.ofEntries(
          entry("io.manifest.file-io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
          entry("s3.delete.enabled", "true"),
          entry("spark.hadoop.fs.s3.impl.disable.cache", "true"),
          entry("spark.hadoop.fs.s3a.impl.disable.cache", "true"),
          entry(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION),
          entry("hive.metastore.warehouse.dir", WAREHOUSE_LOCATION),
          entry("s3.endpoint", "http://localhost:" + MINIO_PORT),
          entry("s3.access-key-id", AWS_ACCESS_KEY),
          entry("s3.secret-access-key", AWS_SECRET_KEY),
          entry("s3.path-style-access", "true"),
          entry("s3.region", AWS_REGION),
          entry("cache-enabled", "false"),
          entry("s3.impl", "org.apache.iceberg.aws.s3.S3FileIO"),
          entry(
              "hadoop.fs.s3a.aws.credentials.provider",
              "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
          entry("client.factory", "org.apache.iceberg.aws.DefaultAwsClientFactory"));

  private IcebergCatalogProperties() {}
}
