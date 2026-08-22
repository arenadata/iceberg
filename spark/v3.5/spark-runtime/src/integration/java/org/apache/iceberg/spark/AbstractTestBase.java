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
import static org.apache.iceberg.spark.TestContext.TEST_CATALOG;
import static org.apache.iceberg.spark.TestContext.TEST_DB;
import static org.apache.iceberg.spark.TestContext.WAREHOUSE_LOCATION;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractTestBase {

  private static TestContext context;

  private SparkSession spark;

  private TableCatalog catalog;

  private FileSystem fs;

  protected abstract void dropTables();

  protected TestContext context() {
    return context;
  }

  protected TableCatalog catalog() {
    return catalog;
  }

  protected SparkSession spark() {
    return spark;
  }

  protected FileSystem fs() {
    return fs;
  }

  @BeforeAll
  public static void baseBeforeAll() {
    context = TestContext.instance();
  }

  @BeforeEach
  public void baseBefore() throws IOException {
    initSpark(TestContext.IcebergCatalogType.HIVE, Map.of());
    createNamespace();
  }

  @AfterEach
  public void baseAfter() throws IOException {
    dropTables();
    Arrays.stream(fs.listStatus(new Path(WAREHOUSE_LOCATION)))
      .forEach(
        fileStatus -> {
          try {
            fs.delete(fileStatus.getPath(), true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    clearNamespace();
    try {
      if (catalog instanceof AutoCloseable) {
        ((AutoCloseable) catalog).close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void clearNamespace() {
    spark().sql(format("DROP NAMESPACE IF EXISTS %s.%s", TEST_CATALOG, TEST_DB));
  }

  protected void createNamespace() {
    spark().sql(format("CREATE NAMESPACE IF NOT EXISTS %s.%s", TEST_CATALOG, TEST_DB));
  }

  protected void initSpark(
    TestContext.IcebergCatalogType catalogType, Map<String, String> customConfigs)
    throws IOException {
    if (this.spark != null) {
      this.spark.close();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    this.spark = context.initLocalSparkSession(catalogType, customConfigs);
    this.catalog = context.provideCatalog(context.provideCatalogPlugin(spark));
    this.fs = (new Path(WAREHOUSE_LOCATION)).getFileSystem(spark.sessionState().newHadoopConf());
  }
}
