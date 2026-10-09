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
package org.apache.iceberg.connect.service;

import static java.lang.String.format;
import static org.apache.iceberg.connect.service.ConnectorService.AWS_ACCESS_KEY;
import static org.apache.iceberg.connect.service.ConnectorService.AWS_REGION;
import static org.apache.iceberg.connect.service.ConnectorService.AWS_SECRET_KEY;
import static org.apache.iceberg.connect.service.ConnectorService.CATALOG_PORT;
import static org.apache.iceberg.connect.service.ConnectorService.S3_PORT;
import static org.apache.iceberg.connect.service.DockerClient.exec;
import static org.apache.iceberg.connect.service.DockerClient.stdin;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.iceberg.connect.service.DockerClient.ExecResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class RestCatalogSparkClient {
  private static final String SPARK_IMAGE = "apache/spark";

  private static final Pattern REPL_ERROR =
      Pattern.compile(
          "(?m)^(?:[ \\t]*error:"
              + "|<console>:\\d+: error:"
              + "|(?:scala> |[ \\t]+\\| )*[a-z][\\w$]*(?:\\.[\\w$]+)*\\.\\w*(?:Exception|Error)\\b"
              + "|[ \\t]+\\.\\.\\. \\d+ elided$)");

  private static final String[] SPARK_CONFS = {
    "--conf",
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf",
    "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog",
    "--conf",
    "spark.sql.catalog.spark_catalog.type=rest",
    "--conf",
    format("spark.sql.catalog.spark_catalog.uri=http://iceberg:%s", CATALOG_PORT),
    "--conf",
    format("spark.sql.catalog.spark_catalog.s3.endpoint=http://object-store:%s", S3_PORT),
    "--conf",
    format("spark.sql.catalog.spark_catalog.s3.access-key-id=%s", AWS_ACCESS_KEY),
    "--conf",
    format("spark.sql.catalog.spark_catalog.s3.secret-access-key=%s", AWS_SECRET_KEY),
    "--conf",
    "spark.sql.catalog.spark_catalog.s3.path-style-access=true",
    "--conf",
    format("spark.sql.catalog.spark_catalog.s3.region=%s", AWS_REGION),
    "--conf",
    "spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  };

  private RestCatalogSparkClient() {}

  public static String runSparkSqlQuery(String query) throws InterruptedException {
    ExecResult result = exec(SPARK_IMAGE, null, sparkCommand("spark-sql", "-e", query));
    if (result.failed()) {
      throw new RuntimeException(
          format("Spark sql query %s failed with error: %s", query, result.combined()));
    }
    return result.combined();
  }

  /**
   * Runs a Scala script in {@code spark-shell} with CBO enabled and returns its stdout. The script
   * is fed through stdin and gets {@code System.exit(0)} appended, otherwise the REPL waits for
   * more input. The REPL prints its own output too, so callers should mark the lines they need. The
   * REPL goes on after a compile error or an exception and still exits with 0, so the stdout is
   * checked for the REPL error markers and such a script fails with the whole output.
   */
  public static String runSparkShell(String script) throws InterruptedException {
    String fullScript = script.stripTrailing() + "\nSystem.exit(0)\n";
    ExecResult result =
        exec(
            SPARK_IMAGE,
            stdin(fullScript),
            sparkCommand("spark-shell", "--conf", "spark.sql.cbo.enabled=true"));
    if (result.failed()) {
      throw new RuntimeException(
          format(
              "Spark shell script %s failed with exit code %s and error: %s",
              script, result.exitCode(), result.combined()));
    }
    if (hasReplError(result.stdout())) {
      throw new RuntimeException(
          format("Spark shell script %s reported an error: %s", script, result.combined()));
    }
    return result.stdout();
  }

  static boolean hasReplError(String output) {
    return REPL_ERROR.matcher(output).find();
  }

  private static String[] sparkCommand(String binary, String... extraArgs) {
    List<String> cmd = Lists.newArrayList();
    cmd.add("/opt/spark/bin/" + binary);
    cmd.addAll(Arrays.asList(SPARK_CONFS));
    cmd.addAll(Arrays.asList(extraArgs));
    return cmd.toArray(new String[0]);
  }
}
