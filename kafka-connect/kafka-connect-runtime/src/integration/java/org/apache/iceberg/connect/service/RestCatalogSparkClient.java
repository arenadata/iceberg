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
import static org.apache.iceberg.connect.service.ConnectorService.MINIO_PORT;
import static org.apache.iceberg.connect.service.DockerClient.DOCKER_CLIENT;
import static org.apache.iceberg.connect.service.DockerClient.getContainer;

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.model.Frame;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RestCatalogSparkClient {
  private static final String SPARK_IMAGE = "apache/spark";

  private RestCatalogSparkClient() {}

  public static String runSparkSqlQuery(String query) throws InterruptedException {
    ExecCreateCmdResponse exec =
        DOCKER_CLIENT
            .execCreateCmd(getContainer(SPARK_IMAGE).getId())
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd(
                "/opt/spark/bin/spark-sql",
                "--conf",
                "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "--conf",
                "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog",
                "--conf",
                "spark.sql.catalog.spark_catalog.type=rest",
                "--conf",
                format("spark.sql.catalog.spark_catalog.uri=http://iceberg:%s", CATALOG_PORT),
                "--conf",
                format("spark.sql.catalog.spark_catalog.s3.endpoint=http://minio:%s", MINIO_PORT),
                "--conf",
                format("spark.sql.catalog.spark_catalog.s3.access-key-id=%s", AWS_ACCESS_KEY),
                "--conf",
                format("spark.sql.catalog.spark_catalog.s3.secret-access-key=%s", AWS_SECRET_KEY),
                "--conf",
                "spark.sql.catalog.spark_catalog.s3.path-style-access=true",
                "--conf",
                format("spark.sql.catalog.spark_catalog.s3.region=%s", AWS_REGION),
                "--conf",
                "spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
                "-e",
                query)
            .exec();

    ByteArrayOutputStream output = new ByteArrayOutputStream();

    DOCKER_CLIENT
        .execStartCmd(exec.getId())
        .exec(
            new ResultCallback.Adapter<Frame>() {
              @Override
              public void onNext(Frame frame) {
                try {
                  output.write(frame.getPayload());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .awaitCompletion();
    String consoleOutput = output.toString();
    InspectExecResponse state = DOCKER_CLIENT.inspectExecCmd(exec.getId()).exec();
    Integer exitCode = state.getExitCode();
    if (!exitCode.equals(null) && exitCode != 0) {
      throw new RuntimeException(
          format("Spark sql query %s failed with error: %s", query, consoleOutput));
    }
    return consoleOutput;
  }
}
