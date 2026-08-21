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
package org.apache.iceberg.spark.service;

import java.net.URI;
import org.apache.spark.sql.connector.catalog.Table;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class IcebergTableClient {

  public static final int MINIO_PORT = 9000;
  public static final String AWS_ACCESS_KEY = "minioadmin";
  public static final String AWS_SECRET_KEY = "minioadmin";
  public static final String AWS_REGION = "us-east-1";

  private IcebergTableClient() {}

  public static String loadCatalogTableLocation(Table table) {
    return table.properties().get("location");
  }
}
