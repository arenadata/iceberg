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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectorService {

  private ConnectorService() {}

  public static final int CATALOG_PORT = 8181;
  public static final int MINIO_PORT = 9000;
  public static final String AWS_ACCESS_KEY = "minioadmin";
  public static final String AWS_SECRET_KEY = "minioadmin";
  public static final String AWS_REGION = "us-east-1";

  public static Map<String, Object> addConnectorConfigs(
      Map<String, Object> baseConfigs, Map<String, Object> additionalConfigs) {
    return Stream.concat(baseConfigs.entrySet().stream(), additionalConfigs.entrySet().stream())
        .collect(
            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2, HashMap::new));
  }
}
