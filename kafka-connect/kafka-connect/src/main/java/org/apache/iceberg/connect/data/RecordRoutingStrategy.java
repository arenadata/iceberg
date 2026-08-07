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
package org.apache.iceberg.connect.data;

public enum RecordRoutingStrategy {
  DYNAMIC_FIELD("dynamic-field"),
  ALL_TABLES("all-tables"),
  REGEX("regex"),
  TOPIC_TO_TABLE("topic-to-table");

  private final String value;

  RecordRoutingStrategy(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static RecordRoutingStrategy fromConfig(String routingStrategy) {
    if (routingStrategy == null || routingStrategy.isBlank()) {
      return null;
    }

    for (RecordRoutingStrategy strategy : values()) {
      if (strategy.value().equals(routingStrategy)) {
        return strategy;
      }
    }

    throw new IllegalArgumentException(
        "Unsupported record routing strategy: "
            + routingStrategy
            + ". Supported values are: "
            + supportedValues());
  }

  private static String supportedValues() {
    StringBuilder supported = new StringBuilder();
    for (RecordRoutingStrategy strategy : values()) {
      if (supported.length() > 0) {
        supported.append(", ");
      }
      supported.append(strategy.value());
    }

    return supported.toString();
  }
}
