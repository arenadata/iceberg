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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Maps the CDC operation field of a source record onto an {@link Operation}.
 *
 * <p>Shared by the merge-on-read and copy-on-write writers: the two must read the same record as
 * the same operation, and a divergence here would be silent.
 */
class CdcOperations {

  private final IcebergSinkConfig config;
  private final Map<String, Operation> operationMappings;
  private final Set<String> ignoredOperations;

  CdcOperations(IcebergSinkConfig config) {
    this.config = config;
    this.operationMappings = Maps.newHashMap();
    this.ignoredOperations = Sets.newHashSet();

    insertOperationMappings(config.tablesCdcOpsInsert(), Operation.INSERT);
    insertOperationMappings(config.tablesCdcOpsUpdate(), Operation.UPDATE);
    insertOperationMappings(config.tablesCdcOpsDelete(), Operation.DELETE);

    normalize(config.tablesCdcIgnoredOps()).forEach(ignoredOperations::add);
  }

  Optional<String> rawOperation(SinkRecord record) {
    return Optional.ofNullable(config.tablesCdcField())
        .map(operationField -> RecordUtils.extractFromRecordValue(record.value(), operationField))
        .map(Object::toString)
        .map(String::trim)
        .map(String::toLowerCase);
  }

  boolean isIgnored(String rawOperation) {
    return ignoredOperations.contains(rawOperation);
  }

  Operation operation(String rawOperation) {
    return operationMappings.get(rawOperation);
  }

  private void insertOperationMappings(List<String> cdcOperations, Operation operation) {
    normalize(cdcOperations).forEach(cdcOp -> operationMappings.put(cdcOp, operation));
  }

  private static Stream<String> normalize(List<String> operations) {
    if (operations == null || operations.isEmpty()) {
      return Stream.empty();
    }

    return operations.stream().map(String::toLowerCase);
  }
}
