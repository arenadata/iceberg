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
package org.apache.iceberg.connect;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.metadata.ColumnLineage;
import org.apache.kafka.connect.metadata.EntityReference;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin facade over Kafka Connect {@link MetadataReporter} SPI. Centralises the null-check, the
 * catalog/database/table FQN derivation and the Iceberg-specific entity-type strings, so call sites
 * elsewhere in the connector can stay declarative.
 *
 * <p>An instance with a {@code null} reporter is the no-op variant: {@link #fromContext} returns
 * one whenever the runtime predates the Connect with {@link MetadataReporter} or the deployer
 * didn't configure {@code metadata.reporter.class}
 */
public class MetadataEvents {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataEvents.class);

  public static final MetadataEvents NOOP = new MetadataEvents(null, null, null, null, null);

  private final MetadataReporter reporter;
  private final String icebergServiceName;
  private final String icebergDatabaseName;
  private final String pipelineName;
  private final String kafkaServiceName;

  public static MetadataEvents fromContext(
      SinkTaskContext context,
      String icebergServiceName,
      String icebergDatabaseName,
      String pipelineName,
      String kafkaServiceName) {
    MetadataReporter reporter = null;
    try {
      Method metadataReporterMethod = context.getClass().getMethod("metadataReporter");
      Object raw = metadataReporterMethod.invoke(context);
      if (raw instanceof MetadataReporter) {
        reporter = (MetadataReporter) raw;
      }
    } catch (NoSuchMethodException | NoSuchMethodError | NoClassDefFoundError e) {
      LOG.debug("Connect runtime predates the MetadataReporter SPI; lineage disabled.", e);
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.debug("Failed to obtain MetadataReporter from context; lineage disabled.", e);
    }
    return new MetadataEvents(
        reporter, icebergServiceName, icebergDatabaseName, pipelineName, kafkaServiceName);
  }

  public MetadataEvents(
      MetadataReporter reporter,
      String icebergServiceName,
      String icebergDatabaseName,
      String pipelineName,
      String kafkaServiceName) {
    this.reporter = reporter;
    this.icebergServiceName = icebergServiceName;
    this.icebergDatabaseName = icebergDatabaseName;
    this.pipelineName = pipelineName;
    this.kafkaServiceName = kafkaServiceName;
  }

  public MetadataEvents(
      MetadataReporter reporter,
      String icebergServiceName,
      String pipelineName,
      String kafkaServiceName) {
    this(reporter, icebergServiceName, "default", pipelineName, kafkaServiceName);
  }

  public boolean enabled() {
    return reporter != null;
  }

  public void lineageCommit(
      Set<String> sourceTopics, TableIdentifier targetTable, Schema targetSchema) {
    if (reporter == null
        || sourceTopics == null
        || sourceTopics.isEmpty()
        || targetSchema == null) {
      return;
    }
    String targetFqn = fqn(targetTable);
    for (String topic : sourceTopics) {
      try {
        String sourceFqn = kafkaServiceName + "." + topic;
        reporter.report(
            new LineageEdge(
                new EntityReference(EntityReference.TYPE_KAFKA_TOPIC, sourceFqn),
                new EntityReference(EntityReference.TYPE_ICEBERG_TABLE, targetFqn),
                pipelineName,
                columnMappings(sourceFqn, targetFqn, targetSchema)));
      } catch (Throwable t) {
        LOG.warn("MetadataReporter threw on LineageEdge {} -> {}; ignoring", topic, targetTable, t);
      }
    }
  }

  private static List<ColumnLineage> columnMappings(
      String sourceFqn, String targetFqn, Schema targetSchema) {
    List<ColumnLineage> mappings = Lists.newArrayList();
    for (Types.NestedField column : targetSchema.columns()) {
      String columnName = column.name();
      mappings.add(
          new ColumnLineage(
              Collections.singletonList(sourceFqn + "." + columnName),
              targetFqn + "." + columnName));
    }
    return mappings;
  }

  private String fqn(TableIdentifier identifier) {
    String[] namespaceLevels = identifier.namespace().levels();
    String schema =
        namespaceLevels.length == 0 ? "default" : String.join(".", namespaceLevels);
    return String.join(
        ".", icebergServiceName, icebergDatabaseName, schema, identifier.name());
  }
}
