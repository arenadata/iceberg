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
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.metadata.EntityReference;
import org.apache.kafka.connect.metadata.LineageEdge;
import org.apache.kafka.connect.metadata.MetadataReporter;
import org.apache.kafka.connect.metadata.SchemaEvolved;
import org.apache.kafka.connect.metadata.TableCreated;
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
 * didn't configure {@code meatadata.reporter.class}
 */
public class MetadataEvents {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataEvents.class);

  public static final MetadataEvents NOOP = new MetadataEvents(null, null, null);

  private final MetadataReporter reporter;
  private final String catalogName;
  private final String pipelineName;

  public static MetadataEvents fromContext(
      SinkTaskContext context, String catalogName, String pipelineName) {
    MetadataReporter reporter = null;
    try {
      Method m = context.getClass().getMethod("metadataReporter");
      Object raw = m.invoke(context);
      if (raw instanceof MetadataReporter) {
        reporter = (MetadataReporter) raw;
      }
    } catch (NoSuchMethodException | NoSuchMethodError | NoClassDefFoundError e) {
      LOG.debug("Connect runtime predates the MetadataReporter SPI; lineage disabled.", e);
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.debug("Failed to obtain MetadataReporter from context; lineage disabled.", e);
    }
    return new MetadataEvents(reporter, catalogName, pipelineName);
  }

  public MetadataEvents(MetadataReporter reporter, String catalogName, String pipelineName) {
    this.reporter = reporter;
    this.catalogName = catalogName;
    this.pipelineName = pipelineName;
  }

  public boolean enabled() {
    return reporter != null;
  }

  public void tableCreated(TableIdentifier identifier, Schema schema) {
    if (reporter == null || schema == null) {
      return;
    }
    try {
      String[] parts = splitNamespace(identifier);
      reporter.report(new TableCreated(catalogName, parts[0], parts[1], schema));
    } catch (Throwable t) {
      LOG.warn("MetadataReporter threw on TableCreated for {}; ignoring.", identifier, t);
    }
  }

  public void schemaEvolved(TableIdentifier identifier, Schema oldSchema, Schema newSchema) {
    if (reporter == null || newSchema == null) {
      return;
    }
    try {
      reporter.report(new SchemaEvolved(fqn(identifier), oldSchema, newSchema));
    } catch (Throwable t) {
      LOG.warn("MetadataReporter threw on SchemaEvolved for {}; ignoring", identifier, t);
    }
  }

  public void lineageCommit(Set<String> sourceTopics, TableIdentifier targetTable) {
    if (reporter == null || sourceTopics == null || sourceTopics.isEmpty()) {
      return;
    }
    EntityReference target =
        new EntityReference(EntityReference.TYPE_ICEBERG_TABLE, fqn(targetTable));
    for (String topic : sourceTopics) {
      try {
        reporter.report(
            new LineageEdge(
                new EntityReference(EntityReference.TYPE_KAFKA_TOPIC, topic),
                target,
                pipelineName));
      } catch (Throwable t) {
        LOG.warn("MetadataReporter threw on LineageEdge {} -> {}; ignoring", topic, targetTable, t);
      }
    }
  }

  private String fqn(TableIdentifier identifier) {
    String[] parts = splitNamespace(identifier);
    String table = parts[0] + "." + parts[1];
    return (catalogName == null || catalogName.isEmpty()) ? table : catalogName + "." + table;
  }

  // OM stores iceberg tables at <service>.<database>.<databaseSchema>.<table>, but
  // the MetadataReporter SPI's TableCreated event has only three slots
  // (catalogName, databaseName, tableName). We pack the OM database + databaseSchema
  // layers into databaseName so the OM reporter's parent-FQN lookup
  // (<catalogName>.<databaseName>) lands on the right databaseSchema.
  // OM's iceberg ingestor uses "default" as the database name and the iceberg
  // namespace as the databaseSchema, falling back to "default" when iceberg has
  // no explicit namespace.
  private static String[] splitNamespace(TableIdentifier identifier) {
    String[] levels = identifier.namespace().levels();
    String schema = levels.length == 0 ? "default" : String.join(".", levels);
    return new String[] {"default." + schema, identifier.name()};
  }
}
