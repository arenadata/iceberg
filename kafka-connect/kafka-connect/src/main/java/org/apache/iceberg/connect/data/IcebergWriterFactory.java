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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.copyonwrite.StagedChangeSchema;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergWriterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriterFactory.class);

  private static final List<String> SERVICE_COLUMNS =
      ImmutableList.of(
          StagedChangeSchema.OP,
          StagedChangeSchema.TOPIC,
          StagedChangeSchema.PARTITION,
          StagedChangeSchema.OFFSET);

  private final Catalog catalog;
  private final IcebergSinkConfig config;

  IcebergWriterFactory(Catalog catalog, IcebergSinkConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  RecordWriter createWriter(String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    TableIdentifier identifier = TableIdentifier.parse(tableName);
    Table table;
    try {
      table = catalog.loadTable(identifier);
    } catch (NoSuchTableException nst) {
      if (config.autoCreateEnabled()) {
        table = autoCreateTable(tableName, sample);
      } else if (ignoreMissingTable) {
        return new NoOpWriter();
      } else {
        throw nst;
      }
    }

    UUID tableUuid = table.uuid();
    if (tableUuid == null) {
      LOG.warn(
          "Table {} does not have a UUID, this may cause issues with commit coordination on table replace",
          identifier);
    }
    TableReference tableReference = TableReference.of(catalog.name(), identifier, tableUuid);

    checkRowLevelModeTableProps(table, identifier, config);

    if (config.isCopyOnWriteMode()) {
      Set<Integer> identifierFieldIds =
          checkCopyOnWritePrerequisites(table, tableReference, config);

      // IcebergSinkConfig refuses copy-on-write without a change stream; not relied on, since a
      // plain append writer here would acknowledge records that never land
      String cdcField = config.tablesCdcField();
      Preconditions.checkState(
          (cdcField != null && !cdcField.isEmpty()) || config.isUpsertMode(),
          "Copy-on-write table %s has neither iceberg.tables.cdc-field nor "
              + "iceberg.tables.upsert-mode-enabled=true; the connector config should have "
              + "refused it",
          identifier);
      return new CopyOnWriteStagingWriter(table, tableReference, config, identifierFieldIds);
    }

    return new IcebergWriter(table, tableReference, config);
  }

  /**
   * Reports a table whose declared write mode disagrees with the connector's: a warning in
   * merge-on-read, which ignores the property, fatal in copy-on-write, since a reader of that table
   * has been told to expect the other mode.
   */
  @VisibleForTesting
  static void checkRowLevelModeTableProps(
      Table table, TableIdentifier identifier, IcebergSinkConfig config) {
    checkRowLevelModeTableProps(table, identifier, config, LOG);
  }

  /** Takes the logger so that a test can see the merge-on-read warning. */
  @VisibleForTesting
  static void checkRowLevelModeTableProps(
      Table table, TableIdentifier identifier, IcebergSinkConfig config, Logger log) {
    String configuredMode = config.rowLevelMode().modeName();
    for (String prop : IcebergSinkConfig.ROW_LEVEL_MODE_TABLE_PROPS) {
      // the core defaults are read-time only and are never written to the
      // metadata, so getOrDefault would report a mismatch on every table
      String declared = table.properties().get(prop);
      if (declared == null || declared.equalsIgnoreCase(configuredMode)) {
        continue;
      }

      String message =
          String.format(
              "Table %s declares %s=%s, but connector is configured with "
                  + "iceberg.tables.row-level-mode=%s.",
              identifier, prop, declared, configuredMode);
      if (config.isCopyOnWriteMode()) {
        throw new ConfigException(
            message
                + " Align the table property or route this table to a connector with a matching mode.");
      }
      log.warn(
          "{} The connector keeps writing in merge-on-read; the table property only affects SQL engines.",
          message);
    }
  }

  /**
   * Fails a copy-on-write table that cannot be written correctly, before the first record is
   * staged, and returns the identifier fields to stage it by.
   *
   * <p><b>A task id must be set.</b> {@code task.id} may be absent, and merge-on-read only logs it.
   * Copy-on-write puts it in the staged file path and keys the rewrite executors by it: a worker
   * finds its assignment, and so its block of the slice's keys, by task id. A missing one fails
   * nowhere later; its keys are silently dropped.
   *
   * <p><b>The identifier fields must pass {@link IdentifierFields#resolveForCopyOnWrite}.</b> The
   * coordinator holds the table to the same rules when it starts a drain.
   *
   * <p><b>No top-level column may be named like a service column.</b> The staged file schema adds
   * {@code _op}, {@code _topic}, {@code _partition} and {@code _offset} beside the table's columns;
   * left to the writer, such a table fails schema validation on its first record and on every
   * restart. A nested field, such as {@code kafka._offset}, does not clash.
   */
  @VisibleForTesting
  static Set<Integer> checkCopyOnWritePrerequisites(
      Table table, TableReference tableReference, IcebergSinkConfig config) {
    if (config.taskId() == null || config.taskId().isEmpty()) {
      throw new ConfigException(
          "Copy-on-write requires a task id, but task.id is not set; table "
              + tableReference.identifier());
    }

    Set<Integer> identifierFieldIds =
        IdentifierFields.resolveForCopyOnWrite(table, tableReference, config);

    List<String> clashing =
        table.schema().columns().stream()
            .map(NestedField::name)
            .filter(SERVICE_COLUMNS::contains)
            .collect(Collectors.toList());
    if (!clashing.isEmpty()) {
      throw new ConfigException(
          String.format(
              "Table %s has columns %s named like the service columns %s that copy-on-write adds to "
                  + "its staged change files; rename them in the table and in whatever writes them, "
                  + "such as an SMT, or route this table to a merge-on-read connector",
              tableReference.identifier(), clashing, SERVICE_COLUMNS));
    }

    return identifierFieldIds;
  }

  @VisibleForTesting
  Table autoCreateTable(String tableName, SinkRecord sample) {
    StructType structType;
    if (sample.valueSchema() == null) {
      Type type = SchemaUtils.inferIcebergType(sample.value(), config);
      if (type == null) {
        throw new DataException("Unable to create table from empty object");
      }
      structType = type.asStructType();
    } else {
      structType = SchemaUtils.toIcebergType(sample.valueSchema(), config).asStructType();
    }

    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(structType.fields());
    TableIdentifier identifier = TableIdentifier.parse(tableName);

    createNamespaceIfNotExist(catalog, identifier.namespace());

    List<String> partitionBy = config.tableConfig(tableName).partitionBy();
    PartitionSpec spec;
    try {
      spec = SchemaUtils.createPartitionSpec(schema, partitionBy);
    } catch (Exception e) {
      LOG.error(
          "Unable to create partition spec {}, table {} will be unpartitioned",
          partitionBy,
          identifier,
          e);
      spec = PartitionSpec.unpartitioned();
    }

    PartitionSpec partitionSpec = spec;
    AtomicReference<Table> result = new AtomicReference<>();
    Tasks.range(1)
        .retry(IcebergSinkConfig.CREATE_TABLE_RETRIES)
        .run(
            notUsed -> {
              try {
                result.set(
                    catalog.createTable(
                        identifier, schema, partitionSpec, config.autoCreateProps()));
              } catch (AlreadyExistsException e) {
                result.set(catalog.loadTable(identifier));
              }
            });
    return result.get();
  }

  @VisibleForTesting
  static void createNamespaceIfNotExist(Catalog catalog, Namespace identifierNamespace) {
    if (!(catalog instanceof SupportsNamespaces)) {
      return;
    }

    String[] levels = identifierNamespace.levels();
    for (int index = 0; index < levels.length; index++) {
      Namespace namespace = Namespace.of(Arrays.copyOfRange(levels, 0, index + 1));
      try {
        ((SupportsNamespaces) catalog).createNamespace(namespace);
      } catch (AlreadyExistsException | ForbiddenException ex) {
        // Ignoring the error as forcefully creating the namespace even if it exists
        // to avoid double namespaceExists() check.
      }
    }
  }
}
