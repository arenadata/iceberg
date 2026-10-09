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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.NdvResult.ColumnWithoutBaseline;
import org.apache.iceberg.connect.data.NdvResult.StaleColumn;
import org.apache.iceberg.connect.data.StatisticsHealth.Flag;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transition of the health flags of a table: each flag is raised, put down or left as it is, with
 * the details of the raised flags.
 */
public final class HealthTransition implements UnaryOperator<Flags> {
  private static final Logger LOG = LoggerFactory.getLogger(HealthTransition.class);

  /** Leaves every flag as it is. */
  public static final HealthTransition UNCHANGED =
      new HealthTransition(null, null, null, ImmutableMap.of());

  /** Puts every flag down. */
  public static final HealthTransition CLEARED =
      new HealthTransition(false, false, false, ImmutableMap.of());

  // null: the flag stays as it is
  private final Boolean gapOverLimit;
  private final Boolean missingBaseline;
  private final Boolean stale;
  private final Map<Flag, String> details;

  private HealthTransition(
      Boolean gapOverLimit, Boolean missingBaseline, Boolean stale, Map<Flag, String> details) {
    this.gapOverLimit = gapOverLimit;
    this.missingBaseline = missingBaseline;
    this.stale = stale;
    this.details = details;
  }

  /** Sets each flag given and leaves each {@code null} one as it is. */
  static HealthTransition of(
      Boolean gapOverLimit, Boolean missingBaseline, Boolean stale, Map<Flag, String> details) {
    return new HealthTransition(gapOverLimit, missingBaseline, stale, ImmutableMap.copyOf(details));
  }

  @Override
  public Flags apply(Flags flags) {
    Flags current = flags == null ? Flags.NONE : flags;
    return new Flags(
        gapOverLimit != null ? gapOverLimit : current.gapOverLimit(),
        missingBaseline != null ? missingBaseline : current.missingBaseline(),
        stale != null ? stale : current.stale());
  }

  /** The text of each flag this transition may raise. */
  public Map<Flag, String> details() {
    return details;
  }

  /**
   * Evaluates the health flags of the table by the metadata of the head of the branch, with the
   * column set, the baseline rule and the stale threshold of the step. Without an NDV record that
   * describes the head the flags stay: only a step tells the gap from a missing baseline.
   *
   * @param branch the branch of the commits, {@code null} for main
   */
  public static HealthTransition fromHead(
      TableIdentifier identifier,
      Table table,
      String branch,
      IcebergSinkConfig config,
      TableSinkConfig tableConfig) {
    Snapshot head = SnapshotUtil.latestSnapshot(table, branch);
    if (head == null) {
      return CLEARED;
    }

    Schema schema = SnapshotUtil.schemaFor(table, head.snapshotId());
    List<Types.NestedField> columns =
        StatisticsColumns.resolve(
            identifier, schema, tableConfig.statisticsColumns(), config.schemaCaseInsensitive());
    if (columns.isEmpty()) {
      return CLEARED;
    }

    StatisticsFile record = recordOf(table, head.snapshotId());
    if (record == null || !NdvChain.describes(table, record, head)) {
      return UNCHANGED;
    }

    List<ColumnWithoutBaseline> withoutBaseline = Lists.newArrayList();
    NdvBaseline.columnsWithoutBaseline(record, schema, schema, columns)
        .forEach(
            (fieldId, reason) ->
                withoutBaseline.add(new ColumnWithoutBaseline(schema.findField(fieldId), reason)));
    List<StaleColumn> staleColumns =
        staleColumns(
            identifier, table, record, columns, head, config.statisticsStaleDeletedRatio());

    Map<Flag, String> details = Maps.newEnumMap(Flag.class);
    if (!withoutBaseline.isEmpty()) {
      details.put(Flag.MISSING_BASELINE, describeWithoutBaseline(withoutBaseline));
    }

    if (!staleColumns.isEmpty()) {
      details.put(Flag.STALE, describeStale(staleColumns));
    }

    return of(false, !withoutBaseline.isEmpty(), !staleColumns.isEmpty(), details);
  }

  private static StatisticsFile recordOf(Table table, long snapshotId) {
    for (StatisticsFile file : table.statisticsFiles()) {
      if (file.snapshotId() == snapshotId) {
        return file;
      }
    }

    return null;
  }

  /** The blobs of the record over the threshold; a blob without the connector's properties is 0. */
  private static List<StaleColumn> staleColumns(
      TableIdentifier identifier,
      Table table,
      StatisticsFile record,
      List<Types.NestedField> columns,
      Snapshot head,
      double threshold) {
    Long live = NdvComputation.live(table, head);
    if (live == null) {
      LOG.debug(
          "No stale ratio of table {} at snapshot {}: its summary has no total records",
          identifier,
          head.snapshotId());
      return Lists.newArrayList();
    }

    List<StaleColumn> result = Lists.newArrayList();
    for (Types.NestedField column : columns) {
      BlobMetadata blob = NdvComputation.blobOf(record, column.fieldId());
      NdvComputation.Recompute recompute =
          blob == null ? null : NdvComputation.Recompute.fromProperties(blob.properties());
      if (recompute == null) {
        continue;
      }

      double ratio = NdvComputation.stale(live, recompute.sketchedRows());
      if (ratio > threshold) {
        result.add(new StaleColumn(column, ratio, recompute.snapshotId()));
      }
    }

    return result;
  }

  static String describeWithoutBaseline(List<ColumnWithoutBaseline> columns) {
    return "columns without a baseline to build NDV on: "
        + columns.stream().map(ColumnWithoutBaseline::toString).collect(Collectors.joining(", "));
  }

  static String describeStale(List<StaleColumn> columns) {
    return "stale NDV columns: "
        + columns.stream().map(StaleColumn::toString).collect(Collectors.joining(", "));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("gapOverLimit", gapOverLimit)
        .add("missingBaseline", missingBaseline)
        .add("stale", stale)
        .add("details", details)
        .toString();
  }
}
