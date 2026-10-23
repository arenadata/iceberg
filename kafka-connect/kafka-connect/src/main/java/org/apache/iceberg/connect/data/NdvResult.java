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
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.data.NdvBaseline.Reason;
import org.apache.iceberg.connect.data.NdvReadPlan.RejectedColumn;
import org.apache.iceberg.connect.data.StatisticsHealth.Flag;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

/** What the NDV calculation of snapshot S came to, and the health transition it makes. */
public class NdvResult {
  public enum Kind {
    /** At least one blob in {@link #blobs()}. */
    BLOBS,
    /** The set of columns is empty: no NDV. */
    NOT_NEEDED,
    /**
     * The set is not empty, but no column got a blob: each is without a baseline, rejected by the
     * limit or has a base blob that does not deserialize.
     */
    NO_BLOBS,
    /** The files of the gap alone exceed the limit, checked before any column: nothing is read. */
    OVER_LIMIT,
    /**
     * No statistics to build on: the chain broke before a base, or the base file cannot be read for
     * good.
     */
    MISSING_BASELINE,
    /** A failure a retry may fix: {@link #failure()}. */
    ERROR,
    /** The step was stopped. */
    STOPPED
  }

  private final Kind kind;
  private final long snapshotId;
  private final List<Blob> blobs;
  private final List<ColumnWithoutBaseline> withoutBaseline;
  private final List<RejectedColumn> rejected;
  private final List<StaleColumn> staleColumns;
  private final long gapFiles;
  private final long gapBytes;
  private final long filesRead;
  private final long bytesRead;
  private final String cause;
  private final Throwable failure;

  private NdvResult(Builder builder) {
    this.kind = builder.kind;
    this.snapshotId = builder.snapshotId;
    this.blobs = ImmutableList.copyOf(builder.blobs);
    this.withoutBaseline = ImmutableList.copyOf(builder.withoutBaseline);
    this.rejected = ImmutableList.copyOf(builder.rejected);
    this.staleColumns = ImmutableList.copyOf(builder.staleColumns);
    this.gapFiles = builder.gapFiles;
    this.gapBytes = builder.gapBytes;
    this.filesRead = builder.filesRead;
    this.bytesRead = builder.bytesRead;
    this.cause = builder.cause;
    this.failure = builder.failure;
  }

  static Builder builder(Kind kind, long snapshotId) {
    return new Builder(kind, snapshotId);
  }

  public Kind kind() {
    return kind;
  }

  public long snapshotId() {
    return snapshotId;
  }

  /** One per column, in the order of the set. */
  public List<Blob> blobs() {
    return blobs;
  }

  public List<ColumnWithoutBaseline> withoutBaseline() {
    return withoutBaseline;
  }

  public List<RejectedColumn> rejected() {
    return rejected;
  }

  public List<StaleColumn> staleColumns() {
    return staleColumns;
  }

  /** For {@link Kind#OVER_LIMIT}, a lower bound of the data files of the gap. */
  public long gapFiles() {
    return gapFiles;
  }

  public long gapBytes() {
    return gapBytes;
  }

  /** The files read to recount the values that no sketch covered. */
  public long filesRead() {
    return filesRead;
  }

  public long bytesRead() {
    return bytesRead;
  }

  /** What made the calculation fail or stop, {@code null} if nothing threw. */
  public Throwable failure() {
    return failure;
  }

  /** The health transition; for {@link Kind#BLOBS}, it holds once the statistics are committed. */
  public HealthTransition transition() {
    switch (kind) {
      case BLOBS:
        return HealthTransition.of(
            !rejected.isEmpty(), !withoutBaseline.isEmpty(), !staleColumns.isEmpty(), details());
      case NO_BLOBS:
        return HealthTransition.of(
            !rejected.isEmpty(), !withoutBaseline.isEmpty(), null, details());
      case OVER_LIMIT:
        return HealthTransition.of(true, !withoutBaseline.isEmpty(), null, details());
      case MISSING_BASELINE:
        return HealthTransition.of(false, true, null, details());
      case NOT_NEEDED:
        return HealthTransition.CLEARED;
      default:
        return HealthTransition.UNCHANGED;
    }
  }

  private Map<Flag, String> details() {
    Map<Flag, String> details = Maps.newEnumMap(Flag.class);
    if (kind == Kind.OVER_LIMIT) {
      details.put(
          Flag.GAP_OVER_LIMIT,
          String.format(
              Locale.ROOT,
              "the gap of the NDV is over the limit: at least %d data files, %d bytes",
              gapFiles,
              gapBytes));
    } else if (!rejected.isEmpty()) {
      details.put(
          Flag.GAP_OVER_LIMIT,
          "columns whose files do not fit within the gap limit: "
              + rejected.stream()
                  .map(
                      column ->
                          String.format(
                              Locale.ROOT,
                              "%s (%d files, %d bytes)",
                              describe(column.column()),
                              column.files(),
                              column.bytes()))
                  .collect(Collectors.joining(", ")));
    }

    List<String> missing = Lists.newArrayList();
    if (cause != null) {
      missing.add(cause);
    }

    if (!withoutBaseline.isEmpty()) {
      missing.add(HealthTransition.describeWithoutBaseline(withoutBaseline));
    }

    if (!missing.isEmpty()) {
      details.put(Flag.MISSING_BASELINE, String.join("; ", missing));
    }

    if (!staleColumns.isEmpty()) {
      details.put(Flag.STALE, HealthTransition.describeStale(staleColumns));
    }

    return details;
  }

  static String describe(Types.NestedField column) {
    return column.name() + " (field id " + column.fieldId() + ")";
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("kind", kind)
        .add("snapshotId", snapshotId)
        .add("blobs", blobs.size())
        .add("withoutBaseline", withoutBaseline)
        .add("rejected", rejected)
        .add("staleColumns", staleColumns)
        .add("gapFiles", gapFiles)
        .add("gapBytes", gapBytes)
        .add("filesRead", filesRead)
        .add("bytesRead", bytesRead)
        .add("cause", cause)
        .add("failure", failure)
        .toString();
  }

  /** A column of the set that has no baseline in the base: no blob, or its type changed. */
  public static class ColumnWithoutBaseline {
    private final Types.NestedField column;
    private final Reason reason;

    ColumnWithoutBaseline(Types.NestedField column, Reason reason) {
      this.column = column;
      this.reason = reason;
    }

    public Types.NestedField column() {
      return column;
    }

    public Reason reason() {
      return reason;
    }

    @Override
    public String toString() {
      return describe(column) + ": " + reason;
    }
  }

  /** A column whose share of sketched rows that are no longer live is over the threshold. */
  public static class StaleColumn {
    private final Types.NestedField column;
    private final double ratio;
    private final long recomputeSnapshotId;

    StaleColumn(Types.NestedField column, double ratio, long recomputeSnapshotId) {
      this.column = column;
      this.ratio = ratio;
      this.recomputeSnapshotId = recomputeSnapshotId;
    }

    public Types.NestedField column() {
      return column;
    }

    public double ratio() {
      return ratio;
    }

    /** The snapshot the sketch of the column was last built in full at. */
    public long recomputeSnapshotId() {
      return recomputeSnapshotId;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "%s: %.2f of the sketched rows are no longer live since recompute snapshot %d",
          describe(column),
          ratio,
          recomputeSnapshotId);
    }
  }

  static class Builder {
    private final Kind kind;
    private final long snapshotId;
    private List<Blob> blobs = ImmutableList.of();
    private List<ColumnWithoutBaseline> withoutBaseline = ImmutableList.of();
    private List<RejectedColumn> rejected = ImmutableList.of();
    private List<StaleColumn> staleColumns = ImmutableList.of();
    private long gapFiles = 0L;
    private long gapBytes = 0L;
    private long filesRead = 0L;
    private long bytesRead = 0L;
    private String cause = null;
    private Throwable failure = null;

    private Builder(Kind kind, long snapshotId) {
      this.kind = kind;
      this.snapshotId = snapshotId;
    }

    Builder blobs(List<Blob> newBlobs) {
      this.blobs = newBlobs;
      return this;
    }

    Builder withoutBaseline(List<ColumnWithoutBaseline> columns) {
      this.withoutBaseline = columns;
      return this;
    }

    Builder rejected(List<RejectedColumn> columns) {
      this.rejected = columns;
      return this;
    }

    Builder staleColumns(List<StaleColumn> columns) {
      this.staleColumns = columns;
      return this;
    }

    Builder gap(long files, long bytes) {
      this.gapFiles = files;
      this.gapBytes = bytes;
      return this;
    }

    Builder read(long files, long bytes) {
      this.filesRead = files;
      this.bytesRead = bytes;
      return this;
    }

    /** Why there is no baseline, for {@link Kind#MISSING_BASELINE}. */
    Builder cause(String text) {
      this.cause = text;
      return this;
    }

    Builder failure(Throwable throwable) {
      this.failure = throwable;
      return this;
    }

    NdvResult build() {
      return new NdvResult(this);
    }
  }
}
