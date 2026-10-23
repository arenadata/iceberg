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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.connect.data.SketchValues;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Two bounds of the {@code ndv} of column c at snapshot S. Only not-null values count, as in a
 * sketch.
 *
 * <ul>
 *   <li><b>L</b>, lower: the distinct values of c in the live rows of S, deletes applied ({@code
 *       IcebergGenerics} with {@code useSnapshot(S)});
 *   <li><b>U</b>, upper: the distinct values of c in everything the test says it wrote after the
 *       base up to S (every version of every row, deleted ones too), the values of the base and the
 *       accepted overestimates of the scenario.
 * </ul>
 *
 * With {@code K = 2^lgK}: U &lt; K is the exact mode, {@code L <= ndv <= min(U, live(S))};
 * otherwise {@code (1 - 3/sqrt(K)) * L <= ndv <= min((1 + 3/sqrt(K)) * U, live(S))}. Values are
 * told apart by their bytes in the sketch ({@link SketchValues#toByteBuffer}), so a {@code String}
 * and a {@code Utf8}, or a {@code UUID} and its 16 bytes, are one value.
 */
public final class NdvBounds {
  private static final String NDV_PROPERTY = "ndv";

  private final Table table;
  private final long snapshotId;
  private final Types.NestedField column;
  private final Set<ByteBuffer> upperValues = Sets.newHashSet();

  private NdvBounds(Table table, long snapshotId, Types.NestedField column) {
    this.table = table;
    this.snapshotId = snapshotId;
    this.column = column;
  }

  /** The bounds of the column, by name in the schema of S, at snapshot S. */
  public static NdvBounds of(Table table, long snapshotId, String columnName) {
    Types.NestedField field = SnapshotUtil.schemaFor(table, snapshotId).findField(columnName);
    Preconditions.checkArgument(
        field != null, "No column %s at snapshot %s", columnName, snapshotId);
    return new NdvBounds(table, snapshotId, field);
  }

  public NdvBounds written(Object... values) {
    return written(Arrays.asList(values));
  }

  /** Values of c the test wrote after the base up to S: every version, deleted ones too. */
  public NdvBounds written(Iterable<?> values) {
    values.forEach(this::addUpper);
    return this;
  }

  /**
   * Values of c in the base: nothing for an empty base, all the test wrote before it for a base of
   * the connector, the live values of the base snapshot for an ANALYZE.
   */
  public NdvBounds base(Iterable<?> values) {
    return written(values);
  }

  /**
   * Accepted overestimates of the scenario: the {@code initial-default}, the values of a shared
   * Puffin cut by a partial commit.
   */
  public NdvBounds overestimate(Object... values) {
    return written(values);
  }

  public long lower() {
    Set<ByteBuffer> values = Sets.newHashSet();
    try (CloseableIterable<Record> rows =
        IcebergGenerics.read(table).useSnapshot(snapshotId).select(column.name()).build()) {
      for (Record row : rows) {
        ByteBuffer bytes = bytes(row.getField(column.name()));
        if (bytes != null) {
          values.add(bytes);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return values.size();
  }

  public long upper() {
    return upperValues.size();
  }

  /**
   * {@code live(S)}: total records, from v3 less total position deletes, or no bound without them.
   */
  public long live() {
    Snapshot snapshot = table.snapshot(snapshotId);
    Map<String, String> summary = snapshot.summary();
    String records = summary.get(SnapshotSummary.TOTAL_RECORDS_PROP);
    if (records == null) {
      return Long.MAX_VALUE;
    }

    if (TableUtil.formatVersion(table) < 3) {
      return Long.parseLong(records);
    }

    String positionDeletes = summary.get(SnapshotSummary.TOTAL_POS_DELETES_PROP);
    if (positionDeletes == null) {
      return Long.MAX_VALUE;
    }

    return Long.parseLong(records) - Long.parseLong(positionDeletes);
  }

  public void check(long ndv, int lgK) {
    check(ndv, lower(), upper(), live(), lgK);
  }

  public void checkRecorded(int lgK) {
    check(recordedNdv(), lgK);
  }

  /** The {@code ndv} property of the blob of c in the NDV record of S. */
  public long recordedNdv() {
    for (StatisticsFile file : table.statisticsFiles()) {
      if (file.snapshotId() != snapshotId) {
        continue;
      }

      for (BlobMetadata blob : file.blobMetadata()) {
        if (StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())
            && blob.fields().equals(List.of(column.fieldId()))) {
          return Long.parseLong(blob.properties().get(NDV_PROPERTY));
        }
      }
    }

    throw new AssertionError(
        String.format(
            Locale.ROOT, "No NDV blob of column %s at snapshot %d", column.name(), snapshotId));
  }

  public static void check(long ndv, long lower, long upper, long live, int lgK) {
    if (lower > upper) {
      throw new AssertionError(
          String.format(
              Locale.ROOT,
              "Inconsistent bounds: L = %d distinct live values, but U = %d written",
              lower,
              upper));
    }

    long nominalEntries = 1L << lgK;
    double min;
    double max;
    String mode;
    if (upper < nominalEntries) {
      mode = "exact";
      min = lower;
      max = Math.min(upper, live);
    } else {
      mode = "approximate";
      double error = 3.0 / Math.sqrt(nominalEntries);
      min = (1.0 - error) * lower;
      max = Math.min((1.0 + error) * upper, live);
    }

    if (ndv < min || ndv > max) {
      throw new AssertionError(
          String.format(
              Locale.ROOT,
              "NDV %d is out of the %s bounds [%.1f, %.1f]: L = %d, U = %d, live = %d, K = %d",
              ndv,
              mode,
              min,
              max,
              lower,
              upper,
              live,
              nominalEntries));
    }
  }

  private void addUpper(Object value) {
    ByteBuffer bytes = bytes(value);
    if (bytes != null) {
      upperValues.add(bytes);
    }
  }

  /** The bytes of the value as the sketch sees them, a copy; {@code null} for {@code null}. */
  private ByteBuffer bytes(Object value) {
    Type.PrimitiveType type = column.type().asPrimitiveType();
    ByteBuffer bytes = SketchValues.toByteBuffer(type, value);
    if (bytes == null) {
      return null;
    }

    byte[] copy = new byte[bytes.remaining()];
    bytes.duplicate().get(copy);
    return ByteBuffer.wrap(copy);
  }
}
