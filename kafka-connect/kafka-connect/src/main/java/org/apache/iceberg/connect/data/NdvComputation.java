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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.NdvReadPlan.Recount;
import org.apache.iceberg.connect.data.NdvResult.ColumnWithoutBaseline;
import org.apache.iceberg.connect.data.NdvResult.Kind;
import org.apache.iceberg.connect.data.NdvResult.StaleColumn;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The NDV of snapshot S: the base and the gap ({@link NdvChain}), the initial sketches ({@link
 * NdvBaseline}), the coverage by the intermediate Puffin files ({@link IntermediateSketchSet}), the
 * limit and the recount ({@link NdvReadPlan}), then the union of it all into one blob per column,
 * bounded by the live rows of S, with the share of sketched rows that are no longer live.
 */
public class NdvComputation {
  private static final Logger LOG = LoggerFactory.getLogger(NdvComputation.class);

  /** Blob property: the NDV estimate, as Spark and Trino write it. */
  public static final String NDV_PROPERTY = "ndv";

  /** Blob property: the snapshot the sketch of the column was last built in full at. */
  public static final String RECOMPUTE_SNAPSHOT_ID_PROPERTY = "kafka.connect.recompute-snapshot-id";

  /** Blob property: the rows passed through the sketch since the last full recompute. */
  public static final String SKETCHED_ROWS_PROPERTY = "kafka.connect.sketched-rows";

  static final String CREATED_BY = "iceberg-kafka-connect " + IcebergBuild.version();

  private NdvComputation() {}

  /**
   * Calculates the NDV blobs of snapshot S; reads files but writes nothing. Does not throw: a
   * failure is {@link Kind#ERROR}, or {@link Kind#STOPPED} once {@code stopped} is up or the thread
   * is interrupted.
   */
  public static NdvResult compute(
      TableIdentifier identifier,
      Table table,
      long snapshotId,
      Set<Long> ownSnapshotIds,
      Set<String> ownChangeSetIds,
      List<StatisticsAnswer> answers,
      IcebergSinkConfig config,
      TableSinkConfig tableConfig,
      BooleanSupplier stopped) {
    try {
      return new Calculation(identifier, table, snapshotId, config, tableConfig, stopped)
          .run(ownSnapshotIds, ownChangeSetIds, answers);
    } catch (RuntimeException e) {
      Kind kind = isStopped(stopped) ? Kind.STOPPED : Kind.ERROR;
      return NdvResult.builder(kind, snapshotId).failure(e).build();
    }
  }

  /**
   * Writes the statistics file of S with the blobs, in their order, at {@link #fileLocation}; an
   * existing file makes it throw.
   *
   * @throws IllegalArgumentException if there are no blobs
   * @throws UncheckedIOException if the file cannot be written
   */
  public static StatisticsFile writeFile(
      Table table, long snapshotId, UUID commitId, List<Blob> blobs) {
    return writeFile(table, snapshotId, commitId, blobs, () -> {});
  }

  /**
   * As {@link #writeFile(Table, long, UUID, List)}; {@code onCreated} runs right after {@code
   * create()} of the file succeeds, and never if it throws.
   */
  static StatisticsFile writeFile(
      Table table, long snapshotId, UUID commitId, List<Blob> blobs, Runnable onCreated) {
    Preconditions.checkArgument(
        !blobs.isEmpty(),
        "Cannot write a statistics file of snapshot %s without blobs",
        snapshotId);
    String path = fileLocation(table, snapshotId, commitId);
    try (PuffinWriter writer =
        Puffin.write(new NotifyingOutputFile(table.io().newOutputFile(path), onCreated))
            .createdBy(CREATED_BY)
            .build()) {
      blobs.forEach(writer::add);
      writer.finish();
      return new GenericStatisticsFile(
          snapshotId,
          path,
          writer.fileSize(),
          writer.footerSize(),
          GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** An output file that reports a successful {@code create()}. */
  private static final class NotifyingOutputFile implements OutputFile {
    private final OutputFile delegate;
    private final Runnable onCreated;

    private NotifyingOutputFile(OutputFile delegate, Runnable onCreated) {
      this.delegate = delegate;
      this.onCreated = onCreated;
    }

    @Override
    public PositionOutputStream create() {
      PositionOutputStream stream = delegate.create();
      onCreated.run();
      return stream;
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      PositionOutputStream stream = delegate.createOrOverwrite();
      onCreated.run();
      return stream;
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public InputFile toInputFile() {
      return delegate.toInputFile();
    }
  }

  public static String fileLocation(Table table, long snapshotId, UUID commitId) {
    Preconditions.checkArgument(
        table instanceof HasTableOperations, "Cannot locate the metadata of table %s", table);
    return ((HasTableOperations) table)
        .operations()
        .metadataFileLocation(
            String.format(Locale.ROOT, "stats-%d-%s.puffin", snapshotId, commitId));
  }

  /**
   * {@code live} of the snapshot of the table, or {@code null} when its summary lacks a total it
   * needs. Before v3 it is {@code total-records} only: removing a data file leaves its position
   * deletes, so {@code total-position-deletes} may count rows that are gone. From v3 it is {@code
   * total-records - total-position-deletes}: a deletion vector goes with its data file.
   */
  static Long live(Table table, Snapshot snapshot) {
    Map<String, String> summary = snapshot == null ? null : snapshot.summary();
    if (summary == null) {
      return null;
    }

    Long records = parseLong(summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
    if (records == null || TableUtil.formatVersion(table) < 3) {
      return records;
    }

    Long positionDeletes = parseLong(summary.get(SnapshotSummary.TOTAL_POS_DELETES_PROP));
    if (positionDeletes == null) {
      return null;
    }

    return Math.max(0L, records - positionDeletes);
  }

  /** The share of the sketched rows that are no longer live. */
  static double stale(long live, long sketchedRows) {
    if (sketchedRows <= 0L) {
      return 0.0;
    }

    return Math.max(0.0, 1.0 - (double) live / sketchedRows);
  }

  /** The theta sketch blob of the column in the record, {@code null} if there is none. */
  static BlobMetadata blobOf(StatisticsFile record, int fieldId) {
    for (BlobMetadata blob : record.blobMetadata()) {
      Integer blobFieldId = NdvBaseline.thetaFieldId(blob.type(), blob.fields());
      if (blobFieldId != null && blobFieldId == fieldId) {
        return blob;
      }
    }

    return null;
  }

  private static Long parseLong(String value) {
    if (value == null) {
      return null;
    }

    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean isStopped(BooleanSupplier stopped) {
    return stopped.getAsBoolean() || Thread.currentThread().isInterrupted();
  }

  /** Where the sketch of a column was last built in full, and the rows it has seen since. */
  static class Recompute {
    private final long snapshotId;
    private final long sketchedRows;

    Recompute(long snapshotId, long sketchedRows) {
      this.snapshotId = snapshotId;
      this.sketchedRows = sketchedRows;
    }

    /** From the properties of a blob of the connector, {@code null} without both of them. */
    static Recompute fromProperties(Map<String, String> properties) {
      if (properties == null) {
        return null;
      }

      Long snapshotId = parseLong(properties.get(RECOMPUTE_SNAPSHOT_ID_PROPERTY));
      Long sketchedRows = parseLong(properties.get(SKETCHED_ROWS_PROPERTY));
      if (snapshotId == null || sketchedRows == null || sketchedRows < 0L) {
        return null;
      }

      return new Recompute(snapshotId, sketchedRows);
    }

    long snapshotId() {
      return snapshotId;
    }

    long sketchedRows() {
      return sketchedRows;
    }
  }

  private static class Calculation {
    private final TableIdentifier identifier;
    private final Table table;
    private final long snapshotId;
    private final Snapshot snapshot;
    private final Schema schemaS;
    private final IcebergSinkConfig config;
    private final TableSinkConfig tableConfig;
    private final int lgK;
    private final BooleanSupplier stopped;

    private Calculation(
        TableIdentifier identifier,
        Table table,
        long snapshotId,
        IcebergSinkConfig config,
        TableSinkConfig tableConfig,
        BooleanSupplier stopped) {
      this.identifier = identifier;
      this.table = table;
      this.snapshotId = snapshotId;
      this.snapshot = table.snapshot(snapshotId);
      Preconditions.checkArgument(
          snapshot != null, "Cannot find snapshot %s of table %s", snapshotId, table.name());
      this.schemaS = SnapshotUtil.schemaFor(table, snapshotId);
      this.config = config;
      this.tableConfig = tableConfig;
      this.lgK = config.statisticsLgNominalEntries();
      this.stopped = stopped;
    }

    private NdvResult run(
        Set<Long> ownSnapshotIds, Set<String> ownChangeSetIds, List<StatisticsAnswer> answers) {
      List<Types.NestedField> columns =
          StatisticsColumns.resolve(
              identifier, schemaS, tableConfig.statisticsColumns(), config.schemaCaseInsensitive());
      if (columns.isEmpty()) {
        LOG.debug(
            "No NDV of table {} at snapshot {}: the set of columns is empty",
            table.name(),
            snapshotId);
        return result(Kind.NOT_NEEDED).build();
      }

      NdvChain chain =
          NdvChain.resolve(table, snapshotId, ownSnapshotIds, ownChangeSetIds, answers);
      if (chain.kind() == NdvChain.Kind.NO_BASELINE) {
        return result(Kind.MISSING_BASELINE)
            .cause("the chain of snapshots breaks before statistics to build NDV on")
            .build();
      }

      List<ColumnWithoutBaseline> withoutBaseline = Lists.newArrayList();
      if (chain.kind() == NdvChain.Kind.BASE) {
        NdvBaseline.columnsWithoutBaseline(chain.base(), chain.baseSchema(), schemaS, columns)
            .forEach(
                (fieldId, reason) ->
                    withoutBaseline.add(
                        new ColumnWithoutBaseline(schemaS.findField(fieldId), reason)));
      }

      NdvBaseline base =
          NdvBaseline.load(table, chain.base(), chain.baseSchema(), schemaS, columns, stopped);
      NdvResult notLoaded = notLoaded(chain, base, withoutBaseline);
      if (notLoaded != null) {
        return notLoaded;
      }

      // the columns that start from a sketch: not without a baseline, no broken base blob
      List<Types.NestedField> candidates = Lists.newArrayList();
      for (Types.NestedField column : columns) {
        if (base.sketches().containsKey(column.fieldId())) {
          candidates.add(column);
        }
      }

      if (candidates.isEmpty()) {
        return result(Kind.NO_BLOBS).withoutBaseline(withoutBaseline).build();
      }

      return sketch(chain, base, candidates, withoutBaseline);
    }

    /** The result when the initial sketches are not loaded, {@code null} when they are. */
    private NdvResult notLoaded(
        NdvChain chain, NdvBaseline base, List<ColumnWithoutBaseline> withoutBaseline) {
      switch (base.kind()) {
        case STOPPED:
          return result(Kind.STOPPED)
              .withoutBaseline(withoutBaseline)
              .failure(base.failure())
              .build();
        case FAILED:
          return result(Kind.ERROR)
              .withoutBaseline(withoutBaseline)
              .failure(base.failure())
              .build();
        case NO_BASELINE:
          return result(Kind.MISSING_BASELINE)
              .withoutBaseline(withoutBaseline)
              .cause("base statistics file " + chain.base().path() + " cannot be read")
              .failure(base.failure())
              .build();
        default:
          return null;
      }
    }

    private NdvResult sketch(
        NdvChain chain,
        NdvBaseline base,
        List<Types.NestedField> candidates,
        List<ColumnWithoutBaseline> withoutBaseline) {
      IntermediateSketchSet intermediate =
          IntermediateSketchSet.read(table, chain.answers(), schemaS, candidates, lgK, stopped);
      if (intermediate.stopped()) {
        return result(Kind.STOPPED).withoutBaseline(withoutBaseline).build();
      } else if (intermediate.failure() != null) {
        return result(Kind.ERROR)
            .withoutBaseline(withoutBaseline)
            .failure(intermediate.failure())
            .build();
      }

      NdvReadPlan plan =
          NdvReadPlan.build(
              table,
              chain.gap(),
              chain.answers(),
              intermediate,
              candidates,
              config.statisticsGapMaxFiles(),
              config.statisticsGapMaxBytes(),
              stopped);
      if (plan.kind() == NdvReadPlan.Kind.OVER_LIMIT) {
        return result(Kind.OVER_LIMIT)
            .withoutBaseline(withoutBaseline)
            .gap(plan.gapFiles(), plan.gapBytes())
            .build();
      } else if (plan.accepted().isEmpty()) {
        return result(Kind.NO_BLOBS)
            .withoutBaseline(withoutBaseline)
            .rejected(plan.rejected())
            .build();
      }

      Recount recount = plan.read(table, schemaS, lgK, stopped);
      switch (recount.status()) {
        case STOPPED:
          return result(Kind.STOPPED)
              .withoutBaseline(withoutBaseline)
              .rejected(plan.rejected())
              .failure(recount.failure())
              .read(recount.filesRead(), recount.bytesRead())
              .build();
        case FAILED:
          return result(Kind.ERROR)
              .withoutBaseline(withoutBaseline)
              .rejected(plan.rejected())
              .failure(recount.failure())
              .read(recount.filesRead(), recount.bytesRead())
              .build();
        default:
          break;
      }

      List<Blob> blobs = Lists.newArrayList();
      List<StaleColumn> staleColumns = Lists.newArrayList();
      Long live = live(table, snapshot);
      for (Types.NestedField column : plan.accepted()) {
        int fieldId = column.fieldId();
        CompactSketch sketch =
            union(
                column,
                base.sketches().get(fieldId),
                intermediate.sketch(fieldId),
                recount.sketches().get(fieldId),
                readsInitialDefault(chain, intermediate, column));
        Recompute origin = origin(chain, fieldId);
        long sketchedRows =
            origin.sketchedRows() + intermediate.coveredRows(fieldId) + recount.rowsRead(fieldId);
        blobs.add(blob(fieldId, sketch, live, origin.snapshotId(), sketchedRows));

        double ratio = live == null ? 0.0 : stale(live, sketchedRows);
        if (ratio > config.statisticsStaleDeletedRatio()) {
          staleColumns.add(new StaleColumn(column, ratio, origin.snapshotId()));
        }
      }

      if (live == null) {
        LOG.debug(
            "No stale ratio of table {} at snapshot {}: its summary has no total records",
            table.name(),
            snapshotId);
      }

      return result(Kind.BLOBS)
          .blobs(blobs)
          .withoutBaseline(withoutBaseline)
          .rejected(plan.rejected())
          .staleColumns(staleColumns)
          .read(recount.filesRead(), recount.bytesRead())
          .build();
    }

    private CompactSketch union(
        Types.NestedField column,
        Sketch initial,
        CompactSketch covered,
        UpdateSketch recounted,
        boolean withInitialDefault) {
      Union union = SetOperation.builder().setNominalEntries(1 << lgK).buildUnion();
      union.union(initial);
      if (covered != null) {
        union.union(covered);
      }

      if (recounted != null) {
        union.union(recounted);
      }

      if (withInitialDefault) {
        UpdateSketch defaults = SketchValues.newSketch(lgK);
        SketchValues.update(
            defaults,
            SketchValues.toByteBuffer(column.type().asPrimitiveType(), column.initialDefault()));
        union.union(defaults);
      }

      return union.getResult();
    }

    /**
     * Whether rows of S that are not read here read the initial default of the column: rows the
     * base had before the column was added, or rows of a writer that did not know it.
     */
    private boolean readsInitialDefault(
        NdvChain chain, IntermediateSketchSet intermediate, Types.NestedField column) {
      if (column.initialDefault() == null) {
        return false;
      } else if (intermediate.hasRowsWithout(column.fieldId())) {
        return true;
      } else if (chain.kind() != NdvChain.Kind.BASE
          || chain.baseSchema().findField(column.fieldId()) != null) {
        return false;
      }

      Long liveBase = live(table, table.snapshot(chain.base().snapshotId()));
      return liveBase == null || liveBase > 0L;
    }

    /** Where the sketch of the column was last built in full. */
    private Recompute origin(NdvChain chain, int fieldId) {
      if (chain.kind() != NdvChain.Kind.BASE) {
        return new Recompute(snapshotId, 0L); // an empty base: the sketch starts at S
      }

      StatisticsFile record = chain.base();
      if (chain.baseSchema().findField(fieldId) != null) {
        BlobMetadata blob = blobOf(record, fieldId);
        Recompute recompute = blob == null ? null : Recompute.fromProperties(blob.properties());
        if (recompute != null) {
          return recompute;
        }
      }

      // a blob of Trino or Spark, or a new column: recomputed at the base over its live rows
      Long liveBase = live(table, table.snapshot(record.snapshotId()));
      return new Recompute(record.snapshotId(), liveBase == null ? 0L : liveBase);
    }

    private Blob blob(
        int fieldId, CompactSketch sketch, Long live, long recomputeSnapshotId, long sketchedRows) {
      long estimate = (long) sketch.getEstimate();
      long ndv = live == null ? estimate : Math.min(estimate, live);
      return new Blob(
          StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
          ImmutableList.of(fieldId),
          snapshotId,
          snapshot.sequenceNumber(),
          ByteBuffer.wrap(sketch.toByteArray()),
          PuffinCompressionCodec.ZSTD,
          ImmutableMap.of(
              NDV_PROPERTY,
              Long.toString(ndv),
              RECOMPUTE_SNAPSHOT_ID_PROPERTY,
              Long.toString(recomputeSnapshotId),
              SKETCHED_ROWS_PROPERTY,
              Long.toString(sketchedRows)));
    }

    private NdvResult.Builder result(Kind kind) {
      return NdvResult.builder(kind, snapshotId);
    }
  }
}
