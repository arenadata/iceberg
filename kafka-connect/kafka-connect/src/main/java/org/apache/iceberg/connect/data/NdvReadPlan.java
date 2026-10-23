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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.data.IntermediateSketchSet.Coverage;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * The files the statistics step reads to recount the NDV of its snapshot S, within the limit: the
 * files of the gap first, by all columns of the set, then the files of the uncovered answers,
 * column by column in the order of the set.
 *
 * <p>The whole list is made before anything is read. A file needed by several columns counts once
 * and is read once, projected onto the columns that need it.
 */
public class NdvReadPlan {
  /** Rows of a file read between two checks whether the step is stopped. */
  public static final int STOP_CHECK_ROWS = 10_000;

  // the fields of a manifest entry the plan and the readers need: no column statistics
  private static final List<String> LISTED_COLUMNS =
      ImmutableList.of(
          "file_path",
          "file_format",
          "file_size_in_bytes",
          "record_count",
          "partition",
          "key_metadata",
          "split_offsets",
          "sort_order_id");

  public enum Kind {
    /** The gap and the accepted columns fit within the limit: {@link #files()} are to be read. */
    PLANNED,
    /** The files of the gap alone exceed the limit: S gets no NDV and nothing is read. */
    OVER_LIMIT
  }

  private final Kind kind;
  private final long gapFiles;
  private final long gapBytes;
  private final List<Types.NestedField> accepted;
  private final List<RejectedColumn> rejected;
  private final List<PlannedFile> files;

  private NdvReadPlan(
      Kind kind,
      long gapFiles,
      long gapBytes,
      List<Types.NestedField> accepted,
      List<RejectedColumn> rejected,
      List<PlannedFile> files) {
    this.kind = kind;
    this.gapFiles = gapFiles;
    this.gapBytes = gapBytes;
    this.accepted = accepted;
    this.rejected = rejected;
    this.files = files;
  }

  /**
   * Makes the list of files to read; {@link Kind#OVER_LIMIT} once the gap alone does not fit. A
   * column whose files not yet in the list do not fit is rejected, and the next one is tried.
   *
   * @param gap the snapshots of the gap ({@link NdvChain#gap()})
   * @param columns the columns to recount, in the order of the set
   * @throws RuntimeException if the manifests of the gap cannot be read
   */
  public static NdvReadPlan build(
      Table table,
      List<Snapshot> gap,
      List<StatisticsAnswer> answers,
      IntermediateSketchSet coverage,
      List<Types.NestedField> columns,
      long maxFiles,
      long maxBytes) {
    return build(table, gap, answers, coverage, columns, maxFiles, maxBytes, () -> false);
  }

  /**
   * As above; the listing of the gap gives up once {@code stopped} is up or the thread is
   * interrupted, since a snapshot with no added data file counts nothing toward the limit.
   *
   * @throws RuntimeException if the manifests of the gap cannot be read, or the step is stopped
   */
  public static NdvReadPlan build(
      Table table,
      List<Snapshot> gap,
      List<StatisticsAnswer> answers,
      IntermediateSketchSet coverage,
      List<Types.NestedField> columns,
      long maxFiles,
      long maxBytes,
      BooleanSupplier stopped) {
    Map<String, Entry> entries = Maps.newLinkedHashMap();
    ReadLimit limit = new ReadLimit(maxFiles, maxBytes);
    if (!listGap(table, gap, entries, limit, stopped)) {
      return new NdvReadPlan(
          Kind.OVER_LIMIT,
          limit.files,
          limit.bytes,
          ImmutableList.of(),
          ImmutableList.of(),
          ImmutableList.of());
    }

    long gapFiles = limit.files;
    long gapBytes = limit.bytes;
    List<Types.NestedField> accepted = Lists.newArrayList();
    List<RejectedColumn> rejected = Lists.newArrayList();
    for (Types.NestedField column : columns) {
      Map<String, Entry> needed = uncoveredFiles(answers, coverage, column.fieldId());
      long extraFiles = 0L;
      long extraBytes = 0L;
      for (Map.Entry<String, Entry> file : needed.entrySet()) {
        if (!entries.containsKey(file.getKey())) {
          extraFiles += 1;
          extraBytes += file.getValue().sizeBytes;
        }
      }

      if (!limit.fits(extraFiles, extraBytes)) {
        rejected.add(new RejectedColumn(column, extraFiles, extraBytes));
        continue;
      }

      limit.add(extraFiles, extraBytes);
      accepted.add(column);
      for (Map.Entry<String, Entry> file : needed.entrySet()) {
        entries.putIfAbsent(file.getKey(), file.getValue());
        entries.get(file.getKey()).fieldIds.add(column.fieldId());
      }
    }

    return new NdvReadPlan(
        Kind.PLANNED,
        gapFiles,
        gapBytes,
        ImmutableList.copyOf(accepted),
        ImmutableList.copyOf(rejected),
        plannedFiles(entries, accepted));
  }

  /**
   * Lists the added data files of the gap into {@code entries}, first counting them by the manifest
   * lists; {@code false} at the first count or file past the limit, without reading further.
   */
  private static boolean listGap(
      Table table,
      List<Snapshot> gap,
      Map<String, Entry> entries,
      ReadLimit limit,
      BooleanSupplier stopped) {
    FileIO io = table.io();
    long counted = 0L;
    for (Snapshot snapshot : gap) {
      checkListing(stopped);
      for (ManifestFile manifest : addedManifests(snapshot, io)) {
        Integer added = manifest.addedFilesCount();
        counted += added != null ? added : 0;
      }

      if (counted > limit.maxFiles) {
        // nothing is listed: the numbers of a gap over the limit are the count alone
        limit.add(counted, 0L);
        return false;
      }
    }

    for (Snapshot snapshot : gap) {
      checkListing(stopped);
      if (!listAdded(table, snapshot, entries, limit)) {
        return false;
      }
    }

    return true;
  }

  private static void checkListing(BooleanSupplier stopped) {
    if (isStopped(stopped)) {
      throw new IllegalStateException("The listing of the gap is stopped");
    }
  }

  private static List<ManifestFile> addedManifests(Snapshot snapshot, FileIO io) {
    List<ManifestFile> manifests = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      if (Objects.equals(manifest.snapshotId(), snapshot.snapshotId())) {
        manifests.add(manifest);
      }
    }

    return manifests;
  }

  /**
   * Lists the data files the snapshot added, without column statistics. A merged manifest also
   * holds files of earlier snapshots; the added ones carry the sequence number of the manifest. A
   * format 1 table has none, so a snapshot with a merged manifest there is listed by {@link
   * Snapshot#addedDataFiles(FileIO)}.
   */
  private static boolean listAdded(
      Table table, Snapshot snapshot, Map<String, Entry> entries, ReadLimit limit) {
    List<ManifestFile> manifests = addedManifests(snapshot, table.io());
    for (ManifestFile manifest : manifests) {
      if (manifest.hasExistingFiles() && manifest.sequenceNumber() <= 0) {
        return list(snapshot.addedDataFiles(table.io()), entries, limit);
      }
    }

    for (ManifestFile manifest : manifests) {
      try (CloseableIterable<DataFile> files = addedFiles(table, manifest)) {
        if (!list(files, entries, limit)) {
          return false;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return true;
  }

  private static CloseableIterable<DataFile> addedFiles(Table table, ManifestFile manifest) {
    CloseableIterable<DataFile> live =
        ManifestFiles.read(manifest, table.io(), table.specs()).select(LISTED_COLUMNS);
    if (!manifest.hasExistingFiles()) {
      return live;
    }

    long sequenceNumber = manifest.sequenceNumber();
    return CloseableIterable.filter(
        live, file -> Objects.equals(file.fileSequenceNumber(), sequenceNumber));
  }

  /**
   * Adds the files not yet listed to {@code entries}; {@code false} at the first file over the
   * limit.
   */
  private static boolean list(
      Iterable<DataFile> files, Map<String, Entry> entries, ReadLimit limit) {
    for (DataFile file : files) {
      if (entries.containsKey(file.location())) {
        continue;
      }

      // counted even past the limit: the numbers of a gap over the limit include this file
      limit.add(1L, file.fileSizeInBytes());
      if (limit.exceeded()) {
        return false;
      }

      entries.put(file.location(), new Entry(file, null, file.fileSizeInBytes(), true));
    }

    return true;
  }

  private static Map<String, Entry> uncoveredFiles(
      List<StatisticsAnswer> answers, IntermediateSketchSet coverage, int fieldId) {
    Map<String, Entry> needed = Maps.newLinkedHashMap();
    for (StatisticsAnswer answer : answers) {
      if (coverage.coverage(answer, fieldId) != Coverage.UNCOVERED) {
        continue;
      }

      for (DataFile file : answer.dataFiles()) {
        needed.putIfAbsent(file.location(), new Entry(file, null, file.fileSizeInBytes(), false));
      }

      for (StagedChangeFile file : answer.stagedFiles()) {
        needed.putIfAbsent(file.location(), new Entry(null, file, file.fileSizeBytes(), false));
      }
    }

    return needed;
  }

  private static List<PlannedFile> plannedFiles(
      Map<String, Entry> entries, List<Types.NestedField> accepted) {
    Set<Integer> acceptedIds =
        ImmutableSet.copyOf(Lists.transform(accepted, Types.NestedField::fieldId));
    List<PlannedFile> files = Lists.newArrayList();
    for (Entry entry : entries.values()) {
      Set<Integer> fieldIds = entry.allColumns ? acceptedIds : ImmutableSet.copyOf(entry.fieldIds);
      if (!fieldIds.isEmpty()) {
        files.add(new PlannedFile(entry.dataFile, entry.stagedFile, entry.sizeBytes, fieldIds));
      }
    }

    return ImmutableList.copyOf(files);
  }

  public Kind kind() {
    return kind;
  }

  /** For {@link Kind#OVER_LIMIT}, the files counted up to the first one past the limit. */
  public long gapFiles() {
    return gapFiles;
  }

  /** Zero when the count by the manifest lists alone exceeds the limit. */
  public long gapBytes() {
    return gapBytes;
  }

  /** The columns whose files fit within the limit, in the order of the set. */
  public List<Types.NestedField> accepted() {
    return accepted;
  }

  public List<RejectedColumn> rejected() {
    return rejected;
  }

  public List<PlannedFile> files() {
    return files;
  }

  /**
   * Reads the files of the plan into one sketch per accepted column, hashing values by their type
   * in {@code schemaS} and skipping {@code null}. Stops once {@code stopped} is up or the thread is
   * interrupted, checked before each file and every {@link #STOP_CHECK_ROWS} rows.
   *
   * @param lgK log2 of the nominal entries of each sketch
   */
  public Recount read(Table table, Schema schemaS, int lgK, BooleanSupplier stopped) {
    return read(schemaS, lgK, stopped, (file, projection) -> open(table, file, projection));
  }

  /**
   * Opens a file of the plan. A staged file gives all its rows, deletes too, with {@code _op} after
   * the columns of the projection.
   */
  interface FileOpener {
    CloseableIterable<Record> open(PlannedFile file, Schema projection);
  }

  Recount read(Schema schemaS, int lgK, BooleanSupplier stopped, FileOpener opener) {
    Map<Integer, UpdateSketch> sketches = Maps.newLinkedHashMap();
    Map<Integer, Long> rowsRead = Maps.newLinkedHashMap();
    for (Types.NestedField column : accepted) {
      sketches.put(column.fieldId(), SketchValues.newSketch(lgK));
      rowsRead.put(column.fieldId(), 0L);
    }

    long filesRead = 0L;
    long bytesRead = 0L;
    for (PlannedFile file : files) {
      if (isStopped(stopped)) {
        return Recount.stopped(filesRead, bytesRead);
      }

      Schema projection = projection(schemaS, file.fieldIds());
      List<Types.NestedField> columns = projection.columns();
      int width = columns.size();
      Type.PrimitiveType[] types = new Type.PrimitiveType[width];
      UpdateSketch[] columnSketches = new UpdateSketch[width];
      for (int pos = 0; pos < width; pos += 1) {
        types[pos] = columns.get(pos).type().asPrimitiveType();
        columnSketches[pos] = sketches.get(columns.get(pos).fieldId());
      }

      // a staged file comes with its deletes and _op after the columns: the stop check counts them
      boolean staged = file.stagedFile() != null;
      long rows = 0L;
      long rawRows = 0L;
      try (CloseableIterable<Record> records = opener.open(file, projection);
          CloseableIterator<Record> iterator = records.iterator()) {
        while (iterator.hasNext()) {
          if (sketch(iterator.next(), staged, types, columnSketches)) {
            rows += 1;
          }

          rawRows += 1;
          if (rawRows % STOP_CHECK_ROWS == 0 && isStopped(stopped)) {
            return Recount.stopped(filesRead, bytesRead);
          }
        }
      } catch (IOException | RuntimeException e) {
        return isStopped(stopped)
            ? Recount.stopped(filesRead, bytesRead)
            : Recount.failed(e, filesRead, bytesRead);
      }

      for (Types.NestedField column : columns) {
        rowsRead.merge(column.fieldId(), rows, Long::sum);
      }

      filesRead += 1;
      bytesRead += file.sizeBytes();
    }

    return new Recount(
        Recount.Status.READ,
        ImmutableMap.copyOf(sketches),
        ImmutableMap.copyOf(rowsRead),
        filesRead,
        bytesRead,
        null);
  }

  /** Adds the values of a row to the sketches of its columns, unless it is a staged delete. */
  private static boolean sketch(
      Record row, boolean staged, Type.PrimitiveType[] types, UpdateSketch[] columnSketches) {
    int width = types.length;
    if (staged && !StatisticsFileReaders.addsValues(row.get(width, Integer.class))) {
      return false;
    }

    for (int pos = 0; pos < width; pos += 1) {
      SketchValues.update(
          columnSketches[pos],
          SketchValues.toByteBuffer(types[pos], widened(types[pos], row.get(pos))));
    }

    return true;
  }

  /**
   * The value as the type of S holds it: a constant of a partition that the worker knew as int or
   * float is an {@link Integer} or a {@link Float} while the column has been widened since.
   */
  private static Object widened(Type.PrimitiveType type, Object value) {
    if (type.typeId() == Type.TypeID.LONG && value instanceof Integer) {
      return ((Integer) value).longValue();
    }

    if (type.typeId() == Type.TypeID.DOUBLE && value instanceof Float) {
      return ((Float) value).doubleValue();
    }

    return value;
  }

  /**
   * The columns of the file, all optional: a column added to the table after the file was written
   * comes back as {@code null} or its {@code initial-default}, whether it is required or not.
   */
  private static Schema projection(Schema schemaS, Set<Integer> fieldIds) {
    List<Types.NestedField> columns = Lists.newArrayList();
    for (Types.NestedField column : TypeUtil.select(schemaS, fieldIds).columns()) {
      columns.add(column.asOptional());
    }

    return new Schema(columns);
  }

  private static CloseableIterable<Record> open(Table table, PlannedFile file, Schema projection) {
    if (file.dataFile() != null) {
      DataFile dataFile = file.dataFile();
      return StatisticsFileReaders.dataFile(
          table.io(),
          dataFile,
          table.specs().get(dataFile.specId()),
          projection,
          StatisticsFileReaders.nameMapping(table));
    }

    return StatisticsFileReaders.stagedFileWithOp(table.io(), file.stagedFile(), projection);
  }

  private static boolean isStopped(BooleanSupplier stopped) {
    return stopped.getAsBoolean() || Thread.currentThread().isInterrupted();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("kind", kind)
        .add("gapFiles", gapFiles)
        .add("gapBytes", gapBytes)
        .add("accepted", accepted.size())
        .add("rejected", rejected.size())
        .add("files", files.size())
        .toString();
  }

  /** A file of the plan and the accepted columns it is read for. */
  public static class PlannedFile {
    private final DataFile dataFile;
    private final StagedChangeFile stagedFile;
    private final long sizeBytes;
    private final Set<Integer> fieldIds;

    private PlannedFile(
        DataFile dataFile, StagedChangeFile stagedFile, long sizeBytes, Set<Integer> fieldIds) {
      this.dataFile = dataFile;
      this.stagedFile = stagedFile;
      this.sizeBytes = sizeBytes;
      this.fieldIds = fieldIds;
    }

    /** The data file of the gap or of a merge-on-read answer, or {@code null}. */
    public DataFile dataFile() {
      return dataFile;
    }

    /** The staged change file of a copy-on-write answer, or {@code null}. */
    public StagedChangeFile stagedFile() {
      return stagedFile;
    }

    public String location() {
      return dataFile != null ? dataFile.location() : stagedFile.location();
    }

    /** The size the limit counts: {@code fileSizeInBytes} or {@code fileSizeBytes}. */
    public long sizeBytes() {
      return sizeBytes;
    }

    public Set<Integer> fieldIds() {
      return fieldIds;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("location", location())
          .add("sizeBytes", sizeBytes)
          .add("fieldIds", fieldIds)
          .toString();
    }
  }

  /** A column whose files do not fit within the limit, and how much they are. */
  public static class RejectedColumn {
    private final Types.NestedField column;
    private final long files;
    private final long bytes;

    private RejectedColumn(Types.NestedField column, long files, long bytes) {
      this.column = column;
      this.files = files;
      this.bytes = bytes;
    }

    public Types.NestedField column() {
      return column;
    }

    /** The files the column needs that are not in the list of the columns before it. */
    public long files() {
      return files;
    }

    public long bytes() {
      return bytes;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("column", column.name())
          .add("files", files)
          .add("bytes", bytes)
          .toString();
    }
  }

  /** What reading the files of the plan gave. */
  public static class Recount {
    public enum Status {
      /** Every file was read. */
      READ,
      /** The step was stopped before all the files were read. */
      STOPPED,
      /** A file could not be read. */
      FAILED
    }

    private final Status status;
    private final Map<Integer, UpdateSketch> sketches;
    private final Map<Integer, Long> rowsRead;
    private final long filesRead;
    private final long bytesRead;
    private final Exception failure;

    private Recount(
        Status status,
        Map<Integer, UpdateSketch> sketches,
        Map<Integer, Long> rowsRead,
        long filesRead,
        long bytesRead,
        Exception failure) {
      this.status = status;
      this.sketches = sketches;
      this.rowsRead = rowsRead;
      this.filesRead = filesRead;
      this.bytesRead = bytesRead;
      this.failure = failure;
    }

    private static Recount stopped(long filesRead, long bytesRead) {
      return new Recount(
          Status.STOPPED, ImmutableMap.of(), ImmutableMap.of(), filesRead, bytesRead, null);
    }

    private static Recount failed(Exception failure, long filesRead, long bytesRead) {
      return new Recount(
          Status.FAILED, ImmutableMap.of(), ImmutableMap.of(), filesRead, bytesRead, failure);
    }

    public Status status() {
      return status;
    }

    /** By field id, in the order of the set; empty unless {@link Status#READ}. */
    public Map<Integer, UpdateSketch> sketches() {
      return sketches;
    }

    /** The rows read for the column; of a staged file, only inserts and updates. */
    public long rowsRead(int fieldId) {
      return rowsRead.getOrDefault(fieldId, 0L);
    }

    /** The files read in full, up to the stop or the failure. */
    public long filesRead() {
      return filesRead;
    }

    public long bytesRead() {
      return bytesRead;
    }

    /** The exception of the file that could not be read, or {@code null}. */
    public Exception failure() {
      return failure;
    }
  }

  /** The files and bytes taken so far against the limits. */
  private static class ReadLimit {
    private final long maxFiles;
    private final long maxBytes;
    private long files = 0L;
    private long bytes = 0L;

    private ReadLimit(long maxFiles, long maxBytes) {
      this.maxFiles = maxFiles;
      this.maxBytes = maxBytes;
    }

    private boolean fits(long moreFiles, long moreBytes) {
      return files + moreFiles <= maxFiles && bytes + moreBytes <= maxBytes;
    }

    private void add(long moreFiles, long moreBytes) {
      files += moreFiles;
      bytes += moreBytes;
    }

    private boolean exceeded() {
      return files > maxFiles || bytes > maxBytes;
    }
  }

  private static class Entry {
    private final DataFile dataFile;
    private final StagedChangeFile stagedFile;
    private final long sizeBytes;
    // a file of the gap is read for all accepted columns
    private final boolean allColumns;
    private final Set<Integer> fieldIds = Sets.newLinkedHashSet();

    private Entry(
        DataFile dataFile, StagedChangeFile stagedFile, long sizeBytes, boolean allColumns) {
      this.dataFile = dataFile;
      this.stagedFile = stagedFile;
      this.sizeBytes = sizeBytes;
      this.allColumns = allColumns;
    }
  }
}
