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
package org.apache.iceberg.connect.data.copyonwrite;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collapses a frozen change set to the final state of its smallest {@code N} identifier keys past
 * the cursor.
 *
 * <p>Runs on the coordinator, reads only the staged change files of one frozen set. Does not know
 * about the control topic, {@code CommitState} or {@code Event}: it is a pure function of its
 * arguments, tested without either.
 *
 * <p>Where the cursor comes from and where it is persisted are the caller's concern; this class
 * only ever sees the cursor and the record quota as plain parameters.
 */
public final class ChangeSetNormalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeSetNormalizer.class);

  private ChangeSetNormalizer() {}

  /**
   * Reads the given staged files and folds them into one slice.
   *
   * @param stagedFiles the frozen change set's staged files, in freeze order
   * @param cursor the last key applied by a previous slice of this change set, or {@code null}
   * @param maxChangeSetRecords the heap quota: the slice never holds more distinct keys than this
   */
  public static ChangeSetSlice normalize(
      Table table,
      Set<Integer> identifierFieldIds,
      List<StagedChangeFile> stagedFiles,
      StructLike cursor,
      long maxChangeSetRecords) {
    // a non-positive quota admits no key at all, so every slice comes back empty and truncated,
    // and a drain that is never exhausted but never advances spins between starting and finishing a
    // slice. The setting itself is range-checked, so reaching here means a caller computed it.
    Preconditions.checkArgument(
        maxChangeSetRecords > 0, "Record quota must be positive: %s", maxChangeSetRecords);

    Schema stagedSchema = StagedChangeSchema.stagedSchema(table.schema(), identifierFieldIds);
    List<NestedField> orderedIdFields =
        IdentifierKeys.orderedFields(stagedSchema, identifierFieldIds);
    IdentifierKeys.KeyBuilder keyBuilder = IdentifierKeys.keyBuilder(stagedSchema, orderedIdFields);
    Comparator<StructLike> keyComparator = IdentifierKeys.comparator(orderedIdFields);
    LeadColumnBounds bounds = new LeadColumnBounds(orderedIdFields);

    NavigableMap<StructLike, ChangeRecord> changeSet = new TreeMap<>(keyComparator);
    boolean truncated = false;
    int skippedFiles = 0;
    long foldedBytes = 0;
    long foldedRecords = 0;

    for (StagedChangeFile file : stagedFiles) {
      boolean full = changeSet.size() >= maxChangeSetRecords;
      if (bounds.isBehindCursor(file, cursor)) {
        // every key in the file was applied by an earlier slice of this change set
        skippedFiles++;
      } else if (full
          && bounds.isBeyondWindow(file, changeSet.isEmpty() ? null : changeSet.lastKey())) {
        // the slice is full and every key in the file sorts after its last one
        skippedFiles++;
        truncated = true;
      } else {
        foldedBytes += file.fileSizeBytes();
        foldedRecords += file.recordCount();
        truncated |=
            foldFile(
                table,
                file,
                stagedSchema,
                keyBuilder,
                keyComparator,
                cursor,
                changeSet,
                maxChangeSetRecords);
      }
    }

    if (skippedFiles > 0) {
      LOG.info(
          "Skipped {} of {} staged file(s) by their {} bounds",
          skippedFiles,
          stagedFiles.size(),
          orderedIdFields.get(0).name());
    }

    // one row per key is retained, so the slice's footprint is the average encoded record size
    // across what it read, times the keys it kept. See ChangeSetSlice#estimatedRetainedBytes for
    // why this is an order of magnitude and not a measurement
    long estimatedRetainedBytes =
        foldedRecords == 0 ? 0 : foldedBytes / foldedRecords * changeSet.size();

    return new ChangeSetSlice(orderedIdFields, changeSet, truncated, estimatedRetainedBytes);
  }

  /**
   * Decides whether a staged file can be skipped without opening it, from the bounds its writer
   * recorded for the first identifier column.
   *
   * <p>Only the first column, and only ever the first: keys sort lexicographically, so a bound on
   * the leading column bounds the whole key, while a bound on any later one says nothing about it.
   * A single-column key gets the tighter of the two comparisons: with a composite key an equal
   * leading value leaves the rest of the key undecided, so equality has to be read as "might
   * match".
   *
   * <p>This is what keeps a change set that drains over many slices from re-reading every staged
   * file for every slice. It prunes nothing when keys are scattered across files, which is the same
   * case where copy-on-write is the wrong mode to begin with.
   */
  private static final class LeadColumnBounds {
    private final int fieldId;
    private final PrimitiveType type;
    private final Comparator<Object> comparator;
    private final boolean singleColumnKey;

    private LeadColumnBounds(List<NestedField> orderedIdFields) {
      NestedField lead = orderedIdFields.get(0);
      this.fieldId = lead.fieldId();
      this.type = lead.type().asPrimitiveType();
      this.comparator = Comparators.forType(type);
      this.singleColumnKey = orderedIdFields.size() == 1;
    }

    /** True if every key in the file is at or before the cursor. */
    private boolean isBehindCursor(StagedChangeFile file, StructLike cursor) {
      if (cursor == null) {
        return false;
      }
      Object upper = bound(file.upperBounds());
      if (upper == null) {
        return false;
      }
      int cmp = comparator.compare(upper, cursor.get(0, Object.class));
      return singleColumnKey ? cmp <= 0 : cmp < 0;
    }

    /** True if every key in the file sorts after the last key the full slice holds. */
    private boolean isBeyondWindow(StagedChangeFile file, StructLike lastKey) {
      if (lastKey == null) {
        return false;
      }
      Object lower = bound(file.lowerBounds());
      if (lower == null) {
        return false;
      }
      return comparator.compare(lower, lastKey.get(0, Object.class)) > 0;
    }

    private Object bound(Map<Integer, ByteBuffer> bounds) {
      if (bounds == null) {
        return null;
      }
      ByteBuffer value = bounds.get(fieldId);
      return value == null ? null : Conversions.fromByteBuffer(type, value);
    }
  }

  /**
   * Folds one staged file into the change set. Returns true if anything was left out for the record
   * quota, which is what makes the slice a truncated one.
   *
   * <p>A file that is not there comes back as a {@link NotFoundException} naming it, told apart
   * from every other read failure and never skipped: the rows it held were acknowledged to Kafka
   * when their envelope was sent, so no later cycle can read them from anywhere else. What the
   * caller does with that is its own: on the coordinator it stops the table, on a worker a missing
   * slice file is an ordinary retryable failure.
   */
  private static boolean foldFile(
      Table table,
      StagedChangeFile file,
      Schema stagedSchema,
      IdentifierKeys.KeyBuilder keyBuilder,
      Comparator<StructLike> keyComparator,
      StructLike cursor,
      NavigableMap<StructLike, ChangeRecord> changeSet,
      long maxChangeSetRecords) {
    boolean truncated = false;
    try (CloseableIterable<Record> records =
        StagedChangeFileReader.open(table.io(), file, stagedSchema)) {
      for (Record rec : records) {
        StructLike key = keyBuilder.keyOf(rec);
        if (cursor != null && keyComparator.compare(key, cursor) <= 0) {
          // already applied by a previous slice of this change set
          continue;
        }

        if (!changeSet.containsKey(key) && changeSet.size() >= maxChangeSetRecords) {
          if (!changeSet.isEmpty() && keyComparator.compare(key, changeSet.lastKey()) >= 0) {
            truncated = true;
            continue;
          }
          changeSet.pollLastEntry();
          truncated = true;
        }

        ChangeRecord fold = ChangeRecord.initial(op(rec), rec, orderKey(rec));
        changeSet.merge(key, fold, ChangeRecord::combine);
      }
    } catch (RuntimeException e) {
      if (isMissingFile(e)) {
        throw new NotFoundException(
            e, "staged change file %s of the change set is gone", file.location());
      }
      throw e;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read staged change file: " + file.location(), e);
    }
    return truncated;
  }

  /**
   * Whether a failed read is the file being gone rather than storage having a bad moment.
   *
   * <p>The cause chain, not the top of it: object stores raise {@link NotFoundException}
   * themselves, a local or Hadoop file system raises {@code FileNotFoundException} and whatever
   * opened the stream wraps it, and Avro wraps again on its way out of the iterator.
   */
  private static boolean isMissingFile(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof NotFoundException || cause instanceof FileNotFoundException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Writes a normalized slice out as a staged change file, one row per key, so that every worker of
   * the slice reads the byte-identical change set: each worker derives which keys it owns from the
   * slice it reads, so two workers reading different slices would silently overlap or drop keys.
   *
   * <p>The file is an ordinary staged change file: {@code PRESENT} keys keep their final row with
   * {@code _op = OP_UPDATE}, and both dead states collapse to the key with {@code _op = OP_DELETE}.
   * The collapse is lossless for the rewriter, which drops {@code ABSENT} and {@code
   * INSERTED_THEN_DELETED} alike; the distinction only ever mattered to merge-on-read.
   *
   * <p>Reading it back is {@link #normalize} with a {@code null} cursor and an unlimited quota:
   * anything else would re-apply a cursor already applied here, or re-truncate a slice already
   * truncated here, and hand two workers different slices.
   */
  public static StagedChangeFile writeNormalized(
      Table table, Set<Integer> identifierFieldIds, ChangeSetSlice slice, String location) {
    Schema stagedSchema = StagedChangeSchema.stagedSchema(table.schema(), identifierFieldIds);
    OutputFile outputFile = table.io().newOutputFile(location);

    long recordCount = 0;
    FileAppender<Record> appender;
    try {
      appender =
          Avro.write(outputFile)
              .schema(stagedSchema)
              .createWriterFunc(DataWriter::create)
              .named("staged_change")
              .meta(
                  StagedChangeSchema.FORMAT_VERSION_META,
                  Integer.toString(StagedChangeSchema.FORMAT_VERSION))
              .meta(StagedChangeSchema.SCHEMA_ID_META, Integer.toString(table.schema().schemaId()))
              .overwrite()
              .build();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open normalized slice file: " + location, e);
    }

    try {
      for (ChangeRecord change : slice.changes().values()) {
        int opCode =
            change.state() == ChangeRecord.State.PRESENT
                ? StagedChangeSchema.OP_UPDATE
                : StagedChangeSchema.OP_DELETE;
        appender.add(change.row().copy(StagedChangeSchema.OP, opCode));
        recordCount += 1;
      }
    } catch (RuntimeException e) {
      closeQuietly(appender, location);
      throw e;
    }

    try {
      appender.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write normalized slice file: " + location, e);
    }

    // no bounds: nothing prunes this file, every worker of the slice reads it whole
    return new StagedChangeFile(
        location,
        appender.length(),
        recordCount,
        table.schema().schemaId(),
        StagedChangeSchema.FORMAT_VERSION,
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  private static void closeQuietly(FileAppender<Record> appender, String location) {
    try {
      appender.close();
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to close normalized slice file after an error: {}", location, e);
    }
  }

  private static int op(Record rec) {
    return (Integer) rec.getField(StagedChangeSchema.OP);
  }

  private static OrderKey orderKey(Record rec) {
    String topic = (String) rec.getField(StagedChangeSchema.TOPIC);
    int partition = (Integer) rec.getField(StagedChangeSchema.PARTITION);
    long offset = (Long) rec.getField(StagedChangeSchema.OFFSET);
    return new OrderKey(topic, partition, offset);
  }
}
