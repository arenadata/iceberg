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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Finds every data file a change-set slice might touch, without reading any of them.
 *
 * <p>Runs on the coordinator. Reads manifests only (never table data) so both quotas (record count,
 * enforced by {@link ChangeSetNormalizer}; rewrite bytes, checked here) are known before the first
 * byte of data is read.
 *
 * <p><b>The plan is never truncated.</b> Exceeding {@code max-rewrite-bytes} is reported on {@link
 * PlanSummary}, never acted on here; shrinking the slice and replanning is the caller's job. That
 * is not a style preference: a distributed rewrite derives who writes a key's new row from the
 * slice alone, on the assumption that a key found in no planned file is absent from the table. Drop
 * a file from the plan and that assumption breaks: the row it held survives untouched while its
 * owner writes the new version beside it, and the key is silently duplicated. No single-task test
 * can catch it.
 */
public final class AffectedFilePlanner {

  private AffectedFilePlanner() {}

  public static PlanResult plan(
      Table table,
      long baseSnapshotId,
      ChangeSetSlice slice,
      int maxInCardinality,
      long maxRewriteBytes) {
    Expression predicate = keyPredicate(table.schema(), slice, maxInCardinality);

    List<FileScanTask> tasks = Lists.newArrayList();
    long totalBytes = 0;
    int filesWithoutBounds = 0;
    // includeColumnStats: bounds are stripped from the returned DataFile by default, and the
    // planner needs them to report files it could not prune.
    // ignoreResiduals is for the delete files (the rewrite reads no residual): without it the
    // filter also prunes delete manifest entries by their own bounds, and an equality delete whose
    // bounds miss every key of the slice goes missing from a planned file it still applies to. The
    // rewrite then carries its deleted rows into a replacement file no older delete reaches, and
    // they come back. Data files are pruned by the filter either way.
    try (CloseableIterable<FileScanTask> planned =
        table
            .newScan()
            .useSnapshot(baseSnapshotId)
            .filter(predicate)
            .ignoreResiduals()
            .includeColumnStats()
            .planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
        totalBytes += task.file().fileSizeInBytes();
        if (missingIdentifierBounds(task.file(), slice.identifierFields())) {
          filesWithoutBounds++;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to plan affected files", e);
    }

    checkOnePartitionSpec(table, tasks);

    double fractionWithoutBounds =
        tasks.isEmpty() ? 0.0 : (double) filesWithoutBounds / tasks.size();
    PlanSummary summary =
        new PlanSummary(
            tasks.size(),
            totalBytes,
            slice.size(),
            fractionWithoutBounds,
            slice.truncated(),
            totalBytes > maxRewriteBytes);
    return new PlanResult(ImmutableList.copyOf(tasks), summary, predicate);
  }

  /**
   * Refuses a plan that spans more than the table's current partition spec.
   *
   * <p>A limit of the wire, not of copy-on-write: {@code RewriteAssigned} and {@code
   * RewriteComplete} build their {@code DataFile} schema from a <b>single</b> partition type, so a
   * file under another spec is encoded through the wrong schema. On a v2 table, where {@code
   * BaseUpdatePartitionSpec} drops a removed field outright rather than voiding it as v1 does, the
   * fields shift and one partition column's value lands under another's name: partition pruning is
   * corrupted silently. Refused here, before anything is written, and as a {@link
   * PermanentCopyOnWriteException}: the next cycle would fail the same way, so the coordinator
   * stops the table's drain instead of retrying it every commit interval. Compacting the old-spec
   * files ({@code rewrite_data_files}) makes the table plan as a single spec again.
   */
  private static void checkOnePartitionSpec(Table table, List<FileScanTask> tasks) {
    int currentSpecId = table.spec().specId();
    Set<Integer> foreignSpecIds =
        tasks.stream()
            .map(task -> task.spec().specId())
            .filter(specId -> specId != currentSpecId)
            .collect(Collectors.toCollection(Sets::newTreeSet));
    if (foreignSpecIds.isEmpty()) {
      return;
    }

    throw new PermanentCopyOnWriteException(
        String.format(
            Locale.ROOT,
            "Cannot rewrite table %s in copy-on-write: the change set touches data files written "
                + "under partition spec(s) %s while the table's current spec is %s, and the "
                + "control protocol cannot carry files of more than one partition spec. Compact "
                + "the files under the older spec (rewrite_data_files), or route this table to a "
                + "merge-on-read connector",
            table.name(),
            foreignSpecIds,
            currentSpecId));
  }

  /**
   * One predicate for the whole slice: {@code AND} of one clause per identifier column, over the
   * column's distinct values in the slice. Deliberately over-inclusive for a composite key (a file
   * can pass every column's clause without holding any key as a whole) but that only ever adds
   * files to the plan, never drops one that holds a real key.
   */
  private static Expression keyPredicate(
      Schema tableSchema, ChangeSetSlice slice, int maxInCardinality) {
    Expression predicate = Expressions.alwaysTrue();
    for (NestedField idField : slice.identifierFields()) {
      predicate =
          Expressions.and(
              predicate, columnPredicate(tableSchema, idField, slice, maxInCardinality));
    }
    return predicate;
  }

  /**
   * One column's clause: the slice's distinct values for it, or their range once there are too many
   * to list.
   *
   * <p>{@code maxInCardinality} is not a free tuning knob. Both {@code ManifestEvaluator} and
   * {@code InclusiveMetricsEvaluator} stop evaluating an {@code IN} predicate past their own
   * (private) {@code IN_PREDICATE_LIMIT} of 200 and answer {@code ROWS_MIGHT_MATCH}, so a longer
   * list prunes nothing at all: worse than the range it replaced, which Iceberg would have
   * evaluated. Configure at or below that limit; the default matches it exactly.
   */
  private static Expression columnPredicate(
      Schema tableSchema, NestedField idField, ChangeSetSlice slice, int maxInCardinality) {
    // the column's name as the scan resolves it, not the field's own: the two differ the moment a
    // field is not top-level, and binding the short name would silently pick the wrong column or
    // fail to bind at all
    String column = tableSchema.findColumnName(idField.fieldId());
    Set<Object> values = slice.distinctValues(idField);
    if (values.size() <= maxInCardinality) {
      return Expressions.in(column, values);
    }

    Comparator<Object> comparator = Comparators.forType((PrimitiveType) idField.type());
    Object min = values.stream().min(comparator).orElseThrow(IllegalStateException::new);
    Object max = values.stream().max(comparator).orElseThrow(IllegalStateException::new);
    return Expressions.and(
        Expressions.greaterThanOrEqual(column, min), Expressions.lessThanOrEqual(column, max));
  }

  /**
   * A file the {@code InclusiveMetricsEvaluator} could not prune for lack of column stats on an
   * identifier column (e.g. {@code write.metadata.metrics.*} set to {@code none}/{@code counts}).
   * Correct to include, but worth flagging: it costs the plan a file with zero pruning benefit.
   */
  private static boolean missingIdentifierBounds(
      DataFile file, List<NestedField> identifierFields) {
    Map<Integer, ?> lower = file.lowerBounds();
    Map<Integer, ?> upper = file.upperBounds();
    if (lower == null || upper == null) {
      return true;
    }
    for (NestedField field : identifierFields) {
      if (!lower.containsKey(field.fieldId()) || !upper.containsKey(field.fieldId())) {
        return true;
      }
    }
    return false;
  }
}
