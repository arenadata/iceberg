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
package org.apache.iceberg.connect.channel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.PluginMetrics;

/**
 * The metrics of statistics health, per task: how many tables are in each state.
 *
 * <p>Registered and unregistered on the task thread. A coordinator, on any thread, only {@link
 * #attach attaches} its own {@link StatisticsHealth} and {@link #detach detaches} it, so that a
 * coordinator still finishing {@code terminate()} after the task got the leader partition back
 * leaves the health of the new one alone.
 *
 * <p>The metrics have no tags of their own: the runtime adds the group ({@code plugins}) and the
 * {@code connector} and {@code task} tags, and refuses a tag with one of those keys. No sensors are
 * used: their names are shared by the whole worker.
 */
class StatisticsMetrics {

  private static final List<TableCountMetric> METRICS =
      ImmutableList.of(
          new TableCountMetric(
              "statistics-tables-gap-over-limit",
              "Number of tables whose latest statistics step could not fit the snapshot gap or an "
                  + "uncovered column within the gap limit; their NDV stays behind until "
                  + "statistics are recomputed",
              Flags::gapOverLimit),
          new TableCountMetric(
              "statistics-tables-missing-baseline",
              "Number of non-empty tables that have no statistics, or have columns without "
                  + "statistics, to build NDV on; compute statistics for the table or those "
                  + "columns once to start incremental updates",
              Flags::missingBaseline),
          new TableCountMetric(
              "statistics-tables-stale",
              "Number of tables whose latest NDV statistics exceed "
                  + "iceberg.tables.statistics.stale-deleted-ratio; recompute table statistics",
              Flags::stale));

  private final AtomicReference<StatisticsHealth> current = new AtomicReference<>();
  private PluginMetrics registeredIn = null;
  private final List<MetricName> registered = Lists.newArrayList();

  void register(PluginMetrics pluginMetrics) {
    List<MetricName> added = Lists.newArrayList();
    try {
      for (TableCountMetric metric : METRICS) {
        MetricName name =
            pluginMetrics.metricName(metric.name, metric.description, new LinkedHashMap<>());
        pluginMetrics.addMetric(name, (Measurable) (config, now) -> count(metric.flag));
        added.add(name);
      }
    } catch (RuntimeException e) {
      for (MetricName name : added) {
        try {
          pluginMetrics.removeMetric(name);
        } catch (RuntimeException suppressed) {
          e.addSuppressed(suppressed);
        }
      }

      throw e;
    }

    registeredIn = pluginMetrics;
    registered.addAll(added);
  }

  /**
   * Takes off what {@link #register} put in, all of it even if one removal fails, and throws the
   * first failure: {@link IllegalStateException} when the runtime has closed the {@code
   * PluginMetrics}, {@link IllegalArgumentException} when a metric is already gone. Nothing is
   * registered afterwards either way. Task thread only.
   */
  void unregister() {
    PluginMetrics from = registeredIn;
    List<MetricName> names = ImmutableList.copyOf(registered);
    registeredIn = null;
    registered.clear();

    RuntimeException failure = null;
    for (MetricName name : names) {
      try {
        from.removeMetric(name);
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }

    if (failure != null) {
      throw failure;
    }
  }

  void attach(StatisticsHealth health) {
    current.set(health);
  }

  void detach(StatisticsHealth health) {
    current.compareAndSet(health, null);
  }

  private double count(Predicate<Flags> flag) {
    StatisticsHealth health = current.get();
    return health == null ? 0 : health.count(flag);
  }

  private static final class TableCountMetric {
    private final String name;
    private final String description;
    private final Predicate<Flags> flag;

    private TableCountMetric(String name, String description, Predicate<Flags> flag) {
      this.name = name;
      this.description = description;
      this.flag = flag;
    }
  }
}
