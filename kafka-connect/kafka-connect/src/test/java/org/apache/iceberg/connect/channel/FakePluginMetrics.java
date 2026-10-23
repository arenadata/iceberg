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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A stand-in for the {@link PluginMetrics} of a task with the runtime's checks: a taken name and
 * removing a missing metric throw {@link IllegalArgumentException}, a closed instance throws {@link
 * IllegalStateException}. Sensors are not supported: their names are shared by the whole worker.
 */
final class FakePluginMetrics implements PluginMetrics {

  private static final String GROUP = "plugins";

  private final Map<MetricName, MetricValueProvider<?>> metrics = Maps.newLinkedHashMap();
  private final Set<String> reserved = Sets.newHashSet();
  private boolean closed = false;
  private int metricsAdded = 0;

  /** Marks {@code name} as taken, as if another registrant of the runtime had added it. */
  synchronized void reserve(String name) {
    reserved.add(name);
  }

  synchronized void unreserve(String name) {
    reserved.remove(name);
  }

  synchronized void close() {
    closed = true;
  }

  synchronized List<String> names() {
    ImmutableList.Builder<String> names = ImmutableList.builder();
    metrics.keySet().forEach(metric -> names.add(metric.name()));
    return names.build();
  }

  /** Successful {@link #addMetric} calls; removals do not decrease it. */
  synchronized int metricsAdded() {
    return metricsAdded;
  }

  synchronized MetricName metric(String name) {
    return metrics.keySet().stream()
        .filter(metric -> metric.name().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No metric " + name + " in " + names()));
  }

  synchronized double metricValue(String name) {
    MetricValueProvider<?> provider = metrics.get(metric(name));
    return ((Measurable) provider).measure(new MetricConfig(), System.currentTimeMillis());
  }

  @Override
  public synchronized MetricName metricName(
      String name, String description, LinkedHashMap<String, String> tags) {
    return new MetricName(name, GROUP, description, tags);
  }

  @Override
  public synchronized void addMetric(MetricName metricName, MetricValueProvider<?> provider) {
    checkOpen();
    if (reserved.contains(metricName.name()) || metrics.containsKey(metricName)) {
      throw new IllegalArgumentException("A metric named '" + metricName + "' already exists");
    }

    metrics.put(metricName, provider);
    metricsAdded += 1;
  }

  @Override
  public synchronized void removeMetric(MetricName metricName) {
    checkOpen();
    if (metrics.remove(metricName) == null) {
      throw new IllegalArgumentException("There is no metric named '" + metricName + "'");
    }
  }

  @Override
  public Sensor addSensor(String name) {
    throw new UnsupportedOperationException("sensors are shared by the worker: not used");
  }

  @Override
  public void removeSensor(String name) {
    throw new UnsupportedOperationException("sensors are shared by the worker: not used");
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("This PluginMetrics is closed");
    }
  }
}
