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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsHealth.Flags;
import org.junit.jupiter.api.Test;

public class TestStatisticsMetrics {

  private static final String GAP_OVER_LIMIT = "statistics-tables-gap-over-limit";
  private static final String MISSING_BASELINE = "statistics-tables-missing-baseline";
  private static final String STALE = "statistics-tables-stale";

  private final FakePluginMetrics pluginMetrics = new FakePluginMetrics();
  private final StatisticsMetrics metrics = new StatisticsMetrics();

  @Test
  public void testThreeMetricsAreNamedAndDescribedAsInSpecification() {
    metrics.register(pluginMetrics);

    assertThat(pluginMetrics.names())
        .containsExactlyInAnyOrder(GAP_OVER_LIMIT, MISSING_BASELINE, STALE);
    assertThat(pluginMetrics.metric(GAP_OVER_LIMIT).description())
        .isEqualTo(
            "Number of tables whose latest statistics step could not fit the snapshot gap or an "
                + "uncovered column within the gap limit; their NDV stays behind until "
                + "statistics are recomputed");
    assertThat(pluginMetrics.metric(MISSING_BASELINE).description())
        .isEqualTo(
            "Number of non-empty tables that have no statistics, or have columns without "
                + "statistics, to build NDV on; compute statistics for the table or those "
                + "columns once to start incremental updates");
    assertThat(pluginMetrics.metric(STALE).description())
        .isEqualTo(
            "Number of tables whose latest NDV statistics exceed "
                + "iceberg.tables.statistics.stale-deleted-ratio; recompute table statistics");
  }

  @Test
  public void testMetricsHaveNoTagsOfTheirOwnAndNoSensorIsAdded() {
    // the group and the connector and task tags are the runtime's; a tag of ours with one of
    // their keys would be refused. FakePluginMetrics throws on addSensor
    metrics.register(pluginMetrics);

    assertThat(pluginMetrics.names()).hasSize(3);
    for (String name : pluginMetrics.names()) {
      assertThat(pluginMetrics.metric(name).tags()).as("tags of %s", name).isEmpty();
    }
  }

  @Test
  public void testEachMetricCountsTablesOfItsFlagInAttachedHealth() {
    metrics.register(pluginMetrics);
    StatisticsHealth health = new StatisticsHealth(0);
    metrics.attach(health);

    // no table yet
    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(0.0);

    health.update(table("a"), flags -> new Flags(true, false, false));
    health.update(table("b"), flags -> new Flags(true, true, false));
    health.update(table("c"), flags -> new Flags(false, true, true));
    health.update(table("d"), flags -> new Flags(false, false, true));
    health.update(table("e"), flags -> new Flags(false, false, false));
    health.update(table("f"), flags -> new Flags(false, false, true));

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).as("a and b").isEqualTo(2.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).as("b and c").isEqualTo(2.0);
    assertThat(pluginMetrics.metricValue(STALE)).as("c, d and f").isEqualTo(3.0);

    // a metric follows its health, it is not a snapshot of it
    health.update(table("a"), flags -> Flags.NONE);
    health.remove(table("f"));

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(1.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(2.0);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(2.0);
  }

  @Test
  public void testWithoutHealthEveryMetricReadsZero() {
    metrics.register(pluginMetrics);

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(0.0);
  }

  @Test
  public void testAttachedHealthTakesMetricsOverFromPreviousOne() {
    metrics.register(pluginMetrics);
    StatisticsHealth first = healthWithOneTableOfEachFlag();
    StatisticsHealth second = new StatisticsHealth(0);
    second.update(table("x"), flags -> new Flags(true, true, false));
    second.update(table("y"), flags -> new Flags(true, false, false));
    second.update(table("z"), flags -> new Flags(true, false, false));

    metrics.attach(first);
    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(1.0);

    metrics.attach(second);
    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(3.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(1.0);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(0.0);
  }

  @Test
  public void testDetachingCurrentHealthMakesMetricsReadZero() {
    metrics.register(pluginMetrics);
    StatisticsHealth health = healthWithOneTableOfEachFlag();
    metrics.attach(health);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(1.0);

    metrics.detach(health);

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(0.0);
  }

  @Test
  public void testDetachingAnotherHealthDoesNotReleaseCurrentOne() {
    // the coordinator that lost the task's leadership finishes terminate() after the next one was
    // attached: its detach must find the new health there and leave it alone
    metrics.register(pluginMetrics);
    StatisticsHealth old = healthWithOneTableOfEachFlag();
    StatisticsHealth current = new StatisticsHealth(0);
    current.update(table("x"), flags -> new Flags(true, false, false));
    current.update(table("y"), flags -> new Flags(true, false, false));
    metrics.attach(old);
    metrics.attach(current);

    metrics.detach(old);

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(2.0);
    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(0.0);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(0.0);
  }

  @Test
  public void testDetachOfHealthThatWasNeverAttachedKeepsCurrentOne() {
    metrics.register(pluginMetrics);
    StatisticsHealth current = healthWithOneTableOfEachFlag();
    metrics.attach(current);

    metrics.detach(new StatisticsHealth(0));
    metrics.detach(StatisticsHealth.NOOP);

    assertThat(pluginMetrics.metricValue(MISSING_BASELINE)).isEqualTo(1.0);
  }

  @Test
  public void testHealthAttachedBeforeRegistrationIsReadOnceRegistered() {
    // a health attached to metrics that are not, or no longer, registered is not read
    StatisticsHealth health = healthWithOneTableOfEachFlag();
    metrics.attach(health);
    assertThat(pluginMetrics.names()).isEmpty();

    metrics.register(pluginMetrics);

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(1.0);
  }

  @Test
  public void testUnregisterRemovesThreeMetricsAndTheyCanBeRegisteredAgain() {
    metrics.register(pluginMetrics);
    assertThat(pluginMetrics.names()).hasSize(3);

    metrics.unregister();

    assertThat(pluginMetrics.names()).isEmpty();

    // the next startCoordinator() of the same task: no clash with the names of the first one
    metrics.register(pluginMetrics);
    StatisticsHealth health = healthWithOneTableOfEachFlag();
    metrics.attach(health);

    assertThat(pluginMetrics.names())
        .containsExactlyInAnyOrder(GAP_OVER_LIMIT, MISSING_BASELINE, STALE);
    assertThat(pluginMetrics.metricValue(STALE)).isEqualTo(1.0);
  }

  @Test
  public void testUnregisterDoesNotTouchHealthAttached() {
    // an old coordinator still in terminate() detaches its own health later; the metrics of the
    // next registration read the health of whoever attached last
    metrics.register(pluginMetrics);
    StatisticsHealth health = healthWithOneTableOfEachFlag();
    metrics.attach(health);

    metrics.unregister();
    metrics.register(pluginMetrics);

    assertThat(pluginMetrics.metricValue(GAP_OVER_LIMIT)).isEqualTo(1.0);
  }

  @Test
  public void testUnregisterBeforeAnyRegistrationDoesNothing() {
    metrics.unregister();

    assertThat(pluginMetrics.names()).isEmpty();
  }

  @Test
  public void testTakenNameFailsRegistrationAndLeavesNoneOfThreeBehind() {
    pluginMetrics.reserve(MISSING_BASELINE);

    assertThatThrownBy(() -> metrics.register(pluginMetrics))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(MISSING_BASELINE);

    assertThat(pluginMetrics.names())
        .as("the metric registered before the clash is taken back")
        .isEmpty();
    metrics.unregister();
    assertThat(pluginMetrics.names()).isEmpty();
  }

  @Test
  public void testUnregisterOnClosedPluginMetricsFailsOnceAndForgetsMetrics() {
    metrics.register(pluginMetrics);
    pluginMetrics.close();

    assertThatThrownBy(metrics::unregister)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("This PluginMetrics is closed");

    // the next stop does not fail again
    metrics.unregister();
  }

  @Test
  public void testUnregisterOfMetricsRuntimeAlreadyDroppedFailsButRemovesRest() {
    metrics.register(pluginMetrics);
    pluginMetrics.removeMetric(pluginMetrics.metric(MISSING_BASELINE));

    assertThatThrownBy(metrics::unregister)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(MISSING_BASELINE);

    assertThat(pluginMetrics.names()).as("the other two are removed all the same").isEmpty();
  }

  private static StatisticsHealth healthWithOneTableOfEachFlag() {
    StatisticsHealth health = new StatisticsHealth(0);
    health.update(table("gap"), flags -> new Flags(true, false, false));
    health.update(table("baseline"), flags -> new Flags(false, true, false));
    health.update(table("stale"), flags -> new Flags(false, false, true));
    return health;
  }

  private static TableIdentifier table(String name) {
    return TableIdentifier.of("db", name);
  }
}
