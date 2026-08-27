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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.regex.Pattern;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestRecordRouters {

  private static final String ROUTE_FIELD = "type";
  private static final String EVENTS_TABLE = "db.events";
  private static final String LOGS_TABLE = "db.logs";

  @Test
  public void testAllTablesRouterRoutesToEveryConfiguredTable() {
    RecordRouter router = new AllTablesRecordRouter(ImmutableList.of(EVENTS_TABLE, LOGS_TABLE));

    assertThat(router.route(record("topic", ImmutableMap.of())))
        .containsExactly(EVENTS_TABLE, LOGS_TABLE);
    assertThat(router.ignoreMissingTable()).isFalse();
  }

  @Test
  public void testDynamicFieldRouterRoutesToLowercaseTableName() {
    RecordRouter router = new DynamicFieldRecordRouter(ROUTE_FIELD);

    assertThat(router.route(record("topic", ImmutableMap.of(ROUTE_FIELD, "DB.Events"))))
        .containsExactly(EVENTS_TABLE);
    assertThat(router.ignoreMissingTable()).isTrue();
  }

  @Test
  public void testDynamicFieldRouterSkipsRecordWithoutRouteValue() {
    RecordRouter router = new DynamicFieldRecordRouter(ROUTE_FIELD);

    assertThat(router.route(record("topic", null))).isEmpty();
    assertThat(router.route(record("topic", ImmutableMap.of("other", "DB.Events")))).isEmpty();
  }

  @Test
  public void testRegexRouterRoutesToMatchingTables() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(EVENTS_TABLE))
        .thenReturn(tableConfigWithRouteRegex(Pattern.compile("event-.*")));
    when(config.tableConfig(LOGS_TABLE))
        .thenReturn(tableConfigWithRouteRegex(Pattern.compile("log-.*")));
    RecordRouter router =
        new RegexRecordRouter(config, ROUTE_FIELD, ImmutableList.of(EVENTS_TABLE, LOGS_TABLE));

    assertThat(router.route(record("topic", ImmutableMap.of(ROUTE_FIELD, "event-created"))))
        .containsExactly(EVENTS_TABLE);
    assertThat(router.ignoreMissingTable()).isFalse();
  }

  @Test
  public void testRegexRouterSkipsRecordWithoutMatchingTable() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(EVENTS_TABLE))
        .thenReturn(tableConfigWithRouteRegex(Pattern.compile("event-.*")));
    RecordRouter router =
        new RegexRecordRouter(config, ROUTE_FIELD, ImmutableList.of(EVENTS_TABLE));

    assertThat(router.route(record("topic", null))).isEmpty();
    assertThat(router.route(record("topic", ImmutableMap.of("other", "event-created")))).isEmpty();
    assertThat(router.route(record("topic", ImmutableMap.of(ROUTE_FIELD, "log-created"))))
        .isEmpty();
  }

  @Test
  public void testTopicToTableRouterRoutesByTopic() {
    RecordRouter router =
        new TopicToTableRecordRouter(ImmutableMap.of("events-topic", EVENTS_TABLE));

    assertThat(router.route(record("events-topic", ImmutableMap.of())))
        .containsExactly(EVENTS_TABLE);
    assertThat(router.ignoreMissingTable()).isFalse();
  }

  @Test
  public void testTopicToTableRouterSkipsUnmappedTopic() {
    RecordRouter router =
        new TopicToTableRecordRouter(ImmutableMap.of("events-topic", EVENTS_TABLE));

    assertThat(router.route(record("logs-topic", ImmutableMap.of()))).isEmpty();
  }

  @Test
  public void testFactoryCreatesRouterForSelectedStrategy() {
    assertThat(RecordRouterFactory.create(configForStrategy(RecordRoutingStrategy.ALL_TABLES)))
        .isInstanceOf(AllTablesRecordRouter.class);
    assertThat(RecordRouterFactory.create(configForStrategy(RecordRoutingStrategy.DYNAMIC_FIELD)))
        .isInstanceOf(DynamicFieldRecordRouter.class);
    assertThat(RecordRouterFactory.create(configForStrategy(RecordRoutingStrategy.REGEX)))
        .isInstanceOf(RegexRecordRouter.class);
    assertThat(RecordRouterFactory.create(configForStrategy(RecordRoutingStrategy.TOPIC_TO_TABLE)))
        .isInstanceOf(TopicToTableRecordRouter.class);
  }

  private static SinkRecord record(String topic, Object value) {
    return new SinkRecord(topic, 0, null, null, null, value, 0);
  }

  private static TableSinkConfig tableConfigWithRouteRegex(Pattern routeRegex) {
    return new TableSinkConfig(routeRegex, ImmutableList.of(), ImmutableList.of(), null);
  }

  private static IcebergSinkConfig configForStrategy(RecordRoutingStrategy strategy) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.routingStrategy()).thenReturn(strategy);
    when(config.tables()).thenReturn(ImmutableList.of(EVENTS_TABLE));
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);
    when(config.topicToTableMapping()).thenReturn(ImmutableMap.of("events-topic", EVENTS_TABLE));
    return config;
  }
}
