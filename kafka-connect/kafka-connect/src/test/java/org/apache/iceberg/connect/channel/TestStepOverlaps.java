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
import static org.mockito.Mockito.mock;

import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.data.StatisticsHealth;
import org.apache.iceberg.connect.data.StatisticsStepInput;
import org.junit.jupiter.api.Test;

public class TestStepOverlaps {
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");

  @Test
  public void secondStepOfSameTableAndSnapshotIsOverlap() {
    StepOverlaps detector = new StepOverlaps();
    StatisticsStepInput input = input(TABLE, 5L);

    detector.track(input, () -> detector.track(input, () -> null));

    assertThat(detector.overlaps()).containsExactly("db.tbl at 5");
  }

  @Test
  public void stepsOfOtherSnapshotsOrTablesOrOneAfterAnotherAreNoOverlap() {
    StepOverlaps detector = new StepOverlaps();

    detector.track(
        input(TABLE, 5L),
        () ->
            detector.track(
                input(TABLE, 6L),
                () -> detector.track(input(TableIdentifier.of("db", "o"), 5L), () -> null)));
    detector.track(input(TABLE, 5L), () -> null);
    detector.track(input(TABLE, 5L), () -> null);

    assertThat(detector.overlaps()).isEmpty();
  }

  private static StatisticsStepInput input(TableIdentifier identifier, Long snapshotId) {
    return StatisticsStepInput.builder()
        .table(mock(Table.class))
        .identifier(identifier)
        .snapshotId(snapshotId)
        .commitId(UUID.randomUUID())
        .stopped(() -> false)
        .config(mock(IcebergSinkConfig.class))
        .tableConfig(mock(TableSinkConfig.class))
        .health(new StatisticsHealth(1L))
        .build();
  }
}
