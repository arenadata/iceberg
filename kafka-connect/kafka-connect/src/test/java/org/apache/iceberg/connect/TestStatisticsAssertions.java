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

import static org.apache.iceberg.connect.StatisticsAssertions.assertStatisticsOnOwnSnapshot;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The checks of {@link StatisticsAssertions} themselves, on records written by hand. */
public class TestStatisticsAssertions {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "tbl");

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    // the in-memory files are shared by every catalog of the JVM: a location of its own
    catalog.initialize(
        "catalog",
        ImmutableMap.of("warehouse", "mem://statistics-assertions-" + UUID.randomUUID()));
    catalog.createNamespace(TABLE.namespace());
    table =
        catalog.createTable(
            TABLE,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void ndvUpToRecordsOfAV2SnapshotWithMorePositionDeletesThanRecordsPasses()
      throws IOException {
    Snapshot snapshot = oneRecordAndFivePositionDeletes();
    register(connectorFile(snapshot, connectorPath(snapshot), "1"));

    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void ndvAboveRecordsOfAV2SnapshotFails() throws IOException {
    Snapshot snapshot = oneRecordAndFivePositionDeletes();
    register(connectorFile(snapshot, connectorPath(snapshot), "2"));

    assertThatThrownBy(() -> assertStatisticsOnOwnSnapshot(table))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("is at most live(S)");
  }

  @Test
  public void ndvOfZeroOnAV3SnapshotWithMorePositionDeletesThanRecordsPasses() throws IOException {
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    Snapshot snapshot = oneRecordAndFivePositionDeletes();
    register(connectorFile(snapshot, connectorPath(snapshot), "0"));

    assertStatisticsOnOwnSnapshot(table);
  }

  @Test
  public void ndvAboveZeroOnAV3SnapshotWithMorePositionDeletesThanRecordsFails()
      throws IOException {
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    Snapshot snapshot = oneRecordAndFivePositionDeletes();
    register(connectorFile(snapshot, connectorPath(snapshot), "1"));

    assertThatThrownBy(() -> assertStatisticsOnOwnSnapshot(table))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("is at most live(S)");
  }

  @Test
  public void recordOfConnectorWhoseFileIsGoneFails() throws IOException {
    Snapshot snapshot = oneRecord();
    String path = connectorPath(snapshot);
    register(connectorFile(snapshot, path, "1"));
    table.io().deleteFile(path);

    assertThatThrownBy(() -> assertStatisticsOnOwnSnapshot(table))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(path);
  }

  @Test
  public void foreignRecordWhoseFileIsElsewhereIsSkipped() {
    Snapshot snapshot = oneRecord();
    // a name like the connector's, but outside the metadata of the table
    String path =
        "mem://nowhere/stats-" + snapshot.snapshotId() + "-" + UUID.randomUUID() + ".puffin";
    register(
        new GenericStatisticsFile(
            snapshot.snapshotId(),
            path,
            100L,
            10L,
            ImmutableList.of(
                new GenericBlobMetadata(
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    snapshot.snapshotId(),
                    snapshot.sequenceNumber(),
                    ImmutableList.of(1),
                    ImmutableMap.of("ndv", "1")))));

    assertStatisticsOnOwnSnapshot(table);
  }

  private Snapshot oneRecord() {
    table.newAppend().appendFile(dataFile()).commit();
    return table.currentSnapshot();
  }

  /**
   * {@code total-position-deletes} above {@code total-records}, as dangling deletes give; from v3 a
   * deletion vector.
   */
  private Snapshot oneRecordAndFivePositionDeletes() {
    DataFile data = dataFile();
    FileMetadata.Builder deletes =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(table.location() + "/data/" + UUID.randomUUID() + "-deletes.parquet")
            .withFileSizeInBytes(10L)
            .withRecordCount(5L);
    if (TableUtil.formatVersion(table) < 3) {
      deletes.withFormat(FileFormat.PARQUET);
    } else {
      deletes
          .withFormat(FileFormat.PUFFIN)
          .withReferencedDataFile(data.location())
          .withContentOffset(4L)
          .withContentSizeInBytes(10L);
    }

    table.newRowDelta().addRows(data).addDeletes(deletes.build()).commit();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "5");
    return snapshot;
  }

  private DataFile dataFile() {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10L)
        .withRecordCount(1L)
        .build();
  }

  private String connectorPath(Snapshot snapshot) {
    return ((HasTableOperations) table)
        .operations()
        .metadataFileLocation(
            "stats-" + snapshot.snapshotId() + "-" + UUID.randomUUID() + ".puffin");
  }

  private StatisticsFile connectorFile(Snapshot snapshot, String path, String ndv)
      throws IOException {
    try (PuffinWriter writer =
        Puffin.write(table.io().newOutputFile(path))
            .createdBy("iceberg-kafka-connect test")
            .build()) {
      writer.add(
          new Blob(
              StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
              ImmutableList.of(1),
              snapshot.snapshotId(),
              snapshot.sequenceNumber(),
              ByteBuffer.wrap(new byte[] {1}),
              PuffinCompressionCodec.NONE,
              ImmutableMap.of("ndv", ndv)));
      writer.finish();
      return new GenericStatisticsFile(
          snapshot.snapshotId(),
          path,
          writer.fileSize(),
          writer.footerSize(),
          GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
    }
  }

  private void register(StatisticsFile file) {
    table.updateStatistics().setStatistics(file).commit();
  }
}
