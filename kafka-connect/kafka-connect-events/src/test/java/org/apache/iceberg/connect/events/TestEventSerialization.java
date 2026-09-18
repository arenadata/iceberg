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
package org.apache.iceberg.connect.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

public class TestEventSerialization {

  // Written once by the events module of 6848f6123, before DataComplete had task_id; do not
  // regenerate from the current code. The values below are the ones it was written with.
  private static final String DATA_COMPLETE_BEFORE_TASK_ID = "data-complete-before-task-id.bin";
  private static final UUID DATA_COMPLETE_BEFORE_TASK_ID_COMMIT =
      UUID.fromString("d1e2f3a4-b5c6-4d7e-8f90-a1b2c3d4e5f6");
  private static final OffsetDateTime DATA_COMPLETE_BEFORE_TASK_ID_TIMESTAMP =
      OffsetDateTime.of(2026, 9, 14, 12, 0, 0, 0, ZoneOffset.UTC);

  @Test
  public void testStartCommitSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event = new Event("cg-connector", new StartCommit(commitId));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testDataWrittenSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new DataWritten(
                EventTestUtil.SPEC.partitionType(),
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(
            "payload\\.partitionType",
            ".*avroSchema",
            ".*icebergSchema",
            ".*schema",
            ".*fromProjectionPos")
        .isEqualTo(event);
  }

  @Test
  public void testDataCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new DataComplete(
                commitId,
                Arrays.asList(
                    new TopicPartitionOffset("topic", 1, 1L, EventTestUtil.now()),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testCommitToTableSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new CommitToTable(
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                1L,
                EventTestUtil.now()));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testCommitCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event = new Event("cg-connector", new CommitComplete(commitId, EventTestUtil.now()));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testDataCompleteWithTaskIdSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new DataComplete(
                commitId,
                Arrays.asList(new TopicPartitionOffset("topic", 1, 1L, EventTestUtil.now())),
                "3"));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(((DataComplete) result.payload()).taskId()).isEqualTo("3");
    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testRowChangesWrittenSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new RowChangesWritten(
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                "0",
                Arrays.asList("topic"),
                Arrays.asList(EventTestUtil.createStagedChangeFile())));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(event);
  }

  @Test
  public void testRewriteAssignedSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new RewriteAssigned(
                EventTestUtil.SPEC.partitionType(),
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                UUID.randomUUID(),
                2,
                123L,
                EventTestUtil.createStagedChangeFile(),
                Arrays.asList(
                    new Assignment(
                        "0",
                        Arrays.asList(new TopicPartitionRef("topic", 0)),
                        Arrays.asList(
                            new FileScanTaskDescriptor(
                                EventTestUtil.createDataFile(),
                                Arrays.asList(EventTestUtil.createDeleteFile()),
                                0,
                                EventTestUtil.SPEC.partitionType()))),
                    // a task with nothing to do is still assigned, so that "nothing to do" and
                    // "never answered" stay distinguishable
                    new Assignment(
                        "1", Arrays.asList(new TopicPartitionRef("topic", 1)), Arrays.asList())),
                1,
                3,
                Arrays.asList(1, 4)));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(
            "payload\\.partitionType",
            ".*avroSchema",
            ".*icebergSchema",
            ".*schema",
            ".*fromProjectionPos")
        .isEqualTo(event);
  }

  @Test
  public void testRewriteAssignedChunkSerialization() {
    // an assignment too large for one message: the worker holds the chunks until it has them all,
    // so which chunk this is and how many there are have to survive the wire
    Event event =
        new Event(
            "cg-connector",
            new RewriteAssigned(
                EventTestUtil.SPEC.partitionType(),
                UUID.randomUUID(),
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                UUID.randomUUID(),
                2,
                123L,
                EventTestUtil.createStagedChangeFile(),
                Arrays.asList(
                    new Assignment(
                        "0",
                        Arrays.asList(new TopicPartitionRef("topic", 0)),
                        Arrays.asList(
                            new FileScanTaskDescriptor(
                                EventTestUtil.createDataFile(),
                                Arrays.asList(EventTestUtil.createDeleteFile()),
                                0,
                                EventTestUtil.SPEC.partitionType())))),
                1,
                3,
                Arrays.asList(1, 4),
                1,
                2));

    RewriteAssigned result = (RewriteAssigned) AvroUtil.decode(AvroUtil.encode(event)).payload();

    assertThat(result.chunkIndex()).isEqualTo(1);
    assertThat(result.chunkCount()).isEqualTo(2);
  }

  @Test
  public void testRewriteCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            new RewriteComplete(
                EventTestUtil.SPEC.partitionType(),
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                UUID.randomUUID(),
                2,
                "0",
                RewriteComplete.STATUS_OK,
                Arrays.asList(EventTestUtil.createDataFile()),
                0,
                1));

    byte[] data = AvroUtil.encode(event);
    Event result = AvroUtil.decode(data);

    assertThat(result)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(
            "payload\\.partitionType",
            ".*avroSchema",
            ".*icebergSchema",
            ".*schema",
            ".*fromProjectionPos")
        .isEqualTo(event);
  }

  @Test
  public void testRewriteCompleteFailedSerialization() {
    Event event =
        new Event(
            "cg-connector",
            new RewriteComplete(
                EventTestUtil.SPEC.partitionType(),
                UUID.randomUUID(),
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                UUID.randomUUID(),
                0,
                "0",
                RewriteComplete.STATUS_FAILED,
                Arrays.asList(),
                0,
                1,
                RewriteComplete.FAILURE_PERMANENT));

    Event result = AvroUtil.decode(AvroUtil.encode(event));

    RewriteComplete payload = (RewriteComplete) result.payload();
    assertThat(payload.failureKind()).isEqualTo(RewriteComplete.FAILURE_PERMANENT);
    assertThat(payload.status()).isEqualTo(RewriteComplete.STATUS_FAILED);
    assertThat(payload.dataFiles()).isEmpty();

    // a worker of an earlier version sends no kind, and its failure defaults to retryable
    Event withoutKind =
        new Event(
            "cg-connector",
            new RewriteComplete(
                EventTestUtil.SPEC.partitionType(),
                UUID.randomUUID(),
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), UUID.randomUUID()),
                UUID.randomUUID(),
                0,
                "0",
                RewriteComplete.STATUS_FAILED,
                Arrays.asList(),
                0,
                1));
    RewriteComplete retryable =
        (RewriteComplete) AvroUtil.decode(AvroUtil.encode(withoutKind)).payload();
    assertThat(retryable.failureKind()).isEqualTo(RewriteComplete.FAILURE_RETRYABLE);
  }

  @Test
  public void testDataCompleteWithoutTaskIdIsReadByAReaderWithoutIt() throws IOException {
    // rolling upgrade in merge-on-read: a worker of this version, which sends no task id outside
    // copy-on-write, and a coordinator one version behind
    UUID commitId = UUID.randomUUID();
    List<TopicPartitionOffset> assignments =
        Arrays.asList(
            new TopicPartitionOffset("topic", 1, 1L, EventTestUtil.now()),
            new TopicPartitionOffset("topic", 2, null, null));
    byte[] data =
        AvroUtil.encode(new Event("cg-connector", new DataComplete(commitId, assignments)));

    // an older reader does not skip a field it does not know: the Avro reader asks the record for
    // each field of the writer's schema before setting it, and get() throws on an unknown id
    assertThat(schemaOf(data)).isEqualTo(schemaOf(readResource(DATA_COMPLETE_BEFORE_TASK_ID)));

    Event result =
        AvroUtil.decode(readPayloadAs(data, DataComplete.class, LegacyDataComplete.class));

    assertThat(result.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    assertThat(result.payload()).isInstanceOf(LegacyDataComplete.class);
    assertThat(result.payload())
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(new LegacyDataComplete(commitId, assignments));
  }

  @Test
  public void testDataCompleteWrittenBeforeTaskIdIsRead() throws IOException {
    // rolling upgrade: a worker does not send task_id yet, the coordinator already knows it
    Event result = AvroUtil.decode(readResource(DATA_COMPLETE_BEFORE_TASK_ID));

    assertThat(result.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    assertThat(result.groupId()).isEqualTo("cg-connector");
    assertThat(result.payload()).isInstanceOf(DataComplete.class);
    assertThat(((DataComplete) result.payload()).taskId()).isNull();
    assertThat(result.payload())
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(".*avroSchema")
        .isEqualTo(
            new DataComplete(
                DATA_COMPLETE_BEFORE_TASK_ID_COMMIT,
                Arrays.asList(
                    new TopicPartitionOffset(
                        "topic", 1, 1L, DATA_COMPLETE_BEFORE_TASK_ID_TIMESTAMP),
                    new TopicPartitionOffset("topic", 2, null, null))));
  }

  /**
   * Returns the encoded event as a connector whose payload class is {@code reader} would receive
   * it: the decoder picks the class by the record name in the schema the event carries.
   */
  private static byte[] readPayloadAs(byte[] encoded, Class<?> writer, Class<?> reader)
      throws IOException {
    byte[] header = new byte[2];
    String schema;
    byte[] datum;
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
      in.readFully(header);
      schema = in.readUTF();
      datum = in.readAllBytes();
    }

    String writerName = "\"" + writer.getSimpleName() + "\"";
    assertThat(schema).contains(writerName);

    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes)) {
      out.write(header);
      out.writeUTF(schema.replace(writerName, "\"" + reader.getSimpleName() + "\""));
      out.write(datum);
      out.flush();
      return bytes.toByteArray();
    }
  }

  private static String schemaOf(byte[] encoded) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
      in.skipNBytes(2);
      return in.readUTF();
    }
  }

  private static byte[] readResource(String name) throws IOException {
    try (InputStream in = TestEventSerialization.class.getResourceAsStream(name)) {
      assertThat(in).as("test resource %s", name).isNotNull();
      return in.readAllBytes();
    }
  }
}
