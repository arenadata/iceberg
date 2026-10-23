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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.events.StagedChangeFile;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
 * The persisted record of one frozen change set: which staged files it froze, at which identifier
 * fields and schema, and the control topic offsets to hold at while it drains.
 *
 * <p>Written once by the coordinator when a change set freezes, before planning starts; read back
 * by a new coordinator to resume a change set it did not start. Immutable once written: a change
 * set that must be abandoned (identifier fields changed mid-drain) is dropped by freezing its
 * staged files into the next change set, not by rewriting this file. The file itself is deleted
 * once that change set first commits, moving the snapshot summary's pointer off it.
 *
 * <p>Encoded as JSON rather than Avro: the content is one small record that never crosses the
 * control topic wire, and every field it needs (a UUID, a handful of ints, a bounded list of {@link
 * StagedChangeFile} descriptors) is already JSON-shaped. Atomicity of one write and restart-safe
 * reads depend on {@code read} returning exactly what {@code write} wrote, not on the byte format.
 */
public final class ChangeSetManifest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * The format version this build writes and reads. Bumped only for a change that is not additive
   * (a field removed, or its meaning changed): a new optional field needs no bump, because a build
   * that does not know it simply does not read it.
   */
  public static final int CURRENT_FORMAT_VERSION = 1;

  private final int formatVersion;
  private final UUID changeSetId;
  private final UUID tableUuid;
  private final long createdAtMs;
  private final int schemaIdAtFreeze;
  private final Set<Integer> identifierFieldIds;
  private final List<StagedChangeFile> stagedFiles;
  private final Set<String> sourceTopics;
  private final Map<Integer, Long> controlOffsets;
  private final OffsetDateTime validThroughTs;

  private ChangeSetManifest(
      int formatVersion,
      UUID changeSetId,
      UUID tableUuid,
      long createdAtMs,
      int schemaIdAtFreeze,
      Set<Integer> identifierFieldIds,
      List<StagedChangeFile> stagedFiles,
      Set<String> sourceTopics,
      Map<Integer, Long> controlOffsets,
      OffsetDateTime validThroughTs) {
    this.formatVersion = formatVersion;
    this.changeSetId = changeSetId;
    this.tableUuid = tableUuid;
    this.createdAtMs = createdAtMs;
    this.schemaIdAtFreeze = schemaIdAtFreeze;
    this.identifierFieldIds = identifierFieldIds;
    this.stagedFiles = stagedFiles;
    this.sourceTopics = sourceTopics;
    this.controlOffsets = controlOffsets;
    this.validThroughTs = validThroughTs;
  }

  /**
   * Freezes a new change set: assigns it an id and records the table state at freeze time.
   *
   * @param validThroughTs how far the data this change set carries is valid: the value of the cycle
   *     that froze it, or {@code null} if that cycle was a partial one
   */
  public static ChangeSetManifest freeze(
      Table table,
      Set<Integer> identifierFieldIds,
      List<StagedChangeFile> stagedFiles,
      Set<String> sourceTopics,
      Map<Integer, Long> controlOffsets,
      OffsetDateTime validThroughTs) {
    return new ChangeSetManifest(
        CURRENT_FORMAT_VERSION,
        UUID.randomUUID(),
        table.uuid(),
        System.currentTimeMillis(),
        table.schema().schemaId(),
        ImmutableSet.copyOf(identifierFieldIds),
        ImmutableList.copyOf(stagedFiles),
        ImmutableSet.copyOf(sourceTopics),
        ImmutableMap.copyOf(controlOffsets),
        validThroughTs);
  }

  public int formatVersion() {
    return formatVersion;
  }

  public UUID changeSetId() {
    return changeSetId;
  }

  public UUID tableUuid() {
    return tableUuid;
  }

  public long createdAtMs() {
    return createdAtMs;
  }

  public int schemaIdAtFreeze() {
    return schemaIdAtFreeze;
  }

  public Set<Integer> identifierFieldIds() {
    return identifierFieldIds;
  }

  public List<StagedChangeFile> stagedFiles() {
    return stagedFiles;
  }

  public Set<String> sourceTopics() {
    return sourceTopics;
  }

  public Map<Integer, Long> controlOffsets() {
    return controlOffsets;
  }

  /**
   * How far the data of this change set is valid, from the cycle that froze it. The exhausting
   * commit of the drain writes it, whichever cycle (and whichever coordinator) that commit falls
   * in; the cycle that resumes a drain froze nothing of its own into it. {@code null} when the
   * freeze happened in a partial cycle, and then no commit of this change set states one.
   */
  public OffsetDateTime validThroughTs() {
    return validThroughTs;
  }

  /**
   * The path a manifest for this change set lives at: {@code
   * <staging>/<tableDir>/<groupDir>/_changesets/changeSetId.json}, under the connector's staging
   * directory as {@link StagedChangeFileWriter#stagingDirectory} spells it.
   */
  public static String location(
      String stagingLocation, TableReference tableReference, String groupId, UUID changeSetId) {
    return String.format(
        "%s/_changesets/%s.json",
        StagedChangeFileWriter.stagingDirectory(stagingLocation, tableReference, groupId),
        changeSetId);
  }

  public void write(FileIO io, String location) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("format-version", formatVersion);
    root.put("changeSetId", changeSetId.toString());
    if (tableUuid != null) {
      root.put("tableUuid", tableUuid.toString());
    }
    root.put("createdAtMs", createdAtMs);
    root.put("schemaIdAtFreeze", schemaIdAtFreeze);

    ArrayNode idFields = root.putArray("identifierFieldIds");
    identifierFieldIds.stream().sorted().forEach(idFields::add);

    ArrayNode topics = root.putArray("sourceTopics");
    sourceTopics.forEach(topics::add);

    if (validThroughTs != null) {
      root.put("validThroughTs", validThroughTs.toString());
    }

    ObjectNode offsets = root.putObject("controlOffsets");
    controlOffsets.forEach((partition, offset) -> offsets.put(String.valueOf(partition), offset));

    ArrayNode files = root.putArray("stagedFiles");
    stagedFiles.forEach(file -> files.add(toJson(file)));

    try (OutputStream out = io.newOutputFile(location).createOrOverwrite()) {
      MAPPER.writeValue(out, root);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write change-set manifest: " + location, e);
    }
  }

  /**
   * Reads back what {@link #write} wrote.
   *
   * @throws org.apache.iceberg.exceptions.NotFoundException if there is no file at the location
   * @throws IllegalArgumentException if the file is not a manifest: not JSON, or a field missing
   * @throws UncheckedIOException if storage fails to serve it
   */
  public static ChangeSetManifest read(FileIO io, String location) {
    JsonNode root;
    try (InputStream in = io.newInputFile(location).newStream()) {
      root = MAPPER.readTree(in);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Not a change-set manifest: " + location, e);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read change-set manifest: " + location, e);
    }

    try {
      return fromJsonRoot(root);
    } catch (RuntimeException e) {
      // nothing here does IO: whatever fails is the content, a field missing or of the wrong shape
      throw new IllegalArgumentException("Not a change-set manifest: " + location, e);
    }
  }

  private static ChangeSetManifest fromJsonRoot(JsonNode root) {
    Set<Integer> identifierFieldIds = Sets.newLinkedHashSet();
    root.get("identifierFieldIds").forEach(node -> identifierFieldIds.add(node.asInt()));

    Set<String> topicsSet = Sets.newLinkedHashSet();
    root.get("sourceTopics").forEach(node -> topicsSet.add(node.asText()));

    Map<Integer, Long> controlOffsets = Maps.newHashMap();
    Iterator<Map.Entry<String, JsonNode>> offsetFields = root.get("controlOffsets").fields();
    while (offsetFields.hasNext()) {
      Map.Entry<String, JsonNode> entry = offsetFields.next();
      controlOffsets.put(Integer.valueOf(entry.getKey()), entry.getValue().asLong());
    }

    List<StagedChangeFile> stagedFiles = Lists.newArrayList();
    root.get("stagedFiles").forEach(node -> stagedFiles.add(fromJson(node)));

    return new ChangeSetManifest(
        root.get("format-version").asInt(),
        UUID.fromString(root.get("changeSetId").asText()),
        root.hasNonNull("tableUuid") ? UUID.fromString(root.get("tableUuid").asText()) : null,
        root.get("createdAtMs").asLong(),
        root.get("schemaIdAtFreeze").asInt(),
        ImmutableSet.copyOf(identifierFieldIds),
        ImmutableList.copyOf(stagedFiles),
        ImmutableSet.copyOf(topicsSet),
        ImmutableMap.copyOf(controlOffsets),
        root.hasNonNull("validThroughTs")
            ? OffsetDateTime.parse(root.get("validThroughTs").asText())
            : null);
  }

  private static ObjectNode toJson(StagedChangeFile file) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("location", file.location());
    node.put("fileSizeBytes", file.fileSizeBytes());
    node.put("recordCount", file.recordCount());
    node.put("schemaId", file.schemaId());
    node.put("formatVersion", file.formatVersion());
    node.set("lowerBounds", boundsToJson(file.lowerBounds()));
    node.set("upperBounds", boundsToJson(file.upperBounds()));
    ArrayNode idFields = node.putArray("identifierFieldIds");
    file.identifierFieldIds().forEach(idFields::add);
    return node;
  }

  private static ObjectNode boundsToJson(Map<Integer, ByteBuffer> bounds) {
    ObjectNode node = MAPPER.createObjectNode();
    if (bounds == null) {
      return node;
    }
    bounds.forEach((fieldId, value) -> node.put(String.valueOf(fieldId), encode(value)));
    return node;
  }

  private static StagedChangeFile fromJson(JsonNode node) {
    // a file entry without the array names no identifier fields: the freeze accepts it under none
    List<Integer> identifierFieldIds = Lists.newArrayList();
    JsonNode idFields = node.get("identifierFieldIds");
    if (idFields != null) {
      idFields.forEach(id -> identifierFieldIds.add(id.asInt()));
    }

    return new StagedChangeFile(
        node.get("location").asText(),
        node.get("fileSizeBytes").asLong(),
        node.get("recordCount").asLong(),
        node.get("schemaId").asInt(),
        node.get("formatVersion").asInt(),
        boundsFromJson(node.get("lowerBounds")),
        boundsFromJson(node.get("upperBounds")),
        identifierFieldIds);
  }

  private static Map<Integer, ByteBuffer> boundsFromJson(JsonNode node) {
    Map<Integer, ByteBuffer> bounds = Maps.newHashMap();
    if (node == null) {
      return bounds;
    }
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      bounds.put(Integer.valueOf(entry.getKey()), decode(entry.getValue().asText()));
    }
    return bounds;
  }

  private static String encode(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static ByteBuffer decode(String encoded) {
    return ByteBuffer.wrap(Base64.getDecoder().decode(encoded));
  }

  /**
   * Whether resuming this change set with {@code currentIdentifierFieldIds} is safe.
   *
   * <p>If the identifier field set itself changed since freeze ({@code ALTER TABLE ... SET
   * IDENTIFIER FIELDS}, or a restart with a different {@code id-columns}) the cursor, keyed by the
   * old fields, is not interpretable against the new ones. The caller drops the change set whole
   * rather than continue.
   */
  public boolean identifierFieldsMatch(Set<Integer> currentIdentifierFieldIds) {
    Preconditions.checkNotNull(
        currentIdentifierFieldIds, "Current identifier fields cannot be null");
    return identifierFieldIds.equals(ImmutableSet.copyOf(currentIdentifierFieldIds));
  }

  /**
   * Whether this build understands {@link #formatVersion}. A manifest a newer build wrote under a
   * version this one does not know cannot be trusted to resume: the evolution rules make an
   * additive change safe to skip over, but nothing guarantees a given change followed them, and
   * nothing here can tell the two cases apart after the fact. The caller drops the change set whole
   * rather than continue, exactly as it does for a mismatched identifier field set.
   */
  public boolean formatVersionSupported() {
    return formatVersion == CURRENT_FORMAT_VERSION;
  }

  /**
   * This change set's identifier fields, resolved against {@code tableSchema} and ordered by
   * ascending field id: the same order {@link #cursorType} builds its struct type in, and the order
   * every key of every slice this change set produces is built in.
   */
  public List<NestedField> identifierFields(Schema tableSchema) {
    return IdentifierKeys.orderedFields(tableSchema, identifierFieldIds);
  }

  /**
   * The struct type the change-set cursor is serialized against with {@code SingleValueParser}: one
   * field per identifier column, in {@link #identifierFields} order. Keyed by field id, so it is
   * stable across the column renames and additions a cursor must survive.
   */
  public StructType cursorType(Schema tableSchema) {
    return IdentifierKeys.keyType(identifierFields(tableSchema));
  }
}
