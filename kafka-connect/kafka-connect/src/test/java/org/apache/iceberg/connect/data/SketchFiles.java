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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.events.SketchFile;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;

/**
 * What the writer tests need to look at the intermediate Puffin files of {@link
 * ColumnSketchCollector}.
 */
final class SketchFiles {
  private SketchFiles() {}

  /** A mock table with only its {@code FileIO}: all {@code ColumnSketchCollector} touches. */
  static Table tableWith(FileIO io) {
    Table table = mock(Table.class);
    when(table.io()).thenReturn(io);
    return table;
  }

  /** The Puffin file of a writer, read back: file properties and the NDV of each column's blob. */
  static Content read(FileIO io, SketchFile file) {
    assertThat(file).as("the reference to the sketch file").isNotNull();
    try (PuffinReader reader =
        Puffin.read(io.newInputFile(file.location()))
            .withFileSize(file.fileSizeBytes())
            .withFooterSize(file.footerSizeBytes())
            .build()) {
      Content content = new Content(reader.fileMetadata().properties());
      List<BlobMetadata> blobs = reader.fileMetadata().blobs();
      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(blobs)) {
        byte[] data = ByteBuffers.toByteArray(blob.second());
        content.ndv.put(
            blob.first().inputFields().get(0),
            CompactSketch.heapify(Memory.wrap(data)).getEstimate());
      }

      return content;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static final class Content {
    private final Map<String, String> properties;
    private final Map<Integer, Double> ndv = Maps.newTreeMap();

    private Content(Map<String, String> properties) {
      this.properties = properties;
    }

    Map<String, String> properties() {
      return properties;
    }

    /** The estimate of the sketch of each column that has a blob, by field id. */
    Map<Integer, Double> ndv() {
      return ndv;
    }
  }

  /** An in-memory {@code FileIO} that remembers the files created and can refuse sketch files. */
  static class RecordingFileIO extends InMemoryFileIO {
    private final List<String> created = new CopyOnWriteArrayList<>();
    private final List<String> attempted = new CopyOnWriteArrayList<>();
    private volatile boolean failOnStats = false;

    RecordingFileIO failOnStatsFiles() {
      this.failOnStats = true;
      return this;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      if (isStatsFile(path)) {
        attempted.add(path);
      }

      if (failOnStats && isStatsFile(path)) {
        throw new UncheckedIOException(new IOException("cannot create " + path));
      }

      created.add(path);
      return super.newOutputFile(path);
    }

    List<String> statsFiles() {
      return created.stream().filter(RecordingFileIO::isStatsFile).collect(Collectors.toList());
    }

    /** The paths of the files under a {@code stats} directory asked for, failed or not. */
    List<String> statsFilesAttempted() {
      return attempted;
    }

    List<String> otherFiles() {
      return created.stream().filter(path -> !isStatsFile(path)).collect(Collectors.toList());
    }

    private static boolean isStatsFile(String path) {
      return path.contains("/" + ColumnSketchCollector.STATS_DIR + "/");
    }
  }
}
