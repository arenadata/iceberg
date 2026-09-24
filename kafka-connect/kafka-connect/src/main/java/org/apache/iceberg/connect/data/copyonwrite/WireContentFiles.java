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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Rebuilds a content file read from a manifest into one that can be put on the control topic.
 *
 * <p>A file that came out of a scan is not encodable as it stands. The event schema is built from
 * {@code DataFile.getType(...)}, and the encoder reads values positionally, but a scanned {@code
 * GenericDataFile} answers {@code get(pos)} in the order of the projection the manifest was read
 * with: {@code content} and {@code spec_id} are not in it, so every value after them lands a couple
 * of fields off and the encoder tries to write, say, the file format into {@code spec_id}. Files
 * produced by a writer (the merge-on-read path) carry the full projection and need no copy.
 *
 * <p>The builders below produce a fully populated file, so the copy encodes and decodes cleanly.
 * The delete-file builder's own {@code copy()} drops the three deletion-vector fields, so they are
 * carried over explicitly: losing them would leave a worker reading a DV it cannot locate.
 *
 * <p>A delete file need not be of its data file's spec: an unpartitioned spec's delete applies to
 * the data files of every spec, and a file-scoped one is matched by path. Such a delete keeps its
 * own spec id and travels without a partition tuple, which the event would encode under the data
 * file's partition type. The reader applies a delete by its content, not by its partition.
 */
public final class WireContentFiles {

  private WireContentFiles() {}

  public static DataFile forWire(PartitionSpec spec, DataFile file) {
    return DataFiles.builder(spec).copy(file).build();
  }

  public static DeleteFile forWire(PartitionSpec dataFileSpec, DeleteFile file) {
    PartitionSpec spec =
        file.specId() == dataFileSpec.specId()
            ? dataFileSpec
            : PartitionSpec.builderFor(dataFileSpec.schema()).withSpecId(file.specId()).build();
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec).copy(file);

    if (file.content() == FileContent.EQUALITY_DELETES) {
      List<Integer> equalityFieldIds = file.equalityFieldIds();
      int[] ids = new int[equalityFieldIds.size()];
      for (int i = 0; i < ids.length; i++) {
        ids[i] = equalityFieldIds.get(i);
      }
      builder.ofEqualityDeletes(ids);
    } else {
      builder.ofPositionDeletes();
      if (ContentFileUtil.isDV(file)) {
        builder
            .withReferencedDataFile(file.referencedDataFile())
            .withContentOffset(file.contentOffset())
            .withContentSizeInBytes(file.contentSizeInBytes());
      } else if (file.referencedDataFile() != null) {
        // a file-scoped position delete: the reference is what makes it file scoped
        builder.withReferencedDataFile(file.referencedDataFile());
      }
    }

    return builder.build();
  }
}
