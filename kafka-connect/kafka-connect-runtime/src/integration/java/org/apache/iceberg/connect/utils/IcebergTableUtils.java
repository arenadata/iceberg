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
package org.apache.iceberg.connect.utils;

import static java.lang.String.format;
import static org.apache.iceberg.connect.utils.ConnectorUtils.AWS_ACCESS_KEY;
import static org.apache.iceberg.connect.utils.ConnectorUtils.AWS_SECRET_KEY;
import static org.apache.iceberg.connect.utils.ConnectorUtils.MINIO_PORT;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class IcebergTableUtils {

  public static final Map<String, String> BASE_V3_TABLE_CONFIG = Map.of("format-version", "3");

  private IcebergTableUtils() {}

  public static List<org.apache.iceberg.data.Record> extractTableRecords(Table table) {
    return Lists.newArrayList(IcebergGenerics.read(table).build());
  }

  public static Table loadCatalogTable(Catalog catalog, TableIdentifier tableIdentifier) {
    return catalog.loadTable(tableIdentifier);
  }

  public static final S3Client S3_CLIENT =
      S3Client.builder()
          .endpointOverride(URI.create("http://localhost:" + MINIO_PORT))
          .credentialsProvider(
              StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
          .region(Region.US_EAST_1)
          .forcePathStyle(true)
          .build();

  public static String loadCatalogTableLocation(Table table) {
    return URI.create(table.location()).getPath();
  }

  public static long refreshTableData(Table table) {
    table.refresh();
    return table.currentSnapshot().snapshotId();
  }

  public static List<String> extractTableRecordsAsString(List<Record> records) {
    return records.stream()
        .map(
            record ->
                StreamSupport.stream(record.struct().fields().spliterator(), false)
                    .map(
                        f ->
                            record.getField(f.name()) instanceof Variant
                                ? castVariantFieldToString((Variant) record.getField(f.name()))
                                : String.valueOf(record.getField(f.name())))
                    .collect(Collectors.joining("|")))
        .toList();
  }

  private static String castVariantFieldToString(Variant variant) {
    VariantValue variantVal = variant.value();
    if (variantVal instanceof VariantObject) {
      String allRows =
          StreamSupport.stream(((VariantObject) variant).fieldNames().spliterator(), false)
              .map(fName -> ((VariantObject) variant).get(fName).asPrimitive().get().toString())
              .collect(Collectors.joining(", "));
      return format("Record(%s)", allRows);
    } else {
      return variantVal
          .asPrimitive()
          .get()
          .toString()
          .replace("Struct{", "Record(")
          .replace("}", ")")
          .replaceAll("\\b\\w+=", "");
    }
  }
}
