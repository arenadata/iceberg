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
package org.apache.iceberg.connect.service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergTableClient {

  private IcebergTableClient() {}

  public static Table loadCatalogTable(Catalog catalog, TableIdentifier tableIdentifier) {
    return catalog.loadTable(tableIdentifier);
  }

  public static List<org.apache.iceberg.data.Record> extractTableRecords(Table table) {
    return Lists.newArrayList(IcebergGenerics.read(table).build());
  }

  public static List<String> extractTableRecordsAsString(
      Catalog catalog, TableIdentifier tableIdentifier) {
    return extractTableRecords(loadCatalogTable(catalog, tableIdentifier)).stream()
        .map(
            record ->
                StreamSupport.stream(record.struct().fields().spliterator(), false)
                    .map(f -> String.valueOf(record.getField(f.name())))
                    .collect(Collectors.joining("|")))
        .collect(Collectors.toList());
  }
}
