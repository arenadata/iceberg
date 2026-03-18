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
package org.apache.iceberg.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

/**
 * SPI interface for customizing Spark ↔ Iceberg type mappings.
 *
 * <p>Implementations allow external libraries (e.g., geometry libraries) to provide custom Spark
 * types for Iceberg types that don't have a native Spark equivalent. For example, a geometry
 * library can map {@link org.apache.iceberg.types.Types.GeometryType} to its own UDT instead of the
 * default {@link org.apache.spark.sql.types.BinaryType}.
 *
 * <p>Implementations are discovered via {@link ServiceLoader}. To register, add a file named {@code
 * META-INF/services/org.apache.iceberg.spark.SparkTypeCustomizer} containing the fully qualified
 * class name of the implementation.
 *
 * <p>Multiple customizers may be registered. They are consulted in order until one returns a
 * non-null result.
 */
public interface SparkTypeCustomizer {

  /**
   * Returns a custom Spark {@link DataType} for the given Iceberg primitive type, or {@code null}
   * to use the default mapping.
   *
   * @param icebergType the Iceberg primitive type to map
   * @return the custom Spark type, or null to use the default
   */
  DataType toSparkType(Type.PrimitiveType icebergType);

  /**
   * Returns the Iceberg {@link Type} for the given Spark {@link UserDefinedType}, or {@code null}
   * if this customizer does not handle the given UDT.
   *
   * @param sparkUdt the Spark UDT to map
   * @return the Iceberg type, or null if not handled
   */
  Type toIcebergType(UserDefinedType<?> sparkUdt);

  /**
   * Returns the Iceberg {@link Type} based on Spark field metadata, or {@code null} if this
   * customizer does not recognize the metadata. This is called when the Spark type is a standard
   * type (e.g., BinaryType) but carries metadata indicating the original type.
   *
   * <p>This method handles the case where Spark's V2 catalog path converts a UDT to its underlying
   * sqlType (e.g., GeometryUDT → BinaryType) before Iceberg sees the schema. The original type
   * information must be preserved via field metadata by the library (e.g., via a Catalyst rule).
   *
   * @param field the Spark StructField with potential type metadata
   * @return the Iceberg type, or null if not handled
   */
  Type toIcebergType(org.apache.spark.sql.types.StructField field);

  /**
   * Returns all registered {@link SparkTypeCustomizer} instances discovered via {@link
   * ServiceLoader}. Uses the thread context classloader to ensure visibility of all jars.
   */
  static List<SparkTypeCustomizer> loadAll() {
    return Holder.get();
  }

  /** Lazy holder with classloader-aware loading. */
  class Holder {
    private static volatile CachedResult cache;

    private Holder() {}

    static List<SparkTypeCustomizer> get() {
      CachedResult current = cache;
      ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
      if (current != null && current.loader == contextLoader) {
        return current.customizers;
      }

      synchronized (Holder.class) {
        current = cache;
        if (current != null && current.loader == contextLoader) {
          return current.customizers;
        }

        List<SparkTypeCustomizer> customizers = new ArrayList<>();
        ServiceLoader<SparkTypeCustomizer> loader =
            ServiceLoader.load(SparkTypeCustomizer.class, contextLoader);
        for (SparkTypeCustomizer customizer : loader) {
          customizers.add(customizer);
        }

        List<SparkTypeCustomizer> result = Collections.unmodifiableList(customizers);
        cache = new CachedResult(result, contextLoader);
        return result;
      }
    }

    private static class CachedResult {
      final List<SparkTypeCustomizer> customizers;
      final ClassLoader loader;

      CachedResult(List<SparkTypeCustomizer> customizers, ClassLoader loader) {
        this.customizers = customizers;
        this.loader = loader;
      }
    }
  }
}
