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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

public class VariantConverter {
  private final Cache<String, VariantMetadata> metadataCache;

  public VariantConverter(long maxCacheSize, Duration expireAfterAccess) {
    this.metadataCache =
        Caffeine.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterAccess(expireAfterAccess)
            .build();
  }

  public Variant fromMap(Map<String, ?> map) {
    VariantMetadata variantMetadata = metadataForKeys(map.keySet());
    ShreddedObject obj = Variants.object(variantMetadata);

    for (Map.Entry<String, ?> e : map.entrySet()) {
      obj.put(e.getKey(), toVariantValue(e.getValue()));
    }

    return Variant.of(variantMetadata, obj);
  }

  public VariantValue toVariantValue(Object value) {
    VariantValue primitive = toPrimitiveVariantValue(value);
    if (primitive != null) {
      return primitive;
    }
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, ?> map = (Map<String, ?>) value;
      return toObjectVariantValue(map);
    }
    if (value instanceof List) {
      return toArrayVariantValue((List<?>) value);
    }
    return Variants.of(value.toString());
  }

  private VariantValue toPrimitiveVariantValue(Object value) {
    if (value == null) {
      return Variants.ofNull();
    } else if (value instanceof Boolean) {
      return Variants.of((boolean) value);
    } else if (value instanceof Integer) {
      return Variants.of((int) value);
    } else if (value instanceof Long) {
      return Variants.of((long) value);
    } else if (value instanceof Float) {
      return Variants.of((float) value);
    } else if (value instanceof Double) {
      return Variants.of((double) value);
    } else if (value instanceof BigDecimal) {
      return Variants.of((BigDecimal) value);
    } else if (value instanceof String) {
      return Variants.of((String) value);
    } else {
      return null;
    }
  }

  private VariantValue toObjectVariantValue(Map<String, ?> map) {
    VariantMetadata variantMetadata = metadataForKeys(map.keySet());
    ShreddedObject obj = Variants.object(variantMetadata);
    map.forEach((k, v) -> obj.put(k, toVariantValue(v)));
    return obj;
  }

  private VariantValue toArrayVariantValue(List<?> list) {
    ValueArray arr = Variants.array();
    for (Object e : list) {
      arr.add(toVariantValue(e));
    }
    return arr;
  }

  private VariantMetadata metadataForKeys(Collection<String> keys) {
    List<String> names = Lists.newArrayListWithExpectedSize(keys.size());
    for (String key : keys) {
      if (key != null) {
        names.add(key);
      }
    }
    Collections.sort(names);
    String key = getUniqueCacheKey(names);
    return metadataCache.get(key, notUsed -> Variants.metadata(names));
  }

  private static String getUniqueCacheKey(List<String> sortedNames) {
    return String.join("\u0001", sortedNames);
  }
}
