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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.BoundMethod;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CatalogUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogUtils.class.getName());
  static final List<String> HADOOP_CONF_FILES =
      ImmutableList.of(
          "core-site.xml",
          "hdfs-default.xml",
          "hdfs-site.xml",
          "hive-site.xml",
          "ozone-default.xml",
          "ozone-site.xml");

  static Catalog loadCatalog(IcebergSinkConfig config) {
    return CatalogUtil.buildIcebergCatalog(
        config.catalogName(), config.catalogProps(), loadHadoopConfig(config));
  }

  // use reflection here to avoid requiring Hadoop as a dependency
  private static Object loadHadoopConfig(IcebergSinkConfig config) {
    Class<?> configClass =
        DynClasses.builder().impl("org.apache.hadoop.conf.Configuration").orNull().build();

    if (configClass == null) {
      LOG.info("Hadoop not found on classpath, not creating Hadoop config");
      return null;
    }

    try {
      Object result = DynConstructors.builder().hiddenImpl(configClass).build().newInstance();
      BoundMethod addResourceMethod =
          DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
      BoundMethod setMethod =
          DynMethods.builder("set").impl(configClass, String.class, String.class).build(result);

      //  load any config files in the specified config directory
      String hadoopConfDir = config.hadoopConfDir();

      Function<String, Path> resourcePathResolver =
          hadoopConfDir != null
              ? resource -> Paths.get(hadoopConfDir, resource)
              : CatalogUtils::getClasspathResource;

      HADOOP_CONF_FILES.forEach(
          resource -> loadResource(resource, resourcePathResolver, addResourceMethod));

      // set any Hadoop properties specified in the sink config
      config.hadoopProps().forEach(setMethod::invoke);

      LOG.info("Hadoop config initialized: {}", configClass.getName());
      return result;
    } catch (Exception e) {
      LOG.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  private static void loadResource(
      String resource, Function<String, Path> resourcePathResolver, BoundMethod addResourceMethod) {
    Path path = resourcePathResolver.apply(resource);
    try {
      if (path != null && Files.exists(path) && Files.size(path) > 0) {
        addResourceMethod.invoke(path.toUri().toURL());
      }
    } catch (IOException e) {
      LOG.warn("Error adding Hadoop resource {}, resource was not added", path, e);
    }
  }

  private static Path getClasspathResource(String resourcePath) {
    URL resource =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElseGet(CatalogUtils.class::getClassLoader)
            .getResource(resourcePath);
    return Optional.ofNullable(resource).map(URL::getPath).map(Paths::get).orElse(null);
  }

  private CatalogUtils() {}
}
