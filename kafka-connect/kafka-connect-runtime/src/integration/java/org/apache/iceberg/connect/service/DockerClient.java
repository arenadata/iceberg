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

import static java.lang.String.format;

import com.github.dockerjava.api.model.Container;
import java.util.List;
import java.util.NoSuchElementException;
import org.testcontainers.DockerClientFactory;

public class DockerClient {
  public static final com.github.dockerjava.api.DockerClient DOCKER_CLIENT =
      DockerClientFactory.instance().client();

  private DockerClient() {}

  public static Container getContainer(String image) {
    List<Container> containers = DOCKER_CLIENT.listContainersCmd().withShowAll(true).exec();
    return containers.stream()
        .filter(container -> container.getImage().contains(image))
        .findFirst()
        .orElseThrow(
            () -> new NoSuchElementException(format("No container named %s is found", image)));
  }
}
