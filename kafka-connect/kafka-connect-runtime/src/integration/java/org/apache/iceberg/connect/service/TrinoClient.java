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
import static org.apache.iceberg.connect.service.DockerClient.DOCKER_CLIENT;
import static org.apache.iceberg.connect.service.DockerClient.exec;
import static org.apache.iceberg.connect.service.DockerClient.getContainer;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.Container;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.connect.service.DockerClient.ExecResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Runs SQL in the Trino container of the stand through {@code docker exec ... trino --execute}. The
 * container is created with the rest of the stand by the compose file; the first query waits for
 * its healthcheck, so tests that never call Trino do not wait for it.
 */
public class TrinoClient {
  private static final String TRINO_IMAGE = "trinodb/trino";
  private static final String TRINO_SERVICE = "trino";
  private static final String CATALOG = "iceberg";
  private static final Duration READY_TIMEOUT = Duration.ofMinutes(3);
  private static final long READY_POLL_MILLIS = 2_000;

  private static volatile boolean ready;

  private TrinoClient() {}

  /**
   * Executes {@code sql} in the {@code iceberg} catalog and returns the result rows, each row a
   * list of cell values. Cells are the TSV text of the Trino CLI with its escape sequences ({@code
   * \t}, {@code \n}, {@code \\} and the like) expanded (a null cell is an empty string, so it
   * cannot be told from an empty string); statements without a result set return no rows. A
   * non-zero exit code of the CLI throws with its output.
   */
  public static List<List<String>> query(String sql) throws InterruptedException {
    Container container = awaitReady();
    ExecResult result =
        exec(
            container,
            null,
            "trino",
            "--catalog",
            CATALOG,
            "--output-format",
            "TSV",
            "--execute",
            sql);
    if (result.failed()) {
      throw new RuntimeException(
          format(
              "Trino query %s failed with exit code %s and error: %s",
              sql, result.exitCode(), result.combined()));
    }
    return parseTsv(result.stdout());
  }

  static List<List<String>> parseTsv(String output) {
    List<List<String>> rows = Lists.newArrayList();
    if (output.isEmpty()) {
      return rows;
    }
    String[] lines = output.split("\n", -1);
    int count = lines[lines.length - 1].isEmpty() ? lines.length - 1 : lines.length;
    for (int i = 0; i < count; i++) {
      String line =
          lines[i].endsWith("\r") ? lines[i].substring(0, lines[i].length() - 1) : lines[i];
      List<String> cells = Lists.newArrayList();
      for (String cell : line.split("\t", -1)) {
        cells.add(unescape(cell));
      }
      rows.add(cells);
    }
    return rows;
  }

  /** Reverts the escaping of the Trino CLI TSV printer; unknown sequences are kept as they are. */
  static String unescape(String cell) {
    if (cell.indexOf('\\') < 0) {
      return cell;
    }
    StringBuilder result = new StringBuilder(cell.length());
    int pos = 0;
    while (pos < cell.length()) {
      char current = cell.charAt(pos);
      if (current == '\\' && pos + 1 < cell.length()) {
        char next = cell.charAt(pos + 1);
        String unescaped = unescaped(next);
        result.append(unescaped != null ? unescaped : "" + current + next);
        pos += 2;
      } else {
        result.append(current);
        pos++;
      }
    }
    return result.toString();
  }

  private static String unescaped(char escaped) {
    switch (escaped) {
      case 't':
        return "\t";
      case 'n':
        return "\n";
      case 'r':
        return "\r";
      case 'b':
        return "\b";
      case 'f':
        return "\f";
      case '0':
        return "\0";
      case '\\':
        return "\\";
      default:
        return null;
    }
  }

  private static Container awaitReady() throws InterruptedException {
    long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
    String status = null;
    while (System.nanoTime() < deadline) {
      try {
        Container container = getContainer(TRINO_IMAGE, TRINO_SERVICE);
        if (ready) {
          return container;
        }
        InspectContainerResponse inspect =
            DOCKER_CLIENT.inspectContainerCmd(container.getId()).exec();
        ContainerState state = inspect.getState();
        status = state.getHealth() == null ? null : state.getHealth().getStatus();
        if ("healthy".equals(status)) {
          ready = true;
          return container;
        }
      } catch (NoSuchElementException e) {
        status = "no running trino container";
      }
      Thread.sleep(READY_POLL_MILLIS);
    }
    throw new IllegalStateException(
        format(
            "Trino did not become healthy in %s, last health status: %s", READY_TIMEOUT, status));
  }
}
