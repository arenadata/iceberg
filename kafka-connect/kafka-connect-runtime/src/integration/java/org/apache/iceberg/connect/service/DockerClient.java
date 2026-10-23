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

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.ExecStartCmd;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.connect.TestContext;
import org.testcontainers.DockerClientFactory;

public class DockerClient {
  public static final com.github.dockerjava.api.DockerClient DOCKER_CLIENT =
      DockerClientFactory.instance().client();

  private static final String COMPOSE_SERVICE_LABEL = "com.docker.compose.service";
  private static final String COMPOSE_PROJECT_LABEL = "com.docker.compose.project";
  private static final long EXEC_EXIT_TIMEOUT_MILLIS = 5_000;
  private static final long EXEC_EXIT_POLL_MILLIS = 50;

  private DockerClient() {}

  /** Finds the first running container of {@code image}. */
  public static Container getContainer(String image) {
    List<Container> containers = DOCKER_CLIENT.listContainersCmd().exec();
    return containers.stream()
        .filter(container -> container.getImage().contains(image))
        .findFirst()
        .orElseThrow(
            () -> new NoSuchElementException(format("No running container of %s is found", image)));
  }

  /**
   * Finds the running container of {@code image} of the compose service {@code service} of the
   * stand: the compose project is matched too, so a container of the same image and service from
   * another compose project on the host is not picked up. Starts the stand if it is not up yet.
   */
  public static Container getContainer(String image, String service) {
    String project = TestContext.instance().composeProject();
    List<Container> containers = DOCKER_CLIENT.listContainersCmd().exec();
    return containers.stream()
        .filter(container -> container.getImage().contains(image))
        .filter(container -> hasLabel(container, COMPOSE_PROJECT_LABEL, project))
        .filter(container -> hasLabel(container, COMPOSE_SERVICE_LABEL, service))
        .findFirst()
        .orElseThrow(
            () ->
                new NoSuchElementException(
                    format(
                        "No running container of %s for service %s of compose project %s is found",
                        image, service, project)));
  }

  private static boolean hasLabel(Container container, String label, String value) {
    return container.getLabels() != null && value.equals(container.getLabels().get(label));
  }

  /** Runs {@code cmd} in the container of {@code image}, feeding {@code stdin} if not null. */
  public static ExecResult exec(String image, InputStream stdin, String... cmd)
      throws InterruptedException {
    return exec(getContainer(image), stdin, cmd);
  }

  /** Runs {@code cmd} in {@code container}, feeding {@code stdin} if not null. */
  public static ExecResult exec(Container container, InputStream stdin, String... cmd)
      throws InterruptedException {
    ExecCreateCmd create =
        DOCKER_CLIENT
            .execCreateCmd(container.getId())
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd(cmd);
    if (stdin != null) {
      create.withAttachStdin(true);
    }
    ExecCreateCmdResponse exec = create.exec();

    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    ByteArrayOutputStream combined = new ByteArrayOutputStream();
    ExecStartCmd start = DOCKER_CLIENT.execStartCmd(exec.getId());
    if (stdin != null) {
      start.withStdIn(stdin);
    }
    start
        .exec(
            new ResultCallback.Adapter<Frame>() {
              @Override
              public void onNext(Frame frame) {
                try {
                  combined.write(frame.getPayload());
                  if (frame.getStreamType() == StreamType.STDERR) {
                    stderr.write(frame.getPayload());
                  } else {
                    stdout.write(frame.getPayload());
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .awaitCompletion();

    Integer exitCode = awaitExitCode(exec.getId());
    return new ExecResult(exitCode, stdout.toString(), stderr.toString(), combined.toString());
  }

  /**
   * Docker may report the exec as still running for a moment after its streams are closed, with no
   * exit code yet; polls until it stops, and gives up (null) after a short timeout.
   */
  private static Integer awaitExitCode(String execId) throws InterruptedException {
    long deadline = System.nanoTime() + EXEC_EXIT_TIMEOUT_MILLIS * 1_000_000;
    InspectExecResponse inspect = DOCKER_CLIENT.inspectExecCmd(execId).exec();
    while (Boolean.TRUE.equals(inspect.isRunning()) && System.nanoTime() < deadline) {
      Thread.sleep(EXEC_EXIT_POLL_MILLIS);
      inspect = DOCKER_CLIENT.inspectExecCmd(execId).exec();
    }
    return inspect.getExitCode();
  }

  public static InputStream stdin(String text) {
    return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
  }

  public static class ExecResult {
    private final Integer exitCode;
    private final String stdout;
    private final String stderr;
    private final String combined;

    ExecResult(Integer exitCode, String stdout, String stderr, String combined) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
      this.combined = combined;
    }

    public boolean failed() {
      return exitCode == null || exitCode != 0;
    }

    public Integer exitCode() {
      return exitCode;
    }

    public String stdout() {
      return stdout;
    }

    public String stderr() {
      return stderr;
    }

    /** Stdout and stderr in the order they arrived. */
    public String combined() {
      return combined;
    }
  }
}
