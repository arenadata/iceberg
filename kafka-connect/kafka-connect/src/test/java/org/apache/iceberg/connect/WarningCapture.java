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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

/**
 * The WARN lines of the static loggers of the connector while open: the tests log through
 * slf4j-simple, which writes to whatever {@code System.err} is at the time of the line. Everything
 * still reaches the original {@code System.err}.
 */
public final class WarningCapture implements AutoCloseable {
  private static final String CANARY = "WarningCapture canary";

  private final PrintStream original;
  private final ByteArrayOutputStream captured = new ByteArrayOutputStream();

  private WarningCapture() {
    assertThat(LoggerFactory.getILoggerFactory().getClass().getSimpleName())
        .as("the log of the tests goes to System.err through slf4j-simple")
        .isEqualTo("SimpleLoggerFactory");
    this.original = System.err;
    OutputStream tee =
        new OutputStream() {
          @Override
          public void write(int value) {
            original.write(value);
            captured.write(value);
          }

          @Override
          public void write(byte[] bytes, int off, int len) {
            original.write(bytes, off, len);
            captured.write(bytes, off, len);
          }
        };
    System.setErr(new PrintStream(tee, true, StandardCharsets.UTF_8));
    try {
      verifyCanary();
    } catch (RuntimeException | Error e) {
      System.setErr(original);
      throw e;
    }
  }

  /**
   * A WARN of its own must be seen by {@link #lines()}: otherwise the log goes elsewhere than the
   * current {@code System.err} or in another format, and a check of "no WARN" passes in vain. The
   * canary is dropped from the buffer.
   */
  private void verifyCanary() {
    LoggerFactory.getLogger(WarningCapture.class).warn(CANARY);
    assertThat(lines())
        .as("the canary WARN is seen: the log goes to System.err as «[thread] WARN …»")
        .anyMatch(line -> line.endsWith(CANARY));
    captured.reset();
  }

  public static WarningCapture capture() {
    return new WarningCapture();
  }

  /** The WARN lines logged so far: {@code [thread] WARN logger - message}. */
  public List<String> lines() {
    return Arrays.stream(captured.toString(StandardCharsets.UTF_8).split("\\R"))
        .filter(line -> line.startsWith("[") && line.contains("] WARN "))
        .collect(Collectors.toList());
  }

  @Override
  public void close() {
    System.setErr(original);
  }
}
