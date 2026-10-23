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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestRestCatalogSparkClient {

  // output captured from spark-shell in apache/spark:4.0.4 (Scala 2.13.16), script fed via stdin
  private static final String SCALA_213_COMPILE_ERROR =
      "scala>                ^\n       error: not found: value foo\n\nscala> ";

  private static final String SCALA_213_TYPE_MISMATCH =
      "scala>                     ^\n"
          + "       error: type mismatch;\n"
          + "        found   : String(\"a\")\n"
          + "        required: Int\n";

  private static final String SCALA_213_EXCEPTION =
      "scala> java.lang.IllegalStateException: boom\n  ... 42 elided\n\nscala> NDV|a|3\n";

  // silent statement before the failing one, captured from the same image
  private static final String SCALA_213_ASSERT =
      "scala> \nscala> java.lang.AssertionError: assertion failed\n"
          + "  at scala.Predef$.assert(Predef.scala:264)\n  ... 42 elided\n\nscala> ";

  @Test
  public void detectsCompileError() {
    assertThat(RestCatalogSparkClient.hasReplError(SCALA_213_COMPILE_ERROR)).isTrue();
    assertThat(RestCatalogSparkClient.hasReplError(SCALA_213_TYPE_MISMATCH)).isTrue();
    // Scala 2.12 format
    assertThat(
            RestCatalogSparkClient.hasReplError(
                "scala> val x = foo\n<console>:23: error: not found: value foo\n"))
        .isTrue();
  }

  @Test
  public void detectsUncaughtException() {
    assertThat(RestCatalogSparkClient.hasReplError(SCALA_213_EXCEPTION)).isTrue();
    assertThat(
            RestCatalogSparkClient.hasReplError(
                "NDV|a|3\norg.apache.spark.sql.AnalysisException: Table not found\n  at x\n"))
        .isTrue();
    assertThat(RestCatalogSparkClient.hasReplError("java.lang.IllegalStateException: boom\n"))
        .isTrue();
  }

  @Test
  public void detectsExceptionAfterStackedPrompts() {
    assertThat(RestCatalogSparkClient.hasReplError(SCALA_213_ASSERT)).isTrue();
    assertThat(
            RestCatalogSparkClient.hasReplError(
                "scala> scala> java.lang.AssertionError: assertion failed\n  at x\n"))
        .isTrue();
    assertThat(
            RestCatalogSparkClient.hasReplError(
                "scala>      |      | org.apache.spark.sql.AnalysisException: Table not found\n"))
        .isTrue();
    // only the prompt-independent marker is left
    assertThat(RestCatalogSparkClient.hasReplError("scala> scala> boom\n  ... 42 elided\n"))
        .isTrue();
  }

  @Test
  public void ignoresCleanOutputAndInlineMentions() {
    assertThat(RestCatalogSparkClient.hasReplError("scala> NDV|a|3\nres0: Unit = ()\n")).isFalse();
    assertThat(RestCatalogSparkClient.hasReplError("NDV|a|3\nres0: Unit = ()\n")).isFalse();
    assertThat(RestCatalogSparkClient.hasReplError("VALUE|error: none\n")).isFalse();
    assertThat(RestCatalogSparkClient.hasReplError("VALUE|java.lang.Exception is a word\n"))
        .isFalse();
  }
}
