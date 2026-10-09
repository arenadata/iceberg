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

import java.util.List;
import org.junit.jupiter.api.Test;

public class TestTrinoClient {

  @Test
  public void parsesRowsAndColumns() {
    assertThat(TrinoClient.parseTsv("a\t1\nb\t2\n"))
        .isEqualTo(List.of(List.of("a", "1"), List.of("b", "2")));
  }

  @Test
  public void keepsEmptyCells() {
    assertThat(TrinoClient.parseTsv("\t1\nx\t\n"))
        .isEqualTo(List.of(List.of("", "1"), List.of("x", "")));
  }

  @Test
  public void emptyOutputHasNoRows() {
    assertThat(TrinoClient.parseTsv("")).isEmpty();
  }

  @Test
  public void singleColumnRows() {
    assertThat(TrinoClient.parseTsv("42\n")).isEqualTo(List.of(List.of("42")));
  }

  @Test
  public void toleratesCrLf() {
    assertThat(TrinoClient.parseTsv("a\t1\r\n")).isEqualTo(List.of(List.of("a", "1")));
  }

  @Test
  public void expandsEscapeSequences() {
    assertThat(TrinoClient.parseTsv("a\\tb\tc\\\\d\\ne\n"))
        .isEqualTo(List.of(List.of("a\tb", "c\\d\ne")));
  }

  @Test
  public void keepsUnknownEscapeSequences() {
    assertThat(TrinoClient.parseTsv("a\\xb\\\n")).isEqualTo(List.of(List.of("a\\xb\\")));
  }
}
