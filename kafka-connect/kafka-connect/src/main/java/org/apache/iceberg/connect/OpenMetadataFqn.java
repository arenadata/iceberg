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

import java.util.Arrays;
import java.util.stream.Collectors;

final class OpenMetadataFqn {

  private OpenMetadataFqn() {}

  static String build(String... segments) {
    return Arrays.stream(segments)
        .map(OpenMetadataFqn::quoteSegment)
        .collect(Collectors.joining("."));
  }

  static String append(String fqn, String segment) {
    return fqn + "." + quoteSegment(segment);
  }

  private static String quoteSegment(String segment) {
    if (isQuotedSegment(segment)) {
      String decoded = segment.substring(1, segment.length() - 1).replace("\"\"", "\"");
      return needsQuoting(decoded) ? segment : decoded;
    }

    if (!needsQuoting(segment)) {
      return segment;
    }

    return '"' + segment.replace("\"", "\"\"") + '"';
  }

  private static boolean needsQuoting(String segment) {
    return segment.indexOf('.') >= 0 || segment.indexOf('"') >= 0;
  }

  private static boolean isQuotedSegment(String segment) {
    if (segment.length() < 2
        || segment.charAt(0) != '"'
        || segment.charAt(segment.length() - 1) != '"') {
      return false;
    }

    String body = segment.substring(1, segment.length() - 1);
    return body.replace("\"\"", "").indexOf('"') < 0;
  }
}
