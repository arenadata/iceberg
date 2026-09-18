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
package org.apache.iceberg.connect.events;

/**
 * The fields of an event written before its payload, read by {@link AvroUtil#decodeHeader(byte[])}
 * when the event as a whole cannot be decoded.
 */
public class EventHeader {

  private final String groupId;
  private final int typeId;

  EventHeader(String groupId, int typeId) {
    this.groupId = groupId;
    this.typeId = typeId;
  }

  public String groupId() {
    return groupId;
  }

  /** Returns the id of the payload type, which may be one this version does not know. */
  public int typeId() {
    return typeId;
  }

  /** Returns the payload type, or null if this version does not know its id. */
  public PayloadType type() {
    for (PayloadType type : PayloadType.values()) {
      if (type.id() == typeId) {
        return type;
      }
    }

    return null;
  }
}
