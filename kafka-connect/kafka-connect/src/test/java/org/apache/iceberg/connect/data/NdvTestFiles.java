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

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.connect.EndlessAvroFile;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/** What the tests of the NDV calculation need to watch and break the reads of Puffin files. */
final class NdvTestFiles {
  private NdvTestFiles() {}

  /** Logger that records formatted messages as {@code LEVEL message}. */
  static Logger recordingLogger(List<String> records) {
    return (Logger)
        Proxy.newProxyInstance(
            Logger.class.getClassLoader(),
            new Class<?>[] {Logger.class},
            (proxy, method, args) -> {
              String name = method.getName();
              if (args != null
                  && args.length > 0
                  && args[0] instanceof String
                  && ImmutableSet.of("warn", "debug", "info", "error", "trace").contains(name)) {
                Object[] formatArgs;
                if (args.length == 2 && args[1] instanceof Object[]) {
                  formatArgs = (Object[]) args[1];
                } else {
                  formatArgs = Arrays.copyOfRange(args, 1, args.length);
                }
                records.add(
                    name.toUpperCase(Locale.ROOT)
                        + " "
                        + MessageFormatter.arrayFormat((String) args[0], formatArgs).getMessage());
              }
              return null;
            });
  }

  /**
   * An in-memory {@code FileIO} that counts the files opened for reading and can make the reads of
   * a file throw.
   */
  static class ReadFileIO extends InMemoryFileIO {
    private final Map<String, Integer> opened = Maps.newConcurrentMap();
    private final Map<String, Supplier<Exception>> failures = Maps.newConcurrentMap();
    private final Map<String, Supplier<RuntimeException>> gone = Maps.newConcurrentMap();
    private final Map<String, Supplier<RuntimeException>> existsFailures = Maps.newConcurrentMap();
    private volatile boolean refuseAll = false;
    private volatile EndlessAvroFile endless = null;

    /** Every read of the file throws the exception: an {@code IOException} or a runtime one. */
    ReadFileIO failReads(String path, Supplier<Exception> failure) {
      failures.put(path, failure);
      return this;
    }

    /**
     * Deletes the file the way a range read of an object store sees it: {@code exists()} is false,
     * but every read throws the exception rather than {@code NotFoundException}.
     */
    ReadFileIO deleteUnderRangeReads(String path, Supplier<RuntimeException> failure) {
      deleteFile(path);
      gone.put(path, failure);
      return this;
    }

    /** {@code exists()} of a file deleted under range reads throws the exception. */
    ReadFileIO failExists(String path, Supplier<RuntimeException> failure) {
      existsFailures.put(path, failure);
      return this;
    }

    ReadFileIO healReads(String path) {
      failures.remove(path);
      return this;
    }

    /** Opening any file for reading fails the test's expectation that nothing is read. */
    ReadFileIO refuseAll() {
      this.refuseAll = true;
      return this;
    }

    /** Serves the file, once armed, without an end. */
    ReadFileIO serve(EndlessAvroFile file) {
      this.endless = file;
      return this;
    }

    int opened(String path) {
      return opened.getOrDefault(path, 0);
    }

    int openedTotal() {
      return opened.values().stream().mapToInt(Integer::intValue).sum();
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path) {
      if (refuseAll) {
        throw new AssertionError("Unexpected read of " + path);
      }

      opened.merge(path, 1, Integer::sum);
      Supplier<RuntimeException> rawFailure = gone.get(path);
      if (rawFailure != null) {
        return new GoneInputFile(path, rawFailure, existsFailures.get(path));
      }

      InputFile file =
          endless == null
              ? super.newInputFile(path)
              : endless.serve(path, super.newInputFile(path));
      Supplier<Exception> failure = failures.get(path);
      return failure == null ? file : new FailingInputFile(file, failure);
    }
  }

  private static class FailingInputFile implements InputFile {
    private final InputFile delegate;
    private final Supplier<Exception> failure;

    private FailingInputFile(InputFile delegate, Supplier<Exception> failure) {
      this.delegate = delegate;
      this.failure = failure;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return failingStream(failure);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }

  private static class GoneInputFile implements InputFile {
    private final String location;
    private final Supplier<RuntimeException> failure;
    private final Supplier<RuntimeException> existsFailure;

    private GoneInputFile(
        String location,
        Supplier<RuntimeException> failure,
        Supplier<RuntimeException> existsFailure) {
      this.location = location;
      this.failure = failure;
      this.existsFailure = existsFailure;
    }

    @Override
    public long getLength() {
      throw failure.get();
    }

    @Override
    public SeekableInputStream newStream() {
      return failingStream(failure);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      if (existsFailure != null) {
        throw existsFailure.get();
      }

      return false;
    }
  }

  private static SeekableInputStream failingStream(Supplier<? extends Exception> failure) {
    return new SeekableInputStream() {
      private long pos = 0L;

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long newPos) {
        this.pos = newPos;
      }

      @Override
      public int read() throws IOException {
        throw failure();
      }

      @Override
      public int read(byte[] bytes, int off, int len) throws IOException {
        throw failure();
      }

      private IOException failure() {
        Exception exception = failure.get();
        if (exception instanceof IOException) {
          return (IOException) exception;
        }

        throw (RuntimeException) exception;
      }
    };
  }
}
