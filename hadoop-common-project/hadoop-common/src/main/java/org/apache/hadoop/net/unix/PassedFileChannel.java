/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net.unix;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

/**
 * A wrapper for FileChannel that is created using FileDescriptor received from
 * DataNode side. As discussed in HDFS-8562, as there is no public Java API yet
 * to open a FileChannel via a FileDescriptor, this tries to do it by calling
 * private method existing in later JDK7 and JDK8 by reflection. In case it does
 * not work due to any error or exception in the way, fallback to using
 * FileInputStream as before, but still use this class as a wrapper. Note it's
 * named as PassedFileChannel and not expected to be used as a generic wrapper.
 */
public class PassedFileChannel implements Closeable {
  private static final String FCHANNEL_IMPL_CLASS =
      "sun.nio.ch.FileChannelImpl";

  // Cache and reuse
  private static Method open5Args; // The method with 5 parameters
  private static Method open4Args; // The method with 4 parameters

  static {
    try {
      Class theClass = Class.forName(FCHANNEL_IMPL_CLASS);

      try {
        // Try this version first, for later JDK7 and JDK8.
        open5Args = theClass.getMethod("open",
            FileDescriptor.class, // fd
            String.class,         // path, may be null
            boolean.class,        // readable
            boolean.class,        // writable
            Object.class          // parent
        );
      } catch (NoSuchMethodException e) {
        // Ignore and try next version
        open4Args = theClass.getMethod("open",
            FileDescriptor.class, // fd
            boolean.class,        // readable
            boolean.class,        // writable
            Object.class          // parent
        );
      }
    } catch (ClassNotFoundException e) {
      // Ignore and will go to fallback.
    } catch (NoSuchMethodException e) {
      // Ignore and will go to fallback.
    }
  }

  private final FileChannel channel;
  private final FileDescriptor fd;
  private FileInputStream holder;

  /**
   * Should be protected, but visible for tests.
   */
  @VisibleForTesting
  public PassedFileChannel(FileChannel channel, FileDescriptor fd) {
    this.channel = channel;
    this.fd = fd;
    this.holder = null;
  }

  /**
   * Should be protected, but visible for tests.
   */
  @VisibleForTesting
  public PassedFileChannel(FileInputStream fis) throws IOException {
    this.channel = fis.getChannel();
    this.fd = fis.getFD();
    this.holder = fis;
  }

  public FileChannel channel() {
    return this.channel;
  }

  public FileDescriptor getFD() {
    return this.fd;
  }

  @Override
  public void close() throws IOException {
    channel.close();
    if (holder != null) {
      holder.close();
    }
  }

  /**
   * Open and wrap a FileChannel from the given file descriptor. The result
   * FileChannel will always be readable, writable according to writable
   * parameter, and not to append.
   * @param fd FileDescriptor
   * @param writable
   * @return a FileChannel wrapper
   */
  public static PassedFileChannel open(FileDescriptor fd,
                                       boolean writable) throws IOException {
    FileChannel channel = null;
    try {
      if (open5Args != null) {
        /**
         * Attempt this private method existing in some JDK:
         * public static FileChannel open(FileDescriptor fd, String path,
         *                  boolean readable, boolean writable, Object parent)
         * Note the path is expected to be nullable.
         */
        channel = (FileChannel) open5Args.invoke(null,
            fd, null, true, writable, null);
      } else if (open4Args != null) {
        /**
         * Attempt this private method existing in some JDK:
         * public static FileChannel open(FileDescriptor fd, boolean readable,
         *                                boolean writable, Object parent)
         */
        channel = (FileChannel) open4Args.invoke(null,
            fd, true, writable, null);
      }
    } catch (IllegalArgumentException e) {
      // Ignore any error due to unexpected JDKs, or JDK versions.
    } catch (InvocationTargetException e) {
      // Ignore
    } catch (IllegalAccessException e) {
      // Ignore
    }

    if (channel != null) {
      return new PassedFileChannel(channel, fd);
    }

    // All errors will fallback to this, as before using FileInputStream.
    FileInputStream fis = new FileInputStream(fd);
    return new PassedFileChannel(fis);
  }
}
