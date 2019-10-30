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
package org.apache.hadoop.io.nativeio;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.PassedFileChannel;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestSharedFileDescriptorFactory {
  static final Log LOG = LogFactory.getLog(TestSharedFileDescriptorFactory.class);

  private static final File TEST_BASE =
      new File(System.getProperty("test.build.data", "/tmp"));

  @Before
  public void setup() throws Exception {
    Assume.assumeTrue(null ==
        SharedFileDescriptorFactory.getLoadingFailureReason());
  }

  @Test(timeout=10000)
  public void testReadAndWrite() throws Exception {
    File path = new File(TEST_BASE, "testReadAndWrite");
    path.mkdirs();
    SharedFileDescriptorFactory factory =
        SharedFileDescriptorFactory.create("woot_",
            new String[] { path.getAbsolutePath() });
    PassedFileChannel fchan =
        factory.createDescriptor("testReadAndWrite", 4096);
    FileOutputStream outStream = new FileOutputStream(fchan.getFD());
    outStream.write(101);
    fchan.channel().position(0);
    byte[] arr = new byte[1];
    IOUtils.readFully(fchan.channel(), arr, 0, arr.length);
    Assert.assertEquals(101, arr[0]);
    outStream.close();
    FileUtil.fullyDelete(path);
  }

  static private void createTempFile(String path) throws Exception {
    FileOutputStream fos = new FileOutputStream(path);
    fos.write(101);
    fos.close();
  }
  
  @Test(timeout=10000)
  public void testCleanupRemainders() throws Exception {
    Assume.assumeTrue(NativeIO.isAvailable());
    Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
    File path = new File(TEST_BASE, "testCleanupRemainders");
    path.mkdirs();
    String remainder1 = path.getAbsolutePath() + 
        Path.SEPARATOR + "woot2_remainder1";
    String remainder2 = path.getAbsolutePath() +
        Path.SEPARATOR + "woot2_remainder2";
    createTempFile(remainder1);
    createTempFile(remainder2);
    SharedFileDescriptorFactory.create("woot2_", 
        new String[] { path.getAbsolutePath() });
    // creating the SharedFileDescriptorFactory should have removed 
    // the remainders
    Assert.assertFalse(new File(remainder1).exists());
    Assert.assertFalse(new File(remainder2).exists());
    FileUtil.fullyDelete(path);
  }
  
  @Test(timeout=60000)
  public void testDirectoryFallbacks() throws Exception {
    File nonExistentPath = new File(TEST_BASE, "nonexistent");
    File permissionDeniedPath = new File("/");
    File goodPath = new File(TEST_BASE, "testDirectoryFallbacks");
    goodPath.mkdirs();
    try {
      SharedFileDescriptorFactory.create("shm_", 
          new String[] { nonExistentPath.getAbsolutePath(),
                          permissionDeniedPath.getAbsolutePath() });
      Assert.fail();
    } catch (IOException e) {
    }
    SharedFileDescriptorFactory factory =
        SharedFileDescriptorFactory.create("shm_", 
            new String[] { nonExistentPath.getAbsolutePath(),
                            permissionDeniedPath.getAbsolutePath(),
                            goodPath.getAbsolutePath() } );
    Assert.assertEquals(goodPath.getAbsolutePath(), factory.getPath());
    FileUtil.fullyDelete(goodPath);
  }
}
