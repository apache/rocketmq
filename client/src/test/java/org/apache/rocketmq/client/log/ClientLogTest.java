/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.log;

import junit.framework.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Date;

public class ClientLogTest {

    public static final String CLIENT_LOG_ROOT = "rocketmq.client.logRoot";
    public static final String LOG_DIR;

    static {
        LOG_DIR = System.getProperty(CLIENT_LOG_ROOT, "${user.home}/logs/rocketmqlogs");
    }

    @Test
    public void testLog4j2() throws IOException {
        long seek = 0;
        boolean result = false;
        File file = new File(LOG_DIR + File.separator + "rocketmq_client.log");
        if (file.exists()) {
            seek = file.length();
        }
        Class logClass = ClientLogger.getLogClass();
        Assert.assertEquals("org.apache.logging.slf4j.Log4jLoggerFactory", logClass.getName());
        for (int i = 0; i < 10; i++) {
            ClientLogger.getLog().info("testcase testLog4j2 " + new Date());
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(seek);
        String line = randomAccessFile.readLine();
        int idx = 1;
        while (line != null) {
            if (line.contains("testcase testLog4j2")) {
                result = true;
                break;
            }
            line = randomAccessFile.readLine();
            idx++;
            if (idx > 50) {
                break;
            }
        }
        randomAccessFile.close();
        Assert.assertTrue(result);
    }
}
