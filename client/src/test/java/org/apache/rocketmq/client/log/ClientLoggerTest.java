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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class ClientLoggerTest {

    public static final String CLIENT_LOG_ROOT = "rocketmq.client.logRoot";
    public static final String LOG_DIR;

    static {
        LOG_DIR = System.getProperty(CLIENT_LOG_ROOT, System.getProperty("user.home") + "/logs/rocketmqlogs");
    }

    @Test
    public void testClientlog() throws IOException {
        InternalLogger logger = ClientLogger.getLog();
        InternalLogger rocketmqCommon = InternalLoggerFactory.getLogger("RocketmqCommon");
        InternalLogger rocketmqRemoting = InternalLoggerFactory.getLogger("RocketmqRemoting");

        for (int i = 0; i < 10; i++) {
            logger.info("testClientlog test {}", i);
            rocketmqCommon.info("common message {}", i, new RuntimeException());
            rocketmqRemoting.info("remoting message {}", i, new RuntimeException());
        }

        String content = MixAll.file2String(LOG_DIR + "/rocketmq_client.log");
        Assert.assertTrue(content.contains("testClientlog"));
        Assert.assertTrue(content.contains("RocketmqClient"));

        Assert.assertTrue(content.contains("RocketmqCommon"));
        Assert.assertTrue(content.contains("RocketmqRemoting"));
    }

    @After
    public void cleanFiles() {
        UtilAll.deleteFile(new File(LOG_DIR));
    }

}
