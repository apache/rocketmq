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

import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class ClientLoggerTest {

    public static final String CLIENT_LOG_ROOT = "rocketmq.client.logRoot";
    public static final String LOG_DIR;

    static {
        LOG_DIR = System.getProperty(CLIENT_LOG_ROOT, System.getProperty("user.home") + "/logs/rocketmqlogs");
    }


    @After
    public void cleanFiles() {
        UtilAll.deleteFile(new File(LOG_DIR));
    }

    // FIXME: Workaround for concrete implementation for slf4j, is there any better solution for all slf4j implementations in one class ? 2017/8/1
    @Test
    public void testLog4j2() throws Exception {
        Logger logger = ClientLogger.getLog();

        System.out.println(logger);
        assertEquals("org.apache.logging.slf4j.Log4jLogger", logger.getClass().getName());
    }
}
