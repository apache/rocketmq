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

package org.apache.rocketmq.broker;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.OverlappingFileLockException;
import java.util.Properties;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.junit.Assert;
import org.junit.Test;

public class BrokerStartupTest {

    private String storePathRootDir = ".";

    @Test
    public void testProperties2SystemEnv() throws NoSuchMethodException, InvocationTargetException,
        IllegalAccessException {
        Properties properties = new Properties();
        Class<BrokerStartup> clazz = BrokerStartup.class;
        Method method = clazz.getDeclaredMethod("properties2SystemEnv", Properties.class);
        method.setAccessible(true);
        System.setProperty("rocketmq.namesrv.domain", "value");
        method.invoke(null, properties);
        Assert.assertEquals("value", System.getProperty("rocketmq.namesrv.domain"));
    }


    @Test
    public void test_checkMQAlreadyStart() throws Exception {

        File file = new File(StorePathConfigHelper.getLockFile(storePathRootDir));
        if (file.exists()) {
            file.delete();
        }
        BrokerStartup brokerStartup = new BrokerStartup();
        Method method = brokerStartup.getClass().getDeclaredMethod("checkMQAlreadyStart", String.class);
        method.setAccessible(true);

        method.invoke(brokerStartup, storePathRootDir);

        try {
            method.invoke(brokerStartup, storePathRootDir);
        } catch (Exception e) {
            Assert.assertEquals(true, e.getCause() instanceof OverlappingFileLockException);
        }

    }

}