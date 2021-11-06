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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import org.apache.rocketmq.common.MixAll;
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
        {
            properties.put("rmqAddressServerDomain", "value1");
            properties.put("rmqAddressServerSubGroup", "value2");
            method.invoke(null, properties);
            Assert.assertEquals("value1", System.getProperty("rocketmq.namesrv.domain"));
            Assert.assertEquals("value2", System.getProperty("rocketmq.namesrv.domain.subgroup"));
        }
        {
            properties.put("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
            properties.put("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
            method.invoke(null, properties);
            Assert.assertEquals(MixAll.WS_DOMAIN_NAME, System.getProperty("rocketmq.namesrv.domain"));
            Assert.assertEquals(MixAll.WS_DOMAIN_SUBGROUP, System.getProperty("rocketmq.namesrv.domain.subgroup"));
        }


    }
}