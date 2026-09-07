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

package org.apache.rocketmq.proxy.service.cluster;

import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.ClusterServiceManager;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceManagerTest {

    @Mock
    private RPCHook rpcHook;

    @Before
    public void setUp() throws Exception {
        System.setProperty("rocketmq.namesrv.addr", "127.0.0.1:9876");
        ConfigurationManager.initEnv();
        ConfigurationManager.initConfig();
    }

    @After
    public void tearDown() {
        System.clearProperty("rocketmq.proxy.accessKey");
        System.clearProperty("rocketmq.proxy.secretKey");
        System.clearProperty("rocketmq.namesrv.addr");
    }

    @Test
    public void testConstructorWithAdminCredentials() {
        System.setProperty("rocketmq.proxy.accessKey", "admin");
        System.setProperty("rocketmq.proxy.secretKey", "admin123");

        ClusterServiceManager manager = new ClusterServiceManager(rpcHook, null);

        assertNotNull(manager);
    }

    @Test
    public void testConstructorWithoutAdminCredentials() {
        System.clearProperty("rocketmq.proxy.accessKey");
        System.clearProperty("rocketmq.proxy.secretKey");

        ClusterServiceManager manager = new ClusterServiceManager(rpcHook, null);

        assertNotNull(manager);
    }
}
