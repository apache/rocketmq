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

package org.apache.rocketmq.client.impl.mqclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MQClientAPITest {

    private NameserverAccessConfig nameserverAccessConfig;
    private final ClientRemotingProcessor clientRemotingProcessor = new DoNothingClientRemotingProcessor(null);
    private final RPCHook rpcHook = null;
    private ScheduledExecutorService scheduledExecutorService;
    private MQClientAPIFactory mqClientAPIFactory;

    @Before
    public void setUp() {
        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("TestScheduledExecutorService", true);
    }

    @After
    public void tearDown() {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    public void testInitWithNamesrvAddr() {
        nameserverAccessConfig = new NameserverAccessConfig("127.0.0.1:9876", "", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );

        assertEquals("127.0.0.1:9876", System.getProperty("rocketmq.namesrv.addr"));
    }

    @Test
    public void testInitWithNamesrvDomain() {
        nameserverAccessConfig = new NameserverAccessConfig("", "test-domain", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );

        assertEquals("test-domain", System.getProperty("rocketmq.namesrv.domain"));
    }

    @Test
    public void testInitThrowsExceptionWhenBothEmpty() {
        nameserverAccessConfig = new NameserverAccessConfig("", "", "");

        RuntimeException exception = assertThrows(RuntimeException.class, () -> new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        ));

        assertEquals("The configuration item NamesrvAddr is not configured", exception.getMessage());
    }

    @Test
    public void testStartCreatesClients() throws Exception {
        nameserverAccessConfig = new NameserverAccessConfig("127.0.0.1:9876", "", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );

        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:123");

        mqClientAPIFactory.start();

        // Assert
        MQClientAPIExt client = mqClientAPIFactory.getClient();
        List<String> nameServerAddressList = client.getNameServerAddressList();
        assertEquals(1, nameServerAddressList.size());
        assertEquals("127.0.0.1:9876", nameServerAddressList.get(0));
    }

    @Test
    public void testOnNameServerAddressChangeUpdatesAllClients() throws Exception {
        nameserverAccessConfig = new NameserverAccessConfig("127.0.0.1:9876", "", "");

        mqClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "TestPrefix",
            2,
            clientRemotingProcessor,
            rpcHook,
            scheduledExecutorService
        );
        mqClientAPIFactory.start();

        // Act
        mqClientAPIFactory.onNameServerAddressChange("new-address0;new-address1");

        MQClientAPIExt client = mqClientAPIFactory.getClient();
        List<String> nameServerAddressList = client.getNameServerAddressList();
        assertEquals(2, nameServerAddressList.size());
        assertTrue(nameServerAddressList.contains("new-address0"));
    }
}
