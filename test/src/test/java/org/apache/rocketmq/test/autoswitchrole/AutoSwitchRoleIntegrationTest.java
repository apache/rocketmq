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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.autoswitchrole;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.hacontroller.ReplicasManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoSwitchRoleIntegrationTest extends AutoSwitchRoleBase {

    private ControllerConfig controllerConfig;
    private NamesrvController namesrvController;

    private BrokerController brokerController1;
    private BrokerController brokerController2;

    private MessageStoreConfig storeConfig1;
    private MessageStoreConfig storeConfig2;
    private BrokerConfig brokerConfig1;
    private BrokerConfig brokerConfig2;
    private NettyServerConfig brokerNettyServerConfig1;
    private NettyServerConfig brokerNettyServerConfig2;


    @Before
    public void init() throws Exception {
        super.initialize();

        // Startup namesrv
        final String peers = String.format("n0-localhost:%d", 30000);
        final NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(31000);

        this.controllerConfig = buildControllerConfig("n0", peers);
        this.namesrvController = new NamesrvController(new NamesrvConfig(), serverConfig, new NettyClientConfig(), controllerConfig);
        assertTrue(namesrvController.initialize());
        namesrvController.start();

        final String namesrvAddress = "127.0.0.1:31000;";
        for (int i = 0; i < 2; i++) {
            final MessageStoreConfig storeConfig = buildMessageStoreConfig("broker" + i, 20000 + i);
            final BrokerConfig brokerConfig = new BrokerConfig();
            brokerConfig.setListenPort(21000 + i);
            brokerConfig.setNamesrvAddr(namesrvAddress);
            brokerConfig.setMetaDataHosts(namesrvAddress);

            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            nettyServerConfig.setListenPort(22000 + i);

            final BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, new NettyClientConfig(), storeConfig);
            assertTrue(brokerController.initialize());
            brokerController.start();
            System.out.println("Start controller success");
            Thread.sleep(1000);
            // The first is master
            if (i == 0) {
                assertTrue(brokerController.getReplicasManager().isMasterState());
                this.brokerController1 = brokerController;
                this.storeConfig1 = storeConfig;
                this.brokerConfig1 = brokerConfig;
                this.brokerNettyServerConfig1 = nettyServerConfig;
            } else {
                assertFalse(brokerController.getReplicasManager().isMasterState());
                this.brokerController2 = brokerController;
                this.storeConfig2 = storeConfig;
                this.brokerConfig2 = brokerConfig;
                this.brokerNettyServerConfig2 = nettyServerConfig;
            }
        }

        // Wait slave connecting to master
        Thread.sleep(15000);
    }

    public void mockData() throws Exception {
        System.out.println("Begin test");
        final MessageStore messageStore = brokerController1.getMessageStore();
        putMessage(messageStore);

        // Check slave message
        checkMessage(brokerController2.getMessageStore(), 10, 0);
    }

    @Test
    public void testCheckSyncStateSet() throws Exception {
        mockData();

        // Check sync state set
        final ReplicasManager replicasManager = brokerController1.getReplicasManager();
        final SyncStateSet syncStateSet = replicasManager.getSyncStateSet();
        assertEquals(2, syncStateSet.getSyncStateSet().size());
    }

    @Test
    public void testChangeMaster() throws Exception {
        mockData();

        // Let master shutdown
        brokerController1.shutdown();
        Thread.sleep(5000);

        // The slave should change to master
        assertTrue(brokerController2.getReplicasManager().isMasterState());
        assertEquals(brokerController2.getReplicasManager().getMasterEpoch(), 2);

        // Restart old master, it should be slave
        brokerController1 = new BrokerController(brokerConfig1, brokerNettyServerConfig1, new NettyClientConfig(), storeConfig1);
        brokerController1.initialize();
        brokerController1.start();

        Thread.sleep(15000);
        assertFalse(brokerController1.getReplicasManager().isMasterState());
        assertEquals(brokerController1.getReplicasManager().getMasterAddress(), brokerController2.getReplicasManager().getLocalAddress());

        // Put another batch messages
        final MessageStore messageStore = brokerController2.getMessageStore();
        putMessage(messageStore);

        // Check slave message
        checkMessage(brokerController1.getMessageStore(), 20, 0);
    }



    @After
    public void shutdown() {
        if (this.brokerController1 != null) {
            this.brokerController1.shutdown();
        }
        if (this.brokerController2 != null) {
            this.brokerController2.shutdown();
        }
        if (this.namesrvController != null) {
            this.namesrvController.shutdown();
        }
        super.destroy();
    }
}
