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

import java.util.ArrayList;
import java.util.List;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AutoSwitchRoleIntegrationTest extends AutoSwitchRoleBase {

    private List<BrokerController> brokerControllerList;
    private List<NamesrvController> namesrvControllerList;
    private BrokerController master;
    private BrokerController slave;

    @Before
    public void init() throws Exception {
        super.initialize();
        this.namesrvControllerList = new ArrayList<>(1);
        this.brokerControllerList = new ArrayList<>(2);
        final String peers = String.format("n0-localhost:%d", 30000);
        for (int i = 0; i < 3; i++) {
            final NettyServerConfig serverConfig = new NettyServerConfig();
            serverConfig.setListenPort(31000 + i);

            final ControllerConfig controllerConfig = buildControllerConfig("n" + i, peers);
            final NamesrvController controller = new NamesrvController(new NamesrvConfig(), serverConfig, new NettyClientConfig(), controllerConfig);
            assertTrue(controller.initialize());
            controller.start();
            this.namesrvControllerList.add(controller);
            System.out.println("Start namesrv controller success");
        }
        Thread.sleep(1000);

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
            this.brokerControllerList.add(brokerController);
            System.out.println("Start controller success");
            Thread.sleep(1000);
            // The first is master
            if (i == 0) {
                assertTrue(brokerController.getReplicasManager().isMasterState());
                this.master = brokerController;
            } else {
                assertFalse(brokerController.getReplicasManager().isMasterState());
                this.slave = brokerController;
            }
        }
        assertNotNull(master);
        assertNotNull(slave);
    }

    @Test
    public void testAppendLog() throws Exception {
        System.out.println("Begin test");
        final MessageStore messageStore = master.getMessageStore();
        putMessage(messageStore);

        // Check slave message
        checkMessage(slave.getMessageStore(), 10, 0);

        Thread.sleep(1000);

        // Check sync state set
        final ReplicasManager replicasManager = master.getReplicasManager();
        final SyncStateSet syncStateSet = replicasManager.getSyncStateSet();
        assertEquals(2, syncStateSet.getSyncStateSet().size());
    }

    @After
    public void shutdown() {
        for (NamesrvController namesrvController : this.namesrvControllerList) {
            namesrvController.shutdown();
        }
        for (BrokerController controller : this.brokerControllerList) {
            controller.shutdown();
        }
        super.destroy();
    }
}
