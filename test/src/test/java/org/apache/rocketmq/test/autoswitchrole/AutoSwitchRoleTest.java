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
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AutoSwitchRoleTest extends AutoSwitchRoleBase {

    private List<BrokerController> brokerControllerList;
    private List<NamesrvController> namesrvControllerList;

    @Test
    public void init() throws Exception {
        this.namesrvControllerList = new ArrayList<>(3);
        this.brokerControllerList = new ArrayList<>(3);
        final String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", 30000, 30001, 30002);
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
        Thread.sleep(4000);

        final String namesrvAddress = "127.0.0.1:31000;127.0.0.1:31001;127.0.0.1:31002";
        for (int i = 0; i < 1; i++) {
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
        }
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
