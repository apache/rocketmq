/**
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

/**
 * $Id: SendMessageTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zander
 */
public class BrokerTestHarness {

    protected BrokerController brokerController = null;

    public final String BROKER_NAME = "TestBrokerName";
    protected String brokerAddr = "";
    protected Logger logger = LoggerFactory.getLogger(BrokerTestHarness.class);
    protected BrokerConfig brokerConfig = new BrokerConfig();
    protected NettyServerConfig nettyServerConfig = new NettyServerConfig();
    protected NettyClientConfig nettyClientConfig = new NettyClientConfig();
    protected MessageStoreConfig storeConfig = new MessageStoreConfig();

    @Before
    public void startup() throws Exception {
        brokerConfig.setBrokerName(BROKER_NAME);
        brokerConfig.setBrokerIP1("127.0.0.1");
        nettyServerConfig.setListenPort(10911);
        brokerAddr = brokerConfig.getBrokerIP1() + ":" + nettyServerConfig.getListenPort();
        brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, storeConfig);
        boolean initResult = brokerController.initialize();
        logger.info("Broker Start name:{} addr:{}", brokerConfig.getBrokerName(), brokerController.getBrokerAddr());
        brokerController.start();
    }

    @After
    public void shutdown() throws Exception {
        if (brokerController != null) {
            brokerController.shutdown();
        }
    }
}
