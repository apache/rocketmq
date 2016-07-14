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
 * $Id: ConsumerOffsetManagerTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.offset;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;

import java.util.Random;


/**
 * @author shijia.wxr
 */
public class ConsumerOffsetManagerTest {
    @Test
    public void test_flushConsumerOffset() throws Exception {
        BrokerController brokerController = new BrokerController(//
                new BrokerConfig(), //
                new NettyServerConfig(), //
                new NettyClientConfig(), //
                new MessageStoreConfig());
        boolean initResult = brokerController.initialize();
        System.out.println("initialize " + initResult);
        brokerController.start();

        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);

        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            String group = "DIANPU_GROUP_" + i;
            for (int id = 0; id < 16; id++) {
                consumerOffsetManager.commitOffset(null, group, "TOPIC_A", id,
                        random.nextLong() % 1024 * 1024 * 1024);
                consumerOffsetManager.commitOffset(null, group, "TOPIC_B", id,
                        random.nextLong() % 1024 * 1024 * 1024);
                consumerOffsetManager.commitOffset(null, group, "TOPIC_C", id,
                        random.nextLong() % 1024 * 1024 * 1024);
            }
        }

        consumerOffsetManager.persist();

        brokerController.shutdown();
    }
}
