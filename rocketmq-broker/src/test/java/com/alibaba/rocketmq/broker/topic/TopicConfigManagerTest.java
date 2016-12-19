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
 * $Id: TopicConfigManagerTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.topic;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * @author shijia.wxr
 */
public class TopicConfigManagerTest {
    @Test
    public void test_flushTopicConfig() throws Exception {
        BrokerController brokerController = new BrokerController(//
                new BrokerConfig(), //
                new NettyServerConfig(), //
                new NettyClientConfig(), //
                new MessageStoreConfig());
        boolean initResult = brokerController.initialize();
        System.out.println("initialize " + initResult);
        brokerController.start();

        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);

        TopicConfig topicConfig =
                topicConfigManager.createTopicInSendMessageMethod("TestTopic_SEND", MixAll.DEFAULT_TOPIC,
                        null, 4, 0);
        assertTrue(topicConfig != null);

        System.out.println(topicConfig);

        for (int i = 0; i < 10; i++) {
            String topic = "UNITTEST-" + i;
            topicConfig =
                    topicConfigManager
                            .createTopicInSendMessageMethod(topic, MixAll.DEFAULT_TOPIC, null, 4, 0);
            assertTrue(topicConfig != null);
        }

        topicConfigManager.persist();

        brokerController.shutdown();
    }
}
