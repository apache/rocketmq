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

package org.apache.rocketmq.broker.topic;


import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.TopicStorePolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicConfigManagerTest {


    private BrokerController brokerController;
    private TopicConfigManager topicConfigManager;
    private String topicOne = "topicOne";
    private String topicTwo = "topicTwo";

    @Before
    public void init() {
        brokerController = new BrokerController(
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig(),
                new MessageStoreConfig());
        topicConfigManager = new TopicConfigManager(brokerController);
        topicConfigManager.getTopicConfigTable().put(topicOne, new TopicConfig(topicOne, 4, 4, 6, 2));
        topicConfigManager.getTopicConfigTable().put(topicTwo, new TopicConfig(topicTwo, 4, 4, 6));
    }

    @Test
    public void getTopicStorePolicyTest() {

        Map<String, TopicStorePolicy> topicStorePolicyTable = topicConfigManager.getTopicStorePolicy();
        topicConfigManager.getTopicConfigTable().forEach((key, topicConfig) -> {
            if (topicConfig.getExpirationTime() != null) {
                assertThat(topicStorePolicyTable.containsKey(key));
            } else {
                assertThat(!topicStorePolicyTable.containsKey(key));
            }
        });

    }

}
