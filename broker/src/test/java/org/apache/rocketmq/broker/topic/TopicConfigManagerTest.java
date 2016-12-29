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

/**
 * $Id: TopicConfigManagerTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerTestHarness;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TopicConfigManagerTest extends BrokerTestHarness {
    @Test
    public void testFlushTopicConfig() throws Exception {
        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);

        for (int i = 0; i < 10; i++) {
            String topic = "UNITTEST-" + i;
            TopicConfig topicConfig = topicConfigManager.createTopicInSendMessageMethod(topic, MixAll.DEFAULT_TOPIC, null, 4, 0);
            assertNotNull(topicConfig);
        }
        topicConfigManager.persist();

        topicConfigManager.getTopicConfigTable().clear();

        for (int i = 0; i < 10; i++) {
            String topic = "UNITTEST-" + i;
            TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
            assertNull(topicConfig);
        }
        topicConfigManager.load();
        for (int i = 0; i < 10; i++) {
            String topic = "UNITTEST-" + i;
            TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
            assertNotNull(topicConfig);
            assertEquals(topicConfig.getTopicSysFlag(), 0);
            assertEquals(topicConfig.getReadQueueNums(), 4);
        }
    }
}
