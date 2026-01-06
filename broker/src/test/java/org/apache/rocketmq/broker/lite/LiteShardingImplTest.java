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

package org.apache.rocketmq.broker.lite;

import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteShardingImplTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private TopicRouteInfoManager topicRouteInfoManager;

    private LiteShardingImpl liteSharding;

    @Before
    public void setUp() {
        liteSharding = new LiteShardingImpl(brokerController, topicRouteInfoManager);
    }

    /**
     * Test normal case: multiple MessageQueues, verify consistent hash selects correct brokerName
     */
    @Test
    public void testShardingByLmqName_NormalCase() {
        // Prepare data
        String parentTopic = "TestTopic";
        String liteTopic = "lite_topic";
        String lmqName = LiteUtil.toLmqName(parentTopic, liteTopic);
        String brokerName1 = "BrokerA";
        String brokerName2 = "BrokerB";

        TopicPublishInfo topicPublishInfo = mock(TopicPublishInfo.class);
        List<MessageQueue> messageQueues = new ArrayList<>();
        MessageQueue mq1 = mock(MessageQueue.class);
        MessageQueue mq2 = mock(MessageQueue.class);
        when(mq1.getBrokerName()).thenReturn(brokerName1);
//        when(mq2.getBrokerName()).thenReturn(brokerName2);
        messageQueues.add(mq1);
        messageQueues.add(mq2);

        when(topicPublishInfo.getMessageQueueList()).thenReturn(messageQueues);
        when(topicRouteInfoManager.tryToFindTopicPublishInfo(parentTopic)).thenReturn(topicPublishInfo);

        // Execute method
        String brokerName = liteSharding.shardingByLmqName(parentTopic, lmqName);

        // Verify consistent hash selected bucket
        int bucket = Hashing.consistentHash(liteTopic.hashCode(), messageQueues.size());
        MessageQueue expectedMq = messageQueues.get(bucket);
        String expectedBrokerName = expectedMq.getBrokerName();

        assertEquals(expectedBrokerName, brokerName);
    }

    /**
     * Test edge case: empty MessageQueue list should return current broker name
     */
    @Test
    public void testShardingByLmqName_EmptyQueueList() {
        String parentTopic = "TestTopic";
        String lmqName = "LmqName2";
        String currentBrokerName = "CurrentBroker";

        BrokerConfig brokerConfig = mock(BrokerConfig.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerConfig.getBrokerName()).thenReturn(currentBrokerName);

        TopicPublishInfo topicPublishInfo = mock(TopicPublishInfo.class);
        when(topicPublishInfo.getMessageQueueList()).thenReturn(new ArrayList<>());
        when(topicRouteInfoManager.tryToFindTopicPublishInfo(parentTopic)).thenReturn(topicPublishInfo);

        String brokerName = liteSharding.shardingByLmqName(parentTopic, lmqName);

        assertEquals(currentBrokerName, brokerName);
    }

    /**
     * Test exception case: tryToFindTopicPublishInfo returns null, should return current broker name
     */
    @Test
    public void testShardingByLmqName_NullTopicPublishInfo() {
        String parentTopic = "TestTopic";
        String lmqName = "LmqName3";
        String currentBrokerName = "CurrentBroker";

        BrokerConfig brokerConfig = mock(BrokerConfig.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerConfig.getBrokerName()).thenReturn(currentBrokerName);

        when(topicRouteInfoManager.tryToFindTopicPublishInfo(parentTopic)).thenReturn(null);

        String brokerName = liteSharding.shardingByLmqName(parentTopic, lmqName);

        assertEquals(currentBrokerName, brokerName);
    }
}
