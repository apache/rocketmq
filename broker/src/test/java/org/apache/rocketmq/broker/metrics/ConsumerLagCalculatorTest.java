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

package org.apache.rocketmq.broker.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.PopBufferMergeService;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerLagCalculatorTest {

    private static final String GROUP = "testGroup";
    private static final String TOPIC = "testTopic";

    @Mock
    private BrokerController brokerController;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private ConsumerManager consumerManager;
    @Mock
    private ConsumerOffsetManager offsetManager;
    @Mock
    private ConsumerFilterManager consumerFilterManager;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;
    @Mock
    private MessageStore messageStore;
    @Mock
    private PopMessageProcessor popMessageProcessor;
    @Mock
    private PopBufferMergeService popBufferMergeService;
    @Mock
    private PopLongPollingService popLongPollingService;
    @Mock
    private PopInflightMessageCounter popInflightMessageCounter;

    private final BrokerConfig brokerConfig = new BrokerConfig();

    private ConsumerLagCalculator consumerLagCalculator;

    @Before
    public void setUp() throws Exception {
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(brokerController.getConsumerOffsetManager()).thenReturn(offsetManager);
        when(brokerController.getConsumerFilterManager()).thenReturn(consumerFilterManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);
        when(popMessageProcessor.getPopLongPollingService()).thenReturn(popLongPollingService);
        when(brokerController.getPopInflightMessageCounter()).thenReturn(popInflightMessageCounter);

        consumerLagCalculator = new ConsumerLagCalculator(brokerController);

        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>();
        subscriptionGroupTable.put(GROUP, new SubscriptionGroupConfig());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroupTable);

        ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo(GROUP, ConsumeType.CONSUME_POP,
            MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumerGroupInfo.updateSubscription(Collections.singleton(new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL)));
        when(consumerManager.getConsumerGroupInfo(GROUP, true)).thenReturn(consumerGroupInfo);

        String retryTopic = KeyBuilder.buildPopRetryTopic(TOPIC, GROUP, brokerConfig.isEnableRetryTopicV2());
        TopicConfig topicConfig = new TopicConfig(TOPIC);
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        topicConfig.setWriteQueueNums(1);
        topicConfig.setReadQueueNums(1);
        when(topicConfigManager.selectTopicConfig(TOPIC)).thenReturn(topicConfig);
        TopicConfig retryTopicConfig = new TopicConfig(retryTopic);
        retryTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        retryTopicConfig.setWriteQueueNums(1);
        retryTopicConfig.setReadQueueNums(1);
        when(topicConfigManager.selectTopicConfig(retryTopic)).thenReturn(retryTopicConfig);

        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(10L);
        when(popBufferMergeService.getLatestOffset(anyString(), anyString(), anyInt())).thenReturn(4L);
        when(popInflightMessageCounter.getGroupPopInFlightMessageNum(anyString(), anyString(), anyInt())).thenReturn(2L);
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(1000L);
    }

    @Test
    public void testCalculateLagWithNotifyKeepsRetryResult() {
        brokerConfig.setEnableNotifyBeforePopCalculateLag(true);
        when(popLongPollingService.notifyMessageArriving(eq(TOPIC), eq(-1), eq(GROUP), eq(true),
            isNull(), eq(0L), isNull(), isNull(), any(CommandCallback.class)))
            .thenAnswer(invocation -> {
                // simulate the pop request being processed asynchronously,
                // which fires the callback with all calculated results
                CommandCallback callback = invocation.getArgument(8);
                callback.accept();
                return true;
            });

        List<ConsumerLagCalculator.CalculateLagResult> results = new ArrayList<>();
        consumerLagCalculator.calculateLag(results::add);

        // the retry result of a pop group must not be dropped by the notify callback
        assertThat(results).hasSize(2);
        assertThat(results.get(0).isRetry).isFalse();
        assertThat(results.get(1).isRetry).isTrue();
        // brokerOffset(10) - pullOffset(4) + inFlight(2)
        assertThat(results.get(0).lag).isEqualTo(8L);
        assertThat(results.get(1).lag).isEqualTo(8L);
    }

    @Test
    public void testCalculateLagWithoutNotifyRecordsBothResults() {
        brokerConfig.setEnableNotifyBeforePopCalculateLag(false);

        List<ConsumerLagCalculator.CalculateLagResult> results = new ArrayList<>();
        consumerLagCalculator.calculateLag(results::add);

        assertThat(results).hasSize(2);
        assertThat(results.get(0).isRetry).isFalse();
        assertThat(results.get(1).isRetry).isTrue();
        assertThat(results.get(0).lag).isEqualTo(8L);
        assertThat(results.get(1).lag).isEqualTo(8L);
    }

    @Test
    public void testCalculateLagWhenNotifyNotAcceptedCalculatesDirectly() {
        brokerConfig.setEnableNotifyBeforePopCalculateLag(true);
        // no pop request is pending, so the notify is not accepted
        when(popLongPollingService.notifyMessageArriving(eq(TOPIC), eq(-1), eq(GROUP), eq(true),
            isNull(), eq(0L), isNull(), isNull(), any(CommandCallback.class)))
            .thenReturn(false);

        List<ConsumerLagCalculator.CalculateLagResult> results = new ArrayList<>();
        consumerLagCalculator.calculateLag(results::add);

        // notify failed, results are calculated directly instead of being lost
        assertThat(results).hasSize(2);
        assertThat(results.get(0).isRetry).isFalse();
        assertThat(results.get(1).isRetry).isTrue();
    }
}
