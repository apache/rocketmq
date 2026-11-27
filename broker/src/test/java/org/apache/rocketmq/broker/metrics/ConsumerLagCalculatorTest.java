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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.longpolling.PopCommandCallback;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.PopBufferMergeService;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerLagCalculatorTest {
    private static final String GROUP = "GID_test";
    private static final String TOPIC = "TopicTest";

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
    @Mock
    private ConsumerGroupInfo consumerGroupInfo;

    private ConsumerLagCalculator calculator;

    @Before
    public void setUp() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setUseStaticSubscription(true);
        brokerConfig.setEnableNotifyBeforePopCalculateLag(true);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(brokerController.getConsumerOffsetManager()).thenReturn(offsetManager);
        when(brokerController.getConsumerFilterManager()).thenReturn(consumerFilterManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        when(brokerController.getPopInflightMessageCounter()).thenReturn(popInflightMessageCounter);
        when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);
        when(popMessageProcessor.getPopLongPollingService()).thenReturn(popLongPollingService);
        calculator = spy(new ConsumerLagCalculator(brokerController));

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(GROUP);
        groupConfig.setSubscriptionDataSet(Collections.singleton(
            new SimpleSubscriptionData(TOPIC, "TAG", "*", 0L)
        ));
        ConcurrentHashMap<String, SubscriptionGroupConfig> groupTable = new ConcurrentHashMap<>();
        groupTable.put(GROUP, groupConfig);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(groupTable);
        when(consumerManager.getConsumerGroupInfo(GROUP, true)).thenReturn(consumerGroupInfo);
        when(consumerGroupInfo.getConsumeType()).thenReturn(ConsumeType.CONSUME_POP);

        TopicConfig topicConfig = new TopicConfig(TOPIC);
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(topicConfig);
    }

    @Test
    public void shouldRecordNormalAndRetryLagFromAsyncPopCallback() {
        doAnswer(invocation -> {
            Consumer<ConsumerLagCalculator.CalculateLagResult> recorder =
                invocation.getArgument(1);
            ConsumerLagCalculator.CalculateLagResult normal =
                new ConsumerLagCalculator.CalculateLagResult(GROUP, TOPIC, false);
            normal.lag = 3;
            ConsumerLagCalculator.CalculateLagResult retry =
                new ConsumerLagCalculator.CalculateLagResult(GROUP, TOPIC, true);
            retry.lag = 7;
            recorder.accept(normal);
            recorder.accept(retry);
            return null;
        }).when(calculator).calculate(any(ConsumerLagCalculator.ProcessGroupInfo.class), any());

        doAnswer(invocation -> {
            PopCommandCallback callback = invocation.getArgument(8);
            callback.accept();
            return true;
        }).when(popLongPollingService).notifyMessageArriving(
            anyString(), anyInt(), anyString(), anyBoolean(), nullable(Long.class),
            anyLong(), nullable(byte[].class), nullable(Map.class), any(PopCommandCallback.class)
        );

        List<ConsumerLagCalculator.CalculateLagResult> results = new ArrayList<>();
        calculator.calculateLag(results::add);

        assertEquals(2, results.size());
        assertFalse(results.get(0).isRetry);
        assertEquals(3, results.get(0).lag);
        assertTrue(results.get(1).isRetry);
        assertEquals(7, results.get(1).lag);
    }
}
