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

package org.apache.rocketmq.proxy.processor;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReceiptHandleProcessorTest extends BaseProcessorTest {
    private ReceiptHandleProcessor receiptHandleProcessor;

    private static final ProxyContext PROXY_CONTEXT = ProxyContext.create();
    private static final String GROUP = "group";
    private static final String TOPIC = "topic";
    private static final String BROKER_NAME = "broker";
    private static final int QUEUE_ID = 1;
    private static final String MESSAGE_ID = "messageId";
    private static final long OFFSET = 123L;
    private static final long INVISIBLE_TIME = 100000L;
    private static final int RECONSUME_TIMES = 1;
    private static final String MSG_ID = MessageClientIDSetter.createUniqID();
    private MessageReceiptHandle messageReceiptHandle;

    private final String receiptHandle = ReceiptHandle.builder()
        .startOffset(0L)
        .retrieveTime(0)
        .invisibleTime(INVISIBLE_TIME)
        .reviveQueueId(1)
        .topicType(ReceiptHandle.NORMAL_TOPIC)
        .brokerName(BROKER_NAME)
        .queueId(QUEUE_ID)
        .offset(OFFSET)
        .commitLogOffset(0L)
        .build().encode();

    @Before
    public void setup() {
        PROXY_CONTEXT.withVal(ContextVariable.CLIENT_ID, "channel-id");
        receiptHandleProcessor = new ReceiptHandleProcessor(messagingProcessor);
        Mockito.doNothing().when(messagingProcessor).registerConsumerListener(Mockito.any(ConsumerIdsChangeListener.class));
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, receiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES, INVISIBLE_TIME);
    }

    @Test
    public void testAddReceiptHandle() {
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(new SubscriptionGroupConfig());
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
    }

    @Test
    public void testRenewReceiptHandle() {
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        long newInvisibleTime = 2000L;
        ReceiptHandle newReceiptHandleClass = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(0)
            .invisibleTime(newInvisibleTime)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(BROKER_NAME)
            .queueId(QUEUE_ID)
            .offset(OFFSET)
            .commitLogOffset(0L)
            .build();
        String newReceiptHandle = newReceiptHandleClass.encode();
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        ackResult.setExtraInfo(newReceiptHandle);
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis())))
            .thenReturn(CompletableFuture.completedFuture(ackResult));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.argThat(r -> r.getInvisibleTime() == INVISIBLE_TIME), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.argThat(r -> r.getInvisibleTime() == newInvisibleTime), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
    }

    @Test
    public void testRenewReceiptHandleWhenTimeout() {
        long newInvisibleTime = 0L;
        String newReceiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(0)
            .invisibleTime(newInvisibleTime)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(BROKER_NAME)
            .queueId(QUEUE_ID)
            .offset(OFFSET)
            .commitLogOffset(0L)
            .build().encode();
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, receiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES, newInvisibleTime);
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(groupConfig.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(RECONSUME_TIMES)));
    }

    @Test
    public void testRenewReceiptHandleWhenNotArrivingTime() {
        String newReceiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(INVISIBLE_TIME)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(BROKER_NAME)
            .queueId(QUEUE_ID)
            .offset(OFFSET)
            .commitLogOffset(0L)
            .build().encode();
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, newReceiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES, INVISIBLE_TIME);
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testRemoveReceiptHandle() {
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, MSG_ID, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.removeReceiptHandle(channelId, MSG_ID, receiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testClearGroup() {
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, MSG_ID, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.clearGroup(channelId);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()));
    }
}