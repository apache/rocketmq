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
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReceiptHandleProcessorTest extends BaseProcessorTest {
    ReceiptHandleProcessor receiptHandleProcessor;

    ProxyContext context = ProxyContext.create();
    String group = "group";
    String topic = "topic";
    String brokerName = "broker";
    int queueId = 1;
    String messageId = "messageId";
    long offset = 123L;
    long invisibleTime = 100000L;
    int reconsumeTimes = 1;
    MessageReceiptHandle messageReceiptHandle;

    String receiptHandle = ReceiptHandle.builder()
        .startOffset(0L)
        .retrieveTime(0)
        .invisibleTime(invisibleTime)
        .reviveQueueId(1)
        .topicType(ReceiptHandle.NORMAL_TOPIC)
        .brokerName(brokerName)
        .queueId(queueId)
        .offset(offset)
        .commitLogOffset(0L)
        .build().encode();

    @Before
    public void setup() {
        context.withVal(ContextVariable.CLIENT_ID, "channel-id");
        receiptHandleProcessor = new ReceiptHandleProcessor(messagingProcessor);
        Mockito.doNothing().when(messagingProcessor).registerConsumerListener(Mockito.any(ConsumerIdsChangeListener.class));
        messageReceiptHandle = new MessageReceiptHandle(group, topic, queueId, receiptHandle, messageId, offset,
            reconsumeTimes, invisibleTime);
    }

    @Test
    public void testAddReceiptHandle() {
        String channelId = context.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, receiptHandle, messageReceiptHandle);
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(group))).thenReturn(new SubscriptionGroupConfig());
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(messageId),
                Mockito.eq(group), Mockito.eq(topic), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
    }

    @Test
    public void testRenewReceiptHandle() {
        String channelId = context.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, receiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(group))).thenReturn(groupConfig);
        long newInvisibleTime = 2000L;
        ReceiptHandle newReceiptHandleClass = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(0)
            .invisibleTime(newInvisibleTime)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(offset)
            .commitLogOffset(0L)
            .build();
        String newReceiptHandle = newReceiptHandleClass.encode();
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        ackResult.setExtraInfo(newReceiptHandle);
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(messageId),
            Mockito.eq(group), Mockito.eq(topic), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis())))
            .thenReturn(CompletableFuture.completedFuture(ackResult));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.argThat(r -> r.getInvisibleTime() == invisibleTime), Mockito.eq(messageId),
                Mockito.eq(group), Mockito.eq(topic), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.argThat(r -> r.getInvisibleTime() == newInvisibleTime), Mockito.eq(messageId),
                Mockito.eq(group), Mockito.eq(topic), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
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
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(offset)
            .commitLogOffset(0L)
            .build().encode();
        messageReceiptHandle = new MessageReceiptHandle(group, topic, queueId, receiptHandle, messageId, offset,
            reconsumeTimes, newInvisibleTime);
        String channelId = context.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, newReceiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(group))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(messageId),
                Mockito.eq(group), Mockito.eq(topic), Mockito.eq(groupConfig.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(reconsumeTimes)));
    }

    @Test
    public void testRenewReceiptHandleWhenNotArrivingTime() {
        String newReceiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(invisibleTime)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(offset)
            .commitLogOffset(0L)
            .build().encode();
        messageReceiptHandle = new MessageReceiptHandle(group, topic, queueId, newReceiptHandle, messageId, offset,
            reconsumeTimes, invisibleTime);
        String channelId = context.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, newReceiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(group))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testRemoveReceiptHandle() {
        String channelId = context.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.removeReceiptHandle(channelId, receiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(group))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testClearGroup() {
        String channelId = context.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.clearGroup(channelId);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(group))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(messageId),
                Mockito.eq(group), Mockito.eq(topic), Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()));
    }
}