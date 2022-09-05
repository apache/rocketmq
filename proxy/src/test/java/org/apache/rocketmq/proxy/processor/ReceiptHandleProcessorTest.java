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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReceiptHandleProcessorTest extends BaseProcessorTest {
    private ReceiptHandleProcessor receiptHandleProcessor;

    private static final ProxyContext PROXY_CONTEXT = ProxyContext.create();
    private static final String GROUP = "group";
    private static final String TOPIC = "topic";
    private static final String BROKER_NAME = "broker";
    private static final int QUEUE_ID = 1;
    private static final String MESSAGE_ID = "messageId";
    private static final long OFFSET = 123L;
    private static final long INVISIBLE_TIME = 60000L;
    private static final int RECONSUME_TIMES = 1;
    private static final String MSG_ID = MessageClientIDSetter.createUniqID();
    private MessageReceiptHandle messageReceiptHandle;

    private String receiptHandle;

    @Before
    public void setup() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        receiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis() - INVISIBLE_TIME + config.getRenewAheadTimeMillis() - 5)
            .invisibleTime(INVISIBLE_TIME)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(BROKER_NAME)
            .queueId(QUEUE_ID)
            .offset(OFFSET)
            .commitLogOffset(0L)
            .build().encode();
        PROXY_CONTEXT.withVal(ContextVariable.CLIENT_ID, "channel-id");
        receiptHandleProcessor = new ReceiptHandleProcessor(messagingProcessor);
        Mockito.doNothing().when(messagingProcessor).registerConsumerListener(Mockito.any(ConsumerIdsChangeListener.class));
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, receiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES);
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
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        long newInvisibleTime = 2000L;
        ReceiptHandle newReceiptHandleClass = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis() - newInvisibleTime + config.getRenewAheadTimeMillis() - 5)
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
    public void testRenewExceedMaxRenewTimes() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);

        CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
        ackResultFuture.completeExceptionally(new MQClientException(0, "error"));
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis())))
            .thenReturn(ackResultFuture);

        await().atMost(Duration.ofSeconds(1)).until(() -> {
            receiptHandleProcessor.scheduleRenewTask();
            try {
                ReceiptHandleGroup receiptHandleGroup = receiptHandleProcessor.receiptHandleGroupMap.values().stream().findFirst().get();
                return receiptHandleGroup.isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
        Mockito.verify(messagingProcessor, Mockito.times(config.getMaxRenewRetryTimes()))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
    }

    @Test
    public void testRenewWithInvalidHandle() {
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);

        CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
        ackResultFuture.completeExceptionally(new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "error"));
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis())))
            .thenReturn(ackResultFuture);

        await().atMost(Duration.ofSeconds(1)).until(() -> {
            receiptHandleProcessor.scheduleRenewTask();
            try {
                ReceiptHandleGroup receiptHandleGroup = receiptHandleProcessor.receiptHandleGroupMap.values().stream().findFirst().get();
                return receiptHandleGroup.isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    public void testRenewWithErrorThenOK() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);

        AtomicInteger count = new AtomicInteger(0);
        List<CompletableFuture<AckResult>> futureList = new ArrayList<>();
        {
            CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
            ackResultFuture.completeExceptionally(new MQClientException(0, "error"));
            futureList.add(ackResultFuture);
            futureList.add(ackResultFuture);
        }
        {
            long newInvisibleTime = 2000L;
            ReceiptHandle newReceiptHandleClass = ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(System.currentTimeMillis() - newInvisibleTime + config.getRenewAheadTimeMillis() - 5)
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
            futureList.add(CompletableFuture.completedFuture(ackResult));
        }
        {
            CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
            ackResultFuture.completeExceptionally(new MQClientException(0, "error"));
            futureList.add(ackResultFuture);
            futureList.add(ackResultFuture);
            futureList.add(ackResultFuture);
            futureList.add(ackResultFuture);
        }
        Mockito.doAnswer((Answer<CompletableFuture<AckResult>>) mock -> {
            return futureList.get(count.getAndIncrement());
        }).when(messagingProcessor).changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getRenewSliceTimeMillis()));
        await().atMost(Duration.ofSeconds(1)).until(() -> {
            receiptHandleProcessor.scheduleRenewTask();
            try {
                ReceiptHandleGroup receiptHandleGroup = receiptHandleProcessor.receiptHandleGroupMap.values().stream().findFirst().get();
                return receiptHandleGroup.isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
        assertEquals(6, count.get());
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
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, newReceiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES);
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new AckResult()));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(groupConfig.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(RECONSUME_TIMES)));

        ReceiptHandleGroup receiptHandleGroup = receiptHandleProcessor.receiptHandleGroupMap.values().stream().findFirst().get();
        assertTrue(receiptHandleGroup.isEmpty());
    }

    @Test
    public void testRenewReceiptHandleWhenTimeoutWithNoSubscription() {
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
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, newReceiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES);
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channelId))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(null);
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new AckResult()));
        receiptHandleProcessor.scheduleRenewTask();
        await().atMost(Duration.ofSeconds(1)).until(() -> {
            try {
                ReceiptHandleGroup receiptHandleGroup = receiptHandleProcessor.receiptHandleGroupMap.values().stream().findFirst().get();
                return receiptHandleGroup.isEmpty();
            } catch (Exception e) {
                return false;
            }
        });

        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
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
            RECONSUME_TIMES);
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
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.removeReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle);
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
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.clearGroup(new ReceiptHandleProcessor.ReceiptHandleGroupKey(channelId, GROUP));
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.eq(GROUP))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()));
    }

    @Test
    public void testClientOffline() {
        ArgumentCaptor<ConsumerIdsChangeListener> listenerArgumentCaptor = ArgumentCaptor.forClass(ConsumerIdsChangeListener.class);
        Mockito.verify(messagingProcessor, Mockito.times(1)).registerConsumerListener(listenerArgumentCaptor.capture());
        String channelId = PROXY_CONTEXT.getVal(ContextVariable.CLIENT_ID);
        receiptHandleProcessor.addReceiptHandle(channelId, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        listenerArgumentCaptor.getValue().handle(ConsumerGroupEvent.CLIENT_UNREGISTER, GROUP, new ClientChannelInfo(null, channelId, LanguageCode.JAVA, 0));
        assertTrue(receiptHandleProcessor.receiptHandleGroupMap.isEmpty());
    }
}