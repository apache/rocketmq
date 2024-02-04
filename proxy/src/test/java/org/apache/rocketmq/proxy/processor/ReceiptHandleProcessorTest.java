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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.net.SocketAddress;
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
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.RenewStrategyPolicy;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.subscription.RetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
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
        PROXY_CONTEXT.withVal(ContextVariable.CHANNEL, new MockChannel());
        receiptHandleProcessor = new ReceiptHandleProcessor(messagingProcessor);
        Mockito.doNothing().when(messagingProcessor).registerConsumerListener(Mockito.any(ConsumerIdsChangeListener.class));
        messageReceiptHandle = new MessageReceiptHandle(GROUP, TOPIC, QUEUE_ID, receiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES);
    }

    @Test
    public void testAddReceiptHandle() {
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(new SubscriptionGroupConfig());
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills()));
    }

    @Test
    public void testRenewReceiptHandle() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        long newInvisibleTime = 18000L;

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

        RetryPolicy retryPolicy = new RenewStrategyPolicy();
        AtomicInteger times = new AtomicInteger(0);

        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        ackResult.setExtraInfo(newReceiptHandle);

        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(retryPolicy.nextDelayDuration(times.get()))))
            .thenReturn(CompletableFuture.completedFuture(ackResult));
        receiptHandleProcessor.scheduleRenewTask();

        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.argThat(r -> r.getInvisibleTime() == INVISIBLE_TIME), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(retryPolicy.nextDelayDuration(times.get())));
        receiptHandleProcessor.scheduleRenewTask();

        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.argThat(r -> r.getInvisibleTime() == newInvisibleTime), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(retryPolicy.nextDelayDuration(times.incrementAndGet())));
        receiptHandleProcessor.scheduleRenewTask();
    }

    @Test
    public void testRenewExceedMaxRenewTimes() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);

        CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
        ackResultFuture.completeExceptionally(new MQClientException(0, "error"));

        RetryPolicy retryPolicy = new RenewStrategyPolicy();

        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(retryPolicy.nextDelayDuration(messageReceiptHandle.getRenewTimes()))))
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

        Mockito.verify(messagingProcessor, Mockito.times(3))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(retryPolicy.nextDelayDuration(messageReceiptHandle.getRenewTimes())));
    }

    @Test
    public void testRenewWithInvalidHandle() {
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);

        CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
        ackResultFuture.completeExceptionally(new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "error"));
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills())))
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
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);

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

        RetryPolicy retryPolicy = new RenewStrategyPolicy();
        AtomicInteger times = new AtomicInteger(0);
        for (int i = 0; i < 6; i++) {
            Mockito.doAnswer((Answer<CompletableFuture<AckResult>>) mock -> {
                return futureList.get(count.getAndIncrement());
            }).when(messagingProcessor).changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                    Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(retryPolicy.nextDelayDuration(times.getAndIncrement())));
        }

        await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(10)).until(() -> {
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
        long newInvisibleTime = 200L;
        long maxRenewMs = ConfigurationManager.getProxyConfig().getRenewMaxTimeMillis();
        String newReceiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis() - maxRenewMs)
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
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.changeInvisibleTime(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new AckResult()));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(groupConfig.getGroupRetryPolicy().getRetryPolicy().nextDelayDuration(RECONSUME_TIMES)));

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            ReceiptHandleGroup receiptHandleGroup = receiptHandleProcessor.receiptHandleGroupMap.values().stream().findFirst().get();
            assertTrue(receiptHandleGroup.isEmpty());
        });
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
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(null);
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
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, newReceiptHandle, messageReceiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(groupConfig);
        Mockito.when(messagingProcessor.findConsumerChannel(Mockito.any(), Mockito.eq(GROUP), Mockito.eq(channel))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testRemoveReceiptHandle() {
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.removeReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(0))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testClearGroup() {
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        receiptHandleProcessor.clearGroup(new ReceiptHandleProcessor.ReceiptHandleGroupKey(channel, GROUP));
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        Mockito.when(metadataService.getSubscriptionGroupConfig(Mockito.any(), Mockito.eq(GROUP))).thenReturn(groupConfig);
        receiptHandleProcessor.scheduleRenewTask();
        Mockito.verify(messagingProcessor, Mockito.timeout(1000).times(1))
            .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()));
    }

    @Test
    public void testClientOffline() {
        ArgumentCaptor<ConsumerIdsChangeListener> listenerArgumentCaptor = ArgumentCaptor.forClass(ConsumerIdsChangeListener.class);
        Mockito.verify(messagingProcessor, Mockito.times(1)).registerConsumerListener(listenerArgumentCaptor.capture());
        Channel channel = PROXY_CONTEXT.getVal(ContextVariable.CHANNEL);
        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, channel, GROUP, MSG_ID, receiptHandle, messageReceiptHandle);
        listenerArgumentCaptor.getValue().handle(ConsumerGroupEvent.CLIENT_UNREGISTER, GROUP, new ClientChannelInfo(channel, "", LanguageCode.JAVA, 0));
        assertTrue(receiptHandleProcessor.receiptHandleGroupMap.isEmpty());
    }

    class MockChannel implements Channel {
        @Override
        public ChannelId id() {
            return new ChannelId() {
                @Override
                public String asShortText() {
                    return "short";
                }

                @Override
                public String asLongText() {
                    return "long";
                }

                @Override
                public int compareTo(ChannelId o) {
                    return 1;
                }
            };
        }

        @Override
        public EventLoop eventLoop() {
            return null;
        }

        @Override
        public Channel parent() {
            return null;
        }

        @Override
        public ChannelConfig config() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public boolean isRegistered() {
            return false;
        }

        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public ChannelMetadata metadata() {
            return null;
        }

        @Override
        public SocketAddress localAddress() {
            return null;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public ChannelFuture closeFuture() {
            return null;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public long bytesBeforeUnwritable() {
            return 0;
        }

        @Override
        public long bytesBeforeWritable() {
            return 0;
        }

        @Override
        public Unsafe unsafe() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return null;
        }

        @Override
        public Channel read() {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg) {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            return null;
        }

        @Override
        public Channel flush() {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return null;
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return false;
        }

        @Override
        public int compareTo(Channel o) {
            return 1;
        }
    }
}
