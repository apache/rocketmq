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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.opentelemetry.api.common.Attributes;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.longpolling.PopLiteLongPollingService;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService.PollingQueue;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.PopConsumerKVStore;
import org.apache.rocketmq.broker.pop.PopConsumerService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotificationResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Verifies that a notification sent between the check and the polling registration still wakes up
 * the suspended POP request. The polling service is never started, so the timeout scan cannot
 * deliver the response.
 */
public class PopLongPollingLostWakeupTest {
    private static final String TOPIC = "LostWakeupTopic";
    private static final String GROUP = "LostWakeupGroup";
    private static final String CLIENT_ID = "LostWakeupClient";

    private enum Mode {
        POP, KV_POP, NOTIFICATION, LITE_NOTIFICATION, LITE_POP;

        boolean isLite() {
            return this == LITE_NOTIFICATION || this == LITE_POP;
        }

        boolean isNotification() {
            return this == NOTIFICATION || this == LITE_NOTIFICATION;
        }
    }
    private static final int QUEUE_ID = 0;
    private static final long POLL_TIME_MS = 1000;

    @Test(timeout = 10000)
    public void testArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(false, false, false);
    }

    @Test(timeout = 10000)
    public void testKvArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(true, false, false);
    }

    @Test(timeout = 10000)
    public void testNotificationArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(Mode.NOTIFICATION);
    }

    @Test(timeout = 10000)
    public void testLiteNotificationArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(Mode.LITE_NOTIFICATION);
    }

    @Test(timeout = 10000)
    public void testLitePopArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(Mode.LITE_POP);
    }

    private void assertArrivalBeforeRegistration(Mode mode) throws Exception {
        try (Harness h = new Harness(mode)) {
            h.assertArrivalBeforeRegistration();
        }
    }

    @Test(timeout = 10000)
    public void testAllQueuesArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(false, true, false);
        assertArrivalBeforeRegistration(true, true, false);
    }

    @Test(timeout = 10000)
    public void testRetryArrivalBeforeRegistration() throws Exception {
        assertArrivalBeforeRegistration(false, true, true);
        assertArrivalBeforeRegistration(true, true, true);
    }

    @Test(timeout = 10000)
    public void testOrderUnblockedBeforeRegistration() throws Exception {
        for (boolean kv : new boolean[] {false, true}) {
            try (Harness h = new Harness(kv)) {
                h.order = true;
                h.visible.set(true);
                when(h.broker.getConsumerOrderInfoManager().checkBlock(any(), anyString(), anyString(),
                    anyInt(), anyLong())).thenReturn(true);
                h.stopBeforeRegistration();
                when(h.broker.getConsumerOrderInfoManager().checkBlock(any(), anyString(), anyString(),
                    anyInt(), anyLong())).thenReturn(false);
                h.processor.notifyMessageArriving(TOPIC, QUEUE_ID, GROUP);
                assertEquals(0, h.wakeups());
                h.finishRegistration();
                h.assertSuccess();
            }
        }
    }

    @Test(timeout = 10000)
    public void testNoMessageRemainsSuspended() throws Exception {
        for (Mode mode : Mode.values()) {
            try (Harness h = new Harness(mode)) {
                h.stopBeforeRegistration();
                h.finishRegistration();
                h.assertPending(0);
            }
        }
    }

    @Test(timeout = 10000)
    public void testOrderBlockedDoesNotBusyLoop() throws Exception {
        for (boolean kv : new boolean[] {false, true}) {
            try (Harness h = new Harness(kv)) {
                h.order = true;
                when(h.broker.getConsumerOrderInfoManager().checkBlock(any(), anyString(), anyString(),
                    anyInt(), anyLong())).thenReturn(true);
                h.stopBeforeRegistration();
                h.publishMessage();
                h.finishRegistration();
                h.assertPending(1);
            }
        }
    }

    @Test(timeout = 10000)
    public void testTagMismatchDoesNotBusyLoop() throws Exception {
        for (boolean kv : new boolean[] {false, true}) {
            try (Harness h = new Harness(kv)) {
                h.expression = "wanted";
                h.stopBeforeRegistration();
                h.publishMessage();
                h.finishRegistration();
                h.assertPending(1);
                assertTrue(h.reads.get() > h.readsBeforeRegistration);
                assertEquals(1L, kv ? h.offsets.queryPullOffset(GROUP, h.sourceTopic, h.sourceQueueId)
                    : h.bufferOffsets.get(h.sourceKey()).longValue());
            }
        }
    }

    @Test(timeout = 10000)
    public void testCrossQueueRetryArrivalBeforeRegistration() throws Exception {
        for (boolean kv : new boolean[] {false, true}) {
            for (boolean retryV2 : new boolean[] {false, true}) {
                try (Harness h = new Harness(kv)) {
                    h.config.setPopFromRetryProbability(100);
                    h.setRetrySource(retryV2);
                    h.assertArrivalBeforeRegistration();
                }
            }
        }
    }

    @Test(timeout = 10000)
    public void testAsyncReadCompletionBeforeRegistration() throws Exception {
        for (boolean kv : new boolean[] {false, true}) {
            try (Harness h = new Harness(kv)) {
                CompletableFuture<GetMessageResult> read = new CompletableFuture<>();
                when(h.broker.getMessageStore().getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                    .thenReturn(read);
                h.polling.getPollingMap().put(KeyBuilder.buildPollingKey(TOPIC, GROUP, QUEUE_ID), h.gate);
                h.request = h.createRequest();
                assertNull(h.requestProcessor.processRequest(h.ctx, h.request));
                GetMessageResult empty = new GetMessageResult();
                empty.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
                Future<?> completion = h.requestExecutor.submit(() -> read.complete(empty));
                assertTrue(h.gate.beforeAdd.await(5, TimeUnit.SECONDS));
                assertTrue(read.isDone());
                when(h.broker.getMessageStore().getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                    .thenAnswer(invocation -> CompletableFuture.completedFuture(h.readMessage(invocation.getArgument(1),
                        invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(5))));
                h.publishMessage();
                assertEquals(0, h.wakeups());
                h.gate.allowAdd.countDown();
                completion.get(5, TimeUnit.SECONDS);
                h.assertSuccess();
            }
        }
    }

    @Test(timeout = 10000)
    public void testZeroPollTimeDoesNotRegisterRouting() throws Exception {
        for (Mode mode : Mode.values()) {
            for (boolean visible : new boolean[] {false, true}) {
                try (Harness h = new Harness(mode)) {
                    h.pollTime = 0;
                    if (visible) {
                        h.publishMessage();
                    }
                    RemotingCommand response = h.requestProcessor.processRequest(h.ctx, h.createRequest());
                    if (response == null) {
                        response = h.responses.poll(3, TimeUnit.SECONDS);
                    }
                    assertNotNull(response);
                    assertEquals(0, h.snapshotCalls());
                    assertTrue(h.polling.getTopicCidMap().asMap().isEmpty());
                    assertTrue(h.notificationProcessor.getPopLongPollingService().getTopicCidMap().asMap().isEmpty());
                    assertEquals(0, h.wakeups());
                }
            }
        }
    }

    private void assertArrivalBeforeRegistration(boolean kv, boolean allQueues, boolean retry) throws Exception {
        try (Harness h = new Harness(kv)) {
            if (allQueues) {
                h.requestQueueId = -1;
                h.topics.put(TOPIC, new TopicConfig(TOPIC, 2, 2));
                h.setSource(TOPIC, 1);
            }
            if (retry) {
                h.setRetrySource(true);
            }
            h.assertArrivalBeforeRegistration();
        }
    }

    private static class Harness implements AutoCloseable {
        final ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
        final ExecutorService pullExecutor = Executors.newSingleThreadExecutor();
        final BlockingQueue<RemotingCommand> responses = new LinkedBlockingQueue<>();
        final AtomicBoolean visible = new AtomicBoolean();
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger wakeups = new AtomicInteger();
        final AtomicInteger snapshots = new AtomicInteger();
        final RegistrationGate gate = new RegistrationGate();
        final CountDownLatch initialRequestDone = new CountDownLatch(1);
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        final PopMessageProcessor processor;
        final PopLongPollingService polling;
        final NotificationProcessor notificationProcessor;
        final NettyRequestProcessor requestProcessor;
        final PopLiteLongPollingService litePolling;
        final LiteEventDispatcher eventDispatcher;
        final Mode mode;
        final BrokerController broker = mock(BrokerController.class, RETURNS_DEEP_STUBS);
        final BrokerConfig config = new BrokerConfig();
        final Map<String, TopicConfig> topics = new ConcurrentHashMap<>();
        final ConsumerOffsetManager offsets;
        final Map<String, Long> bufferOffsets = new ConcurrentHashMap<>();
        byte[] encodedMessage;
        String sourceTopic = TOPIC;
        int sourceQueueId = QUEUE_ID;
        int requestQueueId = QUEUE_ID;
        int readsBeforeRegistration;
        long pollTime = POLL_TIME_MS;
        boolean order;
        String expression;
        RemotingCommand request;
        Future<RemotingCommand> initialRequest;

        Harness(boolean kv) throws Exception {
            this(kv ? Mode.KV_POP : Mode.POP);
        }

        Harness(Mode mode) throws Exception {
            this.mode = mode;
            boolean kv = mode == Mode.KV_POP;
            config.setPopConsumerKVServiceEnable(kv);
            config.setEnablePopBufferMerge(false);
            config.setPopFromRetryProbability(0);
            config.setPopResponseReturnActualRetryTopic(!kv);
            config.setTransferMsgByHeap(true);
            when(broker.getBrokerConfig()).thenReturn(config);
            topics.put(TOPIC, new TopicConfig(TOPIC, 1, 1));
            when(broker.getTopicConfigManager().selectTopicConfig(anyString()))
                .thenAnswer(invocation -> topics.get(invocation.getArgument(0)));
            SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
            groupConfig.setGroupName(GROUP);
            when(broker.getSubscriptionGroupManager().findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
            when(broker.getSubscriptionGroupManager().containsSubscriptionGroup(GROUP)).thenReturn(true);
            offsets = new ConsumerOffsetManager(broker);
            when(broker.getConsumerOffsetManager()).thenReturn(offsets);
            when(broker.getBrokerMetricsManager().newAttributesBuilder()).thenAnswer(invocation -> Attributes.builder());
            when(broker.getBrokerMetricsManager().getRemotingMetricsManager().newAttributesBuilder())
                .thenAnswer(invocation -> Attributes.builder());
            when(broker.getPullMessageExecutor()).thenReturn(pullExecutor);

            Channel channel = mock(Channel.class);
            when(ctx.channel()).thenReturn(channel);
            when(channel.isActive()).thenReturn(true);
            when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
            when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
                responses.add(invocation.getArgument(0));
                return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess();
            });

            setSource(TOPIC, QUEUE_ID);

            // The single message becomes visible at offset 0 of the source queue once published
            MessageStore store = mock(MessageStore.class);
            when(broker.getMessageStore()).thenReturn(store);
            MessageStoreConfig storeConfig = new MessageStoreConfig();
            when(broker.getMessageStoreConfig()).thenReturn(storeConfig);
            when(store.getMessageStoreConfig()).thenReturn(storeConfig);
            when(store.now()).thenAnswer(invocation -> System.currentTimeMillis());
            when(store.getMaxOffsetInQueue(anyString(), anyInt())).thenAnswer(invocation ->
                visible.get() && sourceTopic.equals(invocation.getArgument(0))
                    && sourceQueueId == (int) invocation.getArgument(1) ? 1L : 0L);
            when(store.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(readMessage(invocation.getArgument(1),
                    invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(5))));
            when(store.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenAnswer(invocation -> readMessage(invocation.getArgument(1), invocation.getArgument(2),
                    invocation.getArgument(3), invocation.getArgument(5)));

            processor = new PopMessageProcessor(broker);
            PopBufferMergeService buffer = mock(PopBufferMergeService.class);
            when(buffer.getLatestOffset(anyString()))
                .thenAnswer(invocation -> bufferOffsets.getOrDefault(invocation.getArgument(0), -1L));
            when(buffer.addCk(any(), anyInt(), anyLong(), anyLong())).thenAnswer(invocation -> {
                PopCheckPoint ck = invocation.getArgument(0);
                bufferOffsets.put(KeyBuilder.buildPollingKey(ck.getTopic(), ck.getCId(), ck.getQueueId()),
                    invocation.getArgument(3));
                return true;
            });
            doAnswer(invocation -> {
                bufferOffsets.put(KeyBuilder.buildPollingKey(invocation.getArgument(1), invocation.getArgument(0),
                    invocation.getArgument(2)), invocation.getArgument(7));
                return null;
            }).when(buffer).addCkMock(anyString(), anyString(), anyInt(), anyLong(), anyLong(), anyLong(),
                anyInt(), anyLong(), anyString());
            FieldUtils.writeDeclaredField(processor, "popBufferMergeService", buffer, true);
            notificationProcessor = new NotificationProcessor(broker);
            when(broker.getNotificationProcessor()).thenReturn(notificationProcessor);
            polling = spy(new PopLongPollingService(broker,
                mode == Mode.NOTIFICATION ? notificationProcessor : processor, mode == Mode.NOTIFICATION));
            doAnswer(invocation -> {
                snapshots.incrementAndGet();
                return invocation.callRealMethod();
            }).when(polling).prepareNotificationSnapshot(anyString(), anyString());
            doAnswer(invocation -> {
                wakeups.incrementAndGet();
                return invocation.callRealMethod();
            }).when(polling).wakeUp(any(), any());
            FieldUtils.writeDeclaredField(mode == Mode.NOTIFICATION ? notificationProcessor : processor,
                "popLongPollingService", polling, true);
            when(broker.getPopMessageProcessor()).thenReturn(processor);
            if (mode.isLite()) {
                topics.get(TOPIC).setTopicMessageType(TopicMessageType.LITE);
                groupConfig.setLiteBindTopic(TOPIC);
                setSource(LiteUtil.toLmqName(TOPIC, "child"), QUEUE_ID);
                eventDispatcher = new LiteEventDispatcher(broker, broker.getLiteSubscriptionRegistry(),
                    broker.getLiteLifecycleManager());
                when(broker.getLiteEventDispatcher()).thenReturn(eventDispatcher);
                when(broker.getLiteSubscriptionRegistry().getAllSubscribers(GROUP, sourceTopic))
                    .thenReturn(Collections.singletonMap(GROUP,
                        Collections.singletonList(new ClientGroup(CLIENT_ID, GROUP))));
                PopLiteMessageProcessor liteProcessor = new PopLiteMessageProcessor(broker, eventDispatcher);
                when(broker.getPopLiteMessageProcessor()).thenReturn(liteProcessor);
                requestProcessor = mode == Mode.LITE_POP ? liteProcessor : notificationProcessor;
                litePolling = spy(new PopLiteLongPollingService(broker, requestProcessor, false));
                doAnswer(invocation -> {
                    snapshots.incrementAndGet();
                    return invocation.callRealMethod();
                }).when(litePolling).prepareNotificationSnapshot(anyString(), anyString());
                doAnswer(invocation -> {
                    wakeups.incrementAndGet();
                    return invocation.callRealMethod();
                }).when(litePolling).wakeUp(any());
                FieldUtils.writeDeclaredField(requestProcessor, "popLiteLongPollingService", litePolling, true);
            } else {
                eventDispatcher = null;
                litePolling = null;
                requestProcessor = mode == Mode.NOTIFICATION ? notificationProcessor : processor;
            }
            if (kv) {
                PopConsumerService service = new PopConsumerService(broker);
                FieldUtils.writeDeclaredField(service, "popConsumerStore", mock(PopConsumerKVStore.class), true);
                when(broker.getPopConsumerService()).thenReturn(service);
            }
        }

        GetMessageResult readMessage(String topic, int queueId, long offset, MessageFilter filter) {
            reads.incrementAndGet();
            GetMessageResult result = new GetMessageResult();
            boolean hasMessage = visible.get() && sourceTopic.equals(topic) && sourceQueueId == queueId;
            boolean matched = filter == null || filter.isMatchedByConsumeQueue((long) "other".hashCode(), null);
            boolean found = hasMessage && offset == 0 && matched;
            result.setStatus(found ? GetMessageStatus.FOUND
                : hasMessage && !matched ? GetMessageStatus.NO_MATCHED_MESSAGE : GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            result.setMaxOffset(hasMessage ? 1 : 0);
            result.setNextBeginOffset(hasMessage ? 1 : 0);
            if (found) {
                result.addMessage(new SelectMappedBufferResult(0,
                    ByteBuffer.wrap(encodedMessage.clone()), encodedMessage.length, null), 0);
            }
            return result;
        }

        void setRetrySource(boolean retryV2) throws Exception {
            config.setEnableRetryTopicV2(retryV2);
            config.setRetrieveMessageFromPopRetryTopicV1(!retryV2);
            String topic = KeyBuilder.buildPopRetryTopic(TOPIC, GROUP, retryV2);
            topics.put(topic, new TopicConfig(topic, 2, 2));
            setSource(topic, 1);
        }

        void setSource(String topic, int queueId) throws Exception {
            sourceTopic = topic;
            sourceQueueId = queueId;
            offsets.commitOffset("init", GROUP, topic, queueId, 0L);
            MessageExt message = new MessageExt();
            message.setTopic(topic);
            message.setQueueId(queueId);
            message.setQueueOffset(0);
            message.setTags("other");
            message.setBody("lost-wakeup-message".getBytes(StandardCharsets.UTF_8));
            message.setBornHost(new InetSocketAddress("127.0.0.1", 12345));
            message.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
            message.setStoreTimestamp(System.currentTimeMillis());
            encodedMessage = MessageDecoder.encode(message, false);
        }

        String sourceKey() {
            return KeyBuilder.buildPollingKey(sourceTopic, GROUP, sourceQueueId);
        }

        RemotingCommand createRequest() {
            CommandCustomHeader header;
            int requestCode;
            if (mode.isNotification()) {
                NotificationRequestHeader notification = new NotificationRequestHeader();
                notification.setQueueId(requestQueueId);
                notification.setLiteConsumer(mode.isLite());
                notification.setClientId(CLIENT_ID);
                header = notification;
                requestCode = RequestCode.NOTIFICATION;
            } else if (mode == Mode.LITE_POP) {
                PopLiteMessageRequestHeader lite = new PopLiteMessageRequestHeader();
                lite.setClientId(CLIENT_ID);
                lite.setMaxMsgNum(1);
                lite.setInvisibleTime(10000);
                header = lite;
                requestCode = RequestCode.POP_LITE_MESSAGE;
            } else {
                PopMessageRequestHeader pop = new PopMessageRequestHeader();
                pop.setQueueId(requestQueueId);
                pop.setMaxMsgNums(1);
                pop.setInvisibleTime(10000);
                pop.setInitMode(ConsumeInitMode.MIN);
                pop.setOrder(order);
                pop.setExp(expression);
                pop.setExpType("TAG");
                header = pop;
                requestCode = RequestCode.POP_MESSAGE;
            }
            RemotingCommand command = RemotingCommand.createRequestCommand(requestCode, header);
            command.makeCustomHeaderToNet();
            command.addExtField("topic", TOPIC);
            command.addExtField("consumerGroup", GROUP);
            command.addExtField("pollTime", Long.toString(pollTime));
            command.addExtField("bornTime", Long.toString(System.currentTimeMillis()));
            return command;
        }

        @SuppressWarnings("unchecked")
        void stopBeforeRegistration() throws Exception {
            if (mode.isLite()) {
                Map<String, ConcurrentSkipListSet<PopRequest>> pollingMap =
                    (Map<String, ConcurrentSkipListSet<PopRequest>>) FieldUtils.readField(litePolling, "pollingMap", true);
                pollingMap.put(CLIENT_ID, gate);
                assertFalse(eventDispatcher.hasEvents(CLIENT_ID));
            } else {
                polling.getPollingMap().put(KeyBuilder.buildPollingKey(TOPIC, GROUP, requestQueueId), gate);
            }
            // Deep stubs are not thread-safe, so wakeups run only after the initial request returns
            pullExecutor.submit(() -> initialRequestDone.await(5, TimeUnit.SECONDS));
            request = createRequest();
            initialRequest = requestExecutor.submit(() -> requestProcessor.processRequest(ctx, request));
            assertTrue(gate.beforeAdd.await(5, TimeUnit.SECONDS));
            readsBeforeRegistration = reads.get();
        }

        void finishRegistration() throws Exception {
            gate.allowAdd.countDown();
            assertNull(initialRequest.get(5, TimeUnit.SECONDS));
            initialRequestDone.countDown();
            assertTrue(request.isSuspended());
        }

        void publishMessage() {
            visible.set(true);
            if (mode.isLite()) {
                eventDispatcher.dispatch(GROUP, sourceTopic, QUEUE_ID, 1L, System.currentTimeMillis());
            } else if (mode == Mode.NOTIFICATION) {
                notificationProcessor.notifyMessageArriving(TOPIC, sourceQueueId, 0L, null,
                    System.currentTimeMillis(), null, Collections.emptyMap());
            } else {
                processor.notifyMessageArriving(sourceTopic, sourceQueueId, 0L, null,
                    System.currentTimeMillis(), null, sourceTopic.equals(TOPIC) ? Collections.emptyMap()
                        : Collections.singletonMap(org.apache.rocketmq.common.message.MessageConst.PROPERTY_ORIGIN_GROUP, GROUP));
            }
        }

        int snapshotCalls() {
            return snapshots.get();
        }

        int wakeups() {
            return wakeups.get();
        }

        void assertArrivalBeforeRegistration() throws Exception {
            stopBeforeRegistration();
            publishMessage();
            assertEquals("Arrival notification must miss the unregistered request", 0, wakeups());
            assertTrue(gate.isEmpty());
            if (mode.isLite()) {
                assertTrue(eventDispatcher.hasEvents(CLIENT_ID));
            }
            finishRegistration();
            assertSuccess();
        }

        void assertSuccess() throws Exception {
            RemotingCommand response = responses.poll(3, TimeUnit.SECONDS);
            assertNotNull("Wakeup must re-run the processor and return a response: " + mode, response);
            assertEquals(request.getOpaque(), response.getOpaque());
            assertEquals(ResponseCode.SUCCESS, response.getCode());
            if (mode.isNotification()) {
                assertTrue(((NotificationResponseHeader) response.readCustomHeader()).isHasMsg());
            } else {
                assertArrayEquals(encodedMessage, response.getBody());
                assertTrue(reads.get() > readsBeforeRegistration);
                if (mode == Mode.LITE_POP) {
                    assertFalse(eventDispatcher.hasEvents(CLIENT_ID));
                }
            }
            assertEquals(1, wakeups());
            assertTrue(gate.isEmpty());
            pullExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertTrue("The same request must return only one response", responses.isEmpty());
        }

        void assertPending(int expectedWakeups) throws Exception {
            pullExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertEquals(expectedWakeups, wakeups());
            assertEquals(1, gate.size());
            assertTrue(responses.isEmpty());
            if (expectedWakeups == 0) {
                assertEquals(readsBeforeRegistration, reads.get());
            }
        }

        @Override
        public void close() throws Exception {
            gate.allowAdd.countDown();
            initialRequestDone.countDown();
            requestExecutor.shutdownNow();
            assertTrue(requestExecutor.awaitTermination(5, TimeUnit.SECONDS));
            polling.shutdown();
            notificationProcessor.getPopLongPollingService().shutdown();
            notificationProcessor.getPopLiteLongPollingService().shutdown();
            if (litePolling != null) {
                litePolling.shutdown();
            }
            pullExecutor.shutdownNow();
            assertTrue(pullExecutor.awaitTermination(5, TimeUnit.SECONDS));
            if (litePolling != null) {
                broker.getPopLiteMessageProcessor().getConsumerOrderInfoManager().shutdown();
            }
        }
    }

    /**
     * Pauses the first registration until released, fixing the "empty read -> arrival -> registration" order.
     */
    private static class RegistrationGate extends PollingQueue {
        final CountDownLatch beforeAdd = new CountDownLatch(1);
        final CountDownLatch allowAdd = new CountDownLatch(1);
        final AtomicBoolean firstAdd = new AtomicBoolean(true);

        @Override
        public boolean add(PopRequest request) {
            if (firstAdd.compareAndSet(true, false)) {
                beforeAdd.countDown();
                try {
                    assertTrue(allowAdd.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return super.add(request);
        }
    }

}
