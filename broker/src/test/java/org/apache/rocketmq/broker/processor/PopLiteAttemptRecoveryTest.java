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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.lite.LiteSubscriptionRegistry;
import org.apache.rocketmq.broker.lite.LiteTestUtil;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Recovery of a lost lite pop by its attemptId, driven through the request path a proxy uses.
 *
 * <p>The broker side is real: message store with lmq dispatch, consumer offsets, lite event dispatcher,
 * {@link PopLiteMessageProcessor} with its long polling, order info and lock service, and the ack and renewal
 * processors. A response is lost by simply not reading it. Only metadata lookups (topic, group, subscribers)
 * and stats are mocked, and the channel just records the written responses. The store is shared by the tests,
 * each of them using its own lite topics.
 */
public class PopLiteAttemptRecoveryTest {
    private static final String PARENT = "LiteAttemptParent";
    private static final String GROUP = "LiteAttemptGroup";
    private static final String C1 = "client-1";
    private static final String C2 = "client-2";
    private static final long INVISIBLE = 60_000L;
    private static final long WAIT = 5_000L;

    private static final Map<String, Long> ARRIVED = new ConcurrentHashMap<>(); // lmq -> logic offset notified
    private static volatile MessageArrivingListener notifier; // of the running test
    private static String storePath;
    private static MessageStore store;

    private final BrokerConfig brokerConfig = new BrokerConfig();
    private final BrokerController broker = mock(BrokerController.class);
    private final Map<String, List<String>> subscribers = new ConcurrentHashMap<>(); // lmq -> client ids
    private final Set<String> tombstones = ConcurrentHashMap.newKeySet(); // clientId@lmq evicted in exclusive mode
    private final Map<Integer, RemotingCommand> responses = new ConcurrentHashMap<>(); // opaque -> response
    private String lt1;
    private String lt2;
    private String lmq1;
    private String lmq2;
    private SubscriptionGroupConfig groupConfig;
    private ConsumerOffsetManager offsets;
    private PopLiteMessageProcessor processor;
    private AckMessageProcessor ackProcessor;
    private ChangeInvisibleTimeProcessor renewProcessor;
    private ExecutorService pullExecutor;
    private Channel channel;
    private ChannelHandlerContext ctx;

    @BeforeClass
    public static void startStore() throws Exception {
        storePath = System.getProperty("java.io.tmpdir") + File.separator + "store-liteAttempt-" + UUID.randomUUID();
        MessageArrivingListener listener = (topic, queueId, logicOffset, tagsCode, storeTime, bitMap, properties) -> {
            MessageArrivingListener current = notifier;
            if (current != null) {
                current.arriving(topic, queueId, logicOffset, tagsCode, storeTime, bitMap, properties);
            }
            ARRIVED.put(topic, logicOffset);
        };
        store = LiteTestUtil.buildMessageStore(storePath, new BrokerConfig(), new ConcurrentHashMap<>(), false, listener);
        assertTrue(store.load());
        store.start();
    }

    @AfterClass
    public static void stopStore() {
        store.shutdown();
        store.destroy();
        UtilAll.deleteFile(new File(storePath));
    }

    @Before
    public void setUp() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        lt1 = "lt1-" + suffix;
        lt2 = "lt2-" + suffix;
        lmq1 = LiteUtil.toLmqName(PARENT, lt1);
        lmq2 = LiteUtil.toLmqName(PARENT, lt2);
        when(broker.getBrokerConfig()).thenReturn(brokerConfig);

        groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(GROUP);
        groupConfig.setLiteBindTopic(PARENT);
        SubscriptionGroupManager groupManager = mock(SubscriptionGroupManager.class);
        when(groupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
        when(broker.getSubscriptionGroupManager()).thenReturn(groupManager);

        TopicConfig topicConfig = new TopicConfig(PARENT, 1, 1);
        topicConfig.setTopicMessageType(TopicMessageType.LITE);
        TopicConfigManager topicManager = mock(TopicConfigManager.class);
        when(topicManager.selectTopicConfig(PARENT)).thenReturn(topicConfig);
        when(broker.getTopicConfigManager()).thenReturn(topicManager);

        LiteSubscriptionRegistry registry = mock(LiteSubscriptionRegistry.class);
        when(registry.getAllSubscribers(any(), anyString())).thenAnswer(invocation -> {
            List<ClientGroup> clients = new ArrayList<>();
            for (String clientId : subscribers.getOrDefault(invocation.<String>getArgument(1), Collections.emptyList())) {
                clients.add(new ClientGroup(clientId, GROUP));
            }
            return clients.isEmpty() ? Collections.emptyMap() : Collections.singletonMap(GROUP, clients);
        });
        when(registry.hasExclusiveEvictionTombstone(anyString(), anyString())).thenAnswer(invocation ->
            tombstones.contains(invocation.getArgument(0) + "@" + invocation.getArgument(1)));
        when(broker.getLiteSubscriptionRegistry()).thenReturn(registry);

        AbstractLiteLifecycleManager lifecycle = mock(AbstractLiteLifecycleManager.class);
        when(lifecycle.getMaxOffsetInQueue(anyString()))
            .thenAnswer(invocation -> store.getMaxOffsetInQueue(invocation.getArgument(0), 0));
        when(broker.getLiteLifecycleManager()).thenReturn(lifecycle);

        offsets = new ConsumerOffsetManager(broker);
        when(broker.getConsumerOffsetManager()).thenReturn(offsets);
        when(broker.getPopMessageProcessor()).thenReturn(mock(PopMessageProcessor.class));
        when(broker.getBrokerStatsManager()).thenReturn(mock(BrokerStatsManager.class));
        pullExecutor = Executors.newFixedThreadPool(4);
        when(broker.getPullMessageExecutor()).thenReturn(pullExecutor);

        LiteEventDispatcher dispatcher = new LiteEventDispatcher(broker, registry, lifecycle);
        when(broker.getLiteEventDispatcher()).thenReturn(dispatcher);
        notifier = new NotifyMessageArrivingListener(mock(PullRequestHoldService.class),
            mock(PopMessageProcessor.class), mock(NotificationProcessor.class), dispatcher);
        when(broker.getMessageStore()).thenReturn(store);
        BrokerMetricsManager metricsManager = new BrokerMetricsManager(broker);
        when(broker.getBrokerMetricsManager()).thenReturn(metricsManager);

        processor = new PopLiteMessageProcessor(broker, dispatcher);
        when(broker.getPopLiteMessageProcessor()).thenReturn(processor);
        NotificationProcessor notificationProcessor = new NotificationProcessor(broker);
        when(broker.getNotificationProcessor()).thenReturn(notificationProcessor);
        processor.getPopLiteLongPollingService().start();
        ackProcessor = new AckMessageProcessor(broker);
        renewProcessor = new ChangeInvisibleTimeProcessor(broker);

        channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10911));
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(writeFuture.addListener(any())).thenReturn(writeFuture);
        when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
            RemotingCommand response = invocation.getArgument(0);
            responses.put(response.getOpaque(), response);
            return writeFuture;
        });
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
    }

    @After
    public void tearDown() {
        notifier = null;
        processor.getPopLiteLongPollingService().shutdown();
        processor.getConsumerOrderInfoManager().shutdown();
        pullExecutor.shutdownNow();
    }

    @Test
    public void testRetryAfterLostResponseRedeliversTheBatchWithoutMovingOffsets() throws Exception {
        subscribe(C1, lmq1, lmq2);
        send(lt1, 2);
        send(lt2, 1);
        Batch lost = await(pop(C1, "A", 3, 0));
        assertEquals(Arrays.asList(lt1 + "@0", lt1 + "@1", lt2 + "@0"), lost.messages);

        // while the response is lost, a new message arrives on lt1
        send(lt1, 1);
        nextMillis();
        Batch retry = await(pop(C1, "A", 3, 0));

        // a normal pop composes the batch: lt1 fills the budget, the same attempt does not count a reconsume
        assertEquals(Arrays.asList(lt1 + "@0", lt1 + "@1", lt1 + "@2"), retry.messages);
        assertEquals(Arrays.asList(0, 0, 0), retry.reconsumeTimes);
        assertEquals(0, offsets.queryOffset(GROUP, lmq1, 0));
        assertEquals(0, offsets.queryOffset(GROUP, lmq2, 0));

        // lt2 is released as well, so the next request reads it although no new message arrived
        assertEquals(Collections.singletonList(lt2 + "@0"), await(pop(C1, "B", 3, 0)).messages);
    }

    @Test
    public void testReceiptOfReleasedLeaseCannotAckOrRenew() throws Exception {
        subscribe(C1, lmq1);
        send(lt1, 1);
        Batch lost = await(pop(C1, "A", 32, 0));
        nextMillis();
        Batch retry = await(pop(C1, "A", 32, 0));
        assertEquals(lost.messages, retry.messages);

        assertEquals(ResponseCode.NO_MESSAGE, renew(lost.popTime, lt1, 0));
        assertEquals(ResponseCode.NO_MESSAGE, renew(lost.popTime, lt1, 0, true));
        ack(lost.popTime, lt1, 0);
        assertEquals(0, offsets.queryOffset(GROUP, lmq1, 0));

        assertEquals(ResponseCode.SUCCESS, renew(retry.popTime, lt1, 0));
        ack(retry.popTime, lt1, 0);
        assertEquals(1, offsets.queryOffset(GROUP, lmq1, 0));
    }

    @Test
    public void testRetryReleasesLeaseForTakeoverByAnotherClient() throws Exception {
        subscribe(C1, lmq1);
        send(lt1, 2);
        Batch lost = await(pop(C1, "A", 32, 0));
        assertEquals(Arrays.asList(lt1 + "@0", lt1 + "@1"), lost.messages);

        // lt1 moves to C2 while C1 still holds the lease, C2 waits in long polling
        subscribers.put(lmq1, Collections.singletonList(C2));
        int takeover = pop(C2, "X", 32, WAIT);
        assertPending(takeover);

        // the retry of C1 releases the lease, and the re-dispatched event wakes C2
        assertTrue(await(pop(C1, "A", 32, 0)).messages.isEmpty());
        Batch taken = await(takeover);
        assertEquals(Arrays.asList(lt1 + "@0", lt1 + "@1"), taken.messages);
        assertEquals(Arrays.asList(1, 1), taken.reconsumeTimes);
        assertEquals(0, offsets.queryOffset(GROUP, lmq1, 0));
    }

    @Test
    public void testTimeoutOfAbandonedRequestKeepsTheBatchOfItsRetry() throws Exception {
        subscribe(C1, lmq1);
        // R1 waits in long polling, the client gives up and retries A with a shorter poll, so R2 is woken first
        int r1 = pop(C1, "A", 32, 1_500);
        assertPending(r1);
        int r2 = pop(C1, "A", 32, 500);
        assertPending(r2);

        send(lt1, 1);
        Batch delivered = await(r2);
        assertEquals(Collections.singletonList(lt1 + "@0"), delivered.messages);

        // R1 times out while the batch of R2 is being consumed
        Batch stale = await(r1);
        assertEquals(ResponseCode.POLLING_TIMEOUT, stale.code);
        assertEquals(ResponseCode.SUCCESS, renew(delivered.popTime, lt1, 0));
        ack(delivered.popTime, lt1, 0);
        assertEquals(1, offsets.queryOffset(GROUP, lmq1, 0));
    }

    @Test
    public void testConsecutiveLostResponsesOfOneAttemptStillRecover() throws Exception {
        subscribe(C1, lmq1);
        send(lt1, 2);
        Batch lost1 = await(pop(C1, "A", 32, 0));
        nextMillis();
        Batch lost2 = await(pop(C1, "A", 32, 0));
        nextMillis();
        Batch retry = await(pop(C1, "A", 32, 0));

        assertEquals(Arrays.asList(lt1 + "@0", lt1 + "@1"), lost2.messages);
        assertEquals(lost2.messages, retry.messages);
        assertEquals(Arrays.asList(0, 0), retry.reconsumeTimes);
        assertEquals(ResponseCode.NO_MESSAGE, renew(lost1.popTime, lt1, 0));
        assertEquals(ResponseCode.NO_MESSAGE, renew(lost2.popTime, lt1, 0));
        ack(retry.popTime, lt1, 0);
        ack(retry.popTime, lt1, 1);
        assertEquals(2, offsets.queryOffset(GROUP, lmq1, 0));
    }

    @Test
    public void testClientWithoutAttemptIdKeepsTheLeaseUntilAck() throws Exception {
        subscribe(C1, lmq1);
        send(lt1, 1);
        Batch first = await(pop(C1, null, 32, 0));
        assertEquals(Collections.singletonList(lt1 + "@0"), first.messages);

        // without an attemptId a pop cannot prove the loss, so the lease is not released
        send(lt1, 1);
        nextMillis();
        assertTrue(await(pop(C1, null, 32, 0)).messages.isEmpty());
        assertEquals(ResponseCode.SUCCESS, renew(first.popTime, lt1, 0));

        // the ack unblocks lt1 and the next message is delivered as before
        ack(first.popTime, lt1, 0);
        assertEquals(1, offsets.queryOffset(GROUP, lmq1, 0));
        assertEquals(Collections.singletonList(lt1 + "@1"), await(pop(C1, null, 32, 0)).messages);
    }

    @Test
    public void testConcurrentRequestsWithoutAttemptIdEachPop() throws Exception {
        subscribe(C1, lmq1, lmq2);
        int r1 = pop(C1, null, 32, WAIT);
        assertPending(r1);
        nextMillis();
        int r2 = pop(C1, null, 32, WAIT);
        assertPending(r2);

        // without an attemptId neither request is taken as abandoned, so the older one pops as well
        send(lt1, 1);
        assertEquals(Collections.singletonList(lt1 + "@0"), await(r1, WAIT / 2).messages);
        send(lt2, 1);
        assertEquals(Collections.singletonList(lt2 + "@0"), await(r2, WAIT / 2).messages);
    }

    @Test
    public void testNotificationPassesThroughAChainOfAbandonedRequests() throws Exception {
        subscribe(C1, lmq1);
        int r1 = pop(C1, "A", 32, WAIT);
        assertPending(r1);
        nextMillis();
        int r2 = pop(C1, "A", 32, WAIT);
        assertPending(r2);
        nextMillis();
        int r3 = pop(C1, "A", 32, WAIT);
        assertPending(r3);

        send(lt1, 1);
        // R3 gets the message long before any poll ends, so R1 and R2 must have passed the notification on
        assertEquals(Collections.singletonList(lt1 + "@0"), await(r3, WAIT / 2).messages);
        assertEquals(ResponseCode.POLLING_TIMEOUT, await(r1, 500).code);
        assertEquals(ResponseCode.POLLING_TIMEOUT, await(r2, 500).code);
    }

    @Test
    public void testClientEvictedInExclusiveModeReleasesToTheNewOwner() throws Exception {
        groupConfig.setLiteSubExclusive(true);
        subscribe(C1, lmq1);
        send(lt1, 1);
        assertEquals(Collections.singletonList(lt1 + "@0"), await(pop(C1, "A", 32, 0)).messages);
        // a new message queues an lt1 event for C1 right before C2 takes lt1 over
        send(lt1, 1);
        subscribers.put(lmq1, Collections.singletonList(C2));
        tombstones.add(C1 + "@" + lmq1);
        int owner = pop(C2, "X", 32, WAIT);
        assertPending(owner);

        // the retry of C1 still releases its lease, while its own lt1 event is rejected by the tombstone
        assertTrue(await(pop(C1, "A", 32, 0)).messages.isEmpty());
        assertEquals(Arrays.asList(lt1 + "@0", lt1 + "@1"), await(owner).messages);
        assertEquals(0, offsets.queryOffset(GROUP, lmq1, 0));
    }

    @Test
    public void testRequestWaitingWhileItsRecordIsCleanedStillPops() throws Exception {
        subscribe(C1, lmq1);
        int waiting = pop(C1, "A", 32, WAIT);
        assertPending(waiting);
        processor.cleanAttempts(System.nanoTime() + PopLiteMessageProcessor.ATTEMPT_TTL + TimeUnit.SECONDS.toNanos(1));

        send(lt1, 1);
        assertEquals(Collections.singletonList(lt1 + "@0"), await(waiting, WAIT / 2).messages);
    }

    @Test
    public void testRetryAfterLeaseExpiredStillRedeliversWithoutMovingOffset() throws Exception {
        // the expired lease no longer blocks its own attempt, so this guards releasing an expired lease rather than
        // the release itself: the retry still gets the batch once and the offset stays
        subscribe(C1, lmq1);
        send(lt1, 1);
        Batch lost = await(pop(C1, "A", 32, 0, 500));
        Thread.sleep(800);

        Batch retry = await(pop(C1, "A", 32, 0));
        assertEquals(lost.messages, retry.messages);
        assertEquals(0, offsets.queryOffset(GROUP, lmq1, 0));
        ack(retry.popTime, lt1, 0);
        assertEquals(1, offsets.queryOffset(GROUP, lmq1, 0));
    }

    private void subscribe(String clientId, String... lmqNames) {
        for (String lmqName : lmqNames) {
            subscribers.put(lmqName, Collections.singletonList(clientId));
            offsets.commitOffset("init", GROUP, lmqName, 0, 0L);
        }
    }

    private void send(String liteTopic, int count) throws Exception {
        String lmqName = LiteUtil.toLmqName(PARENT, liteTopic);
        long expected = store.getMaxOffsetInQueue(lmqName, 0) + count;
        for (int i = 0; i < count; i++) {
            store.putMessage(LiteTestUtil.buildMessage(PARENT, liteTopic));
        }
        // the lmq is readable and its arriving event is dispatched
        awaitTrue(() -> ARRIVED.getOrDefault(lmqName, 0L) >= expected, "message arriving of " + lmqName);
    }

    // a pop request as sent by a proxy, returning its opaque to collect the response
    private int pop(String clientId, String attemptId, int maxNum, long pollTime) throws Exception {
        return pop(clientId, attemptId, maxNum, pollTime, INVISIBLE);
    }

    private int pop(String clientId, String attemptId, int maxNum, long pollTime, long invisibleTime)
        throws Exception {
        PopLiteMessageRequestHeader header = new PopLiteMessageRequestHeader();
        header.setClientId(clientId);
        header.setConsumerGroup(GROUP);
        header.setTopic(PARENT);
        header.setMaxMsgNum(maxNum);
        header.setInvisibleTime(invisibleTime);
        header.setPollTime(pollTime);
        header.setBornTime(System.currentTimeMillis());
        header.setAttemptId(attemptId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_LITE_MESSAGE, header);
        request.makeCustomHeaderToNet();
        processor.processRequest(ctx, request);
        return request.getOpaque();
    }

    private void ack(long popTime, String liteTopic, long offset) throws Exception {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setConsumerGroup(GROUP);
        header.setTopic(PARENT);
        header.setQueueId(0);
        header.setOffset(offset);
        header.setLiteTopic(liteTopic);
        header.setExtraInfo(extraInfo(popTime, offset));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, header);
        request.makeCustomHeaderToNet();
        assertEquals(ResponseCode.SUCCESS, ackProcessor.processRequest(ctx, request).getCode());
    }

    private int renew(long popTime, String liteTopic, long offset) throws Exception {
        return renew(popTime, liteTopic, offset, false);
    }

    private int renew(long popTime, String liteTopic, long offset, boolean suspend) throws Exception {
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setConsumerGroup(GROUP);
        header.setTopic(PARENT);
        header.setQueueId(0);
        header.setOffset(offset);
        header.setLiteTopic(liteTopic);
        header.setInvisibleTime(INVISIBLE);
        header.setSuspend(suspend);
        header.setExtraInfo(extraInfo(popTime, offset));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, header);
        request.makeCustomHeaderToNet();
        return renewProcessor.processRequestAsync(channel, request, true).join().getCode();
    }

    // the receipt handle built by a lite consumer from the pop response
    private static String extraInfo(long popTime, long offset) {
        return ExtraInfoUtil.buildExtraInfo(0, popTime, INVISIBLE, KeyBuilder.POP_ORDER_REVIVE_QUEUE, PARENT,
            "broker", 0, offset);
    }

    private Batch await(int opaque) throws Exception {
        return await(opaque, WAIT);
    }

    private Batch await(int opaque, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            RemotingCommand response = responses.get(opaque);
            if (response != null) {
                return new Batch(response);
            }
            Thread.sleep(5);
        }
        fail("no response of request " + opaque + " in " + timeoutMillis + "ms");
        return null;
    }

    private void assertPending(int opaque) throws Exception {
        Thread.sleep(50);
        assertFalse("request " + opaque + " should wait in long polling", responses.containsKey(opaque));
    }

    private static void awaitTrue(BooleanSupplier condition, String what) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    // pop time identifies a lease in milliseconds, so a retry in the same millisecond would reuse it
    private static void nextMillis() throws Exception {
        Thread.sleep(2);
    }

    private static final class Batch {
        private final int code;
        private final long popTime;
        private final List<String> messages = new ArrayList<>(); // liteTopic@offset
        private final List<Integer> reconsumeTimes = new ArrayList<>();

        private Batch(RemotingCommand response) {
            PopLiteMessageResponseHeader header = (PopLiteMessageResponseHeader) response.readCustomHeader();
            this.code = response.getCode();
            this.popTime = header.getPopTime();
            if (code != ResponseCode.SUCCESS) {
                return;
            }
            List<MessageExt> found = MessageDecoder.decodesBatch(ByteBuffer.wrap(response.getBody()), true, false, true);
            List<Integer> counts = ExtraInfoUtil.parseLiteOrderCountInfo(header.getOrderCountInfo(), found.size());
            for (int i = 0; i < found.size(); i++) {
                MessageExt message = found.get(i);
                messages.add(LiteUtil.getLiteTopic(message.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
                    + "@" + message.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET));
                reconsumeTimes.add(counts == null ? 0 : counts.get(i));
            }
        }
    }
}
