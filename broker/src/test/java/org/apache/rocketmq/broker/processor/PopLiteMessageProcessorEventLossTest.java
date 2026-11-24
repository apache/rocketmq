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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.lite.LiteSubscriptionRegistry;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Reproduces the Lite pop event-loss bug: within one popByClientId, a lmq already visited in this
 * request (added to the {@code processed} dedup set) gets re-dispatched by a concurrent ack that
 * advances the offset and unblocks FIFO. The buggy code drops the re-enqueued event, so its
 * remaining messages are never read (queue becomes empty, offset does not advance further).
 *
 * <p>Symptom asserted per iteration: consumer subscribing lt1 and lt2 with 5 messages each must
 * eventually receive all 5+5. On the buggy code lt1 only yields its first message (offset 0) while
 * lt2 yields all 5 (6 total), matching the failing E2E case.
 *
 * <p>The harness boots real components once ({@link MessageStore}, {@link LiteEventDispatcher},
 * {@link PopLiteMessageProcessor} with its internal FIFO order-info + lock service,
 * {@link ConsumerOffsetManager}) and loops the send/consume scenario internally. The ack of lt1's
 * first message is fired while popByClientId is mid-iteration through a test-only white-box spy on
 * {@code popLiteTopic}; this is purely to make the concurrency race deterministic rather than
 * timing-dependent, and is not a dependency of the production code path.
 */
public class PopLiteMessageProcessorEventLossTest {

    private static final String PARENT_TOPIC = "LitePopLossParent";
    private static final String GROUP = "LitePopLossGroup";
    private static final String CLIENT_ID = "clientId-repro";
    private static final String CLIENT_HOST = "127.0.0.1:0";
    private static final long INVISIBLE = 60_000L;
    private static final int MAX_NUM = 32;
    private static final int ITERATIONS = 5;
    private static final int DRAIN_ROUNDS = 6;

    private static final BrokerConfig BROKER_CONFIG = new BrokerConfig();
    private static final ConcurrentMap<String, TopicConfig> TOPIC_CONFIG_TABLE = new ConcurrentHashMap<>();

    private static String storePathRootDir;
    private static MessageStore messageStore;
    private static ConsumerOffsetManager consumerOffsetManager;
    private static LiteEventDispatcher liteEventDispatcher;
    private static PopLiteMessageProcessor popLiteMessageProcessor;
    private static LiteSubscriptionRegistry liteSubscriptionRegistry;

    // Per-pop capture populated by the popLiteTopic spy: lmqName -> offsets read in the current pop.
    private static final Map<String, List<Long>> LAST_POP_READS = new HashMap<>();
    // Cumulative reception across the whole scenario: lmqName -> offsets RECEIVED.
    private static final Map<String, Set<Long>> RECEIVED = new HashMap<>();

    // Injection state: when armed, firing the ack of (injectLmq, offset 0) while popLiteTopic runs.
    private static final AtomicBoolean INJECT_ARMED = new AtomicBoolean(false);
    private static volatile String injectLmq;
    private static volatile long injectPopTime;

    @BeforeClass
    public static void setUp() throws Exception {
        storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-litePopLoss";
        UtilAll.deleteFile(new File(storePathRootDir));

        BROKER_CONFIG.setMaxClientEventCount(1000);

        BrokerController brokerController = Mockito.mock(BrokerController.class);
        SubscriptionGroupManager subscriptionGroupManager = Mockito.mock(SubscriptionGroupManager.class);
        AbstractLiteLifecycleManager liteLifecycleManager = Mockito.mock(AbstractLiteLifecycleManager.class);
        liteSubscriptionRegistry = Mockito.mock(LiteSubscriptionRegistry.class);
        PopMessageProcessor popMessageProcessor = Mockito.mock(PopMessageProcessor.class);

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(GROUP);
        groupConfig.setLiteBindTopic(PARENT_TOPIC);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);

        doReturn(BROKER_CONFIG).when(brokerController).getBrokerConfig();
        doReturn(subscriptionGroupManager).when(brokerController).getSubscriptionGroupManager();
        doReturn(liteSubscriptionRegistry).when(brokerController).getLiteSubscriptionRegistry();
        doReturn(liteLifecycleManager).when(brokerController).getLiteLifecycleManager();
        doReturn(popMessageProcessor).when(brokerController).getPopMessageProcessor();
        when(liteLifecycleManager.getMaxOffsetInQueue(anyString())).thenReturn(1_000_000L);

        consumerOffsetManager = new ConsumerOffsetManager(brokerController);
        doReturn(consumerOffsetManager).when(brokerController).getConsumerOffsetManager();

        messageStore = org.apache.rocketmq.broker.lite.LiteTestUtil.buildMessageStore(
            storePathRootDir, BROKER_CONFIG, TOPIC_CONFIG_TABLE, false, null);
        doReturn(messageStore).when(brokerController).getMessageStore();
        messageStore.load();
        messageStore.start();

        liteEventDispatcher = new LiteEventDispatcher(brokerController, liteSubscriptionRegistry, liteLifecycleManager);
        popLiteMessageProcessor = Mockito.spy(new PopLiteMessageProcessor(brokerController, liteEventDispatcher));
        NotificationProcessor notificationProcessor = new NotificationProcessor(brokerController);

        doReturn(liteEventDispatcher).when(brokerController).getLiteEventDispatcher();
        doReturn(popLiteMessageProcessor).when(brokerController).getPopLiteMessageProcessor();
        doReturn(notificationProcessor).when(brokerController).getNotificationProcessor();

        // Subscribers fan-out: any lmq under GROUP resolves to our single client.
        Map<String, List<ClientGroup>> subscriberMap = new HashMap<>();
        subscriberMap.put(GROUP, Collections.singletonList(new ClientGroup(CLIENT_ID, GROUP)));
        when(liteSubscriptionRegistry.getAllSubscribers(eq(GROUP), anyString())).thenReturn(subscriberMap);
        when(liteSubscriptionRegistry.hasExclusiveEvictionTombstone(anyString(), anyString())).thenReturn(false);

        // Avoid metrics manager dependency.
        doNothing().when(popLiteMessageProcessor).recordPopLiteMetrics(any(), anyString(), anyString());

        // Spy hook on popLiteTopic: capture per-lmq offsets, and inject the mid-pop ack when armed.
        Mockito.doAnswer(invocation -> {
            Object real = invocation.callRealMethod();
            String lmq = invocation.getArgument(3);
            @SuppressWarnings("unchecked")
            Pair<StringBuilder, GetMessageResult> pair = (Pair<StringBuilder, GetMessageResult>) real;
            if (pair != null && pair.getObject2() != null && pair.getObject2().getMessageCount() > 0) {
                List<Long> offsets = new ArrayList<>(pair.getObject2().getMessageQueueOffset());
                synchronized (LAST_POP_READS) {
                    LAST_POP_READS.computeIfAbsent(lmq, k -> new ArrayList<>()).addAll(offsets);
                }
                synchronized (RECEIVED) {
                    RECEIVED.computeIfAbsent(lmq, k -> new HashSet<>()).addAll(offsets);
                }
            }
            if (INJECT_ARMED.get() && lmq.equals(injectLmq)) {
                INJECT_ARMED.set(false);
                // Fire the ack of the first in-flight message concurrently with this pop. This advances
                // the offset, unblocks FIFO and re-dispatches the lmq back into this client's queue,
                // reproducing a duplicate for an already-visited lmq within the same popByClientId.
                ackLite(injectLmq, 0L, injectPopTime);
            }
            return real;
        }).when(popLiteMessageProcessor).popLiteTopic(anyString(), anyString(), anyString(), anyString(),
            anyLong(), anyLong(), anyLong(), anyString(), any());
    }

    @AfterClass
    public static void tearDown() {
        if (messageStore != null) {
            messageStore.shutdown();
            messageStore.destroy();
        }
        UtilAll.deleteFile(new File(storePathRootDir));
    }

    @Test
    public void testLitePopEventLossAcrossIterations() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            runOnce(i);
        }
    }

    private void runOnce(int iteration) throws Exception {
        String lt1 = "lt1-" + iteration + "-" + UUID.randomUUID();
        String lt2 = "lt2-" + iteration + "-" + UUID.randomUUID();
        String lmq1 = LiteUtil.toLmqName(PARENT_TOPIC, lt1);
        String lmq2 = LiteUtil.toLmqName(PARENT_TOPIC, lt2);

        RECEIVED.clear();

        // lt1 initially has only its first message readable; lt2 has all five.
        send(lt1, 1);
        send(lt2, 5);
        awaitReput(lmq1, 1);
        awaitReput(lmq2, 5);

        // Initialize consumer offsets so getPopOffset does not fall back to init-offset lookup.
        consumerOffsetManager.commitOffset("init", GROUP, lmq1, 0, 0L);
        consumerOffsetManager.commitOffset("init", GROUP, lmq2, 0, 0L);

        // The full subscription is used only by the fix's recovery path.
        LiteSubscription subscription = new LiteSubscription().setGroup(GROUP).setTopic(PARENT_TOPIC);
        Set<String> lmqSet = new HashSet<>();
        lmqSet.add(lmq1);
        lmqSet.add(lmq2);
        subscription.setLmqSet(lmqSet);
        when(liteSubscriptionRegistry.getLiteSubscription(CLIENT_ID)).thenReturn(subscription);

        enqueue(lmq1);
        enqueue(lmq2);

        // POP #1: reads lt1 offset 0 (now in-flight, FIFO-blocked) and lt2 offsets 0..4.
        long popTime1 = System.currentTimeMillis();
        pop(popTime1);

        // lt1's remaining four messages become readable now; a single coalesced arrival event enqueues lt1.
        send(lt1, 4);
        awaitReput(lmq1, 5);
        enqueue(lmq1);

        // POP #2: visits lt1 (blocked -> 0 read, added to processed); the spy fires lt1's offset-0 ack
        // mid-pop, which re-dispatches lt1. The buggy code drops the re-enqueued duplicate.
        injectLmq = lmq1;
        injectPopTime = popTime1;
        INJECT_ARMED.set(true);
        long popTime2 = System.currentTimeMillis() + 1;
        pop(popTime2);
        INJECT_ARMED.set(false);

        // Drain: keep popping and acking; on the fixed code lt1 is recovered and yields offsets 1..4.
        for (int r = 0; r < DRAIN_ROUNDS; r++) {
            Set<Long> lt1Received;
            synchronized (RECEIVED) {
                lt1Received = new HashSet<>(RECEIVED.getOrDefault(lmq1, Collections.emptySet()));
            }
            if (lt1Received.size() >= 5) {
                break;
            }
            long popTime = System.currentTimeMillis() + 2 + r;
            Map<String, List<Long>> reads = pop(popTime);
            for (Map.Entry<String, List<Long>> e : reads.entrySet()) {
                for (Long off : e.getValue()) {
                    ackLite(e.getKey(), off, popTime);
                }
            }
        }

        assertAllReceived(iteration, lmq1, lmq2);
    }

    private Map<String, List<Long>> pop(long popTime) {
        synchronized (LAST_POP_READS) {
            LAST_POP_READS.clear();
        }
        popLiteMessageProcessor.popByClientId(CLIENT_HOST, PARENT_TOPIC, GROUP, CLIENT_ID,
            popTime, INVISIBLE, MAX_NUM, UUID.randomUUID().toString());
        synchronized (LAST_POP_READS) {
            Map<String, List<Long>> copy = new HashMap<>();
            for (Map.Entry<String, List<Long>> e : LAST_POP_READS.entrySet()) {
                copy.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
            return copy;
        }
    }

    private void enqueue(String lmqName) {
        liteEventDispatcher.tryDispatchToClient(lmqName, CLIENT_ID, GROUP, false);
    }

    private void send(String liteTopic, int count) {
        for (int i = 0; i < count; i++) {
            messageStore.putMessage(org.apache.rocketmq.broker.lite.LiteTestUtil.buildMessage(PARENT_TOPIC, liteTopic));
        }
    }

    private void awaitReput(String lmqName, long expectedMaxOffset) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (messageStore.getMaxOffsetInQueue(lmqName, 0) >= expectedMaxOffset) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.fail("reput timeout for " + lmqName + ", expected max offset " + expectedMaxOffset
            + ", actual " + messageStore.getMaxOffsetInQueue(lmqName, 0));
    }

    /**
     * Replicates {@link AckMessageProcessor#ackLite} for a single lite message: advance the FIFO
     * order info, commit the consumer offset, and re-dispatch the lmq when it becomes unblocked.
     */
    private static void ackLite(String lmqName, long ackOffset, long popTime) {
        ConsumerOrderInfoManager orderInfoManager = popLiteMessageProcessor.getConsumerOrderInfoManager();
        PopConsumerLockService lockService = popLiteMessageProcessor.getLockService();
        String lockKey = KeyBuilder.buildPopLiteLockKey(GROUP, lmqName);
        while (!lockService.tryLock(lockKey)) {
            // spin, matching AckMessageProcessor.ackLite
        }
        try {
            long nextOffset = orderInfoManager.commitAndNext(lmqName, GROUP, 0, ackOffset, popTime);
            if (nextOffset > -1L) {
                if (!consumerOffsetManager.hasOffsetReset(lmqName, GROUP, 0)) {
                    consumerOffsetManager.commitOffset("AckLiteHost", GROUP, lmqName, 0, nextOffset);
                }
                if (!orderInfoManager.checkBlock(null, lmqName, GROUP, 0, INVISIBLE)) {
                    liteEventDispatcher.dispatch(GROUP, lmqName, 0, nextOffset, -1);
                }
            }
        } finally {
            lockService.unlock(lockKey);
        }
    }

    private void assertAllReceived(int iteration, String lmq1, String lmq2) {
        Set<Long> expected = new HashSet<>();
        for (long o = 0; o < 5; o++) {
            expected.add(o);
        }
        Set<Long> lt1;
        Set<Long> lt2;
        synchronized (RECEIVED) {
            lt1 = new HashSet<>(RECEIVED.getOrDefault(lmq1, Collections.emptySet()));
            lt2 = new HashSet<>(RECEIVED.getOrDefault(lmq2, Collections.emptySet()));
        }
        Assert.assertEquals("iteration " + iteration + ": lt2 must receive 5 messages", expected, lt2);
        Assert.assertEquals("iteration " + iteration + ": lt1 must receive 5 messages but got " + lt1
            + " (event loss reproduced: lt1 stuck after its first message)", expected, lt1);
    }
}
