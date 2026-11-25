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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.lite.LiteSubscriptionRegistry;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLiteLongPollingService;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.MemoryConsumerOrderInfoManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
import org.apache.rocketmq.broker.pop.orderly.QueueLevelConsumerManager.OrderInfo;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Attempt index and lease state of a same-attemptId lite pop retry, at the points a real request flow cannot pin
 * down: exact lease state after a release, and pops of one attempt interleaved at a chosen point. Events and
 * messages are stubbed here; the recovery scenarios through real components are in
 * {@link PopLiteAttemptRecoveryTest}.
 */
public class PopLiteMessageProcessorAttemptReleaseTest {
    private static final String GROUP = "group";
    private static final String CLIENT = "client";
    private static final String LMQ1 = LiteUtil.toLmqName("parent", "one");
    private static final String LMQ2 = LiteUtil.toLmqName("parent", "two");
    private static final long INVISIBLE = 600000;
    private final long firstPopTime = System.currentTimeMillis() - 1000;
    private final BrokerController broker = mock(BrokerController.class);
    private final ConsumerOffsetManager offsets = mock(ConsumerOffsetManager.class);
    private final LiteSubscriptionRegistry registry = mock(LiteSubscriptionRegistry.class);
    private final LiteEventDispatcher dispatcher = mock(LiteEventDispatcher.class);
    private final Map<String, Deque<String>> events = new HashMap<>();
    private final Map<String, Integer> available = new HashMap<>();
    private PopLiteMessageProcessor processor;
    private MemoryConsumerOrderInfoManager manager;
    private String dispatchClient = CLIENT;
    // the first read of this lmq waits on readResume, to interleave two pops of one client
    private volatile String blockedLmq;
    private final CountDownLatch readEntered = new CountDownLatch(1);
    private final CountDownLatch readResume = new CountDownLatch(1);
    private final Map<Integer, RemotingCommand> responses = new ConcurrentHashMap<>();
    private ChannelHandlerContext requestContext;
    private PopLiteLongPollingService polling;

    @Before
    public void setUp() {
        BrokerConfig config = new BrokerConfig();
        config.setEnableNotifyAfterPopOrderLockRelease(false);
        when(broker.getBrokerConfig()).thenReturn(config);
        when(broker.getConsumerOffsetManager()).thenReturn(offsets);
        when(broker.getSubscriptionGroupManager()).thenReturn(mock(SubscriptionGroupManager.class));
        when(broker.getLiteSubscriptionRegistry()).thenReturn(registry);
        // no pending offset reset, otherwise the mock returns 0 and the pop takes the reset branch
        when(offsets.queryThenEraseResetOffset(anyString(), anyString(), anyInt())).thenReturn(null);
        when(dispatcher.getEventIterator(anyString())).thenAnswer(invocation -> {
            Deque<String> queue = queue(invocation.getArgument(0));
            return new Iterator<String>() {
                public boolean hasNext() {
                    return !queue.isEmpty();
                }

                public String next() {
                    return queue.poll();
                }
            };
        });
        processor = spy(new PopLiteMessageProcessor(broker, dispatcher));
        manager = (MemoryConsumerOrderInfoManager) processor.getConsumerOrderInfoManager();
        doNothing().when(processor).recordPopLiteMetrics(any(), anyString(), anyString());
        doAnswer(invocation -> {
            String lmq = invocation.getArgument(1);
            String key = KeyBuilder.buildPopLiteLockKey(GROUP, lmq);
            // the event is dispatched after the queue lock is released
            assertTrue(processor.getLockService().tryLock(key));
            processor.getLockService().unlock(key);
            queue(dispatchClient).add(lmq);
            return null;
        }).when(dispatcher).dispatch(anyString(), anyString(), anyInt(), anyLong(), anyLong());
        MessageStore store = mock(MessageStore.class);
        when(broker.getMessageStore()).thenReturn(store);
        when(store.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
            .thenAnswer(invocation -> {
                String lmq = invocation.getArgument(1);
                if (lmq.equals(blockedLmq)) {
                    blockedLmq = null;
                    readEntered.countDown();
                    assertTrue(readResume.await(5, TimeUnit.SECONDS));
                }
                long offset = invocation.getArgument(3);
                int count = Math.min((Integer) invocation.getArgument(4), Math.max(0, available.getOrDefault(lmq, 0) - (int) offset));
                GetMessageResult result = new GetMessageResult();
                result.setStatus(count > 0 ? GetMessageStatus.FOUND : GetMessageStatus.NO_MESSAGE_IN_QUEUE);
                for (int i = 0; i < count; i++) {
                    result.addMessage(new SelectMappedBufferResult(0, ByteBuffer.wrap(new byte[] {1}), 1, null));
                    result.getMessageQueueOffset().add(offset + i);
                }
                result.setNextBeginOffset(offset + count);
                return result;
            });
        available.put(LMQ1, 2);
        available.put(LMQ2, 1);
    }

    @After
    public void tearDown() {
        manager.shutdown();
    }

    private Deque<String> queue(String client) {
        return events.computeIfAbsent(client, key -> new ArrayDeque<>());
    }

    private OrderInfo order(String lmq) {
        return manager.getTable().get(lmq + "@" + GROUP).get(0);
    }

    private int attemptRecordCount() {
        return ((Map<?, ?>) readField(processor, "attempts")).size();
    }

    private int attemptLeaseCount(String clientId) {
        Map<?, ?> attempts = (Map<?, ?>) readField(processor, "attempts");
        Object attempt = attempts.get(clientId);
        return attempt == null ? -1 : ((Set<?>) readField(attempt, "lmqNames")).size();
    }

    private static Object readField(Object target, String fieldName) {
        try {
            return FieldUtils.readField(target, fieldName, true);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    // a newly arrived request, which releases the leases of its lost attempt before popping
    private Pair<StringBuilder, GetMessageResult> pop(String client, String attempt, long time, int maxNum) {
        return arrive(System.nanoTime(), client, attempt, time, maxNum);
    }

    private Pair<StringBuilder, GetMessageResult> arrive(long arriveTime, String client, String attempt, long time,
        int maxNum) {
        if (attempt != null) {
            processor.onArrival(GROUP, client, attempt, arriveTime);
        }
        return processor.popByClientId("host", "parent", GROUP, client, time, INVISIBLE, maxNum, attempt, arriveTime);
    }

    // a request woken from long polling, which pops only while it is still the latest request of the client
    private Pair<StringBuilder, GetMessageResult> wake(long arriveTime, String client, String attempt, long time,
        int maxNum) {
        if (processor.hasNewerRequest(client, arriveTime)) {
            return new Pair<>(new StringBuilder(), new GetMessageResult());
        }
        return processor.popByClientId("host", "parent", GROUP, client, time, INVISIBLE, maxNum, attempt, arriveTime);
    }

    // attempt A pops [0, 1] from LMQ1 and [0] from LMQ2, consuming both events
    private void firstBatch() {
        queue(CLIENT).addAll(Arrays.asList(LMQ1, LMQ2));
        assertEquals(3, pop(CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount());
        assertTrue(queue(CLIENT).isEmpty());
    }

    private void prepareRequestFlow() throws Exception {
        doReturn(null).when(processor).preCheck(any(), any(), any());
        when(broker.getBrokerStatsManager()).thenReturn(mock(BrokerStatsManager.class));
        when(broker.getBrokerMetricsManager()).thenReturn(mock(BrokerMetricsManager.class));
        polling = mock(PopLiteLongPollingService.class);
        when(polling.polling(any(), any(), anyLong(), anyLong(), anyString(), anyString()))
            .thenReturn(PollingResult.POLLING_TIMEOUT);
        FieldUtils.writeField(processor, "popLiteLongPollingService", polling, true);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10911));
        ChannelFuture written = mock(ChannelFuture.class);
        when(written.addListener(any())).thenReturn(written);
        when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
            RemotingCommand response = invocation.getArgument(0);
            responses.put(response.getOpaque(), response);
            return written;
        });
        requestContext = mock(ChannelHandlerContext.class);
        when(requestContext.channel()).thenReturn(channel);
        clearInvocations(processor, dispatcher);
    }

    private RemotingCommand processRetry() throws Exception {
        PopLiteMessageRequestHeader header = new PopLiteMessageRequestHeader();
        header.setClientId(CLIENT);
        header.setConsumerGroup(GROUP);
        header.setTopic("parent");
        header.setAttemptId("A");
        header.setMaxMsgNum(3);
        header.setInvisibleTime(INVISIBLE);
        header.setBornTime(System.currentTimeMillis());
        header.setPollTime(20000L);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_LITE_MESSAGE, header);
        request.makeCustomHeaderToNet();
        RemotingCommand response = processor.processRequest(requestContext, request);
        return response != null ? response : responses.remove(request.getOpaque());
    }

    private String lockLastLease() {
        Map<?, ?> attempts = (Map<?, ?>) readField(processor, "attempts");
        Set<?> leases = (Set<?>) readField(attempts.get(CLIENT), "lmqNames");
        Iterator<?> iterator = leases.iterator();
        iterator.next();
        String last = (String) iterator.next();
        assertTrue(processor.getLockService().tryLock(KeyBuilder.buildPopLiteLockKey(GROUP, last)));
        return last;
    }

    @Test
    public void testIncompleteReleaseReturnsBusyWithoutPopping() throws Exception {
        firstBatch();
        prepareRequestFlow();
        String pending = lockLastLease();
        try {
            RemotingCommand response = processRetry();
            assertNotNull(response);
            assertEquals(ResponseCode.SYSTEM_BUSY, response.getCode());
            assertNull(response.getBody());
            assertEquals(1, attemptLeaseCount(CLIENT));
            assertEquals(firstPopTime, order(pending).getPopTime());
            String released = pending.equals(LMQ1) ? LMQ2 : LMQ1;
            assertEquals(-firstPopTime, order(released).getPopTime());
            assertEquals(Collections.singletonList(released), Arrays.asList(queue(CLIENT).toArray()));
            verify(dispatcher, never()).getEventIterator(anyString());
            verify(polling, never()).polling(any(), any(), anyLong(), anyLong(), anyString(), anyString());
        } finally {
            processor.getLockService().unlock(KeyBuilder.buildPopLiteLockKey(GROUP, pending));
        }
    }

    @Test
    public void testSameAttemptRetryCompletesReleaseAfterLockRecovers() throws Exception {
        firstBatch();
        prepareRequestFlow();
        String pending = lockLastLease();
        RemotingCommand incomplete;
        try {
            incomplete = processRetry();
            assertEquals(firstPopTime, order(pending).getPopTime());
        } finally {
            processor.getLockService().unlock(KeyBuilder.buildPopLiteLockKey(GROUP, pending));
        }
        RemotingCommand recovered = processRetry();
        assertNotNull(recovered);
        assertEquals(ResponseCode.SUCCESS, recovered.getCode());
        assertEquals(3, recovered.getBody().length);
        assertEquals(2, attemptLeaseCount(CLIENT));
        assertEquals(order(LMQ1).getPopTime(), order(LMQ2).getPopTime());
        assertTrue(queue(CLIENT).isEmpty());
        assertNotNull(incomplete);
        assertEquals(ResponseCode.SYSTEM_BUSY, incomplete.getCode());
        assertNull(incomplete.getBody());
        verify(dispatcher, times(1)).getEventIterator(CLIENT);
    }

    @Test
    public void testSupersededRecoveryReturnsBusyAndKeepsNewLeases() throws Exception {
        firstBatch();
        prepareRequestFlow();
        CountDownLatch beforeLock = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean(true);
        PopConsumerLockService locks = spy(processor.getLockService());
        FieldUtils.writeField(processor, "lockService", locks, true);
        doAnswer(invocation -> {
            if (pause.compareAndSet(true, false)) {
                beforeLock.countDown();
                assertTrue(resume.await(5, TimeUnit.SECONDS));
            }
            return invocation.callRealMethod();
        }).when(locks).tryLock(anyString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<RemotingCommand> stale = executor.submit(this::processRetry);
        try {
            assertTrue(beforeLock.await(5, TimeUnit.SECONDS));
            RemotingCommand latest = processRetry();
            assertNotNull(latest);
            assertEquals(ResponseCode.SUCCESS, latest.getCode());
            assertEquals(3, latest.getBody().length);
            long latestPopTime = order(LMQ1).getPopTime();
            assertTrue(latestPopTime > firstPopTime);
            resume.countDown();
            RemotingCommand superseded = stale.get(5, TimeUnit.SECONDS);
            assertEquals(latestPopTime, order(LMQ1).getPopTime());
            assertEquals(latestPopTime, order(LMQ2).getPopTime());
            assertEquals(2, attemptLeaseCount(CLIENT));
            assertNotNull(superseded);
            assertEquals(ResponseCode.SYSTEM_BUSY, superseded.getCode());
            assertNull(superseded.getBody());
            verify(dispatcher, times(1)).getEventIterator(CLIENT);
            verify(polling, never()).polling(any(), any(), anyLong(), anyLong(), anyString(), anyString());
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testNewAttemptAndOtherClientCannotReleaseOldLeases() {
        firstBatch();
        assertEquals(0, pop(CLIENT, "B", System.currentTimeMillis(), 3).getObject2().getMessageCount());
        assertEquals(0, pop("other", "A", System.currentTimeMillis(), 3).getObject2().getMessageCount());
        assertEquals(firstPopTime, order(LMQ1).getPopTime());
        assertTrue(manager.checkBlock("C", LMQ1, GROUP, 0, INVISIBLE));
    }

    @Test
    public void testReleasePreservesStateAndRejectsOldAckRenewAndSuspend() {
        firstBatch();
        OrderInfo old = order(LMQ1);
        old.setCommitOffsetBit(1);
        old.setOffsetConsumedCount(Collections.singletonMap(1L, 4));
        manager.updateNextVisibleTime(LMQ1, GROUP, 0, 1, firstPopTime, System.currentTimeMillis() + INVISIBLE * 10);

        assertTrue(manager.releaseQueue(LMQ1, GROUP, 0, "A"));
        assertSame(old, order(LMQ1));
        assertNull(old.getOffsetNextVisibleTime());
        assertEquals(1, old.getCommitOffsetBit());
        assertEquals(Integer.valueOf(4), old.getOffsetConsumedCount().get(1L));

        assertEquals(-2, manager.commitAndNext(LMQ1, GROUP, 0, 1, firstPopTime));
        manager.updateNextVisibleTime(LMQ1, GROUP, 0, 1, firstPopTime, System.currentTimeMillis() + INVISIBLE * 20);
        manager.suspendQueue(LMQ1, GROUP, 0, firstPopTime, System.currentTimeMillis() + INVISIBLE * 20);
        assertNull(old.getOffsetNextVisibleTime());
        assertFalse(manager.checkBlock("B", LMQ1, GROUP, 0, INVISIBLE));
        assertFalse(manager.releaseQueue(LMQ1, GROUP, 0, "A"));
        assertEquals(firstPopTime, -old.getPopTime());
    }

    @Test
    public void testRetryReleasesLeaseRenewedBySameAttemptInOverlappingPop() {
        firstBatch();
        // a stale request of the same attempt re-pops LMQ1 later, so its lease is newer than the recorded batch
        long stalePopTime = firstPopTime + 500;
        manager.update("A", false, LMQ1, GROUP, 0, stalePopTime, INVISIBLE, Arrays.asList(0L, 1L), new StringBuilder());

        long retryTime = System.currentTimeMillis();
        assertEquals(3, pop(CLIENT, "A", retryTime, 3).getObject2().getMessageCount());
        assertEquals(retryTime, order(LMQ1).getPopTime());
        assertEquals(-2, manager.commitAndNext(LMQ1, GROUP, 0, 0, stalePopTime));
    }

    @Test
    public void testRecoveryDoesNotReleaseLeaseTakenOverByAnotherAttempt() {
        firstBatch();
        long newer = System.currentTimeMillis();
        manager.update("B", false, LMQ1, GROUP, 0, newer, INVISIBLE, Arrays.asList(0L, 1L), new StringBuilder());
        pop(CLIENT, "A", newer + 1, 3);
        assertEquals(newer, order(LMQ1).getPopTime());
        assertEquals("B", order(LMQ1).getAttemptId());
    }

    @Test
    public void testFullyAcknowledgedQueuesAreNotReleasedOrDispatched() {
        firstBatch();
        order(LMQ1).setCommitOffsetBit(3);
        order(LMQ2).setCommitOffsetBit(1);
        assertEquals(0, pop(CLIENT, "A", System.currentTimeMillis(), 3).getObject2().getMessageCount());
        assertEquals(firstPopTime, order(LMQ1).getPopTime());
        assertTrue(queue(CLIENT).isEmpty());
    }

    @Test
    public void testOverlappingSameAttemptPopsKeepAllQueuesReleasable() throws Exception {
        // R1 is woken while still the latest request, reads LMQ1 and pauses before recording its attempt
        long r1 = System.nanoTime();
        assertEquals(0, arrive(r1, CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount());
        blockedLmq = LMQ1;
        queue(CLIENT).add(LMQ1);
        AtomicInteger staleCount = new AtomicInteger(-1);
        Thread stale = new Thread(() ->
            staleCount.set(wake(r1, CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount()));
        stale.start();
        assertTrue(readEntered.await(5, TimeUnit.SECONDS));

        // meanwhile the retry R2 of the same attempt reads LMQ2 and records it first
        queue(CLIENT).add(LMQ2);
        assertEquals(1, pop(CLIENT, "A", firstPopTime + 1, 3).getObject2().getMessageCount());
        readResume.countDown();
        stale.join(5000);
        assertEquals(2, staleCount.get());
        // R1 recorded LMQ1 to the retry and re-dispatched it, which the long polling retry would take
        assertEquals(LMQ1, queue(CLIENT).poll());

        // both responses are lost, so retrying A must release the leases of both queues
        dispatchClient = "other";
        pop(CLIENT, "A", System.currentTimeMillis(), 3);
        assertFalse(manager.checkBlock("other", LMQ1, GROUP, 0, INVISIBLE));
        assertFalse(manager.checkBlock("other", LMQ2, GROUP, 0, INVISIBLE));
    }

    @Test
    public void testRetryRepopsLeaseRecordedByPopInFlightAtItsArrival() throws Exception {
        // R1 of attempt A reads LMQ1 and pauses before recording its lease
        long r1 = System.nanoTime();
        blockedLmq = LMQ1;
        queue(CLIENT).add(LMQ1);
        AtomicInteger staleCount = new AtomicInteger(-1);
        Thread stale = new Thread(() ->
            staleCount.set(arrive(r1, CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount()));
        stale.start();
        assertTrue(readEntered.await(5, TimeUnit.SECONDS));

        // the response of R1 is lost, and its retry R2 arrives with nothing to release and nothing to pop
        long r2 = System.nanoTime();
        long retryTime = System.currentTimeMillis();
        assertEquals(0, arrive(r2, CLIENT, "A", retryTime, 3).getObject2().getMessageCount());
        readResume.countDown();
        stale.join(5000);
        assertEquals(2, staleCount.get());

        // R1 records its lease to R2 and re-dispatches LMQ1, so R2 woken from long polling re-pops the batch
        assertEquals(Collections.singletonList(LMQ1), Arrays.asList(queue(CLIENT).toArray()));
        assertEquals(2, wake(r2, CLIENT, "A", retryTime, 3).getObject2().getMessageCount());
        assertEquals(retryTime, order(LMQ1).getPopTime());
        assertEquals(1, attemptLeaseCount(CLIENT));
    }

    @Test
    public void testLeaseRecordedAfterRetryIsReleasedForAnotherClient() throws Exception {
        // R1 of attempt A reads LMQ1 and pauses before recording its lease
        long r1 = System.nanoTime();
        blockedLmq = LMQ1;
        queue(CLIENT).add(LMQ1);
        AtomicInteger staleCount = new AtomicInteger(-1);
        Thread stale = new Thread(() ->
            staleCount.set(arrive(r1, CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount()));
        stale.start();
        assertTrue(readEntered.await(5, TimeUnit.SECONDS));

        // the response of R1 is lost, and its retry R2 arrives with nothing to release and nothing to pop
        assertEquals(0, arrive(System.nanoTime(), CLIENT, "A", System.currentTimeMillis(), 3)
            .getObject2().getMessageCount());
        // the event re-dispatched by R1 goes to another client of the group, as in shared consumption or migration
        dispatchClient = "other";
        readResume.countDown();
        stale.join(5000);
        assertEquals(2, staleCount.get());
        assertEquals(Collections.singletonList(LMQ1), Arrays.asList(queue("other").toArray()));
        // the lease released by R1 is not recorded to the retry
        assertEquals(0, attemptLeaseCount(CLIENT));

        // the lease of the lost R1 must not block the client taking the event
        assertEquals(2, pop("other", "C", System.currentTimeMillis(), 3).getObject2().getMessageCount());
    }

    @Test
    public void testNewerRequestBeforeQueueLockKeepsRetryLease() throws Exception {
        // R1 of attempt A takes the LMQ1 event and pauses before the queue lock
        CountDownLatch beforeLock = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (pause.compareAndSet(true, false)) {
                beforeLock.countDown();
                assertTrue(resume.await(5, TimeUnit.SECONDS));
            }
            return invocation.callRealMethod();
        }).when(processor).popLiteTopic(anyString(), anyString(), anyString(), anyString(), anyLong(), anyLong(),
            anyLong(), anyString(), any(), anyString(), anyLong());
        queue(CLIENT).add(LMQ1);
        AtomicInteger staleCount = new AtomicInteger(-1);
        Thread stale = new Thread(() ->
            staleCount.set(pop(CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount()));
        stale.start();
        assertTrue(beforeLock.await(5, TimeUnit.SECONDS));

        // its retry R2 pops LMQ1 on another event, and its response is being delivered
        long retryTime = System.currentTimeMillis();
        queue(CLIENT).add(LMQ1);
        assertEquals(2, pop(CLIENT, "A", retryTime, 3).getObject2().getMessageCount());
        resume.countDown();
        stale.join(5000);

        // R1 leaves the lease of R2 intact and passes its taken event on
        assertEquals(0, staleCount.get());
        assertEquals(retryTime, order(LMQ1).getPopTime());
        assertTrue(manager.checkBlock("C", LMQ1, GROUP, 0, INVISIBLE));
        assertEquals(Collections.singletonList(LMQ1), Arrays.asList(queue(CLIENT).toArray()));
    }

    @Test
    public void testFailedRedispatchKeepsMessagesRead() throws Exception {
        doThrow(new RejectedExecutionException("rejected"))
            .when(dispatcher).dispatch(anyString(), anyString(), anyInt(), anyLong(), anyLong());
        // R1 of attempt A reads LMQ1 and pauses before recording its lease
        blockedLmq = LMQ1;
        queue(CLIENT).add(LMQ1);
        long r1 = System.nanoTime();
        AtomicInteger staleCount = new AtomicInteger(-1);
        Thread stale = new Thread(() ->
            staleCount.set(arrive(r1, CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount()));
        stale.start();
        assertTrue(readEntered.await(5, TimeUnit.SECONDS));

        // the retry makes R1 release and re-dispatch, and the re-dispatch fails
        arrive(System.nanoTime(), CLIENT, "A", System.currentTimeMillis(), 3);
        readResume.countDown();
        stale.join(5000);
        assertEquals(2, staleCount.get());
    }

    @Test
    public void testReleaseWaitingForQueueLockIsNotInterruptedByAnotherAttempt() throws Exception {
        firstBatch();
        // hold both queue locks, so the release of retry R1 stalls before touching any lease
        String lock1 = KeyBuilder.buildPopLiteLockKey(GROUP, LMQ1);
        String lock2 = KeyBuilder.buildPopLiteLockKey(GROUP, LMQ2);
        assertTrue(processor.getLockService().tryLock(lock1));
        assertTrue(processor.getLockService().tryLock(lock2));
        long r1 = System.nanoTime();
        Thread retry = new Thread(() -> processor.onArrival(GROUP, CLIENT, "A", r1));
        try {
            retry.start();
            awaitTrue(() -> processor.hasNewerRequest(CLIENT, r1 - 1));

            // a new attempt inherits nothing, so giving way to it would leave the leases of A unreleased
            processor.onArrival(GROUP, CLIENT, "B", System.nanoTime());
            assertTrue(retry.isAlive());
            processor.getLockService().unlock(lock1);
            processor.getLockService().unlock(lock2);
            retry.join(5000);
        } finally {
            processor.getLockService().unlock(lock1);
            processor.getLockService().unlock(lock2);
        }
        assertFalse(retry.isAlive());

        // R1 released both leases of A and re-dispatched them
        assertFalse(manager.checkBlock("C", LMQ1, GROUP, 0, INVISIBLE));
        assertFalse(manager.checkBlock("C", LMQ2, GROUP, 0, INVISIBLE));
        assertEquals(2, queue(CLIENT).size());
        assertTrue(queue(CLIENT).containsAll(Arrays.asList(LMQ1, LMQ2)));
    }

    @Test
    public void testRecordsAreBoundedByClientsNotRequests() {
        // every delivered batch is followed by a new attempt, which replaces the record of the client
        for (int i = 0; i < 1000; i++) {
            available.put(LMQ1, i + 1);
            queue(CLIENT).add(LMQ1);
            manager.clearBlock(LMQ1, GROUP, 0);
            when(offsets.queryOffset(GROUP, LMQ1, 0)).thenReturn((long) i);
            assertEquals(1, pop(CLIENT, "attempt-" + i, System.currentTimeMillis(), 1).getObject2().getMessageCount());
            assertEquals(1, attemptLeaseCount(CLIENT));
        }
        pop("other", "X", System.currentTimeMillis(), 1);
        // a request without an attemptId leaves no record
        pop("third", null, System.currentTimeMillis(), 1);
        assertEquals(2, attemptRecordCount());
    }

    @Test
    public void testInterruptedReleaseIsTakenOverByNewerRetry() throws Exception {
        firstBatch();
        // the stalled R1 may hold a queue lock briefly while giving up, so the lock is not asserted on dispatch here
        doAnswer(invocation -> queue(dispatchClient).add(invocation.getArgument(1)))
            .when(dispatcher).dispatch(anyString(), anyString(), anyInt(), anyLong(), anyLong());
        // hold both queue locks, so the release of retry R1 stalls before touching any lease
        String lock1 = KeyBuilder.buildPopLiteLockKey(GROUP, LMQ1);
        String lock2 = KeyBuilder.buildPopLiteLockKey(GROUP, LMQ2);
        assertTrue(processor.getLockService().tryLock(lock1));
        assertTrue(processor.getLockService().tryLock(lock2));
        long r1 = System.nanoTime();
        Thread stalled = new Thread(() -> processor.onArrival(GROUP, CLIENT, "A", r1));
        stalled.start();
        awaitTrue(() -> processor.hasNewerRequest(CLIENT, r1 - 1));

        // retry R2 of the same attempt arrives meanwhile, takes over the release and re-pops the batch
        long r2 = System.nanoTime();
        long retryTime = System.currentTimeMillis();
        AtomicInteger retryCount = new AtomicInteger(-1);
        Thread retry = new Thread(() ->
            retryCount.set(arrive(r2, CLIENT, "A", retryTime, 3).getObject2().getMessageCount()));
        retry.start();
        awaitTrue(() -> processor.hasNewerRequest(CLIENT, r2 - 1));
        processor.getLockService().unlock(lock1);
        processor.getLockService().unlock(lock2);
        stalled.join(5000);
        retry.join(5000);

        // R1 released nothing after R2 arrived, so the leases R2 re-took are intact
        assertEquals(3, retryCount.get());
        assertEquals(retryTime, order(LMQ1).getPopTime());
        assertEquals(retryTime, order(LMQ2).getPopTime());
        assertEquals(2, attemptLeaseCount(CLIENT));
    }

    @Test
    public void testEventTakenAfterRetryArrivesIsPassedOnNotDeduped() {
        // like the real dispatcher, an event of an lmq already queued for the client is merged
        doAnswer(invocation -> {
            String lmq = invocation.getArgument(1);
            if (!queue(dispatchClient).contains(lmq)) {
                queue(dispatchClient).add(lmq);
            }
            return null;
        }).when(dispatcher).dispatch(anyString(), anyString(), anyInt(), anyLong(), anyLong());
        // R1 of attempt A reads LMQ1, and a later notification of LMQ1 is queued behind
        queue(CLIENT).addAll(Arrays.asList(LMQ1, LMQ1));
        long r1 = System.nanoTime();
        long r2 = r1 + 1;
        AtomicInteger taken = new AtomicInteger();
        when(dispatcher.getEventIterator(CLIENT)).thenAnswer(invocation -> new Iterator<String>() {
            public boolean hasNext() {
                return !queue(CLIENT).isEmpty();
            }

            public String next() {
                if (taken.incrementAndGet() == 2) {
                    // the retry R2 arrives right before R1 takes the event, and its re-dispatch is merged into it
                    processor.onArrival(GROUP, CLIENT, "A", r2);
                }
                return queue(CLIENT).poll();
            }
        });
        assertEquals(2, arrive(r1, CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount());

        // R1 passes the event on, so R2 woken from long polling re-pops the released batch
        assertEquals(Collections.singletonList(LMQ1), Arrays.asList(queue(CLIENT).toArray()));
        long retryTime = System.currentTimeMillis();
        assertEquals(2, wake(r2, CLIENT, "A", retryTime, 3).getObject2().getMessageCount());
        assertEquals(retryTime, order(LMQ1).getPopTime());
    }

    @Test
    public void testExpiredRecordKeepsItsLeases() {
        firstBatch();
        // within the ttl a retry of the same attempt may still arrive, so the record stays
        processor.cleanAttempts(System.nanoTime() + PopLiteMessageProcessor.ATTEMPT_TTL / 2);
        assertEquals(2, attemptLeaseCount(CLIENT));

        // past the ttl only the record is cleaned, the batch may still be consumed under its leases
        processor.cleanAttempts(System.nanoTime() + PopLiteMessageProcessor.ATTEMPT_TTL + 1);
        assertEquals(-1, attemptLeaseCount(CLIENT));
        assertEquals(firstPopTime, order(LMQ1).getPopTime());
        assertEquals(firstPopTime, order(LMQ2).getPopTime());
        assertTrue(manager.checkBlock("C", LMQ1, GROUP, 0, INVISIBLE));
        assertTrue(manager.checkBlock("C", LMQ2, GROUP, 0, INVISIBLE));
        assertTrue(queue(CLIENT).isEmpty());
    }

    private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline);
            Thread.sleep(1);
        }
    }

    @Test
    public void testLeaseRecordedAfterNewerAttemptIsReleasedForAnotherClient() throws Exception {
        // R1 of attempt A reads LMQ1 and pauses before recording its attempt
        blockedLmq = LMQ1;
        queue(CLIENT).add(LMQ1);
        AtomicInteger olderCount = new AtomicInteger(-1);
        Thread older = new Thread(() ->
            olderCount.set(pop(CLIENT, "A", firstPopTime, 3).getObject2().getMessageCount()));
        older.start();
        assertTrue(readEntered.await(5, TimeUnit.SECONDS));

        // meanwhile the newer R2 of attempt B reads LMQ2 and records it first
        queue(CLIENT).add(LMQ2);
        assertEquals(1, pop(CLIENT, "B", System.currentTimeMillis(), 3).getObject2().getMessageCount());
        dispatchClient = "other";
        readResume.countDown();
        older.join(5000);
        assertEquals(2, olderCount.get());

        // R1 releases its own lease and re-dispatches LMQ1, leaving the lease of B recorded
        assertFalse(manager.checkBlock("other", LMQ1, GROUP, 0, INVISIBLE));
        assertEquals(Collections.singletonList(LMQ1), Arrays.asList(queue("other").toArray()));
        assertTrue(manager.checkBlock("other", LMQ2, GROUP, 0, INVISIBLE));
        assertEquals(1, attemptLeaseCount(CLIENT));

        // the response of B is lost, so retrying B releases its lease
        pop(CLIENT, "B", System.currentTimeMillis(), 3);
        assertFalse(manager.checkBlock("other", LMQ2, GROUP, 0, INVISIBLE));
    }
}
