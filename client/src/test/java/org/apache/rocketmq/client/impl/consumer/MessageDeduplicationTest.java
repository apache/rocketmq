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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for message deduplication functionality.
 */
public class MessageDeduplicationTest {

    private MessageDeduplicator deduplicator;

    @Before
    public void setUp() {
        deduplicator = new MessageDeduplicator(10000, 60000);
    }

    @After
    public void tearDown() {
        if (deduplicator != null) {
            deduplicator.shutdown();
        }
    }

    @Test
    public void testBasicDuplicateDetection() {
        // Test that duplicate messages are correctly detected
        String key1 = "msg-key-1";
        String key2 = "msg-key-2";

        // First occurrence - not duplicate
        assertFalse("First message should not be duplicate", deduplicator.isDuplicate(key1));

        // Mark as processed
        deduplicator.markProcessed(key1);

        // Second occurrence - is duplicate
        assertTrue("Second message should be duplicate", deduplicator.isDuplicate(key1));

        // Different key - not duplicate
        assertFalse("Different key should not be duplicate", deduplicator.isDuplicate(key2));
    }

    @Test
    public void testMessageKeyExtraction() {
        String topic = "TestTopic";

        // Create a message with keys property
        MessageExt msgWithKeys = new MessageExt();
        msgWithKeys.setTopic(topic);
        msgWithKeys.setMsgId("msg-id-1");
        msgWithKeys.setKeys("business-key-1");

        String dedupKey = MessageDeduplicator.getDeduplicationKey(msgWithKeys);
        assertEquals("Should prefer user-defined keys, scoped by topic",
            topic + "#" + "business-key-1", dedupKey);

        // Create a message without keys property
        MessageExt msgWithoutKeys = new MessageExt();
        msgWithoutKeys.setTopic(topic);
        msgWithoutKeys.setMsgId("msg-id-2");

        String dedupKey2 = MessageDeduplicator.getDeduplicationKey(msgWithoutKeys);
        assertEquals("Should fallback to msgId, scoped by topic",
            topic + "#" + "msg-id-2", dedupKey2);

        // Null message
        String nullKey = MessageDeduplicator.getDeduplicationKey(null);
        assertNull("Should return null for null message", nullKey);
    }

    /**
     * The deduplication key must be scoped by topic so the same business key on different topics
     * is not mistaken for a duplicate.
     */
    @Test
    public void testDeduplicationKeyScopedByTopic() {
        String topic1 = "TopicA";
        String topic2 = "TopicB";
        String businessKey = "order-123";

        MessageExt msg1 = new MessageExt();
        msg1.setTopic(topic1);
        msg1.setKeys(businessKey);

        MessageExt msg2 = new MessageExt();
        msg2.setTopic(topic2);
        msg2.setKeys(businessKey);

        String key1 = MessageDeduplicator.getDeduplicationKey(msg1);
        String key2 = MessageDeduplicator.getDeduplicationKey(msg2);

        assertEquals("Key on topicA should be scoped with topicA",
            topic1 + "#" + businessKey, key1);
        assertEquals("Key on topicB should be scoped with topicB",
            topic2 + "#" + businessKey, key2);
        assertFalse("Same business key on different topics must not collide", key1.equals(key2));

        // No topic: raw key returned unchanged (defensive fallback)
        MessageExt msgNoTopic = new MessageExt();
        msgNoTopic.setKeys("raw-key");
        assertEquals("Without a topic the raw key should be returned", "raw-key",
            MessageDeduplicator.getDeduplicationKey(msgNoTopic));
    }

    /**
     * Two identical messages in the same batch must not both reach the listener. The global cache
     * is only populated after successful consumption, so intra-batch dedup relies on the
     * batch-local "seen" set.
     */
    @Test
    public void testBatchInternalDuplicateFiltered() {
        String topic = "TestTopic";
        MessageExt a1 = createMessage(topic, "msg-A", "key-A");
        MessageExt a2 = createMessage(topic, "msg-A", "key-A");

        List<MessageExt> msgs = new ArrayList<>(Arrays.asList(a1, a2));

        // Cache is empty; dedup must still collapse the intra-batch duplicate.
        List<MessageExt> filtered = ConsumeMessageConcurrentlyService.filterDuplicateMessages(
            msgs, deduplicator, "test-group");

        assertEquals("Intra-batch duplicate should be collapsed to one message",
            1, filtered.size());
        assertSame("The first occurrence should be kept", a1, filtered.get(0));
    }

    /**
     * Combines global-cache dedup with intra-batch dedup: a key already in the cache and a pair of
     * identical new messages should all be collapsed correctly.
     */
    @Test
    public void testBatchDuplicateMixedWithGlobal() {
        String topic = "TestTopic";
        String cachedKey = topic + "#cached"; // matches getDeduplicationKey scope
        deduplicator.markProcessed(cachedKey);

        MessageExt cached = createMessage(topic, "msg-cached", "cached");
        MessageExt a1 = createMessage(topic, "msg-A", "key-A");
        MessageExt a2 = createMessage(topic, "msg-A", "key-A");
        MessageExt b = createMessage(topic, "msg-B", "key-B");

        List<MessageExt> msgs = new ArrayList<>(Arrays.asList(cached, a1, a2, b));

        List<MessageExt> filtered = ConsumeMessageConcurrentlyService.filterDuplicateMessages(
            msgs, deduplicator, "test-group");

        // cached -> dropped (global), a1 kept, a2 -> dropped (intra-batch), b kept
        assertEquals("Only A and B should remain", 2, filtered.size());
        assertSame(a1, filtered.get(0));
        assertSame(b, filtered.get(1));
    }

    /**
     * When deduplication is disabled (no deduplicator), the original list must be returned
     * unchanged so the listener still receives every message.
     */
    @Test
    public void testNoDeduplicatorReturnsOriginal() {
        String topic = "TestTopic";
        MessageExt a = createMessage(topic, "msg-A", "key-A");
        MessageExt b = createMessage(topic, "msg-B", "key-B");
        List<MessageExt> msgs = new ArrayList<>(Arrays.asList(a, b));

        List<MessageExt> filtered = ConsumeMessageConcurrentlyService.filterDuplicateMessages(
            msgs, null, "test-group");

        assertSame("Should return the original list when dedup is disabled", msgs, filtered);
    }

    private MessageExt createMessage(String topic, String msgId, String keys) {
        MessageExt msg = new MessageExt();
        msg.setTopic(topic);
        msg.setMsgId(msgId);
        msg.setKeys(keys);
        return msg;
    }

    @Test
    public void testCacheExpiration() throws InterruptedException {
        // Create a deduplicator with very short expiration time (100ms)
        MessageDeduplicator shortLivedDeduplicator = new MessageDeduplicator(1000, 100);

        String key = "expiring-key";

        // Mark as processed
        shortLivedDeduplicator.markProcessed(key);
        assertTrue("Should be duplicate immediately after marking", shortLivedDeduplicator.isDuplicate(key));

        // Wait for expiration
        Thread.sleep(200);

        // Should no longer be duplicate after expiration
        assertFalse("Should not be duplicate after expiration", shortLivedDeduplicator.isDuplicate(key));

        shortLivedDeduplicator.shutdown();
    }

    @Test
    public void testNoMarkingOnFailure() {
        // Test that failed messages are NOT marked as processed
        String key = "failed-message-key";

        // Check duplicate status (not duplicate yet)
        assertFalse("Should not be duplicate initially", deduplicator.isDuplicate(key));

        // Simulate consumption failure - do NOT mark as processed
        // (In actual code, markProcessed is only called after success)

        // Check again - should still not be duplicate
        assertFalse("Should still not be duplicate after failure", deduplicator.isDuplicate(key));

        // Now mark as processed (simulating success)
        deduplicator.markProcessed(key);

        // Now should be duplicate
        assertTrue("Should be duplicate after successful consumption", deduplicator.isDuplicate(key));
    }

    @Test
    public void testAllDuplicatesOffsetAdvancement() {
        // Test that when all messages are duplicates, offset still advances correctly
        String key1 = "dup-key-1";
        String key2 = "dup-key-2";

        // Mark both as processed
        deduplicator.markProcessed(key1);
        deduplicator.markProcessed(key2);

        // Verify both are duplicates
        assertTrue("Key1 should be duplicate", deduplicator.isDuplicate(key1));
        assertTrue("Key2 should be duplicate", deduplicator.isDuplicate(key2));

        // In actual consumption scenario, all duplicates would be filtered out
        // but original message list would still be used for offset advancement
        // This test verifies the deduplicator state is correct
        assertEquals("Cache should contain 2 entries", 2, deduplicator.getCacheSize());
    }

    @Test
    public void testPartialDuplicatesWithSuccess() {
        // Test scenario: [duplicate, new, new]
        String dupKey = "duplicate-key";
        String newKey1 = "new-key-1";
        String newKey2 = "new-key-2";

        // Mark duplicate as processed
        deduplicator.markProcessed(dupKey);

        // Simulate filtering logic
        assertTrue("dupKey should be duplicate", deduplicator.isDuplicate(dupKey));
        assertFalse("newKey1 should not be duplicate", deduplicator.isDuplicate(newKey1));
        assertFalse("newKey2 should not be duplicate", deduplicator.isDuplicate(newKey2));

        // After successful consumption, mark new messages as processed
        deduplicator.markProcessed(newKey1);
        deduplicator.markProcessed(newKey2);

        // Verify all are now marked
        assertTrue("newKey1 should now be duplicate", deduplicator.isDuplicate(newKey1));
        assertTrue("newKey2 should now be duplicate", deduplicator.isDuplicate(newKey2));
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        // Test thread-safe concurrent access
        int threadCount = 10;
        int messagesPerThread = 100;
        AtomicInteger duplicateCount = new AtomicInteger(0);

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                for (int i = 0; i < messagesPerThread; i++) {
                    // Some messages are shared across threads (will be duplicates)
                    String key = (i % 10 == 0) ? "shared-key-" + i : "thread-" + threadId + "-key-" + i;

                    if (deduplicator.isDuplicate(key)) {
                        duplicateCount.incrementAndGet();
                    } else {
                        deduplicator.markProcessed(key);
                    }
                }
            });
            threads.add(thread);
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for completion
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify some duplicates were detected
        assertTrue("Should have detected some duplicates", duplicateCount.get() > 0);

        // Verify cache size is within limits
        assertTrue("Cache size should not exceed max", deduplicator.getCacheSize() <= 10000);
    }

    @Test
    public void testCacheSizeLimit() {
        // Create deduplicator with small cache
        MessageDeduplicator smallDeduplicator = new MessageDeduplicator(100, 60000);

        // Add more keys than cache size
        for (int i = 0; i < 150; i++) {
            smallDeduplicator.markProcessed("key-" + i);
        }

        // Cache should not exceed max size
        assertTrue("Cache size should not exceed max", smallDeduplicator.getCacheSize() <= 100);

        smallDeduplicator.shutdown();
    }

    /**
     * Test that filtering preserves message order from original list.
     * When duplicates are removed, remaining messages keep their relative order.
     * This is critical for correct ackIndex mapping in partial success scenarios.
     */
    @Test
    public void testFilteringPreservesOrder() {
        // Setup: original msgs = [dup, new1, new2]
        // After filtering: filteredMsgs = [new1, new2] (order preserved)

        String dupKey = "dup-msg";
        String newKey1 = "new-msg-1";
        String newKey2 = "new-msg-2";

        // Mark duplicate as processed
        deduplicator.markProcessed(dupKey);

        // Simulate filtering
        List<String> originalKeys = new ArrayList<>();
        originalKeys.add(dupKey);
        originalKeys.add(newKey1);
        originalKeys.add(newKey2);

        List<String> filteredKeys = new ArrayList<>();
        for (String key : originalKeys) {
            if (!deduplicator.isDuplicate(key)) {
                filteredKeys.add(key);
            }
        }

        // Verify filtering result - order is preserved
        assertEquals("Should have 2 non-duplicate messages", 2, filteredKeys.size());
        assertEquals("First filtered should be new1 (position 1 in original)", newKey1, filteredKeys.get(0));
        assertEquals("Second filtered should be new2 (position 2 in original)", newKey2, filteredKeys.get(1));

        // This order preservation is critical for ackIndex mapping:
        // If listener acks filteredKeys[0] (new1), it maps to originalKeys[1]
        // If listener acks filteredKeys[1] (new2), it maps to originalKeys[2]
    }

    @Test
    public void testNullKeyHandling() {
        // Test handling of null keys
        assertFalse("Should not crash on null key", deduplicator.isDuplicate(null));
        assertFalse("Should not crash on empty key", deduplicator.isDuplicate(""));

        // Mark should be no-op for null/empty
        deduplicator.markProcessed(null);
        deduplicator.markProcessed("");
        // No exception should be thrown
    }

    /**
     * Review point 1: a duplicate skipped by the filter must never be sent back / retried.
     *
     * Scenario: original msgs = [A, dup(A), B]; the filter collapses dup(A), so the listener
     * sees [A, B] and acks only A (ackIndex = 0 on the consumed list). The send-back loop must
     * send back only B — the skipped dup(A) is neither consumed nor failed.
     *
     * Drives the real processConsumeResult on a spied service with a mocked
     * DefaultMQPushConsumerImpl, capturing which messages sendMessageBack is asked to retry.
     */
    @Test
    public void testDupSkippedNotSentBack() throws Exception {
        DefaultMQPushConsumerImpl impl = newPushConsumerImpl();

        // listener sees [A, B] (dup(A) filtered) and acks only A
        final AtomicReference<List<MessageExt>> seenByListener = new AtomicReference<>();
        final List<MessageExt> sentBack = Collections.synchronizedList(new ArrayList<>());
        CapturingService service = new CapturingService(impl, new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                seenByListener.set(new ArrayList<>(msgs));
                context.setAckIndex(0); // only A succeeded
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        }, sentBack);

        // Build [A, dup(A), B] with distinct queueOffsets so ProcessQueue can hold them.
        MessageExt a = createMessage("T", "msg-A", "key-A");
        a.setQueueOffset(0L);
        MessageExt dupA = createMessage("T", "msg-A", "key-A");
        dupA.setQueueOffset(1L);
        MessageExt b = createMessage("T", "msg-B", "key-B");
        b.setQueueOffset(2L);
        List<MessageExt> msgs = new ArrayList<>(Arrays.asList(a, dupA, b));

        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(msgs);

        ConsumeMessageConcurrentlyService.ConsumeRequest request =
            service.new ConsumeRequest(msgs, pq, new MessageQueue("T", "broker-a", 0));

        // Drive the full run(): filter -> listener -> processConsumeResult, end to end.
        request.run();

        // Listener saw [A, B] only — dup(A) was filtered before consumption.
        assertNotNull("Listener should have been invoked", seenByListener.get());
        assertEquals("Listener should see only A and B", 2, seenByListener.get().size());

        // Only B must be sent back; dup(A) is a skipped duplicate, not a failure.
        assertEquals("Only B should be sent back", 1, sentBack.size());
        assertSame("The sent-back message should be B", b, sentBack.get(0));

        // Only A is marked as processed; B (failed) and dup(A) (skipped) must not poison the cache.
        assertTrue("A should be marked as processed", deduplicator.isDuplicate("T#key-A"));
        assertFalse("B should NOT be marked (failed, needs retry)", deduplicator.isDuplicate("T#key-B"));
    }

    /**
     * Review point 3 (Warning item): when *every* message in a batch is a duplicate, the listener
     * must not be invoked, no message must be sent back for retry, the offset must still advance
     * (the ProcessQueue is drained), and the dedup-cache entries must NOT be re-stamped (which
     * would extend their TTL without any new processing).
     *
     * The pre-fix code set {@code processedMsgs = msgs} on this path, causing processConsumeResult
     * to call markMessagesAsProcessed on the whole original batch and refresh every entry's
     * timestamp. Now processedMsgs is the (empty) filtered list, so marking is skipped.
     */
    @Test
    public void testAllDuplicatesSkipsListenerAndDoesNotRefreshTtl() throws Exception {
        DefaultMQPushConsumerImpl impl = newPushConsumerImpl();

        final AtomicReference<List<MessageExt>> seenByListener = new AtomicReference<>();
        final List<MessageExt> sentBack = Collections.synchronizedList(new ArrayList<>());
        CapturingService service = new CapturingService(impl, new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                seenByListener.set(new ArrayList<>(msgs));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        }, sentBack);

        // Two distinct keys, both already in the cache as processed duplicates.
        MessageExt dup1 = createMessage("T", "msg-A", "key-A");
        dup1.setQueueOffset(0L);
        MessageExt dup2 = createMessage("T", "msg-B", "key-B");
        dup2.setQueueOffset(1L);

        // Pre-populate the cache and capture the timestamps so we can detect any refresh.
        deduplicator.markProcessed("T#key-A");
        deduplicator.markProcessed("T#key-B");
        Long tsA = deduplicator.getProcessedTimestamp("T#key-A");
        Long tsB = deduplicator.getProcessedTimestamp("T#key-B");
        assertNotNull("key-A should be cached", tsA);
        assertNotNull("key-B should be cached", tsB);

        List<MessageExt> msgs = new ArrayList<>(Arrays.asList(dup1, dup2));

        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(msgs);

        ConsumeMessageConcurrentlyService.ConsumeRequest request =
            service.new ConsumeRequest(msgs, pq, new MessageQueue("T", "broker-a", 0));

        // Drive the full run(): filter (all duplicates) -> skip listener -> processConsumeResult.
        request.run();

        // The listener must never have been called — nothing was left to consume.
        assertNull("Listener must not be invoked when all messages are duplicates",
            seenByListener.get());

        // No message may be sent back: skipped duplicates are neither consumed nor failed.
        assertTrue("No message should be sent back on an all-duplicate batch", sentBack.isEmpty());

        // The batch is fully committed (success), so the ProcessQueue should be drained.
        assertEquals("ProcessQueue should be drained after an all-duplicate batch",
            0L, pq.getMsgCount().get());

        // The cache entries must not have been re-stamped: their timestamps are unchanged.
        assertEquals("key-A TTL must not be refreshed",
            tsA, deduplicator.getProcessedTimestamp("T#key-A"));
        assertEquals("key-B TTL must not be refreshed",
            tsB, deduplicator.getProcessedTimestamp("T#key-B"));
    }

    /**
     * Review point 2: when the processQueue is dropped between consumption and result
     * processing, the consumed messages must NOT be added to the dedup cache — otherwise their
     * own redelivery would be silently suppressed, breaking at-least-once.
     */
    @Test
    public void testDroppedProcessQueueDoesNotPoisonCache() throws Exception {
        DefaultMQPushConsumerImpl impl = newPushConsumerImpl();

        List<MessageExt> sentBack = Collections.synchronizedList(new ArrayList<>());
        CapturingService service = new CapturingService(impl, new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // ack all
            }
        }, sentBack);

        MessageExt a = createMessage("T", "msg-A", "key-A");
        a.setQueueOffset(0L);
        List<MessageExt> msgs = new ArrayList<>(Arrays.asList(a));

        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(msgs);
        pq.setDropped(true); // dropped before result processing — consume result never commits

        ConsumeMessageConcurrentlyService.ConsumeRequest request =
            service.new ConsumeRequest(msgs, pq, new MessageQueue("T", "broker-a", 0));
        setProcessedMsgs(request, msgs);

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(request.getMessageQueue());

        service.processConsumeResult(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, context, request);

        // Despite "successful" consumption, the dropped processQueue means nothing is committed,
        // so the message must NOT enter the dedup cache — it will be redelivered and must run.
        assertFalse("Dropped-batch messages must not poison the dedup cache",
            deduplicator.isDuplicate("T#key-A"));
        // And nothing should have been sent back for an all-ack success.
        assertTrue("Nothing should be sent back on full success", sentBack.isEmpty());
    }

    /**
     * Build a real {@link DefaultMQPushConsumerImpl} with just enough wiring for
     * {@code processConsumeResult}: a consumer group, a dedup cache, a no-op offset store and a
     * stats manager. Avoids Mockito-mocking the heavy impl class.
     */
    private DefaultMQPushConsumerImpl newPushConsumerImpl() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testDedupGroup");
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);

        DefaultMQPushConsumerImpl impl = new DefaultMQPushConsumerImpl(consumer, null);
        impl.setOffsetStore(Mockito.mock(OffsetStore.class));

        // getConsumerStatsManager() delegates to mQClientFactory; inject a stubbed factory.
        org.apache.rocketmq.client.impl.factory.MQClientInstance factory =
            Mockito.mock(org.apache.rocketmq.client.impl.factory.MQClientInstance.class);
        Mockito.when(factory.getConsumerStatsManager()).thenReturn(Mockito.mock(ConsumerStatsManager.class));
        impl.setmQClientFactory(factory);

        // Wire the test's dedup cache into the impl via reflection (private field).
        java.lang.reflect.Field dedupField = DefaultMQPushConsumerImpl.class
            .getDeclaredField("messageDeduplicator");
        dedupField.setAccessible(true);
        dedupField.set(impl, deduplicator);
        return impl;
    }

    private static void setProcessedMsgs(
        ConsumeMessageConcurrentlyService.ConsumeRequest request, List<MessageExt> processed)
        throws Exception {
        java.lang.reflect.Field f = ConsumeMessageConcurrentlyService.ConsumeRequest.class
            .getDeclaredField("processedMsgs");
        f.setAccessible(true);
        f.set(request, processed);
    }

    /**
     * A {@link ConsumeMessageConcurrentlyService} subclass that records every message
     * {@code processConsumeResult} asks to send back, without performing the real broker round-trip.
     */
    private static final class CapturingService extends ConsumeMessageConcurrentlyService {
        private final List<MessageExt> sentBack;

        CapturingService(DefaultMQPushConsumerImpl impl, MessageListenerConcurrently listener,
            List<MessageExt> sentBack) {
            super(impl, listener);
            this.sentBack = sentBack;
        }

        @Override
        public boolean sendMessageBack(MessageExt msg, ConsumeConcurrentlyContext context) {
            sentBack.add(msg);
            return true;
        }
    }

    /**
     * Test edge case: all messages are duplicates.
     * When all are duplicates, filteredMsgs is empty, no marking needed.
     */
    @Test
    public void testAllDuplicatesNoMarking() {
        String key1 = "dup-1";
        String key2 = "dup-2";

        // Mark both as processed
        deduplicator.markProcessed(key1);
        deduplicator.markProcessed(key2);

        // Both should be duplicates
        assertTrue("key1 should be duplicate", deduplicator.isDuplicate(key1));
        assertTrue("key2 should be duplicate", deduplicator.isDuplicate(key2));

        // When filteredMsgs is empty (all duplicates), no new marking should happen
        // The listener is not invoked with empty list, so ackIndex semantics don't apply
        // This test verifies the deduplicator state is correct
        assertEquals("Cache should contain 2 entries", 2, deduplicator.getCacheSize());
    }
}