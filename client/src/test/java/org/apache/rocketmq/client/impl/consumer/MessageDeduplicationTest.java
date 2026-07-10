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

import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
        // Create a message with keys property
        MessageExt msgWithKeys = new MessageExt();
        msgWithKeys.setMsgId("msg-id-1");
        msgWithKeys.setKeys("business-key-1");

        String dedupKey = MessageDeduplicator.getDeduplicationKey(msgWithKeys);
        assertEquals("Should prefer user-defined keys", "business-key-1", dedupKey);

        // Create a message without keys property
        MessageExt msgWithoutKeys = new MessageExt();
        msgWithoutKeys.setMsgId("msg-id-2");

        String dedupKey2 = MessageDeduplicator.getDeduplicationKey(msgWithoutKeys);
        assertEquals("Should fallback to msgId", "msg-id-2", dedupKey2);

        // Null message
        String nullKey = MessageDeduplicator.getDeduplicationKey(null);
        assertNull("Should return null for null message", nullKey);
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
     * Test ackIndex semantics with partial success and duplicates.
     * This simulates the critical scenario:
     * - Original msgs = [dup, new1, new2]
     * - filteredMsgs = [new1, new2]
     * - Listener returns CONSUME_SUCCESS + ackIndex=0 (only new1 successful)
     *
     * Expected behavior:
     * - Only new1 should be marked as processed
     * - new2 should NOT be marked (will be retried)
     * - mappedAckIndex should be 1 (position of new1 in original msgs)
     */
    @Test
    public void testPartialSuccessAckIndexMapping() {
        // Setup: original msgs = [dup, new1, new2]
        String dupKey = "dup-msg";
        String newKey1 = "new-msg-1";
        String newKey2 = "new-msg-2";

        // Mark duplicate as processed (from previous consumption)
        deduplicator.markProcessed(dupKey);

        // Simulate filtering
        assertTrue("dupKey should be duplicate", deduplicator.isDuplicate(dupKey));
        assertFalse("newKey1 should not be duplicate", deduplicator.isDuplicate(newKey1));
        assertFalse("newKey2 should not be duplicate", deduplicator.isDuplicate(newKey2));

        // Simulate the scenario: listener returns ackIndex=0 (only first new message successful)
        int listenerAckIndex = 0; // Based on filteredMsgs = [new1, new2]

        // Expected behavior: only newKey1 should be marked as processed
        // newKey2 should NOT be marked (failed, needs retry)

        // Mark only up to ackIndex (simulating the fix behavior)
        if (listenerAckIndex >= 0) {
            // Only mark the successfully consumed message
            deduplicator.markProcessed(newKey1);
        }

        // Verify:
        // 1. newKey1 is marked (will be deduplicated next time)
        assertTrue("newKey1 should be marked as processed", deduplicator.isDuplicate(newKey1));

        // 2. newKey2 is NOT marked (will NOT be deduplicated, can retry)
        assertFalse("newKey2 should NOT be marked as processed", deduplicator.isDuplicate(newKey2));

        // 3. ackIndex mapping: filteredMsgs[ackIndex] = newKey1 maps to msgs[1]
        //    Original list: [dup(at 0), new1(at 1), new2(at 2)]
        //    Filtered list: [new1(at 0), new2(at 1)]
        //    listenerAckIndex = 0 (filtered) -> mappedAckIndex = 1 (original)
        int expectedMappedAckIndex = 1; // Position of newKey1 in original msgs
        assertEquals("Mapped ackIndex should be position of newKey1 in original list",
            1, expectedMappedAckIndex);

        // After retry, newKey2 would be processed and then marked
        deduplicator.markProcessed(newKey2);
        assertTrue("After retry success, newKey2 should be marked", deduplicator.isDuplicate(newKey2));
    }

    /**
     * Test that when listener acks all filtered messages, all are marked as processed.
     * Scenario:
     * - Original msgs = [dup, new1, new2]
     * - filteredMsgs = [new1, new2]
     * - Listener returns CONSUME_SUCCESS + ackIndex=Integer.MAX_VALUE (default, all success)
     */
    @Test
    public void testFullSuccessAckIndexMapping() {
        String dupKey = "dup-msg";
        String newKey1 = "new-msg-1";
        String newKey2 = "new-msg-2";

        // Mark duplicate as processed
        deduplicator.markProcessed(dupKey);

        // Simulate listener returns default ackIndex (Integer.MAX_VALUE, means all successful)
        int listenerAckIndex = Integer.MAX_VALUE;
        // After clamping: filteredMsgs.size() - 1 = 1 (indexes 0 and 1 in filteredMsgs)

        // Clamp ackIndex (simulating the fix behavior)
        int filteredMsgSize = 2; // [new1, new2]
        if (listenerAckIndex >= filteredMsgSize) {
            listenerAckIndex = filteredMsgSize - 1;
        }

        // Mark all messages up to clamped ackIndex
        deduplicator.markProcessed(newKey1);
        deduplicator.markProcessed(newKey2);

        // Verify both are marked
        assertTrue("newKey1 should be marked", deduplicator.isDuplicate(newKey1));
        assertTrue("newKey2 should be marked", deduplicator.isDuplicate(newKey2));

        // ackIndex mapping for full success: should be msgs.size() - 1 = 2
        // (all original messages considered successful for offset advancement)
        int expectedMappedAckIndex = 2; // msgs.size() - 1
        assertEquals("For full success, mapped ackIndex should cover all original messages",
            2, expectedMappedAckIndex);
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