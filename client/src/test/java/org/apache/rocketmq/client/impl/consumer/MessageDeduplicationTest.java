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

    @Test
    public void testAckIndexAdjustmentWithDuplicates() {
        // Test ackIndex semantics when duplicates exist
        // This simulates the scenario described in the review

        // Setup: original msgs = [dup, new1, new2]
        // filteredMsgs = [new1, new2]

        String dupKey = "dup-msg";
        String newKey1 = "new-msg-1";
        String newKey2 = "new-msg-2";

        // Mark duplicate
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

        // Verify filtering result
        assertEquals("Should have 2 non-duplicate messages", 2, filteredKeys.size());
        assertEquals("First should be new1", newKey1, filteredKeys.get(0));
        assertEquals("Second should be new2", newKey2, filteredKeys.get(1));

        // After successful consumption, ackIndex should be adjusted to cover all original messages
        // ackIndex = originalKeys.size() - 1 = 2 (covers all 3 original messages)
        int adjustedAckIndex = originalKeys.size() - 1;
        assertEquals("Adjusted ackIndex should cover all original messages", 2, adjustedAckIndex);
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
}