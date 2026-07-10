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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Manages message deduplication using a time-based cache.
 * Tracks processed message keys with timestamps for expiration.
 */
public class MessageDeduplicator {
    private static final Logger log = LoggerFactory.getLogger(MessageDeduplicator.class);

    private final ConcurrentHashMap<String, Long> processedMessages;
    private final int maxCacheSize;
    private final long expireTimeMs;
    private final ScheduledExecutorService cleanupExecutor;

    /**
     * Constructor for MessageDeduplicator.
     *
     * @param maxCacheSize Maximum number of message keys to cache
     * @param expireTimeMs Cache entry expire time in milliseconds
     */
    public MessageDeduplicator(int maxCacheSize, long expireTimeMs) {
        this.maxCacheSize = maxCacheSize;
        this.expireTimeMs = expireTimeMs;
        this.processedMessages = new ConcurrentHashMap<>(maxCacheSize);

        // Initialize cleanup executor to run periodically
        String consumerGroupTag = "DedupCleanup_";
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl(consumerGroupTag));

        // Schedule cleanup task to run at half the expire interval
        long cleanupInterval = Math.max(expireTimeMs / 2, 5000);
        this.cleanupExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    cleanupExpiredEntries();
                } catch (Throwable e) {
                    log.error("MessageDeduplicator cleanup task exception", e);
                }
            }
        }, cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);

        log.info("MessageDeduplicator initialized with maxCacheSize={}, expireTimeMs={}, cleanupIntervalMs={}",
            maxCacheSize, expireTimeMs, cleanupInterval);
    }

    /**
     * Check if message has been processed recently.
     *
     * @param messageKey The key to check (msgId or user-defined key)
     * @return true if message should be skipped (already processed and not expired)
     */
    public boolean isDuplicate(String messageKey) {
        if (messageKey == null || messageKey.isEmpty()) {
            return false;
        }

        Long timestamp = processedMessages.get(messageKey);
        if (timestamp == null) {
            return false;
        }

        // Check if entry has expired
        long currentTime = System.currentTimeMillis();
        if (currentTime - timestamp > expireTimeMs) {
            // Entry expired, remove it only if the timestamp hasn't been updated by another thread
            // This prevents race condition with concurrent markProcessed() calls
            processedMessages.remove(messageKey, timestamp);
            return false;
        }

        return true;
    }

    /**
     * Mark message as processed.
     * If cache is full, trigger cleanup before adding new entry.
     *
     * @param messageKey The key to mark
     */
    public void markProcessed(String messageKey) {
        if (messageKey == null || messageKey.isEmpty()) {
            return;
        }

        // Check if cache is near capacity
        if (processedMessages.size() >= maxCacheSize) {
            cleanupExpiredEntries();
        }

        // Still too large after cleanup, remove oldest entries
        if (processedMessages.size() >= maxCacheSize) {
            removeOldestEntries(maxCacheSize / 10); // Remove 10% of cache
        }

        processedMessages.put(messageKey, System.currentTimeMillis());
    }

    /**
     * Clean up expired entries (called periodically).
     * Removes entries older than expireTimeMs.
     */
    public void cleanupExpiredEntries() {
        long currentTime = System.currentTimeMillis();
        int removedCount = 0;

        Iterator<Map.Entry<String, Long>> iterator = processedMessages.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (currentTime - entry.getValue() > expireTimeMs) {
                iterator.remove();
                removedCount++;
            }
        }

        if (removedCount > 0) {
            log.info("MessageDeduplicator cleanup completed. Removed {} expired entries, current size={}",
                removedCount, processedMessages.size());
        }
    }

    /**
     * Remove oldest entries when cache is full.
     *
     * @param count Number of oldest entries to remove
     */
    private void removeOldestEntries(int count) {
        // Sort entries by timestamp and remove oldest
        processedMessages.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .limit(count)
            .map(Map.Entry::getKey)
            .forEach(key -> processedMessages.remove(key));

        log.warn("MessageDeduplicator cache full, removed {} oldest entries. Current size={}",
            count, processedMessages.size());
    }

    /**
     * Get deduplication key from message.
     * Priority: Message.getKeys() > MessageExt.getMsgId()
     *
     * @param message The message
     * @return Deduplication key (user-defined keys or msgId)
     */
    public static String getDeduplicationKey(MessageExt message) {
        if (message == null) {
            return null;
        }

        // Prefer user-defined keys for business-level deduplication
        String keys = message.getKeys();
        if (keys != null && !keys.isEmpty()) {
            return keys;
        }

        // Fall back to msgId if no user keys defined
        String msgId = message.getMsgId();
        if (msgId != null && !msgId.isEmpty()) {
            return msgId;
        }

        return null;
    }

    /**
     * Get current cache size.
     *
     * @return Number of entries in cache
     */
    public int getCacheSize() {
        return processedMessages.size();
    }

    /**
     * Shutdown the deduplicator and cleanup executor.
     */
    public void shutdown() {
        if (cleanupExecutor != null) {
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        processedMessages.clear();
        log.info("MessageDeduplicator shutdown completed. Cache cleared.");
    }

    /**
     * Clear all entries in cache (for testing purposes).
     */
    public void clear() {
        processedMessages.clear();
    }
}