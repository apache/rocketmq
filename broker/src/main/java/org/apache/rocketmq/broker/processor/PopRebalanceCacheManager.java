package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PopRebalanceCacheManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long LOCK_TIMEOUT_MILLIS = 3000L;

    private static final long CACHE_EXPIRE_TIME = 86400 * 1000L;

    // Cache with pop mode rebalancing under the condition that consumer groups and queues remain unchanged
    private final ConcurrentMap<String, CacheEntry> loadBalanceDateTable = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<String>> topicCidAll = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<MessageQueue>> topicMqAll = new ConcurrentHashMap<>();

    private final Lock popRebalanceCacheLock = new ReentrantLock();

    private volatile long lastCleanTime = System.currentTimeMillis();
    private static final long CLEAN_INTERVAL = 3600 * 1000L;

    public Set<MessageQueue> getLoadBalanceData(List<MessageQueue> mqAll, List<String> cidAll, String topic,
                                                String clientId, String strategyName, int popShareQueueNum) {
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    cleanExpiredCacheIfNeeded();

                    // Check if there is caches
                    CacheEntry cacheEntry = loadBalanceDateTable.get(topic);
                    if (cacheEntry == null || cacheEntry.isExpired()) {
                        if (cacheEntry != null) {
                            loadBalanceDateTable.remove(topic);
                            topicCidAll.remove(topic);
                            topicMqAll.remove(topic);
                        }
                        return null;
                    }

                    // Check whether the consumer group and queue information have changed
                    List<MessageQueue> oldMqAll = topicMqAll.get(topic);
                    List<String> oldCidAll = topicCidAll.get(topic);
                    if (oldMqAll == null || oldCidAll == null || !oldMqAll.equals(mqAll) || !oldCidAll.equals(cidAll)) {
                        loadBalanceDateTable.remove(topic);
                        topicCidAll.remove(topic);
                        topicMqAll.remove(topic);
                        return null;
                    }

                    cacheEntry.updateAccessTime();
                    return cacheEntry.getData().get(clientId + "_" + strategyName + "_" + popShareQueueNum);
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager getLoadBalanceData Exception", e);
        }

        return null;
    }

    public void putLoadBalanceDate(List<MessageQueue> mqAll, List<String> cidAll, String topic, String clientId, String strategyName, int popShareQueueNum,
                                   Set<MessageQueue> loadBalanceDate) {
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicCidAll.put(topic, cidAll);
                    topicMqAll.put(topic, mqAll);

                    CacheEntry cacheEntry = loadBalanceDateTable.computeIfAbsent(topic, k -> new CacheEntry());
                    cacheEntry.getData().put(clientId + "_" + strategyName + "_" + popShareQueueNum, loadBalanceDate);
                    cacheEntry.updateAccessTime();
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager putLoadBalanceDate Exception", e);
        }
    }

    private void cleanExpiredCacheIfNeeded() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastCleanTime < CLEAN_INTERVAL) {
            return;
        }

        lastCleanTime = currentTime;

        loadBalanceDateTable.entrySet().removeIf(entry -> {
            if (entry.getValue().isExpired()) {
                String topic = entry.getKey();
                topicCidAll.remove(topic);
                topicMqAll.remove(topic);
                return true;
            }
            return false;
        });
    }

    /**
     * Cache entry with timestamp
      */
    private static class CacheEntry {
        private final ConcurrentHashMap<String, Set<MessageQueue>> data;
        private volatile long lastAccessTime;

        public CacheEntry() {
            this.data = new ConcurrentHashMap<>();
            this.lastAccessTime = System.currentTimeMillis();
        }

        public void updateAccessTime() {
            this.lastAccessTime = System.currentTimeMillis();
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - lastAccessTime > CACHE_EXPIRE_TIME;
        }

        public ConcurrentHashMap<String, Set<MessageQueue>> getData() {
            return data;
        }
    }
}
