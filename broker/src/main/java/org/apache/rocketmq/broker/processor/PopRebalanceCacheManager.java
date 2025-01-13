package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PopRebalanceCacheManager {
    private static final long LOCK_TIMEOUT_MILLIS = 3000L;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // Cache with pop mode rebalancing under the condition that consumer groups and queues remain unchanged
    private final ConcurrentMap<String, ConcurrentHashMap<String, Set<MessageQueue>>> loadBalanceDateTable = new ConcurrentHashMap<>();

    // Version numbers to avoid cache invalidation during the allocation of old requests, resulting in incorrect cache updates
    private final ConcurrentMap<String, Long> versionMap = new ConcurrentHashMap<>();

    private final Lock popRebalanceCacheLock = new ReentrantLock();

    public long getVersion(String topic) {
        long version = 0L;
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                version = versionMap.getOrDefault(topic, 0L);
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager getVersion Exception", e);
        }
        return version;
    }

    public Set<MessageQueue> getLoadBalanceDate(String topic, String clientId, String strategyName, int popShareQueueNum) {
        Set<MessageQueue> loadBalanceDateSet = null;
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    ConcurrentHashMap<String, Set<MessageQueue>> topicCache = loadBalanceDateTable.get(topic);
                    loadBalanceDateSet = topicCache.get(clientId + "_" + strategyName + "_" + popShareQueueNum);
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager getLoadBalanceDate Exception", e);
        }
        return loadBalanceDateSet;
    }

    public void putLoadBalanceDate(String topic, String clientId, String strategyName, int popShareQueueNum,
                                   Set<MessageQueue> loadBalanceDate, long oldVersion) {
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    if (getVersion(topic) == oldVersion) {
                        loadBalanceDateTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                                .put(clientId + "_" + strategyName + "_" + popShareQueueNum, loadBalanceDate);
                        versionMap.put(topic, oldVersion + 1);
                    }
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager putLoadBalanceDate Exception", e);
        }
    }

    public void removeTopicCaches(Set<String> topics) {
        for (String topic : topics) {
            removeTopicCache(topic);
        }
    }

    public void removeTopicCache(String topic) {
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    loadBalanceDateTable.remove(topic);
                    versionMap.merge(topic, 1L, Long::sum);
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager removeTopicCache Exception", e);
        }
    }
}
