package org.apache.rocketmq.broker.processor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PopRebalanceCacheManager {

    private static final long LOCK_TIMEOUT_MILLIS = 3000L;

    // 24 hours
    private static final long CLEAN_INTERVAL = 86400;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("PopRebalanceCacheCleanerThread")
    );

    // Cache with pop mode rebalancing under the condition that consumer groups and queues remain unchanged
    private final ConcurrentMap<String, ConcurrentHashMap<String, Set<MessageQueue>>> loadBalanceDateTable = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<String>> topicCidAll = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<MessageQueue>> topicMqAll = new ConcurrentHashMap<>();

    private final Lock popRebalanceCacheLock = new ReentrantLock();

    public PopRebalanceCacheManager() {
        startCacheCleaner();
    }

    public Set<MessageQueue> getLoadBalanceDate(List<MessageQueue> mqAll, List<String> cidAll, String topic,
                                                String clientId, String strategyName, int popShareQueueNum) {
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // Check if there is caches
                    ConcurrentHashMap<String, Set<MessageQueue>> topicCache = loadBalanceDateTable.get(topic);
                    if(topicCache == null) {
                        return null;
                    }

                    // Check whether the consumer group and queue information have changed
                    List<MessageQueue> oldMqAll = topicMqAll.get(topic);
                    List<String> oldCidAll = topicCidAll.get(topic);
                    if(oldMqAll == null || oldCidAll == null || !oldMqAll.equals(mqAll) || !oldCidAll.equals(cidAll)){
                        return null;
                    }

                    return topicCache.get(clientId + "_" + strategyName + "_" + popShareQueueNum);
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager getLoadBalanceDate Exception", e);
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

                    loadBalanceDateTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                            .put(clientId + "_" + strategyName + "_" + popShareQueueNum, loadBalanceDate);
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("PopRebalanceCacheManager putLoadBalanceDate Exception", e);
        }
    }

    private void startCacheCleaner() {
        executorService.scheduleAtFixedRate(() -> {
            try {
                this.cleanCache();
            } catch (Throwable e) {
                log.error("scheduleAtFixedRate cleanCache exception", e);
            }
        }, CLEAN_INTERVAL, CLEAN_INTERVAL, TimeUnit.SECONDS);
        log.info("PopRebalanceCacheCleaner started with interval: {} seconds", CLEAN_INTERVAL);
    }

    private void cleanCache() {
        try {
            if (this.popRebalanceCacheLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    log.info("Start cleaning PopRebalanceCache.");
                    loadBalanceDateTable.clear();
                    topicCidAll.clear();
                    topicMqAll.clear();
                    log.info("PopRebalanceCache cleaning finished.");
                } finally {
                    this.popRebalanceCacheLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.debug("Error cleaning PopRebalanceCache", e);
        }
    }

}
