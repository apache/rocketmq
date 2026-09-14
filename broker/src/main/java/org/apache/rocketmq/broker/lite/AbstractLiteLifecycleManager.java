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

package org.apache.rocketmq.broker.lite;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;

/**
 * Abstract class of lite lifecycle manager, which is used to manage the TTL of lite topics
 * and the validity of subscription. The subclasses provide file CQ and rocksdb CQ implementations.
 */
public abstract class AbstractLiteLifecycleManager extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);
    static final int MAX_INVALID_SCAN_COUNT = 5;

    protected final BrokerController brokerController;
    protected final String brokerName;
    protected final LiteSharding liteSharding;
    protected MessageStore messageStore;
    protected Map<String, Integer> ttlMap = Collections.emptyMap();
    protected Map<String, Set<String>> subscriberGroupMap = Collections.emptyMap();
    protected Map<String, Integer> offsetInvalidScanCountMap = new ConcurrentHashMap<>();
    protected Map<String, Integer> storeTimeInvalidScanCountMap = new ConcurrentHashMap<>();

    /**
     * Global prefix index over lmqName, maintained on the lmq lifecycle hot path and
     * consumed by {@link LiteEventDispatcher} wildcard full dispatch and lifecycle queries.
     */
    protected final LmqPrefixIndex lmqPrefixIndex = new LmqPrefixIndex();

    public AbstractLiteLifecycleManager(BrokerController brokerController, LiteSharding liteSharding) {
        this.brokerController = brokerController;
        this.brokerName = brokerController.getBrokerConfig().getBrokerName();
        this.liteSharding = liteSharding;
    }

    public boolean init() {
        this.messageStore = brokerController.getMessageStore();
        assert messageStore != null;
        return true;
    }

    /**
     * Populate the prefix index once at startup. Must be called after {@link #init()}.
     */
    public void bootstrapLmqPrefixIndex() {
        long start = System.currentTimeMillis();
        forEachLiteTopic(triple -> {
            lmqPrefixIndex.add(triple.getLeft());
            return true;
        });
        LOGGER.info("bootstrap lmq prefix index finish, indexed:{}, costMs:{}",
            lmqPrefixIndex.size(), System.currentTimeMillis() - start);
    }

    /**
     * Hook fired on the first message of a freshly created lmq.
     */
    public void onLmqCreate(String lmqName) {
        lmqPrefixIndex.add(lmqName);
    }

    /**
     * Hook fired when an lmq is deleted.
     */
    public void onLmqDelete(String lmqName) {
        lmqPrefixIndex.remove(lmqName);
    }

    /**
     * This method actually returns NEXT slot index to use, starting from 0
     */
    public abstract long getMaxOffsetInQueue(String lmqName);

    /**
     * Collect LMQ by parent topic
     * return lmq name list, not null
     */
    public List<String> collectByParentTopic(String parentTopic) {
        if (StringUtils.isEmpty(parentTopic)) {
            return Collections.emptyList();
        }
        List<String> resultList = new ArrayList<>();
        forEachLiteTopicByParent(parentTopic, triple -> {
            resultList.add(triple.getLeft());
            return true;
        });
        return resultList;
    }

    /**
     * Iterator of lite topic, for high frequency iteration
     * Triple<lmqName, maxOffsetInQueue, lastStoreTimestamp>, lastStoreTimestamp is null for now
     * return true to continue, false to break.
     *
     * @param function consumer func
     */
    public abstract void forEachLiteTopic(Function<Triple<String, Long, Long>, Boolean> function);

    /**
     * Delegate to {@link #forEachLiteTopicByPrefix} with prefix = LITE_TOPIC_PREFIX + parentTopic + SEPARATOR.
     *
     * @param parentTopic parent topic to filter by
     * @param function consumer func; caller must NOT add/remove lmqPrefixIndex inside the callback
     */
    public void forEachLiteTopicByParent(String parentTopic, Function<Triple<String, Long, Long>, Boolean> function) {
        forEachLiteTopicByPrefix(LiteUtil.LITE_TOPIC_PREFIX + parentTopic + LiteUtil.SEPARATOR, function);
    }

    /**
     * Iterator of lite topic filtered by lmqName prefix.
     * Triple<lmqName, maxOffsetInQueue, lastStoreTimestamp>, lastStoreTimestamp is null for now.
     * Entries with maxOffset <= 0 (no messages ever written) are skipped and will NOT be applied.
     * Return true to continue, false to break.
     *
     * @param prefix lmqName prefix to filter by
     * @param function consumer func; caller must NOT add/remove lmqPrefixIndex inside the callback
     */
    public void forEachLiteTopicByPrefix(String prefix, Function<Triple<String, Long, Long>, Boolean> function) {
        lmqPrefixIndex.forEachLmqByPrefix(prefix, lmqName -> {
            long maxOffset = getMaxOffsetInQueue(lmqName);
            if (maxOffset <= 0) {
                return true;
            }
            Triple<String, Long, Long> triple = Triple.of(lmqName, maxOffset, null);
            return function.apply(triple);
        });
    }

    /**
     * Check if the subscription for the given LMQ is active.
     * A subscription is considered active if either:
     * - the current broker is responsible for this LMQ according to the sharding strategy
     * - the LMQ exists (has messages) in the message store
     */
    public boolean isSubscriptionActive(String parentTopic, String lmqName) {
        return brokerName.equals(liteSharding.shardingByLmqName(parentTopic, lmqName)) || isLmqExist(lmqName);
    }

    public int getLiteTopicCount(String parentTopic) {
        if (!LiteMetadataUtil.isLiteMessageType(parentTopic, brokerController)) {
            return 0;
        }
        int[] count = {0};
        forEachLiteTopicByParent(parentTopic, triple -> {
            count[0]++;
            return true;
        });
        return count[0];
    }

    public boolean isLmqExist(String lmqName) {
        return getMaxOffsetInQueue(lmqName) > 0;
    }

    public void cleanExpiredLiteTopic() {
        try {
            long startMs = System.currentTimeMillis();
            updateMetadata(); // necessary
            int[] count = {0};
            forEachLiteTopic(triple -> {
                String lmqName = triple.getLeft();
                String parentTopic = LiteUtil.getParentTopic(lmqName);
                if (parentTopic == null) {
                    return true;
                }
                if (isLiteTopicExpired(parentTopic, lmqName, triple.getMiddle())) {
                    deleteLmq(parentTopic, lmqName);
                    count[0]++;
                }
                return true;
            });
            LOGGER.info("clean expired topic, size:{}, cost:{}ms", count[0], System.currentTimeMillis() - startMs);
            if (count[0] > 0) {
                brokerController.getMessageStore().getQueueStore().flush();
            }
        } catch (Exception e) {
            LOGGER.error("cleanExpiredLiteTopic error", e);
        }
    }

    public void cleanByParentTopic(String parentTopic) {
        try {
            if (!LiteMetadataUtil.isLiteMessageType(parentTopic, brokerController)) {
                return;
            }
            long startMs = System.currentTimeMillis();
            updateMetadata(); // necessary
            // collect-then-delete: forEachLiteTopicByParent and deleteLmq each hold a lock, nesting causes deadlock
            List<String> toDelete = new ArrayList<>();
            forEachLiteTopicByParent(parentTopic, triple -> {
                toDelete.add(triple.getLeft());
                return true;
            });
            toDelete.forEach(liteTopic -> deleteLmq(parentTopic, liteTopic));
            LOGGER.info("clean by parent topic:{}, size:{}, cost:{}ms", parentTopic, toDelete.size(), System.currentTimeMillis() - startMs);
        } catch (Exception e) {
            LOGGER.error("cleanByParentTopic error", e);
        }
    }

    @Override
    public void run() {
        LOGGER.info("Start checking lite ttl.");
        while (!this.isStopped()) {
            long runningTime = System.currentTimeMillis() - brokerController.getShouldStartTime();
            if (runningTime < brokerController.getBrokerConfig().getMinLiteTTl()) { // base protection for restart
                this.waitForRunning(20 * 1000);
                continue;
            }

            cleanExpiredLiteTopic();
            long checkInterval = brokerController.getBrokerConfig().getLiteTtlCheckInterval();
            this.waitForRunning(checkInterval);
        }
        LOGGER.info("End checking lite ttl.");
    }

    public void updateMetadata() {
        ttlMap = LiteMetadataUtil.getTopicTtlMap(brokerController);
        subscriberGroupMap = LiteMetadataUtil.getSubscriberGroupMap(brokerController);
    }

    public boolean isLiteTopicExpired(String parentTopic, String lmqName, long maxOffset) {
        if (!LiteUtil.isLiteTopicQueue(lmqName)) {
            return false;
        }
        int offsetInvalidCount = trackInvalidCount(lmqName, maxOffset <= 0, offsetInvalidScanCountMap);
        if (offsetInvalidCount > 0) {
            // check more times in case of concurrent issue
            LOGGER.warn("unexpected condition, max offset <= 0, {}, {}, scanCount:{}", lmqName, maxOffset, offsetInvalidCount);
            return offsetInvalidCount > MAX_INVALID_SCAN_COUNT;
        }
        long latestStoreTime = messageStore.getMessageStoreTimeStamp(lmqName, 0, maxOffset - 1);
        int storeTimeInvalidCount = trackInvalidCount(lmqName, latestStoreTime <= 0, storeTimeInvalidScanCountMap);
        if (storeTimeInvalidCount > 0) {
            // bypass TTL protection on purpose, but debounce against transient read failures
            LOGGER.warn("latest store time <= 0, {}, {}, scanCount:{}", lmqName, latestStoreTime, storeTimeInvalidCount);
            return storeTimeInvalidCount > MAX_INVALID_SCAN_COUNT;
        }
        long inactiveTime = System.currentTimeMillis() - latestStoreTime;
        if (inactiveTime < brokerController.getBrokerConfig().getMinLiteTTl()) {
            return false;
        }
        Integer minutes = ttlMap.get(parentTopic);
        if (null == minutes) {
            LOGGER.warn("unexpected condition, topic ttl not found. {}", lmqName);
            return false;
        }
        if (minutes <= 0) {
            return false;
        }
        if (hasConsumerLag(lmqName, maxOffset, latestStoreTime, parentTopic)) {
            return false;
        }
        return inactiveTime > TimeUnit.MINUTES.toMillis(minutes);
    }

    /**
     * Track the invalid state of the given lmq: increase the count when invalid, reset when recovered.
     * The counter is removed automatically once it exceeds {@link #MAX_INVALID_SCAN_COUNT}.
     *
     * @return the current invalid count, 0 means healthy (and the counter has been reset)
     */
    private int trackInvalidCount(String lmqName, boolean invalid, Map<String, Integer> invalidCountMap) {
        if (!invalid) {
            invalidCountMap.remove(lmqName);
            return 0;
        }
        int invalidCount = invalidCountMap.getOrDefault(lmqName, 0) + 1;
        if (invalidCount > MAX_INVALID_SCAN_COUNT) {
            invalidCountMap.remove(lmqName);
        } else {
            invalidCountMap.put(lmqName, invalidCount);
        }
        return invalidCount;
    }

    private void removeInvalidCount(String lmqName) {
        offsetInvalidScanCountMap.remove(lmqName);
        storeTimeInvalidScanCountMap.remove(lmqName);
    }

    public void deleteLmq(String parentTopic, String lmqName) {
        try {
            Set<String> groups = subscriberGroupMap.getOrDefault(parentTopic, Collections.emptySet());
            groups.forEach(group -> {
                String topicAtGroup = lmqName + TOPIC_GROUP_SEPARATOR + group;
                brokerController.getConsumerOffsetManager().getOffsetTable().remove(topicAtGroup);
                brokerController.getConsumerOffsetManager().eraseResetOffset(lmqName, group, 0);
                brokerController.getConsumerOffsetManager().removeConsumerOffset(topicAtGroup); // no iteration
                brokerController.getPopLiteMessageProcessor().getConsumerOrderInfoManager().remove(lmqName, group);
            });
            brokerController.getMessageStore().deleteTopics(Sets.newHashSet(lmqName));
            boolean sharding = brokerName.equals(liteSharding.shardingByLmqName(parentTopic, lmqName));
            brokerController.getLiteSubscriptionRegistry().cleanSubscription(lmqName, false);
            brokerController.getConsumerOffsetManager().getPullOffsetTable().remove(
                lmqName + TOPIC_GROUP_SEPARATOR + MixAll.TOOLS_CONSUMER_GROUP);
            removeInvalidCount(lmqName);
            onLmqDelete(lmqName);
            LOGGER.info("delete lmq finish. {}, sharding:{}", lmqName, sharding);
        } catch (Exception e) {
            LOGGER.error("delete lmq error. {}", lmqName, e);
        }
    }

    /**
     * Maybe we can check all subscriber groups, but currently consumer lag checking is not performed.
     * Only inactive time of message sending is considered for TTL expiration.
     */
    public boolean hasConsumerLag(String lmqName, long maxOffset, long latestStoreTime, String parentTopic) {
        return false;
    }
}
