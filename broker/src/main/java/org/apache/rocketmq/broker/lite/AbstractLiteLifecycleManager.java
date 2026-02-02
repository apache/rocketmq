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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;

/**
 * Abstract class of lite lifecycle manager, which is used to manage the TTL of lite topics
 * and the validity of subscription. The subclasses provide file CQ and rocksdb CQ implementations.
 */
public abstract class AbstractLiteLifecycleManager extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    protected final BrokerController brokerController;
    protected final String brokerName;
    protected final LiteSharding liteSharding;
    protected MessageStore messageStore;
    protected Map<String, Integer> ttlMap = Collections.emptyMap();
    protected Map<String, Set<String>> subscriberGroupMap = Collections.emptyMap();

    public AbstractLiteLifecycleManager(BrokerController brokerController, LiteSharding liteSharding) {
        this.brokerController = brokerController;
        this.brokerName = brokerController.getBrokerConfig().getBrokerName();
        this.liteSharding = liteSharding;
    }

    public void init() {
        this.messageStore = brokerController.getMessageStore();
        assert messageStore != null;
    }

    /**
     * This method actually returns NEXT slot index to use, starting from 0
     */
    public abstract long getMaxOffsetInQueue(String lmqName);

    /**
     * Collect expired LMQ of lite topic, and also attach its parent topic name
     * return Pair of parent topic and lmq name, not null
     */
    public abstract List<Pair<String, String>> collectExpiredLiteTopic();

    /**
     * Collect LMQ by parent topic
     * return lmq name list, not null
     */
    public abstract List<String> collectByParentTopic(String parentTopic);

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
        return collectByParentTopic(parentTopic).size();
    }

    public boolean isLmqExist(String lmqName) {
        return getMaxOffsetInQueue(lmqName) > 0;
    }

    public void cleanExpiredLiteTopic() {
        try {
            updateMetadata(); // necessary
            List<Pair<String, String>> lmqToDelete = collectExpiredLiteTopic();
            LOGGER.info("collect expired topic, size:{}", lmqToDelete.size());
            lmqToDelete.forEach(pair -> deleteLmq(pair.getObject1(), pair.getObject2()));
            if (!lmqToDelete.isEmpty()) {
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
            updateMetadata(); // necessary
            List<String> lmqToDelete = collectByParentTopic(parentTopic);
            LOGGER.info("clean by parent topic, {}, size:{}", parentTopic, lmqToDelete.size());
            lmqToDelete.forEach(lmqName -> deleteLmq(parentTopic, lmqName));
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
        if (maxOffset <= 0) {
            LOGGER.warn("unexpected condition, max offset <= 0, {}, {}", lmqName, maxOffset);
            return false;
        }
        long latestStoreTime =
            this.brokerController.getMessageStore().getMessageStoreTimeStamp(lmqName, 0, maxOffset - 1);
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
        return inactiveTime > minutes * 60 * 1000;
    }

    public void deleteLmq(String parentTopic, String lmqName) {
        try {
            Set<String> groups = subscriberGroupMap.getOrDefault(parentTopic, Collections.emptySet());
            groups.forEach(group -> {
                String topicAtGroup = lmqName + TOPIC_GROUP_SEPARATOR + group;
                brokerController.getConsumerOffsetManager().getOffsetTable().remove(topicAtGroup);
                brokerController.getConsumerOffsetManager().removeConsumerOffset(topicAtGroup); // no iteration
                brokerController.getPopLiteMessageProcessor().getConsumerOrderInfoManager().remove(lmqName, group);
            });
            brokerController.getMessageStore().deleteTopics(Sets.newHashSet(lmqName));
            boolean sharding = brokerName.equals(liteSharding.shardingByLmqName(parentTopic, lmqName));
            brokerController.getLiteSubscriptionRegistry().cleanSubscription(lmqName, false);
            brokerController.getConsumerOffsetManager().getPullOffsetTable().remove(
                lmqName + TOPIC_GROUP_SEPARATOR + MixAll.TOOLS_CONSUMER_GROUP);
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
