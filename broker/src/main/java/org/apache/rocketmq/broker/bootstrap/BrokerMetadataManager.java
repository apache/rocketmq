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
package org.apache.rocketmq.broker.bootstrap;

import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.offset.LmqConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.RocksDBLmqConsumerOffsetManager;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.RocksDBLmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.RocksDBLmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicQueueMappingManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerMetadataManager {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;
    private final BrokerController brokerController;

    protected TopicQueueMappingManager topicQueueMappingManager;
    private ConsumerOffsetManager consumerOffsetManager;
    private ConsumerFilterManager consumerFilterManager;
    protected ConsumerOrderInfoManager consumerOrderInfoManager;
    protected TopicConfigManager topicConfigManager;
    protected SubscriptionGroupManager subscriptionGroupManager;

    public BrokerMetadataManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        init();
    }

    public boolean load() {
        boolean result = this.topicConfigManager.load();
        result = result && this.topicQueueMappingManager.load();
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        result = result && this.consumerFilterManager.load();
        result = result && this.consumerOrderInfoManager.load();
        return result;
    }

    public void start() {

    }

    public void shutdown() {
        this.consumerOffsetManager.persist();

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }

        if (this.consumerOrderInfoManager != null) {
            this.consumerOrderInfoManager.persist();
        }

        if (this.topicConfigManager != null) {
            this.topicConfigManager.persist();
            this.topicConfigManager.stop();
        }

        if (this.subscriptionGroupManager != null) {
            this.subscriptionGroupManager.persist();
            this.subscriptionGroupManager.stop();
        }

        if (this.consumerOffsetManager != null) {
            this.consumerOffsetManager.persist();
            this.consumerOffsetManager.stop();
        }
    }

    private boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.messageStoreConfig.getStoreType());
    }

    private void init() {
        if (isEnableRocksDBStore()) {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqTopicConfigManager(brokerController) : new RocksDBTopicConfigManager(brokerController);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqSubscriptionGroupManager(brokerController) : new RocksDBSubscriptionGroupManager(brokerController);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqConsumerOffsetManager(brokerController) : new RocksDBConsumerOffsetManager(brokerController);
        } else {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(brokerController) : new TopicConfigManager(brokerController);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(brokerController) : new SubscriptionGroupManager(brokerController);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(brokerController) : new ConsumerOffsetManager(brokerController);
        }

        this.topicQueueMappingManager = new TopicQueueMappingManager(brokerController);
        this.consumerFilterManager = new ConsumerFilterManager(brokerController);
        this.consumerOrderInfoManager = new ConsumerOrderInfoManager(brokerController);
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return consumerOrderInfoManager;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void setTopicQueueMappingManager(TopicQueueMappingManager topicQueueMappingManager) {
        this.topicQueueMappingManager = topicQueueMappingManager;
    }

    public void setConsumerOffsetManager(ConsumerOffsetManager consumerOffsetManager) {
        this.consumerOffsetManager = consumerOffsetManager;
    }

    public void setConsumerFilterManager(ConsumerFilterManager consumerFilterManager) {
        this.consumerFilterManager = consumerFilterManager;
    }

    public void setConsumerOrderInfoManager(ConsumerOrderInfoManager consumerOrderInfoManager) {
        this.consumerOrderInfoManager = consumerOrderInfoManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public void setSubscriptionGroupManager(
        SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

}
