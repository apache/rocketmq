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

package org.apache.rocketmq.broker.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * Consumer filter data manager.Just manage the consumers use expression filter.
 */
public class ConsumerFilterManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    private ConcurrentMap<String/*Topic*/, FilterDataMapByTopic>
        filterDataByTopic = new ConcurrentHashMap<>(256);

    private final transient ConcurrentMap<String/*ConsumerID*/, SubscriptionFilterHandler>
            subscriptionFilterData = new ConcurrentHashMap<>(256);

    private transient BrokerController brokerController;
    private transient BloomFilter bloomFilter;

    public ConsumerFilterManager() {
        // just for test
        this.bloomFilter = BloomFilter.createByFn(20, 64);
    }

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.bloomFilter = BloomFilter.createByFn(
            brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(),
            brokerController.getBrokerConfig().getExpectConsumerNumUseFilter()
        );
        // then set bit map length of store config.
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(
            this.bloomFilter.getM()
        );
    }

    /**
     * Build consumer filter data.Be care, bloom filter data is not included.
     *
     * @return maybe null
     */
    public static ConsumerFilterData build(final String topic, final String consumerGroup,
        final String expression, final String type,
        final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return null;
        }

        ConsumerFilterData consumerFilterData = new ConsumerFilterData();
        consumerFilterData.setTopic(topic);
        consumerFilterData.setConsumerGroup(consumerGroup);
        consumerFilterData.setBornTime(System.currentTimeMillis());
        consumerFilterData.setDeadTime(0);
        consumerFilterData.setExpression(expression);
        consumerFilterData.setExpressionType(type);
        consumerFilterData.setClientVersion(clientVersion);
        try {
            consumerFilterData.setCompiledExpression(
                FilterFactory.INSTANCE.get(type).compile(expression)
            );
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }

        return consumerFilterData;
    }

    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(
                subscriptionData.getTopic(),
                consumerGroup,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion()
            );
        }

        // make illegal topic dead.
        Set<String> curSubList = new HashSet<>();
        for (SubscriptionData subscriptionData : subList) {
            curSubList.add(subscriptionData.getTopic());
        }

        SubscriptionFilterHandler subscriptionFilterHandler = this.subscriptionFilterData.get(consumerGroup);
        if (null != subscriptionFilterHandler) {
            for (Map.Entry<String, ConsumerFilterData> entry : subscriptionFilterHandler.getTopicSqlFilterData().entrySet()) {
                if (!curSubList.contains(entry.getKey())) {
                    ConsumerFilterData filterData = entry.getValue();
                    if (filterData != null) {
                        filterData.setDeadTime(System.currentTimeMillis());
                        log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
                    }
                }
            }
        }
    }

    public boolean register(final String topic, final String consumerGroup, final String expression,
        final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }

        if (expression == null || expression.length() == 0) {
            return false;
        }

        if (null != this.brokerController) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                return false;
            }
        }

        SubscriptionFilterHandler subscriptionFilterHandler = this.subscriptionFilterData.get(consumerGroup);
        if (subscriptionFilterHandler == null) {
            SubscriptionFilterHandler temp = new SubscriptionFilterHandler(consumerGroup);
            SubscriptionFilterHandler prev = this.subscriptionFilterData.putIfAbsent(consumerGroup, temp);
            subscriptionFilterHandler = prev != null ? prev : temp;
        }

        BloomFilterData bloomFilterData = null;
        if (this.brokerController == null
                || this.brokerController.getBrokerConfig().isEnableCalcFilterBitMap()) {
            bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);
        }
        ConsumerFilterData consumerFilterData = subscriptionFilterHandler.register(consumerGroup, expression, type, bloomFilterData, clientVersion, topic);
        if (null == consumerFilterData) {
            FilterDataMapByTopic mapByTopic = this.filterDataByTopic.get(topic);
            if (mapByTopic != null) {
                mapByTopic.getGroupFilterData().remove(consumerGroup);
                if (mapByTopic.getGroupFilterData().isEmpty()) {
                    this.filterDataByTopic.remove(topic);
                }
            }
            return false;
        }
        this.filterDataByTopic.putIfAbsent(topic, new FilterDataMapByTopic(topic));
        this.filterDataByTopic.get(topic).put(consumerFilterData);
        return true;
    }

    public void unRegister(final String consumerGroup) {
        SubscriptionFilterHandler handler = this.subscriptionFilterData.get(consumerGroup);
        if (handler != null) {
            handler.unRegister();
        }
    }

    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        SubscriptionFilterHandler handler = this.subscriptionFilterData.get(consumerGroup);
        if (handler == null) {
            return null;
        }

        return handler.getTopicSqlFilterData().get(topic);
    }

    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<>();

        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic.values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator.next();

            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values().iterator();

            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();

                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }

        return ret;
    }

    public final Collection<ConsumerFilterData> get(final String topic) {
        FilterDataMapByTopic mapByTopic = this.filterDataByTopic.get(topic);
        if (mapByTopic == null || mapByTopic.getGroupFilterData().isEmpty()) {
            return null;
        }

        return mapByTopic.getGroupFilterData().values();
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(
                this.brokerController.getMessageStoreConfig().getStorePathRootDir()
            );
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    @Override
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            for (Entry<String, FilterDataMapByTopic> entry : load.filterDataByTopic.entrySet()) {
                FilterDataMapByTopic dataMapByTopic = entry.getValue();
                if (dataMapByTopic == null) {
                    continue;
                }

                for (Entry<String, ConsumerFilterData> groupEntry : dataMapByTopic.getGroupFilterData().entrySet()) {

                    ConsumerFilterData filterData = groupEntry.getValue();

                    if (filterData == null) {
                        continue;
                    }

                    try {
                        filterData.setCompiledExpression(
                                FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression())
                        );
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }

                    // check whether bloom filter is changed
                    // if changed, ignore the bit map calculated before.
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true;
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }

                    log.info("load exist consumer filter data: {}", filterData);

                    if (filterData.getDeadTime() == 0) {
                        // we think all consumers are dead when load
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(
                                deadTime <= filterData.getBornTime() ? filterData.getBornTime() : deadTime
                        );
                    }
                }
            }

            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }

            // rebuild subscriptionFilterData from filterDataByTopic
            for (Entry<String, FilterDataMapByTopic> entry : this.filterDataByTopic.entrySet()) {
                for (Entry<String, ConsumerFilterData> groupEntry : entry.getValue().getGroupFilterData().entrySet()) {
                    ConsumerFilterData data = groupEntry.getValue();
                    if (data == null) {
                        continue;
                    }
                    SubscriptionFilterHandler handler = this.subscriptionFilterData
                        .computeIfAbsent(data.getConsumerGroup(), SubscriptionFilterHandler::new);
                    handler.getTopicSqlFilterData().put(data.getTopic(), data);
                }
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        // clean
        {
            clean();
        }
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public void clean() {
        Iterator<Map.Entry<String, SubscriptionFilterHandler>> consumerIterator = this.subscriptionFilterData.entrySet().iterator();
        while (consumerIterator.hasNext()) {
            Map.Entry<String, SubscriptionFilterHandler> subscriptionFilterHandlerEntry = consumerIterator.next();

            Iterator<Map.Entry<String, ConsumerFilterData>> filterDataIterator
                    = subscriptionFilterHandlerEntry.getValue().getTopicSqlFilterData().entrySet().iterator();

            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();

                ConsumerFilterData filterData = filterDataByGroup.getValue();
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getKey());
                    filterDataIterator.remove();

                    FilterDataMapByTopic mapByTopic = this.filterDataByTopic.get(filterData.getTopic());
                    if (mapByTopic != null) {
                        log.info("Remove filter data {} {} from filterDataByTopic", filterData.getTopic(), filterData.getConsumerGroup());
                        mapByTopic.getGroupFilterData().remove(filterData.getConsumerGroup());
                        if (mapByTopic.getGroupFilterData().isEmpty()) {
                            this.filterDataByTopic.remove(filterData.getTopic());
                        }
                    }
                }
            }
            if (subscriptionFilterHandlerEntry.getValue().getTopicSqlFilterData().isEmpty()) {
                log.info("subscriptionFilterData Remove filter consumer {}", subscriptionFilterHandlerEntry.getKey());
                consumerIterator.remove();
            }
        }
    }

    public ConcurrentMap<String, FilterDataMapByTopic> getFilterDataByTopic() {
        return filterDataByTopic;
    }

    public void setFilterDataByTopic(final ConcurrentHashMap<String, FilterDataMapByTopic> filterDataByTopic) {
        this.filterDataByTopic = filterDataByTopic;
    }

    public static class FilterDataMapByTopic {

        private ConcurrentMap<String/*consumer group*/, ConsumerFilterData>
            groupFilterData = new ConcurrentHashMap<>();

        private String topic;

        public FilterDataMapByTopic() {
        }

        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        public void put(ConsumerFilterData consumerFilterData) {
            if (null != consumerFilterData) {
                this.groupFilterData.put(consumerFilterData.getConsumerGroup(), consumerFilterData);
            }
        }

        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

        public void setGroupFilterData(final ConcurrentHashMap<String, ConsumerFilterData> groupFilterData) {
            this.groupFilterData = groupFilterData;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(final String topic) {
            this.topic = topic;
        }
    }


    public static class SubscriptionFilterHandler {

        private Map<String/*Topic*/, ConsumerFilterData> topicSqlFilterData = new ConcurrentHashMap<>();

        final private String consumerId;

        public SubscriptionFilterHandler(String consumerId) {
            this.consumerId = consumerId;
        }

        public void unRegister() {
            for (ConsumerFilterData data : topicSqlFilterData.values()) {
                if (data != null && !data.isDead()) {
                    long now = System.currentTimeMillis();
                    log.info("Unregister consumer filter: {}, deadTime: {}", data, now);
                    data.setDeadTime(now);
                }
            }
        }

        public ConsumerFilterData register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData,
                                           long clientVersion, String topic) {
            ConsumerFilterData old = this.topicSqlFilterData.get(topic);
            if (old == null) {
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return null;
                }
                consumerFilterData.setBloomFilterData(bloomFilterData);
                old = this.topicSqlFilterData.putIfAbsent(topic, consumerFilterData);
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return consumerFilterData;
                } else {
                    if (clientVersion <= old.getClientVersion()) {
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                    consumerGroup, topic,
                                    clientVersion, old.getClientVersion(),
                                    old.getExpressionType(), old.getExpression(),
                                    type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                            reAlive(old);
                            return old;
                        }
                        return null;
                    } else {
                        this.topicSqlFilterData.put(topic, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return consumerFilterData;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                consumerGroup, topic,
                                clientVersion, old.getClientVersion(),
                                old.getExpressionType(), old.getExpression(),
                                type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        reAlive(old);
                        return old;
                    }
                    return null;
                }

                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }

                // if subscribe data is changed, or consumer is died too long.
                if (change) {
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        // new expression compile error, remove old, let client report error.
                        this.topicSqlFilterData.remove(topic);
                        return null;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);
                    this.topicSqlFilterData.put(topic, consumerFilterData);
                    log.info("Consumer filter info change, old: {}, new: {}, change: true", old, consumerFilterData);
                    return consumerFilterData;
                } else {
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old);
                    }
                    return old;
                }
            }
        }

        protected void reAlive(ConsumerFilterData filterData) {
            long oldDeadTime = filterData.getDeadTime();
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        public Map<String, ConsumerFilterData> getTopicSqlFilterData() {
            return topicSqlFilterData;
        }

        public void setTopicSqlFilterData(Map<String, ConsumerFilterData> topicSqlFilterData) {
            this.topicSqlFilterData = topicSqlFilterData;
        }

        public String getConsumerId() {
            return consumerId;
        }
    }

}
