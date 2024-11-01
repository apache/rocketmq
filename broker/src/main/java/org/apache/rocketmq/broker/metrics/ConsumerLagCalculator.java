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
package org.apache.rocketmq.broker.metrics;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.PopBufferMergeService;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.DefaultMessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

public class ConsumerLagCalculator {
    private final BrokerConfig brokerConfig;
    private final TopicConfigManager topicConfigManager;
    private final ConsumerManager consumerManager;
    private final ConsumerOffsetManager offsetManager;
    private final ConsumerFilterManager consumerFilterManager;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final MessageStore messageStore;
    private final PopBufferMergeService popBufferMergeService;
    private final PopInflightMessageCounter popInflightMessageCounter;

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public ConsumerLagCalculator(BrokerController brokerController) {
        this.brokerConfig = brokerController.getBrokerConfig();
        this.topicConfigManager = brokerController.getTopicConfigManager();
        this.consumerManager = brokerController.getConsumerManager();
        this.offsetManager = brokerController.getConsumerOffsetManager();
        this.consumerFilterManager = brokerController.getConsumerFilterManager();
        this.subscriptionGroupManager = brokerController.getSubscriptionGroupManager();
        this.messageStore = brokerController.getMessageStore();
        this.popBufferMergeService = brokerController.getPopMessageProcessor().getPopBufferMergeService();
        this.popInflightMessageCounter = brokerController.getPopInflightMessageCounter();
    }

    private static class ProcessGroupInfo {
        public String group;
        public String topic;
        public boolean isPop;
        public String retryTopic;

        public ProcessGroupInfo(String group, String topic, boolean isPop,
            String retryTopic) {
            this.group = group;
            this.topic = topic;
            this.isPop = isPop;
            this.retryTopic = retryTopic;
        }
    }

    public static class BaseCalculateResult {
        public String group;
        public String topic;
        public boolean isRetry;

        public BaseCalculateResult(String group, String topic, boolean isRetry) {
            this.group = group;
            this.topic = topic;
            this.isRetry = isRetry;
        }
    }

    public static class CalculateLagResult extends BaseCalculateResult {
        public long lag;
        public long earliestUnconsumedTimestamp;

        public CalculateLagResult(String group, String topic, boolean isRetry) {
            super(group, topic, isRetry);
        }
    }

    public static class CalculateInflightResult extends BaseCalculateResult {
        public long inFlight;
        public long earliestUnPulledTimestamp;

        public CalculateInflightResult(String group, String topic, boolean isRetry) {
            super(group, topic, isRetry);
        }
    }

    public static class CalculateAvailableResult extends BaseCalculateResult {
        public long available;

        public CalculateAvailableResult(String group, String topic, boolean isRetry) {
            super(group, topic, isRetry);
        }
    }

    private void processAllGroup(Consumer<ProcessGroupInfo> consumer) {
        for (Map.Entry<String, SubscriptionGroupConfig> subscriptionEntry :
            subscriptionGroupManager.getSubscriptionGroupTable().entrySet()) {

            String group = subscriptionEntry.getKey();
            ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(group, true);
            boolean isPop = false;
            if (consumerGroupInfo != null) {
                isPop = consumerGroupInfo.getConsumeType() == ConsumeType.CONSUME_POP;
            }
            Set<String> topics;
            if (brokerConfig.isUseStaticSubscription()) {
                SubscriptionGroupConfig subscriptionGroupConfig = subscriptionEntry.getValue();
                if (subscriptionGroupConfig.getSubscriptionDataSet() == null ||
                    subscriptionGroupConfig.getSubscriptionDataSet().isEmpty()) {
                    continue;
                }
                topics = subscriptionGroupConfig.getSubscriptionDataSet()
                    .stream()
                    .map(SimpleSubscriptionData::getTopic)
                    .collect(Collectors.toSet());
            } else {
                if (consumerGroupInfo == null) {
                    continue;
                }
                topics = consumerGroupInfo.getSubscribeTopics();
            }

            if (null == topics || topics.isEmpty()) {
                continue;
            }
            for (String topic : topics) {
                // skip retry topic
                if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    continue;
                }

                TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
                if (topicConfig == null) {
                    continue;
                }

                // skip no perm topic
                int topicPerm = topicConfig.getPerm() & brokerConfig.getBrokerPermission();
                if (!PermName.isReadable(topicPerm) && !PermName.isWriteable(topicPerm)) {
                    continue;
                }

                if (isPop) {
                    String retryTopic = KeyBuilder.buildPopRetryTopic(topic, group, brokerConfig.isEnableRetryTopicV2());
                    TopicConfig retryTopicConfig = topicConfigManager.selectTopicConfig(retryTopic);
                    if (retryTopicConfig != null) {
                        int retryTopicPerm = retryTopicConfig.getPerm() & brokerConfig.getBrokerPermission();
                        if (PermName.isReadable(retryTopicPerm) || PermName.isWriteable(retryTopicPerm)) {
                            consumer.accept(new ProcessGroupInfo(group, topic, true, retryTopic));
                            continue;
                        }
                    }
                    if (brokerConfig.isEnableRetryTopicV2() && brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                        String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(topic, group);
                        TopicConfig retryTopicConfigV1 = topicConfigManager.selectTopicConfig(retryTopicV1);
                        if (retryTopicConfigV1 != null) {
                            int retryTopicPerm = retryTopicConfigV1.getPerm() & brokerConfig.getBrokerPermission();
                            if (PermName.isReadable(retryTopicPerm) || PermName.isWriteable(retryTopicPerm)) {
                                consumer.accept(new ProcessGroupInfo(group, topic, true, retryTopicV1));
                                continue;
                            }
                        }
                    }
                    consumer.accept(new ProcessGroupInfo(group, topic, true, null));
                } else {
                    consumer.accept(new ProcessGroupInfo(group, topic, false, null));
                }
            }
        }
    }

    public void calculateLag(Consumer<CalculateLagResult> lagRecorder) {
        processAllGroup(info -> {
            if (info.group == null || info.topic == null) {
                return;
            }

            CalculateLagResult result = new CalculateLagResult(info.group, info.topic, false);

            try {
                Pair<Long, Long> lag = getConsumerLagStats(info.group, info.topic, info.isPop);
                if (lag != null) {
                    result.lag = lag.getObject1();
                    result.earliestUnconsumedTimestamp = lag.getObject2();
                }
                lagRecorder.accept(result);
            } catch (ConsumeQueueException e) {
                LOGGER.error("Failed to get lag stats", e);
            }

            if (info.isPop) {
                try {
                    Pair<Long, Long> retryLag = getConsumerLagStats(info.group, info.retryTopic, true);

                    result = new CalculateLagResult(info.group, info.topic, true);
                    if (retryLag != null) {
                        result.lag = retryLag.getObject1();
                        result.earliestUnconsumedTimestamp = retryLag.getObject2();
                    }
                    lagRecorder.accept(result);
                } catch (ConsumeQueueException e) {
                    LOGGER.error("Failed to get lag stats", e);
                }
            }
        });
    }

    public void calculateInflight(Consumer<CalculateInflightResult> inflightRecorder) {
        processAllGroup(info -> {
            CalculateInflightResult result = new CalculateInflightResult(info.group, info.topic, false);
            try {
                Pair<Long, Long> inFlight = getInFlightMsgStats(info.group, info.topic, info.isPop);
                if (inFlight != null) {
                    result.inFlight = inFlight.getObject1();
                    result.earliestUnPulledTimestamp = inFlight.getObject2();
                }
                inflightRecorder.accept(result);
            } catch (ConsumeQueueException e) {
                LOGGER.error("Failed to get inflight message stats", e);
            }

            if (info.isPop) {
                try {
                    Pair<Long, Long> retryInFlight = getInFlightMsgStats(info.group, info.retryTopic, true);

                    result = new CalculateInflightResult(info.group, info.topic, true);
                    if (retryInFlight != null) {
                        result.inFlight = retryInFlight.getObject1();
                        result.earliestUnPulledTimestamp = retryInFlight.getObject2();
                    }
                    inflightRecorder.accept(result);
                } catch (ConsumeQueueException e) {
                    LOGGER.error("Failed to get inflight message stats", e);
                }
            }
        });
    }

    public void calculateAvailable(Consumer<CalculateAvailableResult> availableRecorder) {
        processAllGroup(info -> {
            CalculateAvailableResult result = new CalculateAvailableResult(info.group, info.topic, false);

            try {
                result.available = getAvailableMsgCount(info.group, info.topic, info.isPop);
                availableRecorder.accept(result);
            } catch (ConsumeQueueException e) {
                LOGGER.error("Failed to get available message count", e);
            }


            if (info.isPop) {
                try {
                    long retryAvailable = getAvailableMsgCount(info.group, info.retryTopic, true);
                    result = new CalculateAvailableResult(info.group, info.topic, true);
                    result.available = retryAvailable;
                    availableRecorder.accept(result);
                } catch (ConsumeQueueException e) {
                    LOGGER.error("Failed to get available message count", e);
                }
            }
        });
    }

    public Pair<Long, Long> getConsumerLagStats(String group, String topic, boolean isPop) throws ConsumeQueueException {
        long total = 0L;
        long earliestUnconsumedTimestamp = Long.MAX_VALUE;

        if (group == null || topic == null) {
            return new Pair<>(total, earliestUnconsumedTimestamp);
        }

        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (topicConfig != null) {
            for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                Pair<Long, Long> pair = getConsumerLagStats(group, topic, queueId, isPop);
                total += pair.getObject1();
                earliestUnconsumedTimestamp = Math.min(earliestUnconsumedTimestamp, pair.getObject2());
            }
        } else {
            LOGGER.warn("failed to get config of topic {}", topic);
        }

        if (earliestUnconsumedTimestamp < 0 || earliestUnconsumedTimestamp == Long.MAX_VALUE) {
            earliestUnconsumedTimestamp = 0L;
        }

        return new Pair<>(total, earliestUnconsumedTimestamp);
    }

    public Pair<Long, Long> getConsumerLagStats(String group, String topic, int queueId, boolean isPop)
        throws ConsumeQueueException {
        long brokerOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
        if (brokerOffset < 0) {
            brokerOffset = 0;
        }

        if (isPop) {
            long pullOffset = popBufferMergeService.getLatestOffset(topic, group, queueId);
            if (pullOffset < 0) {
                pullOffset = offsetManager.queryOffset(group, topic, queueId);
            }
            if (pullOffset < 0) {
                pullOffset = brokerOffset;
            }
            long inFlightNum = popInflightMessageCounter.getGroupPopInFlightMessageNum(topic, group, queueId);
            long lag = calculateMessageCount(group, topic, queueId, pullOffset, brokerOffset) + inFlightNum;
            long consumerOffset = pullOffset - inFlightNum;
            long consumerStoreTimeStamp = getStoreTimeStamp(topic, queueId, consumerOffset);
            return new Pair<>(lag, consumerStoreTimeStamp);
        }

        long consumerOffset = offsetManager.queryOffset(group, topic, queueId);
        if (consumerOffset < 0) {
            consumerOffset = brokerOffset;
        }

        long lag = calculateMessageCount(group, topic, queueId, consumerOffset, brokerOffset);
        long consumerStoreTimeStamp = getStoreTimeStamp(topic, queueId, consumerOffset);
        return new Pair<>(lag, consumerStoreTimeStamp);
    }

    public Pair<Long, Long> getInFlightMsgStats(String group, String topic, boolean isPop) throws ConsumeQueueException {
        long total = 0L;
        long earliestUnPulledTimestamp = Long.MAX_VALUE;

        if (group == null || topic == null) {
            return new Pair<>(total, earliestUnPulledTimestamp);
        }

        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (topicConfig != null) {
            for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                Pair<Long, Long> pair = getInFlightMsgStats(group, topic, queueId, isPop);
                total += pair.getObject1();
                earliestUnPulledTimestamp = Math.min(earliestUnPulledTimestamp, pair.getObject2());
            }
        } else {
            LOGGER.warn("failed to get config of topic {}", topic);
        }

        if (earliestUnPulledTimestamp < 0 || earliestUnPulledTimestamp == Long.MAX_VALUE) {
            earliestUnPulledTimestamp = 0L;
        }

        return new Pair<>(total, earliestUnPulledTimestamp);
    }

    public Pair<Long, Long> getInFlightMsgStats(String group, String topic, int queueId, boolean isPop)
        throws ConsumeQueueException {
        if (isPop) {
            long inflight = popInflightMessageCounter.getGroupPopInFlightMessageNum(topic, group, queueId);
            long pullOffset = popBufferMergeService.getLatestOffset(topic, group, queueId);
            if (pullOffset < 0) {
                pullOffset = offsetManager.queryOffset(group, topic, queueId);
            }
            if (pullOffset < 0) {
                pullOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
            }
            long pullStoreTimeStamp = getStoreTimeStamp(topic, queueId, pullOffset);
            return new Pair<>(inflight, pullStoreTimeStamp);
        }

        long pullOffset = offsetManager.queryPullOffset(group, topic, queueId);
        if (pullOffset < 0) {
            pullOffset = 0;
        }

        long commitOffset = offsetManager.queryOffset(group, topic, queueId);
        if (commitOffset < 0) {
            commitOffset = pullOffset;
        }

        long inflight = calculateMessageCount(group, topic, queueId, commitOffset, pullOffset);
        long pullStoreTimeStamp = getStoreTimeStamp(topic, queueId, pullOffset);
        return new Pair<>(inflight, pullStoreTimeStamp);
    }

    public long getAvailableMsgCount(String group, String topic, boolean isPop) throws ConsumeQueueException {
        long total = 0L;

        if (group == null || topic == null) {
            return total;
        }

        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (topicConfig != null) {
            for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                total += getAvailableMsgCount(group, topic, queueId, isPop);
            }
        } else {
            LOGGER.warn("failed to get config of topic {}", topic);
        }

        return total;
    }

    public long getAvailableMsgCount(String group, String topic, int queueId, boolean isPop)
        throws ConsumeQueueException {
        long brokerOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
        if (brokerOffset < 0) {
            brokerOffset = 0;
        }

        long pullOffset;
        if (isPop) {
            pullOffset = popBufferMergeService.getLatestOffset(topic, group, queueId);
            if (pullOffset < 0) {
                pullOffset = offsetManager.queryOffset(group, topic, queueId);
            }
            if (pullOffset < 0) {
                pullOffset = brokerOffset;
            }
        } else {
            pullOffset = offsetManager.queryPullOffset(group, topic, queueId);
        }
        if (pullOffset < 0) {
            pullOffset = brokerOffset;
        }

        return calculateMessageCount(group, topic, queueId, pullOffset, brokerOffset);
    }

    public long getStoreTimeStamp(String topic, int queueId, long offset) {
        long storeTimeStamp = Long.MAX_VALUE;
        if (offset >= 0) {
            storeTimeStamp = messageStore.getMessageStoreTimeStamp(topic, queueId, offset);
            storeTimeStamp = storeTimeStamp > 0 ? storeTimeStamp : Long.MAX_VALUE;
        }
        return storeTimeStamp;
    }

    public long calculateMessageCount(String group, String topic, int queueId, long from, long to) {
        long count = to - from;

        if (brokerConfig.isEstimateAccumulation() && to > from) {
            SubscriptionData subscriptionData = null;
            if (brokerConfig.isUseStaticSubscription()) {
                SubscriptionGroupConfig subscriptionGroupConfig = subscriptionGroupManager.findSubscriptionGroupConfig(group);
                if (subscriptionGroupConfig != null) {
                    for (SimpleSubscriptionData simpleSubscriptionData : subscriptionGroupConfig.getSubscriptionDataSet()) {
                        if (topic.equals(simpleSubscriptionData.getTopic())) {
                            try {
                                subscriptionData = FilterAPI.buildSubscriptionData(simpleSubscriptionData.getTopic(),
                                    simpleSubscriptionData.getExpression(), simpleSubscriptionData.getExpressionType());
                            } catch (Exception e) {
                                LOGGER.error("Try to build subscription for group:{}, topic:{} exception.", group, topic, e);
                            }
                            break;
                        }
                    }
                }
            } else {
                ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(group, true);
                if (consumerGroupInfo != null) {
                    subscriptionData = consumerGroupInfo.findSubscriptionData(topic);
                }
            }

            if (null != subscriptionData) {
                if (ExpressionType.TAG.equalsIgnoreCase(subscriptionData.getExpressionType())
                    && !SubscriptionData.SUB_ALL.equals(subscriptionData.getSubString())) {
                    count = messageStore.estimateMessageCount(topic, queueId, from, to,
                        new DefaultMessageFilter(subscriptionData));
                } else if (ExpressionType.SQL92.equalsIgnoreCase(subscriptionData.getExpressionType())) {
                    ConsumerFilterData consumerFilterData = consumerFilterManager.get(topic, group);
                    count = messageStore.estimateMessageCount(topic, queueId, from, to,
                        new ExpressionMessageFilter(subscriptionData,
                            consumerFilterData,
                            consumerFilterManager));
                }
            }

        }
        return count < 0 ? 0 : count;
    }
}
