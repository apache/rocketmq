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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.PopBufferMergeService;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;

public class ConsumerLagCalculator {
    private final BrokerConfig brokerConfig;
    private final TopicConfigManager topicConfigManager;
    private final ConsumerManager consumerManager;
    private final ConsumerOffsetManager offsetManager;
    private final ConsumerFilterManager consumerFilterManager;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final MessageStore messageStore;
    private final PopBufferMergeService popBufferMergeService;

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
    }

    private static class ProcessGroupInfo {
        public String group;
        public String topic;

        public ProcessGroupInfo(String group, String topic) {
            this.group = group;
            this.topic = topic;
        }
    }

    public static class BaseCalculateResult {
        public String group;
        public String topic;

        public BaseCalculateResult(String group, String topic) {
            this.group = group;
            this.topic = topic;
        }
    }

    public static class CalculateLagResult extends BaseCalculateResult {
        public long lag;
        public long earliestUnconsumedTimestamp;

        public CalculateLagResult(String group, String topic) {
            super(group, topic);
        }
    }

    public static class CalculateInflightResult extends BaseCalculateResult {
        public long inFlight;
        public long earliestUnPulledTimestamp;

        public CalculateInflightResult(String group, String topic) {
            super(group, topic);
        }
    }

    public static class CalculateAvailableResult extends BaseCalculateResult {
        public long available;

        public CalculateAvailableResult(String group, String topic) {
            super(group, topic);
        }
    }

    public static class CalculateSendToDLQResult extends BaseCalculateResult {
        public long dlqMessageCount;

        public CalculateSendToDLQResult(String group, String topic) {
            super(group, topic);
        }
    }

    private void processAllGroup(Consumer<ProcessGroupInfo> consumer) {
        for (Map.Entry<String, SubscriptionGroupConfig> subscriptionEntry : subscriptionGroupManager
            .getSubscriptionGroupTable().entrySet()) {
            String group = subscriptionEntry.getKey();
            SubscriptionGroupConfig subscriptionGroupConfig = subscriptionEntry.getValue();

            ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(group);
            if (consumerGroupInfo == null) {
                continue;
            }
            Set<String> topics = consumerGroupInfo.getSubscribeTopics();
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

                consumer.accept(new ProcessGroupInfo(group, topic));
            }
        }
    }

    public void calculateLag(Consumer<CalculateLagResult> lagRecorder) {
        processAllGroup(info -> {
            CalculateLagResult result = new CalculateLagResult(info.group, info.topic);

            Pair<Long, Long> lag = getConsumerLagStats(info.group, info.topic);
            if (lag != null) {
                result.lag = lag.getObject1();
                result.earliestUnconsumedTimestamp = lag.getObject2();
            }
            lagRecorder.accept(result);
        });
    }

    public void calculateInflight(Consumer<CalculateInflightResult> inflightRecorder) {
        processAllGroup(info -> {
            CalculateInflightResult result = new CalculateInflightResult(info.group, info.topic);
            Pair<Long, Long> inFlight = getInFlightMsgStats(info.group, info.topic);
            if (inFlight != null) {
                result.inFlight = inFlight.getObject1();
                result.earliestUnPulledTimestamp = inFlight.getObject2();
            }
            inflightRecorder.accept(result);
        });
    }

    public void calculateAvailable(Consumer<CalculateAvailableResult> availableRecorder) {
        processAllGroup(info -> {
            CalculateAvailableResult result = new CalculateAvailableResult(info.group, info.topic);

            result.available = getAvailableMsgCount(info.group, info.topic);
            availableRecorder.accept(result);
        });
    }

    public void calculateSendToDLQ(Consumer<CalculateSendToDLQResult> dlqRecorder) {
        processAllGroup(info -> {
            CalculateSendToDLQResult result = new CalculateSendToDLQResult(info.group, info.topic);

            String dlqTopic = MixAll.DLQ_GROUP_TOPIC_PREFIX + info.group;
            TopicConfig topicConfig = topicConfigManager.selectTopicConfig(dlqTopic);
            if (topicConfig == null) {
                return;
            }

            ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(dlqTopic, 0);
            if (consumeQueue == null) {
                return;
            }

            result.dlqMessageCount = consumeQueue.getMaxOffsetInQueue();
            dlqRecorder.accept(result);
        });
    }

    public Pair<Long, Long> getConsumerLagStats(String group, String topic) {
        long total = 0L;
        long earliestUnconsumedTimestamp = Long.MAX_VALUE;

        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (topicConfig != null) {
            for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                Pair<Long, Long> pair = getConsumerLagStats(group, topic, queueId);
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

    public Pair<Long, Long> getConsumerLagStats(String group, String topic, int queueId) {
        long brokerOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
        if (brokerOffset < 0) {
            brokerOffset = 0;
        }

        long consumerOffset = offsetManager.queryOffset(group, topic, queueId);
        if (consumerOffset < 0) {
            consumerOffset = brokerOffset;
        }

        long lag = calculateMessageCount(group, topic, queueId, consumerOffset, brokerOffset);
        long consumerStoreTimeStamp = getStoreTimeStamp(topic, queueId, consumerOffset);
        return new Pair<>(lag, consumerStoreTimeStamp);
    }

    public Pair<Long, Long> getInFlightMsgStats(String group, String topic) {
        long total = 0L;
        long earliestUnPulledTimestamp = Long.MAX_VALUE;

        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (topicConfig != null) {
            for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                Pair<Long, Long> pair = getInFlightMsgStats(group, topic, queueId);
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

    public Pair<Long, Long> getInFlightMsgStats(String group, String topic, int queueId) {
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

    public long getAvailableMsgCount(String group, String topic) {
        long total = 0L;

        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (topicConfig != null) {
            for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                total += getAvailableMsgCount(group, topic, queueId);
            }
        } else {
            LOGGER.warn("failed to get config of topic {}", topic);
        }

        return total;
    }

    public long getAvailableMsgCount(String group, String topic, int queueId) {
        long brokerOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
        if (brokerOffset < 0) {
            brokerOffset = 0;
        }

        long pullOffset = offsetManager.queryPullOffset(group, topic, queueId);
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
        return count < 0 ? 0 : count;
    }
}
