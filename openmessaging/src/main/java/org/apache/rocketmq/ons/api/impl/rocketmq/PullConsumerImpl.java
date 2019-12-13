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
package org.apache.rocketmq.ons.api.impl.rocketmq;

import io.openmessaging.api.Message;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.TopicPartition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.ons.api.Constants;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.PropertyValueConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class PullConsumerImpl extends ONSClientAbstract implements PullConsumer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final static int MAX_CACHED_MESSAGE_SIZE_IN_MIB = 1024;
    private final static int MIN_CACHED_MESSAGE_SIZE_IN_MIB = 16;
    private final static int MAX_CACHED_MESSAGE_AMOUNT = 50000;
    private final static int MIN_CACHED_MESSAGE_AMOUNT = 100;

    private DefaultLitePullConsumer litePullConsumer;

    private int maxCachedMessageSizeInMiB = 512;

    private int maxCachedMessageAmount = 5000;

    public PullConsumerImpl(Properties properties) {
        super(properties);
        String consumerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(consumerGroup)) {
            throw new ONSClientException("Unable to get GROUP_ID property");
        }

        this.litePullConsumer =
            new DefaultLitePullConsumer(this.getNamespace(), consumerGroup, new OnsClientRPCHook(sessionCredentials));

        String messageModel = properties.getProperty(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        this.litePullConsumer.setMessageModel(MessageModel.valueOf(messageModel));

        String maxBatchMessageCount = properties.getProperty(PropertyKeyConst.MAX_BATCH_MESSAGE_COUNT);
        if (!UtilAll.isBlank(maxBatchMessageCount)) {
            this.litePullConsumer.setPullBatchSize(Integer.valueOf(maxBatchMessageCount));
        }

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.litePullConsumer.setVipChannelEnabled(isVipChannelEnabled);
        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.litePullConsumer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.InstanceName, this.buildIntanceName());
        this.litePullConsumer.setInstanceName(instanceName);
        this.litePullConsumer.setNamesrvAddr(this.getNameServerAddr());

        String consumeThreadNums = properties.getProperty(PropertyKeyConst.ConsumeThreadNums);
        if (!UtilAll.isBlank(consumeThreadNums)) {
            this.litePullConsumer.setPullThreadNums(Integer.valueOf(consumeThreadNums));
        }

        String configuredCachedMessageAmount = properties.getProperty(PropertyKeyConst.MaxCachedMessageAmount);
        if (!UtilAll.isBlank(configuredCachedMessageAmount)) {
            maxCachedMessageAmount = Math.min(MAX_CACHED_MESSAGE_AMOUNT, Integer.valueOf(configuredCachedMessageAmount));
            maxCachedMessageAmount = Math.max(MIN_CACHED_MESSAGE_AMOUNT, maxCachedMessageAmount);
            this.litePullConsumer.setPullThresholdForAll(maxCachedMessageAmount);
        }

        String configuredCachedMessageSizeInMiB = properties.getProperty(PropertyKeyConst.MaxCachedMessageSizeInMiB);
        if (!UtilAll.isBlank(configuredCachedMessageSizeInMiB)) {
            maxCachedMessageSizeInMiB = Math.min(MAX_CACHED_MESSAGE_SIZE_IN_MIB, Integer.valueOf(configuredCachedMessageSizeInMiB));
            maxCachedMessageSizeInMiB = Math.max(MIN_CACHED_MESSAGE_SIZE_IN_MIB, maxCachedMessageSizeInMiB);
            this.litePullConsumer.setPullThresholdSizeForQueue(maxCachedMessageSizeInMiB);
        }

        String autoCommit = properties.getProperty(PropertyKeyConst.AUTO_COMMIT);
        if (!UtilAll.isBlank(autoCommit)) {
            this.litePullConsumer.setAutoCommit(Boolean.valueOf(autoCommit));
        }
    }

    @Override protected void updateNameServerAddr(String nameServerAddresses) {
        this.litePullConsumer.updateNameServerAddress(nameServerAddresses);
    }

    private Set<TopicPartition> convertToTopicPartitions(Collection<MessageQueue> messageQueues) {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            TopicPartition topicPartition = convertToTopicPartition(messageQueue);
            topicPartitions.add(topicPartition);
        }
        return topicPartitions;
    }

    private Set<MessageQueue> convertToMessageQueues(Collection<TopicPartition> topicPartitions) {
        Set<MessageQueue> messageQueues = new HashSet<>();
        for (TopicPartition topicPartition : topicPartitions) {
            messageQueues.add(convertToMessageQueue(topicPartition));
        }
        return messageQueues;
    }

    private TopicPartition convertToTopicPartition(MessageQueue messageQueue) {
        String topic = messageQueue.getTopic();
        String partition = messageQueue.getBrokerName() + Constants.TOPIC_PARTITION_SEPARATOR + messageQueue.getQueueId();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return topicPartition;
    }

    private MessageQueue convertToMessageQueue(TopicPartition topicPartition) {
        String topic = topicPartition.getTopic();
        String[] tmp = topicPartition.getPartition().split(Constants.TOPIC_PARTITION_SEPARATOR);
        if (tmp.length != 2) {
            LOGGER.warn("Failed to get message queue from TopicPartition: {}", topicPartition);
            throw new ONSClientException("Failed to get message queue");
        }
        String brokerName = tmp[0];
        int queueId = Integer.valueOf(tmp[1]);
        return new MessageQueue(topic, brokerName, queueId);
    }

    @Override public Set<TopicPartition> topicPartitions(String topic) {
        try {
            Collection<MessageQueue> messageQueues = litePullConsumer.fetchMessageQueues(topic);
            Set<TopicPartition> topicPartitions = new HashSet<>();
            for (MessageQueue messageQueue : messageQueues) {
                topicPartitions.add(convertToTopicPartition(messageQueue));
            }
            return topicPartitions;
        } catch (MQClientException ex) {
            throw new ONSClientException("defaultMQPushConsumer subscribe exception", ex);
        }
    }

    @Override public void assign(Collection<TopicPartition> topicPartitions) {
        Set<MessageQueue> messageQueues = new HashSet<>();
        for (TopicPartition topicPartition : topicPartitions) {
            messageQueues.add(convertToMessageQueue(topicPartition));
        }
        this.litePullConsumer.assign(messageQueues);
    }

    @Override public void registerTopicPartitionChangedListener(String topic, TopicPartitionChangeListener callback) {
        TopicMessageQueueChangeListener listener = new TopicMessageQueueChangeListener() {
            @Override public void onChanged(String topic, Set<MessageQueue> messageQueues) {
                callback.onChanged(convertToTopicPartitions(messageQueues));
            }
        };
        try {
            this.litePullConsumer.registerTopicMessageQueueChangeListener(topic, listener);

        } catch (MQClientException ex) {
            LOGGER.warn("Register listener error", ex);
            throw new ONSClientException("Failed to register topic partition listener");
        }
    }

    @Override public List<Message> poll(long timeout) {
        List<MessageExt> rmqMsgList = litePullConsumer.poll(timeout);
        List<Message> msgList = new ArrayList<Message>();
        for (MessageExt rmqMsg : rmqMsgList) {
            Message msg = ONSUtil.msgConvert(rmqMsg);
            Map<String, String> propertiesMap = rmqMsg.getProperties();
            msg.setMsgID(rmqMsg.getMsgId());
            if (propertiesMap != null && propertiesMap.get(Constants.TRANSACTION_ID) != null) {
                msg.setMsgID(propertiesMap.get(Constants.TRANSACTION_ID));
            }
            msgList.add(msg);
        }
        return msgList;
    }

    @Override public void seek(TopicPartition topicPartition, long offset) {
        MessageQueue messageQueue = convertToMessageQueue(topicPartition);
        try {
            litePullConsumer.seek(messageQueue, offset);
        } catch (MQClientException ex) {
            LOGGER.warn("Topic partition: {} seek to: {} error", topicPartition, offset, ex);
            throw new ONSClientException("Seek offset failed");
        }
    }

    @Override public void seekToBeginning(TopicPartition topicPartition) {
        try {
            this.litePullConsumer.seekToBegin(convertToMessageQueue(topicPartition));
        } catch (MQClientException ex) {
            LOGGER.warn("Topic partition: {} seek to beginning error", topicPartition, ex);
            throw new ONSClientException("Seek offset to beginning failed");
        }
    }

    @Override public void seekToEnd(TopicPartition topicPartition) {
        try {
            this.litePullConsumer.seekToEnd(convertToMessageQueue(topicPartition));
        } catch (MQClientException ex) {
            LOGGER.warn("Topic partition: {} seek to end error", topicPartition, ex);
            throw new ONSClientException("Seek offset to end failed");
        }

    }

    @Override public void pause(Collection<TopicPartition> topicPartitions) {
        this.litePullConsumer.pause(convertToMessageQueues(topicPartitions));
    }

    @Override public void resume(Collection<TopicPartition> topicPartitions) {
        this.litePullConsumer.resume(convertToMessageQueues(topicPartitions));
    }

    @Override public Long offsetForTimestamp(TopicPartition topicPartition, Long timestamp) {
        try {
            return litePullConsumer.offsetForTimestamp(convertToMessageQueue(topicPartition), timestamp);
        } catch (MQClientException ex) {
            LOGGER.warn("Get offset for topic partition:{} with timestamp:{} error", topicPartition, timestamp, ex);
            throw new ONSClientException("Failed to get offset");
        }
    }

    @Override public Long committed(TopicPartition topicPartition) {
        try {
            return litePullConsumer.committed(convertToMessageQueue(topicPartition));
        } catch (MQClientException ex) {
            LOGGER.warn("Get committed offset for topic partition: {} error", topicPartition, ex);
            throw new ONSClientException("Failed to get committed offset");
        }
    }

    @Override public void commitSync() {
        litePullConsumer.commitSync();
    }

    @Override public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.litePullConsumer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.litePullConsumer.shutdown();
        }
        super.shutdown();
    }
}
