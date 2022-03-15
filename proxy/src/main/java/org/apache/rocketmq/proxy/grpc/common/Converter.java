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

package org.apache.rocketmq.proxy.grpc.common;

import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.Encoding;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.ProducerData;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SubscriptionEntry;
import apache.rocketmq.v1.SystemAttribute;
import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    public static String getResourceNameWithNamespace(Resource resource) {
        return NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
    }

    public static SendMessageRequestHeader buildSendMessageRequestHeader(SendMessageRequest request) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        Message message = request.getMessage();
        SystemAttribute systemAttribute = message.getSystemAttribute();

        Map<String, String> property = buildMessageProperty(message);
        requestHeader.setProducerGroup(getResourceNameWithNamespace(systemAttribute.getProducerGroup()));
        requestHeader.setTopic(getResourceNameWithNamespace(message.getTopic()));
        requestHeader.setDefaultTopic("");
        requestHeader.setDefaultTopicQueueNums(0);
        requestHeader.setQueueId(systemAttribute.getPartitionId());
        // sysFlag (body encoding & message type)
        int sysFlag = 0;
        Encoding bodyEncoding = systemAttribute.getBodyEncoding();
        if (bodyEncoding.equals(Encoding.GZIP)) {
            sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
        }
        // transaction
        MessageType messageType = systemAttribute.getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        }
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setBornTimestamp(Timestamps.toMillis(systemAttribute.getBornTimestamp()));
        requestHeader.setFlag(0);
        requestHeader.setProperties(MessageDecoder.messageProperties2String(property));
        requestHeader.setReconsumeTimes(systemAttribute.getDeliveryAttempt());

        return requestHeader;
    }

    public static Map<String, String> buildMessageProperty(Message message) {
        org.apache.rocketmq.common.message.Message messageWithHeader = new org.apache.rocketmq.common.message.Message();
        // set user properties
        Map<String, String> userProperties = message.getUserAttributeMap();
        for (String key : userProperties.keySet()) {
            if (MessageConst.STRING_HASH_SET.contains(key)) {
                throw new IllegalArgumentException("Property is used by system: " + key);
            }
        }
        MessageAccessor.setProperties(messageWithHeader, Maps.newHashMap(userProperties));
        // set tag
        String tag = message.getSystemAttribute().getTag();
        if (!"".equals(tag)) {
            messageWithHeader.setTags(tag);
        }
        // set keys
        List<String> keysList = message.getSystemAttribute().getKeysList();
        if (keysList.size() > 0) {
            messageWithHeader.setKeys(keysList);
        }
        // set message id
        String messageId = message.getSystemAttribute().getMessageId();
        if ("".equals(messageId)) {
            throw new IllegalArgumentException("message id is empty");
        }
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);
        // set transaction property
        MessageType messageType = message.getSystemAttribute().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");

            Duration transactionResolveDelay = message.getSystemAttribute().getOrphanedTransactionRecoveryPeriod();

            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(15));
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                String.valueOf(Durations.toSeconds(transactionResolveDelay)));
        }
        // set delay level or deliver timestamp
        switch (message.getSystemAttribute().getTimedDeliveryCase()) {
            case DELAY_LEVEL:
                int delayLevel = message.getSystemAttribute().getDelayLevel();
                if (delayLevel > 0) {
                    MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_DELAY_TIME_LEVEL,
                        String.valueOf(delayLevel));
                }
                break;
            case DELIVERY_TIMESTAMP:
                Timestamp deliveryTimestamp = message.getSystemAttribute().getDeliveryTimestamp();
                String timestampString = String.valueOf(Timestamps.toMillis(deliveryTimestamp));
                MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TIMER_DELIVER_MS, timestampString);
                break;
            case TIMEDDELIVERY_NOT_SET:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + message.getSystemAttribute().getTimedDeliveryCase());
        }
        // set reconsume times
        int reconsumeTimes = message.getSystemAttribute().getDeliveryAttempt();
        MessageAccessor.setReconsumeTime(messageWithHeader, String.valueOf(reconsumeTimes));
        // set producer group
        Resource producerGroup = message.getSystemAttribute().getProducerGroup();
        String producerGroupName = getResourceNameWithNamespace(producerGroup);
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroupName);
        // set message group
        String messageGroup = message.getSystemAttribute().getMessageGroup();
        if (!messageGroup.isEmpty()) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
        }
        // set trace context
        String traceContext = message.getSystemAttribute().getTraceContext();
        if (!traceContext.isEmpty()) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);
        }
        return messageWithHeader.getProperties();
    }

    public static String buildExpressionType(FilterType filterType) {
        switch (filterType) {
            case SQL:
                return ExpressionType.SQL92;
            case TAG:
            default:
                return ExpressionType.TAG;
        }
    }

    public static HeartbeatData buildHeartbeatData(HeartbeatRequest request) {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(request.getClientId());
        Set<org.apache.rocketmq.common.protocol.heartbeat.ProducerData> producerDataSet = new HashSet<>();
        producerDataSet.add(buildProducerData(request.getProducerData()));
        heartbeatData.setProducerDataSet(producerDataSet);
        Set<org.apache.rocketmq.common.protocol.heartbeat.ConsumerData> consumerDataSet = new HashSet<>();
        consumerDataSet.add(buildConsumerData(request.getConsumerData()));
        heartbeatData.setConsumerDataSet(consumerDataSet);
        return heartbeatData;
    }

    public static org.apache.rocketmq.common.protocol.heartbeat.ProducerData buildProducerData(ProducerData producerData) {
        org.apache.rocketmq.common.protocol.heartbeat.ProducerData buildProducerData = new org.apache.rocketmq.common.protocol.heartbeat.ProducerData();
        buildProducerData.setGroupName(getResourceNameWithNamespace(producerData.getGroup()));
        return buildProducerData;
    }

    public static org.apache.rocketmq.common.protocol.heartbeat.ConsumerData buildConsumerData(ConsumerData consumerData) {
        org.apache.rocketmq.common.protocol.heartbeat.ConsumerData buildConsumerData = new org.apache.rocketmq.common.protocol.heartbeat.ConsumerData();
        buildConsumerData.setGroupName(getResourceNameWithNamespace(consumerData.getGroup()));
        buildConsumerData.setConsumeType(buildConsumeType(consumerData.getConsumeType()));
        buildConsumerData.setMessageModel(buildMessageModel(consumerData.getConsumeModel()));
        buildConsumerData.setConsumeFromWhere(buildConsumeFromWhere(consumerData.getConsumePolicy()));
        Set<SubscriptionData> subscriptionDataSet = buildSubscriptionDataSet(consumerData.getSubscriptionsList());
        buildConsumerData.setSubscriptionDataSet(subscriptionDataSet);
        return buildConsumerData;
    }

    public static ConsumeType buildConsumeType(ConsumeMessageType consumeMessageType) {
        switch (consumeMessageType) {
            case ACTIVE:
                return ConsumeType.CONSUME_ACTIVELY;
            case PASSIVE:
            default:
                return ConsumeType.CONSUME_PASSIVELY;
        }
    }

    public static MessageModel buildMessageModel(ConsumeModel consumeModel) {
        switch (consumeModel) {
            case BROADCASTING:
                return MessageModel.BROADCASTING;
            case CLUSTERING:
            default:
                return MessageModel.CLUSTERING;
        }
    }

    public static ConsumeFromWhere buildConsumeFromWhere(ConsumePolicy policy) {
        switch (policy) {
            case PLAYBACK:
                return ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
            case DISCARD:
                return ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET;
            case TARGET_TIMESTAMP:
                return ConsumeFromWhere.CONSUME_FROM_TIMESTAMP;
            case RESUME:
            default:
                return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
        }
    }

    public static Set<SubscriptionData> buildSubscriptionDataSet(List<SubscriptionEntry> subscriptionEntryList) {
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        for (SubscriptionEntry sub : subscriptionEntryList) {
            String topicName = Converter.getResourceNameWithNamespace(sub.getTopic());
            FilterExpression filterExpression = sub.getExpression();
            String expression = filterExpression.getExpression();
            String expressionType = Converter.buildExpressionType(filterExpression.getType());
            try {
                SubscriptionData subscriptionData = FilterAPI.build(topicName, expression, expressionType);
                subscriptionDataSet.add(subscriptionData);
            } catch (Exception e) {
                throw new IllegalArgumentException("Build subscription failed when apply heartbeat", e);
            }
        }
        return subscriptionDataSet;
    }
}
