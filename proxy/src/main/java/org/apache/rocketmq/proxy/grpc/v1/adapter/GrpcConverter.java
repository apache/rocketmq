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

package org.apache.rocketmq.proxy.grpc.v1.adapter;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.ChangeInvisibleDurationRequest;
import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.Digest;
import apache.rocketmq.v1.DigestType;
import apache.rocketmq.v1.Encoding;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.ProducerData;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReportMessageConsumptionResultRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SubscriptionEntry;
import apache.rocketmq.v1.SystemAttribute;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Context;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.proxy.common.DelayPolicy;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcConverter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    public static String wrapResourceWithNamespace(Resource resource) {
        return NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
    }

    public static SendMessageRequestHeader buildSendMessageRequestHeader(SendMessageRequest request) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        Message message = request.getMessage();
        SystemAttribute systemAttribute = message.getSystemAttribute();

        Map<String, String> property = buildMessageProperty(message);
        requestHeader.setProducerGroup(wrapResourceWithNamespace(systemAttribute.getProducerGroup()));
        requestHeader.setTopic(wrapResourceWithNamespace(message.getTopic()));
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

    public static PopMessageRequestHeader buildPopMessageRequestHeader(ReceiveMessageRequest request, long pollTime) {
        Resource group = request.getGroup();
        String groupName = GrpcConverter.wrapResourceWithNamespace(group);
        Partition partition = request.getPartition();
        Resource topic = partition.getTopic();
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        int queueId = partition.getId();
        int maxMessageNumbers = request.getBatchSize();
        if (maxMessageNumbers > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
            log.warn("change maxNums from {} to {} for pop request, with info: topic:{}, group:{}",
                maxMessageNumbers, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, topicName, groupName);
            maxMessageNumbers = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
        }
        long invisibleTime = Durations.toMillis(request.getInvisibleDuration());
        long bornTime = Timestamps.toMillis(request.getInitializationTimestamp());
        ConsumePolicy policy = request.getConsumePolicy();
        int initMode = GrpcConverter.buildConsumeInitMode(policy);

        FilterExpression filterExpression = request.getFilterExpression();
        String expression = filterExpression.getExpression();
        String expressionType = GrpcConverter.buildExpressionType(filterExpression.getType());

        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(groupName);
        requestHeader.setTopic(topicName);
        requestHeader.setQueueId(queueId);
        requestHeader.setMaxMsgNums(maxMessageNumbers);
        requestHeader.setInvisibleTime(invisibleTime);
        requestHeader.setPollTime(pollTime);
        requestHeader.setBornTime(bornTime);
        requestHeader.setInitMode(initMode);
        requestHeader.setExpType(expressionType);
        requestHeader.setExp(expression);
        requestHeader.setOrder(request.getFifoFlag());

        return requestHeader;
    }

    public static AckMessageRequestHeader buildAckMessageRequestHeader(AckMessageRequest request) {
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());

        AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
        ackMessageRequestHeader.setConsumerGroup(groupName);
        ackMessageRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
        ackMessageRequestHeader.setQueueId(handle.getQueueId());
        ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
        ackMessageRequestHeader.setOffset(handle.getOffset());
        return ackMessageRequestHeader;
    }

    public static ChangeInvisibleTimeRequestHeader buildChangeInvisibleTimeRequestHeader(NackMessageRequest request,
        DelayPolicy delayPolicy) {
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());

        ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
        changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
        changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
        changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
        changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
        changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
        changeInvisibleTimeRequestHeader.setInvisibleTime(
            delayPolicy.getDelayInterval(ConfigurationManager.getProxyConfig().getRetryDelayLevelDelta() + request.getDeliveryAttempt()));
        return changeInvisibleTimeRequestHeader;
    }

    public static ChangeInvisibleTimeRequestHeader buildChangeInvisibleTimeRequestHeader(
        ChangeInvisibleDurationRequest request) {
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());

        ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
        changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
        changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
        changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
        changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
        changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
        changeInvisibleTimeRequestHeader.setInvisibleTime(Durations.toMillis(request.getInvisibleDuration()));
        return changeInvisibleTimeRequestHeader;
    }

    public static ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackRequestHeader(
        ForwardMessageToDeadLetterQueueRequest request) {
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());

        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setOffset(handle.getCommitLogOffset());
        consumerSendMsgBackRequestHeader.setGroup(groupName);
        consumerSendMsgBackRequestHeader.setDelayLevel(-1);
        consumerSendMsgBackRequestHeader.setOriginMsgId(request.getMessageId());
        consumerSendMsgBackRequestHeader.setOriginTopic(handle.getRealTopic(topicName, groupName));
        consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(request.getMaxDeliveryAttempts());
        return consumerSendMsgBackRequestHeader;
    }

    public static ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackToDLQRequestHeader(
        NackMessageRequest request) {
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());

        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setOffset(handle.getCommitLogOffset());
        consumerSendMsgBackRequestHeader.setGroup(groupName);
        consumerSendMsgBackRequestHeader.setDelayLevel(-1);
        consumerSendMsgBackRequestHeader.setOriginMsgId(request.getMessageId());
        consumerSendMsgBackRequestHeader.setOriginTopic(handle.getRealTopic(topicName, groupName));
        consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(request.getMaxDeliveryAttempts());
        return consumerSendMsgBackRequestHeader;
    }

    public static EndTransactionRequestHeader buildEndTransactionRequestHeader(EndTransactionRequest request) {
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String messageId = request.getMessageId();
        String transactionId = request.getTransactionId();
        TransactionId handle;
        try {
            handle = TransactionId.decode(transactionId);
        } catch (UnknownHostException e) {
            throw new ProxyException(Code.INVALID_ARGUMENT, "Parse transaction id failed", e);
        }
        long transactionStateTableOffset = handle.getTranStateTableOffset();
        long commitLogOffset = handle.getCommitLogOffset();
        boolean fromTransactionCheck = request.getSource() == EndTransactionRequest.Source.SERVER_CHECK;
        int commitOrRollback = GrpcConverter.buildTransactionCommitOrRollback(request.getResolution());

        EndTransactionRequestHeader endTransactionRequestHeader = new EndTransactionRequestHeader();
        endTransactionRequestHeader.setProducerGroup(groupName);
        endTransactionRequestHeader.setMsgId(messageId);
        endTransactionRequestHeader.setTransactionId(handle.getBrokerTransactionId());
        endTransactionRequestHeader.setTranStateTableOffset(transactionStateTableOffset);
        endTransactionRequestHeader.setCommitLogOffset(commitLogOffset);
        endTransactionRequestHeader.setCommitOrRollback(commitOrRollback);
        endTransactionRequestHeader.setFromTransactionCheck(fromTransactionCheck);

        return endTransactionRequestHeader;
    }

    public static PullMessageRequestHeader buildPullMessageRequestHeader(PullMessageRequest request, long pollTimeoutInMillis) {
        Partition partition = request.getPartition();
        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        String topicName = GrpcConverter.wrapResourceWithNamespace(partition.getTopic());

        int queueId = partition.getId();
        int sysFlag = PullSysFlag.buildSysFlag(false, true, true, false, false);
        String expression = request.getFilterExpression().getExpression();
        String expressionType = GrpcConverter.buildExpressionType(request.getFilterExpression().getType());

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(groupName);
        requestHeader.setTopic(topicName);
        requestHeader.setQueueId(queueId);
        requestHeader.setQueueOffset(request.getOffset());
        requestHeader.setMaxMsgNums(request.getBatchSize());
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setCommitOffset(0L);
        requestHeader.setSuspendTimeoutMillis(pollTimeoutInMillis);
        requestHeader.setSubscription(expression);
        requestHeader.setSubVersion(0L);
        requestHeader.setExpressionType(expressionType);
        return requestHeader;
    }

    public static Map<String, String> buildMessageProperty(Message message) {
        org.apache.rocketmq.common.message.Message messageWithHeader = new org.apache.rocketmq.common.message.Message();
        // set user properties
        Map<String, String> userProperties = message.getUserAttributeMap();
        for (String key : userProperties.keySet()) {
            if (MessageConst.STRING_HASH_SET.contains(key)) {
                throw new ProxyException(Code.INVALID_ARGUMENT, "property is used by system: " + key);
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
            throw new ProxyException(Code.INVALID_ARGUMENT, "message id is empty");
        }
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);

        // set transaction property
        MessageType messageType = message.getSystemAttribute().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");

            Duration transactionResolveDelay = message.getSystemAttribute().getOrphanedTransactionRecoveryPeriod();

            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                String.valueOf(Durations.toSeconds(transactionResolveDelay)));
        }
        // set delay level or deliver timestamp
        switch (message.getSystemAttribute().getTimedDeliveryCase()) {
            case DELAY_LEVEL:
                int delayLevel = message.getSystemAttribute().getDelayLevel();
                if (delayLevel > 0) {
                    MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(delayLevel));
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
                throw new ProxyException(Code.INVALID_ARGUMENT, "unexpected value: " + message.getSystemAttribute().getTimedDeliveryCase());
        }
        // set reconsume times
        int reconsumeTimes = message.getSystemAttribute().getDeliveryAttempt();
        MessageAccessor.setReconsumeTime(messageWithHeader, String.valueOf(reconsumeTimes));
        // set producer group
        Resource producerGroup = message.getSystemAttribute().getProducerGroup();
        String producerGroupName = wrapResourceWithNamespace(producerGroup);
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

    public static org.apache.rocketmq.common.message.Message buildMessage(Message protoMessage) {
        String topic = wrapResourceWithNamespace(protoMessage.getTopic());

        org.apache.rocketmq.common.message.Message message =
            new org.apache.rocketmq.common.message.Message(topic, protoMessage.getBody().toByteArray());
        Map<String, String> messageProperty = buildMessageProperty(protoMessage);

        MessageAccessor.setProperties(message, messageProperty);
        return message;
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
        buildProducerData.setGroupName(wrapResourceWithNamespace(producerData.getGroup()));
        return buildProducerData;
    }

    public static org.apache.rocketmq.common.protocol.heartbeat.ConsumerData buildConsumerData(ConsumerData consumerData) {
        org.apache.rocketmq.common.protocol.heartbeat.ConsumerData buildConsumerData = new org.apache.rocketmq.common.protocol.heartbeat.ConsumerData();
        buildConsumerData.setGroupName(wrapResourceWithNamespace(consumerData.getGroup()));
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
            String topicName = GrpcConverter.wrapResourceWithNamespace(sub.getTopic());
            FilterExpression filterExpression = sub.getExpression();
            subscriptionDataSet.add(buildSubscriptionData(topicName, filterExpression));
        }
        return subscriptionDataSet;
    }

    public static SubscriptionData buildSubscriptionData(String topicName, FilterExpression filterExpression) {
        String expression = filterExpression.getExpression();
        String expressionType = GrpcConverter.buildExpressionType(filterExpression.getType());
        try {
            return FilterAPI.build(topicName, expression, expressionType);
        } catch (Exception e) {
            throw new ProxyException(Code.INVALID_ARGUMENT, "expression format is not correct", e);
        }
    }

    public static int buildConsumeInitMode(ConsumePolicy policy) {
        switch (policy) {
            case PLAYBACK:
                return ConsumeInitMode.MIN;
            case RESUME:
            default:
                return ConsumeInitMode.MAX;
        }
    }

    public static Message buildMessage(MessageExt messageExt) {
        Map<String, String> userAttributes = buildUserAttributes(messageExt);
        SystemAttribute systemAttributes = buildSystemAttributes(messageExt);
        Resource topic = buildResource(messageExt.getTopic());

        return Message.newBuilder()
            .setTopic(topic)
            .putAllUserAttribute(userAttributes)
            .setSystemAttribute(systemAttributes)
            .setBody(ByteString.copyFrom(messageExt.getBody()))
            .build();
    }

    protected static Map<String, String> buildUserAttributes(MessageExt messageExt) {
        Map<String, String> userAttributes = new HashMap<>();
        Map<String, String> properties = messageExt.getProperties();

        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (!MessageConst.STRING_HASH_SET.contains(property.getKey())) {
                userAttributes.put(property.getKey(), property.getValue());
            }
        }

        return userAttributes;
    }

    protected static SystemAttribute buildSystemAttributes(MessageExt messageExt) {
        SystemAttribute.Builder systemAttributeBuilder = SystemAttribute.newBuilder();

        // tag
        String tag = messageExt.getUserProperty(MessageConst.PROPERTY_TAGS);
        if (tag != null) {
            systemAttributeBuilder.setTag(tag);
        }

        // keys
        String keys = messageExt.getKeys();
        if (keys != null) {
            String[] keysArray = keys.split(MessageConst.KEY_SEPARATOR);
            systemAttributeBuilder.addAllKeys(Arrays.asList(keysArray));
        }

        // message_id
        String uniqKey = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqKey != null) {
            systemAttributeBuilder.setMessageId(uniqKey);
        }

        // body_digest & body_encoding
        String md5Result = BinaryUtil.generateMd5(messageExt.getBody());
        Digest digest = Digest.newBuilder()
            .setType(DigestType.MD5)
            .setChecksum(md5Result)
            .build();
        systemAttributeBuilder.setBodyDigest(digest);

        if ((messageExt.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
            systemAttributeBuilder.setBodyEncoding(Encoding.GZIP);
        } else {
            systemAttributeBuilder.setBodyEncoding(Encoding.IDENTITY);
        }

        // message_type
        String isTrans = messageExt.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        String isTransValue = "true";
        if (isTransValue.equals(isTrans)) {
            systemAttributeBuilder.setMessageType(MessageType.TRANSACTION);
        } else if (messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
            || messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS) != null
            || messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
            systemAttributeBuilder.setMessageType(MessageType.DELAY);
        } else if (messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY) != null) {
            systemAttributeBuilder.setMessageType(MessageType.FIFO);
        } else {
            systemAttributeBuilder.setMessageType(MessageType.NORMAL);
        }

        // born_timestamp (millis)
        long bornTimestamp = messageExt.getBornTimestamp();
        systemAttributeBuilder.setBornTimestamp(Timestamps.fromMillis(bornTimestamp));

        // born_host
        systemAttributeBuilder.setBornHost(messageExt.getBornHostString());

        // store_timestamp (millis)
        long storeTimestamp = messageExt.getStoreTimestamp();
        systemAttributeBuilder.setStoreTimestamp(Timestamps.fromMillis(storeTimestamp));

        // store_host
        SocketAddress storeHost = messageExt.getStoreHost();
        if (storeHost != null) {
            systemAttributeBuilder.setStoreHost(storeHost.toString());
        }

        // delay_level
        String delayLevel = messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (delayLevel != null) {
            systemAttributeBuilder.setDelayLevel(Integer.parseInt(delayLevel));
        }

        // delivery_timestamp
        String deliverMsString;
        long deliverMs;
        if (messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
            long delayMs = TimeUnit.SECONDS.toMillis(Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)));
            deliverMs = System.currentTimeMillis() + delayMs;
            systemAttributeBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliverMs));
        } else {
            deliverMsString = messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
            if (deliverMsString != null) {
                deliverMs = Long.parseLong(deliverMsString);
                systemAttributeBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliverMs));
            }
        }

        // sharding key
        String shardingKey = messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
        if (shardingKey != null) {
            systemAttributeBuilder.setMessageGroup(shardingKey);
        }

        // receipt_handle && invisible_period
        ReceiptHandle receiptHandle = ReceiptHandle.create(messageExt);
        if (receiptHandle != null) {
            systemAttributeBuilder.setReceiptHandle(receiptHandle.encode());
        }

        // partition_id
        systemAttributeBuilder.setPartitionId(messageExt.getQueueId());

        // partition_offset
        systemAttributeBuilder.setPartitionOffset(messageExt.getQueueOffset());

        // delivery_attempt
        systemAttributeBuilder.setDeliveryAttempt(messageExt.getReconsumeTimes() + 1);

        // publisher_group
        String producerGroup = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        if (producerGroup != null) {
            systemAttributeBuilder.setProducerGroup(buildResource(producerGroup));
        }

        // trace context
        String traceContext = messageExt.getProperty(MessageConst.PROPERTY_TRACE_CONTEXT);
        if (traceContext != null) {
            systemAttributeBuilder.setTraceContext(traceContext);
        }

        return systemAttributeBuilder.build();
    }

    public static int buildTransactionCommitOrRollback(EndTransactionRequest.TransactionResolution type) {
        switch (type) {
            case COMMIT:
                return MessageSysFlag.TRANSACTION_COMMIT_TYPE;
            case ROLLBACK:
                return MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
            default:
                return MessageSysFlag.TRANSACTION_NOT_TYPE;
        }
    }

    public static ConsumeMessageDirectlyResult buildConsumeMessageDirectlyResult(
        ReportMessageConsumptionResultRequest request) {
        ConsumeMessageDirectlyResult consumeMessageDirectlyResult = new ConsumeMessageDirectlyResult();
        switch (request.getStatus().getCode()) {
            case Code.OK_VALUE: {
                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_SUCCESS);
                break;
            }
            case Code.INTERNAL_VALUE: {
                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_LATER);
                break;
            }
            case Code.INVALID_ARGUMENT_VALUE: {
                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_RETURN_NULL);
                break;
            }
        }
        consumeMessageDirectlyResult.setRemark("From gRPC client");
        return consumeMessageDirectlyResult;
    }

    public static Resource buildResource(String resourceStr) {
        return Resource.newBuilder()
            .setResourceNamespace(NamespaceUtil.getNamespaceFromResource(resourceStr))
            .setName(NamespaceUtil.withoutNamespace(resourceStr))
            .build();
    }

    public static UnregisterClientRequestHeader buildUnregisterClientRequestHeader(NotifyClientTerminationRequest request) {
        UnregisterClientRequestHeader header = new UnregisterClientRequestHeader();
        header.setClientID(request.getClientId());
        if (request.hasProducerGroup()) {
            header.setProducerGroup(wrapResourceWithNamespace(request.getProducerGroup()));
        }
        if (request.hasConsumerGroup()) {
            header.setConsumerGroup(wrapResourceWithNamespace(request.getConsumerGroup()));
        }
        return header;
    }

    public static long buildPollTimeFromContext(Context ctx) {
        long timeRemaining = ctx.getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS);
        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
        if (pollTime <= 0) {
            pollTime = timeRemaining;
        }

        return pollTime;
    }

}
