///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.rocketmq.proxy.grpc.v2.common;
//
//import apache.rocketmq.v2.AckMessageRequest;
//import apache.rocketmq.v2.Broker;
//import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
//import apache.rocketmq.v2.ClientType;
//import apache.rocketmq.v2.Code;
//import apache.rocketmq.v2.Digest;
//import apache.rocketmq.v2.DigestType;
//import apache.rocketmq.v2.Encoding;
//import apache.rocketmq.v2.EndTransactionRequest;
//import apache.rocketmq.v2.FilterExpression;
//import apache.rocketmq.v2.FilterType;
//import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
//import apache.rocketmq.v2.HeartbeatRequest;
//import apache.rocketmq.v2.Message;
//import apache.rocketmq.v2.MessageQueue;
//import apache.rocketmq.v2.MessageType;
//import apache.rocketmq.v2.NotifyClientTerminationRequest;
//import apache.rocketmq.v2.Permission;
//import apache.rocketmq.v2.ReceiveMessageRequest;
//import apache.rocketmq.v2.Resource;
//import apache.rocketmq.v2.SendMessageRequest;
//import apache.rocketmq.v2.Settings;
//import apache.rocketmq.v2.SubscriptionEntry;
//import apache.rocketmq.v2.SystemProperties;
//import apache.rocketmq.v2.TransactionResolution;
//import apache.rocketmq.v2.TransactionSource;
//import apache.rocketmq.v2.VerifyMessageResult;
//import com.google.common.collect.Maps;
//import com.google.protobuf.ByteString;
//import com.google.protobuf.Duration;
//import com.google.protobuf.Timestamp;
//import com.google.protobuf.util.Durations;
//import com.google.protobuf.util.Timestamps;
//import io.grpc.Context;
//import java.net.SocketAddress;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.rocketmq.common.constant.ConsumeInitMode;
//import org.apache.rocketmq.common.constant.LoggerName;
//import org.apache.rocketmq.common.constant.PermName;
//import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
//import org.apache.rocketmq.common.consumer.ReceiptHandle;
//import org.apache.rocketmq.common.filter.ExpressionType;
//import org.apache.rocketmq.common.filter.FilterAPI;
//import org.apache.rocketmq.common.message.MessageAccessor;
//import org.apache.rocketmq.common.message.MessageConst;
//import org.apache.rocketmq.common.message.MessageDecoder;
//import org.apache.rocketmq.common.message.MessageExt;
//import org.apache.rocketmq.common.protocol.NamespaceUtil;
//import org.apache.rocketmq.common.protocol.body.CMResult;
//import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
//import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
//import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
//import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
//import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
//import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
//import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
//import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
//import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
//import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
//import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
//import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
//import org.apache.rocketmq.common.protocol.route.QueueData;
//import org.apache.rocketmq.common.sysflag.MessageSysFlag;
//import org.apache.rocketmq.common.utils.BinaryUtil;
//import org.apache.rocketmq.logging.InternalLogger;
//import org.apache.rocketmq.logging.InternalLoggerFactory;
//import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
//import org.apache.rocketmq.proxy.config.ConfigurationManager;
//import org.apache.rocketmq.proxy.service.transaction.TransactionId;
//
//public class GrpcConverter {
//    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
//
//    public static String wrapResourceWithNamespace(Resource resource) {
//        return NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
//    }
//
//    public static HeartbeatData buildHeartbeatData(String clientId, HeartbeatRequest request,
//        Settings clientSettings) {
//        HeartbeatData heartbeatData = new HeartbeatData();
//        heartbeatData.setClientID(clientId);
//        switch (clientSettings.getClientType()) {
//            case PRODUCER: {
//                Set<org.apache.rocketmq.common.protocol.heartbeat.ProducerData> producerDataSet = new HashSet<>();
//                for (Resource topic : clientSettings.getPublishing().getTopicsList()) {
//                    String topicName = wrapResourceWithNamespace(topic);
//                    producerDataSet.add(buildProducerData(topicName));
//                }
//                heartbeatData.setProducerDataSet(producerDataSet);
//                break;
//            }
//            case PUSH_CONSUMER:
//            case SIMPLE_CONSUMER: {
//                String groupName = wrapResourceWithNamespace(request.getGroup());
//                Set<org.apache.rocketmq.common.protocol.heartbeat.ConsumerData> consumerDataSet = new HashSet<>();
//                consumerDataSet.add(buildConsumerData(groupName, clientSettings));
//                heartbeatData.setConsumerDataSet(consumerDataSet);
//                break;
//            }
//        }
//        return heartbeatData;
//    }
//
//    public static org.apache.rocketmq.common.protocol.heartbeat.ProducerData buildProducerData(String groupName) {
//        org.apache.rocketmq.common.protocol.heartbeat.ProducerData buildProducerData
//            = new org.apache.rocketmq.common.protocol.heartbeat.ProducerData();
//        buildProducerData.setGroupName(groupName);
//        return buildProducerData;
//    }
//
//    public static org.apache.rocketmq.common.protocol.heartbeat.ConsumerData buildConsumerData(String groupName,
//        Settings clientSettings) {
//        org.apache.rocketmq.common.protocol.heartbeat.ConsumerData buildConsumerData = new org.apache.rocketmq.common.protocol.heartbeat.ConsumerData();
//        buildConsumerData.setGroupName(groupName);
//        buildConsumerData.setConsumeType(buildConsumeType(clientSettings.getClientType()));
//
//        buildConsumerData.setMessageModel(MessageModel.CLUSTERING);
//        buildConsumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
//        Set<SubscriptionData> subscriptionDataSet =
//            buildSubscriptionDataSet(clientSettings.getSubscription().getSubscriptionsList());
//        buildConsumerData.setSubscriptionDataSet(subscriptionDataSet);
//        return buildConsumerData;
//    }
//
//    public static ConsumeType buildConsumeType(ClientType clientType) {
//        switch (clientType) {
//            case SIMPLE_CONSUMER:
//                return ConsumeType.CONSUME_ACTIVELY;
//            case PUSH_CONSUMER:
//                return ConsumeType.CONSUME_PASSIVELY;
//            default:
//                throw new IllegalArgumentException("Client type is not consumer, type: " + clientType);
//        }
//    }
//
//    public static SendMessageRequestHeader buildSendMessageRequestHeader(SendMessageRequest request,
//        String producerGroup, int queueId) {
//        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
//
//        if (request.getMessagesCount() <= 0) {
//            throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "no message to send");
//        }
//        if (request.getMessagesCount() > 1) {
//            requestHeader.setBatch(true);
//        }
//        Message message = request.getMessages(0);
//        SystemProperties systemProperties = message.getSystemProperties();
//
//        Map<String, String> property = buildMessageProperty(message, producerGroup);
//        requestHeader.setProducerGroup(producerGroup);
//        requestHeader.setTopic(wrapResourceWithNamespace(message.getTopic()));
//        requestHeader.setDefaultTopic("");
//        requestHeader.setDefaultTopicQueueNums(0);
//        requestHeader.setQueueId(queueId);
//        // sysFlag (body encoding & message type)
//        int sysFlag = 0;
//        Encoding bodyEncoding = systemProperties.getBodyEncoding();
//        if (bodyEncoding.equals(Encoding.GZIP)) {
//            sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
//        }
//        // transaction
//        MessageType messageType = systemProperties.getMessageType();
//        if (messageType.equals(MessageType.TRANSACTION)) {
//            sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
//        }
//        requestHeader.setSysFlag(sysFlag);
//        requestHeader.setBornTimestamp(Timestamps.toMillis(systemProperties.getBornTimestamp()));
//        requestHeader.setFlag(0);
//        requestHeader.setProperties(MessageDecoder.messageProperties2String(property));
//        requestHeader.setReconsumeTimes(systemProperties.getDeliveryAttempt());
//
//        return requestHeader;
//    }
//
//    public static PopMessageRequestHeader buildPopMessageRequestHeader(ReceiveMessageRequest request, long pollTime, boolean isFifo) {
//        Resource group = request.getGroup();
//        String groupName = GrpcConverter.wrapResourceWithNamespace(group);
//        MessageQueue messageQueue = request.getMessageQueue();
//        Resource topic = messageQueue.getTopic();
//        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
//        int queueId = messageQueue.getId();
//        int maxMessageNumbers = request.getBatchSize();
//        if (maxMessageNumbers > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
//            log.warn("change maxNums from {} to {} for pop request, with info: topic:{}, group:{}",
//                maxMessageNumbers, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, topicName, groupName);
//            maxMessageNumbers = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
//        }
//        long invisibleTime = Durations.toMillis(request.getInvisibleDuration());
//        long bornTime = System.currentTimeMillis();
//
//        FilterExpression filterExpression = request.getFilterExpression();
//        String expression = filterExpression.getExpression();
//        String expressionType = GrpcConverter.buildExpressionType(filterExpression.getType());
//
//        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
//        requestHeader.setConsumerGroup(groupName);
//        requestHeader.setTopic(topicName);
//        requestHeader.setQueueId(queueId);
//        requestHeader.setMaxMsgNums(maxMessageNumbers);
//        requestHeader.setInvisibleTime(invisibleTime);
//        requestHeader.setPollTime(pollTime);
//        requestHeader.setBornTime(bornTime);
//        requestHeader.setInitMode(ConsumeInitMode.MAX);
//        requestHeader.setExpType(expressionType);
//        requestHeader.setExp(expression);
//        requestHeader.setOrder(isFifo);
//
//        return requestHeader;
//    }
//
//    public static AckMessageRequestHeader buildAckMessageRequestHeader(ReceiveMessageRequest request, ReceiptHandle handle) {
//        return buildAckMessageRequestHeader(request.getMessageQueue().getTopic(), request.getGroup(), handle);
//    }
//
//    public static AckMessageRequestHeader buildAckMessageRequestHeader(AckMessageRequest request, ReceiptHandle handle) {
//        return buildAckMessageRequestHeader(request.getTopic(), request.getGroup(), handle);
//    }
//
//    public static AckMessageRequestHeader buildAckMessageRequestHeader(Resource topic, Resource group, ReceiptHandle handle) {
//        String groupName = GrpcConverter.wrapResourceWithNamespace(group);
//        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
//
//        AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
//        ackMessageRequestHeader.setConsumerGroup(groupName);
//        ackMessageRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
//        ackMessageRequestHeader.setQueueId(handle.getQueueId());
//        ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
//        ackMessageRequestHeader.setOffset(handle.getOffset());
//        return ackMessageRequestHeader;
//    }
//
//    public static ChangeInvisibleTimeRequestHeader buildChangeInvisibleTimeRequestHeader(ChangeInvisibleDurationRequest request) {
//        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
//        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
//        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());
//
//        ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
//        changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
//        changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
//        changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
//        changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
//        changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
//        changeInvisibleTimeRequestHeader.setInvisibleTime(Durations.toMillis(request.getInvisibleDuration()));
//        return changeInvisibleTimeRequestHeader;
//    }
//
//    public static ChangeInvisibleTimeRequestHeader buildChangeInvisibleTimeRequestHeader(ReceiveMessageRequest request, ReceiptHandle handle) {
//        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
//        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
//
//        ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
//        changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
//        changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
//        changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
//        changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
//        changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
//        changeInvisibleTimeRequestHeader.setInvisibleTime(Durations.toMillis(request.getInvisibleDuration()));
//        return changeInvisibleTimeRequestHeader;
//    }
//
//    public static ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackRequestHeader(ReceiveMessageRequest request,
//        ReceiptHandle handle, String messageId, int maxReconsumeTimes) {
//        return buildConsumerSendMsgBackRequestHeader(request.getMessageQueue().getTopic(), request.getGroup(), handle, messageId, maxReconsumeTimes);
//    }
//
//    public static ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackRequestHeader(
//        ForwardMessageToDeadLetterQueueRequest request) {
//        ReceiptHandle handle = ReceiptHandle.decode(request.getReceiptHandle());
//        return buildConsumerSendMsgBackRequestHeader(request.getTopic(), request.getGroup(), handle,
//            request.getMessageId(), request.getMaxDeliveryAttempts());
//    }
//
//    public static ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackRequestHeader(Resource topic, Resource group, ReceiptHandle handle,
//        String messageId, int maxReconsumeTimes) {
//        String groupName = GrpcConverter.wrapResourceWithNamespace(group);
//        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
//
//        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
//        consumerSendMsgBackRequestHeader.setOffset(handle.getCommitLogOffset());
//        consumerSendMsgBackRequestHeader.setGroup(groupName);
//        consumerSendMsgBackRequestHeader.setDelayLevel(-1);
//        consumerSendMsgBackRequestHeader.setOriginMsgId(messageId);
//        consumerSendMsgBackRequestHeader.setOriginTopic(handle.getRealTopic(topicName, groupName));
//        consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(maxReconsumeTimes);
//        return consumerSendMsgBackRequestHeader;
//    }
//
//    public static EndTransactionRequestHeader buildEndTransactionRequestHeader(EndTransactionRequest request,
//        String producerGroup) {
//        String messageId = request.getMessageId();
//        String transactionId = request.getTransactionId();
//        TransactionId handle;
//        try {
//            handle = TransactionId.decode(transactionId);
//        } catch (Exception e) {
//            throw new GrpcProxyException(Code.INVALID_TRANSACTION_ID, "Parse transaction id failed", e);
//        }
//        long transactionStateTableOffset = handle.getTranStateTableOffset();
//        long commitLogOffset = handle.getCommitLogOffset();
//        boolean fromTransactionCheck = request.getSource() == TransactionSource.SOURCE_SERVER_CHECK;
//        int commitOrRollback = GrpcConverter.buildTransactionCommitOrRollback(request.getResolution());
//
//        EndTransactionRequestHeader endTransactionRequestHeader = new EndTransactionRequestHeader();
//        endTransactionRequestHeader.setProducerGroup(producerGroup);
//        endTransactionRequestHeader.setMsgId(messageId);
//        endTransactionRequestHeader.setTransactionId(handle.getBrokerTransactionId());
//        endTransactionRequestHeader.setTranStateTableOffset(transactionStateTableOffset);
//        endTransactionRequestHeader.setCommitLogOffset(commitLogOffset);
//        endTransactionRequestHeader.setCommitOrRollback(commitOrRollback);
//        endTransactionRequestHeader.setFromTransactionCheck(fromTransactionCheck);
//
//        return endTransactionRequestHeader;
//    }
//
//    public static UnregisterClientRequestHeader buildUnregisterClientRequestHeader(String clientId,
//        ClientType clientType, NotifyClientTerminationRequest request) {
//        UnregisterClientRequestHeader header = new UnregisterClientRequestHeader();
//        String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
//        header.setClientID(clientId);
//        switch (clientType) {
//            case PRODUCER: {
//                header.setProducerGroup(groupName);
//                break;
//            }
//            case PUSH_CONSUMER:
//            case SIMPLE_CONSUMER: {
//                header.setConsumerGroup(groupName);
//                break;
//            }
//        }
//        return header;
//    }
//
//    public static Map<String, String> buildMessageProperty(Message message, String producerGroup) {
//        org.apache.rocketmq.common.message.Message messageWithHeader = new org.apache.rocketmq.common.message.Message();
//        // set user properties
//        Map<String, String> userProperties = message.getUserPropertiesMap();
//        for (String key : userProperties.keySet()) {
//            if (MessageConst.STRING_HASH_SET.contains(key)) {
//                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "property is used by system: " + key);
//            }
//        }
//        MessageAccessor.setProperties(messageWithHeader, Maps.newHashMap(userProperties));
//
//        // set tag
//        String tag = message.getSystemProperties().getTag();
//        if (!"".equals(tag)) {
//            messageWithHeader.setTags(tag);
//        }
//
//        // set keys
//        List<String> keysList = message.getSystemProperties().getKeysList();
//        if (keysList.size() > 0) {
//            messageWithHeader.setKeys(keysList);
//        }
//
//        // set message id
//        String messageId = message.getSystemProperties().getMessageId();
//        if ("".equals(messageId)) {
//            throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_ID, "message id is empty");
//        }
//        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);
//
//        // set transaction property
//        MessageType messageType = message.getSystemProperties().getMessageType();
//        if (messageType.equals(MessageType.TRANSACTION)) {
//            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
//
//            Duration transactionResolveDelay = message.getSystemProperties().getOrphanedTransactionRecoveryDuration();
//
//            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
//                String.valueOf(Durations.toSeconds(transactionResolveDelay)));
//        }
//
//        // set delay level or deliver timestamp
//        if (message.getSystemProperties().hasDeliveryTimestamp()) {
//            Timestamp deliveryTimestamp = message.getSystemProperties().getDeliveryTimestamp();
//            String timestampString = String.valueOf(Timestamps.toMillis(deliveryTimestamp));
//            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TIMER_DELIVER_MS, timestampString);
//        }
//
//        // set reconsume times
//        int reconsumeTimes = message.getSystemProperties().getDeliveryAttempt();
//        MessageAccessor.setReconsumeTime(messageWithHeader, String.valueOf(reconsumeTimes));
//        // set producer group
//        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
//        // set message group
//        String messageGroup = message.getSystemProperties().getMessageGroup();
//        if (!messageGroup.isEmpty()) {
//            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
//        }
//        // set trace context
//        String traceContext = message.getSystemProperties().getTraceContext();
//        if (!traceContext.isEmpty()) {
//            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);
//        }
//        return messageWithHeader.getProperties();
//    }
//
//    public static List<org.apache.rocketmq.common.message.Message> buildMessage(List<Message> protoMessageList,
//        Resource topic) {
//        String topicName = wrapResourceWithNamespace(topic);
//        List<org.apache.rocketmq.common.message.Message> messages = new ArrayList<>();
//        for (Message protoMessage : protoMessageList) {
//            if (!protoMessage.getTopic().equals(topic)) {
//                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "topic in message is not same");
//            }
//            // here use topicName as producerGroup for transactional checker.
//            messages.add(buildMessage(protoMessage, topicName));
//        }
//        return messages;
//    }
//
//    public static org.apache.rocketmq.common.message.Message buildMessage(Message protoMessage, String producerGroup) {
//        String topicName = wrapResourceWithNamespace(protoMessage.getTopic());
//
//        org.apache.rocketmq.common.message.Message message =
//            new org.apache.rocketmq.common.message.Message(topicName, protoMessage.getBody().toByteArray());
//        Map<String, String> messageProperty = buildMessageProperty(protoMessage, producerGroup);
//
//        MessageAccessor.setProperties(message, messageProperty);
//        return message;
//    }
//
//    public static MessageQueue buildMessageQueue(MessageExt messageExt, String brokerName) {
//        Broker broker = Broker.getDefaultInstance();
//        if (!StringUtils.isEmpty(brokerName)) {
//            broker = Broker.newBuilder()
//                .setName(brokerName)
//                .setId(0)
//                .build();
//        }
//        return MessageQueue.newBuilder()
//            .setId(messageExt.getQueueId())
//            .setTopic(Resource.newBuilder()
//                .setName(NamespaceUtil.withoutNamespace(messageExt.getTopic()))
//                .setResourceNamespace(NamespaceUtil.getNamespaceFromResource(messageExt.getTopic()))
//                .build())
//            .setBroker(broker)
//            .build();
//    }
//
//    public static String buildExpressionType(FilterType filterType) {
//        switch (filterType) {
//            case SQL:
//                return ExpressionType.SQL92;
//            case TAG:
//            default:
//                return ExpressionType.TAG;
//        }
//    }
//
//    public static Set<SubscriptionData> buildSubscriptionDataSet(List<SubscriptionEntry> subscriptionEntryList) {
//        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
//        for (SubscriptionEntry sub : subscriptionEntryList) {
//            String topicName = GrpcConverter.wrapResourceWithNamespace(sub.getTopic());
//            FilterExpression filterExpression = sub.getExpression();
//            subscriptionDataSet.add(buildSubscriptionData(topicName, filterExpression));
//        }
//        return subscriptionDataSet;
//    }
//
//    public static SubscriptionData buildSubscriptionData(String topicName, FilterExpression filterExpression) {
//        String expression = filterExpression.getExpression();
//        String expressionType = GrpcConverter.buildExpressionType(filterExpression.getType());
//        try {
//            return FilterAPI.build(topicName, expression, expressionType);
//        } catch (Exception e) {
//            throw new GrpcProxyException(Code.ILLEGAL_FILTER_EXPRESSION, "expression format is not correct", e);
//        }
//    }
//
//    public static Message buildMessage(MessageExt messageExt) {
//        Map<String, String> userProperties = buildUserAttributes(messageExt);
//        SystemProperties systemProperties = buildSystemProperties(messageExt);
//        Resource topic = buildResource(messageExt.getTopic());
//
//        return Message.newBuilder()
//            .setTopic(topic)
//            .putAllUserProperties(userProperties)
//            .setSystemProperties(systemProperties)
//            .setBody(ByteString.copyFrom(messageExt.getBody()))
//            .build();
//    }
//
//    protected static Map<String, String> buildUserAttributes(MessageExt messageExt) {
//        Map<String, String> userAttributes = new HashMap<>();
//        Map<String, String> properties = messageExt.getProperties();
//
//        for (Map.Entry<String, String> property : properties.entrySet()) {
//            if (!MessageConst.STRING_HASH_SET.contains(property.getKey())) {
//                userAttributes.put(property.getKey(), property.getValue());
//            }
//        }
//
//        return userAttributes;
//    }
//
//    protected static SystemProperties buildSystemProperties(MessageExt messageExt) {
//        SystemProperties.Builder systemPropertiesBuilder = SystemProperties.newBuilder();
//
//        // tag
//        String tag = messageExt.getUserProperty(MessageConst.PROPERTY_TAGS);
//        if (tag != null) {
//            systemPropertiesBuilder.setTag(tag);
//        }
//
//        // keys
//        String keys = messageExt.getKeys();
//        if (keys != null) {
//            String[] keysArray = keys.split(MessageConst.KEY_SEPARATOR);
//            systemPropertiesBuilder.addAllKeys(Arrays.asList(keysArray));
//        }
//
//        // message_id
//        String uniqKey = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
//        if (uniqKey != null) {
//            systemPropertiesBuilder.setMessageId(uniqKey);
//        }
//
//        // body_digest & body_encoding
//        String md5Result = BinaryUtil.generateMd5(messageExt.getBody());
//        Digest digest = Digest.newBuilder()
//            .setType(DigestType.MD5)
//            .setChecksum(md5Result)
//            .build();
//        systemPropertiesBuilder.setBodyDigest(digest);
//
//        if ((messageExt.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
//            systemPropertiesBuilder.setBodyEncoding(Encoding.GZIP);
//        } else {
//            systemPropertiesBuilder.setBodyEncoding(Encoding.IDENTITY);
//        }
//
//        // message_type
//        String isTrans = messageExt.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
//        String isTransValue = "true";
//        if (isTransValue.equals(isTrans)) {
//            systemPropertiesBuilder.setMessageType(MessageType.TRANSACTION);
//        } else if (messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
//            || messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS) != null
//            || messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
//            systemPropertiesBuilder.setMessageType(MessageType.DELAY);
//        } else if (messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY) != null) {
//            systemPropertiesBuilder.setMessageType(MessageType.FIFO);
//        } else {
//            systemPropertiesBuilder.setMessageType(MessageType.NORMAL);
//        }
//
//        // born_timestamp (millis)
//        long bornTimestamp = messageExt.getBornTimestamp();
//        systemPropertiesBuilder.setBornTimestamp(Timestamps.fromMillis(bornTimestamp));
//
//        // born_host
//        systemPropertiesBuilder.setBornHost(messageExt.getBornHostString());
//
//        // store_timestamp (millis)
//        long storeTimestamp = messageExt.getStoreTimestamp();
//        systemPropertiesBuilder.setStoreTimestamp(Timestamps.fromMillis(storeTimestamp));
//
//        // store_host
//        SocketAddress storeHost = messageExt.getStoreHost();
//        if (storeHost != null) {
//            systemPropertiesBuilder.setStoreHost(storeHost.toString());
//        }
//
//        // delivery_timestamp
//        String deliverMsString;
//        long deliverMs;
//        if (messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
//            long delayMs = TimeUnit.SECONDS.toMillis(Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)));
//            deliverMs = System.currentTimeMillis() + delayMs;
//            systemPropertiesBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliverMs));
//        } else {
//            deliverMsString = messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
//            if (deliverMsString != null) {
//                deliverMs = Long.parseLong(deliverMsString);
//                systemPropertiesBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliverMs));
//            }
//        }
//
//        // sharding key
//        String shardingKey = messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
//        if (shardingKey != null) {
//            systemPropertiesBuilder.setMessageGroup(shardingKey);
//        }
//
//        // receipt_handle && invisible_period
//        ReceiptHandle receiptHandle = ReceiptHandle.create(messageExt);
//        if (receiptHandle != null) {
//            systemPropertiesBuilder.setReceiptHandle(receiptHandle.encode());
//        }
//
//        // partition_id
//        systemPropertiesBuilder.setQueueId(messageExt.getQueueId());
//
//        // partition_offset
//        systemPropertiesBuilder.setQueueOffset(messageExt.getQueueOffset());
//
//        // delivery_attempt
//        systemPropertiesBuilder.setDeliveryAttempt(messageExt.getReconsumeTimes() + 1);
//
//        // trace context
//        String traceContext = messageExt.getProperty(MessageConst.PROPERTY_TRACE_CONTEXT);
//        if (traceContext != null) {
//            systemPropertiesBuilder.setTraceContext(traceContext);
//        }
//
//        return systemPropertiesBuilder.build();
//    }
//
//    public static int buildTransactionCommitOrRollback(TransactionResolution type) {
//        switch (type) {
//            case COMMIT:
//                return MessageSysFlag.TRANSACTION_COMMIT_TYPE;
//            case ROLLBACK:
//                return MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
//            default:
//                return MessageSysFlag.TRANSACTION_NOT_TYPE;
//        }
//    }
//
//    public static ConsumeMessageDirectlyResult buildConsumeMessageDirectlyResult(VerifyMessageResult request) {
//        ConsumeMessageDirectlyResult consumeMessageDirectlyResult = new ConsumeMessageDirectlyResult();
//        switch (request.getStatus().getCode().getNumber()) {
//            case Code.OK_VALUE: {
//                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_SUCCESS);
//                break;
//            }
//            case Code.FAILED_TO_CONSUME_MESSAGE_VALUE: {
//                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_LATER);
//                break;
//            }
//            case Code.MESSAGE_CORRUPTED_VALUE: {
//                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_RETURN_NULL);
//                break;
//            }
//        }
//        consumeMessageDirectlyResult.setRemark("From gRPC client");
//        return consumeMessageDirectlyResult;
//    }
//
//    public static Resource buildResource(String resourceNameWithNamespace) {
//        return Resource.newBuilder()
//            .setResourceNamespace(NamespaceUtil.getNamespaceFromResource(resourceNameWithNamespace))
//            .setName(NamespaceUtil.withoutNamespace(resourceNameWithNamespace))
//            .build();
//    }
//
//    public static long buildPollTimeFromContext(Context ctx) {
//        long timeRemaining = ctx.getDeadline()
//            .timeRemaining(TimeUnit.MILLISECONDS);
//        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
//        if (pollTime <= 0) {
//            pollTime = timeRemaining;
//        }
//
//        return pollTime;
//    }
//
//    public static List<MessageQueue> genMessageQueueFromQueueData(QueueData queueData, Resource topic, Broker broker) {
//        List<MessageQueue> messageQueueList = new ArrayList<>();
//
//        int r = 0;
//        int w = 0;
//        int rw = 0;
//        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
//            rw = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
//            r = queueData.getReadQueueNums() - rw;
//            w = queueData.getWriteQueueNums() - rw;
//        } else if (PermName.isWriteable(queueData.getPerm())) {
//            w = queueData.getWriteQueueNums();
//        } else if (PermName.isReadable(queueData.getPerm())) {
//            r = queueData.getReadQueueNums();
//        }
//
//        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means both readable and writable queue nums.
//        int queueIdIndex = 0;
//        for (int i = 0; i < r; i++) {
//            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
//                .setId(queueIdIndex++)
//                .setPermission(Permission.READ)
//                .build();
//            messageQueueList.add(messageQueue);
//        }
//
//        for (int i = 0; i < w; i++) {
//            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
//                .setId(queueIdIndex++)
//                .setPermission(Permission.WRITE)
//                .build();
//            messageQueueList.add(messageQueue);
//        }
//
//        for (int i = 0; i < rw; i++) {
//            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
//                .setId(queueIdIndex++)
//                .setPermission(Permission.READ_WRITE)
//                .build();
//            messageQueueList.add(messageQueue);
//        }
//
//        return messageQueueList;
//    }
//
//}
