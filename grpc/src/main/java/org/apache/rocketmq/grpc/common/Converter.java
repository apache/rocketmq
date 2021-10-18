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

package org.apache.rocketmq.grpc.common;

import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.Digest;
import apache.rocketmq.v1.DigestType;
import apache.rocketmq.v1.Encoding;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SystemAttribute;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.grpc.exception.GrpcConvertException;

public class Converter {
    /**
     * Get resource name with namespace
     *
     * @param resource topic/group
     * @return
     */
    public static String getResourceNameWithNamespace(Resource resource) {
        return NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
    }

    public static List<Partition> generatePartitionList(QueueData queueData, Resource topic, Broker broker) {
        List<Partition> partitionList = new ArrayList<>();

        int readQueue = 0;
        int writeQueue = 0;
        int readAndWriteQueue = 0;
        int totalQueue = Math.max(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            readAndWriteQueue = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            readQueue = queueData.getReadQueueNums() - readAndWriteQueue;
            writeQueue = queueData.getWriteQueueNums() - readAndWriteQueue;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            writeQueue = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            readQueue = queueData.getReadQueueNums();
        }

        for (int i = 0; i < totalQueue; i++) {
            Partition.Builder builder = Partition.newBuilder()
                .setBroker(broker)
                .setTopic(topic)
                .setId(i);
            if (i < readQueue) {
                builder.setPermission(Permission.READ);
            } else if (i < writeQueue) {
                builder.setPermission(Permission.WRITE);
            } else {
                builder.setPermission(Permission.READ_WRITE);
            }
            partitionList.add(builder.build());
        }
        return partitionList;
    }

    public static SendMessageRequestHeader buildSendMessageRequestHeader(SendMessageRequest request) throws GrpcConvertException {
        Message message = request.getMessage();
        org.apache.rocketmq.common.message.Message remotingMessage = buildMessage(message);
        SystemAttribute systemAttribute = message.getSystemAttribute();
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
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
        requestHeader.setProperties(MessageDecoder.messageProperties2String(remotingMessage.getProperties()));
        requestHeader.setReconsumeTimes(systemAttribute.getDeliveryAttempt());

        return requestHeader;
    }

    public static Message buildMessage(MessageExt messageExt) {
        Map<String, String> userAttributes = buildUserAttributes(messageExt);
        SystemAttribute systemAttributes = buildSystemAttributes(messageExt);
        Resource topic = Resource.newBuilder()
            .setResourceNamespace(NamespaceUtil.getNamespaceFromResource(messageExt.getTopic()))
            .setName(NamespaceUtil.withoutNamespace(messageExt.getTopic()))
            .build();
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
        String ckInfo = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
        if (ckInfo != null) {
            systemAttributeBuilder.setReceiptHandle(ckInfo);
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
            String namespaceId = NamespaceUtil.getNamespaceFromResource(producerGroup);
            String group = NamespaceUtil.withoutNamespace(producerGroup);
            systemAttributeBuilder.setProducerGroup(Resource.newBuilder()
                .setResourceNamespace(namespaceId)
                .setName(group)
                .build());
        }

        // trace context
        String traceContext = messageExt.getProperty(MessageConst.PROPERTY_TRACE_CONTEXT);
        if (traceContext != null) {
            systemAttributeBuilder.setTraceContext(traceContext);
        }

        return systemAttributeBuilder.build();
    }

    public static org.apache.rocketmq.common.message.Message buildMessage(
        Message grpcMessage) throws GrpcConvertException {
        String topicName = getResourceNameWithNamespace(grpcMessage.getTopic());

        org.apache.rocketmq.common.message.Message message =
            new org.apache.rocketmq.common.message.Message(topicName, grpcMessage.getBody()
                .toByteArray());

        // set user properties
        Map<String, String> userProperties = grpcMessage.getUserAttributeMap();
        for (String key : userProperties.keySet()) {
            if (MessageConst.STRING_HASH_SET.contains(key)) {
                throw new GrpcConvertException("property is used by system: " + key);
            }
        }
        MessageAccessor.setProperties(message, Maps.newHashMap(userProperties));

        // set message id
        String messageId = grpcMessage.getSystemAttribute().getMessageId();
        if ("".equals(messageId)) {
            throw new GrpcConvertException("message id is empty");
        }
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);

        // set tag
        String tag = grpcMessage.getSystemAttribute().getTag();
        if (!"".equals(tag)) {
            message.setTags(tag);
        }

        // set keys
        List<String> keysList = grpcMessage.getSystemAttribute().getKeysList();
        if (keysList.size() > 0) {
            message.setKeys(keysList);
        }

        // set reconsume times
        int reconsumeTimes = grpcMessage.getSystemAttribute().getDeliveryAttempt();
        MessageAccessor.setReconsumeTime(message, String.valueOf(reconsumeTimes));

        // set transaction property
        MessageType messageType = grpcMessage.getSystemAttribute().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");

            Duration transactionResolveDelay = grpcMessage.getSystemAttribute().getOrphanedTransactionRecoveryPeriod();

            MessageAccessor.putProperty(message, MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(15));
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                String.valueOf(Durations.toSeconds(transactionResolveDelay)));
        }

        // set producer group
        Resource producerGroup = grpcMessage.getSystemAttribute().getProducerGroup();
        String producerGroupName = getResourceNameWithNamespace(producerGroup);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroupName);

        // set message group
        String messageGroup = grpcMessage.getSystemAttribute().getMessageGroup();
        if (!messageGroup.isEmpty()) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
        }

        // set delay level or deliver timestamp
        switch (grpcMessage.getSystemAttribute().getTimedDeliveryCase()) {
            case DELAY_LEVEL:
                int delayLevel = grpcMessage.getSystemAttribute().getDelayLevel();
                if (delayLevel > 0) {
                    MessageAccessor.putProperty(message, MessageConst.PROPERTY_DELAY_TIME_LEVEL,
                        String.valueOf(delayLevel));
                }
                break;
            case DELIVERY_TIMESTAMP:
                Timestamp deliveryTimestamp = grpcMessage.getSystemAttribute().getDeliveryTimestamp();
                String timestampString = String.valueOf(Timestamps.toMillis(deliveryTimestamp));
                MessageAccessor.putProperty(message, MessageConst.PROPERTY_TIMER_DELIVER_MS, timestampString);
                break;
            case TIMEDDELIVERY_NOT_SET:
                break;
            default:
                throw new IllegalStateException(
                    "Unexpected value: " + grpcMessage.getSystemAttribute().getTimedDeliveryCase());
        }

        // set trace context
        String traceContext = grpcMessage.getSystemAttribute().getTraceContext();
        if (!traceContext.isEmpty()) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);
        }

        return message;
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

    public static String buildExpressionType(FilterType filterType) {
        switch (filterType) {
            case SQL:
                return ExpressionType.SQL92;
            case TAG:
            default:
                return ExpressionType.TAG;
        }
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

    public static int buildVersion(String version) {
        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version is blank");
        }
        switch (version) {
            case "5.0.0-SNAPSHOT": {
                return MQVersion.Version.V5_0_0_SNAPSHOT.ordinal();
            }
            case "5.0.0": {
                return MQVersion.Version.V5_0_0.ordinal();
            }
            case "5.0.1-SNAPSHOT": {
                return MQVersion.Version.V5_0_1_SNAPSHOT.ordinal();
            }
            case "5.0.1": {
                return MQVersion.Version.V5_0_1.ordinal();
            }
            case "5.0.2-SNAPSHOT": {
                return MQVersion.Version.V5_0_2_SNAPSHOT.ordinal();
            }
            case "5.0.2": {
                return MQVersion.Version.V5_0_2.ordinal();
            }
            case "5.0.3-SNAPSHOT": {
                return MQVersion.Version.V5_0_3_SNAPSHOT.ordinal();
            }
            case "5.0.3": {
                return MQVersion.Version.V5_0_3.ordinal();
            }
            case "5.0.4-SNAPSHOT": {
                return MQVersion.Version.V5_0_4_SNAPSHOT.ordinal();
            }
            case "5.0.4": {
                return MQVersion.Version.V5_0_4.ordinal();
            }
            case "5.0.5-SNAPSHOT": {
                return MQVersion.Version.V5_0_5_SNAPSHOT.ordinal();
            }
            case "5.0.5": {
                return MQVersion.Version.V5_0_5.ordinal();
            }
            case "5.0.6-SNAPSHOT": {
                return MQVersion.Version.V5_0_6_SNAPSHOT.ordinal();
            }
            case "5.0.6": {
                return MQVersion.Version.V5_0_6.ordinal();
            }
            case "5.0.7-SNAPSHOT": {
                return MQVersion.Version.V5_0_7_SNAPSHOT.ordinal();
            }
            case "5.0.7": {
                return MQVersion.Version.V5_0_7.ordinal();
            }
            case "5.0.8-SNAPSHOT": {
                return MQVersion.Version.V5_0_8_SNAPSHOT.ordinal();
            }
            case "5.0.8": {
                return MQVersion.Version.V5_0_8.ordinal();
            }
            case "5.0.9-SNAPSHOT": {
                return MQVersion.Version.V5_0_9_SNAPSHOT.ordinal();
            }
            case "5.0.9": {
                return MQVersion.Version.V5_0_9.ordinal();
            }
            case "5.1.0-SNAPSHOT": {
                return MQVersion.Version.V5_1_0_SNAPSHOT.ordinal();
            }
            case "5.1.0": {
                return MQVersion.Version.V5_1_0.ordinal();
            }
            case "5.1.1-SNAPSHOT": {
                return MQVersion.Version.V5_1_1_SNAPSHOT.ordinal();
            }
            case "5.1.1": {
                return MQVersion.Version.V5_1_1.ordinal();
            }
            case "5.1.2-SNAPSHOT": {
                return MQVersion.Version.V5_1_2_SNAPSHOT.ordinal();
            }
            case "5.1.2": {
                return MQVersion.Version.V5_1_2.ordinal();
            }
            case "5.1.3-SNAPSHOT": {
                return MQVersion.Version.V5_1_3_SNAPSHOT.ordinal();
            }
            case "5.1.3": {
                return MQVersion.Version.V5_1_3.ordinal();
            }
            case "5.1.4-SNAPSHOT": {
                return MQVersion.Version.V5_1_4_SNAPSHOT.ordinal();
            }
            case "5.1.4": {
                return MQVersion.Version.V5_1_4.ordinal();
            }
            case "5.1.5-SNAPSHOT": {
                return MQVersion.Version.V5_1_5_SNAPSHOT.ordinal();
            }
            case "5.1.5": {
                return MQVersion.Version.V5_1_5.ordinal();
            }
            case "5.1.6-SNAPSHOT": {
                return MQVersion.Version.V5_1_6_SNAPSHOT.ordinal();
            }
            case "5.1.6": {
                return MQVersion.Version.V5_1_6.ordinal();
            }
            case "5.1.7-SNAPSHOT": {
                return MQVersion.Version.V5_1_7_SNAPSHOT.ordinal();
            }
            case "5.1.7": {
                return MQVersion.Version.V5_1_7.ordinal();
            }
            case "5.1.8-SNAPSHOT": {
                return MQVersion.Version.V5_1_8_SNAPSHOT.ordinal();
            }
            case "5.1.8": {
                return MQVersion.Version.V5_1_8.ordinal();
            }
            case "5.1.9-SNAPSHOT": {
                return MQVersion.Version.V5_1_9_SNAPSHOT.ordinal();
            }
            case "5.1.9": {
                return MQVersion.Version.V5_1_9.ordinal();
            }
            case "5.2.0-SNAPSHOT": {
                return MQVersion.Version.V5_2_0_SNAPSHOT.ordinal();
            }
            case "5.2.0": {
                return MQVersion.Version.V5_2_0.ordinal();
            }
            case "5.2.1-SNAPSHOT": {
                return MQVersion.Version.V5_2_1_SNAPSHOT.ordinal();
            }
            case "5.2.1": {
                return MQVersion.Version.V5_2_1.ordinal();
            }
            case "5.2.2-SNAPSHOT": {
                return MQVersion.Version.V5_2_2_SNAPSHOT.ordinal();
            }
            case "5.2.2": {
                return MQVersion.Version.V5_2_2.ordinal();
            }
            case "5.2.3-SNAPSHOT": {
                return MQVersion.Version.V5_2_3_SNAPSHOT.ordinal();
            }
            case "5.2.3": {
                return MQVersion.Version.V5_2_3.ordinal();
            }
            case "5.2.4-SNAPSHOT": {
                return MQVersion.Version.V5_2_4_SNAPSHOT.ordinal();
            }
            case "5.2.4": {
                return MQVersion.Version.V5_2_4.ordinal();
            }
            case "5.2.5-SNAPSHOT": {
                return MQVersion.Version.V5_2_5_SNAPSHOT.ordinal();
            }
            case "5.2.5": {
                return MQVersion.Version.V5_2_5.ordinal();
            }
            case "5.2.6-SNAPSHOT": {
                return MQVersion.Version.V5_2_6_SNAPSHOT.ordinal();
            }
            case "5.2.6": {
                return MQVersion.Version.V5_2_6.ordinal();
            }
            case "5.2.7-SNAPSHOT": {
                return MQVersion.Version.V5_2_7_SNAPSHOT.ordinal();
            }
            case "5.2.7": {
                return MQVersion.Version.V5_2_7.ordinal();
            }
            case "5.2.8-SNAPSHOT": {
                return MQVersion.Version.V5_2_8_SNAPSHOT.ordinal();
            }
            case "5.2.8": {
                return MQVersion.Version.V5_2_8.ordinal();
            }
            case "5.2.9-SNAPSHOT": {
                return MQVersion.Version.V5_2_9_SNAPSHOT.ordinal();
            }
            case "5.2.9": {
                return MQVersion.Version.V5_2_9.ordinal();
            }
            case "5.3.0-SNAPSHOT": {
                return MQVersion.Version.V5_3_0_SNAPSHOT.ordinal();
            }
            case "5.3.0": {
                return MQVersion.Version.V5_3_0.ordinal();
            }
            case "5.3.1-SNAPSHOT": {
                return MQVersion.Version.V5_3_1_SNAPSHOT.ordinal();
            }
            case "5.3.1": {
                return MQVersion.Version.V5_3_1.ordinal();
            }
            case "5.3.2-SNAPSHOT": {
                return MQVersion.Version.V5_3_2_SNAPSHOT.ordinal();
            }
            case "5.3.2": {
                return MQVersion.Version.V5_3_2.ordinal();
            }
            case "5.3.3-SNAPSHOT": {
                return MQVersion.Version.V5_3_3_SNAPSHOT.ordinal();
            }
            case "5.3.3": {
                return MQVersion.Version.V5_3_3.ordinal();
            }
            case "5.3.4-SNAPSHOT": {
                return MQVersion.Version.V5_3_4_SNAPSHOT.ordinal();
            }
            case "5.3.4": {
                return MQVersion.Version.V5_3_4.ordinal();
            }
            case "5.3.5-SNAPSHOT": {
                return MQVersion.Version.V5_3_5_SNAPSHOT.ordinal();
            }
            case "5.3.5": {
                return MQVersion.Version.V5_3_5.ordinal();
            }
            case "5.3.6-SNAPSHOT": {
                return MQVersion.Version.V5_3_6_SNAPSHOT.ordinal();
            }
            case "5.3.6": {
                return MQVersion.Version.V5_3_6.ordinal();
            }
            case "5.3.7-SNAPSHOT": {
                return MQVersion.Version.V5_3_7_SNAPSHOT.ordinal();
            }
            case "5.3.7": {
                return MQVersion.Version.V5_3_7.ordinal();
            }
            case "5.3.8-SNAPSHOT": {
                return MQVersion.Version.V5_3_8_SNAPSHOT.ordinal();
            }
            case "5.3.8": {
                return MQVersion.Version.V5_3_8.ordinal();
            }
            case "5.3.9-SNAPSHOT": {
                return MQVersion.Version.V5_3_9_SNAPSHOT.ordinal();
            }
            case "5.3.9": {
                return MQVersion.Version.V5_3_9.ordinal();
            }
            case "5.4.0-SNAPSHOT": {
                return MQVersion.Version.V5_4_0_SNAPSHOT.ordinal();
            }
            case "5.4.0": {
                return MQVersion.Version.V5_4_0.ordinal();
            }
            case "5.4.1-SNAPSHOT": {
                return MQVersion.Version.V5_4_1_SNAPSHOT.ordinal();
            }
            case "5.4.1": {
                return MQVersion.Version.V5_4_1.ordinal();
            }
            case "5.4.2-SNAPSHOT": {
                return MQVersion.Version.V5_4_2_SNAPSHOT.ordinal();
            }
            case "5.4.2": {
                return MQVersion.Version.V5_4_2.ordinal();
            }
            case "5.4.3-SNAPSHOT": {
                return MQVersion.Version.V5_4_3_SNAPSHOT.ordinal();
            }
            case "5.4.3": {
                return MQVersion.Version.V5_4_3.ordinal();
            }
            case "5.4.4-SNAPSHOT": {
                return MQVersion.Version.V5_4_4_SNAPSHOT.ordinal();
            }
            case "5.4.4": {
                return MQVersion.Version.V5_4_4.ordinal();
            }
            case "5.4.5-SNAPSHOT": {
                return MQVersion.Version.V5_4_5_SNAPSHOT.ordinal();
            }
            case "5.4.5": {
                return MQVersion.Version.V5_4_5.ordinal();
            }
            case "5.4.6-SNAPSHOT": {
                return MQVersion.Version.V5_4_6_SNAPSHOT.ordinal();
            }
            case "5.4.6": {
                return MQVersion.Version.V5_4_6.ordinal();
            }
            case "5.4.7-SNAPSHOT": {
                return MQVersion.Version.V5_4_7_SNAPSHOT.ordinal();
            }
            case "5.4.7": {
                return MQVersion.Version.V5_4_7.ordinal();
            }
            case "5.4.8-SNAPSHOT": {
                return MQVersion.Version.V5_4_8_SNAPSHOT.ordinal();
            }
            case "5.4.8": {
                return MQVersion.Version.V5_4_8.ordinal();
            }
            case "5.4.9-SNAPSHOT": {
                return MQVersion.Version.V5_4_9_SNAPSHOT.ordinal();
            }
            case "5.4.9": {
                return MQVersion.Version.V5_4_9.ordinal();
            }
            case "5.5.0-SNAPSHOT": {
                return MQVersion.Version.V5_5_0_SNAPSHOT.ordinal();
            }
            case "5.5.0": {
                return MQVersion.Version.V5_5_0.ordinal();
            }
            case "5.5.1-SNAPSHOT": {
                return MQVersion.Version.V5_5_1_SNAPSHOT.ordinal();
            }
            case "5.5.1": {
                return MQVersion.Version.V5_5_1.ordinal();
            }
            case "5.5.2-SNAPSHOT": {
                return MQVersion.Version.V5_5_2_SNAPSHOT.ordinal();
            }
            case "5.5.2": {
                return MQVersion.Version.V5_5_2.ordinal();
            }
            case "5.5.3-SNAPSHOT": {
                return MQVersion.Version.V5_5_3_SNAPSHOT.ordinal();
            }
            case "5.5.3": {
                return MQVersion.Version.V5_5_3.ordinal();
            }
            case "5.5.4-SNAPSHOT": {
                return MQVersion.Version.V5_5_4_SNAPSHOT.ordinal();
            }
            case "5.5.4": {
                return MQVersion.Version.V5_5_4.ordinal();
            }
            case "5.5.5-SNAPSHOT": {
                return MQVersion.Version.V5_5_5_SNAPSHOT.ordinal();
            }
            case "5.5.5": {
                return MQVersion.Version.V5_5_5.ordinal();
            }
            case "5.5.6-SNAPSHOT": {
                return MQVersion.Version.V5_5_6_SNAPSHOT.ordinal();
            }
            case "5.5.6": {
                return MQVersion.Version.V5_5_6.ordinal();
            }
            case "5.5.7-SNAPSHOT": {
                return MQVersion.Version.V5_5_7_SNAPSHOT.ordinal();
            }
            case "5.5.7": {
                return MQVersion.Version.V5_5_7.ordinal();
            }
            case "5.5.8-SNAPSHOT": {
                return MQVersion.Version.V5_5_8_SNAPSHOT.ordinal();
            }
            case "5.5.8": {
                return MQVersion.Version.V5_5_8.ordinal();
            }
            case "5.5.9-SNAPSHOT": {
                return MQVersion.Version.V5_5_9_SNAPSHOT.ordinal();
            }
            case "5.5.9": {
                return MQVersion.Version.V5_5_9.ordinal();
            }
            case "5.6.0-SNAPSHOT": {
                return MQVersion.Version.V5_6_0_SNAPSHOT.ordinal();
            }
            case "5.6.0": {
                return MQVersion.Version.V5_6_0.ordinal();
            }
            case "5.6.1-SNAPSHOT": {
                return MQVersion.Version.V5_6_1_SNAPSHOT.ordinal();
            }
            case "5.6.1": {
                return MQVersion.Version.V5_6_1.ordinal();
            }
            case "5.6.2-SNAPSHOT": {
                return MQVersion.Version.V5_6_2_SNAPSHOT.ordinal();
            }
            case "5.6.2": {
                return MQVersion.Version.V5_6_2.ordinal();
            }
            case "5.6.3-SNAPSHOT": {
                return MQVersion.Version.V5_6_3_SNAPSHOT.ordinal();
            }
            case "5.6.3": {
                return MQVersion.Version.V5_6_3.ordinal();
            }
            case "5.6.4-SNAPSHOT": {
                return MQVersion.Version.V5_6_4_SNAPSHOT.ordinal();
            }
            case "5.6.4": {
                return MQVersion.Version.V5_6_4.ordinal();
            }
            case "5.6.5-SNAPSHOT": {
                return MQVersion.Version.V5_6_5_SNAPSHOT.ordinal();
            }
            case "5.6.5": {
                return MQVersion.Version.V5_6_5.ordinal();
            }
            case "5.6.6-SNAPSHOT": {
                return MQVersion.Version.V5_6_6_SNAPSHOT.ordinal();
            }
            case "5.6.6": {
                return MQVersion.Version.V5_6_6.ordinal();
            }
            case "5.6.7-SNAPSHOT": {
                return MQVersion.Version.V5_6_7_SNAPSHOT.ordinal();
            }
            case "5.6.7": {
                return MQVersion.Version.V5_6_7.ordinal();
            }
            case "5.6.8-SNAPSHOT": {
                return MQVersion.Version.V5_6_8_SNAPSHOT.ordinal();
            }
            case "5.6.8": {
                return MQVersion.Version.V5_6_8.ordinal();
            }
            case "5.6.9-SNAPSHOT": {
                return MQVersion.Version.V5_6_9_SNAPSHOT.ordinal();
            }
            case "5.6.9": {
                return MQVersion.Version.V5_6_9.ordinal();
            }
            case "5.7.0-SNAPSHOT": {
                return MQVersion.Version.V5_7_0_SNAPSHOT.ordinal();
            }
            case "5.7.0": {
                return MQVersion.Version.V5_7_0.ordinal();
            }
            case "5.7.1-SNAPSHOT": {
                return MQVersion.Version.V5_7_1_SNAPSHOT.ordinal();
            }
            case "5.7.1": {
                return MQVersion.Version.V5_7_1.ordinal();
            }
            case "5.7.2-SNAPSHOT": {
                return MQVersion.Version.V5_7_2_SNAPSHOT.ordinal();
            }
            case "5.7.2": {
                return MQVersion.Version.V5_7_2.ordinal();
            }
            case "5.7.3-SNAPSHOT": {
                return MQVersion.Version.V5_7_3_SNAPSHOT.ordinal();
            }
            case "5.7.3": {
                return MQVersion.Version.V5_7_3.ordinal();
            }
            case "5.7.4-SNAPSHOT": {
                return MQVersion.Version.V5_7_4_SNAPSHOT.ordinal();
            }
            case "5.7.4": {
                return MQVersion.Version.V5_7_4.ordinal();
            }
            case "5.7.5-SNAPSHOT": {
                return MQVersion.Version.V5_7_5_SNAPSHOT.ordinal();
            }
            case "5.7.5": {
                return MQVersion.Version.V5_7_5.ordinal();
            }
            case "5.7.6-SNAPSHOT": {
                return MQVersion.Version.V5_7_6_SNAPSHOT.ordinal();
            }
            case "5.7.6": {
                return MQVersion.Version.V5_7_6.ordinal();
            }
            case "5.7.7-SNAPSHOT": {
                return MQVersion.Version.V5_7_7_SNAPSHOT.ordinal();
            }
            case "5.7.7": {
                return MQVersion.Version.V5_7_7.ordinal();
            }
            case "5.7.8-SNAPSHOT": {
                return MQVersion.Version.V5_7_8_SNAPSHOT.ordinal();
            }
            case "5.7.8": {
                return MQVersion.Version.V5_7_8.ordinal();
            }
            case "5.7.9-SNAPSHOT": {
                return MQVersion.Version.V5_7_9_SNAPSHOT.ordinal();
            }
            case "5.7.9": {
                return MQVersion.Version.V5_7_9.ordinal();
            }
            case "5.8.0-SNAPSHOT": {
                return MQVersion.Version.V5_8_0_SNAPSHOT.ordinal();
            }
            case "5.8.0": {
                return MQVersion.Version.V5_8_0.ordinal();
            }
            case "5.8.1-SNAPSHOT": {
                return MQVersion.Version.V5_8_1_SNAPSHOT.ordinal();
            }
            case "5.8.1": {
                return MQVersion.Version.V5_8_1.ordinal();
            }
            case "5.8.2-SNAPSHOT": {
                return MQVersion.Version.V5_8_2_SNAPSHOT.ordinal();
            }
            case "5.8.2": {
                return MQVersion.Version.V5_8_2.ordinal();
            }
            case "5.8.3-SNAPSHOT": {
                return MQVersion.Version.V5_8_3_SNAPSHOT.ordinal();
            }
            case "5.8.3": {
                return MQVersion.Version.V5_8_3.ordinal();
            }
            case "5.8.4-SNAPSHOT": {
                return MQVersion.Version.V5_8_4_SNAPSHOT.ordinal();
            }
            case "5.8.4": {
                return MQVersion.Version.V5_8_4.ordinal();
            }
            case "5.8.5-SNAPSHOT": {
                return MQVersion.Version.V5_8_5_SNAPSHOT.ordinal();
            }
            case "5.8.5": {
                return MQVersion.Version.V5_8_5.ordinal();
            }
            case "5.8.6-SNAPSHOT": {
                return MQVersion.Version.V5_8_6_SNAPSHOT.ordinal();
            }
            case "5.8.6": {
                return MQVersion.Version.V5_8_6.ordinal();
            }
            case "5.8.7-SNAPSHOT": {
                return MQVersion.Version.V5_8_7_SNAPSHOT.ordinal();
            }
            case "5.8.7": {
                return MQVersion.Version.V5_8_7.ordinal();
            }
            case "5.8.8-SNAPSHOT": {
                return MQVersion.Version.V5_8_8_SNAPSHOT.ordinal();
            }
            case "5.8.8": {
                return MQVersion.Version.V5_8_8.ordinal();
            }
            case "5.8.9-SNAPSHOT": {
                return MQVersion.Version.V5_8_9_SNAPSHOT.ordinal();
            }
            case "5.8.9": {
                return MQVersion.Version.V5_8_9.ordinal();
            }
            case "5.9.0-SNAPSHOT": {
                return MQVersion.Version.V5_9_0_SNAPSHOT.ordinal();
            }
            case "5.9.0": {
                return MQVersion.Version.V5_9_0.ordinal();
            }
            case "5.9.1-SNAPSHOT": {
                return MQVersion.Version.V5_9_1_SNAPSHOT.ordinal();
            }
            case "5.9.1": {
                return MQVersion.Version.V5_9_1.ordinal();
            }
            case "5.9.2-SNAPSHOT": {
                return MQVersion.Version.V5_9_2_SNAPSHOT.ordinal();
            }
            case "5.9.2": {
                return MQVersion.Version.V5_9_2.ordinal();
            }
            case "5.9.3-SNAPSHOT": {
                return MQVersion.Version.V5_9_3_SNAPSHOT.ordinal();
            }
            case "5.9.3": {
                return MQVersion.Version.V5_9_3.ordinal();
            }
            case "5.9.4-SNAPSHOT": {
                return MQVersion.Version.V5_9_4_SNAPSHOT.ordinal();
            }
            case "5.9.4": {
                return MQVersion.Version.V5_9_4.ordinal();
            }
            case "5.9.5-SNAPSHOT": {
                return MQVersion.Version.V5_9_5_SNAPSHOT.ordinal();
            }
            case "5.9.5": {
                return MQVersion.Version.V5_9_5.ordinal();
            }
            case "5.9.6-SNAPSHOT": {
                return MQVersion.Version.V5_9_6_SNAPSHOT.ordinal();
            }
            case "5.9.6": {
                return MQVersion.Version.V5_9_6.ordinal();
            }
            case "5.9.7-SNAPSHOT": {
                return MQVersion.Version.V5_9_7_SNAPSHOT.ordinal();
            }
            case "5.9.7": {
                return MQVersion.Version.V5_9_7.ordinal();
            }
            case "5.9.8-SNAPSHOT": {
                return MQVersion.Version.V5_9_8_SNAPSHOT.ordinal();
            }
            case "5.9.8": {
                return MQVersion.Version.V5_9_8.ordinal();
            }
            case "5.9.9-SNAPSHOT": {
                return MQVersion.Version.V5_9_9_SNAPSHOT.ordinal();
            }
            case "5.9.9": {
                return MQVersion.Version.V5_9_9.ordinal();
            }
            default: {
                if (Integer.parseInt(Character.toString(version.charAt(0))) < 5) {
                    return 0;
                }
                return MQVersion.Version.HIGHER_VERSION.ordinal();
            }
        }
    }
}
