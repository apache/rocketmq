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

    private static Map<String, String> buildUserAttributes(MessageExt messageExt) {
        Map<String, String> userAttributes = new HashMap<>();
        Map<String, String> properties = messageExt.getProperties();

        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (!MessageConst.STRING_HASH_SET.contains(property.getKey())) {
                userAttributes.put(property.getKey(), property.getValue());
            }
        }

        return userAttributes;
    }

    private static SystemAttribute buildSystemAttributes(MessageExt messageExt) {
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
            || messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
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
            default: {
                return MQVersion.Version.HIGHER_VERSION.ordinal();
            }
        }
    }
}
