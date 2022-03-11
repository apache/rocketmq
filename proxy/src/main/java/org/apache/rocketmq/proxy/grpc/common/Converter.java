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

import apache.rocketmq.v1.Encoding;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SystemAttribute;
import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

public class Converter {
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
}
