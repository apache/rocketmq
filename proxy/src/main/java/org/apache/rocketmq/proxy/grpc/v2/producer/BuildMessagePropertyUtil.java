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
package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessageType;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;

public class BuildMessagePropertyUtil {

    private BuildMessagePropertyUtil() {
    }

    public static void validateMessageBodySize(ByteString body) {
        if (ConfigurationManager.getProxyConfig().isEnableMessageBodyEmptyCheck()) {
            if (body.isEmpty()) {
                throw new GrpcProxyException(Code.MESSAGE_BODY_EMPTY, "message body cannot be empty");
            }
        }
        int max = ConfigurationManager.getProxyConfig().getMaxMessageSize();
        if (max <= 0) {
            return;
        }
        if (body.size() > max) {
            throw new GrpcProxyException(Code.MESSAGE_BODY_TOO_LARGE, "message body cannot exceed the max " + max);
        }
    }

    public static void validateMessageKey(String key) {
        if (StringUtils.isNotEmpty(key)) {
            if (StringUtils.isBlank(key)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_KEY, "key cannot be the char sequence of whitespace");
            }
            if (GrpcValidator.getInstance().containControlCharacter(key)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_KEY, "key cannot contain control character");
            }
        }
    }

    public static void validateMessageGroup(String messageGroup) {
        if (StringUtils.isNotEmpty(messageGroup)) {
            if (StringUtils.isBlank(messageGroup)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_GROUP, "message group cannot be the char sequence of whitespace");
            }
            int maxSize = ConfigurationManager.getProxyConfig().getMaxMessageGroupSize();
            if (maxSize <= 0) {
                return;
            }
            if (messageGroup.getBytes(StandardCharsets.UTF_8).length >= maxSize) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_GROUP, "message group exceed the max size " + maxSize);
            }
            if (GrpcValidator.getInstance().containControlCharacter(messageGroup)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_GROUP, "message group cannot contain control character");
            }
        }
    }

    public static void validateDelayTime(long deliveryTimestampMs) {
        long maxDelay = ConfigurationManager.getProxyConfig().getMaxDelayTimeMills();
        if (maxDelay <= 0) {
            return;
        }
        if (deliveryTimestampMs - System.currentTimeMillis() > maxDelay) {
            throw new GrpcProxyException(Code.ILLEGAL_DELIVERY_TIME, "the max delay time of message is too large, max is " + maxDelay);
        }
    }

    public static void validateTransactionRecoverySecond(long transactionRecoverySecond) {
        long maxTransactionRecoverySecond = ConfigurationManager.getProxyConfig().getMaxTransactionRecoverySecond();
        if (maxTransactionRecoverySecond <= 0) {
            return;
        }
        if (transactionRecoverySecond > maxTransactionRecoverySecond) {
            throw new GrpcProxyException(Code.BAD_REQUEST, "the max transaction recovery time of message is too large, max is " + maxTransactionRecoverySecond);
        }
    }

    public static void fillDelayMessageProperty(apache.rocketmq.v2.Message message,
        org.apache.rocketmq.common.message.Message messageWithHeader) {
        if (message.getSystemProperties().hasDeliveryTimestamp()) {
            Timestamp deliveryTimestamp = message.getSystemProperties().getDeliveryTimestamp();
            long deliveryTimestampMs = Timestamps.toMillis(deliveryTimestamp);
            validateDelayTime(deliveryTimestampMs);

            ProxyConfig config = ConfigurationManager.getProxyConfig();
            if (config.isUseDelayLevel()) {
                int delayLevel = config.computeDelayLevel(deliveryTimestampMs);
                MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(delayLevel));
            }

            String timestampString = String.valueOf(deliveryTimestampMs);
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TIMER_DELIVER_MS, timestampString);
        }
    }

    public static Map<String, String> buildMessageProperty(ProxyContext context, apache.rocketmq.v2.Message message,
        String producerGroup) {
        long userPropertySize = 0;
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        org.apache.rocketmq.common.message.Message messageWithHeader = new org.apache.rocketmq.common.message.Message();
        // set user properties
        Map<String, String> userProperties = message.getUserPropertiesMap();
        if (userProperties.size() > config.getUserPropertyMaxNum()) {
            throw new GrpcProxyException(Code.MESSAGE_PROPERTIES_TOO_LARGE, "too many user properties, max is " + config.getUserPropertyMaxNum());
        }
        for (Map.Entry<String, String> userPropertiesEntry : userProperties.entrySet()) {
            if (MessageConst.STRING_HASH_SET.contains(userPropertiesEntry.getKey())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "property is used by system: " + userPropertiesEntry.getKey());
            }
            if (GrpcValidator.getInstance().containControlCharacter(userPropertiesEntry.getKey())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "the key of property cannot contain control character");
            }
            if (GrpcValidator.getInstance().containControlCharacter(userPropertiesEntry.getValue())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "the value of property cannot contain control character");
            }
            userPropertySize += userPropertiesEntry.getKey().getBytes(StandardCharsets.UTF_8).length;
            userPropertySize += userPropertiesEntry.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        MessageAccessor.setProperties(messageWithHeader, Maps.newHashMap(userProperties));

        // set tag
        String tag = message.getSystemProperties().getTag();
        GrpcValidator.getInstance().validateTag(tag);
        messageWithHeader.setTags(tag);
        userPropertySize += tag.getBytes(StandardCharsets.UTF_8).length;

        // set keys
        List<String> keysList = message.getSystemProperties().getKeysList();
        for (String key : keysList) {
            validateMessageKey(key);
            userPropertySize += key.getBytes(StandardCharsets.UTF_8).length;
        }
        if (keysList.size() > 0) {
            messageWithHeader.setKeys(keysList);
        }

        if (userPropertySize > config.getMaxUserPropertySize()) {
            throw new GrpcProxyException(Code.MESSAGE_PROPERTIES_TOO_LARGE, "the total size of user property is too large, max is " + config.getMaxUserPropertySize());
        }

        // set message id
        String messageId = message.getSystemProperties().getMessageId();
        if (StringUtils.isBlank(messageId)) {
            throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_ID, "message id cannot be empty");
        }
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);

        // set transaction property
        MessageType messageType = message.getSystemProperties().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");

            if (message.getSystemProperties().hasOrphanedTransactionRecoveryDuration()) {
                long transactionRecoverySecond = Durations.toSeconds(message.getSystemProperties().getOrphanedTransactionRecoveryDuration());
                validateTransactionRecoverySecond(transactionRecoverySecond);
                MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                    String.valueOf(transactionRecoverySecond));
            }
        }

        // set delay level or deliver timestamp
        fillDelayMessageProperty(message, messageWithHeader);

        // set priority
        if (message.getSystemProperties().hasPriority()) {
            int priority = message.getSystemProperties().getPriority();
            messageWithHeader.setPriority(priority);
        }

        // set reconsume times
        int reconsumeTimes = message.getSystemProperties().getDeliveryAttempt();
        MessageAccessor.setReconsumeTime(messageWithHeader, String.valueOf(reconsumeTimes));
        // set producer group
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
        // set message group
        String messageGroup = message.getSystemProperties().getMessageGroup();
        if (StringUtils.isNotEmpty(messageGroup)) {
            validateMessageGroup(messageGroup);
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
        }
        // set lite topic
        String liteTopic = message.getSystemProperties().getLiteTopic();
        if (StringUtils.isNotEmpty(liteTopic)) {
            GrpcValidator.getInstance().validateLiteTopic(liteTopic);
            MessageAccessor.setLiteTopic(messageWithHeader, liteTopic);
        }

        // set trace context
        String traceContext = message.getSystemProperties().getTraceContext();
        if (!traceContext.isEmpty()) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);
        }

        String bornHost = message.getSystemProperties().getBornHost();
        if (StringUtils.isBlank(bornHost)) {
            bornHost = context.getRemoteAddress();
        }
        if (StringUtils.isNotBlank(bornHost)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_BORN_HOST, bornHost);
        }

        Timestamp bornTimestamp = message.getSystemProperties().getBornTimestamp();
        if (Timestamps.isValid(bornTimestamp)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_BORN_TIMESTAMP, String.valueOf(Timestamps.toMillis(bornTimestamp)));
        }

        return messageWithHeader.getProperties();
    }
}
