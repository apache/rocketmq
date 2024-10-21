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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MultiDispatch;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class MultiDispatchUtils {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static String lmqQueueKey(String queueName) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = 0;
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    public static boolean isNeedHandleMultiDispatch(MessageStoreConfig messageStoreConfig, String topic) {
        return messageStoreConfig.isEnableMultiDispatch()
            && !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)
            && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }

    public static boolean checkMultiDispatchQueue(MessageStoreConfig messageStoreConfig, DispatchRequest dispatchRequest) {
        if (!isNeedHandleMultiDispatch(messageStoreConfig, dispatchRequest.getTopic())) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    public static AppendMessageResult handlePropertiesForLmqMsg(
            ByteBuffer preEncodeBuffer, final MessageExtBrokerInner msgInner, MultiDispatch multiDispatch, MessageStoreConfig messageStoreConfig) {
        if (msgInner.isEncodeCompleted()) {
            return null;
        }

        int crc32ReservedLength = messageStoreConfig.isEnabledAppendPropCRC() ? CommitLog.CRC32_RESERVED_LEN : 0;

        multiDispatch.wrapMultiDispatch(msgInner);

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

        boolean needAppendLastPropertySeparator = messageStoreConfig.isEnabledAppendPropCRC() && propertiesData != null && propertiesData.length > 0
                && propertiesData[propertiesData.length - 1] != MessageDecoder.PROPERTY_SEPARATOR;

        final int propertiesLength =
            (propertiesData == null ? 0 : propertiesData.length) + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;

        if (propertiesLength > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long. length={}", propertiesData.length);
            return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
        }

        int msgLenWithoutProperties = preEncodeBuffer.getInt(0);

        int msgLen = msgLenWithoutProperties + 2 + propertiesLength;

        // Exceeds the maximum message
        if (msgLen > messageStoreConfig.getMaxMessageSize()) {
            log.warn("message size exceeded, msg total size: " + msgLen + ", maxMessageSize: " + messageStoreConfig.getMaxMessageSize());
            return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
        }

        // Back filling total message length
        preEncodeBuffer.putInt(0, msgLen);
        // Modify position to msgLenWithoutProperties
        preEncodeBuffer.position(msgLenWithoutProperties);

        preEncodeBuffer.putShort((short) propertiesLength);

        if (propertiesLength > crc32ReservedLength) {
            preEncodeBuffer.put(propertiesData);
        }

        if (needAppendLastPropertySeparator) {
            preEncodeBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
        }
        // 18 CRC32
        preEncodeBuffer.position(preEncodeBuffer.position() + crc32ReservedLength);

        msgInner.setEncodeCompleted(true);

        return null;
    }
}
