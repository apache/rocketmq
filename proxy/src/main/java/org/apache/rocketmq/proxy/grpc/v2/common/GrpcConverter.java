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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.DeadLetterQueue;
import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

public class GrpcConverter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    protected static volatile GrpcConverter instance;

    public static GrpcConverter getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new GrpcConverter();
                }
            }
        }
        return instance;
    }

    public String wrapResourceWithNamespace(Resource resource) {
        return NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
    }

    public MessageQueue buildMessageQueue(MessageExt messageExt, String brokerName) {
        Broker broker = Broker.getDefaultInstance();
        if (!StringUtils.isEmpty(brokerName)) {
            broker = Broker.newBuilder()
                .setName(brokerName)
                .setId(0)
                .build();
        }
        return MessageQueue.newBuilder()
            .setId(messageExt.getQueueId())
            .setTopic(Resource.newBuilder()
                .setName(NamespaceUtil.withoutNamespace(messageExt.getTopic()))
                .setResourceNamespace(NamespaceUtil.getNamespaceFromResource(messageExt.getTopic()))
                .build())
            .setBroker(broker)
            .build();
    }

    public String buildExpressionType(FilterType filterType) {
        switch (filterType) {
            case SQL:
                return ExpressionType.SQL92;
            case TAG:
            default:
                return ExpressionType.TAG;
        }
    }

    public Message buildMessage(MessageExt messageExt) {
        Map<String, String> userProperties = buildUserAttributes(messageExt);
        SystemProperties systemProperties = buildSystemProperties(messageExt);
        Resource topic = buildResource(messageExt.getTopic());

        return Message.newBuilder()
            .setTopic(topic)
            .putAllUserProperties(userProperties)
            .setSystemProperties(systemProperties)
            .setBody(ByteString.copyFrom(messageExt.getBody()))
            .build();
    }

    protected Map<String, String> buildUserAttributes(MessageExt messageExt) {
        Map<String, String> userAttributes = new HashMap<>();
        Map<String, String> properties = messageExt.getProperties();

        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (!MessageConst.STRING_HASH_SET.contains(property.getKey())) {
                userAttributes.put(property.getKey(), property.getValue());
            }
        }

        return userAttributes;
    }

    protected SystemProperties buildSystemProperties(MessageExt messageExt) {
        SystemProperties.Builder systemPropertiesBuilder = SystemProperties.newBuilder();

        // tag
        String tag = messageExt.getUserProperty(MessageConst.PROPERTY_TAGS);
        if (tag != null) {
            systemPropertiesBuilder.setTag(tag);
        }

        // keys
        String keys = messageExt.getKeys();
        if (keys != null) {
            String[] keysArray = keys.split(MessageConst.KEY_SEPARATOR);
            systemPropertiesBuilder.addAllKeys(Arrays.asList(keysArray));
        }

        // message_id
        String uniqKey = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

        if (uniqKey == null) {
            uniqKey = messageExt.getMsgId();
        }

        if (uniqKey != null) {
            systemPropertiesBuilder.setMessageId(uniqKey);
        }

        // body_digest & body_encoding
        String md5Result = BinaryUtil.generateMd5(messageExt.getBody());
        Digest digest = Digest.newBuilder()
            .setType(DigestType.MD5)
            .setChecksum(md5Result)
            .build();
        systemPropertiesBuilder.setBodyDigest(digest);

        if ((messageExt.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
            systemPropertiesBuilder.setBodyEncoding(Encoding.GZIP);
        } else {
            systemPropertiesBuilder.setBodyEncoding(Encoding.IDENTITY);
        }

        // message_type
        String isTrans = messageExt.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        String isTransValue = "true";
        if (isTransValue.equals(isTrans)) {
            systemPropertiesBuilder.setMessageType(MessageType.TRANSACTION);
        } else if (messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
            || messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS) != null
            || messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
            systemPropertiesBuilder.setMessageType(MessageType.DELAY);
        } else if (messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY) != null) {
            systemPropertiesBuilder.setMessageType(MessageType.FIFO);
        } else {
            systemPropertiesBuilder.setMessageType(MessageType.NORMAL);
        }

        // born_timestamp (millis)
        long bornTimestamp = messageExt.getBornTimestamp();
        systemPropertiesBuilder.setBornTimestamp(Timestamps.fromMillis(bornTimestamp));

        // born_host
        String bornHostString = messageExt.getProperty(MessageConst.PROPERTY_BORN_HOST);
        if (StringUtils.isBlank(bornHostString)) {
            bornHostString = messageExt.getBornHostString();
        }
        if (StringUtils.isNotBlank(bornHostString)) {
            systemPropertiesBuilder.setBornHost(bornHostString);
        }

        // store_timestamp (millis)
        long storeTimestamp = messageExt.getStoreTimestamp();
        systemPropertiesBuilder.setStoreTimestamp(Timestamps.fromMillis(storeTimestamp));

        // store_host
        SocketAddress storeHost = messageExt.getStoreHost();
        if (storeHost != null) {
            systemPropertiesBuilder.setStoreHost(NetworkUtil.socketAddress2String(storeHost));
        }

        // delivery_timestamp
        String deliverMsString;
        long deliverMs;
        if (messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
            long delayMs = TimeUnit.SECONDS.toMillis(Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)));
            deliverMs = System.currentTimeMillis() + delayMs;
            systemPropertiesBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliverMs));
        } else {
            deliverMsString = messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
            if (deliverMsString != null) {
                deliverMs = Long.parseLong(deliverMsString);
                systemPropertiesBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliverMs));
            }
        }

        // sharding key
        String shardingKey = messageExt.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
        if (shardingKey != null) {
            systemPropertiesBuilder.setMessageGroup(shardingKey);
        }

        // receipt_handle && invisible_period
        String handle = messageExt.getProperty(MessageConst.PROPERTY_POP_CK);
        if (handle != null) {
            systemPropertiesBuilder.setReceiptHandle(handle);
        }

        // partition_id
        systemPropertiesBuilder.setQueueId(messageExt.getQueueId());

        // partition_offset
        systemPropertiesBuilder.setQueueOffset(messageExt.getQueueOffset());

        // delivery_attempt
        systemPropertiesBuilder.setDeliveryAttempt(messageExt.getReconsumeTimes() + 1);

        // trace context
        String traceContext = messageExt.getProperty(MessageConst.PROPERTY_TRACE_CONTEXT);
        if (traceContext != null) {
            systemPropertiesBuilder.setTraceContext(traceContext);
        }

        String dlqOriginTopic = messageExt.getProperty(MessageConst.PROPERTY_DLQ_ORIGIN_TOPIC);
        String dlqOriginMessageId = messageExt.getProperty(MessageConst.PROPERTY_DLQ_ORIGIN_MESSAGE_ID);
        if (dlqOriginTopic != null && dlqOriginMessageId != null) {
            DeadLetterQueue dlq = DeadLetterQueue.newBuilder()
                .setTopic(dlqOriginTopic)
                .setMessageId(dlqOriginMessageId)
                .build();
            systemPropertiesBuilder.setDeadLetterQueue(dlq);
        }
        return systemPropertiesBuilder.build();
    }

    public Resource buildResource(String resourceNameWithNamespace) {
        return Resource.newBuilder()
            .setResourceNamespace(NamespaceUtil.getNamespaceFromResource(resourceNameWithNamespace))
            .setName(NamespaceUtil.withoutNamespace(resourceNameWithNamespace))
            .build();
    }
}
