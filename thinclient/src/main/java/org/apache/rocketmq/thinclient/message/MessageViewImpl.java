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

package org.apache.rocketmq.thinclient.message;

import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.SystemProperties;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.message.MessageView;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.thinclient.misc.LinkedElement;
import org.apache.rocketmq.thinclient.misc.LinkedIterator;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;
import org.apache.rocketmq.thinclient.misc.Utilities;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessageViewImpl implements LinkedElement<MessageViewImpl>, MessageView {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageViewImpl.class);

    private final MessageId messageId;
    private final String topic;
    private final byte[] body;
    private final String tag;
    private final String messageGroup;
    private final Long deliveryTimestamp;
    private final Collection<String> keys;
    private final Map<String, String> properties;
    private final String bornHost;
    private final long bornTimestamp;
    private int deliveryAttempt;
    private final MessageQueueImpl messageQueue;
    private final Endpoints endpoints;
    private volatile String receiptHandle;
    private final String traceContext;
    private final long offset;
    private final boolean corrupted;
    private final Stopwatch decodeStopwatch;
    private final Timestamp deliveryTimestampFromRemote;
    private MessageViewImpl next;

    public MessageViewImpl(MessageId messageId, String topic, byte[] body, String tag, String messageGroup,
        Long deliveryTimestamp, Collection<String> keys, Map<String, String> properties,
        String bornHost, long bornTimestamp, int deliveryAttempt, MessageQueueImpl messageQueue, String receiptHandle,
        String traceContext, long offset, boolean corrupted, Timestamp deliveryTimestampFromRemote) {
        this.messageId = checkNotNull(messageId, "messageId should not be null");
        this.topic = checkNotNull(topic, "topic should not be null");
        this.body = checkNotNull(body, "body should not be null");
        this.tag = tag;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.keys = checkNotNull(keys, "keys should not be null");
        this.properties = checkNotNull(properties, "properties should not be null");
        this.bornHost = checkNotNull(bornHost, "bornHost should not be null");
        this.bornTimestamp = bornTimestamp;
        this.deliveryAttempt = deliveryAttempt;
        this.messageQueue = checkNotNull(messageQueue, "messageQueue should not be null");
        this.endpoints = messageQueue.getBroker().getEndpoints();
        this.receiptHandle = checkNotNull(receiptHandle, "receiptHandle should not be null");
        this.traceContext = traceContext;
        this.offset = offset;
        this.corrupted = corrupted;
        this.decodeStopwatch = Stopwatch.createStarted();
        this.deliveryTimestampFromRemote = deliveryTimestampFromRemote;
        this.next = null;
    }

    public MessageCommon getMessageCommon() {
        return new MessageCommon(messageId, topic, body, tag, messageGroup, deliveryTimestamp, keys, properties, bornHost, traceContext, bornTimestamp, deliveryAttempt, decodeStopwatch, null);
    }

    /**
     * @see MessageView#getMessageId()
     */
    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    /**
     * @see MessageView#getTopic()
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * @see MessageView#getBody()
     */
    @Override
    public ByteBuffer getBody() {
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }

    /**
     * @see MessageView#getProperties()
     */
    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    /**
     * @see MessageView#getTag()
     */
    @Override
    public Optional<String> getTag() {
        return null == tag ? Optional.empty() : Optional.of(tag);
    }

    /**
     * @see MessageView#getKeys()
     */
    @Override
    public Collection<String> getKeys() {
        return keys;
    }

    /**
     * @see MessageView#getMessageGroup()
     */
    @Override
    public Optional<String> getMessageGroup() {
        return null == messageGroup ? Optional.empty() : Optional.of(messageGroup);
    }

    /**
     * @see MessageView#getDeliveryTimestamp()
     */
    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return null == deliveryTimestamp ? Optional.empty() : Optional.of(deliveryTimestamp);
    }

    /**
     * @see MessageView#getBornHost()
     */
    @Override
    public String getBornHost() {
        return bornHost;
    }

    /**
     * @see MessageView#getBornTimestamp()
     */
    @Override
    public long getBornTimestamp() {
        return bornTimestamp;
    }

    /**
     * @see MessageView#getDeliveryAttempt()
     */
    @Override
    public int getDeliveryAttempt() {
        return deliveryAttempt;
    }

    public Optional<String> getTraceContext() {
        return null == traceContext ? Optional.empty() : Optional.of(traceContext);
    }

    public int incrementAndGetDeliveryAttempt() {
        return ++deliveryAttempt;
    }

    public MessageQueueImpl getMessageQueue() {
        return messageQueue;
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }

    public long getOffset() {
        return offset;
    }

    public boolean isCorrupted() {
        return corrupted;
    }

    public void setNext(MessageViewImpl messageView) {
        this.next = messageView;
    }

    @Override
    public MessageViewImpl getNext() {
        return next;
    }

    @Override
    public Iterator<MessageViewImpl> iterator() {
        return new LinkedIterator<>(this);
    }

    public static MessageViewImpl fromProtobuf(Message message, MessageQueueImpl mq) {
        return MessageViewImpl.fromProtobuf(message, mq, null);
    }

    public static MessageViewImpl fromProtobuf(Message message, MessageQueueImpl mq, Timestamp timestamp) {
        final SystemProperties systemProperties = message.getSystemProperties();
        final String topic = message.getTopic().getName();
        final MessageId messageId = MessageIdCodec.getInstance().decode(systemProperties.getMessageId());
        final Digest bodyDigest = systemProperties.getBodyDigest();
        byte[] body = message.getBody().toByteArray();
        boolean corrupted = false;
        final String checksum = bodyDigest.getChecksum();
        String expectedChecksum;
        final DigestType digestType = bodyDigest.getType();
        switch (digestType) {
            case CRC32:
                expectedChecksum = Utilities.crc32CheckSum(body);
                if (!expectedChecksum.equals(checksum)) {
                    corrupted = true;
                }
                break;
            case MD5:
                try {
                    expectedChecksum = Utilities.md5CheckSum(body);
                    if (!expectedChecksum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    LOGGER.error("MD5 is not supported unexpectedly, skip it, topic={}, messageId={}", topic, messageId);
                }
                break;
            case SHA1:
                try {
                    expectedChecksum = Utilities.sha1CheckSum(body);
                    if (!expectedChecksum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    LOGGER.error("SHA-1 is not supported unexpectedly, skip it, topic={}, messageId={}", topic, messageId);
                }
                break;
            default:
                LOGGER.error("Unsupported message body digest algorithm, digestType={}, topic={}, messageId={}", digestType, topic, messageId);
        }
        final Encoding bodyEncoding = systemProperties.getBodyEncoding();
        switch (bodyEncoding) {
            case GZIP:
                try {
                    body = Utilities.uncompressBytesGzip(body);
                } catch (IOException e) {
                    LOGGER.error("Failed to uncompress message body, topic={}, messageId={}", topic, messageId);
                    corrupted = true;
                }
                break;
            case IDENTITY:
                break;
            default:
                LOGGER.error("Unsupported message encoding algorithm, topic={}, messageId={}, bodyEncoding={}", topic, messageId, bodyEncoding);
        }

        String tag = systemProperties.hasTag() ? systemProperties.getTag() : null;
        String messageGroup = systemProperties.hasMessageGroup() ? systemProperties.getMessageGroup() : null;
        Long deliveryTimestamp = systemProperties.hasDeliveryTimestamp() ?
            Timestamps.toMillis(systemProperties.getDeliveryTimestamp()) : null;
        final ProtocolStringList keys = systemProperties.getKeysList();
        final String bornHost = systemProperties.getBornHost();
        final long bornTimestamp = Timestamps.toMillis(systemProperties.getBornTimestamp());
        final int deliveryAttempt = systemProperties.getDeliveryAttempt();
        final long offset = systemProperties.getQueueOffset();
        final Map<String, String> properties = message.getUserPropertiesMap();
        final String receiptHandle = systemProperties.getReceiptHandle();
        String traceContext = systemProperties.hasTraceContext() ? systemProperties.getTraceContext() : null;
        return new MessageViewImpl(messageId, topic, body, tag, messageGroup, deliveryTimestamp, keys, properties, bornHost, bornTimestamp, deliveryAttempt, mq, receiptHandle, traceContext, offset, corrupted, timestamp);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageId", messageId)
            .add("topic", topic)
            .add("tag", tag)
            .add("messageGroup", messageGroup)
            .add("deliveryTimestamp", deliveryTimestamp)
            .add("keys", keys)
            .add("properties", properties)
            .add("bornHost", bornHost)
            .add("bornTimestamp", bornTimestamp)
            .add("deliveryAttempt", deliveryAttempt)
            .add("messageQueue", messageQueue)
            .add("endpoints", endpoints)
            .add("receiptHandle", receiptHandle)
            .add("traceContext", traceContext)
            .add("offset", offset)
            .add("corrupted", corrupted)
            .add("decodeStopwatch", decodeStopwatch)
            .add("deliveryTimestampFromRemote", deliveryTimestampFromRemote)
            .toString();
    }
}
