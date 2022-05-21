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

import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.thinclient.impl.producer.ProducerSettings;
import org.apache.rocketmq.thinclient.message.protocol.Encoding;
import org.apache.rocketmq.thinclient.misc.Utilities;

/**
 * This class is a publishing view for message, which could be considered as an extension of {@link MessageImpl}.
 * Specifically speaking, Some work has been brought forward, e.g. message body compression, message id generation, etc.
 */
public class PublishingMessageImpl extends MessageImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishingMessageImpl.class);

    private final Encoding encoding;
    private final ByteBuffer compressedBody;
    private final MessageId messageId;
    private final MessageType messageType;
    private volatile String traceContext;

    public PublishingMessageImpl(Message message, ProducerSettings producerSettings, boolean txEnabled) throws IOException {
        super(message);
        this.traceContext = null;
        final int length = message.getBody().remaining();
        final int maxBodySizeBytes = producerSettings.getMaxBodySizeBytes();
        if (length > maxBodySizeBytes) {
            throw new IOException("Message body size exceeds the threshold, max size=" + maxBodySizeBytes + " bytes");
        }
        // Message body length exceeds the compression threshold, try to compress it.
        if (length > producerSettings.getCompressBodyThresholdBytes()) {
            byte[] body;
            // Try downcasting to avoid redundant copy because ByteBuffer could not be compressed directly.
            if (message instanceof MessageImpl) {
                MessageImpl messageImpl = (MessageImpl) message;
                body = messageImpl.body;
            } else {
                // Failed to downcast, which is out of expectation.
                LOGGER.error("[Bug] message is not an instance of MessageImpl, have to copy it to compress");
                body = new byte[length];
                message.getBody().get(body);
            }
            final byte[] compressed = Utilities.compressBytesGzip(body, producerSettings.getMessageGzipCompressionLevel());
            this.compressedBody = ByteBuffer.wrap(compressed).asReadOnlyBuffer();
            this.encoding = Encoding.GZIP;
        } else {
            // No need to compress message body.
            this.compressedBody = null;
            this.encoding = Encoding.IDENTITY;
        }
        // Generate message id.
        this.messageId = MessageIdCodec.getInstance().nextMessageId();
        // Normal message.
        if (!message.getMessageGroup().isPresent() &&
            !message.getDeliveryTimestamp().isPresent() && !txEnabled) {
            messageType = MessageType.NORMAL;
            return;
        }
        // Fifo message.
        if (message.getMessageGroup().isPresent() && !txEnabled) {
            messageType = MessageType.FIFO;
            return;
        }
        // Delay message.
        if (message.getDeliveryTimestamp().isPresent() && !txEnabled) {
            messageType = MessageType.DELAY;
            return;
        }
        // Transaction message.
        if (!message.getMessageGroup().isPresent() &&
            !message.getDeliveryTimestamp().isPresent() && txEnabled) {
            messageType = MessageType.TRANSACTION;
            return;
        }
        // Transaction semantics is conflicted with fifo/delay.
        throw new IllegalArgumentException("Transactional message should not set messageGroup or deliveryTimestamp");
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public ByteBuffer getTransportBody() {
        return null == compressedBody ? getBody() : compressedBody;
    }

    public void setTraceContext(String traceContext) {
        this.traceContext = traceContext;
    }

    public Optional<String> getTraceContext() {
        return null == traceContext ? Optional.empty() : Optional.of(traceContext);
    }

    /**
     * Convert {@link PublishingMessageImpl} to protocol buffer.
     *
     * <p>This method should be invoked before each message sending, because the born time is reset before each
     * invocation, which means that it should not be invoked ahead of time.
     */
    public apache.rocketmq.v2.Message toProtobuf() {
        final apache.rocketmq.v2.SystemProperties.Builder systemPropertiesBuilder =
            apache.rocketmq.v2.SystemProperties.newBuilder()
                // Message keys
                .addAllKeys(keys)
                // Message Id
                .setMessageId(messageId.toString())
                // Born time should be reset before each sending
                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                // Born host
                .setBornHost(Utilities.hostName())
                // Body encoding
                .setBodyEncoding(Encoding.toProtobuf(encoding))
                // Message type
                .setMessageType(MessageType.toProtobuf(messageType));
        // Message tag
        this.getTag().ifPresent(systemPropertiesBuilder::setTag);
        // Trace context
        this.getTraceContext().ifPresent(systemPropertiesBuilder::setTraceContext);
        final SystemProperties systemProperties = systemPropertiesBuilder.build();
        Resource topicResource = Resource.newBuilder().setName(getTopic()).build();
        return apache.rocketmq.v2.Message.newBuilder()
            // Topic
            .setTopic(topicResource)
            // Message body
            .setBody(ByteString.copyFrom(getTransportBody()))
            // System properties
            .setSystemProperties(systemProperties)
            // User properties
            .putAllUserProperties(getProperties())
            .build();
    }
}
