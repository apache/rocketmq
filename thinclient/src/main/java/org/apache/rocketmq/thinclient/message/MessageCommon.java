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

import com.google.common.base.Stopwatch;
import com.google.protobuf.Timestamp;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.rocketmq.apis.message.MessageId;

public class MessageCommon {
    @Nullable
    private final MessageId messageId;
    private final String topic;
    private final byte[] body;
    private final Map<String, String> properties;
    @Nullable
    private final String tag;
    private final Collection<String> keys;
    @Nullable
    private final String messageGroup;
    @Nullable
    private final Long deliveryTimestamp;
    @Nullable
    private final String bornHost;
    @Nullable
    private final String parentTraceContext;
    @Nullable
    private final String traceContext;
    @Nullable
    private final Long bornTimestamp;
    @Nullable
    private final Integer deliveryAttempt;
    @Nullable
    private final Stopwatch decodeStopwatch;
    @Nullable
    private final Timestamp deliveryTimestampFromRemote;

    public MessageCommon(String topic, byte[] body, String tag, String messageGroup, Long deliveryTimestamp,
        String parentTraceContext, Collection<String> keys, Map<String, String> properties) {
        this(null, topic, body, properties, tag, keys, messageGroup, deliveryTimestamp, null, parentTraceContext, null, null, null, null, null);
    }

    public MessageCommon(MessageId messageId, String topic, byte[] body, String tag, String messageGroup,
        Long deliveryTimestamp, Collection<String> keys, Map<String, String> properties, String bornHost,
        String traceContext, long bornTimestamp, int deliveryAttempt, Stopwatch decodeStopwatch,
        Timestamp deliveryTimestampFromRemote) {
        this(messageId, topic, body, properties, tag, keys, messageGroup, deliveryTimestamp, bornHost, null, traceContext, bornTimestamp, deliveryAttempt, decodeStopwatch, deliveryTimestampFromRemote);
    }

    private MessageCommon(@Nullable MessageId messageId, String topic, byte[] body,
        Map<String, String> properties, @Nullable String tag, Collection<String> keys,
        @Nullable String messageGroup, @Nullable Long deliveryTimestamp, @Nullable String bornHost,
        @Nullable String parentTraceContext, @Nullable String traceContext, @Nullable Long bornTimestamp,
        @Nullable Integer deliveryAttempt, @Nullable Stopwatch decodeStopwatch, Timestamp deliveryTimestampFromRemote) {
        this.messageId = messageId;
        this.topic = topic;
        this.body = body;
        this.properties = properties;
        this.tag = tag;
        this.keys = keys;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.bornHost = bornHost;
        this.parentTraceContext = parentTraceContext;
        this.traceContext = traceContext;
        this.bornTimestamp = bornTimestamp;
        this.deliveryAttempt = deliveryAttempt;
        this.decodeStopwatch = decodeStopwatch;
        this.deliveryTimestampFromRemote = deliveryTimestampFromRemote;
    }

    public Optional<MessageId> getMessageId() {
        return null == messageId ? Optional.empty() : Optional.of(messageId);
    }

    public String getTopic() {
        return topic;
    }

    public ByteBuffer getBody() {
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }

    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    public Optional<String> getTag() {
        return null == tag ? Optional.empty() : Optional.of(tag);
    }

    public Collection<String> getKeys() {
        return keys;
    }

    public Optional<String> getMessageGroup() {
        return null == messageGroup ? Optional.empty() : Optional.of(messageGroup);
    }

    public Optional<Long> getDeliveryTimestamp() {
        return null == deliveryTimestamp ? Optional.empty() : Optional.of(deliveryTimestamp);
    }

    public Optional<String> getBornHost() {
        return null == bornHost ? Optional.empty() : Optional.of(bornHost);
    }

    public Optional<String> getParentTraceContext() {
        return null == parentTraceContext ? Optional.empty() : Optional.of(parentTraceContext);
    }

    public Optional<String> getTraceContext() {
        return null == traceContext ? Optional.empty() : Optional.of(traceContext);
    }

    public Optional<Long> getBornTimestamp() {
        return null == bornTimestamp ? Optional.empty() : Optional.of(bornTimestamp);
    }

    public Optional<Integer> getDeliveryAttempt() {
        return null == deliveryAttempt ? Optional.empty() : Optional.of(deliveryAttempt);
    }

    public Optional<Duration> getDurationAfterDecoding() {
        return null == decodeStopwatch ? Optional.empty() : Optional.of(decodeStopwatch.elapsed());
    }

    public Optional<Timestamp> getDeliveryTimestampFromRemote() {
        return null == deliveryTimestampFromRemote ? Optional.empty() : Optional.of(deliveryTimestampFromRemote);
    }
}
