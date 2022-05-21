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

import com.google.common.base.MoreObjects;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.rocketmq.apis.message.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Default implementation of {@link Message}
 *
 * @see Message
 */
public class MessageImpl implements Message {
    private final String topic;
    final byte[] body;

    @Nullable
    private final String tag;
    @Nullable
    private final String messageGroup;
    @Nullable
    private final Long deliveryTimestamp;
    @Nullable
    private final String parentTraceContext;

    protected final Collection<String> keys;
    private final Map<String, String> properties;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    MessageImpl(String topic, byte[] body, @Nullable String tag, Collection<String> keys,
        @Nullable String parentTraceContext, @Nullable String messageGroup, @Nullable Long deliveryTimestamp,
        Map<String, String> properties) {
        this.topic = topic;
        this.body = body;
        this.tag = tag;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.keys = keys;
        this.parentTraceContext = parentTraceContext;
        this.properties = properties;
    }

    public MessageCommon getMessageCommon() {
        return new MessageCommon(topic, body, tag, messageGroup, deliveryTimestamp, parentTraceContext, keys, properties);
    }

    MessageImpl(Message message) {
        this.topic = message.getTopic();
        if (message instanceof MessageImpl) {
            MessageImpl impl = (MessageImpl) message;
            this.body = impl.body;
        } else {
            // Should never reach here.
            final ByteBuffer body = message.getBody();
            byte[] bytes = new byte[body.remaining()];
            body.get(bytes);
            this.body = bytes;
        }
        this.tag = message.getTag().orElse(null);
        this.messageGroup = message.getMessageGroup().orElse(null);
        this.deliveryTimestamp = message.getDeliveryTimestamp().orElse(null);
        this.parentTraceContext = message.getParentTraceContext().orElse(null);
        this.keys = message.getKeys();
        this.properties = message.getProperties();
    }

    /**
     * @see Message#getTopic()
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * @see Message#getBody()
     */
    @Override
    public ByteBuffer getBody() {
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }

    /**
     * @see Message#getProperties()
     */
    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    /**
     * @see Message#getTag()
     */
    @Override
    public Optional<String> getTag() {
        return null == tag ? Optional.empty() : Optional.of(tag);
    }

    /**
     * @see Message#getKeys()
     */
    @Override
    public Collection<String> getKeys() {
        return new ArrayList<>(keys);
    }

    /**
     * @see Message#getDeliveryTimestamp()
     */
    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return null == deliveryTimestamp ? Optional.empty() : Optional.of(deliveryTimestamp);
    }

    /**
     * @see Message#getMessageGroup()
     */
    @Override
    public Optional<String> getMessageGroup() {
        return null == messageGroup ? Optional.empty() : Optional.of(messageGroup);
    }

    /**
     * @see Message#getParentTraceContext()
     */
    @Override
    public Optional<String> getParentTraceContext() {
        return null == parentTraceContext ? Optional.empty() : Optional.of(parentTraceContext);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topic", topic)
            .add("tag", tag)
            .add("messageGroup", messageGroup)
            .add("deliveryTimestamp", deliveryTimestamp)
            .add("parentTraceContext", parentTraceContext)
            .add("keys", keys)
            .add("properties", properties)
            .toString();
    }
}
