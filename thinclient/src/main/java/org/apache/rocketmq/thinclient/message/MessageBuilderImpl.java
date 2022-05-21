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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.message.MessageBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MessageBuilderImpl implements MessageBuilder {
    public static final Pattern TOPIC_PATTERN = Pattern.compile("^[%|a-zA-Z0-9._-]{1,127}$");
    private static final int MESSAGE_BODY_LENGTH_THRESHOLD = 1024 * 1024 * 4;

    private String topic = null;
    private byte[] body = null;
    private String tag = null;
    private String messageGroup = null;
    private String traceContext = null;
    private Long deliveryTimestamp = null;
    private Collection<String> keys = new HashSet<>();
    private final Map<String, String> properties = new HashMap<>();

    public MessageBuilderImpl() {
    }

    /**
     * See {@link MessageBuilder#setTopic(String)}
     */
    @Override
    public MessageBuilder setTopic(String topic) {
        checkNotNull(topic, "topic should not be null");
        checkArgument(TOPIC_PATTERN.matcher(topic).matches(), "topic does not match the regex [regex=%s]",
            TOPIC_PATTERN.pattern());
        this.topic = topic;
        return this;
    }

    /**
     * See {@link MessageBuilder#setBody(byte[])}
     */
    @Override
    public MessageBuilder setBody(byte[] body) {
        checkNotNull(body, "body should not be null");
        checkArgument(body.length <= MESSAGE_BODY_LENGTH_THRESHOLD, "message body length exceeds the threshold " +
            "[threshold=%s bytes]", MESSAGE_BODY_LENGTH_THRESHOLD);
        this.body = body.clone();
        return this;
    }

    /**
     * See {@link MessageBuilder#setTag(String)}
     */
    @Override
    public MessageBuilder setTag(String tag) {
        checkArgument(StringUtils.isNotBlank(tag), "tag should not be blank");
        checkArgument(!tag.contains("|"), "tag should not contain \"|\"");
        this.tag = tag;
        return this;
    }

    /**
     * See {@link MessageBuilder#setKeys(String...)}
     */
    @Override
    public MessageBuilder setKeys(String... keys) {
        for (String key : keys) {
            checkArgument(StringUtils.isNotBlank(key), "key should not be blank");
        }
        this.keys = new ArrayList<>();
        this.keys.addAll(Arrays.asList(keys));
        return this;
    }

    /**
     * See {@link MessageBuilder#setMessageGroup(String)}
     */
    @Override
    public MessageBuilder setMessageGroup(String messageGroup) {
        checkArgument(null == deliveryTimestamp, "messageGroup and deliveryTimestamp should not be set at same time");
        checkArgument(StringUtils.isNotBlank(messageGroup), "messageGroup should not be blank");
        this.messageGroup = messageGroup;
        return this;
    }

    /**
     * See {@link MessageBuilder#setTraceContext(String)}
     */
    @Override
    public MessageBuilder setTraceContext(String traceContext) {
        checkArgument(StringUtils.isNotBlank(traceContext), "traceContext should not be blank");
        this.traceContext = traceContext;
        return this;
    }

    /**
     * See {@link MessageBuilder#setDeliveryTimestamp(long)}
     */
    @Override
    public MessageBuilder setDeliveryTimestamp(long deliveryTimestamp) {
        checkArgument(null == messageGroup, "deliveryTimestamp and messageGroup should not be set at same time");
        this.deliveryTimestamp = deliveryTimestamp;
        return this;
    }

    /**
     * See {@link MessageBuilder#addProperty(String, String)}
     */
    @Override
    public MessageBuilder addProperty(String key, String value) {
        checkArgument(StringUtils.isNotBlank(key), "key should not be blank");
        checkArgument(StringUtils.isNotBlank(value), "value should not be blank");
        this.properties.put(key, value);
        return this;
    }

    /**
     * See {@link MessageBuilder#build()}
     */
    @Override
    public Message build() {
        checkNotNull(topic, "topic has not been set yet");
        checkNotNull(body, "body has not been set yet");
        return new MessageImpl(topic, body, tag, keys, messageGroup, traceContext, deliveryTimestamp, properties);
    }
}
