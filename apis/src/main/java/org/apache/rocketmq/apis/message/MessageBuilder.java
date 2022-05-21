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

package org.apache.rocketmq.apis.message;

/**
 * Builder to config {@link Message}.
 */
public interface MessageBuilder {
    /**
     * Set the topic for message, which is essential for each message.
     *
     * @param topic topic for the message.
     * @return the message builder instance.
     */
    MessageBuilder setTopic(String topic);

    /**
     * Set the body for message, which is essential for each message.
     *
     * <p>{@link Message} will deep-copy the body as its payload, thus any modification about the original body would
     * not affect message itself.
     *
     * @param body body for the message.
     * @return the message builder instance.
     */
    MessageBuilder setBody(byte[] body);

    /**
     * Set the tag for message, which is optional.
     *
     * <p>Tag is a secondary classifier for each message besides topic.
     *
     * @param tag tag for the message.
     * @return the message builder instance.
     */
    MessageBuilder setTag(String tag);

    /**
     * Set the key collection for message, which is optional.
     *
     * <p>Message key is another way to locate a message besides the {@link MessageId}, so it should be unique for
     * each message usually.
     *
     * <p>Set a single key for each message is enough in most case, but the variadic argument here allows you to set
     * multiple key at the same time. For example:
     *
     * <pre>{@code
     * // Example 0: single key.
     * messageBuilder.setKeys("8c2e8165-6813-472d-b189-477c88f7420b");
     * // Example 1: multiple keys.
     * messageBuilder.setKeys("524d3246-0165-4d51-8311-527f0f310c9c", "b295b99b-0b0b-4ad7-8e4c-3ebabc44d75a");
     * // Example 2: multiple keys.
     * ArrayList<String> keyList = new ArrayList<>();
     * keyList.add("08f55a3d-0521-445d-9870-58f5d9fa1051");
     * keyList.add("3eddc563-fd5f-4ca9-9eac-71b3f196b26c");
     * String[] keyArray = keyList.toArray(new String[0]);
     * messageBuilder.setKeys(keyArray);
     * }</pre>
     *
     * @param keys key(s) for the message.
     * @return the message builder instance.
     */
    MessageBuilder setKeys(String... keys);

    /**
     * Set the group for message, which is optional.
     *
     * <p>Message group and delivery timestamp should not be set in the same message.
     *
     * @param messageGroup group for the message.
     * @return the message builder instance.
     */
    MessageBuilder setMessageGroup(String messageGroup);

    /**
     * Set the trace context for each message, which should follow openTelemetry specs.
     *
     * @param traceContext trace context for the messages.
     * @return the message builder instance.
     */
    MessageBuilder setTraceContext(String traceContext);

    /**
     * Set the delivery timestamp for message, which is optional.
     *
     * <p>Delivery timestamp and message group should not be set in the same message.
     *
     * @param deliveryTimestamp delivery timestamp for message.
     * @return the message builder instance.
     */
    MessageBuilder setDeliveryTimestamp(long deliveryTimestamp);

    /**
     * Add user property for message.
     *
     * @param key   single property key.
     * @param value single property value.
     * @return the message builder instance.
     */
    MessageBuilder addProperty(String key, String value);

    /**
     * Finalize the build of the {@link Message} instance.
     *
     * <p>Unique {@link MessageId} is generated after message building.</p>
     *
     * @return the message instance.
     */
    Message build();
}
