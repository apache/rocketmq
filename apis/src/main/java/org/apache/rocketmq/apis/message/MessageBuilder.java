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

import java.util.Collection;

/**
 * Builder to config {@link Message}.
 */
public interface MessageBuilder {
    /**
     * Set the topic for message.
     *
     * @param topic topic for the message.
     * @return the message builder instance.
     */
    MessageBuilder setTopic(String topic);

    /**
     * Set the body for message.
     *
     * @param body body for the message.
     * @return the message builder instance.
     */
    MessageBuilder setBody(byte[] body);

    /**
     * Set the tag for message.
     *
     * @param tag tag for the message.
     * @return the message builder instance.
     */
    MessageBuilder setTag(String tag);

    /**
     * Set the key for message.
     *
     * @param key key for the message.
     * @return the message builder instance.
     */
    MessageBuilder setKey(String key);

    /**
     * Set the key collection for message.
     *
     * @param keys key collection for the message.
     * @return the message builder instance.
     */
    MessageBuilder setKeys(Collection<String> keys);

    /**
     * Set the group for message.
     *
     * @param messageGroup group for the message.
     * @return the message builder instance.
     */
    MessageBuilder setMessageGroup(String messageGroup);

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
