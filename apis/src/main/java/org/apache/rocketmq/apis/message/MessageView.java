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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * {@link MessageView} provides a read-only view for message, that's why setters do not exist here. In addition,
 * it only makes sense when {@link Message} is sent successfully.
 */
public interface MessageView {
    /**
     * Get the unique id of message.
     *
     * @return unique id.
     */
    MessageId getMessageId();

    /**
     * Get the topic of message, which is the first classifier for message.
     *
     * @return topic of message.
     */
    String getTopic();

    /**
     * Get the <strong>deep copy</strong> of message body, which means any modification of return value does not
     * affect the built-in message body.
     *
     * @return the <strong>deep copy</strong> of message body.
     */
    ByteBuffer getBody();

    /**
     * Get the <strong>deep copy</strong> of message properties, which makes the modification of return value does
     * not affect the message itself.
     *
     * @return the <strong>deep copy</strong> of message properties.
     */
    Map<String, String> getProperties();

    /**
     * Get the tag of message, which is the second classifier besides topic.
     *
     * @return the tag of message, which is optional, {@link Optional#empty()} means tag does not exist.
     */
    Optional<String> getTag();

    /**
     * Get the key collection of message, which means any modification of return value does not affect the built-in
     * message key collection.
     *
     * @return copy of key collection of message, empty collection means message key is not specified.
     */
    Collection<String> getKeys();

    /**
     * Get the message group, which make sense only when topic type is fifo.
     *
     * @return message group, which is optional, {@link Optional#empty()} means message group is not specified.
     */
    Optional<String> getMessageGroup();

    /**
     * Get the expected delivery timestamp, which make sense only when topic type is delay.
     *
     * @return message expected delivery timestamp, which is optional, {@link Optional#empty()} means delivery
     * timestamp is not specified.
     */
    Optional<Long> getDeliveryTimestamp();

    /**
     * Get the born host of message.
     *
     * @return born host of message.
     */
    String getBornHost();

    Optional<String> getTraceContext();

    /**
     * Get the born timestamp of message.
     *
     * <p>Born time means the timestamp that the message is prepared to send rather than the timestamp the
     * {@link Message} was built.
     *
     * @return born timestamp of message.
     */
    long getBornTimestamp();

    /**
     * Get the delivery attempt for message.
     *
     * @return delivery attempt.
     */
    int getDeliveryAttempt();
}
