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
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.apis.MessageQueue;

/**
 * {@link MessageView} provides a read-only view for message, that's why setters do not exist here. In addition,
 * it only makes sense when {@link Message} is sent successfully, or it could be considered as a return receipt
 * for producer/consumer.
 */
public interface MessageView {
    /**
     * Get the unique id of message.
     *
     * @return unique id.
     */
    MessageId getMessageId();

    /**
     * Get the topic of message.
     *
     * @return topic of message.
     */
    String getTopic();

    /**
     * Get the <strong>deep copy</strong> of message body, which makes the modification of return value does not
     * affect the message itself.
     *
     * @return the <strong>deep copy</strong> of message body.
     */
    byte[] getBody();

    /**
     * Get the <strong>deep copy</strong> of message properties, which makes the modification of return value does
     * not affect the message itself.
     *
     * @return the <strong>deep copy</strong> of message properties.
     */
    Map<String, String> getProperties();

    /**
     * Get the tag of message, which is optional.
     *
     * @return the tag of message, which is optional.
     */
    Optional<String> getTag();

    /**
     * Get the key collection of message.
     *
     * @return <strong>the key collection</strong> of message.
     */
    Collection<String> getKeys();

    /**
     * Get the message group, which is optional and only make sense only when topic type is fifo.
     *
     * @return message group, which is optional.
     */
    Optional<String> getMessageGroup();

    /**
     * Get the expected delivery timestamp, which make sense only when topic type is delay.
     *
     * @return message expected delivery timestamp, which is optional.
     */
    Optional<Long> getDeliveryTimestamp();

    /**
     * Get the born host of message.
     *
     * @return born host of message.
     */
    String getBornHost();

    /**
     * Get the born timestamp of message.
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

    /**
     * Get the {@link MessageQueue} of message.
     *
     * @return message queue.
     */
    MessageQueue getMessageQueue();

    /**
     * Get the position of message in {@link MessageQueue}.
     */
    long getOffset();
}
