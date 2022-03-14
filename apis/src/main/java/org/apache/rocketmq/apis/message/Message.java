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
import org.apache.rocketmq.apis.producer.Producer;

/**
 * Abstract message only used for {@link Producer}.
 */
public interface Message {
    /**
     * Get the topic of message, which is the first classifier for message.
     *
     * @return topic of message.
     */
    String getTopic();

    /**
     * Get the <strong>deep copy</strong> of message body.
     *
     * @return the <strong>deep copy</strong> of message body.
     */
    byte[] getBody();

    /**
     * Get the <strong>deep copy</strong> of message properties.
     *
     * @return the <strong>deep copy</strong> of message properties.
     */
    Map<String, String> getProperties();

    /**
     * Get the tag of message, which is the second classifier besides topic.
     *
     * @return the tag of message.
     */
    Optional<String> getTag();

    /**
     * Get the key collection of message.
     *
     * @return <strong>the key collection</strong> of message.
     */
    Collection<String> getKeys();

    /**
     * Get the message group, which make sense only when topic type is fifo.
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
}
