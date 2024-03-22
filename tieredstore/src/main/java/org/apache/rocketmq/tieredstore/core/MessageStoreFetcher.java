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

package org.apache.rocketmq.tieredstore.core;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.QueryMessageResult;

public interface MessageStoreFetcher {

    /**
     * Asynchronous get the store time of the earliest message in this store.
     *
     * @return timestamp of the earliest message in this store.
     */
    CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId);

    /**
     * Asynchronous get the store time of the message specified.
     *
     * @param topic              Message topic.
     * @param queueId            Queue ID.
     * @param consumeQueueOffset Consume queue offset.
     * @return store timestamp of the message.
     */
    CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId, long consumeQueueOffset);

    /**
     * Look up the physical offset of the message whose store timestamp is as specified.
     *
     * @param topic     Topic of the message.
     * @param queueId   Queue ID.
     * @param timestamp Timestamp to look up.
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType type);

    /**
     * Asynchronous get message
     *
     * @param group         Consumer group that launches this query.
     * @param topic         Topic to query.
     * @param queueId       Queue ID to query.
     * @param offset        Logical offset to start from.
     * @param maxCount      Maximum count of messages to query.
     * @param messageFilter Message filter used to screen desired messages.
     * @return Matched messages.
     */
    CompletableFuture<GetMessageResult> getMessageAsync(
        String group, String topic, int queueId, long offset, int maxCount, MessageFilter messageFilter);

    /**
     * Asynchronous query messages by given key.
     *
     * @param topic    Topic of the message.
     * @param key      Message key.
     * @param maxCount Maximum count of the messages possible.
     * @param begin    Begin timestamp.
     * @param end      End timestamp.
     */
    CompletableFuture<QueryMessageResult> queryMessageAsync(
        String topic, String key, int maxCount, long begin, long end);
}
