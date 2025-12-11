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
package org.apache.rocketmq.broker.pop.orderly;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.store.GetMessageResult;

/**
 *
 * Ordered Consumption Controller Interface
 * This is the top-level interface that encapsulates complete ordered consumption management functionality,
 * supporting different concurrency strategy implementations
 * <p>
 * Design Goals:
 * 1. Support queue-level ordered consumption (existing implementation)
 * 2. Support message group-level ordered consumption (improve concurrency)
 * 3. Support custom ordered consumption strategies
 * </p>
 */
public interface ConsumerOrderInfoManager {

    /**
     * Update the reception status of message list
     * Called by handleGetMessageResult when consumer POPs messages, used to record message status and build consumption information
     *
     * @param attemptId          Distinguish different pop requests
     * @param isRetry            Whether it is a retry topic
     * @param topic              Topic name
     * @param group              Consumer group name
     * @param queueId            Queue ID
     * @param popTime            Time when messages are popped
     * @param invisibleTime      Message invisible time
     * @param msgQueueOffsetList List of message queue offsets
     * @param orderInfoBuilder   String builder for constructing order information
     * @param getMessageResult   Return new result
     */
    void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder, GetMessageResult getMessageResult);

    /**
     * Check whether the current POP request needs to be blocked
     * Used to ensure ordered consumption of ordered messages
     * Called when consumer POPs messages
     *
     * @param attemptId     Attempt ID
     * @param topic         Topic name
     * @param group         Consumer group name
     * @param queueId       Queue ID
     * @param invisibleTime Invisible time
     * @return true indicates blocking is needed, false indicates can proceed
     */
    boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime);

    /**
     * Commit message and calculate next consumption offset
     * Called when consumer ACKs messages
     *
     * @param topic       Topic name
     * @param group       Consumer group name
     * @param queueId     Queue ID
     * @param queueOffset Message queue offset
     * @param popTime     Pop time, used for validation
     * @return -1: invalid, -2: no need to commit, >=0: offset that needs to be committed (indicates messages below this offset have been consumed)
     */
    long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime);

    /**
     * Update the next visible time of message
     * Used for delayed message re-consumption
     *
     * @param topic           Topic name
     * @param group           Consumer group name
     * @param queueId         Queue ID
     * @param queueOffset     Message offset
     * @param popTime         Pop time, used for validation
     * @param nextVisibleTime Next visible time
     */
    void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
        long popTime, long nextVisibleTime);

    /**
     * Clear the blocking status of specified queue
     * Usually called during consumer rebalancing or queue reassignment
     *
     * @param topic   Topic name
     * @param group   Consumer group name
     * @param queueId Queue ID
     */
    void clearBlock(String topic, String group, int queueId);

    /**
     * Get ordered consumption level
     * Used to distinguish different implementation strategies
     *
     * @return Ordered consumption level, such as: QUEUE, MESSAGE_GROUP, etc.
     */
    OrderedConsumptionLevel getOrderedConsumptionLevel();

    /**
     * Start the controller
     * Initialize necessary resources, such as timers, thread pools, etc.
     */
    void start();

    /**
     * Shutdown the controller
     * Release resources, clean up scheduled tasks, etc.
     */
    void shutdown();

    /**
     * Persist the controller
     * Persist the controller's data
     */
    void persist();

    boolean load();

    /**
     * Get available message result
     * Used to retrieve messages from cache
     */
    CompletableFuture<GetMessageResult> getAvailableMessageResult(String attemptId, long popTime, long invisibleTime, String groupId,
        String topicId, int queueId, int batchSize, StringBuilder orderCountInfoBuilder);
}
