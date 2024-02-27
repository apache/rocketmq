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

package org.apache.rocketmq.tieredstore.index;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.tieredstore.common.AppendResult;

public interface IndexService {

    /**
     * Puts a key into the index.
     *
     * @param topic     The topic of the key.
     * @param topicId   The ID of the topic.
     * @param queueId   The ID of the queue.
     * @param keySet    The set of keys to be indexed.
     * @param offset    The offset value of the key.
     * @param size      The size of the key.
     * @param timestamp The timestamp of the key.
     * @return The result of the put operation.
     */
    AppendResult putKey(
        String topic, int topicId, int queueId, Set<String> keySet, long offset, int size, long timestamp);

    /**
     * Asynchronously queries the index for a specific key within a given time range.
     *
     * @param topic     The topic of the key.
     * @param key       The key to be queried.
     * @param beginTime The start time of the query range.
     * @param endTime   The end time of the query range.
     * @return A CompletableFuture that holds the list of IndexItems matching the query.
     */
    CompletableFuture<List<IndexItem>> queryAsync(String topic, String key, int maxCount, long beginTime, long endTime);

    /**
     * Shutdown the index service.
     */
    void shutdown();

    /**
     * Destroys the index service and releases all resources.
     */
    void destroy();
}
