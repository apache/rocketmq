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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * QueueOffsetAssigner is a component for assigning offsets for queues.
 */
public class QueueOffsetAssigner {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private ConcurrentMap<String, Long> topicQueueTable = new ConcurrentHashMap<>(1024);
    private ConcurrentMap<String, Long> batchTopicQueueTable = new ConcurrentHashMap<>(1024);
    private ConcurrentMap<String/* topic-queueid */, Long/* offset */> lmqTopicQueueTable = new ConcurrentHashMap<>(1024);

    public long assignQueueOffset(String topicQueueKey, short messageNum) {
        Long queueOffset = ConcurrentHashMapUtils.computeIfAbsent(this.topicQueueTable, topicQueueKey, k -> 0L);
        this.topicQueueTable.put(topicQueueKey, queueOffset + messageNum);
        return queueOffset;
    }

    public void updateQueueOffset(String topicQueueKey, long offset) {
        this.topicQueueTable.put(topicQueueKey, offset);
    }

    public long assignBatchQueueOffset(String topicQueueKey, short messageNum) {
        Long topicOffset = ConcurrentHashMapUtils.computeIfAbsent(this.batchTopicQueueTable, topicQueueKey, k -> 0L);
        this.batchTopicQueueTable.put(topicQueueKey, topicOffset + messageNum);
        return topicOffset;
    }

    public long assignLmqOffset(String topicQueueKey, short messageNum) {
        Long topicOffset = ConcurrentHashMapUtils.computeIfAbsent(this.lmqTopicQueueTable, topicQueueKey, k -> 0L);
        this.lmqTopicQueueTable.put(topicQueueKey, topicOffset + messageNum);
        return topicOffset;
    }

    public long currentQueueOffset(String topicQueueKey) {
        return this.topicQueueTable.get(topicQueueKey);
    }

    public long currentBatchQueueOffset(String topicQueueKey) {
        return this.batchTopicQueueTable.get(topicQueueKey);
    }

    public long currentLmqOffset(String topicQueueKey) {
        return this.lmqTopicQueueTable.get(topicQueueKey);
    }

    public synchronized void remove(String topic, Integer queueId) {
        String topicQueueKey = topic + "-" + queueId;
        // Beware of thread-safety
        this.topicQueueTable.remove(topicQueueKey);
        this.batchTopicQueueTable.remove(topicQueueKey);
        this.lmqTopicQueueTable.remove(topicQueueKey);

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public ConcurrentMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.batchTopicQueueTable = batchTopicQueueTable;
    }
}