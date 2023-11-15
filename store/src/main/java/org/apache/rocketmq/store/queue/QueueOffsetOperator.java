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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * QueueOffsetOperator is a component for operating offsets for queues.
 */
public class QueueOffsetOperator {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private ConcurrentMap<String, Long> topicQueueTable = new ConcurrentHashMap<>(1024);
    private ConcurrentMap<String, Long> batchTopicQueueTable = new ConcurrentHashMap<>(1024);
    private ConcurrentMap<String/* topic-queueid */, Long/* offset */> lmqTopicQueueTable = new ConcurrentHashMap<>(1024);

    public long getQueueOffset(String topicQueueKey) {
        return ConcurrentHashMapUtils.computeIfAbsent(this.topicQueueTable, topicQueueKey, k -> 0L);
    }

    public Long getTopicQueueNextOffset(String topicQueueKey) {
        return this.topicQueueTable.get(topicQueueKey);
    }

    public void increaseQueueOffset(String topicQueueKey, short messageNum) {
        Long queueOffset = ConcurrentHashMapUtils.computeIfAbsent(this.topicQueueTable, topicQueueKey, k -> 0L);
        topicQueueTable.put(topicQueueKey, queueOffset + messageNum);
    }

    public void updateQueueOffset(String topicQueueKey, long offset) {
        this.topicQueueTable.put(topicQueueKey, offset);
    }

    public long getBatchQueueOffset(String topicQueueKey) {
        return ConcurrentHashMapUtils.computeIfAbsent(this.batchTopicQueueTable, topicQueueKey, k -> 0L);
    }

    public void increaseBatchQueueOffset(String topicQueueKey, short messageNum) {
        Long batchQueueOffset = ConcurrentHashMapUtils.computeIfAbsent(this.batchTopicQueueTable, topicQueueKey, k -> 0L);
        this.batchTopicQueueTable.put(topicQueueKey, batchQueueOffset + messageNum);
    }

    public long getLmqOffset(String topicQueueKey) {
        return ConcurrentHashMapUtils.computeIfAbsent(this.lmqTopicQueueTable, topicQueueKey, k -> 0L);
    }

    public Long getLmqTopicQueueNextOffset(String topicQueueKey) {
        return this.lmqTopicQueueTable.get(topicQueueKey);
    }

    public void increaseLmqOffset(String queueKey, short messageNum) {
        Long lmqOffset = ConcurrentHashMapUtils.computeIfAbsent(this.lmqTopicQueueTable, queueKey, k -> 0L);
        this.lmqTopicQueueTable.put(queueKey, lmqOffset + messageNum);
    }

    public long currentQueueOffset(String topicQueueKey) {
        Long currentQueueOffset = this.topicQueueTable.get(topicQueueKey);
        return currentQueueOffset == null ? 0L : currentQueueOffset;
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

    public void setLmqTopicQueueTable(ConcurrentMap<String, Long> lmqTopicQueueTable) {
        ConcurrentMap<String, Long> table = new ConcurrentHashMap<String, Long>(1024);
        for (Map.Entry<String, Long> entry : lmqTopicQueueTable.entrySet()) {
            if (MixAll.isLmq(entry.getKey())) {
                table.put(entry.getKey(), entry.getValue());
            }
        }
        this.lmqTopicQueueTable = table;
    }

    public ConcurrentMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.batchTopicQueueTable = batchTopicQueueTable;
    }
}