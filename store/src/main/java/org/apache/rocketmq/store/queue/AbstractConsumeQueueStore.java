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
package org.apache.rocketmq.store.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.rocksdb.RocksDBException;

public abstract class AbstractConsumeQueueStore implements ConsumeQueueStoreInterface {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final DefaultMessageStore messageStore;
    protected final MessageStoreConfig messageStoreConfig;
    protected final QueueOffsetOperator queueOffsetOperator = new QueueOffsetOperator();
    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    public AbstractConsumeQueueStore(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStore.getMessageStoreConfig();
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
    }

    @Override
    public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
        consumeQueue.putMessagePositionInfoWrapper(request);
    }

    @Override
    public Long getMaxOffset(String topic, int queueId) {
        return this.queueOffsetOperator.currentQueueOffset(topic + "-" + queueId);
    }

    @Override
    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.queueOffsetOperator.setTopicQueueTable(topicQueueTable);
        this.queueOffsetOperator.setLmqTopicQueueTable(topicQueueTable);
    }

    @Override
    public ConcurrentMap getTopicQueueTable() {
        return this.queueOffsetOperator.getTopicQueueTable();
    }



    @Override
    public void increaseQueueOffset(String topic, int queueId, short messageNum) {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(topic, queueId);
        consumeQueue.increaseQueueOffset(this.queueOffsetOperator, messageNum);
    }

    @Override
    public long getQueueOffset(String topic, int queueId) throws RocksDBException {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(topic, queueId);
        return consumeQueue.getQueueOffset(this.queueOffsetOperator);
    }

    @Override
    public void increaseLmqOffset(String queueKey, short messageNum) {
        queueOffsetOperator.increaseLmqOffset(queueKey, messageNum);
    }

    @Override
    public long getLmqQueueOffset(String queueKey) {
        return queueOffsetOperator.getLmqOffset(queueKey);
    }

    @Override
    public void removeTopicQueueTable(String topic, Integer queueId) {
        this.queueOffsetOperator.remove(topic, queueId);
    }

    @Override
    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return this.consumeQueueTable;
    }

    @Override
    public ConcurrentMap<Integer, ConsumeQueueInterface> findConsumeQueueMap(String topic) {
        return this.consumeQueueTable.get(topic);
    }

    @Override
    public long getStoreTime(CqUnit cqUnit) {
        if (cqUnit != null) {
            try {
                final long phyOffset = cqUnit.getPos();
                final int size = cqUnit.getSize();
                long storeTime = this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            }
        }
        return -1;
    }
}
