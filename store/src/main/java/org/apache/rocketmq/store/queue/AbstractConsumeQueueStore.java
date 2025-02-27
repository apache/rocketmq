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
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
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
        if (messageStoreConfig.isEnableLmq()) {
            this.consumeQueueTable = new ConcurrentHashMap<>(32_768);
        } else {
            this.consumeQueueTable = new ConcurrentHashMap<>(32);
        }
    }

    @Override
    public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
        consumeQueue.putMessagePositionInfoWrapper(request);
    }

    @Override
    public Long getMaxOffset(String topic, int queueId) throws ConsumeQueueException {
        return this.queueOffsetOperator.currentQueueOffset(topic + "-" + queueId);
    }

    @Override
    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.queueOffsetOperator.setTopicQueueTable(topicQueueTable);
        this.queueOffsetOperator.setLmqTopicQueueTable(topicQueueTable);
    }

    @Override
    public ConcurrentMap<String, Long> getTopicQueueTable() {
        return this.queueOffsetOperator.getTopicQueueTable();
    }

    @Override
    public void assignQueueOffset(MessageExtBrokerInner msg) throws RocksDBException {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
        consumeQueue.assignQueueOffset(this.queueOffsetOperator, msg);
    }

    @Override
    public void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum) {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
        consumeQueue.increaseQueueOffset(this.queueOffsetOperator, msg, messageNum);
    }

    @Override
    public void increaseLmqOffset(String topic, int queueId, short delta) throws ConsumeQueueException {
        queueOffsetOperator.increaseLmqOffset(topic, queueId, delta);
    }

    @Override
    public long getLmqQueueOffset(String topic, int queueId) throws ConsumeQueueException {
        return queueOffsetOperator.getLmqOffset(topic, queueId, (t, q) -> 0L);
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
                return this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
            } catch (Exception e) {
                log.error("Failed to getStoreTime", e);
            }
        }
        return -1;
    }
}
