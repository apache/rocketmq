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

import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.StoreUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumeQueueStore {

    protected final MessageStore messageStore;
    protected final MessageStoreConfig messageStoreConfig;
    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    public ConsumeQueueStore(MessageStore messageStore, MessageStoreConfig messageStoreConfig, ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStoreConfig;
        this.consumeQueueTable = consumeQueueTable;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        return (FileQueueLifeCycle) findOrCreateConsumeQueue(topic, queueId);
    }

    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.correctMinOffset(minCommitLogOffset);
    }

    /**
     * Apply the dispatched request and build the consume queue.
     * This function should be idempotent.
     *
     * @param request
     */
    public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.putMessagePositionInfoWrapper(request);
    }

    public boolean load(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.load();
    }

    public void recover(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.recover();
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minOffset);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        return doFindOrCreateConsumeQueue(topic, queueId);
    }

    private ConsumeQueueInterface doFindOrCreateConsumeQueue(String topic, int queueId) {

        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<Integer, ConsumeQueueInterface>(128);
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueueInterface logic = map.get(queueId);
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic;

        if (StoreUtil.isStreamMode(this.messageStore)) {
            newLogic = new BatchConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                    this.messageStore);
            ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        } else {
            newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                    (DefaultMessageStore) this.messageStore);
            ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }
}
