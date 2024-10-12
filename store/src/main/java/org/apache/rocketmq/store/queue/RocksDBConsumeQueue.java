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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.rocksdb.RocksDBException;

public class RocksDBConsumeQueue implements ConsumeQueueInterface {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final MessageStore messageStore;
    private final String topic;
    private final int queueId;

    public RocksDBConsumeQueue(final MessageStore messageStore, final String topic, final int queueId) {
        this.messageStore = messageStore;
        this.topic = topic;
        this.queueId = queueId;
    }

    public RocksDBConsumeQueue(final String topic, final int queueId) {
        this.messageStore = null;
        this.topic = topic;
        this.queueId = queueId;
    }

    @Override
    public boolean load() {
        return true;
    }

    @Override
    public void recover() {
        // ignore
    }

    @Override
    public void checkSelf() {
        // ignore
    }

    @Override
    public boolean flush(final int flushLeastPages) {
        return true;
    }

    @Override
    public void destroy() {
        // ignore
    }

    @Override
    public void truncateDirtyLogicFiles(long maxCommitLogPos) {
        // ignored
    }

    @Override
    public int deleteExpiredFile(long minCommitLogPos) {
        return 0;
    }

    @Override
    public long rollNextFile(long nextBeginOffset) {
        return 0;
    }

    @Override
    public boolean isFirstFileAvailable() {
        return true;
    }

    @Override
    public boolean isFirstFileExist() {
        return true;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        // ignore
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        // ignore
    }

    @Override
    public long getMaxOffsetInQueue() {
        try {
            return this.messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
        } catch (RocksDBException e) {
            ERROR_LOG.error("getMaxOffsetInQueue Failed. topic: {}, queueId: {}", topic, queueId, e);
            return 0;
        }
    }

    @Override
    public long getMessageTotalInQueue() {
        try {
            long maxOffsetInQueue = this.messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
            long minOffsetInQueue = this.messageStore.getQueueStore().getMinOffsetInQueue(topic, queueId);
            return maxOffsetInQueue - minOffsetInQueue;
        } catch (RocksDBException e) {
            ERROR_LOG.error("getMessageTotalInQueue Failed. topic: {}, queueId: {}, {}", topic, queueId, e);
        }
        return -1;
    }

    /**
     * We already implement it in RocksDBConsumeQueueStore.
     * @see RocksDBConsumeQueueStore#getOffsetInQueueByTime
     * @param timestamp timestamp
     * @return
     */
    @Override
    public long getOffsetInQueueByTime(long timestamp) {
        return 0;
    }

    /**
     * We already implement it in RocksDBConsumeQueueStore.
     * @see RocksDBConsumeQueueStore#getOffsetInQueueByTime
     * @param timestamp    timestamp
     * @param boundaryType Lower or Upper
     * @return
     */
    @Override
    public long getOffsetInQueueByTime(long timestamp, BoundaryType boundaryType) {
        return 0;
    }

    @Override
    public long getMaxPhysicOffset() {
        Long maxPhyOffset = this.messageStore.getQueueStore().getMaxPhyOffsetInConsumeQueue(topic, queueId);
        return maxPhyOffset == null ? -1 : maxPhyOffset;
    }

    @Override
    public long getMinLogicOffset() {
        return 0;
    }

    @Override
    public CQType getCQType() {
        return CQType.RocksDBCQ;
    }

    @Override
    public long getTotalSize() {
        // ignored
        return 0;
    }

    @Override
    public int getUnitSize() {
        // attention: unitSize should equal to 'ConsumeQueue.CQ_STORE_UNIT_SIZE'
        return ConsumeQueue.CQ_STORE_UNIT_SIZE;
    }

    /**
     * Ignored, we already implement this method
     * @see org.apache.rocketmq.store.queue.RocksDBConsumeQueueOffsetTable#getMinCqOffset(String, int)
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {

    }

    /**
     * Ignored, in rocksdb mode, we build cq in RocksDBConsumeQueueStore
     * @see org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore#putMessagePosition()
     */
    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {

    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) throws RocksDBException {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        Long queueOffset = queueOffsetOperator.getTopicQueueNextOffset(topicQueueKey);
        if (queueOffset == null) {
            // we will recover topic queue table from rocksdb when we use it.
            queueOffset = this.messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
            queueOffsetOperator.updateQueueOffset(topicQueueKey, queueOffset);
        }
        msg.setQueueOffset(queueOffset);
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg, short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        // Check offset validity
        Pair<CqUnit, Long> fromUnit = getCqUnitAndStoreTime(from);
        Pair<CqUnit, Long> toUnit = getCqUnitAndStoreTime(from);
        if (toUnit == null || fromUnit == null) {
            return -1;
        }

        if (from >= to) {
            return -1;
        }

        if (to > getMaxOffsetInQueue()) {
            to = getMaxOffsetInQueue();
        }

        int maxSampleSize = messageStore.getMessageStoreConfig().getMaxConsumeQueueScan();
        int sampleSize = to - from > maxSampleSize ? maxSampleSize : (int) (to - from);

        int matchThreshold = messageStore.getMessageStoreConfig().getSampleCountThreshold();
        int matchSize = 0;

        for (int i = 0; i < sampleSize; i++) {
            long index = from + i;
            Pair<CqUnit, Long> pair = getCqUnitAndStoreTime(index);
            if (pair == null) {
                continue;
            }
            CqUnit cqUnit = pair.getObject1();
            if (filter.isMatchedByConsumeQueue(cqUnit.getTagsCode(), cqUnit.getCqExtUnit())) {
                matchSize++;
                // if matchSize is plenty, early exit estimate
                if (matchSize > matchThreshold) {
                    sampleSize = i;
                    break;
                }
            }
        }
        // Make sure the second half is a floating point number, otherwise it will be truncated to 0
        return sampleSize == 0 ? 0 : (long) ((to - from) * (matchSize / (sampleSize * 1.0)));
    }


    @Override
    public long getMinOffsetInQueue() {
        return this.messageStore.getMinOffsetInQueue(this.topic, this.queueId);
    }

    private int pullNum(long cqOffset, long maxCqOffset) {
        long diffLong = maxCqOffset - cqOffset;
        if (diffLong < Integer.MAX_VALUE) {
            int diffInt = (int) diffLong;
            return diffInt > 16 ? 16 : diffInt;
        }
        return 16;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(final long startIndex) {
        try {
            long maxCqOffset = getMaxOffsetInQueue();
            if (startIndex < maxCqOffset) {
                int num = pullNum(startIndex, maxCqOffset);
                return iterateFrom0(startIndex, num);
            }
        } catch (RocksDBException e) {
            log.error("[RocksDBConsumeQueue] iterateFrom error!", e);
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws RocksDBException {
        long maxCqOffset = getMaxOffsetInQueue();
        if (startIndex < maxCqOffset) {
            int num = Math.min((int)(maxCqOffset - startIndex), count);
            return iterateFrom0(startIndex, num);
        }
        return null;
    }

    @Override
    public CqUnit get(long index) {
        Pair<CqUnit, Long> pair = getCqUnitAndStoreTime(index);
        return pair == null ? null : pair.getObject1();
    }

    @Override
    public Pair<CqUnit, Long> getCqUnitAndStoreTime(long index) {
        ByteBuffer byteBuffer;
        try {
            byteBuffer = this.messageStore.getQueueStore().get(topic, queueId, index);
        } catch (RocksDBException e) {
            ERROR_LOG.error("getUnitAndStoreTime Failed. topic: {}, queueId: {}", topic, queueId, e);
            return null;
        }
        if (byteBuffer == null || byteBuffer.remaining() < RocksDBConsumeQueueTable.CQ_UNIT_SIZE) {
            return null;
        }
        long phyOffset = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        long tagCode = byteBuffer.getLong();
        long messageStoreTime = byteBuffer.getLong();
        return new Pair<>(new CqUnit(index, phyOffset, size, tagCode), messageStoreTime);
    }

    @Override
    public Pair<CqUnit, Long> getEarliestUnitAndStoreTime() {
        try {
            long minOffset = this.messageStore.getQueueStore().getMinOffsetInQueue(topic, queueId);
            return getCqUnitAndStoreTime(minOffset);
        } catch (RocksDBException e) {
            ERROR_LOG.error("getEarliestUnitAndStoreTime Failed. topic: {}, queueId: {}", topic, queueId, e);
        }
        return null;
    }

    @Override
    public CqUnit getEarliestUnit() {
        Pair<CqUnit, Long> pair = getEarliestUnitAndStoreTime();
        return pair == null ? null : pair.getObject1();
    }

    @Override
    public CqUnit getLatestUnit() {
        try {
            long maxOffset = this.messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
            return get(maxOffset > 0 ? maxOffset - 1 : maxOffset);
        } catch (RocksDBException e) {
            ERROR_LOG.error("getLatestUnit Failed. topic: {}, queueId: {}, {}", topic, queueId, e.getMessage());
        }
        return null;
    }

    @Override
    public long getLastOffset() {
        return getMaxPhysicOffset();
    }

    private ReferredIterator<CqUnit> iterateFrom0(final long startIndex, final int count) throws RocksDBException {
        List<ByteBuffer> byteBufferList = this.messageStore.getQueueStore().rangeQuery(topic, queueId, startIndex, count);
        if (byteBufferList == null || byteBufferList.isEmpty()) {
            if (this.messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                log.warn("iterateFrom0 - find nothing, startIndex:{}, count:{}", startIndex, count);
            }
            return null;
        }
        return new RocksDBConsumeQueueIterator(byteBufferList, startIndex);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    private class RocksDBConsumeQueueIterator implements ReferredIterator<CqUnit> {
        private final List<ByteBuffer> byteBufferList;
        private final long startIndex;
        private final int totalCount;
        private int currentIndex;

        public RocksDBConsumeQueueIterator(final List<ByteBuffer> byteBufferList, final long startIndex) {
            this.byteBufferList = byteBufferList;
            this.startIndex = startIndex;
            this.totalCount = byteBufferList.size();
            this.currentIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return this.currentIndex < this.totalCount;
        }

        @Override
        public CqUnit next() {
            if (!hasNext()) {
                return null;
            }
            final int currentIndex = this.currentIndex;
            final ByteBuffer byteBuffer = this.byteBufferList.get(currentIndex);
            CqUnit cqUnit = new CqUnit(this.startIndex + currentIndex, byteBuffer.getLong(), byteBuffer.getInt(), byteBuffer.getLong());
            this.currentIndex++;
            return cqUnit;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void release() {
        }

        @Override
        public CqUnit nextAndRelease() {
            try {
                return next();
            } finally {
                release();
            }
        }
    }
}
