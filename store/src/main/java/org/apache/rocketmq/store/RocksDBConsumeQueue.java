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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.QueueOffsetOperator;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore;
import org.apache.rocketmq.store.timer.TimerMessageStore;

import static org.apache.rocketmq.common.attribute.CQType.RocksDBCQ;

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
        } catch (Exception e) {
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
        } catch (Exception e) {
            ERROR_LOG.error("getMessageTotalInQueue Failed. topic: {}, queueId: {}, {}", topic, queueId, e);
        }
        return -1;
    }

    @Override
    public long getOffsetInQueueByTime(long timestamp) {
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
        return RocksDBCQ;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public int getUnitSize() {
        // attention: unitSize should equal to 'ConsumeQueue.CQ_STORE_UNIT_SIZE'
        return ConsumeQueue.CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        /**
         * Ignore, we already implement this method
         * @see RocksDBConsumeQueueStore#getMinOffsetInQueue()
         */
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        /**
         * Ignore, in rocksdb mode, we build cq in QueueStore
         * @see RocksDBConsumeQueueStore#putMessagePosition0()
         */
    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) throws Exception {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        Long queueOffset = queueOffsetOperator.getTopicQueueNextOffset(topicQueueKey);
        if (queueOffset == null) {
            // we will recover topic queue table from rocksdb when we use it
            queueOffset = getTopicQueueNextOffset(getTopic(), getQueueId());
            queueOffsetOperator.updateQueueOffset(topicQueueKey, queueOffset);
        }
        msg.setQueueOffset(queueOffset);


        // Handling the multi dispatch message. In the context of a light message queue (as defined in RIP-28),
        // light message queues are constructed based on message properties, which requires special handling of queue offset of the light message queue.
        if (!isNeedHandleMultiDispatch(msg)) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsets[i] = queueOffsetOperator.getLmqTopicQueueNextOffset(key);
            }
        }
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        msg.removeWaitStorePropertyString();
    }

    private long getTopicQueueNextOffset(String topic, int queueId) throws Exception {
        try {
            return this.messageStore.getQueueStore().getMaxOffsetInQueue(topic, queueId);
        } catch (Exception e) {
            ERROR_LOG.error("getTopicQueueNextOffset Failed. topic: {}, queueId: {}", topic, queueId, e);
            throw e;
        }
    }

    public boolean isNeedHandleMultiDispatch(MessageExtBrokerInner msg) {
        return messageStore.getMessageStoreConfig().isEnableMultiDispatch()
            && !msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !msg.getTopic().equals(TimerMessageStore.TIMER_TOPIC)
            && !msg.getTopic().equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }

    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg, short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);

        // Handling the multi dispatch message. In the context of a light message queue (as defined in RIP-28),
        // light message queues are constructed based on message properties, which requires special handling of queue offset of the light message queue.
        if (!isNeedHandleMultiDispatch(msg)) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsetOperator.increaseLmqOffset(key, (short) 1);
            }
        }
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        // todo
        return 0;
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
        } catch (Exception e) {
            log.error("[RocksDBConsumeQueue] iterateFrom error!", e);
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws Exception {
        long maxCqOffset = getMaxOffsetInQueue();
        if (startIndex < maxCqOffset) {
            int num = Math.min((int)(maxCqOffset - startIndex), count);
            return iterateFrom0(startIndex, num);
        }
        return null;
    }

    @Override
    public CqUnit get(long index) {
        Pair<CqUnit, Long> pair = getUnitAndStoreTime(index);
        return pair == null ? null : pair.getObject1();
    }

    @Override
    public Pair<CqUnit, Long> getUnitAndStoreTime(long index) {
        ByteBuffer byteBuffer;
        try {
            byteBuffer = this.messageStore.getQueueStore().get(topic, queueId, index);
        } catch (Exception e) {
            ERROR_LOG.error("getUnitAndStoreTime Failed. topic: {}, queueId: {}", topic, queueId, e);
            return null;
        }
        if (byteBuffer == null || byteBuffer.remaining() < RocksDBConsumeQueueStore.CQ_UNIT_SIZE) {
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
            return getUnitAndStoreTime(minOffset);
        } catch (Exception e) {
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
            return get(maxOffset);
        } catch (Exception e) {
            ERROR_LOG.error("getLatestUnit Failed. topic: {}, queueId: {}, {}", topic, queueId, e.getMessage());
        }
        return null;
    }

    @Override
    public long getLastOffset() {
        return getMaxPhysicOffset();
    }

    private ReferredIterator<CqUnit> iterateFrom0(final long startIndex, final int count) throws Exception {
        List<ByteBuffer> byteBufferList = this.messageStore.getQueueStore().rangeQuery(topic, queueId, startIndex, count);
        if (byteBufferList == null || byteBufferList.isEmpty()) {
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
