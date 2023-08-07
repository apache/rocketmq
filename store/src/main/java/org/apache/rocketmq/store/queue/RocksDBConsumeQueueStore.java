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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

public class RocksDBConsumeQueueStore extends AbstractConsumeQueueStore {
    private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    public static final byte CTRL_0 = '\u0000';
    public static final byte CTRL_1 = '\u0001';
    public static final byte CTRL_2 = '\u0002';

    private static final int BATCH_SIZE = 16;
    private static final int MAX_KEY_LEN = 300;

    private final ScheduledExecutorService scheduledExecutorService;
    private final String storePath;

    /**
     * we use two tables with different ColumnFamilyHandle, called RocksDBConsumeQueueTable and RocksDBConsumeQueueOffsetTable.
     * 1.RocksDBConsumeQueueTable uses to store CqUnit[phyOffset, msgSize, tagHashCode, msgStoreTime]
     * 2.RocksDBConsumeQueueOffsetTable uses to store phyOffset and consumeOffset(@see PhyAndCQOffset) of topic-queueId
     */
    private final ConsumeQueueRocksDBStorage rocksDBStorage;
    private final RocksDBConsumeQueueTable rocksDBConsumeQueueTable;
    private final RocksDBConsumeQueueOffsetTable rocksDBConsumeQueueOffsetTable;

    private final WriteBatch writeBatch;
    private final List<DispatchRequest> bufferDRList;
    private final List<Pair<ByteBuffer, ByteBuffer>> cqBBPairList;
    private final List<Pair<ByteBuffer, ByteBuffer>> offsetBBPairList;
    private final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap;
    private volatile boolean isCQError = false;

    public RocksDBConsumeQueueStore(DefaultMessageStore messageStore) {
        super(messageStore);

        this.storePath = StorePathConfigHelper.getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir());
        this.rocksDBStorage = new ConsumeQueueRocksDBStorage(messageStore, storePath, 4);
        this.rocksDBConsumeQueueTable = new RocksDBConsumeQueueTable(rocksDBStorage, messageStore);
        this.rocksDBConsumeQueueOffsetTable = new RocksDBConsumeQueueOffsetTable(rocksDBConsumeQueueTable, rocksDBStorage, messageStore);

        this.writeBatch = new WriteBatch();
        this.bufferDRList = new ArrayList(BATCH_SIZE);
        this.cqBBPairList = new ArrayList(BATCH_SIZE);
        this.offsetBBPairList = new ArrayList(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            ByteBuffer bbKey = ByteBuffer.allocateDirect(MAX_KEY_LEN);
            ByteBuffer bbValue = ByteBuffer.allocateDirect(RocksDBConsumeQueueTable.CQ_UNIT_SIZE);
            this.cqBBPairList.add(new Pair(bbKey, bbValue));

            ByteBuffer offsetKey = ByteBuffer.allocateDirect(MAX_KEY_LEN);
            ByteBuffer offsetValue = ByteBuffer.allocateDirect(RocksDBConsumeQueueOffsetTable.OFFSET_VALUE_LENGTH);
            this.offsetBBPairList.add(new Pair(offsetKey, offsetValue));
        }

        this.tempTopicQueueMaxOffsetMap = new HashMap<>();
        this.scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RocksDBConsumeQueueStoreScheduledThread", messageStore.getBrokerIdentity()));
    }

    @Override
    public void start() {
        log.info("RocksDB ConsumeQueueStore start!");
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            rocksDBStorage.statRocksdb(ROCKSDB_LOG);
        }, 10, this.messageStoreConfig.getStatConsumeQueueRocksDbIntervalSec(), TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            cleanDirty(messageStore.getTopicConfigs().keySet());
        }, 10, this.messageStoreConfig.getCleanDirtyConsumeQueueIntervalMin(), TimeUnit.MINUTES);
    }

    private void cleanDirty(final Set<String> existTopicSet) {
        try {
            Map<String, Set<Integer>> topicQueueIdToBeDeletedMap =
                this.rocksDBConsumeQueueOffsetTable.iterateOffsetTable2FindDirty(existTopicSet);

            for (Map.Entry<String, Set<Integer>> entry : topicQueueIdToBeDeletedMap.entrySet()) {
                String topic = entry.getKey();
                for (int queueId : entry.getValue()) {
                    destroy(new RocksDBConsumeQueue(topic, queueId));
                }
            }
        } catch (Exception e) {
            log.error("cleanUnusedTopic Failed.", e);
        }
    }

    @Override
    public boolean load() {
        boolean result = this.rocksDBStorage.start();
        this.rocksDBConsumeQueueTable.load();
        this.rocksDBConsumeQueueOffsetTable.load();
        log.info("load rocksdb consume queue {}.", result ? "OK" : "Failed");
        return result;
    }

    @Override
    public void recover() {
        // ignored
    }

    @Override
    public boolean recoverConcurrently() {
        return true;
    }

    @Override
    public boolean shutdown() {
        this.scheduledExecutorService.shutdown();
        return shutdownInner();
    }

    private boolean shutdownInner() {
        return this.rocksDBStorage.shutdown();
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException {
        if (request == null || this.bufferDRList.size() >= BATCH_SIZE) {
            putMessagePosition();
        }
        if (request != null) {
            this.bufferDRList.add(request);
        }
    }

    public void putMessagePosition() throws RocksDBException {
        final int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            if (putMessagePosition0()) {
                if (this.isCQError) {
                    this.messageStore.getRunningFlags().clearLogicsQueueError();
                    this.isCQError = false;
                }
                return;
            } else {
                ERROR_LOG.warn("{} put cq Failed. retryTime: {}", i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
        if (!this.isCQError) {
            ERROR_LOG.error("[BUG] put CQ Failed.");
            this.messageStore.getRunningFlags().makeLogicsQueueError();
            this.isCQError = true;
        }
        throw new RocksDBException("put CQ Failed");
    }

    private boolean putMessagePosition0() {
        if (!this.rocksDBStorage.hold()) {
            return false;
        }

        final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap = this.tempTopicQueueMaxOffsetMap;
        try {
            final List<DispatchRequest> bufferDRList = this.bufferDRList;
            final int size = bufferDRList.size();
            if (size == 0) {
                return true;
            }
            final List<Pair<ByteBuffer, ByteBuffer>> cqBBPairList = this.cqBBPairList;
            final List<Pair<ByteBuffer, ByteBuffer>> offsetBBPairList = this.offsetBBPairList;
            final WriteBatch writeBatch = this.writeBatch;

            long maxPhyOffset = 0;
            for (int i = size - 1; i >= 0; i--) {
                final DispatchRequest request = bufferDRList.get(i);
                final byte[] topicBytes = request.getTopic().getBytes(CHARSET_UTF8);

                final Pair<ByteBuffer, ByteBuffer> cqBBPair = cqBBPairList.get(i);
                final Pair<ByteBuffer, ByteBuffer> offsetBBPair = offsetBBPairList.get(i);

                this.rocksDBConsumeQueueTable.buildAndPutCQByteBuffer(cqBBPair, topicBytes, request, writeBatch);
                this.rocksDBConsumeQueueOffsetTable.updateTempTopicQueueMaxOffset(offsetBBPair, topicBytes, request,
                    tempTopicQueueMaxOffsetMap);

                final int msgSize = request.getMsgSize();
                final long phyOffset = request.getCommitLogOffset();
                if (phyOffset + msgSize >= maxPhyOffset) {
                    maxPhyOffset = phyOffset + msgSize;
                }
            }

            this.rocksDBConsumeQueueOffsetTable.putMaxOffset(tempTopicQueueMaxOffsetMap, writeBatch, maxPhyOffset);

            // clear writeBatch in batchPut
            this.rocksDBStorage.batchPut(writeBatch);

            // put max consumeQueueOffset info to heap, so we can get it from heap quickly
            this.rocksDBConsumeQueueOffsetTable.putHeapMaxCqOffset(tempTopicQueueMaxOffsetMap);

            long storeTimeStamp = bufferDRList.get(size - 1).getStoreTimestamp();
            if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimeStamp);
            }
            this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimeStamp);

            notifyMessageArriveAndClear();
            return true;
        } catch (Exception e) {
            ERROR_LOG.error("putMessagePosition0 Failed.", e);
            return false;
        } finally {
            tempTopicQueueMaxOffsetMap.clear();
            this.rocksDBStorage.release();
        }
    }

    private void notifyMessageArriveAndClear() {
        final List<DispatchRequest> bufferDRList = this.bufferDRList;
        try {
            if (this.messageStore.getBrokerConfig().isLongPollingEnable() && this.messageStore.getMessageArrivingListener() != null) {
                for (DispatchRequest dp : bufferDRList) {
                    this.messageStore.getMessageArrivingListener().arriving(
                        dp.getTopic(), dp.getQueueId(), dp.getConsumeQueueOffset() + 1, dp.getTagsCode(),
                        dp.getStoreTimestamp(), dp.getBitMap(), dp.getPropertiesMap());
                }
            }
        } catch (Exception e) {
            ERROR_LOG.error("notifyMessageArriveAndClear Failed.", e);
        } finally {
            bufferDRList.clear();
        }
    }

    @Override
    public List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException {
        return this.rocksDBConsumeQueueTable.rangeQuery(topic, queueId, startIndex, num);
    }

    @Override
    public ByteBuffer get(final String topic, final int queueId, final long cqOffset) throws RocksDBException {
        return this.rocksDBConsumeQueueTable.getCQInKV(topic, queueId, cqOffset);
    }

    @Override
    public void recoverOffsetTable(long minPhyOffset) {
        // ignored
    }

    @Override
    public void destroy() {
        try {
            shutdownInner();
            FileUtils.deleteDirectory(new File(this.storePath));
        } catch (Exception e) {
            ERROR_LOG.error("destroy cq Failed. {}", this.storePath, e);
        }
    }

    @Override
    public void destroy(ConsumeQueueInterface consumeQueue) throws RocksDBException {
        String topic = consumeQueue.getTopic();
        int queueId = consumeQueue.getQueueId();
        if (StringUtils.isEmpty(topic) || queueId < 0 || !this.rocksDBStorage.hold()) {
            return;
        }

        WriteBatch writeBatch = new WriteBatch();
        try {
            this.rocksDBConsumeQueueTable.destroyCQ(topic, queueId, writeBatch);
            this.rocksDBConsumeQueueOffsetTable.destroyOffset(topic, queueId, writeBatch);

            this.rocksDBStorage.batchPut(writeBatch);
        } catch (RocksDBException e) {
            ERROR_LOG.error("kv deleteTopic {} Failed.", topic, e);
            throw e;
        } finally {
            writeBatch.close();
            this.rocksDBStorage.release();
        }
    }

    @Override
    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        try {
            this.rocksDBStorage.flushWAL();
        } catch (Exception e) {
        }
        return true;
    }

    @Override
    public void checkSelf() {
        // ignored
    }

    @Override
    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        // ignored
        return 0;
    }

    /**
     * We do not need to truncate dirty CQ in RocksDBConsumeQueueTable,  Because dirty CQ in RocksDBConsumeQueueTable
     * will be rewritten by new KV when new messages are appended or will be cleaned up when topics are deleted.
     * But dirty offset info in RocksDBConsumeQueueOffsetTable must be truncated, because we use offset info in
     * RocksDBConsumeQueueOffsetTable to rebuild topicQueueTable(@see RocksDBConsumeQueue#increaseQueueOffset).
     * @param offsetToTruncate
     * @throws RocksDBException
     */
    @Override
    public void truncateDirty(long offsetToTruncate) throws RocksDBException {
        long cqMaxPhyOffset = getMaxPhyOffsetInConsumeQueue();
        if (offsetToTruncate >= cqMaxPhyOffset) {
            return;
        }

        this.rocksDBConsumeQueueOffsetTable.truncateDirty(offsetToTruncate);
    }

    @Override
    public void cleanExpired(final long phyOffset) {
        this.rocksDBStorage.manualCompaction(phyOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) throws RocksDBException {
        long offset = 0;
        final long minPhysicOffset = this.messageStore.getMinPhyOffset();
        long low = getMinOffsetInQueue(topic, queueId);
        Long high = this.rocksDBConsumeQueueOffsetTable.getMaxConsumeOffset(topic, queueId);
        if (high == null || high == -1) {
            return 0;
        }
        long targetOffset = -1L, leftOffset = -1L, rightOffset = -1L;
        long leftValue = -1L, rightValue = -1L;
        while (high >= low) {
            long midOffset = low + ((high - low) >>> 1);
            ByteBuffer byteBuffer = get(topic, queueId, midOffset);
            if (byteBuffer == null) {
                ERROR_LOG.warn("getOffsetInQueueByTime Failed. topic: {}, queueId: {}, timestamp: {}, result: null",
                    topic, queueId, timestamp);
                low = midOffset + 1;
                continue;
            }

            long phyOffset = byteBuffer.getLong(RocksDBConsumeQueueTable.PHY_OFFSET_OFFSET);
            if (phyOffset < minPhysicOffset) {
                low = midOffset + 1;
                leftOffset = midOffset;
                continue;
            }
            long storeTime = byteBuffer.getLong(RocksDBConsumeQueueTable.MSG_STORE_TIME_SIZE_OFFSET);
            if (storeTime < 0) {
                return 0;
            } else if (storeTime == timestamp) {
                targetOffset = midOffset;
                break;
            } else if (storeTime > timestamp) {
                high = midOffset - 1;
                rightOffset = midOffset;
                rightValue = storeTime;
            } else {
                low = midOffset + 1;
                leftOffset = midOffset;
                leftValue = storeTime;
            }
        }
        if (targetOffset != -1) {
            offset = targetOffset;
        } else {
            if (leftValue == -1) {
                offset = rightOffset;
            } else if (rightValue == -1) {
                offset = leftOffset;
            } else {
                offset = Math.abs(timestamp - leftValue) > Math.abs(timestamp - rightValue) ? rightOffset : leftOffset;
            }
        }
        return offset;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) throws RocksDBException {
        Long maxOffset = this.rocksDBConsumeQueueOffsetTable.getMaxConsumeOffset(topic, queueId);
        return (maxOffset != null) ? maxOffset + 1 : 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) throws RocksDBException {
        return this.rocksDBConsumeQueueOffsetTable.getMinConsumeOffset(topic, queueId);
    }

    @Override
    public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
        return this.rocksDBConsumeQueueOffsetTable.getMaxPhyOffset(topic, queueId);
    }

    @Override
    public long getMaxPhyOffsetInConsumeQueue() throws RocksDBException {
        return this.rocksDBConsumeQueueOffsetTable.getMaxPhyOffset();
    }

    @Override
    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<>(128);
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

        ConsumeQueueInterface newLogic = new RocksDBConsumeQueue(this.messageStore, topic, queueId);
        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);

        return oldLogic != null ? oldLogic : newLogic;
    }

    @Override
    public long rollNextFile(ConsumeQueueInterface consumeQueue, long offset) {
        return 0;
    }

    @Override
    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        return true;
    }

    @Override
    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        return true;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }
}
