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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.FixedSizeCache;
import org.apache.rocketmq.store.RocksDBConsumeQueue;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

public class RocksDBConsumeQueueStore extends AbstractConsumeQueueStore {
    private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    private static final byte[] MAX_BYTES = "max".getBytes(CHARSET_UTF8);
    private static final byte[] MIN_BYTES = "min".getBytes(CHARSET_UTF8);
    private static final int BATCH_SIZE = 16;
    private static final byte CTRL_0 = '\u0000';
    private static final byte CTRL_A = '\u0001';
    private static final byte CTRL_2 = '\u0002';

    /**
     * Rocksdb ConsumeQueue's store unit. Format:
     *
     * <pre>
     * ┌─────────────────────────┬───────────┬───────────────────────┬───────────┬───────────┬───────────┬───────────────────────┐
     * │ Topic Bytes Array Size  │  CTRL_A   │   Topic Bytes Array   │  CTRL_A   │  QueueId  │  CTRL_A   │  ConsumeQueue Offset  │
     * │        (4 Bytes)        │ (1 Bytes) │       (n Bytes)       │ (1 Bytes) │ (4 Bytes) │ (1 Bytes) │     (8 Bytes)         │
     * ├─────────────────────────┴───────────┴───────────────────────┴───────────┴───────────┴───────────┴───────────────────────┤
     * │                                                    Key Unit                                                             │
     * │                                                                                                                         │
     * </pre>
     *
     * <pre>
     * ┌─────────────────────────────┬───────────────────┬──────────────────┬──────────────────┬───────────────────────┐
     * │  CommitLog Physical Offset  │      Body Size    │   Tag HashCode   │  Msg Store Time  │  ConsumeQueue Offset  │
     * │        (8 Bytes)            │      (4 Bytes)    │    (8 Bytes)     │    (8 Bytes)     │      (8 Bytes)        │
     * ├─────────────────────────────┴───────────────────┴──────────────────┴──────────────────┴───────────────────────┤
     * │                                                    Value Unit                                                 │
     * │                                                                                                               │
     * </pre>
     * ConsumeQueue's store unit. Size:
     * CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) + Msg Store Time(8) + ConsumeQueue Offset(8) =  36 Bytes
     */
    public static final int PHY_OFFSET_OFFSET = 0;
    public static final int PHY_MSG_LEN_OFFSET = 8;
    public static final int MSG_TAG_HASHCODE_OFFSET = 12;
    public static final int MSG_STORE_TIME_SIZE_OFFSET = 20;
    public static final int CQ_OFFSET_OFFSET = 28;
    public static final int CQ_UNIT_SIZE = 36;

    private static final int MAX_KEY_LEN = 300;
    private static final int MAX_VALUE_LEN = CQ_UNIT_SIZE;

    /**
     * Rocksdb ConsumeQueue's Offset unit. Format:
     *
     * <pre>
     * ┌─────────────────────────┬───────────┬───────────────────────┬───────────┬───────────┬───────────┬─────────────┐
     * │ Topic Bytes Array Size  │  CTRL_A   │   Topic Bytes Array   │  CTRL_A   │  Max(min) │  CTRL_A   │   QueueId   │
     * │        (4 Bytes)        │ (1 Bytes) │       (n Bytes)       │ (1 Bytes) │ (3 Bytes) │ (1 Bytes) │  (4 Bytes)  │
     * ├─────────────────────────┴───────────┴───────────────────────┴───────────┴───────────┴───────────┴─────────────┤
     * │                                                    Key Unit                                                   │
     * │                                                                                                               │
     * </pre>
     *
     * <pre>
     * ┌─────────────────────────────┬───────────┬────────────────────────┐
     * │  CommitLog Physical Offset  │  CTRL_A   │   ConsumeQueue Offset  │
     * │        (8 Bytes)            │ (1 Bytes) │    (8 Bytes)           │
     * ├─────────────────────────────┴───────────┴────────────────────────┤
     * │                     Value Unit                                   │
     * │                                                                  │
     * </pre>
     * ConsumeQueue's Offset unit. Size: CommitLog Physical Offset(8) + CTRL_A(1) + ConsumeQueue Offset(8) =  17 Bytes
     */
    private static final int OFFSET_PHY_OFFSET = 0;
    private static final int OFFSET_CQ_OFFSET = 9;

    private final ScheduledExecutorService scheduledExecutorService;

    private final String storePath;
    private final ConsumeQueueRocksDBStorage rocksDBStorage;
    private final FlushOptions flushOptions;
    private final WriteBatch writeBatch;
    private final List<DispatchRequest> bufferDRList;
    private final List<Pair<ByteBuffer, ByteBuffer>> cqBBPairList;
    private final List<Pair<ByteBuffer, ByteBuffer>> offsetBBPairList;

    private final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap;

    private final Map<String/* topic-queueId */, PhyAndCQOffset> topicQueueMinCqOffset;
    private final Map<String/* topic-queueId */, Long> topicQueueMaxCqOffset;
    private final ByteBuffer maxPhyOffsetBB;

    private static final ByteBuffer INNER_CHECKPOINT_TOPIC;
    private static final int INNER_CHECKPOINT_TOPIC_LEN;
    static {
        byte[] topicBytes = "CHECKPOINT_TOPIC".getBytes(CHARSET_UTF8);
        INNER_CHECKPOINT_TOPIC_LEN = 14 + topicBytes.length;
        INNER_CHECKPOINT_TOPIC = ByteBuffer.allocateDirect(INNER_CHECKPOINT_TOPIC_LEN);
        buildOffsetKeyBB0(INNER_CHECKPOINT_TOPIC, topicBytes, 0, true);
    }
    private volatile boolean isCQError = false;

    private FixedSizeCache<TopicQueueOffset, ByteBuffer> consumeQueueCache;

    public RocksDBConsumeQueueStore(DefaultMessageStore messageStore) {
        super(messageStore);

        this.storePath = StorePathConfigHelper.getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir());
        this.rocksDBStorage = new ConsumeQueueRocksDBStorage(messageStore, storePath, 4);
        this.flushOptions = new FlushOptions();
        this.flushOptions.setWaitForFlush(true);
        this.flushOptions.setAllowWriteStall(false);

        this.writeBatch = new WriteBatch();
        this.bufferDRList = new ArrayList(BATCH_SIZE);
        this.cqBBPairList = new ArrayList(BATCH_SIZE);
        this.offsetBBPairList = new ArrayList(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            ByteBuffer bbKey = ByteBuffer.allocateDirect(MAX_KEY_LEN);
            ByteBuffer bbValue = ByteBuffer.allocateDirect(MAX_VALUE_LEN);
            this.cqBBPairList.add(new Pair(bbKey, bbValue));

            ByteBuffer offsetKey = ByteBuffer.allocateDirect(MAX_KEY_LEN);
            ByteBuffer offsetValue = ByteBuffer.allocateDirect(17);
            this.offsetBBPairList.add(new Pair(offsetKey, offsetValue));
        }
        this.tempTopicQueueMaxOffsetMap = new HashMap<>();
        this.maxPhyOffsetBB = ByteBuffer.allocateDirect(8);

        this.topicQueueMinCqOffset = new ConcurrentHashMap(1024);
        this.topicQueueMaxCqOffset = new ConcurrentHashMap(1024);

        this.consumeQueueCache = new FixedSizeCache(1000000);

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
            cleanTopic(messageStore.getTopicConfigs().keySet());
        }, 10, this.messageStoreConfig.getCleanDirtyConsumeQueueIntervalMin(), TimeUnit.MINUTES);
    }

    @Override
    public boolean load() {
        boolean result = this.rocksDBStorage.start();
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
        this.consumeQueueCache.shutdown();
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
            final ColumnFamilyHandle defaultCFH = this.rocksDBStorage.getDefaultCFHandle();
            final ColumnFamilyHandle offsetCFH = this.rocksDBStorage.getOffsetCFHandle();

            long maxPhyOffset = 0;
            for (int i = size - 1; i >= 0; i--) {
                final DispatchRequest request = bufferDRList.get(i);
                final Pair<ByteBuffer, ByteBuffer> cqBBPair = cqBBPairList.get(i);
                final ByteBuffer cqKey = cqBBPair.getObject1();
                final int msgSize = request.getMsgSize();

                final byte[] topicBytes = request.getTopic().getBytes(CHARSET_UTF8);
                buildCQKeyBB(cqKey, topicBytes, request.getQueueId(), request.getConsumeQueueOffset());

                final ByteBuffer cqValue = cqBBPair.getObject2();
                final long phyOffset = request.getCommitLogOffset();
                buildCQValueBB(cqValue, phyOffset, msgSize, request.getTagsCode(),
                    request.getStoreTimestamp(), request.getConsumeQueueOffset());

                writeBatch.put(defaultCFH, cqKey, cqValue);

                final Pair<ByteBuffer, ByteBuffer> offsetBBPair = offsetBBPairList.get(i);
                final ByteBuffer offsetKey = offsetBBPair.getObject1();
                buildOffsetKeyBB(offsetKey, topicBytes, request.getQueueId(), true);

                final ByteBuffer offsetValue = offsetBBPair.getObject2();
                buildOffsetValueBB(offsetValue, phyOffset, request.getConsumeQueueOffset());

                updateTempTopicQueueMaxOffset(offsetKey, offsetValue, request, tempTopicQueueMaxOffsetMap);

                if (phyOffset + msgSize >= maxPhyOffset) {
                    maxPhyOffset = phyOffset + msgSize;
                }
            }

            for (Map.Entry<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> entry : tempTopicQueueMaxOffsetMap.entrySet()) {
                writeBatch.put(offsetCFH, entry.getKey(), entry.getValue().getObject1());
            }

            appendMaxPhyOffset(writeBatch, offsetCFH, maxPhyOffset);

            // clear writeBatch in batchPut
            this.rocksDBStorage.batchPut(writeBatch);

            updateTopicQueueMaxCqOffset(tempTopicQueueMaxOffsetMap);

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

    private void putCache(final String topic, final int queueId, final long cqOffset, final long phyOffset,
        final int msgSize, final long tagsCode, final long storeTimeStamp) {
        TopicQueueOffset key = new TopicQueueOffset(topic, queueId, cqOffset);
        final ByteBuffer value = buildCQValueBB(phyOffset, msgSize, tagsCode, storeTimeStamp, cqOffset);
        this.consumeQueueCache.putIfAbsent(key, value);
    }

    private ByteBuffer getCache(final String topic, final int queueId, final long cqOffset) {
        ByteBuffer valueBB = null;
        if (this.messageStore.getMessageStoreConfig().isEnableCacheForRocksDBStore()) {
            TopicQueueOffset key = new TopicQueueOffset(topic, queueId, cqOffset);
            ByteBuffer bb = this.consumeQueueCache.get(key);
            if (bb != null) {
                ByteBuffer newBB = bb.duplicate();
                newBB.position(0).limit(bb.capacity());
                valueBB = newBB;
            }
        }
        return valueBB;
    }

    private void appendMaxPhyOffset(final WriteBatch writeBatch, final ColumnFamilyHandle cfh,
        final long maxPhyOffset) throws RocksDBException {
        final ByteBuffer maxPhyOffsetBB = this.maxPhyOffsetBB;
        maxPhyOffsetBB.position(0).limit(8);
        maxPhyOffsetBB.putLong(maxPhyOffset);
        maxPhyOffsetBB.flip();

        INNER_CHECKPOINT_TOPIC.position(0).limit(30);
        writeBatch.put(cfh, INNER_CHECKPOINT_TOPIC, maxPhyOffsetBB);
    }

    public List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
        final List<ColumnFamilyHandle> defaultCFHList = new ArrayList(num);
        final ColumnFamilyHandle defaultCFH = this.rocksDBStorage.getDefaultCFHandle();
        final ByteBuffer[] resultList = new ByteBuffer[num];
        final List<Integer> kvIndexList = new ArrayList(num);
        final List<byte[]> kvKeyList = new ArrayList(num);
        for (int i = 0; i < num; i++) {
            ByteBuffer valueBB = null;
            if ((valueBB = getCache(topic, queueId, startIndex + i)) != null) {
                resultList[i] = valueBB;
            } else {
                final ByteBuffer keyBB = buildCQKeyBB(topicBytes, queueId, startIndex + i);
                kvIndexList.add(i);
                kvKeyList.add(keyBB.array());
                defaultCFHList.add(defaultCFH);
            }
        }
        int keyNum = kvIndexList.size();
        if (keyNum > 0) {
            List<byte[]> kvValueList = this.rocksDBStorage.multiGet(defaultCFHList, kvKeyList);
            final int valueNum = kvValueList.size();
            if (keyNum != valueNum) {
                throw new RocksDBException("rocksdb bug, multiGet");
            }
            for (int i = 0; i < valueNum; i++) {
                byte[] value = kvValueList.get(i);
                if (value == null) {
                    continue;
                }
                ByteBuffer byteBuffer = ByteBuffer.wrap(value);
                resultList[kvIndexList.get(i)] = byteBuffer;
            }
        }

        final int resultSize = resultList.length;
        List<ByteBuffer> bbValueList = new ArrayList(resultSize);
        long preQueueOffset = 0;
        for (int i = 0; i < resultSize; i++) {
            ByteBuffer byteBuffer = resultList[i];
            if (byteBuffer == null) {
                break;
            }
            long queueOffset = byteBuffer.getLong(CQ_OFFSET_OFFSET);
            if (i > 0 && queueOffset != preQueueOffset + 1) {
                throw new RocksDBException("rocksdb bug, data damaged");
            }
            preQueueOffset = queueOffset;
            bbValueList.add(byteBuffer);
        }
        return bbValueList;
    }

    @Override
    public ByteBuffer get(final String topic, final int queueId, final long cqOffset) throws RocksDBException {
        final ByteBuffer valueBB = getCache(topic, queueId, cqOffset);
        if (valueBB != null) {
            return valueBB;
        }
        final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
        final ByteBuffer keyBB = buildCQKeyBB(topicBytes, queueId, cqOffset);
        byte[] value = this.rocksDBStorage.getCQ(keyBB.array());
        return (value != null) ? ByteBuffer.wrap(value) : null;
    }

    @Override
    public void recoverOffsetTable(long minPhyOffset) {

    }

    private ByteBuffer buildCQKeyBB(final byte[] topicBytes, final int queueId, final long cqOffset) {
        final ByteBuffer bb = ByteBuffer.allocate(19 + topicBytes.length);
        buildCQKeyBB0(bb, topicBytes, queueId, cqOffset);
        return bb;
    }

    private void buildCQKeyBB(final ByteBuffer bb, final byte[] topicBytes,
        final int queueId, final long cqOffset) {
        bb.position(0).limit(19 + topicBytes.length);
        buildCQKeyBB0(bb, topicBytes, queueId, cqOffset);
    }

    private void buildCQKeyBB0(final ByteBuffer bb, final byte[] topicBytes,
        final int queueId, final long cqOffset) {
        bb.putInt(topicBytes.length).put(CTRL_A).put(topicBytes).put(CTRL_A).putInt(queueId).put(CTRL_A).putLong(cqOffset);
        bb.flip();
    }

    private void buildCQValueBB(final ByteBuffer bb, final long phyOffset, final int msgSize,
        final long tagsCode, final long storeTimestamp, final long cqOffset) {
        bb.position(0).limit(MAX_VALUE_LEN);
        buildCQValueBB0(bb, phyOffset, msgSize, tagsCode, storeTimestamp, cqOffset);
    }

    private ByteBuffer buildCQValueBB(final long phyOffset, final int msgSize,
        final long tagsCode, final long storeTimestamp, final long cqOffset) {
        final ByteBuffer bb = ByteBuffer.allocate(MAX_VALUE_LEN);
        buildCQValueBB0(bb, phyOffset, msgSize, tagsCode, storeTimestamp, cqOffset);
        return bb;
    }

    private void buildCQValueBB0(final ByteBuffer bb, final long phyOffset, final int msgSize,
        final long tagsCode, final long storeTimestamp, final long cqOffset) {
        bb.putLong(phyOffset).putInt(msgSize).putLong(tagsCode).putLong(storeTimestamp).putLong(cqOffset);
        bb.flip();
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
            final ColumnFamilyHandle defaultCFH = this.rocksDBStorage.getDefaultCFHandle();
            final ColumnFamilyHandle offsetCFH = this.rocksDBStorage.getOffsetCFHandle();
            final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
            final ByteBuffer minOffsetKey = buildOffsetKeyBB(topicBytes, queueId, false);
            byte[] minOffsetBytes = this.rocksDBStorage.getOffset(minOffsetKey.array());
            Long startCQOffset = (minOffsetBytes != null) ? ByteBuffer.wrap(minOffsetBytes).getLong(OFFSET_CQ_OFFSET) : null;

            final ByteBuffer maxOffsetKey = buildOffsetKeyBB(topicBytes, queueId, true);
            byte[] maxOffsetBytes = this.rocksDBStorage.getOffset(maxOffsetKey.array());
            Long endCQOffset = (maxOffsetBytes != null) ? ByteBuffer.wrap(maxOffsetBytes).getLong(OFFSET_CQ_OFFSET) : null;

            final ByteBuffer cqStartKey = buildDeleteCQKey(true, topicBytes, queueId);
            final ByteBuffer cqEndKey = buildDeleteCQKey(false, topicBytes, queueId);

            writeBatch.deleteRange(defaultCFH, cqStartKey.array(), cqEndKey.array());
            writeBatch.delete(offsetCFH, minOffsetKey.array());
            writeBatch.delete(offsetCFH, maxOffsetKey.array());

            log.info("kv delete topic. {}, {}, minOffset: {}, maxOffset: {}", topic, queueId, startCQOffset, endCQOffset);

            this.rocksDBStorage.batchPut(writeBatch);

            String topicQueueId = buildTopicQueueId(topic, queueId);
            removeHeapMinCqOffset(topicQueueId);
            removeHeapMaxCqOffset(topicQueueId);
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
        return true;
    }

    @Override
    public void checkSelf() {
        // ignored
    }

    @Override
    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        return 0;
    }

    @Override
    public void truncateDirty(long offsetToTruncate) throws RocksDBException {
        long cqMaxPhyOffset = getMaxOffsetInConsumeQueue();
        if (offsetToTruncate >= cqMaxPhyOffset) {
            return;
        }

        // consume queue correct maxPhyOffset
        correctMaxPyhOffset(offsetToTruncate);

        ConcurrentMap<String, TopicConfig> allTopicConfigMap = this.messageStore.getTopicConfigs();
        if (allTopicConfigMap == null) {
            return;
        }
        for (TopicConfig topicConfig : allTopicConfigMap.values()) {
            for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                truncateDirtyLogicFile(topicConfig.getTopicName(), i);
            }
        }
    }

    public void correctMaxPyhOffset(long maxPhyOffset) throws RocksDBException {
        if (!this.rocksDBStorage.hold()) {
            return;
        }
        try {
            long oldMaxPhyOffset = getMaxOffsetInConsumeQueue();
            if (oldMaxPhyOffset <= maxPhyOffset) {
                return;
            }
            log.info("correctMaxPyhOffset, oldMaxPhyOffset={}, newMaxPhyOffset={}", oldMaxPhyOffset, maxPhyOffset);
            final ColumnFamilyHandle offsetCFH = this.rocksDBStorage.getOffsetCFHandle();
            appendMaxPhyOffset(writeBatch, offsetCFH, maxPhyOffset);
            this.rocksDBStorage.batchPut(writeBatch);
        } catch (RocksDBException e) {
            ERROR_LOG.error("correctMaxPyhOffset Failed.", e);
            throw e;
        } finally {
            this.rocksDBStorage.release();
        }
    }

    public void correctQueueMaxOffsetMap(List<DispatchRequest> dispatchRequestList) throws RocksDBException {
        if (!this.rocksDBStorage.hold()) {
            return;
        }

        final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap = this.tempTopicQueueMaxOffsetMap;
        try {
            final int size = dispatchRequestList.size();
            if (size <= 0) {
                return;
            }
            final List<Pair<ByteBuffer, ByteBuffer>> offsetBBPairList = this.offsetBBPairList;
            final WriteBatch writeBatch = this.writeBatch;
            final ColumnFamilyHandle offsetCFH = this.rocksDBStorage.getOffsetCFHandle();

            for (int i = size - 1; i >= 0; i--) {
                final DispatchRequest request = dispatchRequestList.get(i);
                final Pair<ByteBuffer, ByteBuffer> offsetBBPair = offsetBBPairList.get(i);

                final byte[] topicBytes = request.getTopic().getBytes(CHARSET_UTF8);
                final long phyOffset = request.getCommitLogOffset();
                final long cqOffset = request.getConsumeQueueOffset();

                final ByteBuffer offsetKey = offsetBBPair.getObject1();
                buildOffsetKeyBB(offsetKey, topicBytes, request.getQueueId(), true);

                final ByteBuffer offsetValue = offsetBBPair.getObject2();
                buildOffsetValueBB(offsetValue, phyOffset, cqOffset);

                updateTempTopicQueueMaxOffset(offsetKey, offsetValue, request, tempTopicQueueMaxOffsetMap);
            }

            for (Map.Entry<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> entry : tempTopicQueueMaxOffsetMap.entrySet()) {
                writeBatch.put(offsetCFH, entry.getKey(), entry.getValue().getObject1());
            }

            this.rocksDBStorage.batchPut(writeBatch);
            updateTopicQueueMaxCqOffset(tempTopicQueueMaxOffsetMap);
        } catch (RocksDBException e) {
            ERROR_LOG.error("correctQueueMaxOffsetMap Failed.", e);
            throw e;
        } finally {
            tempTopicQueueMaxOffsetMap.clear();
            this.rocksDBStorage.release();
        }
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
        Long high = getMaxOffsetInQueue0(topic, queueId);
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

            long phyOffset = byteBuffer.getLong(PHY_OFFSET_OFFSET);
            if (phyOffset < minPhysicOffset) {
                low = midOffset + 1;
                leftOffset = midOffset;
                continue;
            }
            long storeTime = byteBuffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
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

    private byte[] getOffsetInKV(String topic, int queueId, boolean max) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
        final ByteBuffer keyBB = buildOffsetKeyBB(topicBytes, queueId, max);

        return this.rocksDBStorage.getOffset(keyBB.array());
    }

    private static ByteBuffer buildOffsetKeyBB(final byte[] topicBytes, final int queueId, final boolean max) {
        ByteBuffer bb = ByteBuffer.allocate(14 + topicBytes.length);
        buildOffsetKeyBB0(bb, topicBytes, queueId, max);
        return bb;
    }

    private static void buildOffsetKeyBB(final ByteBuffer bb, final byte[] topicBytes,
        final int queueId, final boolean max) {
        bb.position(0).limit(14 + topicBytes.length);
        buildOffsetKeyBB0(bb, topicBytes, queueId, max);
    }

    private static void buildOffsetKeyBB0(final ByteBuffer bb, final byte[] topicBytes,
        final int queueId, final boolean max) {
        bb.putInt(topicBytes.length).put(CTRL_A).put(topicBytes).put(CTRL_A);
        if (max) {
            bb.put(MAX_BYTES);
        } else {
            bb.put(MIN_BYTES);
        }
        bb.put(CTRL_A).putInt(queueId);
        bb.flip();
    }

    private void buildOffsetValueBB(final ByteBuffer bb, final long phyOffset, final long cqOffset) {
        bb.position(0).limit(17);
        buildOffsetValueBB0(bb, phyOffset, cqOffset);
    }

    private ByteBuffer buildOffsetValueBB(final long phyOffset, final long cqOffset) {
        final ByteBuffer bb = ByteBuffer.allocate(17);
        buildOffsetValueBB0(bb, phyOffset, cqOffset);
        return bb;
    }

    private void buildOffsetValueBB0(final ByteBuffer bb, final long phyOffset, final long cqOffset) {
        bb.putLong(phyOffset).put(CTRL_A).putLong(cqOffset);
        bb.flip();
    }

    private void truncateDirtyLogicFile(String topic, int queueId) throws RocksDBException {
        final ByteBuffer byteBuffer = getMaxCQInKV(topic, queueId);
        if (byteBuffer == null) {
            return;
        }

        long maxPhyOffset = byteBuffer.getLong(OFFSET_PHY_OFFSET);
        long maxCqOffset = byteBuffer.getLong(OFFSET_CQ_OFFSET);
        long maxPhyOffsetInCQ = getMaxOffsetInConsumeQueue();

        if (maxPhyOffset >= maxPhyOffsetInCQ) {
            correctMaxCQOffset(topic, queueId, maxCqOffset, maxPhyOffsetInCQ);
            Long newMaxCqOffset = getHeapMaxCqOffset(topic, queueId);
            ROCKSDB_LOG.warn("truncateDirtyLogicFile topic={}, queueId={} from {} to {}", topic, queueId, maxPhyOffset, newMaxCqOffset);
        }
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) throws RocksDBException {
        Long maxOffset = getMaxOffsetInQueue0(topic, queueId);
        return (maxOffset != null) ? maxOffset + 1 : 0;
    }

    private Long getMaxOffsetInQueue0(String topic, int queueId) throws RocksDBException {
        Long maxCqOffset = getHeapMaxCqOffset(topic, queueId);

        if (maxCqOffset == null) {
            final ByteBuffer bb = getMaxCQInKV(topic, queueId);
            maxCqOffset = (bb != null) ? bb.getLong(OFFSET_CQ_OFFSET) : null;
            String topicQueueId = buildTopicQueueId(topic, queueId);
            this.topicQueueMaxCqOffset.putIfAbsent(topicQueueId, maxCqOffset != null ? maxCqOffset : -1L);
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateMaxOffsetInQueue. {}, {}", topicQueueId, maxCqOffset);
            }
        }

        return maxCqOffset;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) throws RocksDBException {
        final long minPhyOffset = this.messageStore.getMinPhyOffset();
        Pair<Boolean, Long> pair = isMinOffsetOk(topic, queueId, minPhyOffset);
        final long cqOffset = pair.getObject2();
        if (!pair.getObject1() && correctMinCQOffset(topic, queueId, cqOffset, minPhyOffset)) {
            PhyAndCQOffset phyAndCQOffset = getHeapMinCqOffset(topic, queueId);
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                String topicQueueId = buildTopicQueueId(topic, queueId);
                ROCKSDB_LOG.warn("getMinOffsetInQueue miss heap. {}, old: {}, new: {}", topicQueueId, cqOffset, phyAndCQOffset);
            }
            if (phyAndCQOffset != null) {
                return phyAndCQOffset.getCqOffset();
            }
        }
        return cqOffset;
    }

    private Pair<Boolean, Long> isMinOffsetOk(final String topic, final int queueId, final long minPhyOffset) throws RocksDBException {
        PhyAndCQOffset phyAndCQOffset = getHeapMinCqOffset(topic, queueId);
        if (phyAndCQOffset != null) {
            final long phyOffset = phyAndCQOffset.getPhyOffset();
            final long cqOffset = phyAndCQOffset.getCqOffset();

            return (phyOffset >= minPhyOffset) ? new Pair(true, cqOffset) : new Pair(false, cqOffset);
        }
        byte[] value = getOffsetInKV(topic, queueId, false);
        if (value == null) {
            return new Pair(false, 0L);
        }
        ByteBuffer bb = ByteBuffer.wrap(value);
        final long phyOffset = bb.getLong(OFFSET_PHY_OFFSET);
        final long cqOffset = bb.getLong(OFFSET_CQ_OFFSET);
        if (phyOffset >= minPhyOffset) {
            String topicQueueId = buildTopicQueueId(topic, queueId);
            PhyAndCQOffset kvPhyAndCQOffset = new PhyAndCQOffset(phyOffset, cqOffset);
            this.topicQueueMinCqOffset.putIfAbsent(topicQueueId, kvPhyAndCQOffset);
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateMinOffsetInQueue. {}, {}", topicQueueId, kvPhyAndCQOffset);
            }
            return new Pair(true, cqOffset);
        }
        return new Pair(false, cqOffset);
    }

    private boolean correctMaxCQOffset(final String topic, final int queueId, final long maxCQOffset,
        final long maxPhyOffsetInCQ) throws RocksDBException {
        // 'getMinOffsetInQueue' may correct minCqOffset and put it into heap
        long minCQOffset = getMinOffsetInQueue(topic, queueId);
        PhyAndCQOffset minPhyAndCQOffset = getHeapMinCqOffset(topic, queueId);
        if (minPhyAndCQOffset == null || minPhyAndCQOffset.getCqOffset() != minCQOffset || minPhyAndCQOffset.getPhyOffset() > maxPhyOffsetInCQ) {
            ROCKSDB_LOG.info("[BUG] correctMaxCqOffset error! topic={}, queueId={}, maxPhyOffsetInCQ={}, minCqOffset={}, phyAndCQOffset={}",
                topic, queueId, maxPhyOffsetInCQ, minCQOffset, minPhyAndCQOffset);
            throw new RocksDBException("correctMaxCQOffset error");
        }

        long high = maxCQOffset;
        long low = minCQOffset;
        PhyAndCQOffset targetPhyAndCQOffset = binarySearchInCQ(topic, queueId, high, low, maxPhyOffsetInCQ, false);

        long targetCQOffset = targetPhyAndCQOffset.getCqOffset();
        long targetPhyOffset = targetPhyAndCQOffset.getPhyOffset();

        if (targetCQOffset == -1) {
            if (maxCQOffset != minCQOffset) {
                updateTopicQueueMaxOffset(topic, queueId, minPhyAndCQOffset.getPhyOffset(), minCQOffset);
            }
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("correct error. {}, {}, {}, {}, {}", topic, queueId, minCQOffset, maxCQOffset, minPhyAndCQOffset.getPhyOffset());
            }
            return false;
        } else {
            updateTopicQueueMaxOffset(topic, queueId, targetPhyOffset, targetCQOffset);
            return true;
        }
    }

    private boolean correctMinCQOffset(final String topic, final int queueId,
        final long minCQOffset, final long minPhyOffset) throws RocksDBException {
        final ByteBuffer maxBB = getMaxCQInKV(topic, queueId);
        if (maxBB == null) {
            updateTopicQueueMinOffset(topic, queueId, minPhyOffset, 0L);
            return true;
        }
        final long maxPhyOffset = maxBB.getLong(OFFSET_PHY_OFFSET);
        final long maxCQOffset = maxBB.getLong(OFFSET_CQ_OFFSET);

        if (maxPhyOffset < minPhyOffset) {
            updateTopicQueueMinOffset(topic, queueId, minPhyOffset, maxCQOffset + 1);
            return true;
        }

        long high = maxCQOffset;
        long low = minCQOffset;
        PhyAndCQOffset phyAndCQOffset = binarySearchInCQ(topic, queueId, high, low, minPhyOffset, true);
        long targetCQOffset = phyAndCQOffset.getCqOffset();
        long targetPhyOffset = phyAndCQOffset.getPhyOffset();

        if (targetCQOffset == -1) {
            if (maxCQOffset != minCQOffset) {
                updateTopicQueueMinOffset(topic, queueId, maxPhyOffset, maxCQOffset);
            }
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("correct error. {}, {}, {}, {}, {}", topic, queueId, minCQOffset, maxCQOffset, minPhyOffset);
            }
            return false;
        } else {
            updateTopicQueueMinOffset(topic, queueId, targetPhyOffset, targetCQOffset);
            return true;
        }
    }

    private PhyAndCQOffset binarySearchInCQ(String topic, int queueId, long high, long low, long targetPhyOffset, boolean min) throws RocksDBException {
        long resultCQOffset = -1L;
        long resultPhyOffset = -1L;
        while (high >= low) {
            long midOffset = low + ((high - low) >>> 1);
            ByteBuffer byteBuffer = get(topic, queueId, midOffset);
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("binarySearchInCQ. {}, {}, {}, {}, {}", topic, queueId, midOffset, low, high);
            }
            if (byteBuffer == null) {
                low = midOffset + 1;
                continue;
            }

            final long phyOffset = byteBuffer.getLong(PHY_OFFSET_OFFSET);
            if (phyOffset == targetPhyOffset) {
                if (min) {
                    resultCQOffset =  midOffset;
                    resultPhyOffset = phyOffset;
                }
                break;
            } else if (phyOffset > targetPhyOffset) {
                high = midOffset - 1;
                if (min) {
                    resultCQOffset = midOffset;
                    resultPhyOffset = phyOffset;
                }
            } else {
                low = midOffset + 1;
                if (!min) {
                    resultCQOffset = midOffset;
                    resultPhyOffset = phyOffset;
                }
            }
        }
        return new PhyAndCQOffset(resultPhyOffset, resultCQOffset);
    }

    private void updateTopicQueueMinOffset(final String topic, final int queueId,
        final long minPhyOffset, final long minCQOffset) throws RocksDBException {
        if (!this.rocksDBStorage.hold()) {
            return;
        }
        WriteBatch writeBatch = new WriteBatch();
        try {
            final ColumnFamilyHandle offsetCFH = this.rocksDBStorage.getOffsetCFHandle();

            final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
            final ByteBuffer offsetKey = buildOffsetKeyBB(topicBytes, queueId, false);

            final ByteBuffer offsetValue = buildOffsetValueBB(minPhyOffset, minCQOffset);
            writeBatch.put(offsetCFH, offsetKey.array(), offsetValue.array());
            this.rocksDBStorage.batchPut(writeBatch);

            putHeapMinCqOffset(topic, queueId, minPhyOffset, minCQOffset);
        } catch (RocksDBException e) {
            ERROR_LOG.error("updateMinOffset Failed.", e);
            throw e;
        } finally {
            writeBatch.close();
            this.rocksDBStorage.release();
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateTopicQueueMinOffset. {}, {}, {}, {}", topic, queueId, minPhyOffset, minCQOffset);
            }
        }
    }

    private void updateTopicQueueMaxOffset(final String topic, final int queueId,
        final long maxPhyOffset, final long maxCQOffset) throws RocksDBException {
        if (!this.rocksDBStorage.hold()) {
            return;
        }
        WriteBatch writeBatch = new WriteBatch();
        try {
            final ColumnFamilyHandle offsetCFH = this.rocksDBStorage.getOffsetCFHandle();

            final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
            final ByteBuffer offsetKey = buildOffsetKeyBB(topicBytes, queueId, true);

            final ByteBuffer offsetValue = buildOffsetValueBB(maxPhyOffset, maxCQOffset);
            writeBatch.put(offsetCFH, offsetKey.array(), offsetValue.array());
            this.rocksDBStorage.batchPut(writeBatch);

            putHeapMaxCqOffset(topic, queueId, maxCQOffset);
        } catch (RocksDBException e) {
            ERROR_LOG.error("updateMaxOffset Failed.", e);
            throw e;
        } finally {
            writeBatch.close();
            this.rocksDBStorage.release();
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateTopicQueueMaxOffset. {}, {}, {}, {}", topic, queueId, maxPhyOffset, maxCQOffset);
            }
        }
    }

    private ByteBuffer getMaxCQInKV(String topic, int queueId) throws RocksDBException {
        byte[] value = getOffsetInKV(topic, queueId, true);
        return (value != null) ? ByteBuffer.wrap(value) : null;
    }

    private ByteBuffer getMinCQInKV(String topic, int queueId) throws RocksDBException {
        byte[] value = getOffsetInKV(topic, queueId, false);
        return (value != null) ? ByteBuffer.wrap(value) : null;
    }

    private void updateTempTopicQueueMaxOffset(final ByteBuffer topicQueueId, final ByteBuffer maxOffsetBB,
        final DispatchRequest request, final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap) {
        Pair<ByteBuffer, DispatchRequest> old = tempTopicQueueMaxOffsetMap.get(topicQueueId);
        if (old == null) {
            tempTopicQueueMaxOffsetMap.put(topicQueueId, new Pair(maxOffsetBB, request));
        } else {
            long oldMaxOffset = old.getObject1().getLong(OFFSET_CQ_OFFSET);
            long maxOffset = maxOffsetBB.getLong(OFFSET_CQ_OFFSET);
            if (maxOffset >= oldMaxOffset) {
                ERROR_LOG.error("cqoffset invalid1. old: {}, now: {}", oldMaxOffset, maxOffset);
            }
        }
    }

    private void updateTopicQueueMaxCqOffset(final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap) {
        final List<DispatchRequest> bufferDRList = this.bufferDRList;
        if (this.messageStore.getMessageStoreConfig().isEnableCacheForRocksDBStore()) {
            for (DispatchRequest dp : bufferDRList) {
                putCache(dp.getTopic(), dp.getQueueId(), dp.getConsumeQueueOffset(),
                    dp.getCommitLogOffset(), dp.getMsgSize(), dp.getTagsCode(), dp.getStoreTimestamp());
            }
        }
        if (tempTopicQueueMaxOffsetMap == null) {
            return;
        }
        for (Map.Entry<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> entry : tempTopicQueueMaxOffsetMap.entrySet()) {
            DispatchRequest request = entry.getValue().getObject2();
            putHeapMaxCqOffset(request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
        }
    }

    private String buildTopicQueueId(final String topic, final int queueId) {
        return topic + "-" + queueId;
    }

    private ByteBuffer buildDeleteCQKey(final boolean start, final byte[] topicBytes, final int queueId) {
        final ByteBuffer bb = ByteBuffer.allocate(11 + topicBytes.length);

        bb.putInt(topicBytes.length).put(CTRL_A).put(topicBytes).put(CTRL_A).putInt(queueId).put(start ? CTRL_0 : CTRL_2);
        bb.flip();
        return bb;
    }

    @Override
    public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
        try {
            ByteBuffer byteBuffer = getMaxCQInKV(topic, queueId);
            if (byteBuffer != null) {
                return byteBuffer.getLong(OFFSET_PHY_OFFSET);
            }
        } catch (Exception e) {
            ERROR_LOG.info("getMaxPhyOffsetInConsumeQueue error. topic: {}, queueId: {}", topic, queueId);
        }

        return null;
    }

    @Override
    public long getMaxOffsetInConsumeQueue() throws RocksDBException {
        INNER_CHECKPOINT_TOPIC.position(0).limit(INNER_CHECKPOINT_TOPIC_LEN);
        byte[] keyBytes = new byte[INNER_CHECKPOINT_TOPIC_LEN];
        INNER_CHECKPOINT_TOPIC.get(keyBytes);

        byte[] valueBytes = this.rocksDBStorage.getOffset(keyBytes);
        if (valueBytes == null) {
            return 0;
        }
        ByteBuffer valueBB = ByteBuffer.wrap(valueBytes);
        return valueBB.getLong(0);
    }

    private void putHeapMinCqOffset(final String topic, final int queueId, final long minPhyOffset, final long minCQOffset) {
        String topicQueueId = buildTopicQueueId(topic, queueId);
        PhyAndCQOffset phyAndCQOffset = new PhyAndCQOffset(minPhyOffset, minCQOffset);
        topicQueueMinCqOffset.put(topicQueueId, phyAndCQOffset);
    }

    private PhyAndCQOffset getHeapMinCqOffset(final String topic, final int queueId) {
        return this.topicQueueMinCqOffset.get(buildTopicQueueId(topic, queueId));
    }

    private PhyAndCQOffset removeHeapMinCqOffset(String topicQueueId) {
        return this.topicQueueMinCqOffset.remove(topicQueueId);
    }

    private void putHeapMaxCqOffset(final String topic, final int queueId, final long maxCQOffset) {
        String topicQueueId = buildTopicQueueId(topic, queueId);
        Long oldMaxCqOffset = this.topicQueueMaxCqOffset.put(topicQueueId, maxCQOffset);
        if (oldMaxCqOffset != null && oldMaxCqOffset > maxCQOffset) {
            ERROR_LOG.error("cqoffset invalid0. old: {}, now: {}", oldMaxCqOffset, maxCQOffset);
        }
    }

    private Long getHeapMaxCqOffset(final String topic, final int queueId) {
        String topicQueueId = buildTopicQueueId(topic, queueId);
        return this.topicQueueMaxCqOffset.get(topicQueueId);
    }

    private Long removeHeapMaxCqOffset(String topicQueueId) {
        return this.topicQueueMaxCqOffset.remove(topicQueueId);
    }

    private void cleanTopic(final Set<String> existTopicSet) {
        RocksIterator iterator = null;
        try {
            Map<String/* topic */, Set<Integer/* queueId */>> topicQueueIdToBeDeletedMap = new HashMap<>();
            iterator = rocksDBStorage.seekOffsetCF();
            if (iterator == null) {
                return;
            }
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                if (key == null || key.length <= 14 || value == null || value.length != 17) {
                    continue;
                }
                ByteBuffer keyBB = ByteBuffer.wrap(key);
                int topicLen = keyBB.getInt(0);
                byte[] topicBytes = new byte[topicLen];
                keyBB.position(5);
                keyBB.get(topicBytes);

                String topic = new String(topicBytes, CHARSET_UTF8);
                if (TopicValidator.isSystemTopic(topic)) {
                    continue;
                }
                if (!existTopicSet.contains(topic)) {
                    int queueId = keyBB.getInt(10 + topicLen);
                    ByteBuffer valueBB = ByteBuffer.wrap(value);
                    long cqOffset = valueBB.getLong(OFFSET_CQ_OFFSET);

                    Set<Integer> topicQueueIdSet = topicQueueIdToBeDeletedMap.get(topic);
                    if (topicQueueIdSet == null) {
                        Set<Integer> newSet = new HashSet<>();
                        newSet.add(queueId);
                        topicQueueIdToBeDeletedMap.put(topic, newSet);
                    } else {
                        topicQueueIdSet.add(queueId);
                    }
                    ERROR_LOG.info("consumeQueue store has dirty cqOffset. topic: {}, queueId: {}, cqOffset: {}",
                        topic, queueId, cqOffset);
                }
            }
            for (Map.Entry<String, Set<Integer>> entry : topicQueueIdToBeDeletedMap.entrySet()) {
                String topic = entry.getKey();
                for (int queueId : entry.getValue()) {
                    destroy(new RocksDBConsumeQueue(topic, queueId));
                }
            }
        } catch (Exception e) {
            log.error("cleanUnusedTopic Failed.", e);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    static class PhyAndCQOffset {
        private final long phyOffset;
        private final long cqOffset;

        public PhyAndCQOffset(final long phyOffset, final long cqOffset) {
            this.phyOffset = phyOffset;
            this.cqOffset = cqOffset;
        }

        public long getPhyOffset() {
            return this.phyOffset;
        }

        public long getCqOffset() {
            return this.cqOffset;
        }

        @Override
        public String toString() {
            return "[cqOffset=" + cqOffset + ", phyOffset=" + phyOffset + "]";
        }
    }

    static class TopicQueueOffset {
        private final String topicName;
        private final int queueId;
        private final long cqOffset;

        public TopicQueueOffset(final String topicName, final int queueId, final long cqOffset) {
            this.topicName = topicName;
            this.queueId = queueId;
            this.cqOffset = cqOffset;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            final TopicQueueOffset that = (TopicQueueOffset) obj;
            if (queueId != that.queueId) {
                return false;
            }
            if (cqOffset != that.cqOffset) {
                return false;
            }
            if (!Objects.equals(topicName, that.topicName)) {
                return false;
            }
            return true;
        }

        private static int hashCode(long value) {
            return (int)(value ^ (value >>> 32));
        }

        @Override
        public int hashCode() {
            int result = (topicName != null) ? topicName.hashCode() : 0;
            result = 31 * result + queueId;
            result = 31 * result + hashCode(cqOffset);
            return result;
        }

        @Override
        public String toString() {
            return "[topicName=" + topicName + ", queueId=" + queueId + ", cqOffset=" + cqOffset + "]";
        }
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
