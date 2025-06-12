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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.logfile.MappedFile;

public class BatchConsumeQueue implements ConsumeQueueInterface {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * BatchConsumeQueue's store unit. Format:
     * <pre>
     * ┌─────────────────────────┬───────────┬────────────┬──────────┬─────────────┬─────────┬───────────────┬─────────┐
     * │CommitLog Physical Offset│ Body Size │Tag HashCode│Store time│msgBaseOffset│batchSize│compactedOffset│reserved │
     * │        (8 Bytes)        │ (4 Bytes) │ (8 Bytes)  │(8 Bytes) │(8 Bytes)    │(2 Bytes)│   (4 Bytes)   │(4 Bytes)│
     * ├─────────────────────────┴───────────┴────────────┴──────────┴─────────────┴─────────┴───────────────┴─────────┤
     * │                                                  Store Unit                                                   │
     * │                                                                                                               │
     * </pre>
     * BatchConsumeQueue's store unit. Size:
     * CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) + Store time(8) +
     * msgBaseOffset(8) + batchSize(2) + compactedOffset(4) + reserved(4)= 46 Bytes
     */
    public static final int CQ_STORE_UNIT_SIZE = 46;
    public static final int MSG_TAG_OFFSET_INDEX = 12;
    public static final int MSG_STORE_TIME_OFFSET_INDEX = 20;
    public static final int MSG_BASE_OFFSET_INDEX = 28;
    public static final int MSG_BATCH_SIZE_INDEX = 36;
    public static final int MSG_COMPACT_OFFSET_INDEX = 38;
    private static final int MSG_COMPACT_OFFSET_LENGTH = 4;
    public static final int INVALID_POS = -1;
    protected final MappedFileQueue mappedFileQueue;
    protected MessageStore messageStore;
    protected ConsumeQueueStore consumeQueueStore;
    protected final String topic;
    protected final int queueId;
    protected final ByteBuffer byteBufferItem;

    protected final String storePath;
    protected final int mappedFileSize;
    protected volatile long maxMsgPhyOffsetInCommitLog = -1;

    protected volatile long minLogicOffset = 0;

    protected volatile long maxOffsetInQueue = 0;
    protected volatile long minOffsetInQueue = -1;
    protected final int commitLogSize;

    protected ConcurrentSkipListMap<Long, MappedFile> offsetCache = new ConcurrentSkipListMap<>();
    protected ConcurrentSkipListMap<Long, MappedFile> timeCache = new ConcurrentSkipListMap<>();

    public BatchConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore messageStore,
        final ConsumeQueueStore consumeQueueStore,
        final String subfolder) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;
        this.consumeQueueStore = consumeQueueStore;
        this.commitLogSize = messageStore.getCommitLog().getCommitLogSize();

        this.topic = topic;
        this.queueId = queueId;

        if (StringUtils.isBlank(subfolder)) {
            String queueDir = this.storePath + File.separator + topic + File.separator + queueId;
            this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        } else {
            String queueDir = this.storePath + File.separator + topic + File.separator + queueId + File.separator + subfolder;
            this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        }

        this.byteBufferItem = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    }

    public BatchConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore messageStore,
        final String subfolder) {
        this(topic, queueId, storePath, mappedFileSize, messageStore, (ConsumeQueueStore) messageStore.getQueueStore(), subfolder);
    }

    public BatchConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore defaultMessageStore) {
        this(topic, queueId, storePath, mappedFileSize, defaultMessageStore, StringUtils.EMPTY);
    }

    @Override
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("Load batch consume queue {}-{} {} {}", topic, queueId, result ? "OK" : "Failed", mappedFileQueue.getMappedFiles().size());
        return result;
    }

    protected void doRefreshCache(Function<MappedFile, BatchOffsetIndex> offsetFunction) {
        if (!this.messageStore.getMessageStoreConfig().isSearchBcqByCacheEnable()) {
            return;
        }
        ConcurrentSkipListMap<Long, MappedFile> newOffsetCache = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, MappedFile> newTimeCache = new ConcurrentSkipListMap<>();

        List<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
        // iterate all BCQ files
        for (int i = 0; i < mappedFiles.size(); i++) {
            MappedFile bcq = mappedFiles.get(i);
            if (isNewFile(bcq)) {
                continue;
            }

            BatchOffsetIndex offset = offsetFunction.apply(bcq);
            if (offset == null) {
                continue;
            }
            newOffsetCache.put(offset.getMsgOffset(), offset.getMappedFile());
            newTimeCache.put(offset.getStoreTimestamp(), offset.getMappedFile());
        }

        this.offsetCache = newOffsetCache;
        this.timeCache = newTimeCache;

        log.info("refreshCache for BCQ [Topic: {}, QueueId: {}]." +
                "offsetCacheSize: {}, minCachedMsgOffset: {}, maxCachedMsgOffset: {}, " +
                "timeCacheSize: {}, minCachedTime: {}, maxCachedTime: {}", this.topic, this.queueId,
            this.offsetCache.size(), this.offsetCache.firstEntry(), this.offsetCache.lastEntry(),
            this.timeCache.size(), this.timeCache.firstEntry(), this.timeCache.lastEntry());
    }

    protected void refreshCache() {
        doRefreshCache(m -> getMinMsgOffset(m, false, true));
    }

    private void destroyCache() {
        this.offsetCache.clear();
        this.timeCache.clear();

        log.info("BCQ [Topic: {}, QueueId: {}]. Cache destroyed", this.topic, this.queueId);
    }

    protected void cacheBcq(MappedFile bcq) {
        try {
            BatchOffsetIndex min = getMinMsgOffset(bcq, false, true);
            this.offsetCache.put(min.getMsgOffset(), min.getMappedFile());
            this.timeCache.put(min.getStoreTimestamp(), min.getMappedFile());
        } catch (Exception e) {
            log.error("Failed caching offset and time on BCQ [Topic: {}, QueueId: {}, File: {}]", this.topic, this.queueId, bcq);
        }
    }

    protected boolean isNewFile(MappedFile mappedFile) {
        return mappedFile.getReadPosition() < CQ_STORE_UNIT_SIZE;
    }

    protected MappedFile searchOffsetFromCache(long msgOffset) {
        Map.Entry<Long, MappedFile> floorEntry = this.offsetCache.floorEntry(msgOffset);
        if (floorEntry == null) {
            // the offset is too small.
            return null;
        } else {
            return floorEntry.getValue();
        }
    }

    private MappedFile searchTimeFromCache(long time) {
        Map.Entry<Long, MappedFile> floorEntry = this.timeCache.floorEntry(time);
        if (floorEntry == null) {
            // the timestamp is too small. so we decide to result first BCQ file.
            return this.mappedFileQueue.getFirstMappedFile();
        } else {
            return floorEntry.getValue();
        }
    }

    @Override
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    byteBuffer.position(i);
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    byteBuffer.getLong();//tagscode
                    byteBuffer.getLong();//timestamp
                    long msgBaseOffset = byteBuffer.getLong();
                    short batchSize = byteBuffer.getShort();
                    if (offset >= 0 && size > 0 && msgBaseOffset >= 0 && batchSize > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxMsgPhyOffsetInCommitLog = offset;
                    } else {
                        log.info("Recover current batch consume queue file over, file:{} offset:{} size:{} msgBaseOffset:{} batchSize:{} mappedFileOffset:{}",
                            mappedFile.getFileName(), offset, size, msgBaseOffset, batchSize, mappedFileOffset);
                        break;
                    }
                }

                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        log.info("Recover last batch consume queue file over, last mapped file:{} ", mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("Recover next batch consume queue file: " + mappedFile.getFileName());
                    }
                } else {
                    log.info("Recover current batch consume queue file over:{} processOffset:{}", mappedFile.getFileName(), processOffset + mappedFileOffset);
                    break;
                }
            }
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
            reviseMaxAndMinOffsetInQueue();
        }
    }

    void reviseMinOffsetInQueue() {
        MappedFile firstMappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (null == firstMappedFile) {
            maxOffsetInQueue = 0;
            minOffsetInQueue = -1;
            minLogicOffset = -1;
            log.info("reviseMinOffsetInQueue found firstMappedFile null, topic:{} queue:{}", topic, queueId);
            return;
        }
        minLogicOffset = firstMappedFile.getFileFromOffset();
        BatchOffsetIndex min = getMinMsgOffset(firstMappedFile, false, false);
        minOffsetInQueue = null == min ? -1 : min.getMsgOffset();
    }

    void reviseMaxOffsetInQueue() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        BatchOffsetIndex max = getMaxMsgOffset(lastMappedFile, true, false);
        if (null == max && this.mappedFileQueue.getMappedFiles().size() >= 2) {
            MappedFile lastTwoMappedFile = this.mappedFileQueue.getMappedFiles().get(this.mappedFileQueue.getMappedFiles().size() - 2);
            max = getMaxMsgOffset(lastTwoMappedFile, true, false);
        }
        maxOffsetInQueue = (null == max) ? 0 : max.getMsgOffset() + max.getBatchSize();
    }

    void reviseMaxAndMinOffsetInQueue() {
        reviseMinOffsetInQueue();
        reviseMaxOffsetInQueue();
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxMsgPhyOffsetInCommitLog;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        SelectMappedBufferResult sbr = getBatchMsgIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new BatchConsumeQueueIterator(sbr);
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) {
        return iterateFrom(startIndex);
    }

    @Override
    public CqUnit get(long offset) {
        ReferredIterator<CqUnit> it = iterateFrom(offset);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public Pair<CqUnit, Long> getCqUnitAndStoreTime(long index) {
        CqUnit cqUnit = get(index);
        Long messageStoreTime = this.consumeQueueStore.getStoreTime(cqUnit);
        return new Pair<>(cqUnit, messageStoreTime);
    }

    @Override
    public Pair<CqUnit, Long> getEarliestUnitAndStoreTime() {
        CqUnit cqUnit = getEarliestUnit();
        Long messageStoreTime = this.consumeQueueStore.getStoreTime(cqUnit);
        return new Pair<>(cqUnit, messageStoreTime);
    }

    @Override
    public CqUnit getEarliestUnit() {
        return get(minOffsetInQueue);
    }

    @Override
    public CqUnit getLatestUnit() {
        return get(maxOffsetInQueue - 1);
    }

    @Override
    public long getLastOffset() {
        CqUnit latestUnit = getLatestUnit();
        return latestUnit.getPos() + latestUnit.getSize();
    }

    @Override
    public boolean isFirstFileAvailable() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            return mappedFile.isAvailable();
        }
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        return mappedFile != null;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {

        long oldMinOffset = minOffsetInQueue;
        long oldMaxOffset = maxOffsetInQueue;

        int logicFileSize = this.mappedFileSize;

        this.maxMsgPhyOffsetInCommitLog = phyOffset - 1;
        boolean stop = false;
        while (!stop) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    byteBuffer.position(i);
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    byteBuffer.getLong();//tagscode
                    byteBuffer.getLong();//timestamp
                    long msgBaseOffset = byteBuffer.getLong();
                    short batchSize = byteBuffer.getShort();

                    if (0 == i) {
                        if (offset >= phyOffset) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxMsgPhyOffsetInCommitLog = offset;
                        }
                    } else {
                        if (offset >= 0 && size > 0 && msgBaseOffset >= 0 && batchSize > 0) {
                            if (offset >= phyOffset) {
                                stop = true;
                                break;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxMsgPhyOffsetInCommitLog = offset;
                            if (pos == logicFileSize) {
                                stop = true;
                                break;
                            }
                        } else {
                            stop = true;
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }
        reviseMaxAndMinOffsetInQueue();
        log.info("Truncate batch logic file topic={} queue={} oldMinOffset={} oldMaxOffset={} minOffset={} maxOffset={} maxPhyOffsetHere={} maxPhyOffsetThere={}",
            topic, queueId, oldMinOffset, oldMaxOffset, minOffsetInQueue, maxOffsetInQueue, maxMsgPhyOffsetInCommitLog, phyOffset);
    }

    @Override
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        return result;
    }

    @Override
    public int deleteExpiredFile(long minCommitLogPos) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(minCommitLogPos, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(minCommitLogPos);
        return cnt;
    }

    @Override
    public void correctMinOffset(long phyMinOffset) {
        reviseMinOffsetInQueue();
        refreshCache();
        long oldMinOffset = minOffsetInQueue;
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    int startPos = result.getByteBuffer().position();
                    for (int i = 0; i < result.getSize(); i += BatchConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        result.getByteBuffer().position(startPos + i);
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt(); //size
                        result.getByteBuffer().getLong();//tagscode
                        result.getByteBuffer().getLong();//timestamp
                        long msgBaseOffset = result.getByteBuffer().getLong();
                        short batchSize = result.getByteBuffer().getShort();

                        if (offsetPy < phyMinOffset) {
                            this.minOffsetInQueue = msgBaseOffset + batchSize;
                        } else {
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            } else {
                /**
                 *  It will go to here under two conditions:
                 1. the files number is 1, and it has no data
                 2. the pull process hold the cq reference, and release it just the moment
                 */
                log.warn("Correct min offset found null cq file topic:{} queue:{} files:{} minOffset:{} maxOffset:{}",
                    topic, queueId, this.mappedFileQueue.getMappedFiles().size(), minOffsetInQueue, maxOffsetInQueue);
            }
        }
        if (oldMinOffset != this.minOffsetInQueue) {
            log.info("BatchCQ Compute new minOffset:{} oldMinOffset{} topic:{} queue:{}", minOffsetInQueue, oldMinOffset, topic, queueId);
        }
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        if (request.getMsgBaseOffset() < 0 || request.getBatchSize() < 0) {
            log.warn("[NOTIFYME]unexpected dispatch request in batch consume queue topic:{} queue:{} offset:{}", topic, queueId, request.getCommitLogOffset());
            return;
        }
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = this.putBatchMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), request.getTagsCode(),
                request.getStoreTimestamp(), request.getMsgBaseOffset(), request.getBatchSize());
            if (result) {
                if (BrokerRole.SLAVE == this.messageStore.getMessageStoreConfig().getBrokerRole()) {
                    this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[NOTIFYME]put commit log position info to batch consume queue " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
        // XXX: warn and notify me
        log.error("[NOTIFYME]batch consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) {
        String topicQueueKey = getTopic() + "-" + getQueueId();

        long queueOffset = queueOffsetOperator.getBatchQueueOffset(topicQueueKey);

        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_BASE, String.valueOf(queueOffset));
            msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        }
        msg.setQueueOffset(queueOffset);
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg,
        short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseBatchQueueOffset(topicQueueKey, messageNum);
    }

    public boolean putBatchMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long storeTime,
        final long msgBaseOffset, final short batchSize) {

        if (offset <= this.maxMsgPhyOffsetInCommitLog) {
            if (System.currentTimeMillis() % 1000 == 0) {
                log.warn("Build batch consume queue repeatedly, maxMsgPhyOffsetInCommitLog:{} offset:{} Topic: {} QID: {}",
                    maxMsgPhyOffsetInCommitLog, offset, this.topic, this.queueId);
            }
            return true;
        }

        long behind = System.currentTimeMillis() - storeTime;
        if (behind > 10000 && System.currentTimeMillis() % 10000 == 0) {
            String flag = "LEVEL" + (behind / 10000);
            log.warn("Reput behind {} topic:{} queue:{} offset:{} behind:{}", flag, topic, queueId, offset, behind);
        }

        this.byteBufferItem.flip();
        this.byteBufferItem.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferItem.putLong(offset);
        this.byteBufferItem.putInt(size);
        this.byteBufferItem.putLong(tagsCode);
        this.byteBufferItem.putLong(storeTime);
        this.byteBufferItem.putLong(msgBaseOffset);
        this.byteBufferItem.putShort(batchSize);
        this.byteBufferItem.putInt(INVALID_POS);
        this.byteBufferItem.putInt(0); // 4 bytes reserved

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(this.mappedFileQueue.getMaxOffset());
        if (mappedFile != null) {
            boolean isNewFile = isNewFile(mappedFile);
            boolean appendRes;
            if (messageStore.getMessageStoreConfig().isPutConsumeQueueDataByFileChannel()) {
                appendRes = mappedFile.appendMessageUsingFileChannel(this.byteBufferItem.array());
            } else {
                appendRes = mappedFile.appendMessage(this.byteBufferItem.array());
            }
            if (appendRes) {
                maxMsgPhyOffsetInCommitLog = offset;
                maxOffsetInQueue = msgBaseOffset + batchSize;
                //only the first time need to correct the minOffsetInQueue
                //the other correctness is done in correctLogicMinoffsetService
                if (mappedFile.isFirstCreateInQueue() && minOffsetInQueue == -1) {
                    reviseMinOffsetInQueue();
                }
                if (isNewFile) {
                    // cache new file
                    this.cacheBcq(mappedFile);
                }
            }
            return appendRes;
        }
        return false;
    }

    protected BatchOffsetIndex getMinMsgOffset(MappedFile mappedFile, boolean getBatchSize, boolean getStoreTime) {
        if (mappedFile.getReadPosition() < CQ_STORE_UNIT_SIZE) {
            return null;
        }
        return getBatchOffsetIndexByPos(mappedFile, 0, getBatchSize, getStoreTime);
    }

    protected BatchOffsetIndex getBatchOffsetIndexByPos(MappedFile mappedFile, int pos, boolean getBatchSize,
        boolean getStoreTime) {
        SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(pos);
        try {
            return new BatchOffsetIndex(mappedFile, pos, sbr.getByteBuffer().getLong(MSG_BASE_OFFSET_INDEX),
                getBatchSize ? sbr.getByteBuffer().getShort(MSG_BATCH_SIZE_INDEX) : 0,
                getStoreTime ? sbr.getByteBuffer().getLong(MSG_STORE_TIME_OFFSET_INDEX) : 0);
        } finally {
            if (sbr != null) {
                sbr.release();
            }
        }
    }

    protected BatchOffsetIndex getMaxMsgOffset(MappedFile mappedFile, boolean getBatchSize, boolean getStoreTime) {
        if (mappedFile == null || mappedFile.getReadPosition() < CQ_STORE_UNIT_SIZE) {
            return null;
        }
        int pos = mappedFile.getReadPosition() - CQ_STORE_UNIT_SIZE;
        return getBatchOffsetIndexByPos(mappedFile, pos, getBatchSize, getStoreTime);
    }

    private static int ceil(int pos) {
        return (pos / CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
    }

    /**
     * Gets SelectMappedBufferResult by batch-message offset
     * Node: the caller is responsible for the release of SelectMappedBufferResult
     * @param msgOffset
     * @return SelectMappedBufferResult
     */
    public SelectMappedBufferResult getBatchMsgIndexBuffer(final long msgOffset) {
        if (msgOffset >= maxOffsetInQueue) {
            return null;
        }
        MappedFile targetBcq;
        BatchOffsetIndex targetMinOffset;

        // first check the last bcq file
        MappedFile lastBcq = mappedFileQueue.getLastMappedFile();
        BatchOffsetIndex minForLastBcq = getMinMsgOffset(lastBcq, false, false);
        if (null != minForLastBcq && minForLastBcq.getMsgOffset() <= msgOffset) {
            // found, it's the last bcq.
            targetBcq = lastBcq;
            targetMinOffset = minForLastBcq;
        } else {
            boolean searchBcqByCacheEnable = this.messageStore.getMessageStoreConfig().isSearchBcqByCacheEnable();
            if (searchBcqByCacheEnable) {
                // it's not the last BCQ file, so search it through cache.
                targetBcq = this.searchOffsetFromCache(msgOffset);
                // not found in cache
                if (targetBcq == null) {
                    MappedFile firstBcq = mappedFileQueue.getFirstMappedFile();
                    BatchOffsetIndex minForFirstBcq = getMinMsgOffset(firstBcq, false, false);
                    if (minForFirstBcq != null && minForFirstBcq.getMsgOffset() <= msgOffset && msgOffset < minForLastBcq.getMsgOffset()) {
                        // old search logic
                        targetBcq = this.searchOffsetFromFiles(msgOffset);
                    }
                    log.warn("cache is not working on BCQ [Topic: {}, QueueId: {}] for msgOffset: {}, targetBcq: {}", this.topic, this.queueId, msgOffset, targetBcq);
                }
            } else {
                // old search logic
                targetBcq = this.searchOffsetFromFiles(msgOffset);
            }

            if (targetBcq == null) {
                return null;
            }

            targetMinOffset = getMinMsgOffset(targetBcq, false, false);
        }

        BatchOffsetIndex targetMaxOffset = getMaxMsgOffset(targetBcq, false, false);
        if (null == targetMinOffset || null == targetMaxOffset) {
            return null;
        }

        // then use binary search to find the indexed position
        SelectMappedBufferResult sbr = targetMinOffset.getMappedFile().selectMappedBuffer(0);
        try {
            ByteBuffer byteBuffer = sbr.getByteBuffer();
            int left = targetMinOffset.getIndexPos(), right = targetMaxOffset.getIndexPos();
            int mid = binarySearch(byteBuffer, left, right, CQ_STORE_UNIT_SIZE, MSG_BASE_OFFSET_INDEX, msgOffset);
            if (mid != -1) {
                // return a buffer that needs to be released manually.
                return targetMinOffset.getMappedFile().selectMappedBuffer(mid);
            }
        } finally {
            sbr.release();
        }
        return null;
    }

    public MappedFile searchOffsetFromFiles(long msgOffset) {
        MappedFile targetBcq = null;
        // find the mapped file one by one reversely
        int mappedFileNum = this.mappedFileQueue.getMappedFiles().size();
        for (int i = mappedFileNum - 1; i >= 0; i--) {
            MappedFile mappedFile = mappedFileQueue.getMappedFiles().get(i);
            BatchOffsetIndex tmpMinMsgOffset = getMinMsgOffset(mappedFile, false, false);
            if (null != tmpMinMsgOffset && tmpMinMsgOffset.getMsgOffset() <= msgOffset) {
                targetBcq = mappedFile;
                break;
            }
        }

        return targetBcq;
    }

    /**
     * Find the message whose timestamp is the smallest, greater than or equal to the given time.
     *
     * @param timestamp
     * @return
     */
    @Deprecated
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        return getOffsetInQueueByTime(timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(long timestamp, BoundaryType boundaryType) {
        MappedFile targetBcq;
        BatchOffsetIndex targetMinOffset;

        // first check the last bcq
        MappedFile lastBcq = mappedFileQueue.getLastMappedFile();
        BatchOffsetIndex minForLastBcq = getMinMsgOffset(lastBcq, false, true);
        if (null != minForLastBcq && minForLastBcq.getStoreTimestamp() <= timestamp) {
            // found, it's the last bcq.
            targetBcq = lastBcq;
            targetMinOffset = minForLastBcq;
        } else {
            boolean searchBcqByCacheEnable = this.messageStore.getMessageStoreConfig().isSearchBcqByCacheEnable();
            if (searchBcqByCacheEnable) {
                // it's not the last BCQ file, so search it through cache.
                targetBcq = this.searchTimeFromCache(timestamp);
                if (targetBcq == null) {
                    // not found in cache
                    MappedFile firstBcq = mappedFileQueue.getFirstMappedFile();
                    BatchOffsetIndex minForFirstBcq = getMinMsgOffset(firstBcq, false, true);
                    if (minForFirstBcq != null && minForFirstBcq.getStoreTimestamp() <= timestamp && timestamp < minForLastBcq.getStoreTimestamp()) {
                        // old search logic
                        targetBcq = this.searchTimeFromFiles(timestamp);
                    }
                    log.warn("cache is not working on BCQ [Topic: {}, QueueId: {}] for timestamp: {}, targetBcq: {}", this.topic, this.queueId, timestamp, targetBcq);
                }
            } else {
                // old search logic
                targetBcq = this.searchTimeFromFiles(timestamp);
            }

            if (targetBcq == null) {
                return -1;
            }
            targetMinOffset = getMinMsgOffset(targetBcq, false, true);
        }

        BatchOffsetIndex targetMaxOffset = getMaxMsgOffset(targetBcq, false, true);
        if (null == targetMinOffset || null == targetMaxOffset) {
            return -1;
        }

        //then use binary search to find the indexed position
        SelectMappedBufferResult sbr = targetMinOffset.getMappedFile().selectMappedBuffer(0);
        try {
            ByteBuffer byteBuffer = sbr.getByteBuffer();
            int left = targetMinOffset.getIndexPos(), right = targetMaxOffset.getIndexPos();
            long maxQueueTimestamp = byteBuffer.getLong(right + MSG_STORE_TIME_OFFSET_INDEX);
            if (timestamp >= maxQueueTimestamp) {
                return byteBuffer.getLong(right + MSG_BASE_OFFSET_INDEX);
            }
            int mid = binarySearchRight(byteBuffer, left, right, CQ_STORE_UNIT_SIZE, MSG_STORE_TIME_OFFSET_INDEX, timestamp, boundaryType);
            if (mid != -1) {
                return byteBuffer.getLong(mid + MSG_BASE_OFFSET_INDEX);
            }
        } finally {
            sbr.release();
        }

        return -1;
    }

    private MappedFile searchTimeFromFiles(long timestamp) {
        MappedFile targetBcq = null;

        int mappedFileNum = this.mappedFileQueue.getMappedFiles().size();
        for (int i = mappedFileNum - 1; i >= 0; i--) {
            MappedFile mappedFile = mappedFileQueue.getMappedFiles().get(i);
            BatchOffsetIndex tmpMinMsgOffset = getMinMsgOffset(mappedFile, false, true);
            if (tmpMinMsgOffset == null) {
                //Maybe the new file
                continue;
            }
            BatchOffsetIndex tmpMaxMsgOffset = getMaxMsgOffset(mappedFile, false, true);
            //Here should not be null
            if (tmpMaxMsgOffset == null) {
                break;
            }
            if (tmpMaxMsgOffset.getStoreTimestamp() >= timestamp) {
                if (tmpMinMsgOffset.getStoreTimestamp() <= timestamp) {
                    targetBcq = mappedFile;
                    break;
                } else {
                    if (i - 1 < 0) {
                        //This is the first file
                        targetBcq = mappedFile;
                        break;
                    } else {
                        //The min timestamp of this file is larger than the given timestamp, so check the next file
                        continue;
                    }
                }
            } else {
                //The max timestamp of this file is smaller than the given timestamp, so double check the previous file
                if (i + 1 <= mappedFileNum - 1) {
                    mappedFile = mappedFileQueue.getMappedFiles().get(i + 1);
                    targetBcq = mappedFile;
                    break;
                } else {
                    //There is no timestamp larger than the given timestamp
                    break;
                }
            }
        }

        return targetBcq;
    }

    /**
     * Find the offset of which the value is equal or larger than the given targetValue.
     * If there are many values equal to the target, then return the lowest offset if boundaryType is LOWER while
     * return the highest offset if boundaryType is UPPER.
     */
    public static int binarySearchRight(ByteBuffer byteBuffer, int left, int right, final int unitSize,
        final int unitShift, long targetValue, BoundaryType boundaryType) {
        int mid = -1;
        while (left <= right) {
            mid = ceil((left + right) / 2);
            long tmpValue = byteBuffer.getLong(mid + unitShift);
            if (mid == right) {
                //Means left and the right are the same
                if (tmpValue >= targetValue) {
                    return mid;
                } else {
                    return -1;
                }
            } else if (mid == left) {
                //Means the left + unitSize = right
                if (tmpValue >= targetValue) {
                    return mid;
                } else {
                    left = mid + unitSize;
                }
            } else {
                //mid is actually in the mid
                switch (boundaryType) {
                    case LOWER:
                        if (tmpValue < targetValue) {
                            left = mid + unitSize;
                        } else {
                            right = mid;
                        }
                        break;
                    case UPPER:
                        if (tmpValue <= targetValue) {
                            left = mid;
                        } else {
                            right = mid - unitSize;
                        }
                        break;
                    default:
                        log.warn("Unknown boundary type");
                        return -1;
                }
            }
        }
        return -1;
    }

    /**
     * Here is vulnerable, the min value of the bytebuffer must be smaller or equal then the given value.
     * Otherwise, it may get -1
     */
    protected int binarySearch(ByteBuffer byteBuffer, int left, int right, final int unitSize, final int unitShift,
        long targetValue) {
        int maxRight = right;
        int mid = -1;
        while (left <= right) {
            mid = ceil((left + right) / 2);
            long tmpValue = byteBuffer.getLong(mid + unitShift);
            if (tmpValue == targetValue) {
                return mid;
            }
            if (tmpValue > targetValue) {
                right = mid - unitSize;
            } else {
                if (mid == left) {
                    //the binary search is converging to the left, so maybe the one on the right of mid is the exactly correct one
                    if (mid + unitSize <= maxRight
                        && byteBuffer.getLong(mid + unitSize + unitShift) <= targetValue) {
                        return mid + unitSize;
                    } else {
                        return mid;
                    }
                } else {
                    left = mid;
                }
            }
        }
        return -1;
    }

    static class BatchConsumeQueueIterator implements ReferredIterator<CqUnit> {
        private SelectMappedBufferResult sbr;
        private int relativePos = 0;

        public BatchConsumeQueueIterator(SelectMappedBufferResult sbr) {
            this.sbr = sbr;
            if (sbr != null && sbr.getByteBuffer() != null) {
                relativePos = sbr.getByteBuffer().position();
            }
        }

        @Override
        public boolean hasNext() {
            if (sbr == null || sbr.getByteBuffer() == null) {
                return false;
            }

            return sbr.getByteBuffer().hasRemaining();
        }

        @Override
        public CqUnit next() {
            if (!hasNext()) {
                return null;
            }
            ByteBuffer tmpBuffer = sbr.getByteBuffer().slice();
            tmpBuffer.position(MSG_COMPACT_OFFSET_INDEX);
            ByteBuffer compactOffsetStoreBuffer = tmpBuffer.slice();
            compactOffsetStoreBuffer.limit(MSG_COMPACT_OFFSET_LENGTH);

            int relativePos = sbr.getByteBuffer().position();
            long offsetPy = sbr.getByteBuffer().getLong();
            int sizePy = sbr.getByteBuffer().getInt();
            long tagsCode = sbr.getByteBuffer().getLong(); //tagscode
            sbr.getByteBuffer().getLong();//timestamp
            long msgBaseOffset = sbr.getByteBuffer().getLong();
            short batchSize = sbr.getByteBuffer().getShort();
            int compactedOffset = sbr.getByteBuffer().getInt();
            sbr.getByteBuffer().position(relativePos + CQ_STORE_UNIT_SIZE);

            return new CqUnit(msgBaseOffset, offsetPy, sizePy, tagsCode, batchSize, compactedOffset, compactOffsetStoreBuffer);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void release() {
            if (sbr != null) {
                sbr.release();
                sbr = null;
            }
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

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public CQType getCQType() {
        return CQType.BatchCQ;
    }

    @Override
    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    @Override
    public int getUnitSize() {
        return CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void destroy() {
        this.maxMsgPhyOffsetInCommitLog = -1;
        this.minOffsetInQueue = -1;
        this.maxOffsetInQueue = 0;
        this.mappedFileQueue.destroy();
        this.destroyCache();
    }

    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    @Override
    public long rollNextFile(long nextBeginOffset) {
        return 0;
    }

    /**
     * Batch msg offset (deep logic offset)
     *
     * @return max deep offset
     */
    @Override
    public long getMaxOffsetInQueue() {
        return maxOffsetInQueue;
    }

    @Override
    public long getMinOffsetInQueue() {
        return minOffsetInQueue;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        // transfer message offset to physical offset
        SelectMappedBufferResult firstMappedFileBuffer = getBatchMsgIndexBuffer(from);
        if (firstMappedFileBuffer == null) {
            return -1;
        }
        long physicalOffsetFrom = firstMappedFileBuffer.getStartOffset();

        SelectMappedBufferResult lastMappedFileBuffer = getBatchMsgIndexBuffer(to);
        if (lastMappedFileBuffer == null) {
            return -1;
        }
        long physicalOffsetTo = lastMappedFileBuffer.getStartOffset();

        List<MappedFile> mappedFiles = mappedFileQueue.range(physicalOffsetFrom, physicalOffsetTo);
        if (mappedFiles.isEmpty()) {
            return -1;
        }

        boolean sample = false;
        long match = 0;
        long matchCqUnitCount = 0;
        long raw = 0;
        long scanCqUnitCount = 0;

        for (MappedFile mappedFile : mappedFiles) {
            int start = 0;
            int len = mappedFile.getFileSize();

            // calculate start and len for first segment and last segment to reduce scanning
            // first file segment
            if (mappedFile.getFileFromOffset() <= physicalOffsetFrom) {
                start = (int) (physicalOffsetFrom - mappedFile.getFileFromOffset());
                if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() >= physicalOffsetTo) {
                    // current mapped file covers search range completely.
                    len = (int) (physicalOffsetTo - physicalOffsetFrom);
                } else {
                    len = mappedFile.getFileSize() - start;
                }
            }

            // last file segment
            if (0 == start && mappedFile.getFileFromOffset() + mappedFile.getFileSize() > physicalOffsetTo) {
                len = (int) (physicalOffsetTo - mappedFile.getFileFromOffset());
            }

            // select partial data to scan
            SelectMappedBufferResult slice = mappedFile.selectMappedBuffer(start, len);
            if (null != slice) {
                try {
                    ByteBuffer buffer = slice.getByteBuffer();
                    int current = 0;
                    while (current < len) {
                        // skip physicalOffset and message length fields.
                        buffer.position(current + MSG_TAG_OFFSET_INDEX);
                        long tagCode = buffer.getLong();
                        buffer.position(current + MSG_BATCH_SIZE_INDEX);
                        long batchSize = buffer.getShort();
                        if (filter.isMatchedByConsumeQueue(tagCode, null)) {
                            match += batchSize;
                            matchCqUnitCount++;
                        }
                        raw += batchSize;
                        scanCqUnitCount++;
                        current += CQ_STORE_UNIT_SIZE;

                        if (scanCqUnitCount >= messageStore.getMessageStoreConfig().getMaxConsumeQueueScan()) {
                            sample = true;
                            break;
                        }

                        if (matchCqUnitCount > messageStore.getMessageStoreConfig().getSampleCountThreshold()) {
                            sample = true;
                            break;
                        }
                    }
                } finally {
                    slice.release();
                }
            }
            // we have scanned enough entries, now is the time to return an educated guess.
            if (sample) {
                break;
            }
        }

        long result = match;
        if (sample) {
            if (0 == raw) {
                log.error("[BUG]. Raw should NOT be 0");
                return 0;
            }
            result = (long) (match * (to - from) * 1.0 / raw);
        }
        log.debug("Result={}, raw={}, match={}, sample={}", result, raw, match, sample);
        return result;
    }

    @Override
    public void initializeWithOffset(long offset, long minPhyOffset) {
        // not support now
    }
}
