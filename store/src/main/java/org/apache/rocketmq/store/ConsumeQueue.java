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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStore;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.FileQueueLifeCycle;
import org.apache.rocketmq.store.queue.MultiDispatchUtils;
import org.apache.rocketmq.store.queue.QueueOffsetOperator;
import org.apache.rocketmq.store.queue.ReferredIterator;

public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * ConsumeQueue's store unit. Format:
     * <pre>
     * ┌───────────────────────────────┬───────────────────┬───────────────────────────────┐
     * │    CommitLog Physical Offset  │      Body Size    │            Tag HashCode       │
     * │          (8 Bytes)            │      (4 Bytes)    │             (8 Bytes)         │
     * ├───────────────────────────────┴───────────────────┴───────────────────────────────┤
     * │                                     Store Unit                                    │
     * │                                                                                   │
     * </pre>
     * ConsumeQueue's store unit. Size: CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) = 20 Bytes
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    public static final int MSG_TAG_OFFSET_INDEX = 12;
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore messageStore;
    private final ConsumeQueueStore consumeQueueStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;

    /**
     * Minimum offset of the consume file queue that points to valid commit log record.
     */
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(final String topic, final int queueId, final String storePath, final int mappedFileSize,
        final DefaultMessageStore messageStore) {
        this(topic, queueId, storePath, mappedFileSize, messageStore, (ConsumeQueueStore) messageStore.getQueueStore());
    }

    public ConsumeQueue(final String topic, final int queueId, final String storePath, final int mappedFileSize,
        final DefaultMessageStore messageStore, final ConsumeQueueStore consumeQueueStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;
        this.consumeQueueStore = consumeQueueStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()),
                messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    @Override
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    @Override
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.setMaxPhysicOffset(offset + size);
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = this.mappedFileQueue.getTotalFileSize();
        if (isExtReadEnable()) {
            totalSize += this.consumeQueueExt.getTotalSize();
        }
        return totalSize;
    }

    @Override
    public int getUnitSize() {
        return CQ_STORE_UNIT_SIZE;
    }

    @Deprecated
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
            messageStore.getCommitLog(), BoundaryType.LOWER);
        return binarySearchInQueueByTime(mappedFile, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
            messageStore.getCommitLog(), boundaryType);
        return binarySearchInQueueByTime(mappedFile, timestamp, boundaryType);
    }

    private long binarySearchInQueueByTime(final MappedFile mappedFile, final long timestamp,
        BoundaryType boundaryType) {
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long minPhysicOffset = this.messageStore.getMinPhyOffset();
            int range = mappedFile.getFileSize();
            if (mappedFile.getWrotePosition() != 0 && mappedFile.getWrotePosition() != mappedFile.getFileSize()) {
                // mappedFile is the last one and is currently being written.
                range = mappedFile.getWrotePosition();
            }
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, range);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                int ceiling = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                int floor = low;
                high = ceiling;
                try {
                    // Handle the following corner cases first:
                    // 1. store time of (high) < timestamp
                    // 2. store time of (low) > timestamp
                    long storeTime;
                    long phyOffset;
                    int size;
                    // Handle case 1
                    byteBuffer.position(ceiling);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                    if (storeTime < timestamp) {
                        switch (boundaryType) {
                            case LOWER:
                                return (mappedFile.getFileFromOffset() + ceiling + CQ_STORE_UNIT_SIZE) / CQ_STORE_UNIT_SIZE;
                            case UPPER:
                                return (mappedFile.getFileFromOffset() + ceiling) / CQ_STORE_UNIT_SIZE;
                            default:
                                log.warn("Unknown boundary type");
                                break;
                        }
                    }

                    // Handle case 2
                    byteBuffer.position(floor);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                    if (storeTime > timestamp) {
                        switch (boundaryType) {
                            case LOWER:
                                return mappedFile.getFileFromOffset() / CQ_STORE_UNIT_SIZE;
                            case UPPER:
                                return 0;
                            default:
                                log.warn("Unknown boundary type");
                                break;
                        }
                    }

                    // Perform binary search
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        phyOffset = byteBuffer.getLong();
                        size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        storeTime = this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            log.warn("Failed to query store timestamp for commit log offset: {}", phyOffset);
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                        }
                    }

                    if (targetOffset != -1) {
                        // We just found ONE matched record. These next to it might also share the same store-timestamp.
                        offset = targetOffset;
                        switch (boundaryType) {
                            case LOWER: {
                                int previousAttempt = targetOffset;
                                while (true) {
                                    int attempt = previousAttempt - CQ_STORE_UNIT_SIZE;
                                    if (attempt < floor) {
                                        break;
                                    }
                                    byteBuffer.position(attempt);
                                    long physicalOffset = byteBuffer.getLong();
                                    int messageSize = byteBuffer.getInt();
                                    long messageStoreTimestamp = messageStore.getCommitLog()
                                        .pickupStoreTimestamp(physicalOffset, messageSize);
                                    if (messageStoreTimestamp == timestamp) {
                                        previousAttempt = attempt;
                                        continue;
                                    }
                                    break;
                                }
                                offset = previousAttempt;
                                break;
                            }
                            case UPPER: {
                                int previousAttempt = targetOffset;
                                while (true) {
                                    int attempt = previousAttempt + CQ_STORE_UNIT_SIZE;
                                    if (attempt > ceiling) {
                                        break;
                                    }
                                    byteBuffer.position(attempt);
                                    long physicalOffset = byteBuffer.getLong();
                                    int messageSize = byteBuffer.getInt();
                                    long messageStoreTimestamp = messageStore.getCommitLog()
                                        .pickupStoreTimestamp(physicalOffset, messageSize);
                                    if (messageStoreTimestamp == timestamp) {
                                        previousAttempt = attempt;
                                        continue;
                                    }
                                    break;
                                }
                                offset = previousAttempt;
                                break;
                            }
                            default: {
                                log.warn("Unknown boundary type");
                                break;
                            }
                        }
                    } else {
                        // Given timestamp does not have any message records. But we have a range enclosing the
                        // timestamp.
                        /*
                         * Consider the follow case: t2 has no consume queue entry and we are searching offset of
                         * t2 for lower and upper boundaries.
                         *  --------------------------
                         *   timestamp   Consume Queue
                         *       t1          1
                         *       t1          2
                         *       t1          3
                         *       t3          4
                         *       t3          5
                         *   --------------------------
                         * Now, we return 3 as upper boundary of t2 and 4 as its lower boundary. It looks
                         * contradictory at first sight, but it does make sense when performing range queries.
                         */
                        switch (boundaryType) {
                            case LOWER: {
                                offset = rightOffset;
                                break;
                            }

                            case UPPER: {
                                offset = leftOffset;
                                break;
                            }
                            default: {
                                log.warn("Unknown boundary type");
                                break;
                            }
                        }
                    }
                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        truncateDirtyLogicFiles(phyOffset, true);
    }

    public void truncateDirtyLogicFiles(long phyOffset, boolean deleteFile) {

        int logicFileSize = this.mappedFileSize;

        this.setMaxPhysicOffset(phyOffset);
        long maxExtAddr = 1;
        boolean shouldDeleteFile = false;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        if (offset >= phyOffset) {
                            shouldDeleteFile = true;
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.setMaxPhysicOffset(offset + size);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffset) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.setMaxPhysicOffset(offset + size);
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }

                if (shouldDeleteFile) {
                    if (deleteFile) {
                        this.mappedFileQueue.deleteLastMappedFile();
                    } else {
                        this.mappedFileQueue.deleteExpiredFile(Collections.singletonList(this.mappedFileQueue.getLastMappedFile()));
                    }
                }

            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    @Override
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    @Override
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    @Override
    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * Update minLogicOffset such that entries after it would point to valid commit log address.
     *
     * @param minCommitLogOffset Minimum commit log offset
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        // Check if the consume queue is the state of deprecation.
        if (minLogicOffset >= mappedFileQueue.getMaxOffset()) {
            log.info("ConsumeQueue[Topic={}, queue-id={}] contains no valid entries", topic, queueId);
            return;
        }

        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO operation.
        // The rationale is that consume queue always preserves the last file. In case there are many deprecated topics,
        // This check would save a lot of efforts.
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == lastMappedFile) {
            return;
        }

        SelectMappedBufferResult lastRecord = null;
        try {
            int maxReadablePosition = lastMappedFile.getReadPosition();
            lastRecord = lastMappedFile.selectMappedBuffer(maxReadablePosition - ConsumeQueue.CQ_STORE_UNIT_SIZE,
                ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (null != lastRecord) {
                ByteBuffer buffer = lastRecord.getByteBuffer();
                long commitLogOffset = buffer.getLong();
                if (commitLogOffset < minCommitLogOffset) {
                    // Keep the largest known consume offset, even if this consume-queue contains no valid entries at
                    // all. Let minLogicOffset point to a future slot.
                    this.minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition;
                    log.info("ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                        topic, queueId, getMinOffsetInQueue());
                    return;
                }
            }
        } finally {
            if (null != lastRecord) {
                lastRecord.release();
            }
        }

        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // Search from previous min logical offset. Typically, a consume queue file segment contains 300,000 entries
            // searching from previous position saves significant amount of comparisons and IOs
            boolean intact = true; // Assume previous value is still valid
            long start = this.minLogicOffset - mappedFile.getFileFromOffset();
            if (start < 0) {
                intact = false;
                start = 0;
            }

            if (start > mappedFile.getReadPosition()) {
                log.error("[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                    mappedFile.getFileName());
                return;
            }

            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) start);
            if (result == null) {
                log.warn("[Bug] Failed to scan consume queue entries from file on correcting min offset: {}",
                    mappedFile.getFileName());
                return;
            }

            try {
                // No valid consume entries
                if (result.getSize() == 0) {
                    log.debug("ConsumeQueue[topic={}, queue-id={}] contains no valid entries", topic, queueId);
                    return;
                }

                ByteBuffer buffer = result.getByteBuffer().slice();
                // Verify whether the previous value is still valid or not before conducting binary search
                long commitLogOffset = buffer.getLong();
                if (intact && commitLogOffset >= minCommitLogOffset) {
                    log.info("Abort correction as previous min-offset points to {}, which is greater than {}",
                        commitLogOffset, minCommitLogOffset);
                    return;
                }

                // Binary search between range [previous_min_logic_offset, first_file_from_offset + file_size)
                // Note the consume-queue deletion procedure ensures the last entry points to somewhere valid.
                int low = 0;
                int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE;
                while (true) {
                    if (high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        break;
                    }
                    int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE;
                    buffer.position(mid);
                    commitLogOffset = buffer.getLong();
                    if (commitLogOffset > minCommitLogOffset) {
                        high = mid;
                    } else if (commitLogOffset == minCommitLogOffset) {
                        low = mid;
                        high = mid;
                        break;
                    } else {
                        low = mid;
                    }
                }

                // Examine the last one or two entries
                for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    buffer.position(i);
                    long offsetPy = buffer.getLong();
                    buffer.position(i + 12);
                    long tagsCode = buffer.getLong();

                    if (offsetPy >= minCommitLogOffset) {
                        this.minLogicOffset = mappedFile.getFileFromOffset() + start + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                            this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has an extended file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                result.release();
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    @Override
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                if (MultiDispatchUtils.checkMultiDispatchQueue(this.messageStore.getMessageStoreConfig(), request)) {
                    multiDispatchLmqQueue(request, maxRetries);
                }
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = multiDispatchQueue.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        String[] queueOffsets = multiQueueOffset.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            if (StringUtils.contains(queueName, File.separator)) {
                continue;
            }
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);
        }
    }

    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
        int queueId) {
        ConsumeQueueInterface cq = this.messageStore.findConsumeQueue(queueName, queueId);
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = ((ConsumeQueue) cq).putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                request.getTagsCode(),
                queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        long queueOffset = queueOffsetOperator.getQueueOffset(topicQueueKey);
        msg.setQueueOffset(queueOffset);
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg,
        short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        if (offset + size <= this.getMaxPhysicOffset()) {
            MessageStoreStateMachine stateMachine = messageStore.getStateMachine();
            MessageStoreStateMachine.MessageStoreState messageStoreState = stateMachine.getCurrentState();

            // During the recovery process after broker crashes, this logs will cause the scrolling of valid logs.
            // So only print the warning log after RECOVER_COMMITLOG_OK or the current state running time < 3 seconds.
            if (messageStoreState.isAfter(MessageStoreStateMachine.MessageStoreState.RECOVER_COMMITLOG_OK) ||
                stateMachine.getCurrentStateRunningTimeMs() < 3000) {
                log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}",
                    this.getMaxPhysicOffset(), offset);
            } else {
                log.debug("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}",
                    this.getMaxPhysicOffset(), offset);
            }
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.setMaxPhysicOffset(offset + size);
            boolean appendResult;
            if (messageStore.getMessageStoreConfig().isPutConsumeQueueDataByFileChannel()) {
                appendResult = mappedFile.appendMessageUsingFileChannel(this.byteBufferIndex.array());
            } else {
                appendResult = mappedFile.appendMessage(this.byteBufferIndex.array());
            }
            return appendResult;
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            if (messageStore.getMessageStoreConfig().isPutConsumeQueueDataByFileChannel()) {
                mappedFile.appendMessageUsingFileChannel(byteBuffer.array());
            } else {
                mappedFile.appendMessage(byteBuffer.array());
            }

        }
    }

    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        SelectMappedBufferResult sbr = getIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new ConsumeQueueIterator(sbr);
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
        /**
         * here maybe should not return null
         */
        ReferredIterator<CqUnit> it = iterateFrom(minLogicOffset / CQ_STORE_UNIT_SIZE);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getLatestUnit() {
        ReferredIterator<CqUnit> it = iterateFrom((mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE) - 1);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public boolean isFirstFileAvailable() {
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        return false;
    }

    private class ConsumeQueueIterator implements ReferredIterator<CqUnit> {
        private SelectMappedBufferResult sbr;
        private int relativePos = 0;

        public ConsumeQueueIterator(SelectMappedBufferResult sbr) {
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
            long queueOffset = (sbr.getStartOffset() + sbr.getByteBuffer().position() - relativePos) / CQ_STORE_UNIT_SIZE;
            CqUnit cqUnit = new CqUnit(queueOffset,
                sbr.getByteBuffer().getLong(),
                sbr.getByteBuffer().getInt(),
                sbr.getByteBuffer().getLong());

            if (isExtAddr(cqUnit.getTagsCode())) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                boolean extRet = getExt(cqUnit.getTagsCode(), cqExtUnit);
                if (extRet) {
                    cqUnit.setTagsCode(cqExtUnit.getTagsCode());
                    cqUnit.setCqExtUnit(cqExtUnit);
                } else {
                    // can't find ext content.Client will filter messages by tag also.
                    log.error("[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}, topic={}",
                        cqUnit.getTagsCode(), cqUnit.getPos(), cqUnit.getPos(), getTopic());
                }
            }
            return cqUnit;
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

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    @Override
    public long rollNextFile(final long nextBeginOffset) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return nextBeginOffset + totalUnitsInFile - nextBeginOffset % totalUnitsInFile;
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
        return CQType.SimpleCQ;
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    @Override
    public void destroy() {
        this.setMaxPhysicOffset(-1);
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    @Override
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.messageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        long physicalOffsetFrom = from * CQ_STORE_UNIT_SIZE;
        long physicalOffsetTo = to * CQ_STORE_UNIT_SIZE;
        List<MappedFile> mappedFiles = mappedFileQueue.range(physicalOffsetFrom, physicalOffsetTo);
        if (mappedFiles.isEmpty()) {
            return -1;
        }

        boolean sample = false;
        long match = 0;
        long raw = 0;

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
                        ConsumeQueueExt.CqExtUnit ext = null;
                        if (isExtWriteEnable()) {
                            ext = consumeQueueExt.get(tagCode);
                            tagCode = ext.getTagsCode();
                        }
                        if (filter.isMatchedByConsumeQueue(tagCode, ext)) {
                            match++;
                        }
                        raw++;
                        current += CQ_STORE_UNIT_SIZE;

                        if (raw >= messageStore.getMessageStoreConfig().getMaxConsumeQueueScan()) {
                            sample = true;
                            break;
                        }

                        if (match > messageStore.getMessageStoreConfig().getSampleCountThreshold()) {
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
        // Because the file version cq requires that files are continuous,
        // If existing cq not be completely deleted, new cq can not initialize with given offset.
        destroy();

        // correct min offset
        // TODO: when min commitLog offset is 0 and restart store, min offset of cq may be set to 0 incorrectly
        setMinLogicOffset(offset * ConsumeQueue.CQ_STORE_UNIT_SIZE);

        // transientStorePool is null, only need set wrote position here
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile(offset * ConsumeQueue.CQ_STORE_UNIT_SIZE, true);
        fillPreBlank(mappedFile, offset * ConsumeQueue.CQ_STORE_UNIT_SIZE);

        flush(0);
    }
}
