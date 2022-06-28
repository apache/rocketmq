package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.logfile.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class SparseConsumeQueue extends BatchConsumeQueue {

    public SparseConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore defaultMessageStore) {
        super(topic, queueId, storePath, mappedFileSize, defaultMessageStore);
    }

    public SparseConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore defaultMessageStore,
        final String subfolder) {
        super(topic, queueId, storePath, mappedFileSize, defaultMessageStore, subfolder);
    }

    @Override
    public void recover() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile == null) {
            return;
        }
        ByteBuffer byteBuffer = lastMappedFile.sliceByteBuffer();
        int mappedFileOffset = 0;
        for (int i = 0; i < mappedFileSize; i += CQ_STORE_UNIT_SIZE) {
            byteBuffer.position(i);
            long offset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            byteBuffer.getLong();   //tagscode
            byteBuffer.getLong();   //timestamp
            long msgBaseOffset = byteBuffer.getLong();
            short batchSize = byteBuffer.getShort();
            if (offset >= 0 && size > 0 && msgBaseOffset >= 0 && batchSize > 0) {
                mappedFileOffset += CQ_STORE_UNIT_SIZE;
            } else {
                log.info("Recover current batch consume queue file over, file:{} offset:{} size:{} msgBaseOffset:{} batchSize:{} mappedFileOffset:{}",
                    lastMappedFile.getFileName(), offset, size, msgBaseOffset, batchSize, mappedFileOffset);
                break;
            }
        }

        lastMappedFile.setWrotePosition(mappedFileOffset);
        lastMappedFile.setFlushedPosition(mappedFileOffset);
        lastMappedFile.setCommittedPosition(mappedFileOffset);
        reviseMaxAndMinOffsetInQueue();
    }

    public ReferredIterator<CqUnit> iterateFromOrNext(long startOffset) {
        SelectMappedBufferResult sbr = getBatchMsgIndexOrNextBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new BatchConsumeQueueIterator(sbr);
    }

    /**
     * Gets SelectMappedBufferResult by batch-message offset, if not found will return the next valid offset buffer
     * Node: the caller is responsible for the release of SelectMappedBufferResult
     * @param msgOffset
     * @return SelectMappedBufferResult
     */
    public SelectMappedBufferResult getBatchMsgIndexOrNextBuffer(final long msgOffset) {

        MappedFile targetBcq;

        if (msgOffset <= minOffsetInQueue) {
            targetBcq = mappedFileQueue.getFirstMappedFile();
        } else {
            targetBcq = searchFileByOffsetOrRight(msgOffset);
        }

        if (targetBcq == null) {
            return null;
        }

        BatchOffsetIndex minOffset = getMinMsgOffset(targetBcq, false, false);
        BatchOffsetIndex maxOffset = getMaxMsgOffset(targetBcq, false, false);
        if (null == minOffset || null == maxOffset) {
            return null;
        }

        SelectMappedBufferResult sbr = minOffset.getMappedFile().selectMappedBuffer(0);
        try {
            ByteBuffer byteBuffer = sbr.getByteBuffer();
            int left = minOffset.getIndexPos();
            int right = maxOffset.getIndexPos();
            int mid = binarySearchRight(byteBuffer, left, right, CQ_STORE_UNIT_SIZE, MSG_BASE_OFFSET_INDEX, msgOffset);
            if (mid != -1) {
                return minOffset.getMappedFile().selectMappedBuffer(mid);
            }
        } finally {
            sbr.release();
        }

        return null;
    }

    protected MappedFile searchOffsetFromCacheOrRight(long msgOffset) {
        Map.Entry<Long, MappedFile> ceilingEntry = this.offsetCache.ceilingEntry(msgOffset);
        if (ceilingEntry == null) {
            return null;
        } else {
            return ceilingEntry.getValue();
        }
    }

    protected MappedFile searchFileByOffsetOrRight(long msgOffset) {
        MappedFile targetBcq = null;
        boolean searchBcqByCacheEnable = this.defaultMessageStore.getMessageStoreConfig().isSearchBcqByCacheEnable();
        if (searchBcqByCacheEnable) {
            // it's not the last BCQ file, so search it through cache.
            targetBcq = this.searchOffsetFromCacheOrRight(msgOffset);
            // not found in cache
            if (targetBcq == null) {
                MappedFile firstBcq = mappedFileQueue.getFirstMappedFile();
                BatchOffsetIndex minForFirstBcq = getMinMsgOffset(firstBcq, false, false);
                if (minForFirstBcq != null && minForFirstBcq.getMsgOffset() <= msgOffset && msgOffset < maxOffsetInQueue) {
                    // old search logic
                    targetBcq = this.searchOffsetFromFilesOrRight(msgOffset);
                }
                log.warn("cache is not working on BCQ [Topic: {}, QueueId: {}] for msgOffset: {}, targetBcq: {}", this.topic, this.queueId, msgOffset, targetBcq);
            }
        } else {
            // old search logic
            targetBcq = this.searchOffsetFromFilesOrRight(msgOffset);
        }

        return targetBcq;
    }

    public MappedFile searchOffsetFromFilesOrRight(long msgOffset) {
        MappedFile targetBcq = null;
        // find the mapped file one by one reversely
        int mappedFileNum = this.mappedFileQueue.getMappedFiles().size();
        for (int i = mappedFileNum - 1; i >= 0; i--) {
            MappedFile mappedFile = mappedFileQueue.getMappedFiles().get(i);
            BatchOffsetIndex tmpMinMsgOffset = getMinMsgOffset(mappedFile, false, false);
            BatchOffsetIndex tmpMaxMsgOffset = getMaxMsgOffset(mappedFile, false, false);
            if (null != tmpMaxMsgOffset && tmpMaxMsgOffset.getMsgOffset() < msgOffset) {
                if (i != mappedFileNum - 1) {   //not the last mapped file max msg offset
                    targetBcq = mappedFileQueue.getMappedFiles().get(i + 1);
                    break;
                }
            }

            if (null != tmpMinMsgOffset && tmpMinMsgOffset.getMsgOffset() <= msgOffset
                && null != tmpMaxMsgOffset && msgOffset <= tmpMaxMsgOffset.getMsgOffset()) {
                targetBcq = mappedFile;
                break;
            }
        }

        return targetBcq;
    }

    private MappedFile getPreFile(MappedFile file) {
        int index = mappedFileQueue.getMappedFiles().indexOf(file);
        if (index < 1) {
            // indicate that this is the first file or not found
            return null;
        } else {
            return mappedFileQueue.getMappedFiles().get(index - 1);
        }
    }

    private void cacheOffset(MappedFile file, Function<MappedFile, BatchOffsetIndex> offsetGetFunc) {
        try {
            BatchOffsetIndex offset = offsetGetFunc.apply(file);
            if (offset != null) {
                this.offsetCache.put(offset.getMsgOffset(), offset.getMappedFile());
                this.timeCache.put(offset.getStoreTimestamp(), offset.getMappedFile());
            }
        } catch (Exception e) {
            log.error("Failed caching offset and time on BCQ [Topic: {}, QueueId: {}, File: {}]",
                this.topic, this.queueId, file);
        }
    }

    @Override
    protected void cacheBcq(MappedFile bcq) {
        MappedFile file = getPreFile(bcq);
        if (file != null) {
            cacheOffset(file, m -> getMaxMsgOffset(m, false, true));
        }
    }

    public void putEndPositionInfo(MappedFile mappedFile) {
        // cache max offset
        if (!mappedFile.isFull()) {
            this.byteBufferItem.flip();
            this.byteBufferItem.limit(CQ_STORE_UNIT_SIZE);
            this.byteBufferItem.putLong(-1);
            this.byteBufferItem.putInt(0);
            this.byteBufferItem.putLong(0);
            this.byteBufferItem.putLong(0);
            this.byteBufferItem.putLong(0);
            this.byteBufferItem.putShort((short)0);
            this.byteBufferItem.putInt(INVALID_POS);
            this.byteBufferItem.putInt(0); // 4 bytes reserved
            boolean appendRes = mappedFile.appendMessage(this.byteBufferItem.array());
            if (!appendRes) {
                log.error("append end position info into {} failed", mappedFile.getFileName());
            }
        }

        cacheOffset(mappedFile, m -> getMaxMsgOffset(m, false, true));
    }

    public MappedFile createFile(final long physicalOffset) throws IOException {
        // cache max offset
        return mappedFileQueue.tryCreateMappedFile(physicalOffset);
    }

    public boolean isLastFileFull() {
        if (mappedFileQueue.getLastMappedFile() != null) {
            return mappedFileQueue.getLastMappedFile().isFull();
        } else {
            return true;
        }
    }

    public boolean shouldRoll() {
        if (mappedFileQueue.getLastMappedFile() == null) {
            return true;
        }
        if (mappedFileQueue.getLastMappedFile().isFull()) {
            return true;
        }
        if (mappedFileQueue.getLastMappedFile().getWrotePosition() + BatchConsumeQueue.CQ_STORE_UNIT_SIZE
            > mappedFileQueue.getMappedFileSize()) {
            return true;
        }

        return false;
    }

    public boolean containsOffsetFile(final long physicalOffset) {
        String fileName = UtilAll.offset2FileName(physicalOffset);
        return mappedFileQueue.getMappedFiles().stream()
            .anyMatch(mf -> Objects.equals(mf.getFile().getName(), fileName));
    }

    @Override
    protected BatchOffsetIndex getMaxMsgOffset(MappedFile mappedFile, boolean getBatchSize, boolean getStoreTime) {
        if (mappedFile == null || mappedFile.getReadPosition() < CQ_STORE_UNIT_SIZE) {
            return null;
        }

        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        for (int i = mappedFile.getReadPosition() - CQ_STORE_UNIT_SIZE; i >= 0; i -= CQ_STORE_UNIT_SIZE) {
            byteBuffer.position(i);
            long offset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            byteBuffer.getLong();   //tagscode
            long timestamp = byteBuffer.getLong();//timestamp
            long msgBaseOffset = byteBuffer.getLong();
            short batchSize = byteBuffer.getShort();
            if (offset >= 0 && size > 0 && msgBaseOffset >= 0 && batchSize > 0) {
//                mappedFile.setWrotePosition(i + CQ_STORE_UNIT_SIZE);
//                mappedFile.setFlushedPosition(i + CQ_STORE_UNIT_SIZE);
//                mappedFile.setCommittedPosition(i + CQ_STORE_UNIT_SIZE);
                return new BatchOffsetIndex(mappedFile, i, msgBaseOffset, batchSize, timestamp);
            }
        }

        return null;
    }

    public long getMaxMsgOffsetFromFile(String simpleFileName) {
        MappedFile mappedFile = mappedFileQueue.getMappedFiles().stream()
            .filter(m -> Objects.equals(m.getFile().getName(), simpleFileName))
            .findFirst()
            .orElse(null);

        if (mappedFile == null) {
            return -1;
        }

        BatchOffsetIndex max = getMaxMsgOffset(mappedFile, false, false);
        if (max == null) {
            return -1;
        }
        return max.getMsgOffset();
    }

    private void refreshMaxCache() {
        doRefreshCache(m -> getMaxMsgOffset(m, false, true));
    }

    @Override
    protected void refreshCache() {
        refreshMaxCache();
    }

    public void refresh() {
        reviseMaxAndMinOffsetInQueue();
        refreshCache();
    }
}
