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
package org.apache.rocketmq.tieredstore.file;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.metadata.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.provider.FileSegmentAllocator;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredFlatFile {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final String filePath;
    private final FileSegmentType fileType;
    private final TieredMetadataStore tieredMetadataStore;

    private volatile long baseOffset = -1L;
    private final FileSegmentAllocator fileSegmentAllocator;
    private final List<TieredFileSegment> fileSegmentList;
    private final List<TieredFileSegment> needCommitFileSegmentList;
    private final ReentrantReadWriteLock fileSegmentLock;

    public TieredFlatFile(FileSegmentAllocator fileSegmentAllocator,
        FileSegmentType fileType, String filePath) {

        this.fileType = fileType;
        this.filePath = filePath;
        this.fileSegmentList = new LinkedList<>();
        this.fileSegmentLock = new ReentrantReadWriteLock();
        this.fileSegmentAllocator = fileSegmentAllocator;
        this.needCommitFileSegmentList = new CopyOnWriteArrayList<>();
        this.tieredMetadataStore = TieredStoreUtil.getMetadataStore(fileSegmentAllocator.getStoreConfig());
        this.recoverMetadata();

        if (fileType != FileSegmentType.INDEX) {
            checkAndFixFileSize();
        }
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public void setBaseOffset(long baseOffset) {
        if (fileSegmentList.size() > 0) {
            throw new IllegalStateException("Can not set base offset after file segment has been created");
        }
        this.baseOffset = baseOffset;
    }

    public long getMinOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentList.isEmpty()) {
                return baseOffset;
            }
            return fileSegmentList.get(0).getBaseOffset();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    public long getCommitOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentList.isEmpty()) {
                return baseOffset;
            }
            return fileSegmentList.get(fileSegmentList.size() - 1).getCommitOffset();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    public long getMaxOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentList.isEmpty()) {
                return baseOffset;
            }
            return fileSegmentList.get(fileSegmentList.size() - 1).getMaxOffset();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    public long getDispatchCommitOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentList.isEmpty()) {
                return 0;
            }
            return fileSegmentList.get(fileSegmentList.size() - 1).getDispatchCommitOffset();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public List<TieredFileSegment> getFileSegmentList() {
        return fileSegmentList;
    }

    protected void recoverMetadata() {
        fileSegmentList.clear();
        needCommitFileSegmentList.clear();

        tieredMetadataStore.iterateFileSegment(filePath, fileType, metadata -> {
            if (metadata.getStatus() == FileSegmentMetadata.STATUS_DELETED) {
                return;
            }

            TieredFileSegment segment = this.newSegment(fileType, metadata.getBaseOffset(), false);
            segment.initPosition(metadata.getSize());
            segment.setMinTimestamp(metadata.getBeginTimestamp());
            segment.setMaxTimestamp(metadata.getEndTimestamp());
            if (metadata.getStatus() == FileSegmentMetadata.STATUS_SEALED) {
                segment.setFull(false);
            }

            // TODO check coda/size
            fileSegmentList.add(segment);
        });

        if (!fileSegmentList.isEmpty()) {
            fileSegmentList.sort(Comparator.comparingLong(TieredFileSegment::getBaseOffset));
            baseOffset = fileSegmentList.get(0).getBaseOffset();
            needCommitFileSegmentList.addAll(
                fileSegmentList.stream().filter(segment -> !segment.isFull()).collect(Collectors.toList()));
        }
    }

    private FileSegmentMetadata getOrCreateFileSegmentMetadata(TieredFileSegment fileSegment) {

        FileSegmentMetadata metadata = tieredMetadataStore.getFileSegment(
            fileSegment.getPath(), fileSegment.getFileType(), fileSegment.getBaseOffset());

        if (metadata != null) {
            return metadata;
        }

        // Note: file segment path may not the same as file base path, use base path here.
        metadata = new FileSegmentMetadata(
            this.filePath, fileSegment.getBaseOffset(), fileSegment.getFileType().getType());

        if (fileSegment.isClosed()) {
            metadata.setStatus(FileSegmentMetadata.STATUS_DELETED);
        }

        metadata.setBeginTimestamp(fileSegment.getMinTimestamp());
        metadata.setEndTimestamp(fileSegment.getMaxTimestamp());

        // Submit to persist
        this.tieredMetadataStore.updateFileSegment(metadata);
        return metadata;
    }

    /**
     * FileQueue Status: Sealed | Sealed | Sealed | Not sealed, Allow appended && Not Full
     */
    @VisibleForTesting
    public void updateFileSegment(TieredFileSegment fileSegment) {
        FileSegmentMetadata segmentMetadata = getOrCreateFileSegmentMetadata(fileSegment);

        if (segmentMetadata.getStatus() == FileSegmentMetadata.STATUS_NEW
            && fileSegment.isFull()
            && !fileSegment.needCommit()) {

            segmentMetadata.markSealed();
        }

        if (fileSegment.isClosed()) {
            segmentMetadata.setStatus(FileSegmentMetadata.STATUS_DELETED);
        }

        segmentMetadata.setSize(fileSegment.getCommitPosition());
        segmentMetadata.setBeginTimestamp(fileSegment.getMinTimestamp());
        segmentMetadata.setEndTimestamp(fileSegment.getMaxTimestamp());
        this.tieredMetadataStore.updateFileSegment(segmentMetadata);
    }

    private void checkAndFixFileSize() {
        for (int i = 1; i < fileSegmentList.size(); i++) {
            TieredFileSegment pre = fileSegmentList.get(i - 1);
            TieredFileSegment cur = fileSegmentList.get(i);
            if (pre.getCommitOffset() != cur.getBaseOffset()) {
                logger.warn("TieredFlatFile#checkAndFixFileSize: file segment has incorrect size: " +
                    "filePath:{}, file type: {}, base offset: {}", filePath, fileType, pre.getBaseOffset());
                try {
                    long actualSize = pre.getSize();
                    if (pre.getBaseOffset() + actualSize != cur.getBaseOffset()) {
                        logger.error("[Bug]TieredFlatFile#checkAndFixFileSize: " +
                                "file segment has incorrect size and can not fix: " +
                                "filePath:{}, file type: {}, base offset: {}, actual size: {}, next file offset: {}",
                            filePath, fileType, pre.getBaseOffset(), actualSize, cur.getBaseOffset());
                        continue;
                    }
                    pre.initPosition(actualSize);
                    this.updateFileSegment(pre);
                } catch (Exception e) {
                    logger.error("TieredFlatFile#checkAndFixFileSize: " +
                            "fix file segment size failed: filePath: {}, file type: {}, base offset: {}",
                        filePath, fileType, pre.getBaseOffset());
                }
            }
        }

        if (!fileSegmentList.isEmpty()) {
            TieredFileSegment lastFile = fileSegmentList.get(fileSegmentList.size() - 1);
            long lastFileSize = lastFile.getSize();
            if (lastFile.getCommitPosition() != lastFileSize) {
                logger.warn("TieredFlatFile#checkAndFixFileSize: fix last file {} size: origin: {}, actual: {}",
                    lastFile.getPath(), lastFile.getCommitOffset() - lastFile.getBaseOffset(), lastFileSize);
                lastFile.initPosition(lastFileSize);
            }
        }
    }

    private TieredFileSegment newSegment(FileSegmentType fileType, long baseOffset, boolean createMetadata) {
        TieredFileSegment segment = null;
        try {
            segment = fileSegmentAllocator.createSegment(fileType, filePath, baseOffset);
            if (fileType != FileSegmentType.INDEX) {
                segment.createFile();
            }
            if (createMetadata) {
                this.updateFileSegment(segment);
            }
        } catch (Exception e) {
            logger.error("create file segment failed: filePath:{}, file type: {}, base offset: {}",
                filePath, fileType, baseOffset, e);
        }
        return segment;
    }

    public void rollingNewFile() {
        TieredFileSegment segment = getFileToWrite();
        segment.setFull();
        // create new segment
        getFileToWrite();
    }

    public int getFileSegmentCount() {
        return fileSegmentList.size();
    }

    @Nullable
    protected TieredFileSegment getFileByIndex(int index) {
        fileSegmentLock.readLock().lock();
        try {
            if (index < fileSegmentList.size()) {
                return fileSegmentList.get(index);
            }
            return null;
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    protected TieredFileSegment getFileToWrite() {
        if (baseOffset == -1) {
            throw new IllegalStateException("need to set base offset before create file segment");
        }
        fileSegmentLock.readLock().lock();
        try {
            if (!fileSegmentList.isEmpty()) {
                TieredFileSegment fileSegment = fileSegmentList.get(fileSegmentList.size() - 1);
                if (!fileSegment.isFull()) {
                    return fileSegment;
                }
            }
        } finally {
            fileSegmentLock.readLock().unlock();
        }
        // Create new file segment
        fileSegmentLock.writeLock().lock();
        try {
            long offset = baseOffset;
            if (!fileSegmentList.isEmpty()) {
                TieredFileSegment segment = fileSegmentList.get(fileSegmentList.size() - 1);
                if (!segment.isFull()) {
                    return segment;
                }
                if (segment.commit()) {
                    try {
                        this.updateFileSegment(segment);
                    } catch (Exception e) {
                        return segment;
                    }
                } else {
                    return segment;
                }

                offset = segment.getMaxOffset();
            }
            TieredFileSegment fileSegment = this.newSegment(fileType, offset, true);
            fileSegmentList.add(fileSegment);
            needCommitFileSegmentList.add(fileSegment);

            Collections.sort(fileSegmentList);

            logger.debug("Create a new file segment: baseOffset: {}, file: {}, file type: {}", baseOffset, fileSegment.getPath(), fileType);
            return fileSegment;
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }

    @Nullable
    protected TieredFileSegment getFileByTime(long timestamp, BoundaryType boundaryType) {
        fileSegmentLock.readLock().lock();
        try {
            List<TieredFileSegment> segmentList = fileSegmentList.stream()
                .sorted(boundaryType == BoundaryType.UPPER ? Comparator.comparingLong(TieredFileSegment::getMaxTimestamp) : Comparator.comparingLong(TieredFileSegment::getMinTimestamp))
                .filter(segment -> boundaryType == BoundaryType.UPPER ? segment.getMaxTimestamp() >= timestamp : segment.getMinTimestamp() <= timestamp)
                .collect(Collectors.toList());
            if (!segmentList.isEmpty()) {
                return boundaryType == BoundaryType.UPPER ? segmentList.get(0) : segmentList.get(segmentList.size() - 1);
            }
            return fileSegmentList.isEmpty() ? null : fileSegmentList.get(fileSegmentList.size() - 1);
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    protected List<TieredFileSegment> getFileListByTime(long beginTime, long endTime) {
        fileSegmentLock.readLock().lock();
        try {
            return fileSegmentList.stream()
                .filter(segment -> Math.max(beginTime, segment.getMinTimestamp()) <= Math.min(endTime, segment.getMaxTimestamp()))
                .collect(Collectors.toList());
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    protected int getSegmentIndexByOffset(long offset) {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentList.size() == 0) {
                return -1;
            }

            int left = 0;
            int right = fileSegmentList.size() - 1;
            int mid = (left + right) / 2;

            long firstSegmentOffset = fileSegmentList.get(left).getBaseOffset();
            long lastSegmentOffset = fileSegmentList.get(right).getCommitOffset();
            long midSegmentOffset = fileSegmentList.get(mid).getBaseOffset();

            if (offset < firstSegmentOffset || offset > lastSegmentOffset) {
                return -1;
            }

            while (left < right - 1) {
                if (offset == midSegmentOffset) {
                    return mid;
                }
                if (offset < midSegmentOffset) {
                    right = mid;
                } else {
                    left = mid;
                }
                mid = (left + right) / 2;
                midSegmentOffset = fileSegmentList.get(mid).getBaseOffset();
            }
            return offset < fileSegmentList.get(right).getBaseOffset() ? mid : right;
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    public AppendResult append(ByteBuffer byteBuf) {
        return append(byteBuf, Long.MAX_VALUE, false);
    }

    public AppendResult append(ByteBuffer byteBuf, long timeStamp) {
        return append(byteBuf, timeStamp, false);
    }

    public AppendResult append(ByteBuffer byteBuf, long timeStamp, boolean commit) {
        TieredFileSegment fileSegment = getFileToWrite();
        AppendResult result = fileSegment.append(byteBuf, timeStamp);
        if (commit && result == AppendResult.BUFFER_FULL && fileSegment.commit()) {
            result = fileSegment.append(byteBuf, timeStamp);
        }
        if (result == AppendResult.FILE_FULL) {
            // write to new file
            return getFileToWrite().append(byteBuf, timeStamp);
        }
        return result;
    }

    public void cleanExpiredFile(long expireTimestamp) {
        Set<Long> needToDeleteSet = new HashSet<>();
        try {
            tieredMetadataStore.iterateFileSegment(filePath, fileType, metadata -> {
                if (metadata.getEndTimestamp() < expireTimestamp) {
                    needToDeleteSet.add(metadata.getBaseOffset());
                }
            });
        } catch (Exception e) {
            logger.error("clean expired failed: filePath: {}, file type: {}, expire timestamp: {}",
                filePath, fileType, expireTimestamp);
        }

        if (needToDeleteSet.isEmpty()) {
            return;
        }

        fileSegmentLock.writeLock().lock();
        try {
            for (int i = 0; i < fileSegmentList.size(); i++) {
                try {
                    TieredFileSegment fileSegment = fileSegmentList.get(i);
                    if (needToDeleteSet.contains(fileSegment.getBaseOffset())) {
                        fileSegment.close();
                        fileSegmentList.remove(fileSegment);
                        needCommitFileSegmentList.remove(fileSegment);
                        i--;
                        this.updateFileSegment(fileSegment);
                        logger.info("expired file {} is been cleaned", fileSegment.getPath());
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("clean expired file failed: filePath: {}, file type: {}, expire timestamp: {}",
                        filePath, fileType, expireTimestamp, e);
                }
            }
            if (fileSegmentList.size() > 0) {
                baseOffset = fileSegmentList.get(0).getBaseOffset();
            } else if (fileType == FileSegmentType.CONSUME_QUEUE) {
                baseOffset = -1;
            } else {
                baseOffset = 0;
            }
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    protected List<TieredFileSegment> getNeedCommitFileSegmentList() {
        return needCommitFileSegmentList;
    }

    public void destroyExpiredFile() {
        try {
            tieredMetadataStore.iterateFileSegment(filePath, fileType, metadata -> {
                if (metadata.getStatus() == FileSegmentMetadata.STATUS_DELETED) {
                    try {
                        TieredFileSegment fileSegment =
                            this.newSegment(fileType, metadata.getBaseOffset(), false);
                        fileSegment.destroyFile();
                        if (!fileSegment.exists()) {
                            tieredMetadataStore.deleteFileSegment(filePath, fileType, metadata.getBaseOffset());
                            logger.info("expired file {} is been destroyed", fileSegment.getPath());
                        }
                    } catch (Exception e) {
                        logger.error("destroy expired failed: file path: {}, file type: {}",
                            filePath, fileType, e);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("destroy expired file failed: file path: {}, file type: {}", filePath, fileType);
        }
    }

    public void commit(boolean sync) {
        ArrayList<CompletableFuture<Void>> futureList = new ArrayList<>();
        try {
            for (TieredFileSegment segment : needCommitFileSegmentList) {
                if (segment.isClosed()) {
                    continue;
                }
                futureList.add(segment
                    .commitAsync()
                    .thenAccept(success -> {
                        try {
                            this.updateFileSegment(segment);
                        } catch (Exception e) {
                            // TODO handle update segment metadata failed exception
                            logger.error("update file segment metadata failed: " +
                                    "file path: {}, file type: {}, base offset: {}",
                                filePath, fileType, segment.getBaseOffset(), e);
                        }
                        if (segment.isFull() && !segment.needCommit()) {
                            needCommitFileSegmentList.remove(segment);
                        }
                    })
                );
            }
        } catch (Exception e) {
            logger.error("commit file segment failed: topic: {}, queue: {}, file type: {}", filePath, fileType, e);
        }
        if (sync) {
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        }
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        int index = getSegmentIndexByOffset(offset);
        if (index == -1) {
            String errorMsg = String.format("TieredFlatFile#readAsync: offset is illegal, " +
                    "file path: %s, file type: %s, start: %d, length: %d, file num: %d",
                filePath, fileType, offset, length, fileSegmentList.size());
            logger.error(errorMsg);
            throw new TieredStoreException(TieredStoreErrorCode.ILLEGAL_OFFSET, errorMsg);
        }
        TieredFileSegment fileSegment1;
        TieredFileSegment fileSegment2 = null;
        fileSegmentLock.readLock().lock();
        try {
            fileSegment1 = fileSegmentList.get(index);
            if (offset + length > fileSegment1.getCommitOffset()) {
                if (fileSegmentList.size() > index + 1) {
                    fileSegment2 = fileSegmentList.get(index + 1);
                }
            }
        } finally {
            fileSegmentLock.readLock().unlock();
        }
        if (fileSegment2 == null) {
            return fileSegment1.readAsync(offset - fileSegment1.getBaseOffset(), length);
        }
        int segment1Length = (int) (fileSegment1.getCommitOffset() - offset);
        return fileSegment1.readAsync(offset - fileSegment1.getBaseOffset(), segment1Length)
            .thenCombine(fileSegment2.readAsync(0, length - segment1Length), (buffer1, buffer2) -> {
                ByteBuffer compositeBuffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
                compositeBuffer.put(buffer1).put(buffer2);
                compositeBuffer.flip();
                return compositeBuffer;
            });
    }

    public void destroy() {
        fileSegmentLock.writeLock().lock();
        try {
            for (TieredFileSegment fileSegment : fileSegmentList) {
                fileSegment.close();
                try {
                    this.updateFileSegment(fileSegment);
                } catch (Exception e) {
                    logger.error("TieredFlatFile#destroy: mark file segment: {} is deleted failed", fileSegment.getPath(), e);
                }
                fileSegment.destroyFile();
            }
            fileSegmentList.clear();
            needCommitFileSegmentList.clear();
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }
}
