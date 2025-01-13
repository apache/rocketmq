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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatAppendFile {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    public static final long GET_FILE_SIZE_ERROR = -1L;

    protected final String filePath;
    protected final FileSegmentType fileType;
    protected final MetadataStore metadataStore;
    protected final FileSegmentFactory fileSegmentFactory;
    protected final ReentrantReadWriteLock fileSegmentLock;
    protected final CopyOnWriteArrayList<FileSegment> fileSegmentTable;

    protected FlatAppendFile(FileSegmentFactory fileSegmentFactory, FileSegmentType fileType, String filePath) {

        this.fileType = fileType;
        this.filePath = filePath;
        this.metadataStore = fileSegmentFactory.getMetadataStore();
        this.fileSegmentFactory = fileSegmentFactory;
        this.fileSegmentLock = new ReentrantReadWriteLock();
        this.fileSegmentTable = new CopyOnWriteArrayList<>();
        this.recover();
        this.recoverFileSize();
    }

    public void recover() {
        List<FileSegment> fileSegmentList = new ArrayList<>();
        this.metadataStore.iterateFileSegment(this.filePath, this.fileType, metadata -> {
            FileSegment fileSegment = this.fileSegmentFactory.createSegment(
                this.fileType, metadata.getPath(), metadata.getBaseOffset());
            fileSegment.initPosition(metadata.getSize());
            fileSegment.setMinTimestamp(metadata.getBeginTimestamp());
            fileSegment.setMaxTimestamp(metadata.getEndTimestamp());
            fileSegmentList.add(fileSegment);
        });
        this.fileSegmentTable.addAll(fileSegmentList.stream().sorted().collect(Collectors.toList()));
    }

    public void recoverFileSize() {
        if (fileSegmentTable.isEmpty() || FileSegmentType.INDEX.equals(fileType)) {
            return;
        }
        FileSegment fileSegment = fileSegmentTable.get(fileSegmentTable.size() - 1);
        long fileSize = fileSegment.getSize();
        if (fileSize == GET_FILE_SIZE_ERROR) {
            log.warn("FlatAppendFile get last file size error, filePath: {}", this.filePath);
            return;
        }
        if (fileSegment.getCommitPosition() != fileSize) {
            fileSegment.initPosition(fileSize);
            flushFileSegmentMeta(fileSegment);
            log.warn("FlatAppendFile last file size not correct, filePath: {}", this.filePath);
        }
    }

    public void initOffset(long offset) {
        if (this.fileSegmentTable.isEmpty()) {
            FileSegment fileSegment = fileSegmentFactory.createSegment(fileType, filePath, offset);
            fileSegment.initPosition(fileSegment.getSize());
            this.flushFileSegmentMeta(fileSegment);
            this.fileSegmentTable.add(fileSegment);
        }
    }

    public void flushFileSegmentMeta(FileSegment fileSegment) {
        FileSegmentMetadata metadata = this.metadataStore.getFileSegment(
            this.filePath, fileSegment.getFileType(), fileSegment.getBaseOffset());
        if (metadata == null) {
            metadata = new FileSegmentMetadata(
                this.filePath, fileSegment.getBaseOffset(), fileSegment.getFileType().getCode());
            metadata.setCreateTimestamp(System.currentTimeMillis());
        }
        metadata.setSize(fileSegment.getCommitPosition());
        metadata.setBeginTimestamp(fileSegment.getMinTimestamp());
        metadata.setEndTimestamp(fileSegment.getMaxTimestamp());
        this.metadataStore.updateFileSegment(metadata);
    }

    public String getFilePath() {
        return filePath;
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public List<FileSegment> getFileSegmentList() {
        return fileSegmentTable;
    }

    public long getMinOffset() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? GET_FILE_SIZE_ERROR : list.get(0).getBaseOffset();
    }

    public long getCommitOffset() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? GET_FILE_SIZE_ERROR : list.get(list.size() - 1).getCommitOffset();
    }

    public long getAppendOffset() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? GET_FILE_SIZE_ERROR : list.get(list.size() - 1).getAppendOffset();
    }

    public long getMinTimestamp() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? GET_FILE_SIZE_ERROR : list.get(0).getMinTimestamp();
    }

    public long getMaxTimestamp() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? GET_FILE_SIZE_ERROR : list.get(list.size() - 1).getMaxTimestamp();
    }

    public FileSegment rollingNewFile(long offset) {
        FileSegment fileSegment;
        fileSegmentLock.writeLock().lock();
        try {
            fileSegment = this.fileSegmentFactory.createSegment(this.fileType, this.filePath, offset);
            this.flushFileSegmentMeta(fileSegment);
            this.fileSegmentTable.add(fileSegment);
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
        return fileSegment;
    }

    public FileSegment getFileToWrite() {
        List<FileSegment> fileSegmentList = this.fileSegmentTable;
        if (fileSegmentList.isEmpty()) {
            throw new IllegalStateException("Need to set base offset before create file segment");
        } else {
            return fileSegmentList.get(fileSegmentList.size() - 1);
        }
    }

    public AppendResult append(ByteBuffer buffer, long timestamp) {
        AppendResult result;
        fileSegmentLock.writeLock().lock();
        try {
            FileSegment fileSegment = this.getFileToWrite();
            result = fileSegment.append(buffer, timestamp);
            if (result == AppendResult.FILE_FULL) {
                boolean commitResult = fileSegment.commitAsync().join();
                log.info("FlatAppendFile#append not successful for the file {} is full, commit result={}",
                    fileSegment.getPath(), commitResult);
                if (commitResult) {
                    this.flushFileSegmentMeta(fileSegment);
                    return this.rollingNewFile(this.getAppendOffset()).append(buffer, timestamp);
                } else {
                    return AppendResult.UNKNOWN_ERROR;
                }
            }
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
        return result;
    }

    public CompletableFuture<Boolean> commitAsync() {
        List<FileSegment> fileSegmentsList = this.fileSegmentTable;
        if (fileSegmentsList.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        FileSegment fileSegment = fileSegmentsList.get(fileSegmentsList.size() - 1);
        return fileSegment.commitAsync().thenApply(success -> {
            if (success) {
                this.flushFileSegmentMeta(fileSegment);
            }
            return success;
        });
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        List<FileSegment> fileSegmentList = this.fileSegmentTable;
        int index = fileSegmentList.size() - 1;
        for (; index >= 0; index--) {
            if (fileSegmentList.get(index).getBaseOffset() <= offset) {
                break;
            }
        }

        if (index < 0) {
            log.error("FlatAppendFile#readAsync offset={} is lower than minimum offset:{}", offset, fileSegmentList.get(0).getBaseOffset());
            throw new IndexOutOfBoundsException("offset is lower than minimum offset");
        }

        FileSegment fileSegment1 = fileSegmentList.get(index);
        FileSegment fileSegment2 = offset + length > fileSegment1.getCommitOffset() &&
            fileSegmentList.size() > index + 1 ? fileSegmentList.get(index + 1) : null;

        if (fileSegment2 == null) {
            return fileSegment1.readAsync(offset - fileSegment1.getBaseOffset(), length);
        }

        int segment1Length = (int) (fileSegment1.getCommitOffset() - offset);
        return fileSegment1.readAsync(offset - fileSegment1.getBaseOffset(), segment1Length)
            .thenCombine(fileSegment2.readAsync(0, length - segment1Length),
                (buffer1, buffer2) -> {
                    ByteBuffer buffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
                    buffer.put(buffer1).put(buffer2);
                    buffer.flip();
                    return buffer;
                });
    }

    public void shutdown() {
        fileSegmentLock.writeLock().lock();
        try {
            fileSegmentTable.forEach(FileSegment::close);
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }

    public void destroyExpiredFile(long expireTimestamp) {
        fileSegmentLock.writeLock().lock();
        try {
            while (!fileSegmentTable.isEmpty()) {

                // first remove expired file from fileSegmentTable
                // then close and delete expired file
                FileSegment fileSegment = fileSegmentTable.get(0);

                if (fileSegment.getMaxTimestamp() != Long.MAX_VALUE &&
                    fileSegment.getMaxTimestamp() > expireTimestamp) {
                    log.debug("FileSegment has not expired, filePath={}, fileType={}, " +
                            "offset={}, expireTimestamp={}, maxTimestamp={}", filePath, fileType,
                        fileSegment.getBaseOffset(), expireTimestamp, fileSegment.getMaxTimestamp());
                    break;
                }

                fileSegment.destroyFile();
                if (!fileSegment.exists()) {
                    fileSegmentTable.remove(0);
                    metadataStore.deleteFileSegment(filePath, fileType, fileSegment.getBaseOffset());
                }
            }
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }

    public void destroy() {
        this.destroyExpiredFile(Long.MAX_VALUE);
    }
}
