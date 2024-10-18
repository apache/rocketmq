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
package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStreamFactory;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileSegment implements Comparable<FileSegment>, FileSegmentProvider {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected static final Long GET_FILE_SIZE_ERROR = -1L;

    protected final long baseOffset;
    protected final String filePath;
    protected final FileSegmentType fileType;
    protected final MessageStoreConfig storeConfig;

    protected final long maxSize;
    protected final ReentrantLock fileLock = new ReentrantLock();
    protected final Semaphore commitLock = new Semaphore(1);

    protected volatile boolean closed = false;
    protected volatile long minTimestamp = Long.MAX_VALUE;
    protected volatile long maxTimestamp = Long.MAX_VALUE;
    protected volatile long commitPosition = 0L;
    protected volatile long appendPosition = 0L;

    protected volatile List<ByteBuffer> bufferList = new ArrayList<>();
    protected volatile FileSegmentInputStream fileSegmentInputStream;
    protected volatile CompletableFuture<Boolean> flightCommitRequest;

    public FileSegment(MessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        this.storeConfig = storeConfig;
        this.fileType = fileType;
        this.filePath = filePath;
        this.baseOffset = baseOffset;
        this.maxSize = this.getMaxSizeByFileType();
    }

    @Override
    public int compareTo(FileSegment o) {
        return Long.compare(this.baseOffset, o.baseOffset);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public void initPosition(long pos) {
        fileLock.lock();
        try {
            this.commitPosition = pos;
            this.appendPosition = pos;
        } finally {
            fileLock.unlock();
        }
    }

    public long getCommitPosition() {
        return commitPosition;
    }

    public long getAppendPosition() {
        return appendPosition;
    }

    public long getCommitOffset() {
        return baseOffset + commitPosition;
    }

    public long getAppendOffset() {
        return baseOffset + appendPosition;
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public long getMaxSizeByFileType() {
        switch (fileType) {
            case COMMIT_LOG:
                return storeConfig.getTieredStoreCommitLogMaxSize();
            case CONSUME_QUEUE:
                return storeConfig.getTieredStoreConsumeQueueMaxSize();
            case INDEX:
                return storeConfig.getTieredStoreOriginalIndexFileMaxSize();
            case INDEX_COMPACTED:
                return storeConfig.getTieredStoreCompactedIndexFileMaxSize();
            default:
                return Long.MAX_VALUE;
        }
    }

    public long getMaxSize() {
        return maxSize;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        fileLock.lock();
        try {
            this.closed = true;
        } finally {
            fileLock.unlock();
        }
    }

    protected List<ByteBuffer> borrowBuffer() {
        List<ByteBuffer> temp;
        fileLock.lock();
        try {
            temp = bufferList;
            bufferList = new ArrayList<>();
        } finally {
            fileLock.unlock();
        }
        return temp;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    protected void updateTimestamp(long timestamp) {
        fileLock.lock();
        try {
            if (maxTimestamp == Long.MAX_VALUE && minTimestamp == Long.MAX_VALUE) {
                maxTimestamp = timestamp;
                minTimestamp = timestamp;
                return;
            }
            maxTimestamp = Math.max(maxTimestamp, timestamp);
            minTimestamp = Math.min(minTimestamp, timestamp);
        } finally {
            fileLock.unlock();
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public AppendResult append(ByteBuffer buffer, long timestamp) {
        fileLock.lock();
        try {
            if (closed) {
                return AppendResult.FILE_CLOSED;
            }
            if (appendPosition + buffer.remaining() > maxSize) {
                return AppendResult.FILE_FULL;
            }
            if (bufferList.size() >= storeConfig.getTieredStoreMaxGroupCommitCount()) {
                return AppendResult.BUFFER_FULL;
            }
            this.appendPosition += buffer.remaining();
            this.bufferList.add(buffer);
            this.updateTimestamp(timestamp);
        } finally {
            fileLock.unlock();
        }
        return AppendResult.SUCCESS;
    }

    public boolean needCommit() {
        return appendPosition > commitPosition;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public CompletableFuture<Boolean> commitAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(false);
        }

        if (!needCommit()) {
            return CompletableFuture.completedFuture(true);
        }

        // acquire lock
        if (commitLock.drainPermits() <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        // handle last commit error
        if (fileSegmentInputStream != null) {
            long fileSize = this.getSize();
            if (fileSize == GET_FILE_SIZE_ERROR) {
                log.error("FileSegment correct position error, fileName={}, commit={}, append={}, buffer={}",
                    this.getPath(), commitPosition, appendPosition, fileSegmentInputStream.getContentLength());
                releaseCommitLock();
                return CompletableFuture.completedFuture(false);
            }
            if (correctPosition(fileSize)) {
                fileSegmentInputStream = null;
            }
        }

        int bufferSize;
        if (fileSegmentInputStream != null) {
            fileSegmentInputStream.rewind();
            bufferSize = fileSegmentInputStream.available();
        } else {
            List<ByteBuffer> bufferList = this.borrowBuffer();
            bufferSize = bufferList.stream().mapToInt(ByteBuffer::remaining).sum();
            if (bufferSize == 0) {
                releaseCommitLock();
                return CompletableFuture.completedFuture(true);
            }
            fileSegmentInputStream = FileSegmentInputStreamFactory.build(
                fileType, this.getCommitOffset(), bufferList, null, bufferSize);
        }

        boolean append = fileType != FileSegmentType.INDEX && fileType != FileSegmentType.INDEX_COMPACTED;
        return flightCommitRequest =
            this.commit0(fileSegmentInputStream, commitPosition, bufferSize, append)
                .thenApply(result -> {
                    if (result) {
                        commitPosition += bufferSize;
                        fileSegmentInputStream = null;
                        return true;
                    } else {
                        fileSegmentInputStream.rewind();
                        return false;
                    }
                })
                .exceptionally(this::handleCommitException)
                .whenComplete((result, e) -> releaseCommitLock());
    }

    private boolean handleCommitException(Throwable e) {

        log.warn("FileSegment commit exception, filePath={}", this.filePath, e);

        // Get root cause here
        Throwable rootCause = e.getCause() != null ? e.getCause() : e;

        long fileSize = rootCause instanceof TieredStoreException ?
            ((TieredStoreException) rootCause).getPosition() : this.getSize();

        long expectPosition = commitPosition + fileSegmentInputStream.getContentLength();
        if (fileSize == GET_FILE_SIZE_ERROR) {
            log.error("Get file size error after commit, FileName: {}, Commit: {}, Content: {}, Expect: {}, Append: {}",
                this.getPath(), commitPosition, fileSegmentInputStream.getContentLength(), expectPosition, appendPosition);
            return false;
        }

        if (correctPosition(fileSize)) {
            fileSegmentInputStream = null;
            return true;
        } else {
            fileSegmentInputStream.rewind();
            return false;
        }
    }

    private void releaseCommitLock() {
        if (commitLock.availablePermits() == 0) {
            commitLock.release();
        }
    }

    /**
     * return true to clear buffer
     */
    private boolean correctPosition(long fileSize) {

        // Current we have three offsets here: commit offset, expect offset, file size.
        // We guarantee that the commit offset is less than or equal to the expect offset.
        // Max offset will increase because we can continuously put in new buffers

        // We are believing that the file size returned by the server is correct,
        // can reset the commit offset to the file size reported by the storage system.

        long expectPosition = commitPosition + fileSegmentInputStream.getContentLength();
        commitPosition = fileSize;
        return expectPosition == fileSize;
    }

    public ByteBuffer read(long position, int length) {
        return readAsync(position, length).join();
    }

    public CompletableFuture<ByteBuffer> readAsync(long position, int length) {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        if (position < 0 || position >= commitPosition) {
            future.completeExceptionally(new TieredStoreException(
                TieredStoreErrorCode.ILLEGAL_PARAM, "FileSegment read position is illegal position"));
            return future;
        }

        if (length <= 0) {
            future.completeExceptionally(new TieredStoreException(
                TieredStoreErrorCode.ILLEGAL_PARAM, "FileSegment read length illegal"));
            return future;
        }

        int readableBytes = (int) (commitPosition - position);
        if (readableBytes < length) {
            length = readableBytes;
            log.debug("FileSegment#readAsync, expect request position is greater than commit position, " +
                    "file: {}, request position: {}, commit position: {}, change length from {} to {}",
                getPath(), position, commitPosition, length, readableBytes);
        }
        return this.read0(position, length);
    }
}
