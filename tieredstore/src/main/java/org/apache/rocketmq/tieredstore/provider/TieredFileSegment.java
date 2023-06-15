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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.file.TieredCommitLog;
import org.apache.rocketmq.tieredstore.file.TieredConsumeQueue;
import org.apache.rocketmq.tieredstore.file.TieredIndexFile;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStream;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStreamFactory;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public abstract class TieredFileSegment implements Comparable<TieredFileSegment>, TieredStoreProvider {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String filePath;
    protected final long baseOffset;
    protected final FileSegmentType fileType;
    protected final TieredMessageStoreConfig storeConfig;

    private final long maxSize;
    private final ReentrantLock bufferLock;
    private final Semaphore commitLock;

    private volatile boolean full;
    private volatile boolean closed;

    private volatile long minTimestamp;
    private volatile long maxTimestamp;
    private volatile long commitPosition;
    private volatile long appendPosition;

    // only used in commitLog
    private volatile long dispatchCommitOffset = 0;

    private ByteBuffer codaBuffer;
    private List<ByteBuffer> uploadBufferList = new ArrayList<>();
    private CompletableFuture<Boolean> flightCommitRequest = CompletableFuture.completedFuture(false);

    public TieredFileSegment(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        this.storeConfig = storeConfig;
        this.fileType = fileType;
        this.filePath = filePath;
        this.baseOffset = baseOffset;

        this.closed = false;
        this.bufferLock = new ReentrantLock();
        this.commitLock = new Semaphore(1);

        this.commitPosition = 0L;
        this.appendPosition = 0L;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MAX_VALUE;

        // The max segment size of a file is determined by the file type
        this.maxSize = getMaxSizeAccordingFileType(storeConfig);
    }

    private long getMaxSizeAccordingFileType(TieredMessageStoreConfig storeConfig) {
        switch (fileType) {
            case COMMIT_LOG:
                return storeConfig.getTieredStoreCommitLogMaxSize();
            case CONSUME_QUEUE:
                return storeConfig.getTieredStoreConsumeQueueMaxSize();
            case INDEX:
                return Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("Unsupported file type: " + fileType);
        }
    }

    @Override
    public int compareTo(TieredFileSegment o) {
        return Long.compare(this.baseOffset, o.baseOffset);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getCommitOffset() {
        return baseOffset + commitPosition;
    }

    public long getCommitPosition() {
        return commitPosition;
    }

    public long getDispatchCommitOffset() {
        return dispatchCommitOffset;
    }

    public long getMaxOffset() {
        return baseOffset + appendPosition;
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

    public boolean isFull() {
        return full;
    }

    public void setFull() {
        setFull(true);
    }

    public void setFull(boolean appendCoda) {
        bufferLock.lock();
        try {
            full = true;
            if (fileType == FileSegmentType.COMMIT_LOG && appendCoda) {
                appendCoda();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        closed = true;
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public void initPosition(long pos) {
        this.commitPosition = pos;
        this.appendPosition = pos;
    }

    private List<ByteBuffer> rollingUploadBuffer() {
        bufferLock.lock();
        try {
            List<ByteBuffer> tmp = uploadBufferList;
            uploadBufferList = new ArrayList<>();
            return tmp;
        } finally {
            bufferLock.unlock();
        }
    }

    private void sendBackBuffer(TieredFileSegmentInputStream inputStream) {
        bufferLock.lock();
        try {
            List<ByteBuffer> tmpBufferList = inputStream.getUploadBufferList();
            for (ByteBuffer buffer : tmpBufferList) {
                buffer.rewind();
            }
            tmpBufferList.addAll(uploadBufferList);
            uploadBufferList = tmpBufferList;
            if (inputStream.getCodaBuffer() != null) {
                codaBuffer.rewind();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public AppendResult append(ByteBuffer byteBuf, long timeStamp) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        bufferLock.lock();
        try {
            if (full || codaBuffer != null) {
                return AppendResult.FILE_FULL;
            }

            if (fileType == FileSegmentType.INDEX) {
                minTimestamp = byteBuf.getLong(TieredIndexFile.INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION);
                maxTimestamp = byteBuf.getLong(TieredIndexFile.INDEX_FILE_HEADER_END_TIME_STAMP_POSITION);
                appendPosition += byteBuf.remaining();
                uploadBufferList.add(byteBuf);
                setFull();
                return AppendResult.SUCCESS;
            }

            if (appendPosition + byteBuf.remaining() > maxSize) {
                setFull();
                return AppendResult.FILE_FULL;
            }
            if (uploadBufferList.size() > storeConfig.getTieredStoreGroupCommitCount()
                || appendPosition - commitPosition > storeConfig.getTieredStoreGroupCommitSize()) {
                commitAsync();
            }
            if (uploadBufferList.size() > storeConfig.getTieredStoreMaxGroupCommitCount()) {
                logger.debug("TieredFileSegment#append: buffer full: file: {}, upload buffer size: {}",
                    getPath(), uploadBufferList.size());
                return AppendResult.BUFFER_FULL;
            }
            if (timeStamp != Long.MAX_VALUE) {
                maxTimestamp = timeStamp;
                if (minTimestamp == Long.MAX_VALUE) {
                    minTimestamp = timeStamp;
                }
            }
            appendPosition += byteBuf.remaining();
            uploadBufferList.add(byteBuf);
            return AppendResult.SUCCESS;
        } finally {
            bufferLock.unlock();
        }
    }

    public void setCommitPosition(long commitPosition) {
        this.commitPosition = commitPosition;
    }

    public long getAppendPosition() {
        return appendPosition;
    }

    @VisibleForTesting
    public void setAppendPosition(long appendPosition) {
        this.appendPosition = appendPosition;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void appendCoda() {
        if (codaBuffer != null) {
            return;
        }
        codaBuffer = ByteBuffer.allocate(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.BLANK_MAGIC_CODE);
        codaBuffer.putLong(maxTimestamp);
        codaBuffer.flip();
        appendPosition += TieredCommitLog.CODA_SIZE;
    }

    public ByteBuffer read(long position, int length) {
        return readAsync(position, length).join();
    }

    public CompletableFuture<ByteBuffer> readAsync(long position, int length) {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        if (position < 0 || length < 0) {
            future.completeExceptionally(
                new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "position or length is negative"));
            return future;
        }
        if (length == 0) {
            future.completeExceptionally(
                new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "length is zero"));
            return future;
        }
        if (position >= commitPosition) {
            future.completeExceptionally(
                new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "position is illegal"));
            return future;
        }
        if (position + length > commitPosition) {
            logger.warn("TieredFileSegment#readAsync request position + length is greater than commit position," +
                    " correct length using commit position, file: {}, request position: {}, commit position:{}, change length from {} to {}",
                getPath(), position, commitPosition, length, commitPosition - position);
            length = (int) (commitPosition - position);
            if (length == 0) {
                future.completeExceptionally(
                    new TieredStoreException(TieredStoreErrorCode.NO_NEW_DATA, "request position is equal to commit position"));
                return future;
            }
            if (fileType == FileSegmentType.CONSUME_QUEUE && length % TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE != 0) {
                future.completeExceptionally(
                    new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "position and length is illegal"));
                return future;
            }
        }
        return read0(position, length);
    }

    public boolean needCommit() {
        return appendPosition > commitPosition;
    }

    public boolean commit() {
        if (closed) {
            return false;
        }
        Boolean result = commitAsync().join();
        if (!result) {
            result = flightCommitRequest.join();
        }
        return result;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public CompletableFuture<Boolean> commitAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(false);
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (!needCommit()) {
            return CompletableFuture.completedFuture(true);
        }
        try {
            int permits = commitLock.drainPermits();
            if (permits <= 0) {
                return CompletableFuture.completedFuture(false);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
        List<ByteBuffer> bufferList = rollingUploadBuffer();
        int bufferSize = 0;
        for (ByteBuffer buffer : bufferList) {
            bufferSize += buffer.remaining();
        }
        if (codaBuffer != null) {
            bufferSize += codaBuffer.remaining();
        }
        if (bufferSize == 0) {
            return CompletableFuture.completedFuture(true);
        }
        TieredFileSegmentInputStream inputStream = TieredFileSegmentInputStreamFactory.build(
            fileType, baseOffset + commitPosition, bufferList, codaBuffer, bufferSize);
        int finalBufferSize = bufferSize;
        try {
            flightCommitRequest = commit0(inputStream, commitPosition, bufferSize, fileType != FileSegmentType.INDEX)
                .thenApply(result -> {
                    if (result) {
                        if (fileType == FileSegmentType.COMMIT_LOG && bufferList.size() > 0) {
                            dispatchCommitOffset = MessageBufferUtil.getQueueOffset(bufferList.get(bufferList.size() - 1));
                        }
                        commitPosition += finalBufferSize;
                        return true;
                    }
                    sendBackBuffer(inputStream);
                    return false;
                })
                .exceptionally(e -> handleCommitException(inputStream, e))
                .whenComplete((result, e) -> {
                    if (commitLock.availablePermits() == 0) {
                        logger.debug("TieredFileSegment#commitAsync: commit cost: {}ms, file: {}, item count: {}, buffer size: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), getPath(), bufferList.size(), finalBufferSize);
                        commitLock.release();
                    } else {
                        logger.error("[Bug]TieredFileSegment#commitAsync: commit lock is already released: available permits: {}", commitLock.availablePermits());
                    }
                });
            return flightCommitRequest;
        } catch (Exception e) {
            handleCommitException(inputStream, e);
            if (commitLock.availablePermits() == 0) {
                logger.debug("TieredFileSegment#commitAsync: commit cost: {}ms, file: {}, item count: {}, buffer size: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), getPath(), bufferList.size(), finalBufferSize);
                commitLock.release();
            } else {
                logger.error("[Bug]TieredFileSegment#commitAsync: commit lock is already released: available permits: {}", commitLock.availablePermits());
            }
        }
        return CompletableFuture.completedFuture(false);
    }

    private boolean handleCommitException(TieredFileSegmentInputStream inputStream, Throwable e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        sendBackBuffer(inputStream);
        long realSize = 0;
        if (cause instanceof TieredStoreException && ((TieredStoreException) cause).getPosition() > 0) {
            realSize = ((TieredStoreException) cause).getPosition();
        }
        if (realSize <= 0) {
            realSize = getSize();
        }
        if (realSize > 0 && realSize > commitPosition) {
            logger.error("TieredFileSegment#handleCommitException: commit failed: file: {}, try to fix position: origin: {}, real: {}", getPath(), commitPosition, realSize, cause);
            // TODO check if this diff part is uploaded to backend storage
            long diff = appendPosition - commitPosition;
            commitPosition = realSize;
            appendPosition = realSize + diff;
            // TODO check if appendPosition is large than maxOffset
        } else if (realSize < commitPosition) {
            logger.error("[Bug]TieredFileSegment#handleCommitException: commit failed: file: {}, can not fix position: origin: {}, real: {}", getPath(), commitPosition, realSize, cause);
        }
        return false;
    }
}
