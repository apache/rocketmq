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
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStreamFactory;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public abstract class TieredFileSegment implements Comparable<TieredFileSegment>, TieredStoreProvider {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String filePath;
    protected final long baseOffset;
    protected final FileSegmentType fileType;
    protected final TieredMessageStoreConfig storeConfig;

    private final long maxSize;
    private final ReentrantLock bufferLock = new ReentrantLock();
    private final Semaphore commitLock = new Semaphore(1);

    private volatile boolean full = false;
    private volatile boolean closed = false;

    private volatile long minTimestamp = Long.MAX_VALUE;
    private volatile long maxTimestamp = Long.MAX_VALUE;
    private volatile long commitPosition = 0L;
    private volatile long appendPosition = 0L;

    // only used in commitLog
    private volatile long dispatchCommitOffset = 0L;

    private ByteBuffer codaBuffer;
    private List<ByteBuffer> bufferList = new ArrayList<>();
    private FileSegmentInputStream fileSegmentInputStream;
    private CompletableFuture<Boolean> flightCommitRequest = CompletableFuture.completedFuture(false);

    public TieredFileSegment(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        this.storeConfig = storeConfig;
        this.fileType = fileType;
        this.filePath = filePath;
        this.baseOffset = baseOffset;
        this.maxSize = getMaxSizeByFileType();
    }

    /**
     * The max segment size of a file is determined by the file type
     */
    protected long getMaxSizeByFileType() {
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

    private List<ByteBuffer> borrowBuffer() {
        bufferLock.lock();
        try {
            List<ByteBuffer> tmp = bufferList;
            bufferList = new ArrayList<>();
            return tmp;
        } finally {
            bufferLock.unlock();
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public AppendResult append(ByteBuffer byteBuf, long timestamp) {
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
                // IndexFile is large and not change after compaction, no need deep copy
                bufferList.add(byteBuf);
                setFull();
                return AppendResult.SUCCESS;
            }

            if (appendPosition + byteBuf.remaining() > maxSize) {
                setFull();
                return AppendResult.FILE_FULL;
            }

            if (bufferList.size() > storeConfig.getTieredStoreGroupCommitCount()
                || appendPosition - commitPosition > storeConfig.getTieredStoreGroupCommitSize()) {
                commitAsync();
            }

            if (bufferList.size() > storeConfig.getTieredStoreMaxGroupCommitCount()) {
                logger.debug("File segment append buffer full, file: {}, buffer size: {}, pending bytes: {}",
                    getPath(), bufferList.size(), appendPosition - commitPosition);
                return AppendResult.BUFFER_FULL;
            }

            if (timestamp != Long.MAX_VALUE) {
                maxTimestamp = timestamp;
                if (minTimestamp == Long.MAX_VALUE) {
                    minTimestamp = timestamp;
                }
            }

            appendPosition += byteBuf.remaining();

            // deep copy buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(byteBuf.remaining());
            byteBuffer.put(byteBuf);
            byteBuffer.flip();
            byteBuf.rewind();

            bufferList.add(byteBuffer);
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
        // result is false when we send real commit request
        // use join for wait flight request done
        Boolean result = commitAsync().join();
        if (!result) {
            result = flightCommitRequest.join();
        }
        return result;
    }

    private void releaseCommitLock() {
        if (commitLock.availablePermits() == 0) {
            commitLock.release();
        } else {
            logger.error("[Bug] FileSegmentCommitAsync, lock is already released: available permits: {}",
                commitLock.availablePermits());
        }
    }

    private void updateDispatchCommitOffset(List<ByteBuffer> bufferList) {
        if (fileType == FileSegmentType.COMMIT_LOG && bufferList.size() > 0) {
            dispatchCommitOffset =
                MessageBufferUtil.getQueueOffset(bufferList.get(bufferList.size() - 1));
        }
    }

    /**
     * @return false: commit, true: no commit operation
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public CompletableFuture<Boolean> commitAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(false);
        }

        if (!needCommit()) {
            return CompletableFuture.completedFuture(true);
        }

        if (commitLock.drainPermits() <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        try {
            if (fileSegmentInputStream != null) {
                long fileSize = this.getSize();
                if (fileSize == -1L) {
                    logger.error("Get commit position error before commit, Commit: %d, Expect: %d, Current Max: %d, FileName: %s",
                        commitPosition, commitPosition + fileSegmentInputStream.getContentLength(), appendPosition, getPath());
                    releaseCommitLock();
                    return CompletableFuture.completedFuture(false);
                } else {
                    if (correctPosition(fileSize, null)) {
                        updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
                        fileSegmentInputStream = null;
                    }
                }
            }

            int bufferSize;
            if (fileSegmentInputStream != null) {
                bufferSize = fileSegmentInputStream.available();
            } else {
                List<ByteBuffer> bufferList = borrowBuffer();
                bufferSize = bufferList.stream().mapToInt(ByteBuffer::remaining).sum()
                    + (codaBuffer != null ? codaBuffer.remaining() : 0);
                if (bufferSize == 0) {
                    releaseCommitLock();
                    return CompletableFuture.completedFuture(true);
                }
                fileSegmentInputStream = FileSegmentInputStreamFactory.build(
                    fileType, baseOffset + commitPosition, bufferList, codaBuffer, bufferSize);
            }

            return flightCommitRequest = this
                .commit0(fileSegmentInputStream, commitPosition, bufferSize, fileType != FileSegmentType.INDEX)
                .thenApply(result -> {
                    if (result) {
                        updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
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

        } catch (Exception e) {
            handleCommitException(e);
            releaseCommitLock();
        }
        return CompletableFuture.completedFuture(false);
    }

    private long getCorrectFileSize(Throwable throwable) {
        if (throwable instanceof TieredStoreException) {
            long fileSize = ((TieredStoreException) throwable).getPosition();
            if (fileSize > 0) {
                return fileSize;
            }
        }
        return getSize();
    }

    private boolean handleCommitException(Throwable e) {
        // Get root cause here
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        long fileSize = this.getCorrectFileSize(cause);

        if (fileSize == -1L) {
            logger.error("Get commit position error, Commit: %d, Expect: %d, Current Max: %d, FileName: %s",
                commitPosition, commitPosition + fileSegmentInputStream.getContentLength(), appendPosition, getPath());
            fileSegmentInputStream.rewind();
            return false;
        }

        if (correctPosition(fileSize, cause)) {
            updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
            fileSegmentInputStream = null;
            return true;
        } else {
            fileSegmentInputStream.rewind();
            return false;
        }
    }

    /**
     * return true to clear buffer
     */
    private boolean correctPosition(long fileSize, Throwable throwable) {

        // Current we have three offsets here: commit offset, expect offset, file size.
        // We guarantee that the commit offset is less than or equal to the expect offset.
        // Max offset will increase because we can continuously put in new buffers
        String handleInfo = throwable == null ? "before commit" : "after commit";
        long expectPosition = commitPosition + fileSegmentInputStream.getContentLength();

        String offsetInfo = String.format("Correct Commit Position, %s, result=[{}], " +
                "Commit: %d, Expect: %d, Current Max: %d, FileSize: %d, FileName: %s",
            handleInfo, commitPosition, expectPosition, appendPosition, fileSize, this.getPath());

        // We are believing that the file size returned by the server is correct,
        // can reset the commit offset to the file size reported by the storage system.
        if (fileSize == expectPosition) {
            logger.info(offsetInfo, "Success", throwable);
            commitPosition = fileSize;
            return true;
        }

        if (fileSize < commitPosition) {
            logger.error(offsetInfo, "FileSizeIncorrect", throwable);
        } else if (fileSize == commitPosition) {
            logger.warn(offsetInfo, "CommitFailed", throwable);
        } else if (fileSize > commitPosition) {
            logger.warn(offsetInfo, "PartialSuccess", throwable);
        }
        commitPosition = fileSize;
        return false;
    }
}
