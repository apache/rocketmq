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

import com.google.common.base.Stopwatch;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.container.TieredCommitLog;
import org.apache.rocketmq.tieredstore.container.TieredConsumeQueue;
import org.apache.rocketmq.tieredstore.container.TieredIndexFile;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public abstract class TieredFileSegment implements Comparable<TieredFileSegment>, TieredStoreProvider {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private volatile boolean closed = false;
    private final ReentrantLock bufferLock = new ReentrantLock();
    private final Semaphore commitLock = new Semaphore(1);
    private List<ByteBuffer> uploadBufferList = new ArrayList<>();
    private boolean full;
    protected final FileSegmentType fileType;
    protected final MessageQueue messageQueue;
    protected final TieredMessageStoreConfig storeConfig;
    protected final long baseOffset;
    private volatile long commitPosition;
    private volatile long appendPosition;
    private final long maxSize;
    private long beginTimestamp = Long.MAX_VALUE;
    private long endTimestamp = Long.MAX_VALUE;
    // only used in commitLog
    private long commitMsgQueueOffset = 0;
    private ByteBuffer codaBuffer;

    private CompletableFuture<Boolean> inflightCommitRequest = CompletableFuture.completedFuture(false);

    public TieredFileSegment(FileSegmentType fileType, MessageQueue messageQueue, long baseOffset,
        TieredMessageStoreConfig storeConfig) {
        this.fileType = fileType;
        this.messageQueue = messageQueue;
        this.storeConfig = storeConfig;
        this.baseOffset = baseOffset;
        this.commitPosition = 0;
        this.appendPosition = 0;
        switch (fileType) {
            case COMMIT_LOG:
                this.maxSize = storeConfig.getTieredStoreCommitLogMaxSize();
                break;
            case CONSUME_QUEUE:
                this.maxSize = storeConfig.getTieredStoreConsumeQueueMaxSize();
                break;
            case INDEX:
                this.maxSize = Long.MAX_VALUE;
                break;
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

    public long getCommitMsgQueueOffset() {
        return commitMsgQueueOffset;
    }

    public long getMaxOffset() {
        return baseOffset + appendPosition;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp = beginTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
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

    public MessageQueue getMessageQueue() {
        return messageQueue;
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
                beginTimestamp = byteBuf.getLong(TieredIndexFile.INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION);
                endTimestamp = byteBuf.getLong(TieredIndexFile.INDEX_FILE_HEADER_END_TIME_STAMP_POSITION);
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
                endTimestamp = timeStamp;
                if (beginTimestamp == Long.MAX_VALUE) {
                    beginTimestamp = timeStamp;
                }
            }
            appendPosition += byteBuf.remaining();
            uploadBufferList.add(byteBuf);
            return AppendResult.SUCCESS;
        } finally {
            bufferLock.unlock();
        }
    }

    private void appendCoda() {
        if (codaBuffer != null) {
            return;
        }
        codaBuffer = ByteBuffer.allocate(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.BLANK_MAGIC_CODE);
        codaBuffer.putLong(endTimestamp);
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
            result = inflightCommitRequest.join();
        }
        return result;
    }

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
        TieredFileSegmentInputStream inputStream = new TieredFileSegmentInputStream(fileType, baseOffset + commitPosition, bufferList, codaBuffer, bufferSize);
        int finalBufferSize = bufferSize;
        try {
            inflightCommitRequest = commit0(inputStream, commitPosition, bufferSize, fileType != FileSegmentType.INDEX)
                .thenApply(result -> {
                    if (result) {
                        if (fileType == FileSegmentType.COMMIT_LOG && bufferList.size() > 0) {
                            commitMsgQueueOffset = MessageBufferUtil.getQueueOffset(bufferList.get(bufferList.size() - 1));
                        }
                        commitPosition += finalBufferSize;
                        return true;
                    }
                    sendBackBuffer(inputStream);
                    return false;
                }).exceptionally(e -> handleCommitException(inputStream, e))
                .whenComplete((result, e) -> {
                    if (commitLock.availablePermits() == 0) {
                        logger.debug("TieredFileSegment#commitAsync: commit cost: {}ms, file: {}, item count: {}, buffer size: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS), getPath(), bufferList.size(), finalBufferSize);
                        commitLock.release();
                    } else {
                        logger.error("[Bug]TieredFileSegment#commitAsync: commit lock is already released: available permits: {}", commitLock.availablePermits());
                    }
                });
            return inflightCommitRequest;
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

    public enum FileSegmentType {
        COMMIT_LOG(0),
        CONSUME_QUEUE(1),
        INDEX(2);

        private int type;

        FileSegmentType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public static FileSegmentType valueOf(int type) {
            switch (type) {
                case 0:
                    return COMMIT_LOG;
                case 1:
                    return CONSUME_QUEUE;
                case 2:
                    return INDEX;
                default:
                    throw new IllegalStateException("Unexpected value: " + type);
            }
        }
    }

    public static class TieredFileSegmentInputStream extends InputStream {

        private final FileSegmentType fileType;
        private final List<ByteBuffer> uploadBufferList;
        private int bufferReadIndex = 0;
        private int readOffset = 0;
        // only used in commitLog
        private long commitLogOffset;
        private final ByteBuffer commitLogOffsetBuffer = ByteBuffer.allocate(8);
        private final ByteBuffer codaBuffer;
        private ByteBuffer curBuffer;
        private final int contentLength;
        private int readBytes = 0;

        public TieredFileSegmentInputStream(FileSegmentType fileType, long startOffset,
            List<ByteBuffer> uploadBufferList, ByteBuffer codaBuffer, int contentLength) {
            this.fileType = fileType;
            this.commitLogOffset = startOffset;
            this.commitLogOffsetBuffer.putLong(0, startOffset);
            this.uploadBufferList = uploadBufferList;
            this.codaBuffer = codaBuffer;
            this.contentLength = contentLength;
            if (uploadBufferList.size() > 0) {
                this.curBuffer = uploadBufferList.get(0);
            }
            if (fileType == FileSegmentType.INDEX && uploadBufferList.size() != 1) {
                logger.error("[Bug]TieredFileSegmentInputStream: index file must have only one buffer");
            }
        }

        public List<ByteBuffer> getUploadBufferList() {
            return uploadBufferList;
        }

        public ByteBuffer getCodaBuffer() {
            return codaBuffer;
        }

        @Override
        public int available() {
            return contentLength - readBytes;
        }

        @Override
        public int read() {
            if (bufferReadIndex >= uploadBufferList.size()) {
                return readCoda();
            }

            int res;
            switch (fileType) {
                case COMMIT_LOG:
                    if (readOffset >= curBuffer.remaining()) {
                        bufferReadIndex++;
                        if (bufferReadIndex >= uploadBufferList.size()) {
                            return readCoda();
                        }
                        curBuffer = uploadBufferList.get(bufferReadIndex);
                        commitLogOffset += readOffset;
                        commitLogOffsetBuffer.putLong(0, commitLogOffset);
                        readOffset = 0;
                    }
                    if (readOffset >= MessageBufferUtil.PHYSICAL_OFFSET_POSITION && readOffset < MessageBufferUtil.SYS_FLAG_OFFSET_POSITION) {
                        res = commitLogOffsetBuffer.get(readOffset - MessageBufferUtil.PHYSICAL_OFFSET_POSITION) & 0xff;
                        readOffset++;
                    } else {
                        res = curBuffer.get(readOffset++) & 0xff;
                    }
                    break;
                case CONSUME_QUEUE:
                    if (!curBuffer.hasRemaining()) {
                        bufferReadIndex++;
                        if (bufferReadIndex >= uploadBufferList.size()) {
                            return -1;
                        }
                        curBuffer = uploadBufferList.get(bufferReadIndex);
                    }
                    res = curBuffer.get() & 0xff;
                    break;
                case INDEX:
                    if (!curBuffer.hasRemaining()) {
                        return -1;
                    }
                    res = curBuffer.get() & 0xff;
                    break;
                default:
                    throw new IllegalStateException("unknown file type");
            }
            readBytes++;
            return res;
        }

        private int readCoda() {
            if (fileType != FileSegmentType.COMMIT_LOG || codaBuffer == null) {
                return -1;
            }
            if (!codaBuffer.hasRemaining()) {
                return -1;
            }
            readBytes++;
            return codaBuffer.get() & 0xff;
        }
    }
}
