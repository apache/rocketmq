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

import com.alibaba.fastjson.JSON;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatMessageFile implements FlatFileInterface {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    protected volatile boolean closed = false;

    protected TopicMetadata topicMetadata;
    protected QueueMetadata queueMetadata;

    protected final String filePath;
    protected final ReentrantLock fileLock;
    protected final MessageStoreConfig storeConfig;
    protected final MetadataStore metadataStore;
    protected final FlatCommitLogFile commitLog;
    protected final FlatConsumeQueueFile consumeQueue;
    protected final AtomicLong lastDestroyTime;

    protected final List<SelectMappedBufferResult> bufferResultList;
    protected final List<DispatchRequest> dispatchRequestList;
    protected final ConcurrentMap<String, CompletableFuture<?>> inFlightRequestMap;

    public FlatMessageFile(FlatFileFactory fileFactory, String topic, int queueId) {
        this(fileFactory, MessageStoreUtil.toFilePath(
            new MessageQueue(topic, fileFactory.getStoreConfig().getBrokerName(), queueId)));
        this.topicMetadata = this.recoverTopicMetadata(topic);
        this.queueMetadata = this.recoverQueueMetadata(topic, queueId);
    }

    public FlatMessageFile(FlatFileFactory fileFactory, String filePath) {
        this.filePath = filePath;
        this.fileLock = new ReentrantLock(false);
        this.storeConfig = fileFactory.getStoreConfig();
        this.metadataStore = fileFactory.getMetadataStore();
        this.commitLog = fileFactory.createFlatFileForCommitLog(filePath);
        this.consumeQueue = fileFactory.createFlatFileForConsumeQueue(filePath);
        this.lastDestroyTime = new AtomicLong();
        this.bufferResultList = new ArrayList<>();
        this.dispatchRequestList = new ArrayList<>();
        this.inFlightRequestMap = new ConcurrentHashMap<>();
    }

    @Override
    public long getTopicId() {
        return topicMetadata.getTopicId();
    }

    @Override
    public MessageQueue getMessageQueue() {
        return queueMetadata != null ? queueMetadata.getQueue() : null;
    }

    @Override
    public boolean isFlatFileInit() {
        return !this.consumeQueue.fileSegmentTable.isEmpty();
    }

    public TopicMetadata recoverTopicMetadata(String topic) {
        TopicMetadata topicMetadata = this.metadataStore.getTopic(topic);
        if (topicMetadata == null) {
            topicMetadata = this.metadataStore.addTopic(topic, -1L);
        }
        return topicMetadata;
    }

    public QueueMetadata recoverQueueMetadata(String topic, int queueId) {
        MessageQueue mq = new MessageQueue(topic, storeConfig.getBrokerName(), queueId);
        QueueMetadata queueMetadata = this.metadataStore.getQueue(mq);
        if (queueMetadata == null) {
            queueMetadata = this.metadataStore.addQueue(mq, -1L);
        }
        return queueMetadata;
    }

    public void flushMetadata() {
        if (queueMetadata != null) {
            queueMetadata.setMinOffset(this.getConsumeQueueMinOffset());
            queueMetadata.setMaxOffset(this.getConsumeQueueCommitOffset());
            queueMetadata.setUpdateTimestamp(System.currentTimeMillis());
            metadataStore.updateQueue(queueMetadata);
        }
    }

    @Override
    public Lock getFileLock() {
        return this.fileLock;
    }

    @Override
    public boolean rollingFile(long interval) {
        return this.commitLog.tryRollingFile(interval);
    }

    @Override
    public void initOffset(long offset) {
        fileLock.lock();
        try {
            this.commitLog.initOffset(0L);
            this.consumeQueue.initOffset(offset * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void addMessage(SelectMappedBufferResult message) {
        this.bufferResultList.add(message);
    }

    @Override
    public AppendResult appendCommitLog(ByteBuffer message) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        return commitLog.append(message, MessageFormatUtil.getStoreTimeStamp(message));
    }

    @Override
    public AppendResult appendCommitLog(SelectMappedBufferResult message) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        return this.appendCommitLog(message.getByteBuffer());
    }

    @Override
    public AppendResult appendConsumeQueue(DispatchRequest request) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

        ByteBuffer buffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        buffer.putLong(request.getCommitLogOffset());
        buffer.putInt(request.getMsgSize());
        buffer.putLong(request.getTagsCode());
        buffer.flip();

        this.dispatchRequestList.add(request);
        return consumeQueue.append(buffer, request.getStoreTimestamp());
    }

    @Override
    public List<DispatchRequest> getDispatchRequestList() {
        return dispatchRequestList;
    }

    @Override
    public void release() {
        for (SelectMappedBufferResult bufferResult : bufferResultList) {
            bufferResult.release();
        }

        if (queueMetadata != null) {
            log.trace("FlatMessageFile release, topic={}, queueId={}, bufferSize={}, requestListSize={}",
                queueMetadata.getQueue().getTopic(), queueMetadata.getQueue().getQueueId(),
                bufferResultList.size(), dispatchRequestList.size());
        }

        bufferResultList.clear();
        dispatchRequestList.clear();
    }

    @Override
    public long getMinStoreTimestamp() {
        return commitLog.getMinTimestamp();
    }

    @Override
    public long getMaxStoreTimestamp() {
        return commitLog.getMaxTimestamp();
    }

    @Override
    public long getFirstMessageOffset() {
        return commitLog.getMinOffsetFromFile();
    }

    @Override
    public long getCommitLogMinOffset() {
        return commitLog.getMinOffset();
    }

    @Override
    public long getCommitLogMaxOffset() {
        return commitLog.getAppendOffset();
    }

    @Override
    public long getCommitLogCommitOffset() {
        return commitLog.getCommitOffset();
    }

    @Override
    public long getConsumeQueueMinOffset() {
        long cqOffset = consumeQueue.getMinOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
        long effectiveOffset = this.commitLog.getMinOffsetFromFile();
        return Math.max(cqOffset, effectiveOffset);
    }

    @Override
    public long getConsumeQueueMaxOffset() {
        return consumeQueue.getAppendOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
    }

    @Override
    public long getConsumeQueueCommitOffset() {
        return consumeQueue.getCommitOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
    }

    @Override
    public CompletableFuture<Boolean> commitAsync() {
        return this.commitLog.commitAsync()
            .thenCompose(result -> {
                if (result) {
                    return consumeQueue.commitAsync();
                }
                return CompletableFuture.completedFuture(false);
            });
    }

    @Override
    public CompletableFuture<ByteBuffer> getMessageAsync(long queueOffset) {
        return getConsumeQueueAsync(queueOffset).thenCompose(cqBuffer -> {
            long commitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer);
            int length = MessageFormatUtil.getSizeFromItem(cqBuffer);
            return getCommitLogAsync(commitLogOffset, length);
        });
    }

    @Override
    public CompletableFuture<ByteBuffer> getCommitLogAsync(long offset, int length) {
        return commitLog.readAsync(offset, length);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset) {
        return this.getConsumeQueueAsync(queueOffset, 1);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset, int count) {
        return consumeQueue.readAsync(
            queueOffset * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE,
            count * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
    }

    @Override
    public CompletableFuture<Long> getQueueOffsetByTimeAsync(long timestamp, BoundaryType boundaryType) {
        long cqMin = getConsumeQueueMinOffset();
        long cqMax = getConsumeQueueCommitOffset() - 1;
        if (cqMax == -1 || cqMax < cqMin) {
            return CompletableFuture.completedFuture(cqMin);
        }

        ByteBuffer buffer = getMessageAsync(cqMax).join();
        long storeTime = MessageFormatUtil.getStoreTimeStamp(buffer);
        if (storeTime < timestamp) {
            log.info("FlatMessageFile getQueueOffsetByTimeAsync, exceeded maximum time, " +
                "filePath={}, timestamp={}, result={}", filePath, timestamp, cqMax + 1);
            return CompletableFuture.completedFuture(cqMax + 1);
        }

        buffer = getMessageAsync(cqMin).join();
        storeTime = MessageFormatUtil.getStoreTimeStamp(buffer);
        if (storeTime > timestamp) {
            log.info("FlatMessageFile getQueueOffsetByTimeAsync, less than minimum time, " +
                "filePath={}, timestamp={}, result={}", filePath, timestamp, cqMin);
            return CompletableFuture.completedFuture(cqMin);
        }

        // binary search lower bound index in a sorted array
        long minOffset = cqMin;
        long maxOffset = cqMax;
        List<String> queryLog = new ArrayList<>();
        while (minOffset < maxOffset) {
            long middle = minOffset + (maxOffset - minOffset) / 2;
            buffer = this.getMessageAsync(middle).join();
            storeTime = MessageFormatUtil.getStoreTimeStamp(buffer);
            queryLog.add(String.format("(range=%d-%d, middle=%d, timestamp=%d, diff=%dms)",
                minOffset, maxOffset, middle, storeTime, timestamp - storeTime));
            if (storeTime < timestamp) {
                minOffset = middle + 1;
            } else {
                maxOffset = middle;
            }
        }

        long offset = minOffset;
        if (boundaryType == BoundaryType.UPPER) {
            while (true) {
                long next = offset + 1;
                if (next > cqMax) {
                    break;
                }
                buffer = this.getMessageAsync(next).join();
                storeTime = MessageFormatUtil.getStoreTimeStamp(buffer);
                if (storeTime == timestamp) {
                    offset = next;
                } else {
                    break;
                }
            }
        }

        log.info("FlatMessageFile getQueueOffsetByTimeAsync, filePath={}, timestamp={}, result={}, log={}",
            filePath, timestamp, offset, JSON.toJSONString(queryLog));
        return CompletableFuture.completedFuture(offset);
    }

    @Override
    public int hashCode() {
        return filePath.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return StringUtils.equals(filePath, ((FlatMessageFile) obj).filePath);
    }

    @Override
    public void shutdown() {
        closed = true;
        fileLock.lock();
        try {
            commitLog.shutdown();
            consumeQueue.shutdown();
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void destroyExpiredFile(long timestamp) {
        fileLock.lock();
        try {
            commitLog.destroyExpiredFile(timestamp);
            consumeQueue.destroyExpiredFile(timestamp);
        } finally {
            fileLock.unlock();
        }
    }

    public void destroy() {
        this.shutdown();
        fileLock.lock();
        try {
            commitLog.destroyExpiredFile(Long.MAX_VALUE);
            consumeQueue.destroyExpiredFile(Long.MAX_VALUE);
            if (queueMetadata != null) {
                metadataStore.deleteQueue(queueMetadata.getQueue());
            }
        } finally {
            fileLock.unlock();
        }
    }

    public long getFileReservedHours() {
        if (topicMetadata.getReserveTime() > 0) {
            return topicMetadata.getReserveTime();
        }
        return storeConfig.getTieredStoreFileReservedTime();
    }
}
