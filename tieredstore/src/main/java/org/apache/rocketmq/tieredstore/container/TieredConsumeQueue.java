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
package org.apache.rocketmq.tieredstore.container;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;

public class TieredConsumeQueue {
    public static final int CONSUME_QUEUE_STORE_UNIT_SIZE = 8 /* commit log offset: long, 8 bytes */
        + 4 /* message size: int, 4 bytes */
        + 8 /* tag hash code: long, 8 bytes */;
    private final MessageQueue messageQueue;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFileQueue fileQueue;


    public TieredConsumeQueue(MessageQueue messageQueue, TieredMessageStoreConfig storeConfig) throws ClassNotFoundException, NoSuchMethodException {
        this.messageQueue = messageQueue;
        this.storeConfig = storeConfig;
        this.fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.CONSUME_QUEUE, messageQueue, storeConfig);
    }

    public boolean isInitialized() {
        return fileQueue.getBaseOffset() != -1;
    }

    public long getBaseOffset() {
        return fileQueue.getBaseOffset();
    }

    public void setBaseOffset(long baseOffset) {
        fileQueue.setBaseOffset(baseOffset);
    }

    public long getMinOffset() {
        return fileQueue.getMinOffset();
    }

    public long getCommitOffset() {
        return fileQueue.getCommitOffset();
    }

    public long getMaxOffset() {
        return fileQueue.getMaxOffset();
    }

    public long getEndTimestamp() {
        return fileQueue.getFileToWrite().getEndTimestamp();
    }

    public AppendResult append(final long offset, final int size, final long tagsCode, long timeStamp) {
        return append(offset, size, tagsCode, timeStamp, false);
    }

    public AppendResult append(final long offset, final int size, final long tagsCode, long timeStamp, boolean commit) {
        ByteBuffer cqItem = ByteBuffer.allocate(CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqItem.putLong(offset);
        cqItem.putInt(size);
        cqItem.putLong(tagsCode);
        cqItem.flip();
        return fileQueue.append(cqItem, timeStamp, commit);
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        return fileQueue.readAsync(offset, length);
    }

    public void commit(boolean sync) {
        fileQueue.commit(sync);
    }

    public void cleanExpiredFile(long expireTimestamp) {
        fileQueue.cleanExpiredFile(expireTimestamp);
    }

    public void destroyExpiredFile() {
        fileQueue.destroyExpiredFile();
    }

    protected Pair<Long, Long> getQueueOffsetInFileByTime(long timestamp, BoundaryType boundaryType) {
        TieredFileSegment fileSegment = fileQueue.getFileByTime(timestamp, boundaryType);
        if (fileSegment == null) {
            return Pair.of(-1L, -1L);
        }
        return Pair.of(fileSegment.getBaseOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE,
            fileSegment.getCommitOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE - 1);
    }

    public void destroy() {
        fileQueue.destroy();
    }
}
