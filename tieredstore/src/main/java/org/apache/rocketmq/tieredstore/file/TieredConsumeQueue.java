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
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;

public class TieredConsumeQueue {

    /**
     * commit log offset: long, 8 bytes
     * message size: int, 4 bytes
     * tag hash code: long, 8 bytes
     */
    public static final int CONSUME_QUEUE_STORE_UNIT_SIZE = 8 + 4 + 8;

    private final TieredFlatFile flatFile;

    public TieredConsumeQueue(TieredFileAllocator fileQueueFactory, String filePath) {
        this.flatFile = fileQueueFactory.createFlatFileForConsumeQueue(filePath);
    }

    public boolean isInitialized() {
        return flatFile.getBaseOffset() != -1;
    }

    @VisibleForTesting
    public TieredFlatFile getFlatFile() {
        return flatFile;
    }

    public long getBaseOffset() {
        return flatFile.getBaseOffset();
    }

    public void setBaseOffset(long baseOffset) {
        flatFile.setBaseOffset(baseOffset);
    }

    public long getMinOffset() {
        return flatFile.getMinOffset();
    }

    public long getCommitOffset() {
        return flatFile.getCommitOffset();
    }

    public long getMaxOffset() {
        return flatFile.getMaxOffset();
    }

    public long getEndTimestamp() {
        return flatFile.getFileToWrite().getMaxTimestamp();
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
        return flatFile.append(cqItem, timeStamp, commit);
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        return flatFile.readAsync(offset, length);
    }

    public void commit(boolean sync) {
        flatFile.commit(sync);
    }

    public void cleanExpiredFile(long expireTimestamp) {
        flatFile.cleanExpiredFile(expireTimestamp);
    }

    public void destroyExpiredFile() {
        flatFile.destroyExpiredFile();
    }

    protected Pair<Long, Long> getQueueOffsetInFileByTime(long timestamp, BoundaryType boundaryType) {
        TieredFileSegment fileSegment = flatFile.getFileByTime(timestamp, boundaryType);
        if (fileSegment == null) {
            return Pair.of(-1L, -1L);
        }
        return Pair.of(fileSegment.getBaseOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE,
            fileSegment.getCommitOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE - 1);
    }

    public void destroy() {
        flatFile.destroy();
    }
}
