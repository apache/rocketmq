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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;

public class FlatCommitLogFile extends FlatAppendFile {

    private static final long GET_OFFSET_ERROR = -1L;

    private final AtomicLong firstOffset = new AtomicLong(GET_OFFSET_ERROR);

    public FlatCommitLogFile(FileSegmentFactory fileSegmentFactory, String filePath) {
        super(fileSegmentFactory, FileSegmentType.COMMIT_LOG, filePath);
        this.initOffset(0L);
    }

    /**
     * Two rules are set here:
     * 1. Single file must be saved for more than one day as default.
     * 2. Single file must reach the minimum size before switching.
     * When calculating storage space, due to the limitation of condition 2,
     * the actual usage of storage space may be slightly higher than expected.
     */
    public boolean tryRollingFile(long interval) {
        FileSegment fileSegment = this.getFileToWrite();
        long timestamp = fileSegment.getMinTimestamp();
        if (timestamp != Long.MAX_VALUE && timestamp + interval < System.currentTimeMillis() &&
            fileSegment.getAppendPosition() >=
                fileSegmentFactory.getStoreConfig().getCommitLogRollingMinimumSize()) {
            this.rollingNewFile(this.getAppendOffset());
            return true;
        }
        return false;
    }

    public long getMinOffsetFromFile() {
        return firstOffset.get() == GET_OFFSET_ERROR ?
            this.getMinOffsetFromFileAsync().join() : firstOffset.get();
    }

    public CompletableFuture<Long> getMinOffsetFromFileAsync() {
        int length = MessageFormatUtil.QUEUE_OFFSET_POSITION + Long.BYTES;
        if (this.fileSegmentTable.isEmpty() ||
            this.getCommitOffset() - this.getMinOffset() < length) {
            return CompletableFuture.completedFuture(GET_OFFSET_ERROR);
        }
        return this.readAsync(this.getMinOffset(), length)
            .thenApply(buffer -> {
                firstOffset.set(MessageFormatUtil.getQueueOffset(buffer));
                return firstOffset.get();
            });
    }

    @Override
    public void destroyExpiredFile(long expireTimestamp) {
        long beforeOffset = this.getMinOffset();
        super.destroyExpiredFile(expireTimestamp);
        long afterOffset = this.getMinOffset();

        if (beforeOffset != afterOffset) {
            log.info("CommitLog min cq offset reset, filePath={}, offset={}, expireTimestamp={}, change={}-{}",
                filePath, firstOffset.get(), expireTimestamp, beforeOffset, afterOffset);
            firstOffset.set(GET_OFFSET_ERROR);
        }
    }
}
