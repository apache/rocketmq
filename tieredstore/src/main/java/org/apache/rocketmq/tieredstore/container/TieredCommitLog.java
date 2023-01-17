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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredCommitLog {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    public static final int CODA_SIZE = 4 /* item size: int, 4 bytes */
        + 4 /* magic code: int, 4 bytes */
        + 8 /* max store timestamp: long, 8 bytes */;
    public static final int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;

    private final MessageQueue messageQueue;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFileQueue fileQueue;

    public TieredCommitLog(MessageQueue messageQueue, TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {
        this.messageQueue = messageQueue;
        this.storeConfig = storeConfig;
        this.fileQueue = new TieredFileQueue(TieredFileSegment.FileSegmentType.COMMIT_LOG, messageQueue, storeConfig);
        if (fileQueue.getBaseOffset() == -1) {
            fileQueue.setBaseOffset(0);
        }
    }

    public long getMinOffset() {
        return fileQueue.getMinOffset();
    }

    public long getCommitOffset() {
        return fileQueue.getCommitOffset();
    }

    public long getCommitMsgQueueOffset() {
        return fileQueue.getCommitMsgQueueOffset();
    }

    public long getMaxOffset() {
        return fileQueue.getMaxOffset();
    }

    public long getBeginTimestamp() {
        TieredFileSegment firstIndexFile = fileQueue.getFileByIndex(0);
        if (firstIndexFile == null) {
            return -1;
        }
        long beginTimestamp = firstIndexFile.getBeginTimestamp();
        return beginTimestamp != Long.MAX_VALUE ? beginTimestamp : -1;
    }

    public long getEndTimestamp() {
        return fileQueue.getFileToWrite().getEndTimestamp();
    }

    public AppendResult append(ByteBuffer byteBuf) {
        return fileQueue.append(byteBuf, MessageBufferUtil.getStoreTimeStamp(byteBuf));
    }

    public AppendResult append(ByteBuffer byteBuf, boolean commit) {
        return fileQueue.append(byteBuf, MessageBufferUtil.getStoreTimeStamp(byteBuf), commit);
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

        if (fileQueue.getFileSegmentCount() == 0) {
            return;
        }
        TieredFileSegment fileSegment = fileQueue.getFileToWrite();
        try {
            if (System.currentTimeMillis() - fileSegment.getEndTimestamp() > (long) storeConfig.getCommitLogRollingInterval() * 60 * 60 * 1000
                && fileSegment.getSize() > storeConfig.getCommitLogRollingMinimumSize()) {
                fileQueue.rollingNewFile();
            }
        } catch (Exception e) {
            logger.error("Rolling to next file failed:", e);
        }
    }

    public void destroy() {
        fileQueue.destroy();
    }
}
