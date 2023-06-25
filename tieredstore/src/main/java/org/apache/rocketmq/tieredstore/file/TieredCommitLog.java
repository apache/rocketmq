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
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredCommitLog {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    /**
     * item size: int, 4 bytes
     * magic code: int, 4 bytes
     * max store timestamp: long, 8 bytes
     */
    public static final int CODA_SIZE = 4 + 8 + 4;
    public static final int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;

    private final TieredMessageStoreConfig storeConfig;
    private final TieredFlatFile flatFile;

    public TieredCommitLog(TieredFileAllocator fileQueueFactory, String filePath) {
        this.storeConfig = fileQueueFactory.getStoreConfig();
        this.flatFile = fileQueueFactory.createFlatFileForCommitLog(filePath);
    }

    @VisibleForTesting
    public TieredFlatFile getFlatFile() {
        return flatFile;
    }

    public long getMinOffset() {
        return flatFile.getMinOffset();
    }

    public long getCommitOffset() {
        return flatFile.getCommitOffset();
    }

    public long getDispatchCommitOffset() {
        return flatFile.getDispatchCommitOffset();
    }

    public long getMaxOffset() {
        return flatFile.getMaxOffset();
    }

    public long getBeginTimestamp() {
        TieredFileSegment firstIndexFile = flatFile.getFileByIndex(0);
        if (firstIndexFile == null) {
            return -1L;
        }
        long beginTimestamp = firstIndexFile.getMinTimestamp();
        return beginTimestamp != Long.MAX_VALUE ? beginTimestamp : -1;
    }

    public long getEndTimestamp() {
        return flatFile.getFileToWrite().getMaxTimestamp();
    }

    public AppendResult append(ByteBuffer byteBuf) {
        return flatFile.append(byteBuf, MessageBufferUtil.getStoreTimeStamp(byteBuf));
    }

    public AppendResult append(ByteBuffer byteBuf, boolean commit) {
        return flatFile.append(byteBuf, MessageBufferUtil.getStoreTimeStamp(byteBuf), commit);
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
        if (flatFile.getFileSegmentCount() == 0) {
            return;
        }
        TieredFileSegment fileSegment = flatFile.getFileToWrite();
        try {
            if (System.currentTimeMillis() - fileSegment.getMaxTimestamp() >
                TimeUnit.HOURS.toMillis(storeConfig.getCommitLogRollingInterval())
                && fileSegment.getAppendPosition() > storeConfig.getCommitLogRollingMinimumSize()) {
                flatFile.rollingNewFile();
            }
        } catch (Exception e) {
            log.error("Rolling to next file failed", e);
        }
    }

    public void destroy() {
        flatFile.destroy();
    }
}
