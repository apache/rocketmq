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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;

public class StoreCheckpoint {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;

    /*
     * Checkpoint file index. Only the six fields listed below are persisted; each value is an
     * 8-byte long written at a fixed byte offset (ByteBuffer default big-endian order):
     *
     *   offset  0 : physicMsgTimestamp   - commit log flush progress (timestamp)
     *   offset  8 : logicsMsgTimestamp   - consume queue flush progress (timestamp)
     *   offset 16 : indexMsgTimestamp    - index file flush progress (timestamp)
     *   offset 24 : masterFlushedOffset  - master commit log flush progress (physical offset)
     *   offset 32 : confirmPhyOffset     - confirmed commit log offset
     *   offset 40 : logicsPhysicalOffset - commit log offset matching the flushed consume queue
     *
     * The two tmp* fields are runtime-only staging values and are deliberately not persisted.
     */

    /**
     * Candidate value for {@link #logicsMsgTimestamp}: the timestamp of the latest message that has
     * been appended to a consume queue but is not yet known to be flushed.
     * <p>
     * Runtime only, never persisted. It is overwritten on every successful dispatch and promoted to
     * {@link #logicsMsgTimestamp} by {@code FlushConsumeQueueService} after the consume queue flush
     * completes.
     */
    private volatile long tmpLogicsMsgTimestamp = 0;

    /**
     * Timestamp in milliseconds of the latest message that has been flushed to the commit log file.
     * <p>
     * Persisted at byte offset {@code 0}. On the master it is updated by the commit log flush
     * service after the mapped files are flushed; on a slave it is updated by the dispatcher when a
     * dispatch request is appended. It is used together with {@link #logicsMsgTimestamp} to compute
     * {@link #getMinTimestamp()}, the lower bound for timestamp-based recovery.
     */
    private volatile long physicMsgTimestamp = 0;

    /**
     * Timestamp in milliseconds of the latest message whose consume queue entries have been flushed
     * to disk.
     * <p>
     * Persisted at byte offset {@code 8}. It is advanced by {@code FlushConsumeQueueService} only
     * after a thorough consume queue flush, using the latest {@link #tmpLogicsMsgTimestamp} as the
     * candidate value. This keeps the checkpoint from getting ahead of the durable consume queue.
     * It is also used as a fallback recovery bound when {@link #logicsPhysicalOffset} is not
     * available, for example when reading a checkpoint written by an older version.
     */
    private volatile long logicsMsgTimestamp = 0;

    /**
     * Candidate value for {@link #logicsPhysicalOffset}: the commit log offset of the latest message
     * that has been appended to a consume queue but is not yet known to be flushed.
     * <p>
     * Runtime only, never persisted. Like {@link #tmpLogicsMsgTimestamp}, it is staged on every
     * successful dispatch and promoted to {@link #logicsPhysicalOffset} after the consume queue
     * flush completes.
     */
    private volatile long tmpLogicsPhysicalOffset = 0;

    /**
     * Physical offset in the commit log of the latest message whose consume queue entries have been
     * flushed to disk.
     * <p>
     * Persisted at byte offset {@code 40}. It is advanced by {@code FlushConsumeQueueService} from
     * {@link #tmpLogicsPhysicalOffset} after a thorough consume queue flush. During abnormal
     * recovery it is the preferred starting point for re-dispatching the commit log; a value less
     * than or equal to zero means the checkpoint predates this field, so recovery falls back to
     * {@link #logicsMsgTimestamp}.
     */
    private volatile long logicsPhysicalOffset = 0;

    /**
     * Timestamp in milliseconds of the latest message covered by a flushed index file.
     * <p>
     * Persisted at byte offset {@code 16}. Updated by {@code IndexService#flush} when an index file
     * becomes full. During recovery, it lets the index service skip commit log files whose messages
     * are already indexed.
     */
    private volatile long indexMsgTimestamp = 0;

    /**
     * Physical offset in the commit log that the master has flushed to disk.
     * <p>
     * Persisted at byte offset {@code 24}. A slave obtains this value from the master's HA runtime
     * information, or from an admin reset command, persists it, and restores it into
     * {@code DefaultMessageStore#masterFlushedOffset} on startup. It is used for HA state reporting
     * and failover decisions, not for message recovery.
     */
    private volatile long masterFlushedOffset = 0;

    /**
     * Confirmed physical offset in the commit log, mainly used in controller (auto-switch) mode.
     * <p>
     * Persisted at byte offset {@code 32}. Updated by {@code CommitLog#setConfirmOffset} and
     * restored on startup so that the commit log can resume from the confirmed position instead of
     * recomputing it from scratch.
     * <p>
     * set by CommitLog.recover() method
     * get by ReputMessageService
     *
     */
    private volatile long confirmPhyOffset = 0;

    public StoreCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        UtilAll.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, DefaultMappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16);
            this.masterFlushedOffset = this.mappedByteBuffer.getLong(24);
            this.confirmPhyOffset = this.mappedByteBuffer.getLong(32);
            this.logicsPhysicalOffset = this.mappedByteBuffer.getLong(40);

            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
            log.info("store checkpoint file indexMsgTimestamp " + this.indexMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.indexMsgTimestamp));
            log.info("store checkpoint file masterFlushedOffset " + this.masterFlushedOffset);
            log.info("store checkpoint file confirmPhyOffset " + this.confirmPhyOffset);
            log.info("store checkpoint file logicsPhysicalOffset " + this.logicsPhysicalOffset);
        } else {
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }

    public void shutdown() {

        this.flush();

        // unmap mappedByteBuffer
        UtilAll.cleanBuffer(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (Throwable e) {
            log.error("Failed to close file channel", e);
        }
    }

    public void flush() {
        try {
            this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
            this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
            this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
            this.mappedByteBuffer.putLong(24, this.masterFlushedOffset);
            this.mappedByteBuffer.putLong(32, this.confirmPhyOffset);
            this.mappedByteBuffer.putLong(40, this.logicsPhysicalOffset);
            this.mappedByteBuffer.force();
        } catch (Throwable e) {
            log.error("Failed to flush", e);
        }
    }

    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }

    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }

    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }

    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }

    public long getTmpLogicsMsgTimestamp() {
        return tmpLogicsMsgTimestamp;
    }

    public void setTmpLogicsMsgTimestamp(long tmpLogicsMsgTimestamp) {
        this.tmpLogicsMsgTimestamp = tmpLogicsMsgTimestamp;
    }

    public long getTmpLogicsPhysicalOffset() {
        return tmpLogicsPhysicalOffset;
    }

    public void setTmpLogicsPhysicalOffset(long tmpLogicsPhysicalOffset) {
        this.tmpLogicsPhysicalOffset = tmpLogicsPhysicalOffset;
    }

    public long getLogicsPhysicalOffset() {
        return logicsPhysicalOffset;
    }

    public void setLogicsPhysicalOffset(long logicsPhysicalOffset) {
        this.logicsPhysicalOffset = logicsPhysicalOffset;
    }

    public long getConfirmPhyOffset() {
        return confirmPhyOffset;
    }

    public void setConfirmPhyOffset(long confirmPhyOffset) {
        this.confirmPhyOffset = confirmPhyOffset;
    }

    public long getMinTimestampIndex() {
        return Math.min(this.getMinTimestamp(), this.indexMsgTimestamp);
    }

    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        min -= 1000 * 3;
        if (min < 0) {
            min = 0;
        }

        return min;
    }

    public long getIndexMsgTimestamp() {
        return indexMsgTimestamp;
    }

    public void setIndexMsgTimestamp(long indexMsgTimestamp) {
        this.indexMsgTimestamp = indexMsgTimestamp;
    }

    public long getMasterFlushedOffset() {
        return masterFlushedOffset;
    }

    public void setMasterFlushedOffset(long masterFlushedOffset) {
        this.masterFlushedOffset = masterFlushedOffset;
    }
}
