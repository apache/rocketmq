
package org.apache.rocketmq.store.schedule;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * delay time line file
 */
public class DelayTimeLineFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final int fileSupportSeconds;
    // linkRef + storeTimestamp
    private final int unitSize = 16;
    private final int headSize = 8;
    private final int headIndex = 0;
    private final long createTime;
    private static long invalidRef = 0;

    private final DelayLinkLog delayLinkLog;

    public DelayTimeLineFile(String fileName, int fileSecondsSize, long createTime, DelayLinkLog delayLinkLog) throws IOException {
        this.mappedFile = new MappedFile(fileName, fileSecondsSize * unitSize  + headSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.fileSupportSeconds = fileSecondsSize;
        this.createTime = createTime;
        this.delayLinkLog = delayLinkLog;
    }

    public long getFirstOffset() {
        return createTime;
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public long getLastStoreTime() {
        return this.mappedByteBuffer.getLong(0);
    }

    public void load() {

    }

    public void truncateDirtyDelayFile(long checkPointTimestamp) {
        if (checkPointTimestamp <  getLastStoreTime()) {
            int index = headIndex;
            while(index < fileSupportSeconds * unitSize + headIndex) {
                final long storeTimestamp = mappedByteBuffer.getLong(index + 8);
                if (checkPointTimestamp < storeTimestamp) {
                    mappedByteBuffer.putLong(index, invalidRef);
                    mappedByteBuffer.putLong(index + 8, invalidRef);
                }
                index = index + unitSize;
            }
        }
    }

    public boolean putTimeTask(long phyOffset, long expireTime, long storeTimestamp, int msgSize) {
        if (isIncludeInTime(expireTime)) {
            try {
                int index = (int)((expireTime - createTime) * unitSize) + headSize;
                long oldLinkRef = mappedByteBuffer.getLong(index);
                long newLinkRef = putLinkLog(phyOffset, msgSize, oldLinkRef);
                mappedByteBuffer.putLong(index, newLinkRef);
                mappedByteBuffer.putLong(index + 8, storeTimestamp);
                mappedByteBuffer.putLong(headIndex, storeTimestamp);
                return true;
            } catch (Exception e) {
                log.error("putTimeTask exception, phyOffset: " + phyOffset + " expireTime: " + expireTime, e);
            }
        } else {
            log.warn("Over delay time file capacity: expireTime = " + expireTime + "; file name = " + getFileName());
        }
        return false;
    }

    public DelayTimeResult queryByTimestamp(long timestamp) {
        if (isIncludeInTime(timestamp)) {
            int index = (int)((timestamp - createTime) * unitSize) + headSize;
            long oldLinkRef = mappedByteBuffer.getLong(index);
            if (oldLinkRef == invalidRef) {
                return null;
            }
            return delayLinkLog.getLinkLogByLinkRef(oldLinkRef);
        }
        return null;
    }

    private long putLinkLog(long phyOffset, int msgSize, long linkRef) {
        return delayLinkLog.putLinkLog(phyOffset, msgSize, linkRef);
    }


    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return System.currentTimeMillis() / 1000 > createTime + fileSupportSeconds;
    }

    public boolean isIncludeInTime(Long expireTime) {
        return (expireTime < (createTime + fileSupportSeconds)) && (expireTime >= createTime);
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

}