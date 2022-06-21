
package org.apache.rocketmq.store.schedule;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;

/**
 * delay linkLog sequential write, random read
 */
public class DelayLinkLog {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected final MappedFileQueue mappedFileQueue;
    private final DefaultMessageStore defaultMessageStore;
    private final ByteBuffer byteBufferIndex;
    // phyOffset + msgSize + linkRef
    public static final int LINK_LOG_STORE_UNIT_SIZE = 20;
    private final GroupCommitService groupCommitService;
    private final FlushDiskWatcher flushDiskWatcher;
    private final byte[] headBytes;
    private final long invalidRef = 0L;

    public DelayLinkLog(final DefaultMessageStore defaultMessageStore) {
        mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathDelayLinkLog(),
            defaultMessageStore.getMessageStoreConfig().getMappedFileSizeDelayLinkLog(), defaultMessageStore.getAllocateMappedFileService()
            );
        this.defaultMessageStore = defaultMessageStore;
        byteBufferIndex = ByteBuffer.allocate(LINK_LOG_STORE_UNIT_SIZE);
        groupCommitService = new GroupCommitService();
        flushDiskWatcher = new FlushDiskWatcher();
        ByteBuffer headByteBuffer = ByteBuffer.allocate(LINK_LOG_STORE_UNIT_SIZE);
        headByteBuffer.flip();
        headByteBuffer.limit(LINK_LOG_STORE_UNIT_SIZE);
        headByteBuffer.putLong(-1);
        headByteBuffer.putInt(-1);
        headByteBuffer.putLong(-1);
        headBytes = headByteBuffer.array();
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load linkLog  " + (result ? "OK" : "Failed"));
        return result;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public void start() {
        groupCommitService.start();
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();
    }

    public DelayTimeResult getLinkLogByLinkRef(long linkRef) {
        if (linkRef == invalidRef) {
            return null;
        }

        DelayTimeResult result = null;
        List<DelayTimeResult.DelayTimeItem> itemList = new ArrayList<>();
        while (linkRef != invalidRef) {
            final MappedFile mappedFileByOffset = mappedFileQueue.findMappedFileByOffset(linkRef);
            if (mappedFileByOffset == null) {
                break;
            }
            int pos = (int)(linkRef % defaultMessageStore.getMessageStoreConfig().getMappedFileSizeDelayLinkLog());
            final SelectMappedBufferResult selectMappedBufferResult =
                mappedFileByOffset.selectMappedBuffer(pos, LINK_LOG_STORE_UNIT_SIZE);
            if (selectMappedBufferResult == null) {
                break;
            }
            try {
                final ByteBuffer byteBuffer = selectMappedBufferResult.getByteBuffer();
                final long phyOffset = byteBuffer.getLong(0);
                final int msgSize = byteBuffer.getInt(8);
                final long nextLinkRef = byteBuffer.getLong(12);
                linkRef = nextLinkRef;
                DelayTimeResult.DelayTimeItem item = new DelayTimeResult.DelayTimeItem();
                item.setPhyOffset(phyOffset);
                item.setMsgSize(msgSize);
                itemList.add(item);
                if (result == null) {
                    result = new DelayTimeResult();
                    result.setDelayTimeItemList(itemList);
                }
            } finally {
                selectMappedBufferResult.release();
            }
        }
        return result;
    }

    public long putLinkLog(long phyOffset, int msgSize, long linkRef) {
        byteBufferIndex.flip();
        this.byteBufferIndex.limit(LINK_LOG_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(phyOffset);
        this.byteBufferIndex.putInt(msgSize);
        this.byteBufferIndex.putLong(linkRef);
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
        }
        if (mappedFile.isFirstCreateInQueue() && mappedFile.getWrotePosition() == 0) {
            mappedFile.appendMessage(headBytes);
        }
        boolean appendMessage = mappedFile.appendMessage(this.byteBufferIndex.array());
        if (appendMessage) {
            GroupCommitRequest groupCommitRequest =
                new GroupCommitRequest(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition(),
                this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
            groupCommitService.putRequest(groupCommitRequest);
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition() - LINK_LOG_STORE_UNIT_SIZE;
        }
        // 需要验证是否会出现这种情况(currentPos + data.length) > this.fileSize
        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        appendMessage = mappedFile.appendMessage(this.byteBufferIndex.array());
        if (appendMessage) {
            GroupCommitRequest groupCommitRequest =
                new GroupCommitRequest(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition(),
                    this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
            groupCommitService.putRequest(groupCommitRequest);
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition() - LINK_LOG_STORE_UNIT_SIZE;
        }
        log.error("mappedFile.appendMessage fail");
        return -1;
    }

    public void shutdown(){
        groupCommitService.shutdown();
        flushDiskWatcher.shutdown(true);
        flush();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }


    public static class GroupCommitRequest {
        private final long nextOffset;
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public long getDeadLine() {
            return deadLine;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOKFuture.complete(putMessageStatus);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;

        private volatile LinkedList<DelayLinkLog.GroupCommitRequest>
            requestsWrite = new LinkedList<DelayLinkLog.GroupCommitRequest>();
        private volatile LinkedList<DelayLinkLog.GroupCommitRequest> requestsRead = new LinkedList<DelayLinkLog.GroupCommitRequest>();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        public synchronized void putRequest(final DelayLinkLog.GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<DelayLinkLog.GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (DelayLinkLog.GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    boolean flushOK = DelayLinkLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        DelayLinkLog.this.mappedFileQueue.flush(0);
                        flushOK = DelayLinkLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    }

                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = DelayLinkLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    DelayLinkLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                DelayLinkLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            DelayLinkLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    DelayLinkLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                DelayLinkLog.log.warn(this.getServiceName() + " Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            DelayLinkLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return DelayLinkLog.GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }
}