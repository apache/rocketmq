package org.apache.rocketmq.store.delay;

import io.netty.util.HashedWheelTimer;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.store.delay.DelayService.resolveFolder;

public class DelayIdxService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final long TIME_SLICE_MS = 30 * 60 * 1000; // 30分钟一个切片
    private static final int DELAY_IDX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
    private static final long LOOK_AHEAD_MS = 5 * 60 * 1000; // 预加载滑动窗口：5分钟
    private static final int MAX_READ_ENTRIES_PER_SCAN = 1000; // 每次扫描最多读取的索引条目数

    // 重试相关参数
    private static final long RETRY_BASE_DELAY_MS = 5 * 1000;
    private static final long RETRY_MAX_DELAY_MS = 5 * 60 * 1000;
    private static final int RETRY_MAX_TIMES = 16;

    private final String indexDir;
    private final DelayLogService delayLogService;
    private final DefaultMessageStore messageStore;
    private final DelayIndexScanner indexScanner;
    private final HashedWheelTimer timer = new HashedWheelTimer(1, TimeUnit.SECONDS, 60);
    private final ConcurrentHashMap<Long, MappedFileQueue> indexFileMap = new ConcurrentHashMap<>();
    private final AllocateMappedFileService allocateMappedFileService;
    private final ThreadPoolExecutor dispatchExecutor;
    private final DelayQueue<RetryMessage> retryQueue = new DelayQueue<>();
    private final CheckpointManager checkpointManager;

    public DelayIdxService(String indexDir, DelayLogService delayLogService,
                           AllocateMappedFileService allocateMappedFileService,
                           DefaultMessageStore messageStore) {
        this.delayLogService = delayLogService;
        this.allocateMappedFileService = allocateMappedFileService;
        this.messageStore = messageStore;
        this.indexDir = indexDir;
        ensureDirOK();
        String checkpointFilePath = indexDir + File.separator + "delay_idx_checkpoint.dat";
        try {
            this.checkpointManager = new CheckpointManager(checkpointFilePath);
        } catch (IOException e) {
            throw new RuntimeException("Init CheckpointManager failed: " + checkpointFilePath, e);
        }
        this.indexScanner = new DelayIndexScanner();
        this.dispatchExecutor = new ThreadPoolExecutor(
                4, 16, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                new ThreadFactory() {
                    private final AtomicInteger indexer = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "DelayMessageDispatcher-" + indexer.getAndIncrement());
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public synchronized void buildIndex(long scheduleTime, long walOffset, int msgSize) {
        try {
            long fileId = (scheduleTime / TIME_SLICE_MS) * TIME_SLICE_MS;

            MappedFileQueue idxQueue = getOrCreateIdxQueue(fileId);
            MappedFile activeIdxFile = idxQueue.getLastMappedFile();
            if (activeIdxFile == null || activeIdxFile.isFull()) {
                activeIdxFile = idxQueue.getLastMappedFile(0);
            }
            if (activeIdxFile == null) {
                log.error("[DelayIdx] no active idx file, fileId={}", fileId);
                return;
            }

            long reserved = 0L;
            ByteBuffer buffer = ByteBuffer.allocate(DelayIndex.ENTRY_SIZE);
            buffer.putLong(scheduleTime);
            buffer.putLong(walOffset);
            buffer.putInt(msgSize);
            buffer.putLong(reserved);

            // 计算checksum（前 28 字节）
            java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
            crc32.update(buffer.array(), 0, 28);
            int checksum = (int) crc32.getValue();

            buffer.putInt(checksum);
            buffer.flip();

            activeIdxFile.appendMessage(buffer.array());
        } catch (Exception e) {
            log.error("[DelayIdx] buildIndex failed, scheduleTime=" + scheduleTime, e);
        }
    }

    public void loadAndDispatch() {
        long now = System.currentTimeMillis();

        long currentFileId = (now / TIME_SLICE_MS) * TIME_SLICE_MS;
        long nextFileId = currentFileId + TIME_SLICE_MS;

        scanAndDispatch(currentFileId, now);
        // 距离当前切片结束不足 5 分钟时，提前开始扫描下一个切片
        if ((currentFileId + TIME_SLICE_MS - now) < LOOK_AHEAD_MS) {
            scanAndDispatch(nextFileId, now);
        }

        checkpointManager.flush();
    }

    private void scanAndDispatch(long fileId, long now) {
        MappedFileQueue indexQueue = getOrCreateIdxQueue(fileId);

        // 从 checkpoint 获取已扫描到的 offset
        long fileSlotBase = fileId / TIME_SLICE_MS;
        long lastScanOffset = checkpointManager.getScanOffset(fileSlotBase);

        MappedFile mappedFile = indexQueue.findMappedFileByOffset(lastScanOffset);
        if (mappedFile == null) return;

        int relativePosition = (int) (lastScanOffset - mappedFile.getFileFromOffset());

        // 批量读取
        int availableBytes = mappedFile.getReadPosition() - relativePosition;
        if (availableBytes < DelayIndex.ENTRY_SIZE) return;
        int maxExpectedBytes = MAX_READ_ENTRIES_PER_SCAN * DelayIndex.ENTRY_SIZE;
        int readSize = Math.min(maxExpectedBytes, availableBytes);

        SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(relativePosition, readSize);
        if (sbr == null) return;

        List<DelayIndex> toDispatch = new ArrayList<>();
        int scannedBytes = 0;
        try {
            ByteBuffer buffer = sbr.getByteBuffer();
            while (buffer.remaining() >= DelayIndex.ENTRY_SIZE) {
                buffer.mark();
                long scheduleTime = buffer.getLong();
                if (scheduleTime > now + LOOK_AHEAD_MS) {
                    buffer.reset();
                    break;
                }
                buffer.reset();
                DelayIndex index = DelayIndex.decode(buffer);
                toDispatch.add(index);
            }
            scannedBytes = buffer.position();
        } catch (Exception e) {
            log.error("[DelayIdx] decode failed, fileId=" + fileId, e);
        } finally {
            sbr.release();
        }

        // 投递
        for (DelayIndex index : toDispatch) {
            long delayMs = index.getScheduleTime() - now;
            if (delayMs <= 0) {
                executeForwardAsync(fileId, index.getOffset(), index.getSize(), 0);
            } else {
                timer.newTimeout(timeout ->
                        executeForwardAsync(fileId, index.getOffset(), index.getSize(), 0), delayMs, TimeUnit.MILLISECONDS);
            }
        }

        checkpointManager.putScanOffset(fileSlotBase, lastScanOffset + scannedBytes);
    }

    public void cleanExpiredIndices(long expiredTimeBound) {
        indexFileMap.keySet().removeIf(fileId -> {
            if (fileId < expiredTimeBound) {
                MappedFileQueue queue = indexFileMap.get(fileId);
                if (queue != null) {
                    queue.shutdown(1000 * 10);
                }
                String sliceIndexDir = indexDir + File.separator + resolveFolder(fileId);
                deleteDirectory(new File(sliceIndexDir));
                indexFileMap.remove(fileId);
                long fileSlotBase = fileId / TIME_SLICE_MS;
                checkpointManager.resetSlice(fileSlotBase);
                return true;
            }
            return false;
        });
    }

    public void start() {
        this.indexScanner.start();
    }

    public void shutdown() {
        this.indexScanner.shutdown();
        this.timer.stop();
        this.dispatchExecutor.shutdown();
        checkpointManager.flush();
        try {
            checkpointManager.close();
        } catch (IOException e) {
            log.error("[DelayIdx] close checkpointManager failed", e);
        }
    }

    private void ensureDirOK() {
        File dir = new File(indexDir);
        if (!dir.exists()) dir.mkdirs();
    }

    private void deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }
            path.delete();
        }
    }

    private void executeForwardAsync(long fileId, long offset, int msgSize, int retryCount) {
        this.dispatchExecutor.submit(() -> {
            try {
                executeForward(fileId, offset, msgSize, retryCount);
            } catch (Throwable e) {
                log.error("[DelayIdx] executeForward task failed, fileId={}, offset={}, retry={}", fileId, offset, retryCount, e);
                scheduleRetry(fileId, offset, msgSize, retryCount);
            }
        });
    }

    private void executeForward(long fileId, long offset, int msgSize, int retryCount) {
        MessageExt msgExt = this.delayLogService.getMessage(fileId, offset, msgSize);
        if (msgExt == null) {
            log.warn("[DelayIdx] message not found in WAL, fileId={}, offset={}, size={}", fileId, offset, msgSize);
            return;
        }
        // TODO: 暂时先写入本地commit,将来直接发送到正常的broker
        MessageExtBrokerInner msgInner = convertToInnerMessage(msgExt);
        PutMessageResult result = this.messageStore.putMessage(msgInner);
        if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            log.info("[DelayIdx] forward success, topic={}, msgId={}, retry={}",
                    msgInner.getTopic(), msgInner.getMsgId(), retryCount);
        } else {
            log.warn("[DelayIdx] forward failed, putStatus={}, topic={}, retry={}",
                    result != null ? result.getPutMessageStatus() : "null",
                    msgInner.getTopic(), retryCount);
            scheduleRetry(fileId, offset, msgSize, retryCount);
        }
    }

    private MessageExtBrokerInner convertToInnerMessage(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);

        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME);

        String realTopic = msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
        if (realTopic != null) {
            msgInner.setTopic(realTopic);
        } else {
            msgInner.setTopic(msgExt.getTopic());
        }

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        if (queueIdStr != null) {
            try {
                msgInner.setQueueId(Integer.parseInt(queueIdStr));
            } catch (NumberFormatException e) {
                msgInner.setQueueId(0);
            }
        } else {
            msgInner.setQueueId(msgExt.getQueueId());
        }

        return msgInner;
    }

    // ===================== 重试机制 =====================

    private void scheduleRetry(long fileId, long offset, int msgSize, int retryCount) {
        if (retryCount >= RETRY_MAX_TIMES) {
            log.error("[DelayIdx] retry exhausted (max={}), dropping message, fileId={}, offset={}, size={}",
                    RETRY_MAX_TIMES, fileId, offset, msgSize);
            return;
        }

        long delayMs = Math.min(RETRY_BASE_DELAY_MS * (1L << retryCount), RETRY_MAX_DELAY_MS);
        log.warn("[DelayIdx] scheduling retry #{}, delay={}ms, fileId={}, offset={}",
                retryCount + 1, delayMs, fileId, offset);
        retryQueue.offer(new RetryMessage(fileId, offset, msgSize, retryCount + 1, delayMs));
    }

    private static class RetryMessage implements Delayed {
        final long fileId;
        final long offset;
        final int msgSize;
        final int retryCount;
        final long executeTimeMs;

        RetryMessage(long fileId, long offset, int msgSize, int retryCount, long delayMs) {
            this.fileId = fileId;
            this.offset = offset;
            this.msgSize = msgSize;
            this.retryCount = retryCount;
            this.executeTimeMs = System.currentTimeMillis() + delayMs;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(this.executeTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof RetryMessage) {
                return Long.compare(this.executeTimeMs, ((RetryMessage) o).executeTimeMs);
            }
            return 0;
        }
    }

    private MappedFileQueue getOrCreateIdxQueue(long fileId) {
        return indexFileMap.computeIfAbsent(fileId, id -> {
            String sliceIndexDir = indexDir + File.separator + resolveFolder(id);
            File dir = new File(sliceIndexDir);
            if (!dir.exists()) dir.mkdirs();
            return new MappedFileQueue(sliceIndexDir, DELAY_IDX_FILE_SIZE, allocateMappedFileService);
        });
    }

    private static class CheckpointManager implements Closeable {

        private static final int REGION_COUNT = 1800;
        private static final int SLOT_SIZE = 16;
        private static final int FILE_SIZE = REGION_COUNT * SLOT_SIZE;

        private final RandomAccessFile file;
        private final FileChannel fileChannel;
        private final MappedByteBuffer mappedByteBuffer;

        public CheckpointManager(String fileName) throws IOException {
            File f = new File(fileName);
            if (!f.exists()) {
                File parent = f.getParentFile();
                if (parent != null && !parent.exists()) {
                    parent.mkdirs();
                }
                f.createNewFile();
            }
            try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                if (raf.length() < FILE_SIZE) {
                    raf.setLength(FILE_SIZE);
                }
            }
            this.file = new RandomAccessFile(f, "rw");
            this.fileChannel = this.file.getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE);
        }

        private int slotOffset(long fileSlotBase) {
            return (int) (fileSlotBase % REGION_COUNT) * SLOT_SIZE;
        }

        long getScanOffset(long fileSlotBase) {
            int offset = slotOffset(fileSlotBase);
            long storedFileSlotBase = mappedByteBuffer.getLong(offset);
            if (storedFileSlotBase != fileSlotBase) {
                // slot 被其他切片复用过，重置
                mappedByteBuffer.putLong(offset, fileSlotBase);
                mappedByteBuffer.putLong(offset + 8, 0L);
                return 0L;
            }
            return mappedByteBuffer.getLong(offset + 8);
        }

        void putScanOffset(long fileSlotBase, long scanOffset) {
            int offset = slotOffset(fileSlotBase);
            mappedByteBuffer.putLong(offset, fileSlotBase);
            mappedByteBuffer.putLong(offset + 8, scanOffset);
        }

        void resetSlice(long fileSlotBase) {
            int offset = slotOffset(fileSlotBase);
            mappedByteBuffer.putLong(offset, 0L);
            mappedByteBuffer.putLong(offset + 8, 0L);
        }

        void flush() {
            mappedByteBuffer.force();
        }

        @Override
        public void close() throws IOException {
            mappedByteBuffer.force();
            fileChannel.close();
            file.close();
        }
    }

    private class DelayIndexScanner extends ServiceThread {

        private static final long SCAN_INTERVAL_MS = 500;

        @Override
        public String getServiceName() {
            return DelayIndexScanner.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    this.waitForRunning(SCAN_INTERVAL_MS);
                    if (!this.isStopped()) {
                        loadAndDispatch();
                    }
                    processRetryQueue();
                } catch (Exception e) {
                    log.error("[DelayIdx] scanner error", e);
                }
            }
        }

        private void processRetryQueue() {
            while (true) {
                RetryMessage retryMsg = retryQueue.poll();
                if (retryMsg == null) break;
                executeForwardAsync(retryMsg.fileId, retryMsg.offset, retryMsg.msgSize, retryMsg.retryCount);
            }
        }
    }
}
