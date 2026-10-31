package org.apache.rocketmq.store.delay;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.util.HashedWheelTimer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.AllocateMappedFileService;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.store.delay.DelayService.resolveFolder;

public class DelayIdxService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final long TIME_SLICE_MS = 30 * 60 * 1000; // 30分钟一个切片
    private static final int DELAY_IDX_FILE_SIZE = 50 * 1024 * 1024; // 延时消息索引文件，50MB

    private final String indexDir;
    private final DelayLogService delayLogService;
    private final DelayIndexScanner indexScanner;
    private final HashedWheelTimer timer = new HashedWheelTimer(1, TimeUnit.SECONDS, 60);
    private final ConcurrentHashMap<Long, Long> offsetTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, MappedFileQueue> indexFileMap = new ConcurrentHashMap<>();
    private final AllocateMappedFileService allocateMappedFileService;
    private final String checkpointFile;

    public DelayIdxService(String indexDir, DelayLogService delayLogService, AllocateMappedFileService allocateMappedFileService) {
        this.delayLogService = delayLogService;
        this.allocateMappedFileService = allocateMappedFileService;
        this.indexDir = indexDir;
        this.checkpointFile = indexDir + File.separator + "delay_idx_checkpoint.json";
        ensureDirOK();
        this.indexScanner = new DelayIndexScanner();
    }

    public synchronized void buildIndex(long scheduleTime, long offset, int msgSize) {
        try {
            // 1.
            long fileId = (scheduleTime / TIME_SLICE_MS) * TIME_SLICE_MS;

            // 2.
            MappedFileQueue indexQueue = getOrCreateIdxQueue(fileId);
            MappedFile activeFile = indexQueue.getLastMappedFile(0);

            // 3. 计算checksum
            ByteBuffer buffer = ByteBuffer.allocate(DelayIndex.ENTRY_SIZE);
            long reserved = 0L;
            buffer.putLong(scheduleTime);
            buffer.putLong(offset);
            buffer.putInt(msgSize);
            buffer.putLong(reserved);
            java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
            crc32.update(buffer.array(), 0, 28);
            int checksum = (int) crc32.getValue();
            buffer.putInt(checksum);
            buffer.flip();

            // 4. 写入index
            activeFile.appendMessage(buffer.array());
        } catch (Exception e) {
            log.error("[DelayIdx] append idx failed", e);
        }
    }

    public void loadAndDispatch() {
        long now = System.currentTimeMillis();

        long currentFileId = (now / TIME_SLICE_MS) * TIME_SLICE_MS;
        long nextFileId = currentFileId + TIME_SLICE_MS;

        flushIncrementalToWheel(currentFileId, now);
        flushIncrementalToWheel(nextFileId, now);

        persist();
    }

    public void cleanExpiredIndices(long expiredTimeBound) {
        indexFileMap.forEach((fileId, queue) -> {
            if (fileId < expiredTimeBound) {
                queue.shutdown(1000 * 10);
                String sliceIndexDir = indexDir + File.separator + fileId;
                deleteDirectory(new File(sliceIndexDir));
                indexFileMap.remove(fileId);
                offsetTable.remove(fileId);
            }
        });
    }

    public void start() {
        load();
        this.indexScanner.start();
    }

    public void shutdown() {
        this.indexScanner.shutdown();
        this.timer.stop();
    }

    private void load() {
        File file = new File(checkpointFile);
        if (!file.exists()) {
            return;
        }
        try {
            String content = MixAll.file2String(checkpointFile);
            if (content == null || content.isEmpty()) return;
            Map<Long, Long> map = JSON.parseObject(content, new TypeReference<Map<Long, Long>>() {
            });
            this.offsetTable.putAll(map);
        } catch (Exception ignore) {
        }
    }

    private void persist() {
        if (this.offsetTable.isEmpty()) return;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            for (Map.Entry<Long, Long> entry : this.offsetTable.entrySet()) {
                sb.append("  ").append(entry.getKey()).append(":").append(entry.getValue()).append(",\n");
            }
            sb.append("}");
            MixAll.string2File(sb.toString(), this.checkpointFile);
        } catch (Exception ignore) {
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

    private void flushIncrementalToWheel(long fileId, long now) {
        // 1.
        MappedFileQueue indexQueue = getOrCreateIdxQueue(fileId);

        // 2.
        long lastLoadOffset = offsetTable.getOrDefault(fileId, 0L);
        MappedFile mappedFile = indexQueue.findMappedFileByOffset(lastLoadOffset);
        if (mappedFile == null) return;

        // 3.
        int relativePosition = (int) (lastLoadOffset - mappedFile.getFileFromOffset());
        // 4.
        List<DelayIndex> newIndices = new ArrayList<>();
        int availableBytes = mappedFile.getReadPosition() - relativePosition;
        if (availableBytes < DelayIndex.ENTRY_SIZE) return;
        int maxExpectedBytes = 1000 * DelayIndex.ENTRY_SIZE;
        int size = Math.min(maxExpectedBytes, availableBytes);
        SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(relativePosition, size);
        if (sbr != null) {
            try {
                ByteBuffer buffer = sbr.getByteBuffer();
                while (buffer.remaining() >= DelayIndex.ENTRY_SIZE) {
                    DelayIndex index = DelayIndex.decode(buffer);
                    newIndices.add(index);
                }
            } catch (Exception e) {
                log.error("[DelayIdx] decode failed", e);
            } finally {
                sbr.release();
            }
        }

        // 5.
        for (DelayIndex index : newIndices) {
            long delayMs = index.getScheduleTime() - now;
            if (delayMs <= 0) {
                // 情况 A：已经过期错过了,立刻触发执行内核分发
                executeForward(fileId, index.getOffset(), index.getSize());
            } else {
                // 情况 B：未来的热数据,挂载到内存时间轮上
                timer.newTimeout(timeout -> executeForward(fileId, index.getOffset(), index.getSize()), delayMs, TimeUnit.MILLISECONDS);
            }

            lastLoadOffset += DelayIndex.ENTRY_SIZE;
        }

        offsetTable.put(fileId, lastLoadOffset);
    }

    private void executeForward(long fileId, long offset, int msgSize) {
        MessageExt msgExt = this.delayLogService.getMessage(fileId, offset, msgSize);
        if (msgExt != null) {
            String s = msgExt.getProperties().get(MessageConst.PROPERTY_DELAY_TIME_LEVEL + MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ss = Long.parseLong(s);
            try {
                // TODO: 1. 写入远端的 delay-server
                //       2. 失败重新写入wal或者idx
                System.out.printf("[消息到期] 现在[%s],延迟:[%s] msg = %s\n", sdf.format(new Date()), sdf.format(new Date(ss)), msgExt);
            } catch (Exception e) {
                // 异常退避：转发失败可扔进本地轻量级 DelayQueue 执行 5s、10s 指数退避重试，确保 At-least-once 语义
            }
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

    public class DelayIndexScanner extends ServiceThread {

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
                } catch (Exception ignore) {
                }
            }
        }
    }
}
