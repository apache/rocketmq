package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.AllocateMappedFileService;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.store.delay.DelayService.resolveFolder;

public class DelayLogService {
    private static final int DELAY_LOG_FILE_SIZE = 50 * 1024 * 1024; // 专属延时 WAL 文件固定 50MB
    private final String storePath;
    private final AllocateMappedFileService allocateMappedFileService;
    private final ConcurrentHashMap<Long, MappedFileQueue> delayCommitLogPool = new ConcurrentHashMap<>();

    public DelayLogService(String logDir, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = logDir;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public long appendMessage(long fileId, byte[] rawBytes) {
        try {
            MappedFileQueue walFileQueue = getOrCreateLogQueue(fileId);
            MappedFile activeLogFile = walFileQueue.getLastMappedFile(0);

            long delayWalOffset = activeLogFile.getFileFromOffset() + activeLogFile.getWrotePosition();

            activeLogFile.appendMessage(rawBytes);

            return delayWalOffset;
        } catch (Exception e) {
            throw new RuntimeException("Append to shadow delay commitlog failed", e);
        }
    }

    public MessageExt getMessage(long fileId, long offset, int size) {
        MappedFileQueue delayLogQueue = getOrCreateLogQueue(fileId);
        if (delayLogQueue == null) return null;

        MappedFile mappedFile = delayLogQueue.findMappedFileByOffset(offset);
        if (mappedFile == null) return null;

        int relativePosition = (int) (offset - mappedFile.getFileFromOffset());

        SelectMappedBufferResult selectResult = mappedFile.selectMappedBuffer(relativePosition, size);
        if (selectResult != null) {
            try {
                ByteBuffer byteBuffer = selectResult.getByteBuffer();
                byteBuffer.limit(size);
                return MessageDecoder.decode(byteBuffer);
            } finally {
                selectResult.release(); // 极其重要：立马回收堆外引用计数，防止 PageCache 内存熔断
            }
        }
        return null;
    }

    private MappedFileQueue getOrCreateLogQueue(long fileId) {
        return delayCommitLogPool.computeIfAbsent(fileId, id -> {
            String folderName = resolveFolder(id);
            String delayWalDir = storePath + File.separator + folderName;
            File dir = new File(delayWalDir);
            if (!dir.exists()) dir.mkdirs();
            return new MappedFileQueue(delayWalDir, DELAY_LOG_FILE_SIZE, allocateMappedFileService);
        });
    }

    public void deleteExpiredLogs(long expiredTimeBound) {
        delayCommitLogPool.forEach((fileId, queue) -> {
            if (fileId < expiredTimeBound) {
                queue.shutdown(1000 * 10);
                String folderName = resolveFolder(fileId);
                String delayWalDir = storePath + folderName;
                deleteDirectory(new File(delayWalDir));
                delayCommitLogPool.remove(fileId);
            }
        });
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

}
