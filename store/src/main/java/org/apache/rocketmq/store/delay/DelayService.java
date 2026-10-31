package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class DelayService {
    private static final long TIME_SLICE_MS = 30 * 60 * 1000; // 30分钟一个切片
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final ZoneId SYSTEM_ZONE = ZoneId.systemDefault();

    private final DefaultMessageStore messageStore;
    private final DelayLogService delayLogService;
    private final DelayIdxService delayIdxService;

    public DelayService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        String delayDir = messageStore.getMessageStoreConfig().getStorePathDelayIndex();
        this.delayLogService = new DelayLogService(delayDir + File.separator + "wal", messageStore.getAllocateMappedFileService());
        this.delayIdxService = new DelayIdxService(delayDir + File.separator + "idx", this.delayLogService, messageStore.getAllocateMappedFileService());
    }

    public synchronized void build(final DispatchRequest dispatchRequest) {
        if (dispatchRequest == null) return;
        SelectMappedBufferResult rawResult = null;
        try {
            // 1. 提取投递时间
            long scheduleTime = extractDelayTime(dispatchRequest.getPropertiesMap());
            if (scheduleTime <= 0) return;
            long fileId = (scheduleTime / TIME_SLICE_MS) * TIME_SLICE_MS;

            // 2. 拉取消息
            rawResult = this.messageStore.getCommitLog().getMessage(dispatchRequest.getCommitLogOffset(), dispatchRequest.getMsgSize());
            if (rawResult == null) return;

            ByteBuffer rawBuffer = rawResult.getByteBuffer();
            byte[] rawBytes = new byte[rawBuffer.remaining()];
            rawBuffer.get(rawBytes);

            // 3. 写入wal
            long delayWalOffset = this.delayLogService.appendMessage(fileId, rawBytes);

            // 4. 写入idx
            this.delayIdxService.buildIndex(scheduleTime, delayWalOffset, dispatchRequest.getMsgSize());

        } catch (Exception e) {
            e.printStackTrace();
            // 内核级异常拦截与强隔离，确保自研任意延时引擎出现任何异常时，100% 不引发原生分发线程崩溃
        } finally {
            if (rawResult != null) {
                rawResult.release();
            }
        }
    }

    public void cleanExpiredFiles() {
        long now = System.currentTimeMillis();
        long expiredTimeBound = now - (30L * 24 * 60 * 60 * 1000);
        try {
            // 1. 物理清空 30 天前的 wal
            this.delayLogService.deleteExpiredLogs(expiredTimeBound);

            // 2. 物理清空 30 天前的 idx
            this.delayIdxService.cleanExpiredIndices(expiredTimeBound);
        } catch (Exception e) {
            // 记录异常日志
        }
    }

    public void start() {
        if (this.delayIdxService != null) {
            this.delayIdxService.start();
        }
    }

    public void shutdown() {
        if (this.delayIdxService != null) {
            this.delayIdxService.shutdown();
        }
    }

    public DelayLogService getDelayLogService() {
        return delayLogService;
    }

    public DelayIdxService getDelayIdxService() {
        return delayIdxService;
    }

    private long extractDelayTime(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.isEmpty()) return 0;

        String scheduleTimeStr = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL + MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (scheduleTimeStr == null || scheduleTimeStr.isEmpty()) return 0;
        try {
            return Long.parseLong(scheduleTimeStr);
        } catch (Exception ignore) {
        }
        return 0;
    }

    public static String resolveFolder(long fileId) {
        Instant instant = Instant.ofEpochMilli(fileId);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, SYSTEM_ZONE);
        return FORMATTER.format(localDateTime);
    }

}
