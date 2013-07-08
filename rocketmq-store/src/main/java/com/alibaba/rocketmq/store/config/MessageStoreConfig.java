/**
 * $Id: MessageStoreConfig.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.config;

import java.io.File;

import com.alibaba.rocketmq.store.ConsumeQueue;
import com.alibaba.rocketmq.store.transaction.TransactionStateService;


/**
 * 存储层配置文件类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MessageStoreConfig {
    // CommitLog存储目录
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "commitlog";
    // ConsumeQueue存储目录
    private String storePathConsumeQueue = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "consumequeue";
    // 索引文件存储目录
    private String storePathIndex = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "index";
    // 异常退出产生的文件
    private String storeCheckpoint = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "storeCheckpoint";
    // 异常退出产生的文件
    private String abortFile = System.getProperty("user.home") + File.separator + "store" + File.separator
            + "storeAbort";
    // CommitLog每个文件大小 1G
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024;
    // ConsumeQueue每个文件大小 默认存储50W条消息
    private int mapedFileSizeConsumeQueue = 500000 * ConsumeQueue.CQStoreUnitSize;
    // CommitLog刷盘间隔时间（单位毫秒）
    private int flushIntervalCommitLog = 1000;
    // ConsumeQueue刷盘间隔时间（单位毫秒）
    private int flushIntervalConsumeQueue = 1000;
    // 清理资源间隔时间（单位毫秒）
    private int cleanResourceInterval = 10000;
    // 删除多个CommitLog文件的间隔时间（单位毫秒）
    private int deleteCommitLogFilesInterval = 100;
    // 删除多个ConsumeQueue文件的间隔时间（单位毫秒）
    private int deleteConsumeQueueFilesInterval = 100;
    // 强制删除文件间隔时间（单位毫秒）
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    // 定期检查Hanged文件间隔时间（单位毫秒）
    private int redeleteHangedFileInterval = 1000 * 120;
    // 何时触发删除文件, 默认凌晨4点删除文件
    private String deleteWhen = "04";
    // 磁盘空间最大使用率
    private int diskMaxUsedSpaceRatio = 75;
    // 文件保留时间（单位小时）
    private int fileReservedTime = 12;

    // 写消息索引到ConsumeQueue，缓冲区高水位，超过则开始流控
    private int putMsgIndexHightWater = 600000;
    // 最大消息大小，默认512K
    private int maxMessageSize = 1024 * 512;
    // 重启时，是否校验CRC
    private boolean checkCRCOnRecover = true;
    // 刷CommitLog，至少刷几个PAGE
    private int flushCommitLogLeastPages = 4;
    // 刷ConsumeQueue，至少刷几个PAGE
    private int flushConsumeQueueLeastPages = 2;
    // 刷CommitLog，彻底刷盘间隔时间
    private int flushCommitLogThoroughInterval = 1000 * 10;
    // 刷ConsumeQueue，彻底刷盘间隔时间
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    // 最大被拉取的消息字节数，消息在内存
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    // 最大被拉取的消息个数，消息在内存
    private int maxTransferCountOnMessageInMemory = 32;
    // 最大被拉取的消息字节数，消息在磁盘
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    // 最大被拉取的消息个数，消息在磁盘
    private int maxTransferCountOnMessageInDisk = 8;
    // 命中消息在内存的最大比例
    private int accessMessageInMemoryMaxRatio = 30;

    // 是否开启消息索引功能
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 32;

    // HA功能
    private int haListenPort = 10912;
    private int haSendHeartbeatInterval = 1000 * 5;
    private int haHousekeepingInterval = 1000 * 20;
    private int haTransferBatchSize = 1024 * 32;
    // 如果不设置，则从NameServer获取Master HA服务地址
    private String haMasterAddress = null;

    // Slave落后Master超过此值，则认为存在异常
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;

    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;

    // 同步刷盘超时时间
    private int syncFlushTimeout = 1000 * 5;

    // 定时消息相关
    private String messageDelayLevel = "1s 5s 10s 30s 1m 5m 10m 30m 1h 2h 6h 12h 1d";
    private long flushDelayOffsetInterval = 1000 * 5;
    private String delayOffsetStorePath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "delayOffset.properties";

    // 分布式事务配置
    private String tranStateTableStorePath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "transaction" + File.separator + "statetable";
    private int tranStateTableMapedFileSize = 2000000 * TransactionStateService.TSStoreUnitSize;

    private String tranRedoLogStorePath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "transaction" + File.separator + "redolog";
    private int tranRedoLogMapedFileSize = 2000000 * ConsumeQueue.CQStoreUnitSize;

    // 事务回查至少间隔时间
    private long checkTransactionMessageAtleastInterval = 1000 * 40;
    // 事务回查定时间隔时间
    private long checkTransactionMessageTimerInterval = 1000 * 20;

    // 磁盘空间超过90%警戒水位，自动开始删除文件
    private boolean cleanFileForciblyEnable = true;


    public int getMapedFileSizeCommitLog() {
        return mapedFileSizeCommitLog;
    }


    public void setMapedFileSizeCommitLog(int mapedFileSizeCommitLog) {
        this.mapedFileSizeCommitLog = mapedFileSizeCommitLog;
    }


    public int getMapedFileSizeConsumeQueue() {
        // 此处需要向上取整
        int factor = (int) Math.ceil(this.mapedFileSizeConsumeQueue / (ConsumeQueue.CQStoreUnitSize * 1.0));
        return (int) (factor * ConsumeQueue.CQStoreUnitSize);
    }


    public void setMapedFileSizeConsumeQueue(int mapedFileSizeConsumeQueue) {
        this.mapedFileSizeConsumeQueue = mapedFileSizeConsumeQueue;
    }


    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }


    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }


    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }


    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }


    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }


    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }


    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }


    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }


    public int getMaxMessageSize() {
        return maxMessageSize;
    }


    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }


    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }


    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }


    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }


    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }


    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }


    public String getStorePathConsumeQueue() {
        return storePathConsumeQueue;
    }


    public void setStorePathConsumeQueue(String storePathConsumeQueue) {
        this.storePathConsumeQueue = storePathConsumeQueue;
    }


    public String getAbortFile() {
        return abortFile;
    }


    public void setAbortFile(String abortFile) {
        this.abortFile = abortFile;
    }


    public String getDeleteWhen() {
        return deleteWhen;
    }


    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }


    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;

        if (this.diskMaxUsedSpaceRatio > 95)
            return 95;

        return diskMaxUsedSpaceRatio;
    }


    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }


    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }


    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }


    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }


    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }


    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }


    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }


    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }


    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }


    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }


    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }


    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }


    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }


    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }


    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }


    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }


    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }


    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }


    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }


    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }


    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }


    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }


    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }


    public String getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public void setStoreCheckpoint(String storeCheckpoint) {
        this.storeCheckpoint = storeCheckpoint;
    }


    public int getFileReservedTime() {
        return fileReservedTime;
    }


    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }


    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }


    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }


    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }


    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }


    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }


    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }


    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }


    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }


    public int getMaxIndexNum() {
        return maxIndexNum;
    }


    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }


    public String getStorePathIndex() {
        return storePathIndex;
    }


    public void setStorePathIndex(String storePathIndex) {
        this.storePathIndex = storePathIndex;
    }


    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }


    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }


    public int getHaListenPort() {
        return haListenPort;
    }


    public void setHaListenPort(int haListenPort) {
        this.haListenPort = haListenPort;
    }


    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }


    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }


    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }


    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }


    public BrokerRole getBrokerRole() {
        return brokerRole;
    }


    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }


    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }


    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }


    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }


    public int getHaSlaveFallbehindMax() {
        return haSlaveFallbehindMax;
    }


    public void setHaSlaveFallbehindMax(int haSlaveFallbehindMax) {
        this.haSlaveFallbehindMax = haSlaveFallbehindMax;
    }


    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }


    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }


    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }


    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }


    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }


    public String getHaMasterAddress() {
        return haMasterAddress;
    }


    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }


    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }


    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }


    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }


    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }


    public String getDelayOffsetStorePath() {
        return delayOffsetStorePath;
    }


    public void setDelayOffsetStorePath(String delayOffsetStorePath) {
        this.delayOffsetStorePath = delayOffsetStorePath;
    }


    public String getTranStateTableStorePath() {
        return tranStateTableStorePath;
    }


    public void setTranStateTableStorePath(String tranStateTableStorePath) {
        this.tranStateTableStorePath = tranStateTableStorePath;
    }


    public int getTranStateTableMapedFileSize() {
        return tranStateTableMapedFileSize;
    }


    public void setTranStateTableMapedFileSize(int tranStateTableMapedFileSize) {
        this.tranStateTableMapedFileSize = tranStateTableMapedFileSize;
    }


    public String getTranRedoLogStorePath() {
        return tranRedoLogStorePath;
    }


    public void setTranRedoLogStorePath(String tranRedoLogStorePath) {
        this.tranRedoLogStorePath = tranRedoLogStorePath;
    }


    public int getTranRedoLogMapedFileSize() {
        return tranRedoLogMapedFileSize;
    }


    public void setTranRedoLogMapedFileSize(int tranRedoLogMapedFileSize) {
        this.tranRedoLogMapedFileSize = tranRedoLogMapedFileSize;
    }


    public long getCheckTransactionMessageAtleastInterval() {
        return checkTransactionMessageAtleastInterval;
    }


    public void setCheckTransactionMessageAtleastInterval(long checkTransactionMessageAtleastInterval) {
        this.checkTransactionMessageAtleastInterval = checkTransactionMessageAtleastInterval;
    }


    public long getCheckTransactionMessageTimerInterval() {
        return checkTransactionMessageTimerInterval;
    }


    public void setCheckTransactionMessageTimerInterval(long checkTransactionMessageTimerInterval) {
        this.checkTransactionMessageTimerInterval = checkTransactionMessageTimerInterval;
    }


    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }


    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }
}
