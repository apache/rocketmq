/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store.config;

import java.io.File;

import com.alibaba.rocketmq.common.annotation.ImportantField;
import com.alibaba.rocketmq.store.ConsumeQueue;


/**
 * 存储层配置文件类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class MessageStoreConfig {
    // 存储跟目录
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    // CommitLog存储目录
    @ImportantField
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "commitlog";

    // CommitLog每个文件大小 1G
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024;
    // ConsumeQueue每个文件大小 默认存储30W条消息
    private int mapedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQStoreUnitSize;
    // CommitLog刷盘间隔时间（单位毫秒）
    @ImportantField
    private int flushIntervalCommitLog = 1000;
    // 是否定时方式刷盘，默认是实时刷盘
    @ImportantField
    private boolean flushCommitLogTimed = false;
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
    @ImportantField
    private String deleteWhen = "04";
    // 磁盘空间最大使用率
    private int diskMaxUsedSpaceRatio = 75;
    // 文件保留时间（单位小时）
    @ImportantField
    private int fileReservedTime = 72;
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
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    // 最大被拉取的消息个数，消息在内存
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    // 最大被拉取的消息字节数，消息在磁盘
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    // 最大被拉取的消息个数，消息在磁盘
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    // 命中消息在内存的最大比例
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    // 是否开启消息索引功能
    @ImportantField
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 64;
    // 是否使用安全的消息索引功能，即可靠模式。
    // 可靠模式下，异常宕机恢复慢
    // 非可靠模式下，异常宕机恢复快
    @ImportantField
    private boolean messageIndexSafe = false;
    // HA功能
    private int haListenPort = 10912;
    private int haSendHeartbeatInterval = 1000 * 5;
    private int haHousekeepingInterval = 1000 * 20;
    private int haTransferBatchSize = 1024 * 32;
    // 如果不设置，则从NameServer获取Master HA服务地址
    @ImportantField
    private String haMasterAddress = null;
    // Slave落后Master超过此值，则认为存在异常
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;
    @ImportantField
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    // 同步刷盘超时时间
    private int syncFlushTimeout = 1000 * 5;
    // 定时消息相关
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    private long flushDelayOffsetInterval = 1000 * 10;
    // 磁盘空间超过90%警戒水位，自动开始删除文件
    @ImportantField
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


    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }


    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }


    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }


    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }


    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }


    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }


    public String getStorePathRootDir() {
        return storePathRootDir;
    }


    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }
}
