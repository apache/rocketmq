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
package org.apache.rocketmq.tieredstore;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class MessageStoreConfig {

    private String brokerName = localHostName();
    private String brokerClusterName = "DefaultCluster";
    private TieredStorageLevel tieredStorageLevel = TieredStorageLevel.NOT_IN_DISK;

    /**
     * All fetch requests are judged against this level first,
     * and if the message cannot be read from the TiredMessageStore,
     * these requests will still go to the next store for fallback processing.
     */
    public enum TieredStorageLevel {
        /**
         * Disable tiered storage, all fetch request will be handled by default message store.
         */
        DISABLE(0),
        /**
         * Only fetch request with offset not in disk will be handled by tiered storage.
         */
        NOT_IN_DISK(1),
        /**
         * Only fetch request with offset not in memory(page cache) will be handled by tiered storage.
         */
        NOT_IN_MEM(2),
        /**
         * All fetch request will be handled by tiered storage.
         */
        FORCE(3);

        private final int value;

        TieredStorageLevel(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static TieredStorageLevel valueOf(int value) {
            switch (value) {
                case 1:
                    return NOT_IN_DISK;
                case 2:
                    return NOT_IN_MEM;
                case 3:
                    return FORCE;
                default:
                    return DISABLE;
            }
        }

        public boolean isEnable() {
            return this.value > 0;
        }

        public boolean check(TieredStorageLevel targetLevel) {
            return this.value >= targetLevel.value;
        }
    }

    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";
    private boolean messageIndexEnable = true;
    private boolean recordGetMessageResult = false;

    // CommitLog file size, default is 1G
    private long tieredStoreCommitLogMaxSize = 1024 * 1024 * 1024;
    // ConsumeQueue file size, default is 100M
    private long tieredStoreConsumeQueueMaxSize = 100 * 1024 * 1024;
    private int tieredStoreIndexFileMaxHashSlotNum = 5000000;
    private int tieredStoreIndexFileMaxIndexNum = 5000000 * 4;
    // index file will force rolling to next file after idle specified time, default is 3h
    private int tieredStoreIndexFileRollingIdleInterval = 3 * 60 * 60 * 1000;
    private String tieredMetadataServiceProvider = "org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore";
    private String tieredBackendServiceProvider = "org.apache.rocketmq.tieredstore.provider.MemoryFileSegment";
    // file reserved time, default is 72 hour
    private int tieredStoreFileReservedTime = 72;
    // time of forcing commitLog to roll to next file, default is 24 hour
    private int commitLogRollingInterval = 24;
    // rolling will only happen if file segment size is larger than commitcp b  LogRollingMinimumSize, default is 128M
    private int commitLogRollingMinimumSize = 128 * 1024 * 1024;
    // default is 100, unit is millisecond
    private int maxCommitJitter = 100;

    private boolean tieredStoreGroupCommit = true;
    // Cached message count larger than this value will trigger async commit. default is 1000
    private int tieredStoreGroupCommitCount = 4 * 1024;
    // Cached message size larger than this value will trigger async commit. default is 32M
    private int tieredStoreGroupCommitSize = 4 * 1024 * 1024;
    // Cached message count larger than this value will suspend append. default is 2000
    private int tieredStoreMaxGroupCommitCount = 10000;
    private long tieredStoreMaxFallBehindSize = 128 * 1024 * 1024;

    private boolean readAheadCacheEnable = true;
    private int readAheadMessageCountThreshold = 4096;
    private int readAheadMessageSizeThreshold = 16 * 1024 * 1024;
    private long readAheadCacheExpireDuration = 15 * 1000;
    private double readAheadCacheSizeThresholdRate = 0.3;

    private int tieredStoreMaxPendingLimit = 10000;
    private boolean tieredStoreCrcCheckEnable = false;

    private String tieredStoreFilePath = "";
    private String objectStoreEndpoint = "";
    private String objectStoreBucket = "";
    private String objectStoreAccessKey = "";
    private String objectStoreSecretKey = "";

    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) {
        }

        return "DEFAULT_BROKER";
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public TieredStorageLevel getTieredStorageLevel() {
        return tieredStorageLevel;
    }

    public void setTieredStorageLevel(TieredStorageLevel tieredStorageLevel) {
        this.tieredStorageLevel = tieredStorageLevel;
    }

    public void setTieredStorageLevel(int tieredStorageLevel) {
        this.tieredStorageLevel = TieredStorageLevel.valueOf(tieredStorageLevel);
    }

    public void setTieredStorageLevel(String tieredStorageLevel) {
        this.tieredStorageLevel = TieredStorageLevel.valueOf(tieredStorageLevel);
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public boolean isRecordGetMessageResult() {
        return recordGetMessageResult;
    }

    public void setRecordGetMessageResult(boolean recordGetMessageResult) {
        this.recordGetMessageResult = recordGetMessageResult;
    }

    public long getTieredStoreCommitLogMaxSize() {
        return tieredStoreCommitLogMaxSize;
    }

    public void setTieredStoreCommitLogMaxSize(long tieredStoreCommitLogMaxSize) {
        this.tieredStoreCommitLogMaxSize = tieredStoreCommitLogMaxSize;
    }

    public long getTieredStoreConsumeQueueMaxSize() {
        return tieredStoreConsumeQueueMaxSize;
    }

    public void setTieredStoreConsumeQueueMaxSize(long tieredStoreConsumeQueueMaxSize) {
        this.tieredStoreConsumeQueueMaxSize = tieredStoreConsumeQueueMaxSize;
    }

    public int getTieredStoreIndexFileMaxHashSlotNum() {
        return tieredStoreIndexFileMaxHashSlotNum;
    }

    public void setTieredStoreIndexFileMaxHashSlotNum(int tieredStoreIndexFileMaxHashSlotNum) {
        this.tieredStoreIndexFileMaxHashSlotNum = tieredStoreIndexFileMaxHashSlotNum;
    }

    public int getTieredStoreIndexFileMaxIndexNum() {
        return tieredStoreIndexFileMaxIndexNum;
    }

    public void setTieredStoreIndexFileMaxIndexNum(int tieredStoreIndexFileMaxIndexNum) {
        this.tieredStoreIndexFileMaxIndexNum = tieredStoreIndexFileMaxIndexNum;
    }

    public int getTieredStoreIndexFileRollingIdleInterval() {
        return tieredStoreIndexFileRollingIdleInterval;
    }

    public void setTieredStoreIndexFileRollingIdleInterval(int tieredStoreIndexFileRollingIdleInterval) {
        this.tieredStoreIndexFileRollingIdleInterval = tieredStoreIndexFileRollingIdleInterval;
    }

    public String getTieredMetadataServiceProvider() {
        return tieredMetadataServiceProvider;
    }

    public void setTieredMetadataServiceProvider(String tieredMetadataServiceProvider) {
        this.tieredMetadataServiceProvider = tieredMetadataServiceProvider;
    }

    public String getTieredBackendServiceProvider() {
        return tieredBackendServiceProvider;
    }

    public void setTieredBackendServiceProvider(String tieredBackendServiceProvider) {
        this.tieredBackendServiceProvider = tieredBackendServiceProvider;
    }

    public int getTieredStoreFileReservedTime() {
        return tieredStoreFileReservedTime;
    }

    public void setTieredStoreFileReservedTime(int tieredStoreFileReservedTime) {
        this.tieredStoreFileReservedTime = tieredStoreFileReservedTime;
    }

    public int getCommitLogRollingInterval() {
        return commitLogRollingInterval;
    }

    public void setCommitLogRollingInterval(int commitLogRollingInterval) {
        this.commitLogRollingInterval = commitLogRollingInterval;
    }

    public int getCommitLogRollingMinimumSize() {
        return commitLogRollingMinimumSize;
    }

    public void setCommitLogRollingMinimumSize(int commitLogRollingMinimumSize) {
        this.commitLogRollingMinimumSize = commitLogRollingMinimumSize;
    }

    public int getMaxCommitJitter() {
        return maxCommitJitter;
    }

    public void setMaxCommitJitter(int maxCommitJitter) {
        this.maxCommitJitter = maxCommitJitter;
    }

    public boolean isTieredStoreGroupCommit() {
        return tieredStoreGroupCommit;
    }

    public void setTieredStoreGroupCommit(boolean tieredStoreGroupCommit) {
        this.tieredStoreGroupCommit = tieredStoreGroupCommit;
    }

    public int getTieredStoreGroupCommitCount() {
        return tieredStoreGroupCommitCount;
    }

    public void setTieredStoreGroupCommitCount(int tieredStoreGroupCommitCount) {
        this.tieredStoreGroupCommitCount = tieredStoreGroupCommitCount;
    }

    public int getTieredStoreGroupCommitSize() {
        return tieredStoreGroupCommitSize;
    }

    public void setTieredStoreGroupCommitSize(int tieredStoreGroupCommitSize) {
        this.tieredStoreGroupCommitSize = tieredStoreGroupCommitSize;
    }

    public int getTieredStoreMaxGroupCommitCount() {
        return tieredStoreMaxGroupCommitCount;
    }

    public void setTieredStoreMaxGroupCommitCount(int tieredStoreMaxGroupCommitCount) {
        this.tieredStoreMaxGroupCommitCount = tieredStoreMaxGroupCommitCount;
    }

    public long getTieredStoreMaxFallBehindSize() {
        return tieredStoreMaxFallBehindSize;
    }

    public void setTieredStoreMaxFallBehindSize(long tieredStoreMaxFallBehindSize) {
        this.tieredStoreMaxFallBehindSize = tieredStoreMaxFallBehindSize;
    }

    public boolean isReadAheadCacheEnable() {
        return readAheadCacheEnable;
    }

    public void setReadAheadCacheEnable(boolean readAheadCacheEnable) {
        this.readAheadCacheEnable = readAheadCacheEnable;
    }

    public int getReadAheadMessageCountThreshold() {
        return readAheadMessageCountThreshold;
    }

    public void setReadAheadMessageCountThreshold(int readAheadMessageCountThreshold) {
        this.readAheadMessageCountThreshold = readAheadMessageCountThreshold;
    }

    public int getReadAheadMessageSizeThreshold() {
        return readAheadMessageSizeThreshold;
    }

    public void setReadAheadMessageSizeThreshold(int readAheadMessageSizeThreshold) {
        this.readAheadMessageSizeThreshold = readAheadMessageSizeThreshold;
    }

    public long getReadAheadCacheExpireDuration() {
        return readAheadCacheExpireDuration;
    }

    public void setReadAheadCacheExpireDuration(long duration) {
        this.readAheadCacheExpireDuration = duration;
    }

    public double getReadAheadCacheSizeThresholdRate() {
        return readAheadCacheSizeThresholdRate;
    }

    public void setReadAheadCacheSizeThresholdRate(double rate) {
        this.readAheadCacheSizeThresholdRate = rate;
    }

    public int getTieredStoreMaxPendingLimit() {
        return tieredStoreMaxPendingLimit;
    }

    public void setTieredStoreMaxPendingLimit(int tieredStoreMaxPendingLimit) {
        this.tieredStoreMaxPendingLimit = tieredStoreMaxPendingLimit;
    }

    public boolean isTieredStoreCrcCheckEnable() {
        return tieredStoreCrcCheckEnable;
    }

    public void setTieredStoreCrcCheckEnable(boolean tieredStoreCrcCheckEnable) {
        this.tieredStoreCrcCheckEnable = tieredStoreCrcCheckEnable;
    }

    public String getTieredStoreFilePath() {
        return tieredStoreFilePath;
    }

    public void setTieredStoreFilePath(String tieredStoreFilePath) {
        this.tieredStoreFilePath = tieredStoreFilePath;
    }

    public void setObjectStoreEndpoint(String objectStoreEndpoint) {
        this.objectStoreEndpoint = objectStoreEndpoint;
    }

    public String getObjectStoreBucket() {
        return objectStoreBucket;
    }

    public void setObjectStoreBucket(String objectStoreBucket) {
        this.objectStoreBucket = objectStoreBucket;
    }

    public String getObjectStoreAccessKey() {
        return objectStoreAccessKey;
    }

    public void setObjectStoreAccessKey(String objectStoreAccessKey) {
        this.objectStoreAccessKey = objectStoreAccessKey;
    }

    public String getObjectStoreSecretKey() {
        return objectStoreSecretKey;
    }

    public void setObjectStoreSecretKey(String objectStoreSecretKey) {
        this.objectStoreSecretKey = objectStoreSecretKey;
    }

    public String getObjectStoreEndpoint() {
        return objectStoreEndpoint;
    }
}
