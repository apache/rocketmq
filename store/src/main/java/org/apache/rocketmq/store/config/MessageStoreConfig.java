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
package org.apache.rocketmq.store.config;

import java.io.File;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.queue.BatchConsumeQueue;
import org.rocksdb.CompressionType;
import org.rocksdb.util.SizeUnit;

public class MessageStoreConfig {

    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

    //The root directory in which the log data is kept
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    //The directory in which the commitlog is kept
    @ImportantField
    private String storePathCommitLog = null;

    @ImportantField
    private String storePathDLedgerCommitLog = null;

    //The directory in which the epochFile is kept
    @ImportantField
    private String storePathEpochFile = null;

    @ImportantField
    private String storePathBrokerIdentity = null;

    private String readOnlyCommitLogStorePaths = null;

    // CommitLog file size,default is 1G
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    // CompactionLog file size, default is 100M
    private int compactionMappedFileSize = 100 * 1024 * 1024;

    // CompactionLog consumeQueue file size, default is 10M
    private int compactionCqMappedFileSize = 10 * 1024 * 1024;

    private int compactionScheduleInternal = 15 * 60 * 1000;

    private int maxOffsetMapSize = 100 * 1024 * 1024;

    private int compactionThreadNum = 6;

    private boolean enableCompaction = true;

    // TimerLog file size, default is 100M
    private int mappedFileSizeTimerLog = 100 * 1024 * 1024;

    private int timerPrecisionMs = 1000;

    private int timerRollWindowSlot = 3600 * 24 * 2;
    private int timerFlushIntervalMs = 1000;
    private int timerGetMessageThreadNum = 3;
    private int timerPutMessageThreadNum = 3;

    private boolean timerEnableDisruptor = false;

    private boolean timerEnableCheckMetrics = true;
    private boolean timerInterceptDelayLevel = false;
    private int timerMaxDelaySec = 3600 * 24 * 3;
    private boolean timerWheelEnable = true;

    /**
     * 1. Register to broker after (startTime + disappearTimeAfterStart)
     * 2. Internal msg exchange will start after (startTime + disappearTimeAfterStart)
     * A. PopReviveService
     * B. TimerDequeueGetService
     */
    @ImportantField
    private int disappearTimeAfterStart = -1;

    private boolean timerStopEnqueue = false;

    private String timerCheckMetricsWhen = "05";

    private boolean timerSkipUnknownError = false;
    private boolean timerWarmEnable = false;
    private boolean timerStopDequeue = false;
    private boolean timerEnableRetryUntilSuccess = false;
    private int timerCongestNumEachSlot = Integer.MAX_VALUE;

    private int timerMetricSmallThreshold = 1000000;
    private int timerProgressLogIntervalMs = 10 * 1000;

    // default, defaultRocksDB
    @ImportantField
    private String storeType = StoreType.DEFAULT.getStoreType();

    // ConsumeQueue file size,default is 30W
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    // enable consume queue ext
    private boolean enableConsumeQueueExt = false;
    // ConsumeQueue extend file size, 48M
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;
    private int mapperFileSizeBatchConsumeQueue = 300000 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;
    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    private int bitMapLengthConsumeQueueExt = 64;

    // CommitLog flush interval
    // flush data to disk
    @ImportantField
    private int flushIntervalCommitLog = 500;

    // Only used if TransientStorePool enabled
    // flush data to FileChannel
    @ImportantField
    private int commitIntervalCommitLog = 200;

    private int maxRecoveryCommitlogFiles = 30;

    private int diskSpaceWarningLevelRatio = 90;

    private int diskSpaceCleanForciblyRatio = 85;

    /**
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
     */
    private boolean useReentrantLockWhenPutMessage = true;

    // Whether schedule flush
    @ImportantField
    private boolean flushCommitLogTimed = true;
    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;
    // Resource reclaim interval
    private int cleanResourceInterval = 10000;
    // CommitLog removal interval
    private int deleteCommitLogFilesInterval = 100;
    // ConsumeQueue removal interval
    private int deleteConsumeQueueFilesInterval = 100;
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    private int redeleteHangedFileInterval = 1000 * 120;
    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";
    private int diskMaxUsedSpaceRatio = 75;
    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;
    @ImportantField
    private int deleteFileBatchMax = 10;
    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;
    // The maximum size of message body,default is 4M,4M only for body length,not include others.
    private int maxMessageSize = 1024 * 1024 * 4;

    // The maximum size of message body can be  set in config;count with maxMsgNums * CQ_STORE_UNIT_SIZE(20 || 46)
    private int maxFilterMessageSize = 16000;
    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    private boolean checkCRCOnRecover = true;
    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;
    // How many pages are to be committed when commit data to file
    private int commitCommitLogLeastPages = 4;
    // Flush page size when the disk in warming state
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
    // How many pages are to be flushed when flush ConsumeQueue
    private int flushConsumeQueueLeastPages = 2;
    private int flushCommitLogThoroughInterval = 1000 * 10;
    private int commitCommitLogThoroughInterval = 200;
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    @ImportantField
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 64;
    @ImportantField
    private boolean messageIndexSafe = false;
    private int haListenPort = 10912;
    private int haSendHeartbeatInterval = 1000 * 5;
    private int haHousekeepingInterval = 1000 * 20;
    /**
     * Maximum size of data to transfer to slave.
     * NOTE: cannot be larger than HAClient.READ_MAX_BUFFER_SIZE
     */
    private int haTransferBatchSize = 1024 * 32;
    @ImportantField
    private String haMasterAddress = null;
    private int haMaxGapNotInSync = 1024 * 1024 * 256;
    @ImportantField
    private volatile BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    // Used by GroupTransferService to sync messages from master to slave
    private int syncFlushTimeout = 1000 * 5;
    // Used by PutMessage to wait messages be flushed to disk and synchronized in current broker member group.
    private int putMessageTimeout = 1000 * 8;
    private int slaveTimeout = 3000;
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    private long flushDelayOffsetInterval = 1000 * 10;
    @ImportantField
    private boolean cleanFileForciblyEnable = true;
    private boolean warmMapedFileEnable = false;
    private boolean offsetCheckInSlave = false;
    private boolean debugLockEnable = false;
    private boolean duplicationEnable = false;
    private boolean diskFallRecorded = true;
    private long osPageCacheBusyTimeOutMills = 1000;
    private int defaultQueryMaxNum = 32;

    @ImportantField
    private boolean transientStorePoolEnable = false;
    private int transientStorePoolSize = 5;
    private boolean fastFailIfNoBufferInStorePool = false;

    /**
     * When true, use RandomAccessFile for writing instead of MappedByteBuffer. This can be useful for certain scenarios
     * where mmap is not desired.
     *
     * The configurations writeWithoutMmap and transientStorePoolEnable are mutually exclusive. When both are set to
     * true, only transientStorePoolEnable will be effective.
     */
    @ImportantField
    private boolean writeWithoutMmap = false;

    // DLedger message store config
    private boolean enableDLegerCommitLog = false;
    private String dLegerGroup;
    private String dLegerPeers;
    private String dLegerSelfId;
    private String preferredLeaderId;
    private boolean enableBatchPush = false;

    private boolean enableScheduleMessageStats = true;

    private boolean enableLmq = false;
    private boolean enableMultiDispatch = false;
    private int maxLmqConsumeQueueNum = 20000;

    private boolean enableScheduleAsyncDeliver = false;
    private int scheduleAsyncDeliverMaxPendingLimit = 2000;
    private int scheduleAsyncDeliverMaxResendNum2Blocked = 3;

    private int maxBatchDeleteFilesNum = 50;
    //Polish dispatch
    private int dispatchCqThreads = 10;
    private int dispatchCqCacheNum = 1024 * 4;
    private boolean enableAsyncReput = true;
    //For recheck the reput
    private boolean recheckReputOffsetFromCq = false;

    // Maximum length of topic, it will be removed in the future release
    @Deprecated
    private int maxTopicLength = Byte.MAX_VALUE;

    /**
     * Use MessageVersion.MESSAGE_VERSION_V2 automatically if topic length larger than Bytes.MAX_VALUE.
     * Otherwise, store use MESSAGE_VERSION_V1. Note: Client couldn't decode MESSAGE_VERSION_V2 version message.
     * Enable this config to resolve this issue. https://github.com/apache/rocketmq/issues/5568
     */
    private boolean autoMessageVersionOnTopicLen = true;

    /**
     * It cannot be changed after the broker is started.
     * Modifications need to be restarted to take effect.
     */
    private boolean enabledAppendPropCRC = false;
    private boolean forceVerifyPropCRC = false;
    private int travelCqFileNumWhenGetMessage = 1;
    // Sleep interval between to corrections
    private int correctLogicMinOffsetSleepInterval = 1;
    // Force correct min offset interval
    private int correctLogicMinOffsetForceInterval = 5 * 60 * 1000;
    // swap
    private boolean mappedFileSwapEnable = true;
    private long commitLogForceSwapMapInterval = 12L * 60 * 60 * 1000;
    private long commitLogSwapMapInterval = 1L * 60 * 60 * 1000;
    private int commitLogSwapMapReserveFileNum = 100;
    private long logicQueueForceSwapMapInterval = 12L * 60 * 60 * 1000;
    private long logicQueueSwapMapInterval = 1L * 60 * 60 * 1000;
    private long cleanSwapedMapInterval = 5L * 60 * 1000;
    private int logicQueueSwapMapReserveFileNum = 20;

    private boolean searchBcqByCacheEnable = true;

    @ImportantField
    private boolean dispatchFromSenderThread = false;

    @ImportantField
    private boolean wakeCommitWhenPutMessage = true;
    @ImportantField
    private boolean wakeFlushWhenPutMessage = false;

    @ImportantField
    private boolean enableCleanExpiredOffset = false;

    private int maxAsyncPutMessageRequests = 5000;

    private int pullBatchMaxMessageCount = 160;

    @ImportantField
    private int totalReplicas = 1;

    /**
     * Each message must be written successfully to at least in-sync replicas.
     * The master broker is considered one of the in-sync replicas, and it's included in the count of total.
     * If a master broker is ASYNC_MASTER, inSyncReplicas will be ignored.
     * If enableControllerMode is true and ackAckInSyncStateSet is true, inSyncReplicas will be ignored.
     */
    @ImportantField
    private int inSyncReplicas = 1;

    /**
     * Will be worked in auto multiple replicas mode, to provide minimum in-sync replicas.
     * It is still valid in controller mode.
     */
    @ImportantField
    private int minInSyncReplicas = 1;

    /**
     * Each message must be written successfully to all replicas in SyncStateSet.
     */
    @ImportantField
    private boolean allAckInSyncStateSet = false;

    /**
     * Dynamically adjust in-sync replicas to provide higher availability, the real time in-sync replicas
     * will smaller than inSyncReplicas config.
     */
    @ImportantField
    private boolean enableAutoInSyncReplicas = false;

    /**
     * Enable or not ha flow control
     */
    @ImportantField
    private boolean haFlowControlEnable = false;

    /**
     * The max speed for one slave when transfer data in ha
     */
    private long maxHaTransferByteInSecond = 100 * 1024 * 1024;

    /**
     * The max gap time that slave doesn't catch up to master.
     */
    private long haMaxTimeSlaveNotCatchup = 1000 * 15;

    /**
     * Sync flush offset from master when broker startup, used in upgrading from old version broker.
     */
    private boolean syncMasterFlushOffsetWhenStartup = false;

    /**
     * Max checksum range.
     */
    private long maxChecksumRange = 1024 * 1024 * 1024;

    private int replicasPerDiskPartition = 1;

    private double logicalDiskSpaceCleanForciblyThreshold = 0.8;

    private long maxSlaveResendLength = 256 * 1024 * 1024;

    /**
     * Whether sync from lastFile when a new broker replicas(no data) join the master.
     */
    private boolean syncFromLastFile = false;

    private boolean asyncLearner = false;

    /**
     * Number of records to scan before starting to estimate.
     */
    private int maxConsumeQueueScan = 20_000;

    /**
     * Number of matched records before starting to estimate.
     */
    private int sampleCountThreshold = 5000;

    private boolean coldDataFlowControlEnable = false;
    private boolean coldDataScanEnable = false;
    private boolean dataReadAheadEnable = true;
    private int timerColdDataCheckIntervalMs = 60 * 1000;
    private int sampleSteps = 32;
    private int accessMessageInMemoryHotRatio = 26;
    /**
     * Build ConsumeQueue concurrently with multi-thread
     */
    private boolean enableBuildConsumeQueueConcurrently = false;

    private int batchDispatchRequestThreadPoolNums = 16;

    // rocksdb mode
    private long cleanRocksDBDirtyCQIntervalMin = 60;
    private long statRocksDBCQIntervalSec = 10;
    private long memTableFlushIntervalMs = 60 * 60 * 1000L;
    private boolean realTimePersistRocksDBConfig = true;
    private boolean enableRocksDBLog = false;

    private int topicQueueLockNum = 32;

    /**
     * If readUnCommitted is true, the dispatch of the consume queue will exceed the confirmOffset, which may cause the client to read uncommitted messages.
     * For example, reput offset exceeding the flush offset during synchronous disk flushing.
     */
    private boolean readUnCommitted = false;

    private boolean putConsumeQueueDataByFileChannel = true;

    private boolean rocksdbCQDoubleWriteEnable = false;

    /**
     * CombineConsumeQueueStore
     * combineCQLoadingCQTypes is used to configure the loading types of CQ. load / recover / start order: [default -> defaultRocksDB]
     * combineCQPreferCQType is used to configure the preferred CQ type when reading. Make sure the CQ type is included in combineCQLoadingCQTypes
     * combineAssignOffsetCQType is used to configure the CQ type when assign offset. Make sure the CQ type is included in combineCQLoadingCQTypes
     */
    private String combineCQLoadingCQTypes = StoreType.DEFAULT.getStoreType() + ";" + StoreType.DEFAULT_ROCKSDB.getStoreType();
    private String combineCQPreferCQType = StoreType.DEFAULT.getStoreType();
    private String combineAssignOffsetCQType = StoreType.DEFAULT.getStoreType();
    private boolean combineCQEnableCheckSelf = false;
    private int combineCQMaxExtraSearchCommitLogFiles = 3;

    /**
     * If ConsumeQueueStore is RocksDB based, this option is to configure bottom-most tier compression type.
     * The following values are valid:
     * <ul>
     *     <li>snappy</li>
     *     <li>z</li>
     *     <li>bzip2</li>
     *     <li>lz4</li>
     *     <li>lz4hc</li>
     *     <li>xpress</li>
     *     <li>zstd</li>
     * </ul>
     *
     * LZ4 is the recommended one.
     */
    private String bottomMostCompressionTypeForConsumeQueueStore = CompressionType.ZSTD_COMPRESSION.getLibraryName();

    private String rocksdbCompressionType = CompressionType.LZ4_COMPRESSION.getLibraryName();

    /**
     * Flush RocksDB WAL frequency, aka, flush WAL every N write ops.
     */
    private int rocksdbFlushWalFrequency = 1024;

    private long rocksdbWalFileRollingThreshold = SizeUnit.GB;

    /**
     * Note: For correctness, this switch should be enabled only if the previous startup was configured with SYNC_FLUSH
     * and the storeType was defaultRocksDB. This switch is not recommended for normal use cases (include master-slave
     * or controller mode).
     */
    private boolean enableAcceleratedRecovery = false;

    public String getRocksdbCompressionType() {
        return rocksdbCompressionType;
    }

    public void setRocksdbCompressionType(String compressionType) {
        this.rocksdbCompressionType = compressionType;
    }

    /**
     * Spin number in the retreat strategy of spin lock
     * Default is 1000
     */
    private int spinLockCollisionRetreatOptimalDegree = 1000;

    /**
     * Use AdaptiveBackOffLock
     **/
    private boolean useABSLock = false;

    private boolean enableLogConsumeQueueRepeatedlyBuildWhenRecover = false;

    public boolean isRocksdbCQDoubleWriteEnable() {
        return rocksdbCQDoubleWriteEnable;
    }

    public void setRocksdbCQDoubleWriteEnable(boolean rocksdbWriteEnable) {
        this.rocksdbCQDoubleWriteEnable = rocksdbWriteEnable;
    }


    public boolean isEnabledAppendPropCRC() {
        return enabledAppendPropCRC;
    }

    public void setEnabledAppendPropCRC(boolean enabledAppendPropCRC) {
        this.enabledAppendPropCRC = enabledAppendPropCRC;
    }

    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(final boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(final boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(final boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }

    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }

    public int getCompactionMappedFileSize() {
        return compactionMappedFileSize;
    }

    public int getCompactionCqMappedFileSize() {
        return compactionCqMappedFileSize;
    }

    public void setCompactionMappedFileSize(int compactionMappedFileSize) {
        this.compactionMappedFileSize = compactionMappedFileSize;
    }

    public void setCompactionCqMappedFileSize(int compactionCqMappedFileSize) {
        this.compactionCqMappedFileSize = compactionCqMappedFileSize;
    }

    public int getCompactionScheduleInternal() {
        return compactionScheduleInternal;
    }

    public void setCompactionScheduleInternal(int compactionScheduleInternal) {
        this.compactionScheduleInternal = compactionScheduleInternal;
    }

    public int getMaxOffsetMapSize() {
        return maxOffsetMapSize;
    }

    public void setMaxOffsetMapSize(int maxOffsetMapSize) {
        this.maxOffsetMapSize = maxOffsetMapSize;
    }

    public int getCompactionThreadNum() {
        return compactionThreadNum;
    }

    public void setCompactionThreadNum(int compactionThreadNum) {
        this.compactionThreadNum = compactionThreadNum;
    }

    public boolean isEnableCompaction() {
        return enableCompaction;
    }

    public void setEnableCompaction(boolean enableCompaction) {
        this.enableCompaction = enableCompaction;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.storeType);
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public int getMappedFileSizeConsumeQueue() {
        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
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

    public int getMaxFilterMessageSize() {
        return maxFilterMessageSize;
    }

    public void setMaxFilterMessageSize(int maxFilterMessageSize) {
        this.maxFilterMessageSize = maxFilterMessageSize;
    }

    @Deprecated
    public int getMaxTopicLength() {
        return maxTopicLength;
    }

    @Deprecated
    public void setMaxTopicLength(int maxTopicLength) {
        this.maxTopicLength = maxTopicLength;
    }

    public boolean isAutoMessageVersionOnTopicLen() {
        return autoMessageVersionOnTopicLen;
    }

    public void setAutoMessageVersionOnTopicLen(boolean autoMessageVersionOnTopicLen) {
        this.autoMessageVersionOnTopicLen = autoMessageVersionOnTopicLen;
    }

    public int getTravelCqFileNumWhenGetMessage() {
        return travelCqFileNumWhenGetMessage;
    }

    public void setTravelCqFileNumWhenGetMessage(int travelCqFileNumWhenGetMessage) {
        this.travelCqFileNumWhenGetMessage = travelCqFileNumWhenGetMessage;
    }

    public int getCorrectLogicMinOffsetSleepInterval() {
        return correctLogicMinOffsetSleepInterval;
    }

    public void setCorrectLogicMinOffsetSleepInterval(int correctLogicMinOffsetSleepInterval) {
        this.correctLogicMinOffsetSleepInterval = correctLogicMinOffsetSleepInterval;
    }

    public int getCorrectLogicMinOffsetForceInterval() {
        return correctLogicMinOffsetForceInterval;
    }

    public void setCorrectLogicMinOffsetForceInterval(int correctLogicMinOffsetForceInterval) {
        this.correctLogicMinOffsetForceInterval = correctLogicMinOffsetForceInterval;
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

    public boolean isForceVerifyPropCRC() {
        return forceVerifyPropCRC;
    }

    public void setForceVerifyPropCRC(boolean forceVerifyPropCRC) {
        this.forceVerifyPropCRC = forceVerifyPropCRC;
    }

    public String getStorePathCommitLog() {
        if (storePathCommitLog == null) {
            return storePathRootDir + File.separator + "commitlog";
        }
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getStorePathDLedgerCommitLog() {
        return storePathDLedgerCommitLog;
    }

    public void setStorePathDLedgerCommitLog(String storePathDLedgerCommitLog) {
        this.storePathDLedgerCommitLog = storePathDLedgerCommitLog;
    }

    public String getStorePathEpochFile() {
        if (storePathEpochFile == null) {
            return storePathRootDir + File.separator + "epochFileCheckpoint";
        }
        return storePathEpochFile;
    }

    public void setStorePathEpochFile(String storePathEpochFile) {
        this.storePathEpochFile = storePathEpochFile;
    }

    public String getStorePathBrokerIdentity() {
        if (storePathBrokerIdentity == null) {
            return storePathRootDir + File.separator + "brokerIdentity";
        }
        return storePathBrokerIdentity;
    }

    public void setStorePathBrokerIdentity(String storePathBrokerIdentity) {
        this.storePathBrokerIdentity = storePathBrokerIdentity;
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
        if (haListenPort < 0) {
            this.haListenPort = 0;
            return;
        }
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

    public int getHaMaxGapNotInSync() {
        return haMaxGapNotInSync;
    }

    public void setHaMaxGapNotInSync(int haMaxGapNotInSync) {
        this.haMaxGapNotInSync = haMaxGapNotInSync;
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

    public int getPutMessageTimeout() {
        return putMessageTimeout;
    }

    public void setPutMessageTimeout(int putMessageTimeout) {
        this.putMessageTimeout = putMessageTimeout;
    }

    public int getSlaveTimeout() {
        return slaveTimeout;
    }

    public void setSlaveTimeout(int slaveTimeout) {
        this.slaveTimeout = slaveTimeout;
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

    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }

    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable;
    }

    public void setTransientStorePoolEnable(final boolean transientStorePoolEnable) {
        this.transientStorePoolEnable = transientStorePoolEnable;
    }

    public boolean isWriteWithoutMmap() {
        return writeWithoutMmap;
    }

    public void setWriteWithoutMmap(final boolean writeWithoutMmap) {
        this.writeWithoutMmap = writeWithoutMmap;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(final int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public int getCommitIntervalCommitLog() {
        return commitIntervalCommitLog;
    }

    public void setCommitIntervalCommitLog(final int commitIntervalCommitLog) {
        this.commitIntervalCommitLog = commitIntervalCommitLog;
    }

    public boolean isFastFailIfNoBufferInStorePool() {
        return fastFailIfNoBufferInStorePool;
    }

    public void setFastFailIfNoBufferInStorePool(final boolean fastFailIfNoBufferInStorePool) {
        this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(final boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public int getCommitCommitLogLeastPages() {
        return commitCommitLogLeastPages;
    }

    public void setCommitCommitLogLeastPages(final int commitCommitLogLeastPages) {
        this.commitCommitLogLeastPages = commitCommitLogLeastPages;
    }

    public int getCommitCommitLogThoroughInterval() {
        return commitCommitLogThoroughInterval;
    }

    public void setCommitCommitLogThoroughInterval(final int commitCommitLogThoroughInterval) {
        this.commitCommitLogThoroughInterval = commitCommitLogThoroughInterval;
    }

    public boolean isWakeCommitWhenPutMessage() {
        return wakeCommitWhenPutMessage;
    }

    public void setWakeCommitWhenPutMessage(boolean wakeCommitWhenPutMessage) {
        this.wakeCommitWhenPutMessage = wakeCommitWhenPutMessage;
    }

    public boolean isWakeFlushWhenPutMessage() {
        return wakeFlushWhenPutMessage;
    }

    public void setWakeFlushWhenPutMessage(boolean wakeFlushWhenPutMessage) {
        this.wakeFlushWhenPutMessage = wakeFlushWhenPutMessage;
    }

    public int getMapperFileSizeBatchConsumeQueue() {
        return mapperFileSizeBatchConsumeQueue;
    }

    public void setMapperFileSizeBatchConsumeQueue(int mapperFileSizeBatchConsumeQueue) {
        this.mapperFileSizeBatchConsumeQueue = mapperFileSizeBatchConsumeQueue;
    }

    public boolean isEnableCleanExpiredOffset() {
        return enableCleanExpiredOffset;
    }

    public void setEnableCleanExpiredOffset(boolean enableCleanExpiredOffset) {
        this.enableCleanExpiredOffset = enableCleanExpiredOffset;
    }

    public String getReadOnlyCommitLogStorePaths() {
        return readOnlyCommitLogStorePaths;
    }

    public void setReadOnlyCommitLogStorePaths(String readOnlyCommitLogStorePaths) {
        this.readOnlyCommitLogStorePaths = readOnlyCommitLogStorePaths;
    }

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public boolean isEnableDLegerCommitLog() {
        return enableDLegerCommitLog;
    }

    public void setEnableDLegerCommitLog(boolean enableDLegerCommitLog) {
        this.enableDLegerCommitLog = enableDLegerCommitLog;
    }

    public String getPreferredLeaderId() {
        return preferredLeaderId;
    }

    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderId = preferredLeaderId;
    }

    public boolean isEnableBatchPush() {
        return enableBatchPush;
    }

    public void setEnableBatchPush(boolean enableBatchPush) {
        this.enableBatchPush = enableBatchPush;
    }

    public boolean isEnableScheduleMessageStats() {
        return enableScheduleMessageStats;
    }

    public void setEnableScheduleMessageStats(boolean enableScheduleMessageStats) {
        this.enableScheduleMessageStats = enableScheduleMessageStats;
    }

    public int getMaxAsyncPutMessageRequests() {
        return maxAsyncPutMessageRequests;
    }

    public void setMaxAsyncPutMessageRequests(int maxAsyncPutMessageRequests) {
        this.maxAsyncPutMessageRequests = maxAsyncPutMessageRequests;
    }

    public int getMaxRecoveryCommitlogFiles() {
        return maxRecoveryCommitlogFiles;
    }

    public void setMaxRecoveryCommitlogFiles(final int maxRecoveryCommitlogFiles) {
        this.maxRecoveryCommitlogFiles = maxRecoveryCommitlogFiles;
    }

    public boolean isDispatchFromSenderThread() {
        return dispatchFromSenderThread;
    }

    public void setDispatchFromSenderThread(boolean dispatchFromSenderThread) {
        this.dispatchFromSenderThread = dispatchFromSenderThread;
    }

    public int getDispatchCqThreads() {
        return dispatchCqThreads;
    }

    public void setDispatchCqThreads(final int dispatchCqThreads) {
        this.dispatchCqThreads = dispatchCqThreads;
    }

    public int getDispatchCqCacheNum() {
        return dispatchCqCacheNum;
    }

    public void setDispatchCqCacheNum(final int dispatchCqCacheNum) {
        this.dispatchCqCacheNum = dispatchCqCacheNum;
    }

    public boolean isEnableAsyncReput() {
        return enableAsyncReput;
    }

    public void setEnableAsyncReput(final boolean enableAsyncReput) {
        this.enableAsyncReput = enableAsyncReput;
    }

    public boolean isRecheckReputOffsetFromCq() {
        return recheckReputOffsetFromCq;
    }

    public void setRecheckReputOffsetFromCq(final boolean recheckReputOffsetFromCq) {
        this.recheckReputOffsetFromCq = recheckReputOffsetFromCq;
    }

    public long getCommitLogForceSwapMapInterval() {
        return commitLogForceSwapMapInterval;
    }

    public void setCommitLogForceSwapMapInterval(long commitLogForceSwapMapInterval) {
        this.commitLogForceSwapMapInterval = commitLogForceSwapMapInterval;
    }

    public int getCommitLogSwapMapReserveFileNum() {
        return commitLogSwapMapReserveFileNum;
    }

    public void setCommitLogSwapMapReserveFileNum(int commitLogSwapMapReserveFileNum) {
        this.commitLogSwapMapReserveFileNum = commitLogSwapMapReserveFileNum;
    }

    public long getLogicQueueForceSwapMapInterval() {
        return logicQueueForceSwapMapInterval;
    }

    public void setLogicQueueForceSwapMapInterval(long logicQueueForceSwapMapInterval) {
        this.logicQueueForceSwapMapInterval = logicQueueForceSwapMapInterval;
    }

    public int getLogicQueueSwapMapReserveFileNum() {
        return logicQueueSwapMapReserveFileNum;
    }

    public void setLogicQueueSwapMapReserveFileNum(int logicQueueSwapMapReserveFileNum) {
        this.logicQueueSwapMapReserveFileNum = logicQueueSwapMapReserveFileNum;
    }

    public long getCleanSwapedMapInterval() {
        return cleanSwapedMapInterval;
    }

    public void setCleanSwapedMapInterval(long cleanSwapedMapInterval) {
        this.cleanSwapedMapInterval = cleanSwapedMapInterval;
    }

    public long getCommitLogSwapMapInterval() {
        return commitLogSwapMapInterval;
    }

    public void setCommitLogSwapMapInterval(long commitLogSwapMapInterval) {
        this.commitLogSwapMapInterval = commitLogSwapMapInterval;
    }

    public long getLogicQueueSwapMapInterval() {
        return logicQueueSwapMapInterval;
    }

    public void setLogicQueueSwapMapInterval(long logicQueueSwapMapInterval) {
        this.logicQueueSwapMapInterval = logicQueueSwapMapInterval;
    }

    public int getMaxBatchDeleteFilesNum() {
        return maxBatchDeleteFilesNum;
    }

    public void setMaxBatchDeleteFilesNum(int maxBatchDeleteFilesNum) {
        this.maxBatchDeleteFilesNum = maxBatchDeleteFilesNum;
    }

    public boolean isSearchBcqByCacheEnable() {
        return searchBcqByCacheEnable;
    }

    public void setSearchBcqByCacheEnable(boolean searchBcqByCacheEnable) {
        this.searchBcqByCacheEnable = searchBcqByCacheEnable;
    }

    public int getDiskSpaceWarningLevelRatio() {
        return diskSpaceWarningLevelRatio;
    }

    public void setDiskSpaceWarningLevelRatio(int diskSpaceWarningLevelRatio) {
        this.diskSpaceWarningLevelRatio = diskSpaceWarningLevelRatio;
    }

    public int getDiskSpaceCleanForciblyRatio() {
        return diskSpaceCleanForciblyRatio;
    }

    public void setDiskSpaceCleanForciblyRatio(int diskSpaceCleanForciblyRatio) {
        this.diskSpaceCleanForciblyRatio = diskSpaceCleanForciblyRatio;
    }

    public boolean isMappedFileSwapEnable() {
        return mappedFileSwapEnable;
    }

    public void setMappedFileSwapEnable(boolean mappedFileSwapEnable) {
        this.mappedFileSwapEnable = mappedFileSwapEnable;
    }

    public int getPullBatchMaxMessageCount() {
        return pullBatchMaxMessageCount;
    }

    public void setPullBatchMaxMessageCount(int pullBatchMaxMessageCount) {
        this.pullBatchMaxMessageCount = pullBatchMaxMessageCount;
    }

    public int getDeleteFileBatchMax() {
        return deleteFileBatchMax;
    }

    public void setDeleteFileBatchMax(int deleteFileBatchMax) {
        this.deleteFileBatchMax = deleteFileBatchMax;
    }

    public int getTotalReplicas() {
        return totalReplicas;
    }

    public void setTotalReplicas(int totalReplicas) {
        this.totalReplicas = totalReplicas;
    }

    public int getInSyncReplicas() {
        return inSyncReplicas;
    }

    public void setInSyncReplicas(int inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public int getMinInSyncReplicas() {
        return minInSyncReplicas;
    }

    public void setMinInSyncReplicas(int minInSyncReplicas) {
        this.minInSyncReplicas = minInSyncReplicas;
    }

    public boolean isAllAckInSyncStateSet() {
        return allAckInSyncStateSet;
    }

    public void setAllAckInSyncStateSet(boolean allAckInSyncStateSet) {
        this.allAckInSyncStateSet = allAckInSyncStateSet;
    }

    public boolean isEnableAutoInSyncReplicas() {
        return enableAutoInSyncReplicas;
    }

    public void setEnableAutoInSyncReplicas(boolean enableAutoInSyncReplicas) {
        this.enableAutoInSyncReplicas = enableAutoInSyncReplicas;
    }

    public boolean isHaFlowControlEnable() {
        return haFlowControlEnable;
    }

    public void setHaFlowControlEnable(boolean haFlowControlEnable) {
        this.haFlowControlEnable = haFlowControlEnable;
    }

    public long getMaxHaTransferByteInSecond() {
        return maxHaTransferByteInSecond;
    }

    public void setMaxHaTransferByteInSecond(long maxHaTransferByteInSecond) {
        this.maxHaTransferByteInSecond = maxHaTransferByteInSecond;
    }

    public long getHaMaxTimeSlaveNotCatchup() {
        return haMaxTimeSlaveNotCatchup;
    }

    public void setHaMaxTimeSlaveNotCatchup(long haMaxTimeSlaveNotCatchup) {
        this.haMaxTimeSlaveNotCatchup = haMaxTimeSlaveNotCatchup;
    }

    public boolean isSyncMasterFlushOffsetWhenStartup() {
        return syncMasterFlushOffsetWhenStartup;
    }

    public void setSyncMasterFlushOffsetWhenStartup(boolean syncMasterFlushOffsetWhenStartup) {
        this.syncMasterFlushOffsetWhenStartup = syncMasterFlushOffsetWhenStartup;
    }

    public long getMaxChecksumRange() {
        return maxChecksumRange;
    }

    public void setMaxChecksumRange(long maxChecksumRange) {
        this.maxChecksumRange = maxChecksumRange;
    }

    public int getReplicasPerDiskPartition() {
        return replicasPerDiskPartition;
    }

    public void setReplicasPerDiskPartition(int replicasPerDiskPartition) {
        this.replicasPerDiskPartition = replicasPerDiskPartition;
    }

    public double getLogicalDiskSpaceCleanForciblyThreshold() {
        return logicalDiskSpaceCleanForciblyThreshold;
    }

    public void setLogicalDiskSpaceCleanForciblyThreshold(double logicalDiskSpaceCleanForciblyThreshold) {
        this.logicalDiskSpaceCleanForciblyThreshold = logicalDiskSpaceCleanForciblyThreshold;
    }

    public int getDisappearTimeAfterStart() {
        return disappearTimeAfterStart;
    }

    public void setDisappearTimeAfterStart(int disappearTimeAfterStart) {
        this.disappearTimeAfterStart = disappearTimeAfterStart;
    }

    public long getMaxSlaveResendLength() {
        return maxSlaveResendLength;
    }

    public void setMaxSlaveResendLength(long maxSlaveResendLength) {
        this.maxSlaveResendLength = maxSlaveResendLength;
    }

    public boolean isSyncFromLastFile() {
        return syncFromLastFile;
    }

    public void setSyncFromLastFile(boolean syncFromLastFile) {
        this.syncFromLastFile = syncFromLastFile;
    }

    public boolean isEnableLmq() {
        return enableLmq;
    }

    public void setEnableLmq(boolean enableLmq) {
        this.enableLmq = enableLmq;
    }

    public boolean isEnableMultiDispatch() {
        return enableMultiDispatch;
    }

    public void setEnableMultiDispatch(boolean enableMultiDispatch) {
        this.enableMultiDispatch = enableMultiDispatch;
    }

    public int getMaxLmqConsumeQueueNum() {
        return maxLmqConsumeQueueNum;
    }

    public void setMaxLmqConsumeQueueNum(int maxLmqConsumeQueueNum) {
        this.maxLmqConsumeQueueNum = maxLmqConsumeQueueNum;
    }

    public boolean isEnableScheduleAsyncDeliver() {
        return enableScheduleAsyncDeliver;
    }

    public void setEnableScheduleAsyncDeliver(boolean enableScheduleAsyncDeliver) {
        this.enableScheduleAsyncDeliver = enableScheduleAsyncDeliver;
    }

    public int getScheduleAsyncDeliverMaxPendingLimit() {
        return scheduleAsyncDeliverMaxPendingLimit;
    }

    public void setScheduleAsyncDeliverMaxPendingLimit(int scheduleAsyncDeliverMaxPendingLimit) {
        this.scheduleAsyncDeliverMaxPendingLimit = scheduleAsyncDeliverMaxPendingLimit;
    }

    public int getScheduleAsyncDeliverMaxResendNum2Blocked() {
        return scheduleAsyncDeliverMaxResendNum2Blocked;
    }

    public void setScheduleAsyncDeliverMaxResendNum2Blocked(int scheduleAsyncDeliverMaxResendNum2Blocked) {
        this.scheduleAsyncDeliverMaxResendNum2Blocked = scheduleAsyncDeliverMaxResendNum2Blocked;
    }

    public boolean isAsyncLearner() {
        return asyncLearner;
    }

    public void setAsyncLearner(boolean asyncLearner) {
        this.asyncLearner = asyncLearner;
    }

    public int getMappedFileSizeTimerLog() {
        return mappedFileSizeTimerLog;
    }

    public void setMappedFileSizeTimerLog(final int mappedFileSizeTimerLog) {
        this.mappedFileSizeTimerLog = mappedFileSizeTimerLog;
    }

    public int getTimerPrecisionMs() {
        return timerPrecisionMs;
    }

    public void setTimerPrecisionMs(int timerPrecisionMs) {
        int[] candidates = {100, 200, 500, 1000};
        for (int i = 1; i < candidates.length; i++) {
            if (timerPrecisionMs < candidates[i]) {
                this.timerPrecisionMs = candidates[i - 1];
                return;
            }
        }
        this.timerPrecisionMs = candidates[candidates.length - 1];
    }

    public int getTimerRollWindowSlot() {
        return timerRollWindowSlot;
    }

    public int getTimerGetMessageThreadNum() {
        return timerGetMessageThreadNum;
    }

    public void setTimerGetMessageThreadNum(int timerGetMessageThreadNum) {
        this.timerGetMessageThreadNum = timerGetMessageThreadNum;
    }

    public int getTimerPutMessageThreadNum() {
        return timerPutMessageThreadNum;
    }

    public void setTimerPutMessageThreadNum(int timerPutMessageThreadNum) {
        this.timerPutMessageThreadNum = timerPutMessageThreadNum;
    }

    public boolean isTimerEnableDisruptor() {
        return timerEnableDisruptor;
    }

    public boolean isTimerEnableCheckMetrics() {
        return timerEnableCheckMetrics;
    }

    public void setTimerEnableCheckMetrics(boolean timerEnableCheckMetrics) {
        this.timerEnableCheckMetrics = timerEnableCheckMetrics;
    }

    public boolean isTimerStopEnqueue() {
        return timerStopEnqueue;
    }

    public void setTimerStopEnqueue(boolean timerStopEnqueue) {
        this.timerStopEnqueue = timerStopEnqueue;
    }

    public String getTimerCheckMetricsWhen() {
        return timerCheckMetricsWhen;
    }

    public boolean isTimerSkipUnknownError() {
        return timerSkipUnknownError;
    }

    public void setTimerSkipUnknownError(boolean timerSkipUnknownError) {
        this.timerSkipUnknownError = timerSkipUnknownError;
    }

    public boolean isTimerEnableRetryUntilSuccess() {
        return timerEnableRetryUntilSuccess;
    }

    public void setTimerEnableRetryUntilSuccess(boolean timerEnableRetryUntilSuccess) {
        this.timerEnableRetryUntilSuccess = timerEnableRetryUntilSuccess;
    }

    public boolean isTimerWarmEnable() {
        return timerWarmEnable;
    }

    public boolean isTimerWheelEnable() {
        return timerWheelEnable;
    }

    public void setTimerWheelEnable(boolean timerWheelEnable) {
        this.timerWheelEnable = timerWheelEnable;
    }

    public boolean isTimerStopDequeue() {
        return timerStopDequeue;
    }

    public int getTimerMetricSmallThreshold() {
        return timerMetricSmallThreshold;
    }

    public void setTimerMetricSmallThreshold(int timerMetricSmallThreshold) {
        this.timerMetricSmallThreshold = timerMetricSmallThreshold;
    }

    public int getTimerCongestNumEachSlot() {
        return timerCongestNumEachSlot;
    }

    public void setTimerCongestNumEachSlot(int timerCongestNumEachSlot) {
        // In order to get this value from messageStoreConfig properties file created before v4.4.1.
        this.timerCongestNumEachSlot = timerCongestNumEachSlot;
    }

    public int getTimerFlushIntervalMs() {
        return timerFlushIntervalMs;
    }

    public void setTimerFlushIntervalMs(final int timerFlushIntervalMs) {
        this.timerFlushIntervalMs = timerFlushIntervalMs;
    }

    public void setTimerRollWindowSlot(final int timerRollWindowSlot) {
        this.timerRollWindowSlot = timerRollWindowSlot;
    }

    public int getTimerProgressLogIntervalMs() {
        return timerProgressLogIntervalMs;
    }

    public void setTimerProgressLogIntervalMs(final int timerProgressLogIntervalMs) {
        this.timerProgressLogIntervalMs = timerProgressLogIntervalMs;
    }

    public boolean isTimerInterceptDelayLevel() {
        return timerInterceptDelayLevel;
    }

    public void setTimerInterceptDelayLevel(boolean timerInterceptDelayLevel) {
        this.timerInterceptDelayLevel = timerInterceptDelayLevel;
    }

    public int getTimerMaxDelaySec() {
        return timerMaxDelaySec;
    }

    public void setTimerMaxDelaySec(final int timerMaxDelaySec) {
        this.timerMaxDelaySec = timerMaxDelaySec;
    }

    public int getMaxConsumeQueueScan() {
        return maxConsumeQueueScan;
    }

    public void setMaxConsumeQueueScan(int maxConsumeQueueScan) {
        this.maxConsumeQueueScan = maxConsumeQueueScan;
    }

    public int getSampleCountThreshold() {
        return sampleCountThreshold;
    }

    public void setSampleCountThreshold(int sampleCountThreshold) {
        this.sampleCountThreshold = sampleCountThreshold;
    }

    public boolean isColdDataFlowControlEnable() {
        return coldDataFlowControlEnable;
    }

    public void setColdDataFlowControlEnable(boolean coldDataFlowControlEnable) {
        this.coldDataFlowControlEnable = coldDataFlowControlEnable;
    }

    public boolean isColdDataScanEnable() {
        return coldDataScanEnable;
    }

    public void setColdDataScanEnable(boolean coldDataScanEnable) {
        this.coldDataScanEnable = coldDataScanEnable;
    }

    public int getTimerColdDataCheckIntervalMs() {
        return timerColdDataCheckIntervalMs;
    }

    public void setTimerColdDataCheckIntervalMs(int timerColdDataCheckIntervalMs) {
        this.timerColdDataCheckIntervalMs = timerColdDataCheckIntervalMs;
    }

    public int getSampleSteps() {
        return sampleSteps;
    }

    public void setSampleSteps(int sampleSteps) {
        this.sampleSteps = sampleSteps;
    }

    public int getAccessMessageInMemoryHotRatio() {
        return accessMessageInMemoryHotRatio;
    }

    public void setAccessMessageInMemoryHotRatio(int accessMessageInMemoryHotRatio) {
        this.accessMessageInMemoryHotRatio = accessMessageInMemoryHotRatio;
    }

    public boolean isDataReadAheadEnable() {
        return dataReadAheadEnable;
    }

    public void setDataReadAheadEnable(boolean dataReadAheadEnable) {
        this.dataReadAheadEnable = dataReadAheadEnable;
    }

    public boolean isEnableBuildConsumeQueueConcurrently() {
        return enableBuildConsumeQueueConcurrently;
    }

    public void setEnableBuildConsumeQueueConcurrently(boolean enableBuildConsumeQueueConcurrently) {
        this.enableBuildConsumeQueueConcurrently = enableBuildConsumeQueueConcurrently;
    }

    public int getBatchDispatchRequestThreadPoolNums() {
        return batchDispatchRequestThreadPoolNums;
    }

    public void setBatchDispatchRequestThreadPoolNums(int batchDispatchRequestThreadPoolNums) {
        this.batchDispatchRequestThreadPoolNums = batchDispatchRequestThreadPoolNums;
    }

    public boolean isRealTimePersistRocksDBConfig() {
        return realTimePersistRocksDBConfig;
    }

    public void setRealTimePersistRocksDBConfig(boolean realTimePersistRocksDBConfig) {
        this.realTimePersistRocksDBConfig = realTimePersistRocksDBConfig;
    }

    public long getStatRocksDBCQIntervalSec() {
        return statRocksDBCQIntervalSec;
    }

    public void setStatRocksDBCQIntervalSec(long statRocksDBCQIntervalSec) {
        this.statRocksDBCQIntervalSec = statRocksDBCQIntervalSec;
    }

    public long getCleanRocksDBDirtyCQIntervalMin() {
        return cleanRocksDBDirtyCQIntervalMin;
    }

    public void setCleanRocksDBDirtyCQIntervalMin(long cleanRocksDBDirtyCQIntervalMin) {
        this.cleanRocksDBDirtyCQIntervalMin = cleanRocksDBDirtyCQIntervalMin;
    }

    public long getMemTableFlushIntervalMs() {
        return memTableFlushIntervalMs;
    }

    public void setMemTableFlushIntervalMs(long memTableFlushIntervalMs) {
        this.memTableFlushIntervalMs = memTableFlushIntervalMs;
    }

    public boolean isEnableRocksDBLog() {
        return enableRocksDBLog;
    }

    public void setEnableRocksDBLog(boolean enableRocksDBLog) {
        this.enableRocksDBLog = enableRocksDBLog;
    }

    public int getTopicQueueLockNum() {
        return topicQueueLockNum;
    }

    public void setTopicQueueLockNum(int topicQueueLockNum) {
        this.topicQueueLockNum = topicQueueLockNum;
    }

    public boolean isReadUnCommitted() {
        return readUnCommitted;
    }

    public void setReadUnCommitted(boolean readUnCommitted) {
        this.readUnCommitted = readUnCommitted;
    }

    public boolean isPutConsumeQueueDataByFileChannel() {
        return putConsumeQueueDataByFileChannel;
    }

    public void setPutConsumeQueueDataByFileChannel(boolean putConsumeQueueDataByFileChannel) {
        this.putConsumeQueueDataByFileChannel = putConsumeQueueDataByFileChannel;
    }

    public String getBottomMostCompressionTypeForConsumeQueueStore() {
        return bottomMostCompressionTypeForConsumeQueueStore;
    }

    public void setBottomMostCompressionTypeForConsumeQueueStore(String bottomMostCompressionTypeForConsumeQueueStore) {
        this.bottomMostCompressionTypeForConsumeQueueStore = bottomMostCompressionTypeForConsumeQueueStore;
    }

    public int getRocksdbFlushWalFrequency() {
        return rocksdbFlushWalFrequency;
    }

    public void setRocksdbFlushWalFrequency(int rocksdbFlushWalFrequency) {
        this.rocksdbFlushWalFrequency = rocksdbFlushWalFrequency;
    }

    public long getRocksdbWalFileRollingThreshold() {
        return rocksdbWalFileRollingThreshold;
    }

    public void setRocksdbWalFileRollingThreshold(long rocksdbWalFileRollingThreshold) {
        this.rocksdbWalFileRollingThreshold = rocksdbWalFileRollingThreshold;
    }

    public int getSpinLockCollisionRetreatOptimalDegree() {
        return spinLockCollisionRetreatOptimalDegree;
    }

    public void setSpinLockCollisionRetreatOptimalDegree(int spinLockCollisionRetreatOptimalDegree) {
        this.spinLockCollisionRetreatOptimalDegree = spinLockCollisionRetreatOptimalDegree;
    }

    public void setUseABSLock(boolean useABSLock) {
        this.useABSLock = useABSLock;
    }

    public boolean getUseABSLock() {
        return useABSLock;
    }

    public String getCombineCQPreferCQType() {
        return combineCQPreferCQType;
    }

    public void setCombineCQPreferCQType(String combineCQPreferCQType) {
        this.combineCQPreferCQType = combineCQPreferCQType;
    }

    public String getCombineCQLoadingCQTypes() {
        return combineCQLoadingCQTypes;
    }

    public void setCombineCQLoadingCQTypes(String combineCQLoadingCQTypes) {
        this.combineCQLoadingCQTypes = combineCQLoadingCQTypes;
    }

    public String getCombineAssignOffsetCQType() {
        return combineAssignOffsetCQType;
    }

    public void setCombineAssignOffsetCQType(String combineAssignOffsetCQType) {
        this.combineAssignOffsetCQType = combineAssignOffsetCQType;
    }

    public boolean isCombineCQEnableCheckSelf() {
        return combineCQEnableCheckSelf;
    }

    public void setCombineCQEnableCheckSelf(boolean combineCQEnableCheckSelf) {
        this.combineCQEnableCheckSelf = combineCQEnableCheckSelf;
    }

    public int getCombineCQMaxExtraSearchCommitLogFiles() {
        return combineCQMaxExtraSearchCommitLogFiles;
    }

    public void setCombineCQMaxExtraSearchCommitLogFiles(int combineCQMaxExtraSearchCommitLogFiles) {
        this.combineCQMaxExtraSearchCommitLogFiles = combineCQMaxExtraSearchCommitLogFiles;
    }

    public boolean isEnableLogConsumeQueueRepeatedlyBuildWhenRecover() {
        return enableLogConsumeQueueRepeatedlyBuildWhenRecover;
    }

    public void setEnableLogConsumeQueueRepeatedlyBuildWhenRecover(
        boolean enableLogConsumeQueueRepeatedlyBuildWhenRecover) {
        this.enableLogConsumeQueueRepeatedlyBuildWhenRecover = enableLogConsumeQueueRepeatedlyBuildWhenRecover;
    }

    public boolean isEnableAcceleratedRecovery() {
        return enableAcceleratedRecovery;
    }

    public void setEnableAcceleratedRecovery(boolean enableAcceleratedRecovery) {
        this.enableAcceleratedRecovery = enableAcceleratedRecovery;
    }
}
