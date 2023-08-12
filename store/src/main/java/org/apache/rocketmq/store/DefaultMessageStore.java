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
package org.apache.rocketmq.store;

import com.google.common.hash.Hashing;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.View;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.DefaultHAService;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.kv.CommitLogDispatcherCompaction;
import org.apache.rocketmq.store.kv.CompactionService;
import org.apache.rocketmq.store.kv.CompactionStore;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStore;
import org.apache.rocketmq.store.service.BatchDispatchRequest;
import org.apache.rocketmq.store.service.CleanCommitLogService;
import org.apache.rocketmq.store.service.CleanConsumeQueueService;
import org.apache.rocketmq.store.service.CommitLogDispatcherBuildConsumeQueue;
import org.apache.rocketmq.store.service.CommitLogDispatcherBuildIndex;
import org.apache.rocketmq.store.service.ConcurrentReputMessageService;
import org.apache.rocketmq.store.service.ConsumeQueueService;
import org.apache.rocketmq.store.service.CorrectLogicOffsetService;
import org.apache.rocketmq.store.service.DelayLevelService;
import org.apache.rocketmq.store.service.DispatchRequestOrderlyQueue;
import org.apache.rocketmq.store.service.FlushConsumeQueueService;
import org.apache.rocketmq.store.service.GetMessageService;
import org.apache.rocketmq.store.service.PutMessageService;
import org.apache.rocketmq.store.service.QueryMessageService;
import org.apache.rocketmq.store.service.ReputMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.PerfCounter;

public class DefaultMessageStore implements MessageStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public final PerfCounter.Ticks perfs = new PerfCounter.Ticks(LOGGER);

    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private CommitLog commitLog;


    private ConsumeQueueStore consumeQueueStore;

    private FlushConsumeQueueService flushConsumeQueueService;

    private CleanCommitLogService cleanCommitLogService;

    private CleanConsumeQueueService cleanConsumeQueueService;

    private CorrectLogicOffsetService correctLogicOffsetService;


    private IndexService indexService;

    private AllocateMappedFileService allocateMappedFileService;


    private ReputMessageService reputMessageService;

    private HAService haService;


    // CompactionLog
    private CompactionStore compactionStore;

    private CompactionService compactionService;

    private StoreStatsService storeStatsService;

    private TransientStorePool transientStorePool;

    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private ScheduledExecutorService scheduledExecutorService;
    private final BrokerStatsManager brokerStatsManager;


    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    private StoreCheckpoint storeCheckpoint;
    private TimerMessageStore timerMessageStore;

    private LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    private volatile int aliveReplicasNum = 1;

    // Refer the MessageStore of MasterBroker in the same process.
    // If current broker is master, this reference point to null or itself.
    // If current broker is slave, this reference point to the store of master broker, and the two stores belong to
    // different broker groups.
    private MessageStore masterStoreInProcess = null;
    private volatile long masterFlushedOffset = -1L;
    private volatile long brokerInitMaxOffset = -1L;

    private SendMessageBackHook sendMessageBackHook;


    private final AtomicInteger mappedPageHoldCount = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<BatchDispatchRequest> batchDispatchRequestQueue = new ConcurrentLinkedQueue<>();
    private final int dispatchRequestOrderlyQueueSize = 16;
    private final DispatchRequestOrderlyQueue dispatchRequestOrderlyQueue = new DispatchRequestOrderlyQueue(dispatchRequestOrderlyQueueSize);

    private long stateMachineVersion = 0L;

    // this is a unmodifiableMap
    private final ConcurrentMap<String, TopicConfig> topicConfigTable;

    private final ScheduledExecutorService scheduledCleanQueueExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreCleanQueueScheduledThread"));

    private PutMessageService putMessageService;
    private QueryMessageService queryMessageService;
    private GetMessageService getMessageService;
    private DelayLevelService delayLevelService;
    private ConsumeQueueService consumeQueueService;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig,
        final ConcurrentMap<String, TopicConfig> topicConfigTable) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.aliveReplicasNum = messageStoreConfig.getTotalReplicas();
        this.brokerStatsManager = brokerStatsManager;
        this.topicConfigTable = topicConfigTable;

        initStoreService();
        initCommitLog();
        initHaService();
        initReputMessageService();
        initDispatchList();
        initScheduledExecutorService();
        initLockFile();

        initProcessService();
    }


    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        this.consumeQueueStore.truncateDirty(phyOffset);
    }

    /**
     * @throws IOException
     */
    @Override
    public boolean load() {
        boolean result = true;
        try {
            boolean lastExitOK = !this.isTempFileExist();
            LOGGER.info("last shutdown {}, store path root dir: {}", lastExitOK ? "normally" : "abnormally", messageStoreConfig.getStorePathRootDir());

            result = this.commitLog.load();
            result = result && this.consumeQueueStore.load();

            if (messageStoreConfig.isEnableCompaction()) {
                result = result && this.compactionService.load(lastExitOK);
            }

            if (result) {
                result = loadIndexService(lastExitOK);
            }

            long maxOffset = this.getMaxPhyOffset();
            this.setBrokerInitMaxOffset(maxOffset);
            LOGGER.info("load over, and the max phy offset = {}", maxOffset);
        } catch (Exception e) {
            LOGGER.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        if (!messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            this.haService.init(this);
        }

        if (this.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();
        this.indexService.start();

        this.prepareLock();

        this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        this.reputMessageService.start();

        // Checking is not necessary, as long as the dLedger's implementation exactly follows the definition of Recover,
        // which is eliminating the dispatch inconsistency between the commitLog and consumeQueue at the end of recovery.
        this.doRecheckReputOffsetFromCq();

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        if (this.haService != null) {
            this.haService.start();
        }

        this.createTempFile();
        this.addScheduleTask();
        this.perfs.start();
        this.shutdown = false;
    }

    @Override
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;
            shutdownServices();
        }

        this.transientStorePool.destroy();
        releaseLockAndLockFile();
    }

    @Override
    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public long getMajorFileSize() {
        long commitLogSize = 0;
        if (this.commitLog != null) {
            commitLogSize = this.commitLog.getTotalSize();
        }

        long consumeQueueSize = 0;
        if (this.consumeQueueStore != null) {
            consumeQueueSize = this.consumeQueueStore.getTotalSize();
        }

        long indexFileSize = 0;
        if (this.indexService != null) {
            indexFileSize = this.indexService.getTotalSize();
        }

        return commitLogSize + consumeQueueSize + indexFileSize;
    }

    @Override
    public void destroyLogics() {
        this.consumeQueueStore.destroy();
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        return putMessageService.asyncPutMessage(msg);
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        return putMessageService.asyncPutMessages(messageExtBatch);
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return putMessageService.putMessage(msg);
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return putMessageService.putMessages(messageExtBatch);
    }

    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
            && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }


    public void truncateDirtyFiles(long offsetToTruncate) {

        LOGGER.info("truncate dirty files to {}", offsetToTruncate);

        if (offsetToTruncate >= this.getMaxPhyOffset()) {
            LOGGER.info("no need to truncate files, truncate offset is {}, max physical offset is {}", offsetToTruncate, this.getMaxPhyOffset());
            return;
        }

        this.reputMessageService.shutdown();

        long oldReputFromOffset = this.reputMessageService.getReputFromOffset();

        // truncate commitLog
        this.commitLog.truncateDirtyFiles(offsetToTruncate);

        // truncate consume queue
        this.truncateDirtyLogicFiles(offsetToTruncate);

        this.recoverTopicQueueTable();

        if (!messageStoreConfig.isEnableBuildConsumeQueueConcurrently()) {
            this.reputMessageService = new ReputMessageService(this);
        } else {
            this.reputMessageService = new ConcurrentReputMessageService(this);
        }

        long resetReputOffset = Math.min(oldReputFromOffset, offsetToTruncate);

        LOGGER.info("oldReputFromOffset is {}, reset reput from offset to {}", oldReputFromOffset, resetReputOffset);

        this.reputMessageService.setReputFromOffset(resetReputOffset);
        this.reputMessageService.start();
    }

    @Override
    public boolean truncateFiles(long offsetToTruncate) {
        if (offsetToTruncate >= this.getMaxPhyOffset()) {
            LOGGER.info("no need to truncate files, truncate offset is {}, max physical offset is {}", offsetToTruncate, this.getMaxPhyOffset());
            return true;
        }

        if (!isOffsetAligned(offsetToTruncate)) {
            LOGGER.error("offset {} is not align, truncate failed, need manual fix", offsetToTruncate);
            return false;
        }
        truncateDirtyFiles(offsetToTruncate);
        return true;
    }

    @Override
    public boolean isOffsetAligned(long offset) {
        SelectMappedBufferResult mappedBufferResult = this.getCommitLogData(offset);

        if (mappedBufferResult == null) {
            return true;
        }

        DispatchRequest dispatchRequest = this.commitLog.checkMessageAndReturnSize(mappedBufferResult.getByteBuffer(), true, false);
        return dispatchRequest.isSuccess();
    }

    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final MessageFilter messageFilter) {
        return getMessageService.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        return getMessageService.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
    }

    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter) {
        return getMessageService.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter);
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
        return getMessageService.getMessageAsync(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter);
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        return consumeQueueService.getMaxOffsetInQueue(topic, queueId);
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) {
        return consumeQueueService.getMaxOffsetInQueue(topic, queueId, committed);
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        return consumeQueueService.getMinOffsetInQueue(topic, queueId);
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        return consumeQueueService.getCommitLogOffsetInQueue(topic, queueId, consumeQueueOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return consumeQueueService.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        return consumeQueueService.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return queryMessageService.lookMessageByOffset(commitLogOffset);
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        return queryMessageService.lookMessageByOffset(commitLogOffset, size);
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return queryMessageService.selectOneMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return queryMessageService.selectOneMessageByOffset(commitLogOffset, msgSize);
    }

    @Override
    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    public String getStorePathPhysic() {
        String storePathPhysic;
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            storePathPhysic = ((DLedgerCommitLog) DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    public String getStorePathLogic() {
        return StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double minPhysicsUsedRatio = Double.MAX_VALUE;
            String commitLogStorePath = getStorePathPhysic();
            String[] paths = commitLogStorePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
            for (String clPath : paths) {
                double physicRatio = UtilAll.isPathExists(clPath) ?
                    UtilAll.getDiskPartitionSpaceUsedPercent(clPath) : -1;
                result.put(RunningStats.commitLogDiskRatio.name() + "_" + clPath, String.valueOf(physicRatio));
                minPhysicsUsedRatio = Math.min(minPhysicsUsedRatio, physicRatio);
            }
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(minPhysicsUsedRatio));
        }

        {
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathLogic());
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }


    @Override
    public boolean getLastMappedFile(long startOffset) {
        return this.commitLog.getLastMappedFile(startOffset);
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return consumeQueueService.getEarliestMessageTime(topic, queueId);
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        return consumeQueueService.getEarliestMessageTimeAsync(topic, queueId);
    }

    @Override
    public long getEarliestMessageTime() {
        return consumeQueueService.getEarliestMessageTime();
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        return consumeQueueService.getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset);
    }

    @Override public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId,
        long consumeQueueOffset) {
        return consumeQueueService.getMessageStoreTimeStampAsync(topic, queueId, consumeQueueOffset);
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        return consumeQueueService.getMessageTotalInQueue(topic, queueId);
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public List<SelectMappedBufferResult> getBulkCommitLogData(final long offset, final int size) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so getBulkCommitLogData is forbidden");
            return null;
        }

        return this.commitLog.getBulkData(offset, size);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so appendToCommitLog is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data, dataStart, dataLength);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            LOGGER.error(
                "DefaultMessageStore#appendToCommitLog: failed to append data to commitLog, physical offset={}, data "
                    + "length={}", startOffset, data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.executeDeleteFilesManually();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return queryMessageService.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key,
        int maxNum, long begin, long end) {
        return queryMessageService.queryMessageAsync(topic, key, maxNum, begin, end);
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haService != null) {
            this.haService.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void updateMasterAddress(String newAddr) {
        if (this.haService != null) {
            this.haService.updateMasterAddress(newAddr);
        }
        if (this.compactionService != null) {
            this.compactionService.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void setAliveReplicaNumInGroup(int aliveReplicaNums) {
        this.aliveReplicasNum = aliveReplicaNums;
    }

    @Override
    public void wakeupHAClient() {
        if (this.haService != null) {
            this.haService.getHAClient().wakeup();
        }
    }

    @Override
    public int getAliveReplicaNumInGroup() {
        return this.aliveReplicasNum;
    }

    @Override
    public long slaveFallBehindMuch() {
        if (this.haService == null || this.messageStoreConfig.isDuplicationEnable() || this.messageStoreConfig.isEnableDLegerCommitLog()) {
            LOGGER.warn("haServer is null or duplication is enable or enableDLegerCommitLog is true");
            return -1;
        } else {
            return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
        }
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }


    @Override
    public int deleteTopics(final Set<String> deleteTopics) {
        return consumeQueueService.deleteTopics(deleteTopics);
    }

    @Override
    public int cleanUnusedTopic(final Set<String> retainTopics) {
        return consumeQueueService.cleanUnusedTopic(retainTopics);
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        consumeQueueService.cleanExpiredConsumerQueue();
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        return consumeQueueService.getMessageIds(topic, queueId, minOffset, maxOffset, storeHost);
    }

    @Override
    @Deprecated
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {
        return false;
    }

    @Override
    public boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize) {
        return consumeQueueService.checkInMemByConsumeOffset(topic, queueId, consumeOffset, batchSize);
    }

    @Override
    public boolean checkInStoreByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return consumeQueueService.checkInStoreByConsumeOffset(topic, queueId, consumeOffset);
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    public long flushBehindBytes() {
        return this.commitLog.remainHowManyDataToCommit() + this.commitLog.remainHowManyDataToFlush();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return consumeQueueService.resetWriteOffset(phyOffset);
    }

    @Override
    public byte[] calcDeltaChecksum(long from, long to) {
        if (from < 0 || to <= from) {
            return new byte[0];
        }

        int size = (int) (to - from);

        if (size > this.messageStoreConfig.getMaxChecksumRange()) {
            LOGGER.error("Checksum range from {}, size {} exceeds threshold {}", from, size, this.messageStoreConfig.getMaxChecksumRange());
            return null;
        }

        List<MessageExt> msgList = new ArrayList<>();
        List<SelectMappedBufferResult> bufferResultList = this.getBulkCommitLogData(from, size);
        if (bufferResultList.isEmpty()) {
            return new byte[0];
        }

        for (SelectMappedBufferResult bufferResult : bufferResultList) {
            msgList.addAll(MessageDecoder.decodesBatch(bufferResult.getByteBuffer(), true, false, false));
            bufferResult.release();
        }

        if (msgList.isEmpty()) {
            return new byte[0];
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        for (MessageExt msg : msgList) {
            try {
                byteBuffer.put(MessageDecoder.encodeUniquely(msg, false));
            } catch (IOException ignore) {
            }
        }

        return Hashing.murmur3_128().hashBytes(byteBuffer.array()).asBytes();
    }

    @Override
    public boolean isMappedFilesEmpty() {
        return this.commitLog.isMappedFilesEmpty();
    }

    @Override
    public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
        return consumeQueueService.findConsumeQueue(topic, queueId);
    }
    /**
     * The ratio val is estimated by the experiment and experience
     * so that the result is not high accurate for different business
     * @return
     */
    public boolean checkInColdAreaByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long)(StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryHotRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    @Override
    public long getTimingMessageCount(String topic) {
        if (null == timerMessageStore) {
            return 0L;
        } else {
            return timerMessageStore.getTimerMetrics().getTimingCount(topic);
        }
    }

    public void recoverTopicQueueTable() {
        long minPhyOffset = this.commitLog.getMinOffset();
        this.consumeQueueStore.recoverOffsetTable(minPhyOffset);
    }

    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        this.consumeQueueStore.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(final ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {
        return this.commitLog.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
    }

    @Override
    public boolean getData(long offset, int size, ByteBuffer byteBuffer) {
        return this.commitLog.getData(offset, size, byteBuffer);
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        return consumeQueueService.getConsumeQueue(topic, queueId);
    }

    @Override
    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    @Override
    public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        // empty
    }

    @Override
    public void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile,
        boolean isRecover, boolean isFileEnd) {
        if (doDispatch && !isFileEnd) {
            this.doDispatch(dispatchRequest);
        }
    }

    @Override
    public void assignOffset(MessageExtBrokerInner msg) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            this.consumeQueueStore.assignQueueOffset(msg);
        }
    }

    @Override
    public void increaseOffset(MessageExtBrokerInner msg, short messageNum) {
        consumeQueueService.increaseOffset(msg, messageNum);
    }

    @Override
    public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
        return consumeQueueService.estimateMessageCount(topic, queueId, from, to, filter);
    }

    @Override
    public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        DefaultStoreMetricsManager.init(meter, attributesBuilderSupplier, this);
    }

    /**
     * Enable transient commitLog store pool only if transientStorePoolEnable is true and broker role is not SLAVE or
     * enableControllerMode is true
     *
     * @return <tt>true</tt> or <tt>false</tt>
     */
    public boolean isTransientStorePoolEnable() {
        return this.messageStoreConfig.isTransientStorePoolEnable() &&
            (this.brokerConfig.isEnableControllerMode() || this.messageStoreConfig.getBrokerRole() != BrokerRole.SLAVE);
    }

    @Override
    public List<Pair<InstrumentSelector, View>> getMetricsView() {
        return DefaultStoreMetricsManager.getMetricsView();
    }

    //**************************************** private or protected methods start ****************************************************
    private void initProcessService() {
        this.delayLevelService = new DelayLevelService(this);
        this.putMessageService = new PutMessageService(this);
        this.queryMessageService = new QueryMessageService(this);
        this.getMessageService = new GetMessageService(this);
        this.consumeQueueService = new ConsumeQueueService(this);
    }

    private void initScheduledExecutorService() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread", getBrokerIdentity()));
    }

    private void initCommitLog() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
    }

    private void initHaService() {
        if (messageStoreConfig.isEnableDLegerCommitLog() || this.messageStoreConfig.isDuplicationEnable()) {
            return;
        }

        if (brokerConfig.isEnableControllerMode()) {
            this.haService = new AutoSwitchHAService();
            LOGGER.warn("Load AutoSwitch HA Service: {}", AutoSwitchHAService.class.getSimpleName());
        } else {
            this.haService = ServiceProvider.loadClass(HAService.class);
            if (null == this.haService) {
                this.haService = new DefaultHAService();
                LOGGER.warn("Load default HA Service: {}", DefaultHAService.class.getSimpleName());
            }
        }
    }

    private void initReputMessageService() {
        if (!messageStoreConfig.isEnableBuildConsumeQueueConcurrently()) {
            this.reputMessageService = new ReputMessageService(this);
        } else {
            this.reputMessageService = new ConcurrentReputMessageService(this);
        }
    }

    private void initDispatchList() {
        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue(this));
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex(this));
        if (!messageStoreConfig.isEnableCompaction()) {
            return;
        }

        this.compactionStore = new CompactionStore(this);
        this.compactionService = new CompactionService(commitLog, this, compactionStore);
        this.dispatcherList.addLast(new CommitLogDispatcherCompaction(compactionService));
    }

    private void initLockFile() throws IOException {
        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        UtilAll.ensureDirOK(file.getParent());
        UtilAll.ensureDirOK(getStorePathPhysic());
        UtilAll.ensureDirOK(getStorePathLogic());
        lockFile = new RandomAccessFile(file, "rw");
    }

    private void initStoreService() {
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        this.consumeQueueStore = new ConsumeQueueStore(this, this.messageStoreConfig);
        this.flushConsumeQueueService = new FlushConsumeQueueService(this);
        this.cleanCommitLogService = new CleanCommitLogService(this);
        this.cleanConsumeQueueService = new CleanConsumeQueueService(this);
        this.correctLogicOffsetService = new CorrectLogicOffsetService(this);
        this.storeStatsService = new StoreStatsService(getBrokerIdentity());
        this.indexService = new IndexService(this);
        this.transientStorePool = new TransientStorePool(this);
    }

    private void doRecheckReputOffsetFromCq() throws InterruptedException {
        if (!messageStoreConfig.isRecheckReputOffsetFromCq()) {
            return;
        }

        /**
         * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
         * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
         * 3. Calculate the reput offset according to the consume queue;
         * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
         */
        long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.getConsumeQueueTable().values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                    maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                }
            }
        }
        // If maxPhyPos(CQs) < minPhyPos(CommitLog), some newly deleted topics may be re-dispatched into cqs mistakenly.
        if (maxPhysicalPosInLogicQueue < 0) {
            maxPhysicalPosInLogicQueue = 0;
        }
        if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
            maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
            /**
             * This happens in following conditions:
             * 1. If someone removes all the consumequeue files or the disk get damaged.
             * 2. Launch a new broker, and copy the commitlog from other brokers.
             *
             * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
             * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
             */
            LOGGER.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
        }
        LOGGER.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
            maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
        this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);

        /**
         *  1. Finish dispatching the messages fall behind, then to start other services.
         *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
         */
        while (true) {
            if (dispatchBehindBytes() <= 0) {
                break;
            }
            Thread.sleep(1000);
            LOGGER.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
        }
        this.recoverTopicQueueTable();
    }

    private void shutdownScheduleServices() {
        this.scheduledExecutorService.shutdown();
        this.scheduledCleanQueueExecutorService.shutdown();

        try {
            this.scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS);
            this.scheduledCleanQueueExecutorService.awaitTermination(3, TimeUnit.SECONDS);
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
            LOGGER.error("shutdown Exception, ", e);
        }
    }

    private void shutdownServices() {
        shutdownScheduleServices();

        if (this.haService != null) {
            this.haService.shutdown();
        }

        this.storeStatsService.shutdown();
        this.commitLog.shutdown();
        this.reputMessageService.shutdown();
        // dispatch-related services must be shut down after reputMessageService
        this.indexService.shutdown();
        if (this.compactionService != null) {
            this.compactionService.shutdown();
        }

        this.flushConsumeQueueService.shutdown();
        this.allocateMappedFileService.shutdown();
        this.storeCheckpoint.flush();
        this.storeCheckpoint.shutdown();

        this.perfs.shutdown();

        if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
            this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
            shutDownNormal = true;
        } else {
            LOGGER.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
        }
    }

    private void releaseLockAndLockFile() {
        try {
            if (null != lock) {
                lock.release();
            }

            if (null != lockFile) {
                lockFile.close();
            }
        } catch (IOException ignored) {
        }
    }

    private boolean loadIndexService(boolean lastExitOK) throws IOException {
        this.storeCheckpoint = new StoreCheckpoint(
            StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
        this.masterFlushedOffset = this.storeCheckpoint.getMasterFlushedOffset();
        setConfirmOffset(this.storeCheckpoint.getConfirmPhyOffset());

        boolean result = this.indexService.load(lastExitOK);
        this.recover(lastExitOK);
        LOGGER.info("message store recover end, and the max phy offset = {}", this.getMaxPhyOffset());

        return result;
    }

    private void prepareLock() throws IOException {
        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes(StandardCharsets.UTF_8)));
        lockFile.getChannel().force(true);
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        LOGGER.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        UtilAll.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        LOGGER.info(fileName + (result ? " create OK" : " already exists"));
        MixAll.string2File(Long.toString(MixAll.getPID()), file.getAbsolutePath());
    }

    private void addScheduleTask() {

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                    + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                DefaultMessageStore.this.storeCheckpoint.flush();
            }
        }, 1, 1, TimeUnit.SECONDS);

        this.scheduledCleanQueueExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanQueueFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);


        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
    }

    private void cleanQueueFilesPeriodically() {
        this.correctLogicOffsetService.run();
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();
        this.consumeQueueStore.checkSelf();
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    private void recover(final boolean lastExitOK) {
        boolean recoverConcurrently = this.brokerConfig.isRecoverConcurrently();
        LOGGER.info("message store recover mode: {}", recoverConcurrently ? "concurrent" : "normal");

        // recover consume queue
        long recoverConsumeQueueStart = System.currentTimeMillis();
        this.recoverConsumeQueue();
        long maxPhyOffsetOfConsumeQueue = this.getMaxOffsetInConsumeQueue();
        long recoverConsumeQueueEnd = System.currentTimeMillis();

        // recover commitlog
        if (lastExitOK) {
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        // recover consume offset table
        long recoverCommitLogEnd = System.currentTimeMillis();
        this.recoverTopicQueueTable();
        long recoverConsumeOffsetEnd = System.currentTimeMillis();

        LOGGER.info("message store recover total cost: {} ms, " +
                "recoverConsumeQueue: {} ms, recoverCommitLog: {} ms, recoverOffsetTable: {} ms",
            recoverConsumeOffsetEnd - recoverConsumeQueueStart, recoverConsumeQueueEnd - recoverConsumeQueueStart,
            recoverCommitLogEnd - recoverConsumeQueueEnd, recoverConsumeOffsetEnd - recoverCommitLogEnd);
    }

    private void recoverConsumeQueue() {
        if (!this.brokerConfig.isRecoverConcurrently()) {
            this.consumeQueueStore.recover();
        } else {
            this.consumeQueueStore.recoverConcurrently();
        }
    }

    //**************************************** getter and setter start ****************************************************
    public DelayLevelService getDelayLevelService() {
        return delayLevelService;
    }

    @Override
    public TimerMessageStore getTimerMessageStore() {
        return this.timerMessageStore;
    }

    @Override
    public void setTimerMessageStore(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getLastFileFromOffset() {
        return this.commitLog.getLastFileFromOffset();
    }

    @Override
    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    @Override
    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    @Override
    public long getFlushedWhere() {
        return this.commitLog.getFlushedWhere();
    }

    // Fetch and compute the newest confirmOffset.
    // Even if it is just inited.
    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    // Fetch the original confirmOffset's value.
    // Without checking and re-computing.
    public long getConfirmOffsetDirectly() {
        return this.commitLog.getConfirmOffsetDirectly();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    @Override
    public void setPhysicalOffset(long phyOffset) {
        this.commitLog.setMappedFileQueueOffset(phyOffset);
    }

    @Override
    public long getMasterFlushedOffset() {
        return this.masterFlushedOffset;
    }

    @Override
    public void setMasterFlushedOffset(long masterFlushedOffset) {
        this.masterFlushedOffset = masterFlushedOffset;
        this.storeCheckpoint.setMasterFlushedOffset(masterFlushedOffset);
    }

    @Override
    public long getBrokerInitMaxOffset() {
        return this.brokerInitMaxOffset;
    }

    @Override
    public void setBrokerInitMaxOffset(long brokerInitMaxOffset) {
        this.brokerInitMaxOffset = brokerInitMaxOffset;
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    @Override
    public CommitLog getCommitLog() {
        return commitLog;
    }

    private long getMaxOffsetInConsumeQueue() {
        return this.consumeQueueStore.getMaxOffsetInConsumeQueue();
    }

    @Override
    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    @Override
    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return consumeQueueStore.getConsumeQueueTable();
    }

    @Override
    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    @Override
    public HAService getHaService() {
        return haService;
    }

    @Override
    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    @Override
    public long getStateMachineVersion() {
        return stateMachineVersion;
    }

    public void setStateMachineVersion(long stateMachineVersion) {
        this.stateMachineVersion = stateMachineVersion;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public long remainHowManyDataToCommit() {
        return this.commitLog.remainHowManyDataToCommit();
    }

    @Override
    public long remainHowManyDataToFlush() {
        return this.commitLog.remainHowManyDataToFlush();
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public void addDispatcher(CommitLogDispatcher dispatcher) {
        this.dispatcherList.add(dispatcher);
    }

    @Override
    public void setMasterStoreInProcess(MessageStore masterStoreInProcess) {
        this.masterStoreInProcess = masterStoreInProcess;
    }

    @Override
    public MessageStore getMasterStoreInProcess() {
        return this.masterStoreInProcess;
    }

    @Override
    public PerfCounter.Ticks getPerfCounter() {
        return perfs;
    }

    @Override
    public ConsumeQueueStore getConsumeQueueStore() {
        return consumeQueueStore;
    }

    @Override
    public boolean isSyncDiskFlush() {
        return FlushDiskType.SYNC_FLUSH == this.getMessageStoreConfig().getFlushDiskType();
    }

    @Override
    public boolean isSyncMaster() {
        return BrokerRole.SYNC_MASTER == this.getMessageStoreConfig().getBrokerRole();
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigs() {
        return this.topicConfigTable;
    }

    public Optional<TopicConfig> getTopicConfig(String topic) {
        if (this.topicConfigTable == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(this.topicConfigTable.get(topic));
    }

    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }

    @Override
    public HARuntimeInfo getHARuntimeInfo() {
        if (haService != null) {
            return this.haService.getRuntimeInfo(this.commitLog.getMaxOffset());
        } else {
            return null;
        }
    }

    public List<PutMessageHook> getPutMessageHookList() {
        return putMessageService.getPutMessageHookList();
    }

    @Override
    public void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook) {
        this.sendMessageBackHook = sendMessageBackHook;
    }

    @Override
    public SendMessageBackHook getSendMessageBackHook() {
        return sendMessageBackHook;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    public long getReputFromOffset() {
        return this.reputMessageService.getReputFromOffset();
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public CompactionStore getCompactionStore() {
        return compactionStore;
    }

    public AtomicInteger getMappedPageHoldCount() {
        return mappedPageHoldCount;
    }

    public MessageArrivingListener getMessageArrivingListener() {
        return messageArrivingListener;
    }

    public ConcurrentLinkedQueue<BatchDispatchRequest> getBatchDispatchRequestQueue() {
        return batchDispatchRequestQueue;
    }

    public DispatchRequestOrderlyQueue getDispatchRequestOrderlyQueue() {
        return dispatchRequestOrderlyQueue;
    }

    public ReputMessageService getReputMessageService() {
        return reputMessageService;
    }
}
