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

import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.ha.DefaultHAService;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.kv.CommitLogDispatcherCompaction;
import org.apache.rocketmq.store.kv.CompactionService;
import org.apache.rocketmq.store.kv.CompactionStore;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStore;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.PerfCounter;
import org.rocksdb.RocksDBException;

public class DefaultMessageStore implements MessageStore {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    public final PerfCounter.Ticks perfs = new PerfCounter.Ticks(LOGGER);

    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    protected final CommitLog commitLog;

    protected final ConsumeQueueStoreInterface consumeQueueStore;

    private final FlushConsumeQueueService flushConsumeQueueService;

    protected final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final CorrectLogicOffsetService correctLogicOffsetService;

    protected final IndexService indexService;

    private final AllocateMappedFileService allocateMappedFileService;

    private ReputMessageService reputMessageService;

    private HAService haService;

    // CompactionLog
    private CompactionStore compactionStore;

    private CompactionService compactionService;

    private final StoreStatsService storeStatsService;

    private final TransientStorePool transientStorePool;

    protected final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;
    protected boolean notifyMessageArriveInBatch = false;

    protected StoreCheckpoint storeCheckpoint;
    private TimerMessageStore timerMessageStore;

    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RocksDBMessageStore rocksDBMessageStore;

    private final RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;
    // Max pull msg size
    private final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

    private volatile int aliveReplicasNum = 1;

    // Refer the MessageStore of MasterBroker in the same process.
    // If current broker is master, this reference point to null or itself.
    // If current broker is slave, this reference point to the store of master broker, and the two stores belong to
    // different broker groups.
    private MessageStore masterStoreInProcess = null;

    private volatile long masterFlushedOffset = -1L;

    private volatile long brokerInitMaxOffset = -1L;

    private final List<PutMessageHook> putMessageHookList = new ArrayList<>();

    private SendMessageBackHook sendMessageBackHook;

    private final ConcurrentSkipListMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentSkipListMap<>();

    private int maxDelayLevel;

    private final AtomicInteger mappedPageHoldCount = new AtomicInteger(0);

    private final ConcurrentLinkedQueue<BatchDispatchRequest> batchDispatchRequestQueue = new ConcurrentLinkedQueue<>();

    private final int dispatchRequestOrderlyQueueSize = 16;

    private final DispatchRequestOrderlyQueue dispatchRequestOrderlyQueue = new DispatchRequestOrderlyQueue(dispatchRequestOrderlyQueueSize);

    private long stateMachineVersion = 0L;

    // this is a unmodifiableMap
    private final ConcurrentMap<String, TopicConfig> topicConfigTable;

    private final ScheduledExecutorService scheduledCleanQueueExecutorService =
        ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreCleanQueueScheduledThread"));

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig,
        final ConcurrentMap<String, TopicConfig> topicConfigTable) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.aliveReplicasNum = messageStoreConfig.getTotalReplicas();
        this.brokerStatsManager = brokerStatsManager;
        this.topicConfigTable = topicConfigTable;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }

        this.consumeQueueStore = createConsumeQueueStore();

        this.flushConsumeQueueService = createFlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = createCleanConsumeQueueService();
        this.correctLogicOffsetService = createCorrectLogicOffsetService();
        this.storeStatsService = new StoreStatsService(getBrokerIdentity());
        this.indexService = new IndexService(this);

        if (!messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
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

        if (!messageStoreConfig.isEnableBuildConsumeQueueConcurrently()) {
            this.reputMessageService = new ReputMessageService();
        } else {
            this.reputMessageService = new ConcurrentReputMessageService();
        }

        this.transientStorePool = new TransientStorePool(messageStoreConfig.getTransientStorePoolSize(), messageStoreConfig.getMappedFileSizeCommitLog());

        this.scheduledExecutorService =
            ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread", getBrokerIdentity()));

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());
        if (messageStoreConfig.isEnableCompaction()) {
            this.compactionStore = new CompactionStore(this);
            this.compactionService = new CompactionService(commitLog, this, compactionStore);
            this.dispatcherList.addLast(new CommitLogDispatcherCompaction(compactionService));
        }

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        UtilAll.ensureDirOK(file.getParent());
        UtilAll.ensureDirOK(getStorePathPhysic());
        UtilAll.ensureDirOK(getStorePathLogic());
        lockFile = new RandomAccessFile(file, "rw");

        parseDelayLevel();
    }

    public ConsumeQueueStoreInterface createConsumeQueueStore() {
        return new ConsumeQueueStore(this);
    }

    public CleanConsumeQueueService createCleanConsumeQueueService() {
        return new CleanConsumeQueueService();
    }

    public FlushConsumeQueueService createFlushConsumeQueueService() {
        return new FlushConsumeQueueService();
    }

    public CorrectLogicOffsetService createCorrectLogicOffsetService() {
        return new CorrectLogicOffsetService();
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = messageStoreConfig.getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            LOGGER.error("parse message delay level failed. messageDelayLevel = {}", levelString, e);
            return false;
        }

        return true;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) throws RocksDBException {
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
            LOGGER.info("last shutdown {}, store path root dir: {}",
                lastExitOK ? "normally" : "abnormally", messageStoreConfig.getStorePathRootDir());

            // load Commit Log
            result = this.commitLog.load();

            // load Consume Queue
            result = result && this.consumeQueueStore.load();

            if (messageStoreConfig.isEnableCompaction()) {
                result = result && this.compactionService.load(lastExitOK);
            }

            if (result) {
                loadCheckPoint();
                result = this.indexService.load(lastExitOK);
                this.recover(lastExitOK);
                LOGGER.info("message store recover end, and the max phy offset = {}", this.getMaxPhyOffset());
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

    public void loadCheckPoint() throws IOException {
        this.storeCheckpoint =
            new StoreCheckpoint(
                StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
        this.masterFlushedOffset = this.storeCheckpoint.getMasterFlushedOffset();
        setConfirmOffset(this.storeCheckpoint.getConfirmPhyOffset());
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

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes(StandardCharsets.UTF_8)));
        lockFile.getChannel().force(true);

        this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        this.reputMessageService.start();

        // Checking is not necessary, as long as the dLedger's implementation exactly follows the definition of Recover,
        // which is eliminating the dispatch inconsistency between the commitLog and consumeQueue at the end of recovery.
        this.doRecheckReputOffsetFromCq();

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.consumeQueueStore.start();
        this.storeStatsService.start();

        if (this.haService != null) {
            this.haService.start();
        }

        this.createTempFile();
        this.addScheduleTask();
        this.perfs.start();
        this.shutdown = false;
    }

    private void doRecheckReputOffsetFromCq() throws InterruptedException {
        if (!messageStoreConfig.isRecheckReputOffsetFromCq()) {
            return;
        }

        /*
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
            /*
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

    @Override
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            this.scheduledCleanQueueExecutorService.shutdown();

            try {
                this.scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS);
                this.scheduledCleanQueueExecutorService.awaitTermination(3, TimeUnit.SECONDS);
                Thread.sleep(1000 * 3);
            } catch (InterruptedException e) {
                LOGGER.error("shutdown Exception, ", e);
            }

            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.consumeQueueStore.shutdown();
            // dispatch-related services must be shut down after reputMessageService
            this.indexService.shutdown();
            if (this.compactionService != null) {
                this.compactionService.shutdown();
            }
            if (this.rocksDBMessageStore != null && this.rocksDBMessageStore.consumeQueueStore != null) {
                this.rocksDBMessageStore.consumeQueueStore.shutdown();
            }
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.shutdown();

            this.perfs.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                LOGGER.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    public void destroy() {
        this.consumeQueueStore.destroy();
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
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {

        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(msg);
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }

        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
            && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            LOGGER.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            Optional<TopicConfig> topicConfig = this.getTopicConfig(msg.getTopic());
            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
                LOGGER.error("[BUG]The message is an inner batch but cq type is not batch cq");
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
            }
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        putResultFuture.thenAccept(result -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                LOGGER.warn("DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms, topic={}, bodyLength={}",
                    elapsedTime, msg.getTopic(), msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {

        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(messageExtBatch);
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        putResultFuture.thenAccept(result -> {
            long eclipseTime = this.getSystemClock().now() - beginTime;
            if (eclipseTime > 500) {
                LOGGER.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return waitForPutResult(asyncPutMessage(msg));
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return waitForPutResult(asyncPutMessages(messageExtBatch));
    }

    private PutMessageResult waitForPutResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                Math.max(this.messageStoreConfig.getSyncFlushTimeout(),
                    this.messageStoreConfig.getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            LOGGER.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
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

    public void truncateDirtyFiles(long offsetToTruncate) throws RocksDBException {

        LOGGER.info("truncate dirty files to {}", offsetToTruncate);

        if (offsetToTruncate >= this.getMaxPhyOffset()) {
            LOGGER.info("no need to truncate files, truncate offset is {}, max physical offset is {}", offsetToTruncate, this.getMaxPhyOffset());
            return;
        }

        this.reputMessageService.shutdown();

        long oldReputFromOffset = this.reputMessageService.getReputFromOffset();

        // truncate consume queue
        this.truncateDirtyLogicFiles(offsetToTruncate);

        // truncate commitLog
        this.commitLog.truncateDirtyFiles(offsetToTruncate);

        this.recoverTopicQueueTable();

        if (!messageStoreConfig.isEnableBuildConsumeQueueConcurrently()) {
            this.reputMessageService = new ReputMessageService();
        } else {
            this.reputMessageService = new ConcurrentReputMessageService();
        }

        long resetReputOffset = Math.min(oldReputFromOffset, offsetToTruncate);

        LOGGER.info("oldReputFromOffset is {}, reset reput from offset to {}", oldReputFromOffset, resetReputOffset);

        this.reputMessageService.setReputFromOffset(resetReputOffset);
        this.reputMessageService.start();
    }

    @Override
    public boolean truncateFiles(long offsetToTruncate) throws RocksDBException {
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
        return getMessage(group, topic, queueId, offset, maxMsgNums, MAX_PULL_MSG_SIZE, messageFilter);
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        return CompletableFuture.completedFuture(getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter));
    }

    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            LOGGER.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        Optional<TopicConfig> topicConfig = getTopicConfig(topic);
        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(topicConfig);
        //check request topic flag
        if (Objects.equals(policy, CleanupPolicy.COMPACTION) && messageStoreConfig.isEnableCompaction()) {
            return compactionStore.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
        } // else skip

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
            } else {
                final int maxFilterMessageSize = Math.max(this.messageStoreConfig.getMaxFilterMessageSize(), maxMsgNums * consumeQueue.getUnitSize());
                final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

                long maxPullSize = Math.max(maxTotalMsgSize, 100);
                if (maxPullSize > MAX_PULL_MSG_SIZE) {
                    LOGGER.warn("The max pull size is too large maxPullSize={} topic={} queueId={}", maxPullSize, topic, queueId);
                    maxPullSize = MAX_PULL_MSG_SIZE;
                }
                status = GetMessageStatus.NO_MATCHED_MESSAGE;
                long maxPhyOffsetPulling = 0;
                int cqFileNum = 0;

                while (getResult.getBufferTotalSize() <= 0
                    && nextBeginOffset < maxOffset
                    && cqFileNum++ < this.messageStoreConfig.getTravelCqFileNumWhenGetMessage()) {
                    ReferredIterator<CqUnit> bufferConsumeQueue = null;

                    try {
                        bufferConsumeQueue = consumeQueue.iterateFrom(nextBeginOffset, maxMsgNums);

                        if (bufferConsumeQueue == null) {
                            status = GetMessageStatus.OFFSET_FOUND_NULL;
                            nextBeginOffset = nextOffsetCorrection(nextBeginOffset, this.consumeQueueStore.rollNextFile(consumeQueue, nextBeginOffset));
                            LOGGER.warn("consumer request topic: " + topic + ", offset: " + offset + ", minOffset: " + minOffset + ", maxOffset: "
                                + maxOffset + ", but access logic queue failed. Correct nextBeginOffset to " + nextBeginOffset);
                            break;
                        }

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        while (bufferConsumeQueue.hasNext()
                            && nextBeginOffset < maxOffset) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            int sizePy = cqUnit.getSize();

                            boolean isInMem = estimateInMemByCommitOffset(offsetPy, maxOffsetPy);

                            if ((cqUnit.getQueueOffset() - offset) * consumeQueue.getUnitSize() > maxFilterMessageSize) {
                                break;
                            }

                            if (this.isTheBatchFull(sizePy, cqUnit.getBatchNum(), maxMsgNums, maxPullSize, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInMem)) {
                                break;
                            }

                            if (getResult.getBufferTotalSize() >= maxPullSize) {
                                break;
                            }

                            maxPhyOffsetPulling = offsetPy;

                            //Be careful, here should before the isTheBatchFull
                            nextBeginOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset) {
                                    continue;
                                }
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByConsumeQueue(cqUnit.getValidTagsCodeAsLong(), cqUnit.getCqExtUnit())) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            if (messageStoreConfig.isColdDataFlowControlEnable() && !MixAll.isSysConsumerGroupForNoColdReadLimit(group) && !selectResult.isInCache()) {
                                getResult.setColdDataSum(getResult.getColdDataSum() + sizePy);
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }
                            this.storeStatsService.getGetMessageTransferredMsgCount().add(cqUnit.getBatchNum());
                            getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }
                    } catch (RocksDBException e) {
                        ERROR_LOG.error("getMessage Failed. cid: {}, topic: {}, queueId: {}, offset: {}, minOffset: {}, maxOffset: {}, {}",
                            group, topic, queueId, offset, minOffset, maxOffset, e.getMessage());
                    } finally {
                        if (bufferConsumeQueue != null) {
                            bufferConsumeQueue.release();
                        }
                    }
                }

                if (diskFallRecorded) {
                    long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                    brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                }

                long diff = maxOffsetPy - maxPhyOffsetPulling;
                long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                    * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                getResult.setSuggestPullingFromSlave(diff > memory);
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().add(1);
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().add(1);
        }
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        // lazy init no data found.
        if (getResult == null) {
            getResult = new GetMessageResult(0);
        }

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
        return CompletableFuture.completedFuture(getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter));
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) throws ConsumeQueueException {
        return getMaxOffsetInQueue(topic, queueId, true);
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) throws ConsumeQueueException {
        if (committed) {
            ConsumeQueueInterface logic = this.getConsumeQueue(topic, queueId);
            if (logic != null) {
                return logic.getMaxOffsetInQueue();
            }
        } else {
            Long offset = this.consumeQueueStore.getMaxOffset(topic, queueId);
            if (offset != null) {
                return offset;
            }
        }

        return 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        try {
            return this.consumeQueueStore.getMinOffsetInQueue(topic, queueId);
        } catch (RocksDBException e) {
            ERROR_LOG.error("getMinOffsetInQueue Failed. topic: {}, queueId: {}", topic, queueId, e);
            return -1;
        }
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
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            CqUnit cqUnit = consumeQueue.get(consumeQueueOffset);
            if (cqUnit != null) {
                return cqUnit.getPos();
            }
        }
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return this.getOffsetInQueueByTime(topic, queueId, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        try {
            return this.consumeQueueStore.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
        } catch (RocksDBException e) {
            ERROR_LOG.error("getOffsetInQueueByTime Failed. topic: {}, queueId: {}, timestamp: {} boundaryType: {}, {}",
                topic, queueId, timestamp, boundaryType, e.getMessage());
        }
        return 0;
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
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

    public MessageArrivingListener getMessageArrivingListener() {
        return messageArrivingListener;
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
    public boolean getLastMappedFile(long startOffset) {
        return this.commitLog.getLastMappedFile(startOffset);
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.getConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            Pair<CqUnit, Long> pair = logicQueue.getEarliestUnitAndStoreTime();
            if (pair != null && pair.getObject2() != null) {
                return pair.getObject2();
            }
        }

        return -1;
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        return CompletableFuture.completedFuture(getEarliestMessageTime(topic, queueId));
    }

    @Override
    public long getEarliestMessageTime() {
        long minPhyOffset = this.getMinPhyOffset();
        if (this.getCommitLog() instanceof DLedgerCommitLog) {
            minPhyOffset += DLedgerEntry.BODY_OFFSET;
        }
        int size = MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION + 8;
        InetAddressValidator validator = InetAddressValidator.getInstance();
        if (validator.isValidInet6Address(this.brokerConfig.getBrokerIP1())) {
            size = MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION + 20;
        }
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface logicQueue = this.getConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            Pair<CqUnit, Long> pair = logicQueue.getCqUnitAndStoreTime(consumeQueueOffset);
            if (pair != null && pair.getObject2() != null) {
                return pair.getObject2();
            }
        }
        return -1;
    }

    @Override
    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId,
        long consumeQueueOffset) {
        return CompletableFuture.completedFuture(getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset));
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.getConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return 0;
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
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

                    SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                    if (result != null) {
                        int size = result.getByteBuffer().getInt(0);
                        result.getByteBuffer().limit(size);
                        result.setSize(size);
                        queryMessageResult.addMessage(result);
                    }
                } catch (Exception e) {
                    LOGGER.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key,
        int maxNum, long begin, long end) {
        return CompletableFuture.completedFuture(queryMessage(topic, key, maxNum, begin, end));
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

    /**
     * Lazy clean queue offset table.
     * If offset table is cleaned, and old messages are dispatching after the old consume queue is cleaned,
     * consume queue will be created with old offset, then later message with new offset table can not be
     * dispatched to consume queue.
     */
    @Override
    public int deleteTopics(final Set<String> deleteTopics) {
        if (deleteTopics == null || deleteTopics.isEmpty()) {
            return 0;
        }

        int deleteCount = 0;
        for (String topic : deleteTopics) {
            ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = this.consumeQueueStore.findConsumeQueueMap(topic);

            if (queueTable == null || queueTable.isEmpty()) {
                continue;
            }

            for (ConsumeQueueInterface cq : queueTable.values()) {
                try {
                    this.consumeQueueStore.destroy(cq);
                } catch (RocksDBException e) {
                    LOGGER.error("DeleteTopic: ConsumeQueue cleans error!, topic={}, queueId={}", cq.getTopic(), cq.getQueueId(), e);
                }
                LOGGER.info("DeleteTopic: ConsumeQueue has been cleaned, topic={}, queueId={}", cq.getTopic(), cq.getQueueId());
                this.consumeQueueStore.removeTopicQueueTable(cq.getTopic(), cq.getQueueId());
            }

            // remove topic from cq table
            this.consumeQueueStore.getConsumeQueueTable().remove(topic);

            if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                this.brokerStatsManager.onTopicDeleted(topic);
            }

            // destroy consume queue dir
            String consumeQueueDir = StorePathConfigHelper.getStorePathConsumeQueue(
                this.messageStoreConfig.getStorePathRootDir()) + File.separator + topic;
            String consumeQueueExtDir = StorePathConfigHelper.getStorePathConsumeQueueExt(
                this.messageStoreConfig.getStorePathRootDir()) + File.separator + topic;
            String batchConsumeQueueDir = StorePathConfigHelper.getStorePathBatchConsumeQueue(
                this.messageStoreConfig.getStorePathRootDir()) + File.separator + topic;

            UtilAll.deleteEmptyDirectory(new File(consumeQueueDir));
            UtilAll.deleteEmptyDirectory(new File(consumeQueueExtDir));
            UtilAll.deleteEmptyDirectory(new File(batchConsumeQueueDir));

            LOGGER.info("DeleteTopic: Topic has been destroyed, topic={}", topic);
            deleteCount++;
        }
        return deleteCount;
    }

    @Override
    public int cleanUnusedTopic(final Set<String> retainTopics) {
        Set<String> consumeQueueTopicSet = this.getConsumeQueueTable().keySet();
        int deleteCount = 0;
        for (String topicName : Sets.difference(consumeQueueTopicSet, retainTopics)) {
            if (retainTopics.contains(topicName) ||
                TopicValidator.isSystemTopic(topicName) ||
                MixAll.isLmq(topicName)) {
                continue;
            }
            deleteCount += this.deleteTopics(Sets.newHashSet(topicName));
        }
        return deleteCount;
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        this.consumeQueueStore.cleanExpired(minCommitLogOffset);
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextOffset);
                try {
                    if (bufferConsumeQueue != null && bufferConsumeQueue.hasNext()) {
                        while (bufferConsumeQueue.hasNext()) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, cqUnit.getQueueOffset());
                            nextOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();
                            if (nextOffset >= maxOffset) {
                                return messageIds;
                            }
                        }
                    } else {
                        return messageIds;
                    }
                } finally {
                    if (bufferConsumeQueue != null) {
                        bufferConsumeQueue.release();
                    }
                }
            }
        }
        return messageIds;
    }

    @Override
    @Deprecated
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            CqUnit cqUnit = consumeQueue.get(consumeOffset);

            if (cqUnit != null) {
                long offsetPy = cqUnit.getPos();
                return !estimateInMemByCommitOffset(offsetPy, maxOffsetPy);
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize) {
        ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            CqUnit firstCQItem = consumeQueue.get(consumeOffset);
            if (firstCQItem == null) {
                return false;
            }
            long startOffsetPy = firstCQItem.getPos();
            if (batchSize <= 1) {
                int size = firstCQItem.getSize();
                return checkInMemByCommitOffset(startOffsetPy, size);
            }

            CqUnit lastCQItem = consumeQueue.get(consumeOffset + batchSize);
            if (lastCQItem == null) {
                int size = firstCQItem.getSize();
                return checkInMemByCommitOffset(startOffsetPy, size);
            }
            long endOffsetPy = lastCQItem.getPos();
            int size = (int) (endOffsetPy - startOffsetPy) + lastCQItem.getSize();
            return checkInMemByCommitOffset(startOffsetPy, size);
        }
        return false;
    }

    @Override
    public boolean checkInStoreByConsumeOffset(String topic, int queueId, long consumeOffset) {
        long commitLogOffset = getCommitLogOffsetInQueue(topic, queueId, consumeOffset);
        return checkInDiskByCommitOffset(commitLogOffset);
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }
    @Override
    public long dispatchBehindMilliseconds() {
        return this.reputMessageService.behindMs();
    }

    public long flushBehindBytes() {
        if (this.messageStoreConfig.isTransientStorePoolEnable()) {
            return this.commitLog.remainHowManyDataToCommit() + this.commitLog.remainHowManyDataToFlush();
        } else {
            return this.commitLog.remainHowManyDataToFlush();
        }
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public long getFlushedWhere() {
        return this.commitLog.getFlushedWhere();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        //copy a new map
        ConcurrentHashMap<String, Long> newMap = new ConcurrentHashMap<>(consumeQueueStore.getTopicQueueTable());
        SelectMappedBufferResult lastBuffer = null;
        long startReadOffset = phyOffset == -1 ? 0 : phyOffset;
        while ((lastBuffer = selectOneMessageByOffset(startReadOffset)) != null) {
            try {
                if (lastBuffer.getStartOffset() > startReadOffset) {
                    startReadOffset = lastBuffer.getStartOffset();
                    continue;
                }

                ByteBuffer bb = lastBuffer.getByteBuffer();
                int magicCode = bb.getInt(bb.position() + 4);
                if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                    startReadOffset += bb.getInt(bb.position());
                    continue;
                } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                    throw new RuntimeException("Unknown magicCode: " + magicCode);
                }

                lastBuffer.getByteBuffer().mark();

                DispatchRequest dispatchRequest = checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStoreConfig.isDuplicationEnable(), true);
                if (!dispatchRequest.isSuccess())
                    break;

                lastBuffer.getByteBuffer().reset();

                MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                if (msg == null) {
                    break;
                }
                String key = msg.getTopic() + "-" + msg.getQueueId();
                Long cur = newMap.get(key);
                if (cur != null && cur > msg.getQueueOffset()) {
                    newMap.put(key, msg.getQueueOffset());
                }
                startReadOffset += msg.getStoreSize();
            } catch (Throwable e) {
                LOGGER.error("resetWriteOffset error.", e);
            } finally {
                if (lastBuffer != null)
                    lastBuffer.release();
            }
        }
        if (this.commitLog.resetOffset(phyOffset)) {
            this.consumeQueueStore.setTopicQueueTable(newMap);
            return true;
        } else {
            return false;
        }
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
    public void setPhysicalOffset(long phyOffset) {
        this.commitLog.setMappedFileQueueOffset(phyOffset);
    }

    @Override
    public boolean isMappedFilesEmpty() {
        return this.commitLog.isMappedFilesEmpty();
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
        return this.consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE ||
            this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean estimateInMemByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) <= memory;
    }

    private boolean checkInMemByCommitOffset(long offsetPy, int size) {
        SelectMappedBufferResult message = this.commitLog.getMessage(offsetPy, size);
        if (message != null) {
            try {
                return message.isInMem();
            } finally {
                message.release();
            }
        }
        return false;
    }

    public boolean checkInDiskByCommitOffset(long offsetPy) {
        return offsetPy >= commitLog.getMinOffset();
    }

    /**
     * The ratio val is estimated by the experiment and experience
     * so that the result is not high accurate for different business
     *
     * @return
     */
    public boolean checkInColdAreaByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryHotRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int unitBatchNum, int maxMsgNums, long maxMsgSize, int bufferTotal,
        int messageTotal, boolean isInMem) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (messageTotal + unitBatchNum > maxMsgNums) {
            return true;
        }

        if (bufferTotal + sizePy > maxMsgSize) {
            return true;
        }

        if (isInMem) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            return messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1;
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            return messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1;
        }
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

    private boolean isRecoverConcurrently() {
        return this.brokerConfig.isRecoverConcurrently() && !this.messageStoreConfig.isEnableRocksDBStore();
    }

    private void recover(final boolean lastExitOK) throws RocksDBException {
        boolean recoverConcurrently = this.isRecoverConcurrently();
        LOGGER.info("message store recover mode: {}", recoverConcurrently ? "concurrent" : "normal");

        // recover consume queue
        long recoverConsumeQueueStart = System.currentTimeMillis();
        this.recoverConsumeQueue();
        long maxPhyOffsetOfConsumeQueue = this.consumeQueueStore.getMaxPhyOffsetInConsumeQueue();
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

    @Override
    public long getTimingMessageCount(String topic) {
        if (null == timerMessageStore) {
            return 0L;
        } else {
            return timerMessageStore.getTimerMetrics().getTimingCount(topic);
        }
    }

    @Override
    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    @Override
    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private void recoverConsumeQueue() {
        if (!this.isRecoverConcurrently()) {
            this.consumeQueueStore.recover();
        } else {
            this.consumeQueueStore.recoverConcurrently();
        }
    }

    @Override
    public void recoverTopicQueueTable() {
        long minPhyOffset = this.commitLog.getMinOffset();
        this.consumeQueueStore.recoverOffsetTable(minPhyOffset);
    }

    @Override
    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    @Override
    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
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

    public void doDispatch(DispatchRequest req) throws RocksDBException {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    /**
     * @param dispatchRequest
     * @throws RocksDBException only in rocksdb mode
     */
    protected void putMessagePositionInfo(DispatchRequest dispatchRequest) throws RocksDBException {
        this.consumeQueueStore.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(final ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {
        return this.commitLog.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
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
        if (this.isTransientStorePoolEnable()) {
            return this.transientStorePool.availableBufferNums();
        }
        return Integer.MAX_VALUE;
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
    public boolean getData(long offset, int size, ByteBuffer byteBuffer) {
        return this.commitLog.getData(offset, size, byteBuffer);
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = this.getConsumeQueueTable().get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
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
    public PerfCounter.Ticks getPerfCounter() {
        return perfs;
    }

    @Override
    public ConsumeQueueStoreInterface getQueueStore() {
        return consumeQueueStore;
    }

    @Override
    public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        // empty
    }

    @Override
    public void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile,
        boolean isRecover, boolean isFileEnd) throws RocksDBException {
        if (doDispatch && !isFileEnd) {
            this.doDispatch(dispatchRequest);
        }
    }

    @Override
    public boolean isSyncDiskFlush() {
        return FlushDiskType.SYNC_FLUSH == this.getMessageStoreConfig().getFlushDiskType();
    }

    @Override
    public boolean isSyncMaster() {
        return BrokerRole.SYNC_MASTER == this.getMessageStoreConfig().getBrokerRole();
    }

    @Override
    public void assignOffset(MessageExtBrokerInner msg) throws RocksDBException {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            this.consumeQueueStore.assignQueueOffset(msg);
        }
    }

    @Override
    public void increaseOffset(MessageExtBrokerInner msg, short messageNum) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            this.consumeQueueStore.increaseQueueOffset(msg, messageNum);
        }
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

    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) throws RocksDBException {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        private final String diskSpaceWarningLevelRatio =
            System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "");

        private final String diskSpaceCleanForciblyRatio =
            System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "");
        private long lastRedeleteTimestamp = 0;

        private final AtomicInteger manualDeleteFileSeveralTimes = new AtomicInteger();

        private volatile boolean cleanImmediately = false;

        private int forceCleanFailedTimes = 0;

        double getDiskSpaceWarningLevelRatio() {
            double finalDiskSpaceWarningLevelRatio;
            if ("".equals(diskSpaceWarningLevelRatio)) {
                finalDiskSpaceWarningLevelRatio = DefaultMessageStore.this.getMessageStoreConfig().getDiskSpaceWarningLevelRatio() / 100.0;
            } else {
                finalDiskSpaceWarningLevelRatio = Double.parseDouble(diskSpaceWarningLevelRatio);
            }

            if (finalDiskSpaceWarningLevelRatio > 0.90) {
                finalDiskSpaceWarningLevelRatio = 0.90;
            }
            if (finalDiskSpaceWarningLevelRatio < 0.35) {
                finalDiskSpaceWarningLevelRatio = 0.35;
            }

            return finalDiskSpaceWarningLevelRatio;
        }

        double getDiskSpaceCleanForciblyRatio() {
            double finalDiskSpaceCleanForciblyRatio;
            if ("".equals(diskSpaceCleanForciblyRatio)) {
                finalDiskSpaceCleanForciblyRatio = DefaultMessageStore.this.getMessageStoreConfig().getDiskSpaceCleanForciblyRatio() / 100.0;
            } else {
                finalDiskSpaceCleanForciblyRatio = Double.parseDouble(diskSpaceCleanForciblyRatio);
            }

            if (finalDiskSpaceCleanForciblyRatio > 0.85) {
                finalDiskSpaceCleanForciblyRatio = 0.85;
            }
            if (finalDiskSpaceCleanForciblyRatio < 0.30) {
                finalDiskSpaceCleanForciblyRatio = 0.30;
            }

            return finalDiskSpaceCleanForciblyRatio;
        }

        public void executeDeleteFilesManually() {
            this.manualDeleteFileSeveralTimes.set(MAX_MANUAL_DELETE_FILE_TIMES);
            DefaultMessageStore.LOGGER.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                this.deleteExpiredFiles();
                this.reDeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            int destroyMappedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
            int deleteFileBatchMax = DefaultMessageStore.this.getMessageStoreConfig().getDeleteFileBatchMax();

            boolean isTimeUp = this.isTimeToDelete();
            boolean isUsageExceedsThreshold = this.isSpaceToDelete();
            boolean isManualDelete = this.manualDeleteFileSeveralTimes.get() > 0;

            if (isTimeUp || isUsageExceedsThreshold || isManualDelete) {

                if (isManualDelete) {
                    this.manualDeleteFileSeveralTimes.decrementAndGet();
                }

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                LOGGER.info("begin to delete before {} hours file. isTimeUp: {} isUsageExceedsThreshold: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {} deleteFileBatchMax: {}",
                    fileReservedTime,
                    isTimeUp,
                    isUsageExceedsThreshold,
                    manualDeleteFileSeveralTimes.get(),
                    cleanAtOnce,
                    deleteFileBatchMax);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                    destroyMappedFileIntervalForcibly, cleanAtOnce, deleteFileBatchMax);
                if (deleteCount > 0) {
                    // If in the controller mode, we should notify the AutoSwitchHaService to truncateEpochFile
                    if (DefaultMessageStore.this.brokerConfig.isEnableControllerMode()) {
                        if (DefaultMessageStore.this.haService instanceof AutoSwitchHAService) {
                            final long minPhyOffset = getMinPhyOffset();
                            ((AutoSwitchHAService) DefaultMessageStore.this.haService).truncateEpochFilePrefix(minPhyOffset - 1);
                        }
                    }
                } else if (isUsageExceedsThreshold) {
                    LOGGER.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        private void reDeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMappedFileIntervalForcibly =
                    DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMappedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return DefaultMessageStore.this.brokerConfig.getIdentifier() + CleanCommitLogService.class.getSimpleName();
        }

        protected boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.LOGGER.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        private boolean isSpaceToDelete() {
            cleanImmediately = false;

            String commitLogStorePath = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            String[] storePaths = commitLogStorePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
            Set<String> fullStorePath = new HashSet<>();
            double minPhysicRatio = 100;
            String minStorePath = null;
            for (String storePathPhysic : storePaths) {
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (minPhysicRatio > physicRatio) {
                    minPhysicRatio = physicRatio;
                    minStorePath = storePathPhysic;
                }
                if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                    fullStorePath.add(storePathPhysic);
                }
            }
            DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
            if (minPhysicRatio > getDiskSpaceWarningLevelRatio()) {
                boolean diskFull = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskFull) {
                    DefaultMessageStore.LOGGER.error("physic disk maybe full soon " + minPhysicRatio +
                        ", so mark disk full, storePathPhysic=" + minStorePath);
                }

                cleanImmediately = true;
                return true;
            } else if (minPhysicRatio > getDiskSpaceCleanForciblyRatio()) {
                cleanImmediately = true;
                return true;
            } else {
                boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                if (!diskOK) {
                    DefaultMessageStore.LOGGER.info("physic disk space OK " + minPhysicRatio +
                        ", so mark disk ok, storePathPhysic=" + minStorePath);
                }
            }

            String storePathLogics = StorePathConfigHelper
                .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            if (logicsRatio > getDiskSpaceWarningLevelRatio()) {
                boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskOK) {
                    DefaultMessageStore.LOGGER.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                }

                cleanImmediately = true;
                return true;
            } else if (logicsRatio > getDiskSpaceCleanForciblyRatio()) {
                cleanImmediately = true;
                return true;
            } else {
                boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                if (!diskOK) {
                    DefaultMessageStore.LOGGER.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                }
            }

            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            int replicasPerPartition = DefaultMessageStore.this.getMessageStoreConfig().getReplicasPerDiskPartition();
            // Only one commitLog in node
            if (replicasPerPartition <= 1) {
                if (minPhysicRatio < 0 || minPhysicRatio > ratio) {
                    DefaultMessageStore.LOGGER.info("commitLog disk maybe full soon, so reclaim space, " + minPhysicRatio);
                    return true;
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.LOGGER.info("consumeQueue disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
                return false;
            } else {
                long majorFileSize = DefaultMessageStore.this.getMajorFileSize();
                long partitionLogicalSize = UtilAll.getDiskPartitionTotalSpace(minStorePath) / replicasPerPartition;
                double logicalRatio = 1.0 * majorFileSize / partitionLogicalSize;

                if (logicalRatio > DefaultMessageStore.this.getMessageStoreConfig().getLogicalDiskSpaceCleanForciblyThreshold()) {
                    // if logical ratio exceeds 0.80, then clean immediately
                    DefaultMessageStore.LOGGER.info("Logical disk usage {} exceeds logical disk space clean forcibly threshold {}, forcibly: {}",
                        logicalRatio, minPhysicRatio, cleanImmediately);
                    cleanImmediately = true;
                    return true;
                }

                boolean isUsageExceedsThreshold = logicalRatio > ratio;
                if (isUsageExceedsThreshold) {
                    DefaultMessageStore.LOGGER.info("Logical disk usage {} exceeds clean threshold {}, forcibly: {}",
                        logicalRatio, ratio, cleanImmediately);
                }
                return isUsageExceedsThreshold;
            }
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes.get();
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes.set(manualDeleteFileSeveralTimes);
        }

        public double calcStorePathPhysicRatio() {
            Set<String> fullStorePath = new HashSet<>();
            String storePath = getStorePathPhysic();
            String[] paths = storePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
            double minPhysicRatio = 100;
            for (String path : paths) {
                double physicRatio = UtilAll.isPathExists(path) ?
                    UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
                minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
                if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                    fullStorePath.add(path);
                }
            }
            DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
            return minPhysicRatio;

        }

        public boolean isSpaceFull() {
            double physicRatio = calcStorePathPhysicRatio();
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                DefaultMessageStore.LOGGER.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.getDiskSpaceWarningLevelRatio()) {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    DefaultMessageStore.LOGGER.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    DefaultMessageStore.LOGGER.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }

    class CleanConsumeQueueService {
        protected long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        protected void deleteExpiredFiles() {
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();

                for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                    for (ConsumeQueueInterface logic : maps.values()) {
                        int deleteCount = DefaultMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minOffset);
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return DefaultMessageStore.this.brokerConfig.getIdentifier() + CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class CorrectLogicOffsetService {
        private long lastForceCorrectTime = -1L;

        public void run() {
            try {
                this.correctLogicMinOffset();
            } catch (Throwable e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private boolean needCorrect(ConsumeQueueInterface logic, long minPhyOffset, long lastForeCorrectTimeCurRun) {
            if (logic == null) {
                return false;
            }
            // If first exist and not available, it means first file may destroy failed, delete it.
            if (DefaultMessageStore.this.consumeQueueStore.isFirstFileExist(logic) && !DefaultMessageStore.this.consumeQueueStore.isFirstFileAvailable(logic)) {
                LOGGER.error("CorrectLogicOffsetService.needCorrect. first file not available, trigger correct." +
                        " topic:{}, queue:{}, maxPhyOffset in queue:{}, minPhyOffset " +
                        "in commit log:{}, minOffset in queue:{}, maxOffset in queue:{}, cqType:{}"
                    , logic.getTopic(), logic.getQueueId(), logic.getMaxPhysicOffset()
                    , minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getCQType());
                return true;
            }

            // logic.getMaxPhysicOffset() or minPhyOffset = -1
            // means there is no message in current queue, so no need to correct.
            if (logic.getMaxPhysicOffset() == -1 || minPhyOffset == -1) {
                return false;
            }

            if (logic.getMaxPhysicOffset() < minPhyOffset) {
                if (logic.getMinOffsetInQueue() < logic.getMaxOffsetInQueue()) {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is less than min phy offset: {}, " +
                            "but min offset: {} is less than max offset: {}. topic:{}, queue:{}, cqType:{}."
                        , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                        , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                } else if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                    return false;
                } else {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. It should not happen, logic max phy offset: {} is less than min phy offset: {}," +
                            " but min offset: {} is larger than max offset: {}. topic:{}, queue:{}, cqType:{}"
                        , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                        , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return false;
                }
            }
            //the logic.getMaxPhysicOffset() >= minPhyOffset
            int forceCorrectInterval = DefaultMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetForceInterval();
            if ((System.currentTimeMillis() - lastForeCorrectTimeCurRun) > forceCorrectInterval) {
                lastForceCorrectTime = System.currentTimeMillis();
                CqUnit cqUnit = logic.getEarliestUnit();
                if (cqUnit == null) {
                    if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                        return false;
                    } else {
                        LOGGER.error("CorrectLogicOffsetService.needCorrect. cqUnit is null, logic max phy offset: {} is greater than min phy offset: {}, " +
                                "but min offset: {} is not equal to max offset: {}. topic:{}, queue:{}, cqType:{}."
                            , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                        return true;
                    }
                }

                if (cqUnit.getPos() < minPhyOffset) {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is greater than min phy offset: {}, " +
                            "but minPhyPos in cq is: {}. min offset in queue: {}, max offset in queue: {}, topic:{}, queue:{}, cqType:{}."
                        , logic.getMaxPhysicOffset(), minPhyOffset, cqUnit.getPos(), logic.getMinOffsetInQueue()
                        , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                }

                if (cqUnit.getPos() >= minPhyOffset) {

                    // Normal case, do not need to correct.
                    return false;
                }
            }

            return false;
        }

        private void correctLogicMinOffset() {

            long lastForeCorrectTimeCurRun = lastForceCorrectTime;
            long minPhyOffset = getMinPhyOffset();
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface logic : maps.values()) {
                    if (Objects.equals(CQType.SimpleCQ, logic.getCQType())) {
                        // cq is not supported for now.
                        continue;
                    }
                    if (needCorrect(logic, minPhyOffset, lastForeCorrectTimeCurRun)) {
                        doCorrect(logic, minPhyOffset);
                    }
                }
            }
        }

        private void doCorrect(ConsumeQueueInterface logic, long minPhyOffset) {
            DefaultMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minPhyOffset);
            int sleepIntervalWhenCorrectMinOffset = DefaultMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetSleepInterval();
            if (sleepIntervalWhenCorrectMinOffset > 0) {
                try {
                    Thread.sleep(sleepIntervalWhenCorrectMinOffset);
                } catch (InterruptedException ignored) {
                }
            }
        }

        public String getServiceName() {
            if (brokerConfig.isInBrokerContainer()) {
                return brokerConfig.getIdentifier() + CorrectLogicOffsetService.class.getSimpleName();
            }
            return CorrectLogicOffsetService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();

            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = DefaultMessageStore.this.consumeQueueStore.flush(cq, flushConsumeQueueLeastPages);
                    }
                }
            }

            if (messageStoreConfig.isEnableCompaction()) {
                compactionStore.flush(flushConsumeQueueLeastPages);
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.brokerConfig.isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + FlushConsumeQueueService.class.getSimpleName();
            }
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60;
        }
    }

    static class BatchDispatchRequest {

        private final ByteBuffer byteBuffer;

        private final int position;

        private final int size;

        private final long id;

        public BatchDispatchRequest(ByteBuffer byteBuffer, int position, int size, long id) {
            this.byteBuffer = byteBuffer;
            this.position = position;
            this.size = size;
            this.id = id;
        }
    }

    static class DispatchRequestOrderlyQueue {

        DispatchRequest[][] buffer;

        long ptr = 0;

        AtomicLong maxPtr = new AtomicLong();

        public DispatchRequestOrderlyQueue(int bufferNum) {
            this.buffer = new DispatchRequest[bufferNum][];
        }

        public void put(long index, DispatchRequest[] dispatchRequests) {
            while (ptr + this.buffer.length <= index) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            int mod = (int) (index % this.buffer.length);
            this.buffer[mod] = dispatchRequests;
            maxPtr.incrementAndGet();
        }

        public DispatchRequest[] get(List<DispatchRequest[]> dispatchRequestsList) {
            synchronized (this) {
                for (int i = 0; i < this.buffer.length; i++) {
                    int mod = (int) (ptr % this.buffer.length);
                    DispatchRequest[] ret = this.buffer[mod];
                    if (ret == null) {
                        this.notifyAll();
                        return null;
                    }
                    dispatchRequestsList.add(ret);
                    this.buffer[mod] = null;
                    ptr++;
                }
            }
            return null;
        }

        public synchronized boolean isEmpty() {
            return maxPtr.get() == ptr;
        }

    }

    @Override
    public void notifyMessageArriveIfNecessary(DispatchRequest dispatchRequest) {
        if (DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
            && DefaultMessageStore.this.messageArrivingListener != null) {
            DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
            DefaultMessageStore.this.reputMessageService.notifyMessageArrive4MultiQueue(dispatchRequest);
        }
    }

    class ReputMessageService extends ServiceThread {

        protected volatile long reputFromOffset = 0;
        protected volatile long currentReputTimestamp = System.currentTimeMillis();

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        public long getCurrentReputTimestamp() {
            return currentReputTimestamp;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                LOGGER.warn("shutdown ReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max" +
                        " offset={}, reputFromOffset={}", DefaultMessageStore.this.commitLog.getMaxOffset(),
                    this.reputFromOffset);
            }

            super.shutdown();
        }

        public long behind() {
            return DefaultMessageStore.this.getConfirmOffset() - this.reputFromOffset;
        }

        public long behindMs() {
            long lastCommitLogFileTimeStamp = System.currentTimeMillis();
            MappedFile lastMappedFile = DefaultMessageStore.this.commitLog.getMappedFileQueue().getLastMappedFile();
            if (lastMappedFile != null) {
                lastCommitLogFileTimeStamp = lastMappedFile.getStoreTimestamp();
            }
            return Math.max(0, lastCommitLogFileTimeStamp - this.currentReputTimestamp);
        }

        public boolean isCommitLogAvailable() {
            return this.reputFromOffset < getReputEndOffset();
        }

        protected long getReputEndOffset() {
            return DefaultMessageStore.this.getMessageStoreConfig().isReadUnCommitted() ? DefaultMessageStore.this.commitLog.getMaxOffset() : DefaultMessageStore.this.commitLog.getConfirmOffset();
        }

        public void doReput() {
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                LOGGER.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                    this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            boolean isCommitLogAvailable = isCommitLogAvailable();
            if (!isCommitLogAvailable) {
                currentReputTimestamp = System.currentTimeMillis();
            }
            for (boolean doNext = true; isCommitLogAvailable && doNext; ) {

                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);

                if (result == null) {
                    break;
                }

                try {
                    this.reputFromOffset = result.getStartOffset();

                    for (int readSize = 0; readSize < result.getSize() && reputFromOffset < getReputEndOffset() && doNext; ) {
                        DispatchRequest dispatchRequest =
                            DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false, false);
                        int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                        if (reputFromOffset + size > getReputEndOffset()) {
                            doNext = false;
                            break;
                        }

                        if (dispatchRequest.isSuccess()) {
                            if (size > 0) {
                                currentReputTimestamp = dispatchRequest.getStoreTimestamp();
                                DefaultMessageStore.this.doDispatch(dispatchRequest);

                                if (!notifyMessageArriveInBatch) {
                                    notifyMessageArriveIfNecessary(dispatchRequest);
                                }

                                this.reputFromOffset += size;
                                readSize += size;
                                if (!DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable() &&
                                    DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                    DefaultMessageStore.this.storeStatsService
                                        .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(dispatchRequest.getBatchSize());
                                    DefaultMessageStore.this.storeStatsService
                                        .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                        .add(dispatchRequest.getMsgSize());
                                }
                            } else if (size == 0) {
                                this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                readSize = result.getSize();
                            }
                        } else {
                            if (size > 0) {
                                LOGGER.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                this.reputFromOffset += size;
                            } else {
                                doNext = false;
                                // If user open the dledger pattern or the broker is master node,
                                // it will not ignore the exception and fix the reputFromOffset variable
                                if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                    DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                    LOGGER.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                        this.reputFromOffset);
                                    this.reputFromOffset += result.getSize() - readSize;
                                }
                            }
                        }
                    }
                } catch (RocksDBException e) {
                    ERROR_LOG.info("dispatch message to cq exception. reputFromOffset: {}", this.reputFromOffset, e);
                    return;
                } finally {
                    result.release();
                }
            }
        }

        private void notifyMessageArrive4MultiQueue(DispatchRequest dispatchRequest) {
            Map<String, String> prop = dispatchRequest.getPropertiesMap();
            if (prop == null || dispatchRequest.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                return;
            }
            String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
            String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
            if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
                return;
            }
            String[] queues = multiDispatchQueue.split(MixAll.LMQ_DISPATCH_SEPARATOR);
            String[] queueOffsets = multiQueueOffset.split(MixAll.LMQ_DISPATCH_SEPARATOR);
            if (queues.length != queueOffsets.length) {
                return;
            }
            for (int i = 0; i < queues.length; i++) {
                String queueName = queues[i];
                long queueOffset = Long.parseLong(queueOffsets[i]);
                int queueId = dispatchRequest.getQueueId();
                if (DefaultMessageStore.this.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                    queueId = MixAll.LMQ_QUEUE_ID;
                }
                DefaultMessageStore.this.messageArrivingListener.arriving(
                    queueName, queueId, queueOffset + 1, dispatchRequest.getTagsCode(),
                    dispatchRequest.getStoreTimestamp(), dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                    this.doReput();
                } catch (Throwable e) {
                    DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + ReputMessageService.class.getSimpleName();
            }
            return ReputMessageService.class.getSimpleName();
        }

    }

    class MainBatchDispatchRequestService extends ServiceThread {

        private final ExecutorService batchDispatchRequestExecutor;

        public MainBatchDispatchRequestService() {
            batchDispatchRequestExecutor = ThreadUtils.newThreadPoolExecutor(
                DefaultMessageStore.this.getMessageStoreConfig().getBatchDispatchRequestThreadPoolNums(),
                DefaultMessageStore.this.getMessageStoreConfig().getBatchDispatchRequestThreadPoolNums(),
                1000 * 60,
                TimeUnit.MICROSECONDS,
                new LinkedBlockingQueue<>(4096),
                new ThreadFactoryImpl("BatchDispatchRequestServiceThread_"),
                new ThreadPoolExecutor.AbortPolicy());
        }

        private void pollBatchDispatchRequest() {
            try {
                if (!batchDispatchRequestQueue.isEmpty()) {
                    BatchDispatchRequest task = batchDispatchRequestQueue.peek();
                    batchDispatchRequestExecutor.execute(() -> {
                        try {
                            ByteBuffer tmpByteBuffer = task.byteBuffer;
                            tmpByteBuffer.position(task.position);
                            tmpByteBuffer.limit(task.position + task.size);
                            List<DispatchRequest> dispatchRequestList = new ArrayList<>();
                            while (tmpByteBuffer.hasRemaining()) {
                                DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(tmpByteBuffer, false, false, false);
                                if (dispatchRequest.isSuccess()) {
                                    dispatchRequestList.add(dispatchRequest);
                                } else {
                                    LOGGER.error("[BUG]read total count not equals msg total size.");
                                }
                            }
                            dispatchRequestOrderlyQueue.put(task.id, dispatchRequestList.toArray(new DispatchRequest[dispatchRequestList.size()]));
                            mappedPageHoldCount.getAndDecrement();
                        } catch (Exception e) {
                            LOGGER.error("There is an exception in task execution.", e);
                        }
                    });
                    batchDispatchRequestQueue.poll();
                }
            } catch (Exception e) {
                DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                    pollBatchDispatchRequest();
                } catch (Exception e) {
                    DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + MainBatchDispatchRequestService.class.getSimpleName();
            }
            return MainBatchDispatchRequestService.class.getSimpleName();
        }

    }

    class DispatchService extends ServiceThread {

        private final List<DispatchRequest[]> dispatchRequestsList = new ArrayList<>();

        // dispatchRequestsList:[
        //      {dispatchRequests:[{dispatchRequest}, {dispatchRequest}]},
        //      {dispatchRequests:[{dispatchRequest}, {dispatchRequest}]}]
        private void dispatch() throws Exception {
            dispatchRequestsList.clear();
            dispatchRequestOrderlyQueue.get(dispatchRequestsList);
            if (!dispatchRequestsList.isEmpty()) {
                for (DispatchRequest[] dispatchRequests : dispatchRequestsList) {
                    for (DispatchRequest dispatchRequest : dispatchRequests) {
                        DefaultMessageStore.this.doDispatch(dispatchRequest);
                        // wake up long-polling
                        DefaultMessageStore.this.notifyMessageArriveIfNecessary(dispatchRequest);

                        if (!DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable() &&
                            DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                            DefaultMessageStore.this.storeStatsService
                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
                            DefaultMessageStore.this.storeStatsService
                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                .add(dispatchRequest.getMsgSize());
                        }
                    }
                }
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                    dispatch();
                } catch (Exception e) {
                    DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + DispatchService.class.getSimpleName();
            }
            return DispatchService.class.getSimpleName();
        }
    }

    class ConcurrentReputMessageService extends ReputMessageService {

        private static final int BATCH_SIZE = 1024 * 1024 * 4;

        private long batchId = 0;

        private MainBatchDispatchRequestService mainBatchDispatchRequestService;

        private DispatchService dispatchService;

        public ConcurrentReputMessageService() {
            super();
            this.mainBatchDispatchRequestService = new MainBatchDispatchRequestService();
            this.dispatchService = new DispatchService();
        }

        public void createBatchDispatchRequest(ByteBuffer byteBuffer, int position, int size) {
            if (position < 0) {
                return;
            }
            mappedPageHoldCount.getAndIncrement();
            BatchDispatchRequest task = new BatchDispatchRequest(byteBuffer.duplicate(), position, size, batchId++);
            batchDispatchRequestQueue.offer(task);
        }

        @Override
        public void start() {
            super.start();
            this.mainBatchDispatchRequestService.start();
            this.dispatchService.start();
        }

        @Override
        public void doReput() {
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                LOGGER.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                    this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);

                if (result == null) {
                    break;
                }

                int batchDispatchRequestStart = -1;
                int batchDispatchRequestSize = -1;
                try {
                    this.reputFromOffset = result.getStartOffset();

                    for (int readSize = 0; readSize < result.getSize() && reputFromOffset < getReputEndOffset() && doNext; ) {
                        ByteBuffer byteBuffer = result.getByteBuffer();

                        int totalSize = preCheckMessageAndReturnSize(byteBuffer);

                        if (totalSize > 0) {
                            if (batchDispatchRequestStart == -1) {
                                batchDispatchRequestStart = byteBuffer.position();
                                batchDispatchRequestSize = 0;
                            }
                            batchDispatchRequestSize += totalSize;
                            if (batchDispatchRequestSize > BATCH_SIZE) {
                                this.createBatchDispatchRequest(byteBuffer, batchDispatchRequestStart, batchDispatchRequestSize);
                                batchDispatchRequestStart = -1;
                                batchDispatchRequestSize = -1;
                            }
                            byteBuffer.position(byteBuffer.position() + totalSize);
                            this.reputFromOffset += totalSize;
                            readSize += totalSize;
                        } else {
                            doNext = false;
                            if (totalSize == 0) {
                                this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                            }
                            this.createBatchDispatchRequest(byteBuffer, batchDispatchRequestStart, batchDispatchRequestSize);
                            batchDispatchRequestStart = -1;
                            batchDispatchRequestSize = -1;
                        }
                    }
                } finally {
                    this.createBatchDispatchRequest(result.getByteBuffer(), batchDispatchRequestStart, batchDispatchRequestSize);
                    boolean over = mappedPageHoldCount.get() == 0;
                    while (!over) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        over = mappedPageHoldCount.get() == 0;
                    }
                    result.release();
                }
            }
        }

        /**
         * pre-check the message and returns the message size
         *
         * @return 0 Come to the end of file // >0 Normal messages // -1 Message checksum failure
         */
        public int preCheckMessageAndReturnSize(ByteBuffer byteBuffer) {
            byteBuffer.mark();

            int totalSize = byteBuffer.getInt();
            if (reputFromOffset + totalSize > DefaultMessageStore.this.getConfirmOffset()) {
                return -1;
            }

            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MessageDecoder.MESSAGE_MAGIC_CODE:
                case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                    break;
                case MessageDecoder.BLANK_MAGIC_CODE:
                    return 0;
                default:
                    return -1;
            }

            byteBuffer.reset();

            return totalSize;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                LOGGER.warn("shutdown concurrentReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max" +
                        " offset={}, reputFromOffset={}", DefaultMessageStore.this.commitLog.getMaxOffset(),
                    this.reputFromOffset);
            }

            this.mainBatchDispatchRequestService.shutdown();
            this.dispatchService.shutdown();
            super.shutdown();
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getIdentifier() + ConcurrentReputMessageService.class.getSimpleName();
            }
            return ConcurrentReputMessageService.class.getSimpleName();
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

    public void enableRocksdbCQWrite() {
        try {
            RocksDBMessageStore store = new RocksDBMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig, this.topicConfigTable);
            this.rocksDBMessageStore = store;
            store.loadAndStartConsumerServiceOnly();
            addDispatcher(store.getDispatcherBuildRocksdbConsumeQueue());
        } catch (Exception e) {
            LOGGER.error("enableRocksdbCqWrite error", e);
        }
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public List<PutMessageHook> getPutMessageHookList() {
        return putMessageHookList;
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

    @Override
    public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
        if (from < 0) {
            from = 0;
        }

        if (from >= to) {
            return 0;
        }

        if (null == filter) {
            return to - from;
        }

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (null == consumeQueue) {
            return 0;
        }

        // correct the "from" argument to min offset in queue if it is too small
        long minOffset = consumeQueue.getMinOffsetInQueue();
        if (from < minOffset) {
            long diff = to - from;
            from = minOffset;
            to = from + diff;
        }

        long msgCount = consumeQueue.estimateMessageCount(from, to, filter);
        return msgCount == -1 ? to - from : msgCount;
    }

    @Override
    public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        return DefaultStoreMetricsManager.getMetricsView();
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

    public long getReputFromOffset() {
        return this.reputMessageService.getReputFromOffset();
    }

    public RocksDBMessageStore getRocksDBMessageStore() {
        return this.rocksDBMessageStore;
    }

    public ConsumeQueueStoreInterface getConsumeQueueStore() {
        return consumeQueueStore;
    }
}
