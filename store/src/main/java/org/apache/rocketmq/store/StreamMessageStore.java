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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.BatchConsumeQueue;
import org.apache.rocketmq.store.queue.CQType;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStore;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.util.PerfCounter;
import org.apache.rocketmq.store.util.QueueTypeUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class StreamMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public final PerfCounter.Ticks perfs = new PerfCounter.Ticks(log);

    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;

    private final ConsumeQueueStore consumeQueueStore;

    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    protected HashMap<String/* topic-queueid */, Long/* offset */> batchTopicQueueTable = new HashMap<String, Long>(1024);

    private final FlushConsumeQueueService flushConsumeQueueService;

    private final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final CorrectLogicOffsetService correctLogicOffsetService;

    private final IndexService indexService;

    private final AllocateMappedFileService allocateMappedFileService;

    private final ReputMessageService reputMessageService;

    private final HAService haService;

    private final ScheduleMessageService scheduleMessageService;

    private final StoreStatsService storeStatsService;

    private final TransientStorePool transientStorePool;

    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    //polish for reput
    private ThreadPoolExecutor[] reputExecutors;

    private BlockingQueue<Runnable>[] reputQueues;

    private boolean isDispatchFromSenderThread;

    private static final Future EMPTY_FUTURE = new Future() {
        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(final long timeout, final TimeUnit unit) {
            return null;
        }
    };

    // Max pull msg size
    private final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

    public StreamMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            throw new RuntimeException("dleger is not supported in this message store.");
        }
        this.isDispatchFromSenderThread = messageStoreConfig.isDispatchFromSenderThread();
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
        this.consumeQueueStore = new ConsumeQueueStore(this, this.messageStoreConfig, this.consumeQueueTable);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.correctLogicOffsetService = new CorrectLogicOffsetService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }
        if (isDispatchFromSenderThread) {
            this.reputMessageService = new SyncReputMessageService();
        } else {
            this.reputMessageService = new ReputMessageService();
        }

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        DefaultMappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
        initAsyncReputThreads(messageStoreConfig.getDispatchCqThreads(), messageStoreConfig.getDispatchCqCacheNum());
    }

    /**
     * @throws IOException
     */
    @Override
    public boolean load() {
        boolean result = true;

        try {
            long start = System.currentTimeMillis();
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}, root dir: {}", lastExitOK ? "normally" : "abnormally", messageStoreConfig.getStorePathRootDir());

            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // load Commit Log
            result = result && this.commitLog.load();

            // load Batch Consume Queue
            result = result && this.loadBatchConsumeQueue();

            if (result) {
                this.storeCheckpoint =
                    new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                this.indexService.load(lastExitOK);

                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {} cost = {}", this.getMaxPhyOffset(), System.currentTimeMillis() - start);
            }
        } catch (Exception e) {
            log.error("load exception", e);
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

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);

        if (this.getMessageStoreConfig().isDuplicationEnable()) {
            this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        } else {
            this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        }
        this.reputMessageService.start();

        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.perfs.start();
        this.shutdown = false;
    }

    @Override
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {

                Thread.sleep(1000 * 3);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            this.perfs.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file. writable: {}, dispatchBehindBytes: {}, abort file: {}",
                        this.runningFlags.isWriteable(), dispatchBehindBytes(), StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
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
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    @Override
    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.consumeQueueStore.destroy(logic);
            }
        }
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }

        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
                && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            log.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            CQType cqType = QueueTypeUtils.getCQType(this);

            if (!CQType.BatchCQ.equals(cqType)) {
                log.warn("[BUG]The message is an inner batch but cq type is not batch consume queue");
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
            }
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        } else {
            this.printTimes.set(0);
        }

        int topicLen = msg.getTopic().length();
        if (topicLen > this.messageStoreConfig.getMaxTopicLength()) {
            log.warn("putMessage message topic[{}] length too long {}", msg.getTopic(), topicLen);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (topicLen > Byte.MAX_VALUE) {
            log.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker",
                    msg.getTopic(), topicLen);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null));
        }

        if (this.isOSPageCacheBusy()) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null));
        }

        long beginTime = this.getSystemClock().now();
        perfs.startTick("PUT_MESSAGE_TIME_MS");
        CompletableFuture<PutMessageResult> result = this.commitLog.asyncPutMessage(msg);
        perfs.endTick("PUT_MESSAGE_TIME_MS");

        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }

        return result;
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        CompletableFuture<PutMessageResult> future = asyncPutMessage(msg);
        try {
            return future.get(3, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("Get async put result failed", t);
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        CompletableFuture<PutMessageResult> future = asyncPutMessages(messageExtBatch);
        try {
            return future.get(3, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("Get async put result failed", t);
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        if (this.shutdown) {
            log.warn("StreamMessageStore has shutdown, so putMessages is forbidden");
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("StreamMessageStore is in slave mode, so putMessages is forbidden ");
            }

            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("StreamMessageStore is not writable, so putMessages is forbidden " + this.runningFlags.getFlagBits());
            }

            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        } else {
            this.printTimes.set(0);
        }

        int topicLen = messageExtBatch.getTopic().length();
        if (topicLen > this.messageStoreConfig.getMaxTopicLength()) {
            log.warn("putMessage batch message topic[{}] length too long {}", messageExtBatch.getTopic(), topicLen);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (topicLen > Byte.MAX_VALUE) {
            log.warn("putMessage batch message topic[{}] length too long {}, but it is not supported by broker",
                    messageExtBatch.getTopic(), topicLen);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (this.isOSPageCacheBusy()) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null));
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> result = this.commitLog.asyncPutMessages(messageExtBatch);

        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        return result;
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
    public SystemClock getSystemClock() {
        return systemClock;
    }

    @Override
    public CommitLog getCommitLog() {
        return commitLog;
    }

    public boolean isDispatchFromSenderThread() {
        return isDispatchFromSenderThread;
    }

    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums,
        final MessageFilter messageFilter) {
        return getMessage(group, topic, queueId, offset, maxMsgNums, MAX_PULL_MSG_SIZE, messageFilter);
    }

    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums,
        final int maxTotalMsgSize,
        final MessageFilter messageFilter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
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
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                final int maxFilterMessageCount = Math.max(messageStoreConfig.getPullBatchMaxMessageCount(), maxMsgNums);
                final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

                long maxPullSize = Math.max(maxTotalMsgSize, 100);
                if (maxPullSize > MAX_PULL_MSG_SIZE) {
                    log.warn("The max pull size is too large maxPullSize={} topic={} queueId={}", maxPullSize, topic, queueId);
                    maxPullSize = MAX_PULL_MSG_SIZE;
                }
                status = GetMessageStatus.NO_MATCHED_MESSAGE;
                long maxPhyOffsetPulling = 0;
                int cqFileNum = 0;

                while (getResult.getBufferTotalSize() <= 0
                        && nextBeginOffset < maxOffset
                        && cqFileNum++ < this.messageStoreConfig.getTravelCqFileNumWhenGetMessage()) {
                    ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextBeginOffset);

                    if (bufferConsumeQueue == null) {
                        status = GetMessageStatus.OFFSET_FOUND_NULL;
                        nextBeginOffset = nextOffsetCorrection(nextBeginOffset, this.consumeQueueStore.rollNextFile(consumeQueue, nextBeginOffset));
                        log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                                + maxOffset + ", but access logic queue failed. Correct nextBeginOffset to " + nextBeginOffset);
                        break;
                    }

                    try {
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        while (bufferConsumeQueue.hasNext()
                                && nextBeginOffset < maxOffset) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            int sizePy = cqUnit.getSize();

                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (cqUnit.getQueueOffset() - offset > maxFilterMessageCount) {
                                break;
                            }

                            if (this.isTheBatchFull(sizePy, cqUnit.getBatchNum(), maxMsgNums, maxPullSize, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                    isInDisk)) {
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

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferedMsgCount().add(1);
                            getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }
                    } finally {
                        bufferConsumeQueue.release();
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

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = this.getConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxOffsetInQueue();
        }

        return 0;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) {
        if (committed) {
            ConsumeQueueInterface logic = this.getConsumeQueue(topic, queueId);
            if (logic != null) {
                return logic.getMaxOffsetInQueue();
            }
        } else {
            Long offset = this.batchTopicQueueTable.get(topic + "-" + queueId);
            if (offset != null) {
                return offset;
            }
        }

        return 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = this.getConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue != null) {

            ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    if (bufferConsumeQueue.hasNext()) {
                        long offsetPy = bufferConsumeQueue.next().getPos();
                        return offsetPy;
                    }
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueueInterface logic = getConsumeQueue(topic, queueId);
        if (logic != null) {
            long resultOffset = logic.getOffsetInQueueByTime(timestamp);
            // -1 means no msg found.
            if (resultOffset == -1) {
                return -1;
            }
            // Make sure the result offset should in valid range.
            resultOffset = Math.max(resultOffset, logic.getMinOffsetInQueue());
            resultOffset = Math.min(resultOffset, logic.getMaxOffsetInQueue());
            return resultOffset;
        }

        // logic is null means there is no message in this queue, return -1.
        return -1;
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

    private String getStorePathPhysic() {
        String storePathPhysic = StreamMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        return storePathPhysic;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            String storePathPhysic = this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(this.getMaxPhyOffset()));

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
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.getConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return getStoreTime(logicQueue.getEarliestUnit());
        }

        return -1;
    }

    private long getStoreTime(CqUnit result) {
        if (result != null) {
            try {
                final long phyOffset = result.getPos();
                final int size = result.getSize();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface logicQueue = this.getConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return getStoreTime(logicQueue.get(consumeQueueOffset));
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.getConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data, dataStart, dataLength);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
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

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
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

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                for (ConsumeQueueInterface cq : queueTable.values()) {
                    this.consumeQueueStore.destroy(cq);
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();
                log.info("cleanUnusedTopic: {},topic consumeQueue destroyed", topic);
            }
        }
        return 0;
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Map.Entry<Integer, ConsumeQueueInterface> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getMaxPhysicOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        this.consumeQueueStore.destroy(nextQT.getValue());
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public double getDiskSpaceWarningLevelRatio() {
        return cleanCommitLogService.getDiskSpaceWarningLevelRatio();
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueueInterface consumeQueue = getConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            CqUnit cqUnit = consumeQueue.get(consumeOffset);

            if (cqUnit != null) {
                long offsetPy = cqUnit.getPos();
                return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
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

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int unitBatchNum, int maxMsgNums, long maxMsgSize, int bufferTotal,
                                   int messageTotal, boolean isInDisk) {

        //At least has one message(batch)
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (messageTotal + unitBatchNum > maxMsgNums) {
            return true;
        }

        if (bufferTotal + sizePy > maxMsgSize) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }
            if (messageTotal + unitBatchNum > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk()) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal + unitBatchNum > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory()) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        DefaultMappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    private void addScheduleTask() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                StreamMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                StreamMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!getMessageStoreConfig().isMappedFileSwapEnable()) {
                        log.warn("Swap is not enabled.");
                        return ;
                    }
                    StreamMessageStore.this.commitLog.swapMap(getMessageStoreConfig().getCommitLogSwapMapReserveFileNum(),
                            getMessageStoreConfig().getCommitLogForceSwapMapInterval(), getMessageStoreConfig().getCommitLogSwapMapInterval());
                    for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : StreamMessageStore.this.consumeQueueTable.values()) {
                        for (ConsumeQueueInterface logic : maps.values()) {
                            StreamMessageStore.this.consumeQueueStore.swapMap(logic, getMessageStoreConfig().getLogicQueueSwapMapReserveFileNum(),
                                    getMessageStoreConfig().getLogicQueueForceSwapMapInterval(), getMessageStoreConfig().getLogicQueueSwapMapInterval());
                        }
                    }
                } catch (Exception e) {
                    log.error("swap map exception", e);
                }
            }
        }, 1, 5, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    StreamMessageStore.this.commitLog.cleanSwappedMap(getMessageStoreConfig().getCleanSwapedMapInterval());
                    for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : StreamMessageStore.this.consumeQueueTable.values()) {
                        for (ConsumeQueueInterface logic : maps.values()) {
                            StreamMessageStore.this.consumeQueueStore.cleanSwappedMap(logic, getMessageStoreConfig().getCleanSwapedMapInterval());
                        }
                    }
                } catch (Exception e) {
                    log.error("clean swap map exception", e);
                }
            }
        }, 1, 5, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (StreamMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (StreamMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - StreamMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + StreamMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // StreamMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
        this.correctLogicOffsetService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueueInterface>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueueInterface> cq = itNext.next();
                this.consumeQueueStore.checkSelf(cq.getValue());
            }
        }
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    protected boolean loadBatchConsumeQueue() {
        checkOtherConsumeQueue();

        File dirLogic = new File(StorePathConfigHelper.getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ConsumeQueueInterface logic = new BatchConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.getMessageStoreConfig().getMapperFileSizeBatchConsumeQueue(),
                                this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!this.consumeQueueStore.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    private void checkOtherConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        if (dirLogic.exists()) {
            throw new RuntimeException(format("Consume queue directory: [%s] exist. Can not load batch consume queue while consume queue exists.",
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir())));
        }
    }

    private void recover(final boolean lastExitOK) {
        long recoverCqStart = System.currentTimeMillis();
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();
        long recoverCqEnd = System.currentTimeMillis();

        if (lastExitOK) {
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }
        long recoverClogEnd = System.currentTimeMillis();
        this.recoverTopicQueueTable();
        long recoverOffsetEnd = System.currentTimeMillis();

        log.info("Recover end total:{} recoverCq:{} recoverClog:{} recoverOffset:{}",
                recoverOffsetEnd - recoverCqStart, recoverCqEnd - recoverCqStart, recoverClogEnd - recoverCqEnd, recoverOffsetEnd - recoverClogEnd);
    }

    @Override
    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    @Override
    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueueInterface>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.consumeQueueStore.recover(logic);
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }

    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                this.consumeQueueStore.correctMinOffset(logic, minPhyOffset);
            }
        }

        this.batchTopicQueueTable = table;
    }

    @Override
    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = StreamMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.consumeQueueStore.truncateDirtyLogicFiles(logic, phyOffset);
            }
        }
    }

    @Override
    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return consumeQueueTable;
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
    public void registerCleanFileHook(CleanFilesHook logicalQueueCleanHook) {

    }

    @Override
    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    @Override
    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    public void initAsyncReputThreads(int tsNum, int cacheNum) {
        if (tsNum <= 0) {
            tsNum = 1;
        }
        if (cacheNum < 512) {
            cacheNum = 512;
        }
        reputExecutors = new ThreadPoolExecutor[tsNum];
        reputQueues = new BlockingQueue[tsNum];

        for (int i = 0; i < tsNum; i++) {
            final int tmp = i;
            reputQueues[i] = new LinkedBlockingDeque<>(cacheNum);
            //Each executor can only have one thread, otherwise the cq index will get wrong
            reputExecutors[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    reputQueues[i],
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "MQDispatchThread-" + tmp);
                        }
                    });
        }
        for (ThreadPoolExecutor executorService: reputExecutors) {
            if (executorService.getMaximumPoolSize() != 1 ||
                    executorService.getCorePoolSize() != 1) {
                throw new RuntimeException("The MQDispatchThreadPoll can only have one thread");
            }
        }

    }

    public Future doDispatch(final DispatchRequest request) {
        return doDispatch(request, false);
    }

    public Future doDispatch(final DispatchRequest request, boolean async) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                for (CommitLogDispatcher dispatcher : StreamMessageStore.this.dispatcherList) {
                    dispatcher.dispatch(request);
                }
            }
        };
        if (!async) {
            task.run();
            return EMPTY_FUTURE;
        }
        int hash = Math.abs((request.getTopic() + request.getQueueId()).hashCode());
        int slot = hash % reputExecutors.length;
        try {
            return reputExecutors[slot].submit(task);
        } catch (RejectedExecutionException ignored) {
            int tryNum = 0;
            while (tryNum++ < Integer.MAX_VALUE) {
                try {
                    Thread.sleep(1);
                } catch (Throwable ignored2) {

                }
                try {
                    return reputExecutors[slot].submit(task);
                } catch (RejectedExecutionException e) {
                    log.warn("DispatchReject topic:{} queue:{} pyOffset:{} tryNum:{}", request.getTopic(), request.getQueueId(), request.getCommitLogOffset(), tryNum,  e);
                }
            }
        }
        return EMPTY_FUTURE;
    }

    public void syncProcessDispatchRequest(DispatchRequest request, boolean isRecover) throws InterruptedException {
        if (!isDispatchFromSenderThread) {
            log.error("addDispatchRequestQueue operation not supported while isCreateDispatchRequestAsync is true");
        } else {
            if (isRecover) {
                ((SyncReputMessageService) this.reputMessageService).processDispatchRequestForRecover(request);
            } else {
                ((SyncReputMessageService) this.reputMessageService).processDispatchRequest(request);
            }
        }
    }

    public boolean dispatched(long physicalOffset) {
        return reputMessageService.dispatched(physicalOffset);
    }

    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        ConsumeQueueInterface cq = this.findOrCreateConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        this.consumeQueueStore.putMessagePositionInfoWrapper(cq, dispatchRequest);
    }

    private ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        return this.consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
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
    public ConsumeQueueStore getQueueStore() {
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

    @Override
    public void assignOffset(String topicQueueKey, MessageExtBrokerInner msg, short batchNum) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            assignOffsetForCq(topicQueueKey, msg);
            assignOffsetForBcq(topicQueueKey, msg, batchNum);
        }
    }

    private void assignOffsetForCq(String topicQueueKey, MessageExtBrokerInner msg) {
        // not supported yet
    }

    private void assignOffsetForBcq(String topicQueueKey, MessageExtBrokerInner msg, short batchNum) {
        Long batchTopicOffset = this.batchTopicQueueTable.computeIfAbsent(topicQueueKey, k -> 0L);
        CQType cqType = QueueTypeUtils.getCQType(this);
        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_BASE, String.valueOf(batchTopicOffset));
            msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        }
        msg.setQueueOffset(batchTopicOffset);
        this.batchTopicQueueTable.put(topicQueueKey, batchTopicOffset + batchNum);
    }

    @Override
    public void removeOffsetTable(String topicQueueKey) {
        this.topicQueueTable.remove(topicQueueKey);
        this.batchTopicQueueTable.remove(topicQueueKey);
    }

    @Override
    public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        DispatchRequest dispatchRequest;
        switch (result.getStatus()) {
            case PUT_OK:
                dispatchRequest = constructDispatchRequest(msg, result);
                onCommitLogDispatch(dispatchRequest, this.isDispatchFromSenderThread(), commitLogFile, false, false);
                break;
            case END_OF_FILE:
                dispatchRequest = new DispatchRequest(0, true);
                onCommitLogDispatch(dispatchRequest, this.isDispatchFromSenderThread(), commitLogFile, false, true);
                break;
            default:
                throw new RuntimeException("");
        }
    }

    @Override
    public void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile, boolean isRecover, boolean isFileEnd) {
        if (isFileEnd) {
            if (doDispatch) {
                long nextReputFromOffset = this.getCommitLog().rollNextFile(commitLogFile.getFileFromOffset());
                dispatchRequest.setNextReputFromOffset(nextReputFromOffset);
                syncDispatch(dispatchRequest, isRecover);
            }
        } else {
            if (doDispatch) {
                dispatchRequest.setNextReputFromOffset(dispatchRequest.getCommitLogOffset() + dispatchRequest.getMsgSize());
                syncDispatch(dispatchRequest, isRecover);
            }
        }
    }

    private DispatchRequest constructDispatchRequest(MessageExtBrokerInner msg, AppendMessageResult appendResult) {
        long tagsCode = 0;
        String keys = "";
        String uniqKey = null;
        int sysFlag = msg.getSysFlag();
        String topic = msg.getTopic();
        long storeTimestamp = msg.getStoreTimestamp();
        int queueId = msg.getQueueId();
        long preparedTransactionOffset = msg.getPreparedTransactionOffset();
        Map<String, String> propertiesMap = msg.getProperties();

        if (msg.getProperties() != null && msg.getProperties().size() > 0) {

            keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

            uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

            String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
            if (tags != null && tags.length() > 0) {
                tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
            }

            // Timing message processing
            {
                String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                    int delayLevel = Integer.parseInt(t);

                    if (delayLevel > this.getScheduleMessageService().getMaxDelayLevel()) {
                        delayLevel = this.getScheduleMessageService().getMaxDelayLevel();
                    }

                    if (delayLevel > 0) {
                        tagsCode = this.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                    }
                }
            }
        }

        DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                queueId,
                appendResult.getWroteOffset(),
                appendResult.getWroteBytes(),
                tagsCode,
                storeTimestamp,
                appendResult.getLogicsOffset(),
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
        );

        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM) && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
        return dispatchRequest;
    }

    private void syncDispatch(DispatchRequest dispatchRequest, boolean isRecover) {
        try {
            this.syncProcessDispatchRequest(dispatchRequest, isRecover);
        } catch (InterruptedException e) {
            log.error("OnCommitlogAppend sync dispatch failed, addDispatchRequestQueue interrupted. DispatchRequest:{}", dispatchRequest);
        }
    }

    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest dispatchRequest) {
            final int tranType = MessageSysFlag.getTransactionValue(dispatchRequest.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    StreamMessageStore.this.putMessagePositionInfo(dispatchRequest);
                    if (BrokerRole.SLAVE != StreamMessageStore.this.getMessageStoreConfig().getBrokerRole()
                            && StreamMessageStore.this.brokerConfig.isLongPollingEnable()) {
                        StreamMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                    }
                    if (StreamMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                        StreamMessageStore.this.storeStatsService
                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
                        StreamMessageStore.this.storeStatsService
                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                .add(dispatchRequest.getMsgSize());
                    }
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
            if (StreamMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                StreamMessageStore.this.indexService.buildIndex(request);
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

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        double getDiskSpaceWarningLevelRatio() {
            double finalDiskSpaceWarningLevelRatio;
            if ("".equals(diskSpaceWarningLevelRatio)) {
                finalDiskSpaceWarningLevelRatio = StreamMessageStore.this.getMessageStoreConfig().getDiskSpaceWarningLevelRatio() / 100.0;
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
                finalDiskSpaceCleanForciblyRatio = StreamMessageStore.this.getMessageStoreConfig().getDiskSpaceCleanForciblyRatio() / 100.0;
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

        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            StreamMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public long run() {
            int deleteCount = 0;
            try {
                deleteCount = this.deleteExpiredFiles();

                this.redeleteHangedFile();
            } catch (Throwable e) {
                StreamMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
            return deleteCount;
        }

        private int deleteExpiredFiles() {
            int deleteCount = 0;
            long fileReservedTime = StreamMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            int deletePhysicFilesInterval = StreamMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            int destroyMapedFileIntervalForcibly = StreamMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
            int maxBatchDeleteFilesNum = StreamMessageStore.this.getMessageStoreConfig().getMaxBatchDeleteFilesNum();
            if (maxBatchDeleteFilesNum < 10) {
                maxBatchDeleteFilesNum = 10;
            }

            boolean timeup = this.isTimeToDelete();
            boolean spacefull = this.isSpaceToDelete();
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            boolean needDelete = timeup || spacefull || manualDelete;

            if (needDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = StreamMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                String storePathPhysic = StreamMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                long totalSpace = UtilAll.getTotalSpace(storePathPhysic);
                double realRatio = (StreamMessageStore.this.commitLog.getMaxOffset() - StreamMessageStore.this.commitLog.getMinOffset()) / (totalSpace + 0.001);

                cleanAtOnce = cleanAtOnce && (realRatio > 0.3);

                log.info("begin to delete before {} hours file. timeup:{} spacefull:{} manualDeleteFileSeveralTimes:{} cleanAtOnce:{} maxBatchDeleteFilesNum:{} physicRatio:{}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce,
                        maxBatchDeleteFilesNum,
                        physicRatio);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = StreamMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed. timeup:{} manualDelete:{} cleanAtOnce:{} physicRatio:{}",
                            timeup,
                            manualDelete,
                            cleanAtOnce,
                            physicRatio);
                }
            }
            return deleteCount;
        }

        private void redeleteHangedFile() {
            int interval = StreamMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        StreamMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (StreamMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        private boolean isTimeToDelete() {
            String when = StreamMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                StreamMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        private boolean isSpaceToDelete() {
            double ratio = StreamMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
                if (physicRatio > getDiskSpaceWarningLevelRatio()) {
                    boolean diskok = StreamMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        StreamMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = StreamMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        StreamMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    StreamMessageStore.log.info("physic disk maybe full soon, so reclaim space, {}, cleanImmediately {}", physicRatio, cleanImmediately);
                    return true;
                }
            }

            {
                String storePathLogics = StorePathConfigHelper
                        .getStorePathConsumeQueue(StreamMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > getDiskSpaceWarningLevelRatio()) {
                    boolean diskok = StreamMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        StreamMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > getDiskSpaceCleanForciblyRatio()) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = StreamMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        StreamMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    StreamMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                StreamMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval = StreamMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = StreamMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = StreamMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                    for (ConsumeQueueInterface logic : maps.values()) {
                        int deleteCount = StreamMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                StreamMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class CorrectLogicOffsetService {

        private long lastForceCorrectTime = -1L;

        public void run() {
            try {
                this.correctLogicMinOffset();
            } catch (Throwable e) {
                StreamMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private boolean needCorrect(ConsumeQueueInterface logic, long minPhyOffset, long lastForeCorrectTimeCurRun) {
            if (logic == null) {
                return false;
            }
            // If first exist and not available, it means first file may destroy failed, delete it.
            if (StreamMessageStore.this.consumeQueueStore.isFirstFileExist(logic) && !StreamMessageStore.this.consumeQueueStore.isFirstFileAvailable(logic)) {
                log.error("CorrectLogicOffsetService.needCorrect. first file not available, trigger correct." +
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
                    log.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is less than min phy offset: {}, " +
                                    "but min offset: {} is less than max offset: {}. topic:{}, queue:{}, cqType:{}."
                            , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                } else if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                    return false;
                } else {
                    log.error("CorrectLogicOffsetService.needCorrect. It should not happen, logic max phy offset: {} is less than min phy offset: {}," +
                                    " but min offset: {} is larger than max offset: {}. topic:{}, queue:{}, cqType:{}"
                            , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return false;
                }
            }
            //the logic.getMaxPhysicOffset() >= minPhyOffset
            int forceCorrectInterval = StreamMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetForceInterval();
            if ((System.currentTimeMillis() - lastForeCorrectTimeCurRun) > forceCorrectInterval) {
                lastForceCorrectTime = System.currentTimeMillis();
                CqUnit cqUnit = logic.getEarliestUnit();
                if (cqUnit == null) {
                    if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                        return false;
                    } else {
                        log.error("CorrectLogicOffsetService.needCorrect. cqUnit is null, logic max phy offset: {} is greater than min phy offset: {}, " +
                                        "but min offset: {} is not equal to max offset: {}. topic:{}, queue:{}, cqType:{}."
                                , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                                , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                        return true;
                    }
                }

                if (cqUnit.getPos() < minPhyOffset) {
                    log.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is greater than min phy offset: {}, " +
                                    "but minPhyPos in cq is: {}. min offset in queue: {}, max offset in queue: {}, topic:{}, queue:{}, cqType:{}."
                            , logic.getMaxPhysicOffset(), minPhyOffset, cqUnit.getPos(), logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                }

                if (cqUnit.getPos() >= minPhyOffset) {

                    // Normal case, do not need correct.
                    return false;
                }
            }

            return false;
        }

        private void correctLogicMinOffset() {

            long lastForeCorrectTimeCurRun = lastForceCorrectTime;
            long minPhyOffset = getMinPhyOffset();
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = StreamMessageStore.this.consumeQueueTable;
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface logic : maps.values()) {
                    if (needCorrect(logic, minPhyOffset, lastForeCorrectTimeCurRun)) {
                        doCorrect(logic, minPhyOffset);
                    }
                }
            }
        }

        private void doCorrect(ConsumeQueueInterface logic, long minPhyOffset) {
            StreamMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minPhyOffset);
            int sleepIntervalWhenCorrectMinOffset = StreamMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetSleepInterval();
            if (sleepIntervalWhenCorrectMinOffset > 0) {
                try {
                    Thread.sleep(sleepIntervalWhenCorrectMinOffset);
                } catch (InterruptedException ignored) {
                }
            }
        }

        public String getServiceName() {
            return CorrectLogicOffsetService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = StreamMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = StreamMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = StreamMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = StreamMessageStore.this.consumeQueueTable;

            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = StreamMessageStore.this.consumeQueueStore.flush(cq, flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    StreamMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                StreamMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        @Override
        public void run() {
            StreamMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = StreamMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    StreamMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            StreamMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    private class SyncReputMessageServiceFutureItem {
        private Future future;
        private long nextReputFromOffset;

        public SyncReputMessageServiceFutureItem(Future future, long nextReputFromOffset) {
            this.future = future;
            this.nextReputFromOffset = nextReputFromOffset;
        }

        public Future getFuture() {
            return future;
        }

        public void setFuture(Future future) {
            this.future = future;
        }

        public long getNextReputFromOffset() {
            return nextReputFromOffset;
        }

        public void setNextReputFromOffset(long nextReputFromOffset) {
            this.nextReputFromOffset = nextReputFromOffset;
        }
    }

    class SyncReputMessageService extends ReputMessageService {

        private BlockingQueue<SyncReputMessageServiceFutureItem> reputResultQueue;

        SyncReputMessageService() {
            reputResultQueue = new LinkedBlockingDeque<>(messageStoreConfig.getDispatchCqThreads() * messageStoreConfig.getDispatchCqCacheNum());
        }

        void processDispatchRequestForRecover(DispatchRequest dispatchRequest) {
            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();
            if (dispatchRequest.isSuccess() && size > 0) {
                StreamMessageStore.this.doDispatch(dispatchRequest);
                this.reputFromOffset = Math.max(dispatchRequest.getNextReputFromOffset(), this.reputFromOffset);
            }
        }

        void processDispatchRequest(DispatchRequest dispatchRequest) throws InterruptedException {
            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();
            if (dispatchRequest.isSuccess() && size > 0) {
                Future future = StreamMessageStore.this.doDispatch(dispatchRequest, messageStoreConfig.isEnableAsyncReput());
                reputResultQueue.put(new SyncReputMessageServiceFutureItem(future, dispatchRequest.getNextReputFromOffset()));
            }
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 30 && this.reputResultQueue.size() > 0; i++) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.reputResultQueue.size() > 0) {
                log.warn("shutdown SyncReputMessageService, but reputResultQueue not all processed, CLMaxOffset: {} reputFromOffset: {}",
                        StreamMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }
        @Override
        public void run() {
            StreamMessageStore.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    SyncReputMessageServiceFutureItem item = reputResultQueue.poll(1, TimeUnit.SECONDS);
                    if (null == item) {
                        continue;
                    }
                    item.getFuture().get();
                    this.reputFromOffset = Math.max(item.getNextReputFromOffset(), this.reputFromOffset);

                } catch (Exception e) {
                    StreamMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                    waitForRunning(100);
                }
            }

            StreamMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return SyncReputMessageService.class.getSimpleName();
        }
    }

    class ReputMessageService extends ServiceThread {

        protected volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        public boolean dispatched(long physicalOffset) {
            return this.reputFromOffset > physicalOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 30 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        StreamMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        public long behind() {
            return StreamMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        protected boolean isCommitLogAvailable() {
            return this.reputFromOffset < StreamMessageStore.this.commitLog.getMaxOffset();
        }

        private void doReput() throws Exception {
            if (this.reputFromOffset < StreamMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                        this.reputFromOffset, StreamMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = StreamMessageStore.this.commitLog.getMinOffset();
            }
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                if (StreamMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                        && this.reputFromOffset >= StreamMessageStore.this.getConfirmOffset()) {
                    break;
                }

                SelectMappedBufferResult result = StreamMessageStore.this.commitLog.getData(reputFromOffset);
                boolean reputAsync = messageStoreConfig.isEnableAsyncReput();
                if (result != null) {
                    long cacheReputFromOffset = this.reputFromOffset;
                    List<Future> futures = new LinkedList<>();
                    try {
                        this.reputFromOffset = result.getStartOffset();
                        cacheReputFromOffset = this.reputFromOffset;

                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            DispatchRequest dispatchRequest =
                                    StreamMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    futures.add(StreamMessageStore.this.doDispatch(dispatchRequest, reputAsync));

                                    if (BrokerRole.SLAVE != StreamMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && StreamMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                        StreamMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }

                                    cacheReputFromOffset += size;
                                    readSize += size;
                                    if (StreamMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        StreamMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
                                        StreamMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .add(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    cacheReputFromOffset = StreamMessageStore.this.commitLog.rollNextFile(cacheReputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {

                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", cacheReputFromOffset);
                                    cacheReputFromOffset += size;
                                } else {
                                    doNext = false;
                                    if (StreamMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                cacheReputFromOffset);
                                        cacheReputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                        for (Future future: futures) {
                            future.get();
                        }
                        futures.clear();
                        this.reputFromOffset = cacheReputFromOffset;
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        @Override
        public void run() {
            StreamMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    StreamMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            StreamMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
