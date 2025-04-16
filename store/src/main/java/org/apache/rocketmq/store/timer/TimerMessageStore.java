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
package org.apache.rocketmq.store.timer;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.util.PerfCounter;

public class TimerMessageStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;
    private volatile int state = INITIAL;

    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final String TIMER_OUT_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
    public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
    public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    public static final String TIMER_DELETE_UNIQUE_KEY = MessageConst.PROPERTY_TIMER_DEL_UNIQKEY;

    public static final Random RANDOM = new Random();
    public static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PUT_NO_RETRY = 2;
    public static final int DAY_SECS = 24 * 3600;
    public static final int DEFAULT_CAPACITY = 1024;

    // The total days in the timer wheel when precision is 1000ms.
    // If the broker shutdown last more than the configured days, will cause message loss
    public static final int TIMER_WHEEL_TTL_DAY = 7;
    public static final int TIMER_BLANK_SLOTS = 60;
    public static final int MAGIC_DEFAULT = 1;
    public static final int MAGIC_ROLL = 1 << 1;
    public static final int MAGIC_DELETE = 1 << 2;
    public boolean debug = false;

    protected static final String ENQUEUE_PUT = "enqueue_put";
    protected static final String DEQUEUE_PUT = "dequeue_put";
    protected final PerfCounter.Ticks perfCounterTicks = new PerfCounter.Ticks(LOGGER);

    protected final BlockingQueue<TimerRequest> enqueuePutQueue;
    protected final BlockingQueue<List<TimerRequest>> dequeueGetQueue;
    protected final BlockingQueue<TimerRequest> dequeuePutQueue;

    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(4 * 1024);
    private final ThreadLocal<ByteBuffer> bufferLocal;
    private final ScheduledExecutorService scheduler;

    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;

    private TimerEnqueueGetService enqueueGetService;
    private TimerEnqueuePutService enqueuePutService;
    private TimerDequeueWarmService dequeueWarmService;
    private TimerDequeueGetService dequeueGetService;
    private TimerDequeuePutMessageService[] dequeuePutMessageServices;
    private TimerDequeueGetMessageService[] dequeueGetMessageServices;
    private TimerFlushService timerFlushService;

    protected volatile long currReadTimeMs;
    protected volatile long currWriteTimeMs;
    protected volatile long preReadTimeMs;
    protected volatile long commitReadTimeMs;
    protected volatile long currQueueOffset; //only one queue that is 0
    protected volatile long commitQueueOffset;
    protected volatile long lastCommitReadTimeMs;
    protected volatile long lastCommitQueueOffset;

    private long lastEnqueueButExpiredTime;
    private long lastEnqueueButExpiredStoreTime;

    private final int commitLogFileSize;
    private final int timerLogFileSize;
    private final int timerRollWindowSlots;
    private final int slotsTotal;

    protected final int precisionMs;
    protected final MessageStoreConfig storeConfig;
    protected TimerMetrics timerMetrics;
    protected long lastTimeOfCheckMetrics = System.currentTimeMillis();
    protected AtomicInteger frequency = new AtomicInteger(0);

    private volatile BrokerRole lastBrokerRole = BrokerRole.SLAVE;
    //the dequeue is an asynchronous process, use this flag to track if the status has changed
    private boolean dequeueStatusChangeFlag = false;
    private long shouldStartTime;

    // True if current store is master or current brokerId is equal to the minimum brokerId of the replica group in slaveActingMaster mode.
    protected volatile boolean shouldRunningDequeue;
    private final BrokerStatsManager brokerStatsManager;
    private Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook;

    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
        TimerCheckpoint timerCheckpoint, TimerMetrics timerMetrics,
        final BrokerStatsManager brokerStatsManager) throws IOException {

        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        this.commitLogFileSize = storeConfig.getMappedFileSizeCommitLog();
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
        this.precisionMs = storeConfig.getTimerPrecisionMs();

        // TimerWheel contains the fixed number of slots regardless of precision.
        this.slotsTotal = TIMER_WHEEL_TTL_DAY * DAY_SECS;
        this.timerWheel = new TimerWheel(
            getTimerWheelPath(storeConfig.getStorePathRootDir()), this.slotsTotal, precisionMs);
        this.timerLog = new TimerLog(getTimerLogPath(storeConfig.getStorePathRootDir()), timerLogFileSize);
        this.timerMetrics = timerMetrics;
        this.timerCheckpoint = timerCheckpoint;
        this.lastBrokerRole = storeConfig.getBrokerRole();

        if (messageStore instanceof DefaultMessageStore) {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("TimerScheduledThread",
                    ((DefaultMessageStore) messageStore).getBrokerIdentity()));
        } else {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("TimerScheduledThread"));
        }

        // timerRollWindow contains the fixed number of slots regardless of precision.
        if (storeConfig.getTimerRollWindowSlot() > slotsTotal - TIMER_BLANK_SLOTS
            || storeConfig.getTimerRollWindowSlot() < 2) {
            this.timerRollWindowSlots = slotsTotal - TIMER_BLANK_SLOTS;
        } else {
            this.timerRollWindowSlots = storeConfig.getTimerRollWindowSlot();
        }

        bufferLocal = new ThreadLocal<ByteBuffer>() {
            @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocateDirect(storeConfig.getMaxMessageSize() + 100);
            }
        };

        if (storeConfig.isTimerEnableDisruptor()) {
            enqueuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            dequeueGetQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            dequeuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
        } else {
            enqueuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            dequeueGetQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            dequeuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        }
        this.brokerStatsManager = brokerStatsManager;
    }

    public void initService() {
        enqueueGetService = new TimerEnqueueGetService();
        enqueuePutService = new TimerEnqueuePutService();
        dequeueWarmService = new TimerDequeueWarmService();
        dequeueGetService = new TimerDequeueGetService();
        timerFlushService = new TimerFlushService();

        int getThreadNum = Math.max(storeConfig.getTimerGetMessageThreadNum(), 1);
        dequeueGetMessageServices = new TimerDequeueGetMessageService[getThreadNum];
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i] = new TimerDequeueGetMessageService();
        }

        int putThreadNum = Math.max(storeConfig.getTimerPutMessageThreadNum(), 1);
        dequeuePutMessageServices = new TimerDequeuePutMessageService[putThreadNum];
        for (int i = 0; i < dequeuePutMessageServices.length; i++) {
            dequeuePutMessageServices[i] = new TimerDequeuePutMessageService();
        }
    }

    public boolean load() {
        this.initService();
        boolean load = timerLog.load();
        load = load && this.timerMetrics.load();
        recover();
        calcTimerDistribution();
        return load;
    }

    public static String getTimerWheelPath(final String rootDir) {
        return rootDir + File.separator + "timerwheel";
    }

    public static String getTimerLogPath(final String rootDir) {
        return rootDir + File.separator + "timerlog";
    }

    private void calcTimerDistribution() {
        long startTime = System.currentTimeMillis();
        List<Integer> timerDist = this.timerMetrics.getTimerDistList();
        long currTime = System.currentTimeMillis() / precisionMs * precisionMs;
        for (int i = 0; i < timerDist.size(); i++) {
            int slotBeforeNum = i == 0 ? 0 : timerDist.get(i - 1) * 1000 / precisionMs;
            int slotTotalNum = timerDist.get(i) * 1000 / precisionMs;
            int periodTotal = 0;
            for (int j = slotBeforeNum; j < slotTotalNum; j++) {
                Slot slotEach = timerWheel.getSlot(currTime + (long) j * precisionMs);
                periodTotal += slotEach.num;
            }
            LOGGER.debug("{} period's total num: {}", timerDist.get(i), periodTotal);
            this.timerMetrics.updateDistPair(timerDist.get(i), periodTotal);
        }
        long endTime = System.currentTimeMillis();
        LOGGER.debug("Total cost Time: {}", endTime - startTime);
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void recover() {
        //recover timerLog
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        if (lastFlushPos < 0) {
            lastFlushPos = 0;
        }
        long processOffset = recoverAndRevise(lastFlushPos, true);

        timerLog.getMappedFileQueue().setFlushedWhere(processOffset);
        //revise queue offset
        long queueOffset = reviseQueueOffset(processOffset);
        if (-1 == queueOffset) {
            currQueueOffset = timerCheckpoint.getLastTimerQueueOffset();
        } else {
            currQueueOffset = queueOffset + 1;
        }
        currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());

        ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, 0);

        // Correction based consume queue
        if (cq != null && currQueueOffset < cq.getMinOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                currQueueOffset, cq.getMinOffsetInQueue());
            currQueueOffset = cq.getMinOffsetInQueue();
        } else if (cq != null && currQueueOffset > cq.getMaxOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is larger than maxOffsetInQueue:{}",
                currQueueOffset, cq.getMaxOffsetInQueue());
            currQueueOffset = cq.getMaxOffsetInQueue();
        }

        //check timer wheel
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        long nextReadTimeMs = formatTimeMs(
            System.currentTimeMillis()) - (long) slotsTotal * precisionMs + (long) TIMER_BLANK_SLOTS * precisionMs;
        if (currReadTimeMs < nextReadTimeMs) {
            currReadTimeMs = nextReadTimeMs;
        }
        //the timer wheel may contain physical offset bigger than timerLog
        //This will only happen when the timerLog is damaged
        //hard to test
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs, processOffset);
        if (debug) {
            minFirst = 0;
        }
        if (minFirst < processOffset) {
            LOGGER.warn("Timer recheck because of minFirst:{} processOffset:{}", minFirst, processOffset);
            recoverAndRevise(minFirst, false);
        }
        LOGGER.info("Timer recover ok currReadTimerMs:{} currQueueOffset:{} checkQueueOffset:{} processOffset:{}",
            currReadTimeMs, currQueueOffset, timerCheckpoint.getLastTimerQueueOffset(), processOffset);

        commitReadTimeMs = currReadTimeMs;
        commitQueueOffset = currQueueOffset;

        prepareTimerCheckPoint();
    }

    public long reviseQueueOffset(long processOffset) {
        SelectMappedBufferResult selectRes = timerLog.getTimerMessage(processOffset - (TimerLog.UNIT_SIZE - TimerLog.UNIT_PRE_SIZE_FOR_MSG));
        if (null == selectRes) {
            return -1;
        }
        try {
            long offsetPy = selectRes.getByteBuffer().getLong();
            int sizePy = selectRes.getByteBuffer().getInt();
            MessageExt messageExt = getMessageByCommitOffset(offsetPy, sizePy);
            if (null == messageExt) {
                return -1;
            }

            // check offset in msg is equal to offset of cq.
            // if not, use cq offset.
            long msgQueueOffset = messageExt.getQueueOffset();
            int queueId = messageExt.getQueueId();
            ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
            if (null == cq) {
                return msgQueueOffset;
            }
            long cqOffset = msgQueueOffset;
            long tmpOffset = msgQueueOffset;
            int maxCount = 20000;
            while (maxCount-- > 0) {
                if (tmpOffset < 0) {
                    LOGGER.warn("reviseQueueOffset check cq offset fail, msg in cq is not found.{}, {}",
                        offsetPy, sizePy);
                    break;
                }
                ReferredIterator<CqUnit> iterator = null;
                try {
                    iterator = cq.iterateFrom(tmpOffset);
                    CqUnit cqUnit = null;
                    if (null == iterator || (cqUnit = iterator.next()) == null) {
                        // offset in msg may be greater than offset of cq.
                        tmpOffset -= 1;
                        continue;
                    }

                    long offsetPyTemp = cqUnit.getPos();
                    int sizePyTemp = cqUnit.getSize();
                    if (offsetPyTemp == offsetPy && sizePyTemp == sizePy) {
                        LOGGER.info("reviseQueueOffset check cq offset ok. {}, {}, {}",
                            tmpOffset, offsetPyTemp, sizePyTemp);
                        cqOffset = tmpOffset;
                        break;
                    }
                    tmpOffset -= 1;
                } catch (Throwable e) {
                    LOGGER.error("reviseQueueOffset check cq offset error.", e);
                } finally {
                    if (iterator != null) {
                        iterator.release();
                    }
                }
            }

            return cqOffset;
        } finally {
            selectRes.release();
        }
    }

    //recover timerLog and revise timerWheel
    //return process offset
    private long recoverAndRevise(long beginOffset, boolean checkTimerLog) {
        LOGGER.info("Begin to recover timerLog offset:{} check:{}", beginOffset, checkTimerLog);
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null == lastFile) {
            return 0;
        }

        List<MappedFile> mappedFiles = timerLog.getMappedFileQueue().getMappedFiles();
        int index = mappedFiles.size() - 1;
        for (; index >= 0; index--) {
            MappedFile mappedFile = mappedFiles.get(index);
            if (beginOffset >= mappedFile.getFileFromOffset()) {
                break;
            }
        }
        if (index < 0) {
            index = 0;
        }
        long checkOffset = mappedFiles.get(index).getFileFromOffset();
        for (; index < mappedFiles.size(); index++) {
            MappedFile mappedFile = mappedFiles.get(index);
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, checkTimerLog ? mappedFiles.get(index).getFileSize() : mappedFile.getReadPosition());
            ByteBuffer bf = sbr.getByteBuffer();
            int position = 0;
            boolean stopCheck = false;
            for (; position < sbr.getSize(); position += TimerLog.UNIT_SIZE) {
                try {
                    bf.position(position);
                    int size = bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt();
                    if (magic == TimerLog.BLANK_MAGIC_CODE) {
                        break;
                    }
                    if (checkTimerLog && (!isMagicOK(magic) || TimerLog.UNIT_SIZE != size)) {
                        stopCheck = true;
                        break;
                    }
                    long delayTime = bf.getLong() + bf.getInt();
                    if (TimerLog.UNIT_SIZE == size && isMagicOK(magic)) {
                        timerWheel.reviseSlot(delayTime, TimerWheel.IGNORE, sbr.getStartOffset() + position, true);
                    }
                } catch (Exception e) {
                    LOGGER.error("Recover timerLog error", e);
                    stopCheck = true;
                    break;
                }
            }
            sbr.release();
            checkOffset = mappedFiles.get(index).getFileFromOffset() + position;
            if (stopCheck) {
                break;
            }
        }
        if (checkTimerLog) {
            timerLog.getMappedFileQueue().truncateDirtyFiles(checkOffset);
        }
        return checkOffset;
    }

    public static boolean isMagicOK(int magic) {
        return (magic | 0xF) == 0xF;
    }

    public void start() {
        this.shouldStartTime = storeConfig.getDisappearTimeAfterStart() + System.currentTimeMillis();
        maybeMoveWriteTime();
        enqueueGetService.start();
        enqueuePutService.start();
        dequeueWarmService.start();
        dequeueGetService.start();
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i].start();
        }
        for (int i = 0; i < dequeuePutMessageServices.length; i++) {
            dequeuePutMessageServices[i].start();
        }
        timerFlushService.start();

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    long minPy = messageStore.getMinPhyOffset();
                    int checkOffset = timerLog.getOffsetForLastUnit();
                    timerLog.getMappedFileQueue()
                        .deleteExpiredFileByOffsetForTimerLog(minPy, checkOffset, TimerLog.UNIT_SIZE);
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (storeConfig.isTimerEnableCheckMetrics()) {
                        String when = storeConfig.getTimerCheckMetricsWhen();
                        if (!UtilAll.isItTimeToDo(when)) {
                            return;
                        }
                        long curr = System.currentTimeMillis();
                        if (curr - lastTimeOfCheckMetrics > 70 * 60 * 1000) {
                            lastTimeOfCheckMetrics = curr;
                            checkAndReviseMetrics();
                            LOGGER.info("[CheckAndReviseMetrics]Timer do check timer metrics cost {} ms",
                                System.currentTimeMillis() - curr);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 45, 45, TimeUnit.MINUTES);

        state = RUNNING;
        LOGGER.info("Timer start ok currReadTimerMs:[{}] queueOffset:[{}]", new Timestamp(currReadTimeMs), currQueueOffset);
    }

    public void start(boolean shouldRunningDequeue) {
        this.shouldRunningDequeue = shouldRunningDequeue;
        this.start();
    }

    public void shutdown() {
        if (SHUTDOWN == state) {
            return;
        }
        state = SHUTDOWN;
        //first save checkpoint
        prepareTimerCheckPoint();
        timerFlushService.shutdown();
        timerLog.shutdown();
        timerCheckpoint.shutdown();

        enqueuePutQueue.clear(); //avoid blocking
        dequeueGetQueue.clear(); //avoid blocking
        dequeuePutQueue.clear(); //avoid blocking

        enqueueGetService.shutdown();
        enqueuePutService.shutdown();
        dequeueWarmService.shutdown();
        dequeueGetService.shutdown();
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i].shutdown();
        }
        for (int i = 0; i < dequeuePutMessageServices.length; i++) {
            dequeuePutMessageServices[i].shutdown();
        }
        timerWheel.shutdown(false);

        this.scheduler.shutdown();
        UtilAll.cleanBuffer(this.bufferLocal.get());
        this.bufferLocal.remove();
    }

    protected void maybeMoveWriteTime() {
        if (currWriteTimeMs < formatTimeMs(System.currentTimeMillis())) {
            currWriteTimeMs = formatTimeMs(System.currentTimeMillis());
        }
    }

    private void moveReadTime() {
        currReadTimeMs = currReadTimeMs + precisionMs;
        commitReadTimeMs = currReadTimeMs;
    }

    private boolean isRunning() {
        return RUNNING == state;
    }

    private void checkBrokerRole() {
        BrokerRole currRole = storeConfig.getBrokerRole();
        if (lastBrokerRole != currRole) {
            synchronized (lastBrokerRole) {
                LOGGER.info("Broker role change from {} to {}", lastBrokerRole, currRole);
                //if change to master, do something
                if (BrokerRole.SLAVE != currRole) {
                    currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());
                    commitQueueOffset = currQueueOffset;
                    prepareTimerCheckPoint();
                    timerCheckpoint.flush();
                    currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
                    commitReadTimeMs = currReadTimeMs;
                }
                //if change to slave, just let it go
                lastBrokerRole = currRole;
            }
        }
    }

    private boolean isRunningEnqueue() {
        checkBrokerRole();
        if (!shouldRunningDequeue && !isMaster() && currQueueOffset >= timerCheckpoint.getMasterTimerQueueOffset()) {
            return false;
        }

        return isRunning();
    }

    private boolean isRunningDequeue() {
        if (!this.shouldRunningDequeue) {
            syncLastReadTimeMs();
            return false;
        }
        return isRunning();
    }

    public void syncLastReadTimeMs() {
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        commitReadTimeMs = currReadTimeMs;
    }

    public void setShouldRunningDequeue(final boolean shouldRunningDequeue) {
        this.shouldRunningDequeue = shouldRunningDequeue;
    }

    public boolean isShouldRunningDequeue() {
        return shouldRunningDequeue;
    }

    public void addMetric(MessageExt msg, int value) {
        try {
            if (null == msg || null == msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC)) {
                return;
            }
            if (msg.getProperty(TIMER_ENQUEUE_MS) != null
                && NumberUtils.toLong(msg.getProperty(TIMER_ENQUEUE_MS)) == Long.MAX_VALUE) {
                return;
            }
            // pass msg into addAndGet, for further more judgement extension.
            timerMetrics.addAndGet(msg, value);
        } catch (Throwable t) {
            if (frequency.incrementAndGet() % 1000 == 0) {
                LOGGER.error("error in adding metric", t);
            }
        }

    }

    public void holdMomentForUnknownError(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception ignored) {

        }
    }

    public void holdMomentForUnknownError() {
        holdMomentForUnknownError(50);
    }

    public boolean enqueue(int queueId) {
        if (storeConfig.isTimerStopEnqueue()) {
            return false;
        }
        if (!isRunningEnqueue()) {
            return false;
        }
        ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
        if (null == cq) {
            return false;
        }
        if (currQueueOffset < cq.getMinOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                currQueueOffset, cq.getMinOffsetInQueue());
            currQueueOffset = cq.getMinOffsetInQueue();
        }
        long offset = currQueueOffset;
        ReferredIterator<CqUnit> iterator = null;
        try {
            iterator = cq.iterateFrom(offset);
            if (null == iterator) {
                return false;
            }

            int i = 0;
            while (iterator.hasNext()) {
                i++;
                perfCounterTicks.startTick("enqueue_get");
                try {
                    CqUnit cqUnit = iterator.next();
                    long offsetPy = cqUnit.getPos();
                    int sizePy = cqUnit.getSize();
                    cqUnit.getTagsCode(); //tags code
                    MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                    if (null == msgExt) {
                        perfCounterTicks.getCounter("enqueue_get_miss");
                    } else {
                        lastEnqueueButExpiredTime = System.currentTimeMillis();
                        lastEnqueueButExpiredStoreTime = msgExt.getStoreTimestamp();
                        long delayedTime = Long.parseLong(msgExt.getProperty(TIMER_OUT_MS));
                        // use CQ offset, not offset in Message
                        msgExt.setQueueOffset(offset + i);
                        TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, System.currentTimeMillis(), MAGIC_DEFAULT, msgExt);
                        // System.out.printf("build enqueue request, %s%n", timerRequest);
                        while (!enqueuePutQueue.offer(timerRequest, 3, TimeUnit.SECONDS)) {
                            if (!isRunningEnqueue()) {
                                return false;
                            }
                        }
                        Attributes attributes = DefaultStoreMetricsManager.newAttributesBuilder()
                                .put(DefaultStoreMetricsConstant.LABEL_TOPIC, msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC)).build();
                        DefaultStoreMetricsManager.timerMessageSetLatency.record((delayedTime - msgExt.getBornTimestamp()) / 1000, attributes);
                    }
                } catch (Exception e) {
                    // here may cause the message loss
                    if (storeConfig.isTimerSkipUnknownError()) {
                        LOGGER.warn("Unknown error in skipped in enqueuing", e);
                    } else {
                        holdMomentForUnknownError();
                        throw e;
                    }
                } finally {
                    perfCounterTicks.endTick("enqueue_get");
                }
                // if broker role changes, ignore last enqueue
                if (!isRunningEnqueue()) {
                    return false;
                }
                currQueueOffset = offset + i;
            }
            currQueueOffset = offset + i;
            return i > 0;
        } catch (Exception e) {
            LOGGER.error("Unknown exception in enqueuing", e);
        } finally {
            if (iterator != null) {
                iterator.release();
            }
        }
        return false;
    }

    public boolean doEnqueue(long offsetPy, int sizePy, long delayedTime, MessageExt messageExt) {
        LOGGER.debug("Do enqueue [{}] [{}]", new Timestamp(delayedTime), messageExt);
        //copy the value first, avoid concurrent problem
        long tmpWriteTimeMs = currWriteTimeMs;
        boolean needRoll = delayedTime - tmpWriteTimeMs >= (long) timerRollWindowSlots * precisionMs;
        int magic = MAGIC_DEFAULT;
        if (needRoll) {
            magic = magic | MAGIC_ROLL;
            if (delayedTime - tmpWriteTimeMs - (long) timerRollWindowSlots * precisionMs < (long) timerRollWindowSlots / 3 * precisionMs) {
                //give enough time to next roll
                delayedTime = tmpWriteTimeMs + (long) (timerRollWindowSlots / 2) * precisionMs;
            } else {
                delayedTime = tmpWriteTimeMs + (long) timerRollWindowSlots * precisionMs;
            }
        }
        boolean isDelete = messageExt.getProperty(TIMER_DELETE_UNIQUE_KEY) != null;
        if (isDelete) {
            magic = magic | MAGIC_DELETE;
        }
        String realTopic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
        Slot slot = timerWheel.getSlot(delayedTime);
        ByteBuffer tmpBuffer = timerLogBuffer;
        tmpBuffer.clear();
        tmpBuffer.putInt(TimerLog.UNIT_SIZE); //size
        tmpBuffer.putLong(slot.lastPos); //prev pos
        tmpBuffer.putInt(magic); //magic
        tmpBuffer.putLong(tmpWriteTimeMs); //currWriteTime
        tmpBuffer.putInt((int) (delayedTime - tmpWriteTimeMs)); //delayTime
        tmpBuffer.putLong(offsetPy); //offset
        tmpBuffer.putInt(sizePy); //size
        tmpBuffer.putInt(hashTopicForMetrics(realTopic)); //hashcode of real topic
        tmpBuffer.putLong(0); //reserved value, just set to 0 now
        long ret = timerLog.append(tmpBuffer.array(), 0, TimerLog.UNIT_SIZE);
        if (-1 != ret) {
            // If it's a delete message, then slot's total num -1
            // TODO: check if the delete msg is in the same slot with "the msg to be deleted".
            timerWheel.putSlot(delayedTime, slot.firstPos == -1 ? ret : slot.firstPos, ret,
                isDelete ? slot.num - 1 : slot.num + 1, slot.magic);
            addMetric(messageExt, isDelete ? -1 : 1);
        }
        return -1 != ret;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public int warmDequeue() {
        if (!isRunningDequeue()) {
            return -1;
        }
        if (!storeConfig.isTimerWarmEnable()) {
            return -1;
        }
        if (preReadTimeMs <= currReadTimeMs) {
            preReadTimeMs = currReadTimeMs + precisionMs;
        }
        if (preReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        if (preReadTimeMs >= currReadTimeMs + 3L * precisionMs) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(preReadTimeMs);
        if (-1 == slot.timeMs) {
            preReadTimeMs = preReadTimeMs + precisionMs;
            return 0;
        }
        long currOffsetPy = slot.lastPos;
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        SelectMappedBufferResult timeSbr = null;
        SelectMappedBufferResult msgSbr = null;
        try {
            //read the msg one by one
            while (currOffsetPy != -1) {
                if (!isRunning()) {
                    break;
                }
                perfCounterTicks.startTick("warm_dequeue");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) {
                        sbrs.add(timeSbr);
                    }
                }
                if (null == timeSbr) {
                    break;
                }
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    timeSbr.getByteBuffer().position(position + TimerLog.UNIT_PRE_SIZE_FOR_MSG);
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    if (null == msgSbr || msgSbr.getStartOffset() > offsetPy) {
                        msgSbr = messageStore.getCommitLogData(offsetPy - offsetPy % commitLogFileSize);
                        if (null != msgSbr) {
                            sbrs.add(msgSbr);
                        }
                    }
                    if (null != msgSbr) {
                        ByteBuffer bf = msgSbr.getByteBuffer();
                        int firstPos = (int) (offsetPy % commitLogFileSize);
                        for (int pos = firstPos; pos < firstPos + sizePy; pos += 4096) {
                            bf.position(pos);
                            bf.get();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Unexpected error in warm", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("warm_dequeue");
                }
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        } finally {
            preReadTimeMs = preReadTimeMs + precisionMs;
        }
        return 1;
    }

    public boolean checkStateForPutMessages(int state) {
        for (AbstractStateService service : dequeuePutMessageServices) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkStateForGetMessages(int state) {
        for (AbstractStateService service : dequeueGetMessageServices) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public void checkDequeueLatch(CountDownLatch latch, long delayedTime) throws Exception {
        if (latch.await(1, TimeUnit.SECONDS)) {
            return;
        }
        int checkNum = 0;
        while (true) {
            if (dequeuePutQueue.size() > 0
                || !checkStateForGetMessages(AbstractStateService.WAITING)
                || !checkStateForPutMessages(AbstractStateService.WAITING)) {
                //let it go
            } else {
                checkNum++;
                if (checkNum >= 2) {
                    break;
                }
            }
            if (latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }
        if (!latch.await(1, TimeUnit.SECONDS)) {
            LOGGER.warn("Check latch failed delayedTime:{}", delayedTime);
        }
    }

    public int dequeue() throws Exception {
        if (storeConfig.isTimerStopDequeue()) {
            return -1;
        }
        if (!isRunningDequeue()) {
            return -1;
        }
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }

        Slot slot = timerWheel.getSlot(currReadTimeMs);
        if (-1 == slot.timeMs) {
            moveReadTime();
            return 0;
        }
        try {
            //clear the flag
            dequeueStatusChangeFlag = false;

            long currOffsetPy = slot.lastPos;
            Set<String> deleteUniqKeys = new ConcurrentSkipListSet<>();
            LinkedList<TimerRequest> normalMsgStack = new LinkedList<>();
            LinkedList<TimerRequest> deleteMsgStack = new LinkedList<>();
            LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
            SelectMappedBufferResult timeSbr = null;
            //read the timer log one by one
            while (currOffsetPy != -1) {
                perfCounterTicks.startTick("dequeue_read_timerlog");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) {
                        sbrs.add(timeSbr);
                    }
                }
                if (null == timeSbr) {
                    break;
                }
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    int magic = timeSbr.getByteBuffer().getInt();
                    long enqueueTime = timeSbr.getByteBuffer().getLong();
                    long delayedTime = timeSbr.getByteBuffer().getInt() + enqueueTime;
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, enqueueTime, magic);
                    timerRequest.setDeleteList(deleteUniqKeys);
                    if (needDelete(magic) && !needRoll(magic)) {
                        deleteMsgStack.add(timerRequest);
                    } else {
                        normalMsgStack.addFirst(timerRequest);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in dequeue_read_timerlog", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("dequeue_read_timerlog");
                }
            }
            if (deleteMsgStack.size() == 0 && normalMsgStack.size() == 0) {
                LOGGER.warn("dequeue time:{} but read nothing from timerLog", currReadTimeMs);
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            CountDownLatch deleteLatch = new CountDownLatch(deleteMsgStack.size());
            //read the delete msg: the msg used to mark another msg is deleted
            for (List<TimerRequest> deleteList : splitIntoLists(deleteMsgStack)) {
                for (TimerRequest tr : deleteList) {
                    tr.setLatch(deleteLatch);
                }
                dequeueGetQueue.put(deleteList);
            }
            //do we need to use loop with tryAcquire
            checkDequeueLatch(deleteLatch, currReadTimeMs);

            CountDownLatch normalLatch = new CountDownLatch(normalMsgStack.size());
            //read the normal msg
            for (List<TimerRequest> normalList : splitIntoLists(normalMsgStack)) {
                for (TimerRequest tr : normalList) {
                    tr.setLatch(normalLatch);
                }
                dequeueGetQueue.put(normalList);
            }
            checkDequeueLatch(normalLatch, currReadTimeMs);
            // if master -> slave -> master, then the read time move forward, and messages will be lossed
            if (dequeueStatusChangeFlag) {
                return -1;
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            moveReadTime();
        } catch (Throwable t) {
            LOGGER.error("Unknown error in dequeue process", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                moveReadTime();
            }
        }
        return 1;
    }

    private List<List<TimerRequest>> splitIntoLists(List<TimerRequest> origin) {
        //this method assume that the origin is not null;
        List<List<TimerRequest>> lists = new LinkedList<>();
        if (origin.size() < 100) {
            lists.add(origin);
            return lists;
        }
        List<TimerRequest> currList = null;
        int fileIndexPy = -1;
        int msgIndex = 0;
        for (TimerRequest tr : origin) {
            if (fileIndexPy != tr.getOffsetPy() / commitLogFileSize) {
                msgIndex = 0;
                if (null != currList && currList.size() > 0) {
                    lists.add(currList);
                }
                currList = new LinkedList<>();
                currList.add(tr);
                fileIndexPy = (int) (tr.getOffsetPy() / commitLogFileSize);
            } else {
                currList.add(tr);
                if (++msgIndex % 2000 == 0) {
                    lists.add(currList);
                    currList = new ArrayList<>();
                }
            }
        }
        if (null != currList && currList.size() > 0) {
            lists.add(currList);
        }
        return lists;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        for (int i = 0; i < 3; i++) {
            MessageExt msgExt = null;
            bufferLocal.get().position(0);
            bufferLocal.get().limit(sizePy);
            boolean res = messageStore.getData(offsetPy, sizePy, bufferLocal.get());
            if (res) {
                bufferLocal.get().flip();
                msgExt = MessageDecoder.decode(bufferLocal.get(), true, false, false);
            }
            if (null == msgExt) {
                LOGGER.warn("Fail to read msg from commitLog offsetPy:{} sizePy:{}", offsetPy, sizePy);
            } else {
                return msgExt;
            }
        }
        return null;
    }

    public MessageExtBrokerInner convert(MessageExt messageExt, long enqueueTime, boolean needRoll) {
        if (enqueueTime != -1) {
            MessageAccessor.putProperty(messageExt, TIMER_ENQUEUE_MS, enqueueTime + "");
        }
        if (needRoll) {
            if (messageExt.getProperty(TIMER_ROLL_TIMES) != null) {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, Integer.parseInt(messageExt.getProperty(TIMER_ROLL_TIMES)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, 1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TIMER_DEQUEUE_MS, System.currentTimeMillis() + "");
        MessageExtBrokerInner message = convertMessage(messageExt, needRoll);
        return message;
    }

    //0 succ; 1 fail, need retry; 2 fail, do not retry;
    public int doPut(MessageExtBrokerInner message, boolean roll) throws Exception {

        if (!roll && null != message.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            LOGGER.warn("Trying do put delete timer msg:[{}] roll:[{}]", message, roll);
            return PUT_NO_RETRY;
        }

        PutMessageResult putMessageResult = null;
        if (escapeBridgeHook != null) {
            putMessageResult = escapeBridgeHook.apply(message);
        } else {
            putMessageResult = messageStore.putMessage(message);
        }

        if (putMessageResult != null && putMessageResult.getPutMessageStatus() != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    if (brokerStatsManager != null) {
                        brokerStatsManager.incTopicPutNums(message.getTopic(), 1, 1);
                        if (putMessageResult.getAppendMessageResult() != null) {
                            brokerStatsManager.incTopicPutSize(message.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                        }
                        brokerStatsManager.incBrokerPutNums(message.getTopic(), 1);
                    }
                    return PUT_OK;

                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                case WHEEL_TIMER_NOT_ENABLE:
                case WHEEL_TIMER_MSG_ILLEGAL:
                    return PUT_NO_RETRY;

                case SERVICE_NOT_AVAILABLE:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case OS_PAGE_CACHE_BUSY:
                case CREATE_MAPPED_FILE_FAILED:
                case SLAVE_NOT_AVAILABLE:
                    return PUT_NEED_RETRY;

                case UNKNOWN_ERROR:
                default:
                    if (storeConfig.isTimerSkipUnknownError()) {
                        LOGGER.warn("Skipping message due to unknown error, msg: {}", message);
                        return PUT_NO_RETRY;
                    } else {
                        holdMomentForUnknownError();
                        return PUT_NEED_RETRY;
                    }
            }
        }
        return PUT_NEED_RETRY;
    }

    public MessageExtBrokerInner convertMessage(MessageExt msgExt, boolean needRoll) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, MessageAccessor.deepCopyProperties(msgExt.getProperties()));
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);

        if (needRoll) {
            msgInner.setTopic(msgExt.getTopic());
            msgInner.setQueueId(msgExt.getQueueId());
        } else {
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgInner.setQueueId(Integer.parseInt(msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        }
        return msgInner;
    }

    protected String getRealTopic(MessageExt msgExt) {
        if (msgExt == null) {
            return null;
        }
        return msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
    }

    private long formatTimeMs(long timeMs) {
        return timeMs / precisionMs * precisionMs;
    }

    public int hashTopicForMetrics(String topic) {
        return null == topic ? 0 : topic.hashCode();
    }

    public void checkAndReviseMetrics() {
        Map<String, TimerMetrics.Metric> smallOnes = new HashMap<>();
        Map<String, TimerMetrics.Metric> bigOnes = new HashMap<>();
        Map<Integer, String> smallHashs = new HashMap<>();
        Set<Integer> smallHashCollisions = new HashSet<>();
        for (Map.Entry<String, TimerMetrics.Metric> entry : timerMetrics.getTimingCount().entrySet()) {
            if (entry.getValue().getCount().get() < storeConfig.getTimerMetricSmallThreshold()) {
                smallOnes.put(entry.getKey(), entry.getValue());
                int hash = hashTopicForMetrics(entry.getKey());
                if (smallHashs.containsKey(hash)) {
                    LOGGER.warn("[CheckAndReviseMetrics]Metric hash collision between small-small code:{} small topic:{}{} small topic:{}{}", hash,
                        entry.getKey(), entry.getValue(),
                        smallHashs.get(hash), smallOnes.get(smallHashs.get(hash)));
                    smallHashCollisions.add(hash);
                }
                smallHashs.put(hash, entry.getKey());
            } else {
                bigOnes.put(entry.getKey(), entry.getValue());
            }
        }
        //check the hash collision between small ons and big ons
        for (Map.Entry<String, TimerMetrics.Metric> bjgEntry : bigOnes.entrySet()) {
            if (smallHashs.containsKey(hashTopicForMetrics(bjgEntry.getKey()))) {
                Iterator<Map.Entry<String, TimerMetrics.Metric>> smallIt = smallOnes.entrySet().iterator();
                while (smallIt.hasNext()) {
                    Map.Entry<String, TimerMetrics.Metric> smallEntry = smallIt.next();
                    if (hashTopicForMetrics(smallEntry.getKey()) == hashTopicForMetrics(bjgEntry.getKey())) {
                        LOGGER.warn("[CheckAndReviseMetrics]Metric hash collision between small-big code:{} small topic:{}{} big topic:{}{}", hashTopicForMetrics(smallEntry.getKey()),
                            smallEntry.getKey(), smallEntry.getValue(),
                            bjgEntry.getKey(), bjgEntry.getValue());
                        smallIt.remove();
                    }
                }
            }
        }
        //refresh
        smallHashs.clear();
        Map<String, TimerMetrics.Metric> newSmallOnes = new HashMap<>();
        for (String topic : smallOnes.keySet()) {
            newSmallOnes.put(topic, new TimerMetrics.Metric());
            smallHashs.put(hashTopicForMetrics(topic), topic);
        }

        //travel the timer log
        long readTimeMs = currReadTimeMs;
        long currOffsetPy = timerWheel.checkPhyPos(readTimeMs, 0);
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        boolean hasError = false;
        try {
            while (true) {
                SelectMappedBufferResult timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                if (timeSbr == null) {
                    break;
                } else {
                    sbrs.add(timeSbr);
                }
                ByteBuffer bf = timeSbr.getByteBuffer();
                for (int position = 0; position < timeSbr.getSize(); position += TimerLog.UNIT_SIZE) {
                    bf.position(position);
                    bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt(); //magic
                    long enqueueTime = bf.getLong();
                    long delayedTime = bf.getInt() + enqueueTime;
                    long offsetPy = bf.getLong();
                    int sizePy = bf.getInt();
                    int hashCode = bf.getInt();
                    if (delayedTime < readTimeMs) {
                        continue;
                    }
                    if (!smallHashs.containsKey(hashCode)) {
                        continue;
                    }
                    String topic = null;
                    if (smallHashCollisions.contains(hashCode)) {
                        MessageExt messageExt = getMessageByCommitOffset(offsetPy, sizePy);
                        if (null != messageExt) {
                            topic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
                        }
                    } else {
                        topic = smallHashs.get(hashCode);
                    }
                    if (null != topic && newSmallOnes.containsKey(topic)) {
                        newSmallOnes.get(topic).getCount().addAndGet(needDelete(magic) ? -1 : 1);
                    } else {
                        LOGGER.warn("[CheckAndReviseMetrics]Unexpected topic in checking timer metrics topic:{} code:{} offsetPy:{} size:{}", topic, hashCode, offsetPy, sizePy);
                    }
                }
                if (timeSbr.getSize() < timerLogFileSize) {
                    break;
                } else {
                    currOffsetPy = currOffsetPy + timerLogFileSize;
                }
            }

        } catch (Exception e) {
            hasError = true;
            LOGGER.error("[CheckAndReviseMetrics]Unknown error in checkAndReviseMetrics and abort", e);
        } finally {
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        }

        if (!hasError) {
            //update
            for (String topic : newSmallOnes.keySet()) {
                LOGGER.info("[CheckAndReviseMetrics]Revise metric for topic {} from {} to {}", topic, smallOnes.get(topic), newSmallOnes.get(topic));
            }
            timerMetrics.getTimingCount().putAll(newSmallOnes);
        }

    }

    public class TimerEnqueueGetService extends ServiceThread {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (!TimerMessageStore.this.enqueue(0)) {
                        waitForRunning(100L * precisionMs / 1000);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) TimerMessageStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    public class TimerEnqueuePutService extends ServiceThread {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        /**
         * collect the requests
         */
        protected List<TimerRequest> fetchTimerRequests() throws InterruptedException {
            List<TimerRequest> trs = null;
            TimerRequest firstReq = enqueuePutQueue.poll(10, TimeUnit.MILLISECONDS);
            if (null != firstReq) {
                trs = new ArrayList<>(16);
                trs.add(firstReq);
                while (true) {
                    TimerRequest tmpReq = enqueuePutQueue.poll(3, TimeUnit.MILLISECONDS);
                    if (null == tmpReq) {
                        break;
                    }
                    trs.add(tmpReq);
                    if (trs.size() > 10) {
                        break;
                    }
                }
            }
            return trs;
        }

        protected void putMessageToTimerWheel(TimerRequest req) {
            try {
                perfCounterTicks.startTick(ENQUEUE_PUT);
                DefaultStoreMetricsManager.incTimerEnqueueCount(getRealTopic(req.getMsg()));
                if (shouldRunningDequeue && req.getDelayTime() < currWriteTimeMs) {
                    req.setEnqueueTime(Long.MAX_VALUE);
                    dequeuePutQueue.put(req);
                } else {
                    boolean doEnqueueRes = doEnqueue(
                        req.getOffsetPy(), req.getSizePy(), req.getDelayTime(), req.getMsg());
                    req.idempotentRelease(doEnqueueRes || storeConfig.isTimerSkipUnknownError());
                }
                perfCounterTicks.endTick(ENQUEUE_PUT);
            } catch (Throwable t) {
                LOGGER.error("Unknown error", t);
                if (storeConfig.isTimerSkipUnknownError()) {
                    req.idempotentRelease(true);
                } else {
                    holdMomentForUnknownError();
                }
            }
        }

        protected void fetchAndPutTimerRequest() throws Exception {
            long tmpCommitQueueOffset = currQueueOffset;
            List<TimerRequest> trs = this.fetchTimerRequests();
            if (CollectionUtils.isEmpty(trs)) {
                commitQueueOffset = tmpCommitQueueOffset;
                maybeMoveWriteTime();
                return;
            }

            while (!isStopped()) {
                CountDownLatch latch = new CountDownLatch(trs.size());
                for (TimerRequest req : trs) {
                    req.setLatch(latch);
                    this.putMessageToTimerWheel(req);
                }
                checkDequeueLatch(latch, -1);
                boolean allSuccess = trs.stream().allMatch(TimerRequest::isSucc);
                if (allSuccess) {
                    break;
                } else {
                    holdMomentForUnknownError();
                }
            }
            commitQueueOffset = trs.get(trs.size() - 1).getMsg().getQueueOffset();
            maybeMoveWriteTime();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped() || enqueuePutQueue.size() != 0) {
                try {
                    fetchAndPutTimerRequest();
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Unknown error", e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public class TimerDequeueGetService extends ServiceThread {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (System.currentTimeMillis() < shouldStartTime) {
                        TimerMessageStore.LOGGER.info("TimerDequeueGetService ready to run after {}.", shouldStartTime);
                        waitForRunning(1000);
                        continue;
                    }
                    if (-1 == TimerMessageStore.this.dequeue()) {
                        waitForRunning(100L * precisionMs / 1000);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    abstract class AbstractStateService extends ServiceThread {
        public static final int INITIAL = -1, START = 0, WAITING = 1, RUNNING = 2, END = 3;
        protected int state = INITIAL;

        protected void setState(int state) {
            this.state = state;
        }

        protected boolean isState(int state) {
            return this.state == state;
        }
    }

    public class TimerDequeuePutMessageService extends AbstractStateService {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            setState(AbstractStateService.START);
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");

            while (!this.isStopped() || dequeuePutQueue.size() != 0) {
                try {
                    setState(AbstractStateService.WAITING);
                    TimerRequest tr = dequeuePutQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (null == tr) {
                        continue;
                    }

                    setState(AbstractStateService.RUNNING);
                    boolean tmpDequeueChangeFlag = false;

                    try {
                        while (!isStopped()) {
                            if (!isRunningDequeue()) {
                                dequeueStatusChangeFlag = true;
                                tmpDequeueChangeFlag = true;
                                break;
                            }

                            try {
                                perfCounterTicks.startTick(DEQUEUE_PUT);

                                MessageExt msgExt = tr.getMsg();
                                DefaultStoreMetricsManager.incTimerDequeueCount(getRealTopic(msgExt));

                                if (tr.getEnqueueTime() == Long.MAX_VALUE) {
                                    // Never enqueue, mark it.
                                    MessageAccessor.putProperty(msgExt, TIMER_ENQUEUE_MS, String.valueOf(Long.MAX_VALUE));
                                }

                                addMetric(msgExt, -1);
                                MessageExtBrokerInner msg = convert(msgExt, tr.getEnqueueTime(), needRoll(tr.getMagic()));

                                boolean processed = false;
                                int retryCount = 0;

                                while (!processed && !isStopped()) {
                                    int result = doPut(msg, needRoll(tr.getMagic()));

                                    if (result == PUT_OK) {
                                        processed = true;
                                    } else if (result == PUT_NO_RETRY) {
                                        TimerMessageStore.LOGGER.warn("Skipping message due to unrecoverable error. Msg: {}", msg);
                                        processed = true;
                                    } else {
                                        retryCount++;
                                        // Without enabling TimerEnableRetryUntilSuccess, messages will retry up to 3 times before being discarded
                                        if (!storeConfig.isTimerEnableRetryUntilSuccess() && retryCount >= 3) {
                                            TimerMessageStore.LOGGER.error("Message processing failed after {} retries. Msg: {}", retryCount, msg);
                                            processed = true;
                                        } else {
                                            Thread.sleep(500L * precisionMs / 1000);
                                            TimerMessageStore.LOGGER.warn("Retrying to process message. Retry count: {}, Msg: {}", retryCount, msg);
                                        }
                                    }
                                }

                                perfCounterTicks.endTick(DEQUEUE_PUT);
                                break;

                            } catch (Throwable t) {
                                TimerMessageStore.LOGGER.info("Unknown error", t);
                                if (storeConfig.isTimerSkipUnknownError()) {
                                    break;
                                } else {
                                    holdMomentForUnknownError();
                                }
                            }
                        }
                    } finally {
                        tr.idempotentRelease(!tmpDequeueChangeFlag);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
            setState(AbstractStateService.END);
        }
    }

    public class TimerDequeueGetMessageService extends AbstractStateService {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            setState(AbstractStateService.START);
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            //Mark different rounds
            boolean isRound = true;
            Map<String ,MessageExt> avoidDeleteLose = new HashMap<>();
            while (!this.isStopped()) {
                try {
                    setState(AbstractStateService.WAITING);
                    List<TimerRequest> trs = dequeueGetQueue.poll(100L * precisionMs / 1000, TimeUnit.MILLISECONDS);
                    if (null == trs || trs.size() == 0) {
                        continue;
                    }
                    setState(AbstractStateService.RUNNING);
                    for (int i = 0; i < trs.size(); ) {
                        TimerRequest tr = trs.get(i);
                        boolean doRes = false;
                        try {
                            long start = System.currentTimeMillis();
                            MessageExt msgExt = getMessageByCommitOffset(tr.getOffsetPy(), tr.getSizePy());
                            if (null != msgExt) {
                                if (needDelete(tr.getMagic()) && !needRoll(tr.getMagic())) {
                                    //Clearing is performed once in each round.
                                    //The deletion message is received first and the common message is received once
                                    if (!isRound) {
                                        isRound = true;
                                        for (MessageExt messageExt: avoidDeleteLose.values()) {
                                            addMetric(messageExt, 1);
                                        }
                                        avoidDeleteLose.clear();
                                    }
                                    if (msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null && tr.getDeleteList() != null) {

                                        avoidDeleteLose.put(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY), msgExt);
                                        tr.getDeleteList().add(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
                                    }
                                    tr.idempotentRelease();
                                    doRes = true;
                                } else {
                                    String uniqueKey = MessageClientIDSetter.getUniqID(msgExt);
                                    if (null == uniqueKey) {
                                        LOGGER.warn("No uniqueKey for msg:{}", msgExt);
                                    }
                                    //Mark ready for next round
                                    if (isRound) {
                                        isRound = false;
                                    }
                                    if (null != uniqueKey && tr.getDeleteList() != null && tr.getDeleteList().size() > 0
                                        && tr.getDeleteList().contains(buildDeleteKey(getRealTopic(msgExt), uniqueKey))) {
                                        avoidDeleteLose.remove(uniqueKey);
                                        doRes = true;
                                        tr.idempotentRelease();
                                        perfCounterTicks.getCounter("dequeue_delete").flow(1);
                                    } else {
                                        tr.setMsg(msgExt);
                                        while (!isStopped() && !doRes) {
                                            doRes = dequeuePutQueue.offer(tr, 3, TimeUnit.SECONDS);
                                        }
                                    }
                                }
                                perfCounterTicks.getCounter("dequeue_get_msg").flow(System.currentTimeMillis() - start);
                            } else {
                                //the tr will never be processed afterwards, so idempotentRelease it
                                tr.idempotentRelease();
                                doRes = true;
                                perfCounterTicks.getCounter("dequeue_get_msg_miss").flow(System.currentTimeMillis() - start);
                            }
                        } catch (Throwable e) {
                            LOGGER.error("Unknown exception", e);
                            if (storeConfig.isTimerSkipUnknownError()) {
                                tr.idempotentRelease();
                                doRes = true;
                            } else {
                                holdMomentForUnknownError();
                            }
                        } finally {
                            if (doRes) {
                                i++;
                            }
                        }
                    }
                    trs.clear();
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
            setState(AbstractStateService.END);
        }
    }

    public class TimerDequeueWarmService extends ServiceThread {

        @Override
        public String getServiceName() {
            String brokerIdentifier = "";
            if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore && ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().getIdentifier();
            }
            return brokerIdentifier + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    //if (!storeConfig.isTimerWarmEnable() || -1 == TimerMessageStore.this.warmDequeue()) {
                    waitForRunning(50);
                    //}
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public boolean needRoll(int magic) {
        return (magic & MAGIC_ROLL) != 0;
    }

    public boolean needDelete(int magic) {
        return (magic & MAGIC_DELETE) != 0;
    }

    public class TimerFlushService extends ServiceThread {
        private final SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss");

        @Override public String getServiceName() {
            String brokerIdentifier = "";
            if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore && ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().getIdentifier();
            }
            return brokerIdentifier + this.getClass().getSimpleName();
        }

        private String format(long time) {
            return sdf.format(new Date(time));
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            long start = System.currentTimeMillis();
            while (!this.isStopped()) {
                try {
                    prepareTimerCheckPoint();
                    timerLog.getMappedFileQueue().flush(0);
                    timerWheel.flush();
                    timerCheckpoint.flush();
                    if (System.currentTimeMillis() - start > storeConfig.getTimerProgressLogIntervalMs()) {
                        start = System.currentTimeMillis();
                        long tmpQueueOffset = currQueueOffset;
                        ConsumeQueueInterface cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
                        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
                        TimerMessageStore.LOGGER.info("[{}]Timer progress-check commitRead:[{}] currRead:[{}] currWrite:[{}] readBehind:{} currReadOffset:{} offsetBehind:{} behindMaster:{} " +
                                "enqPutQueue:{} deqGetQueue:{} deqPutQueue:{} allCongestNum:{} enqExpiredStoreTime:{}",
                            storeConfig.getBrokerRole(),
                            format(commitReadTimeMs), format(currReadTimeMs), format(currWriteTimeMs), getDequeueBehind(),
                            tmpQueueOffset, maxOffsetInQueue - tmpQueueOffset, timerCheckpoint.getMasterTimerQueueOffset() - tmpQueueOffset,
                            enqueuePutQueue.size(), dequeueGetQueue.size(), dequeuePutQueue.size(), getAllCongestNum(), format(lastEnqueueButExpiredStoreTime));
                    }
                    timerMetrics.persist();
                    waitForRunning(storeConfig.getTimerFlushIntervalMs());
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public long getAllCongestNum() {
        return timerWheel.getAllNum(currReadTimeMs);
    }

    public long getCongestNum(long deliverTimeMs) {
        return timerWheel.getNum(deliverTimeMs);
    }

    public boolean isReject(long deliverTimeMs) {
        long congestNum = timerWheel.getNum(deliverTimeMs);
        if (congestNum <= storeConfig.getTimerCongestNumEachSlot()) {
            return false;
        }
        if (congestNum >= storeConfig.getTimerCongestNumEachSlot() * 2L) {
            return true;
        }
        if (RANDOM.nextInt(1000) > 1000 * (congestNum - storeConfig.getTimerCongestNumEachSlot()) / (storeConfig.getTimerCongestNumEachSlot() + 0.1)) {
            return true;
        }
        return false;
    }

    public long getEnqueueBehindMessages() {
        long tmpQueueOffset = currQueueOffset;
        ConsumeQueueInterface cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
        return maxOffsetInQueue - tmpQueueOffset;
    }

    public long getEnqueueBehindMillis() {
        if (System.currentTimeMillis() - lastEnqueueButExpiredTime < 2000) {
            return System.currentTimeMillis() - lastEnqueueButExpiredStoreTime;
        }
        return 0;
    }

    public long getEnqueueBehind() {
        return getEnqueueBehindMillis() / 1000;
    }

    public long getDequeueBehindMessages() {
        return timerWheel.getAllNum(currReadTimeMs);
    }

    public long getDequeueBehindMillis() {
        return System.currentTimeMillis() - currReadTimeMs;
    }

    public long getDequeueBehind() {
        return getDequeueBehindMillis() / 1000;
    }

    public float getEnqueueTps() {
        return perfCounterTicks.getCounter(ENQUEUE_PUT).getLastTps();
    }

    public float getDequeueTps() {
        return perfCounterTicks.getCounter("dequeue_put").getLastTps();
    }

    public void prepareTimerCheckPoint() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
        timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);
        if (shouldRunningDequeue) {
            timerCheckpoint.setMasterTimerQueueOffset(commitQueueOffset);
            if (commitReadTimeMs != lastCommitReadTimeMs || commitQueueOffset != lastCommitQueueOffset) {
                timerCheckpoint.updateDateVersion(messageStore.getStateMachineVersion());
                lastCommitReadTimeMs = commitReadTimeMs;
                lastCommitQueueOffset = commitQueueOffset;
            }
        }
        timerCheckpoint.setLastTimerQueueOffset(Math.min(commitQueueOffset, timerCheckpoint.getMasterTimerQueueOffset()));
    }

    public void registerEscapeBridgeHook(Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook) {
        this.escapeBridgeHook = escapeBridgeHook;
    }

    public boolean isMaster() {
        return BrokerRole.SLAVE != lastBrokerRole;
    }

    public long getCurrReadTimeMs() {
        return this.currReadTimeMs;
    }

    public long getQueueOffset() {
        return currQueueOffset;
    }

    public long getCommitQueueOffset() {
        return this.commitQueueOffset;
    }

    public long getCommitReadTimeMs() {
        return this.commitReadTimeMs;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerWheel getTimerWheel() {
        return timerWheel;
    }

    public TimerLog getTimerLog() {
        return timerLog;
    }

    public TimerMetrics getTimerMetrics() {
        return this.timerMetrics;
    }

    public int getPrecisionMs() {
        return precisionMs;
    }

    public TimerEnqueueGetService getEnqueueGetService() {
        return enqueueGetService;
    }

    public void setEnqueueGetService(TimerEnqueueGetService enqueueGetService) {
        this.enqueueGetService = enqueueGetService;
    }

    public TimerEnqueuePutService getEnqueuePutService() {
        return enqueuePutService;
    }

    public void setEnqueuePutService(TimerEnqueuePutService enqueuePutService) {
        this.enqueuePutService = enqueuePutService;
    }

    public TimerDequeueWarmService getDequeueWarmService() {
        return dequeueWarmService;
    }

    public void setDequeueWarmService(
        TimerDequeueWarmService dequeueWarmService) {
        this.dequeueWarmService = dequeueWarmService;
    }

    public TimerDequeueGetService getDequeueGetService() {
        return dequeueGetService;
    }

    public void setDequeueGetService(TimerDequeueGetService dequeueGetService) {
        this.dequeueGetService = dequeueGetService;
    }

    public TimerDequeuePutMessageService[] getDequeuePutMessageServices() {
        return dequeuePutMessageServices;
    }

    public void setDequeuePutMessageServices(
        TimerDequeuePutMessageService[] dequeuePutMessageServices) {
        this.dequeuePutMessageServices = dequeuePutMessageServices;
    }

    public TimerDequeueGetMessageService[] getDequeueGetMessageServices() {
        return dequeueGetMessageServices;
    }

    public void setDequeueGetMessageServices(
        TimerDequeueGetMessageService[] dequeueGetMessageServices) {
        this.dequeueGetMessageServices = dequeueGetMessageServices;
    }

    public void setTimerMetrics(TimerMetrics timerMetrics) {
        this.timerMetrics = timerMetrics;
    }

    public AtomicInteger getFrequency() {
        return frequency;
    }

    public void setFrequency(AtomicInteger frequency) {
        this.frequency = frequency;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    // identify a message by topic + uk, like query operation
    public static String buildDeleteKey(String realTopic, String uniqueKey) {
        return realTopic + "+" + uniqueKey;
    }
}
