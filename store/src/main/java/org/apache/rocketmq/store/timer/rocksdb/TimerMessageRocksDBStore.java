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
package org.apache.rocketmq.store.timer.rocksdb;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.google.common.util.concurrent.RateLimiter;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.StoreUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.metrics.StoreMetricsManager;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.apache.rocketmq.store.util.PerfCounter;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_ROLL_LABEL;
import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TIMER_COLUMN_FAMILY;
import static org.apache.rocketmq.store.timer.TimerMessageStore.TIMER_TOPIC;

public class TimerMessageRocksDBStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final String SCAN_SYS_TOPIC = "scanSysTopic";
    private static final String SCAN_SYS_TOPIC_MISS = "scanSysTopicMiss";
    private static final String OUT_BIZ_MESSAGE = "outBizMsg";
    private static final String ROLL_LABEL = "R";
    private static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PUT_NO_RETRY = 2;
    private static final int MAX_GET_MSG_TIMES = 3, MAX_PUT_MSG_TIMES = 3;
    private static final int TIME_UP_CAPACITY = 100, ROLL_CAPACITY = 50;
    private static final int INITIAL = 0, RUNNING = 1, SHUTDOWN = 2;
    private volatile int state = INITIAL;
    private static long expirationThresholdMillis = 999L;
    private final AtomicLong readOffset = new AtomicLong(0);//timerSysTopic read offset
    private final MessageStore messageStore;
    private final TimerMetrics timerMetrics;
    private final MessageStoreConfig storeConfig;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageRocksDBStorage messageRocksDBStorage;
    private Timeline timeline;
    private TimerSysTopicScanService timerSysTopicScanService;
    private TimerMessageReputService expiredMessageReputService;
    private TimerMessageReputService rollMessageReputService;
    protected long precisionMs;
    private BlockingQueue<List<TimerRocksDBRecord>> expiredMessageQueue;
    private BlockingQueue<List<TimerRocksDBRecord>> rollMessageQueue;
    protected final PerfCounter.Ticks perfCounterTicks = new PerfCounter.Ticks(log);
    private Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook;
    private ThreadLocal<ByteBuffer> bufferLocal = null;

    public TimerMessageRocksDBStore(final MessageStore messageStore, final TimerMetrics timerMetrics,
        final BrokerStatsManager brokerStatsManager) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getMessageStoreConfig();
        this.precisionMs = storeConfig.getTimerRocksDBPrecisionMs() < 100L ? 1000L : storeConfig.getTimerRocksDBPrecisionMs();
        expirationThresholdMillis = precisionMs - 1L;
        this.messageRocksDBStorage = messageStore.getMessageRocksDBStorage();
        this.timerMetrics = timerMetrics;
        this.brokerStatsManager = brokerStatsManager;
        bufferLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(storeConfig.getMaxMessageSize()));
    }

    public synchronized boolean load() {
        initService();
        boolean result = this.timerMetrics.load();
        log.info("TimerMessageRocksDBStore load result: {}", result);
        return result;
    }

    public synchronized void start() {
        if (this.state == RUNNING) {
            return;
        }
        long commitOffsetFile = null != this.messageStore.getTimerMessageStore() ? this.messageStore.getTimerMessageStore().getCommitQueueOffset() : 0L;
        long commitOffsetRocksDB = messageRocksDBStorage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.SYS_TOPIC_SCAN_OFFSET_CHECK_POINT);
        long maxCommitOffset = Math.max(commitOffsetFile, commitOffsetRocksDB);
        this.readOffset.set(maxCommitOffset);
        this.expiredMessageReputService.start();
        this.rollMessageReputService.start();
        this.timeline.start();
        this.timerSysTopicScanService.start();
        this.state = RUNNING;
        log.info("TimerMessageRocksDBStore start success, start commitOffsetFile: {}, commitOffsetRocksDB: {}, readOffset: {}", commitOffsetFile, commitOffsetRocksDB, this.readOffset.get());
    }

    public synchronized boolean restart() {
        try {
            this.storeConfig.setTimerStopEnqueue(true);
            if (this.state == RUNNING && !this.storeConfig.isTimerRocksDBStopScan()) {
                log.info("restart TimerMessageRocksDBStore has been running");
                return true;
            }
            if (this.state == RUNNING && this.storeConfig.isTimerRocksDBStopScan()) {
                long commitOffsetFile = null != this.messageStore.getTimerMessageStore() ? this.messageStore.getTimerMessageStore().getCommitQueueOffset() : 0L;
                long commitOffsetRocksDB = messageRocksDBStorage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.SYS_TOPIC_SCAN_OFFSET_CHECK_POINT);
                long maxCommitOffset = Math.max(commitOffsetFile, commitOffsetRocksDB);
                this.readOffset.set(maxCommitOffset);
                log.info("restart TimerMessageRocksDBStore has been recover running, commitOffsetFile: {}, commitOffsetRocksDB: {}, readOffset: {}", commitOffsetFile, commitOffsetRocksDB, readOffset.get());
            } else {
                this.load();
                this.start();
            }
            this.storeConfig.setTimerRocksDBEnable(true);
            this.storeConfig.setTimerRocksDBStopScan(false);
            return true;
        } catch (Exception e) {
            logError.error("TimerMessageRocksDBStore restart error: {}", e.getMessage());
            return false;
        }
    }

    public void shutdown() {
        if (this.state != RUNNING || this.state == SHUTDOWN) {
            return;
        }
        if (null != this.timerSysTopicScanService) {
            this.timerSysTopicScanService.shutdown();
        }
        if (null != this.timeline) {
            this.timeline.shutDown();
        }
        if (null != this.expiredMessageReputService) {
            this.expiredMessageReputService.shutdown();
        }
        if (null != this.rollMessageReputService) {
            this.rollMessageReputService.shutdown();
        }
        this.state = SHUTDOWN;
        log.info("TimerMessageRocksDBStore shutdown success");
    }

    public void putRealTopicMessage(MessageExt msg) {
        if (null == msg) {
            logError.error("putRealTopicMessage msg is null");
            return;
        }
        MessageExtBrokerInner messageExtBrokerInner = convertMessage(msg);
        if (null == messageExtBrokerInner) {
            logError.error("putRealTopicMessage error, messageExtBrokerInner is null");
            return;
        }
        doPut(messageExtBrokerInner);
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerMetrics getTimerMetrics() {
        return timerMetrics;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public AtomicLong getReadOffset() {
        return readOffset;
    }

    public BlockingQueue<List<TimerRocksDBRecord>> getExpiredMessageQueue() {
        return expiredMessageQueue;
    }

    public BlockingQueue<List<TimerRocksDBRecord>> getRollMessageQueue() {
        return rollMessageQueue;
    }

    public long getCommitOffsetInRocksDB() {
        if (null == messageRocksDBStorage || !storeConfig.isTimerRocksDBEnable()) {
            return 0L;
        }
        return messageRocksDBStorage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.SYS_TOPIC_SCAN_OFFSET_CHECK_POINT);
    }

    public Timeline getTimeline() {
        return timeline;
    }

    private void initService() {
        if (storeConfig.isTimerEnableDisruptor()) {
            this.expiredMessageQueue = new DisruptorBlockingQueue<>(TIME_UP_CAPACITY);
            this.rollMessageQueue = new DisruptorBlockingQueue<>(ROLL_CAPACITY);
        } else {
            this.expiredMessageQueue = new LinkedBlockingDeque<>(TIME_UP_CAPACITY);
            this.rollMessageQueue = new LinkedBlockingDeque<>(ROLL_CAPACITY);
        }
        this.expiredMessageReputService = new TimerMessageReputService(expiredMessageQueue, storeConfig.getTimerRocksDBTimeExpiredMaxTps(), true);
        this.rollMessageReputService = new TimerMessageReputService(rollMessageQueue, storeConfig.getTimerRocksDBRollMaxTps(), false);
        this.timeline = new Timeline(messageStore, messageRocksDBStorage, this, timerMetrics);
        this.timerSysTopicScanService = new TimerSysTopicScanService();
    }

    private MessageExtBrokerInner convertMessage(MessageExt msgExt) {
        if (null == msgExt) {
            logError.error("convertMessage msgExt is null");
            return null;
        }
        MessageExtBrokerInner msgInner = null;
        try {
            msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            msgInner.setTags(msgExt.getTags());
            msgInner.setSysFlag(msgExt.getSysFlag());
            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.setProperties(msgInner, MessageAccessor.deepCopyProperties(msgExt.getProperties()));
            if (isNeedRoll(msgInner)) {
                msgInner.setTopic(msgExt.getTopic());
                msgInner.setQueueId(msgExt.getQueueId());
                msgInner.getProperties().put(PROPERTY_TIMER_ROLL_LABEL, ROLL_LABEL);
            } else {
                msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
                msgInner.setQueueId(Integer.parseInt(msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
                MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
                MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
                MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_ROLL_LABEL);
            }
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        } catch (Exception e) {
            logError.error("convertMessage error: {}", e.getMessage());
            return null;
        }
        return msgInner;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        if (offsetPy < 0L || sizePy <= 0 || sizePy > storeConfig.getMaxMessageSize()) {
            logError.error("getMessageByCommitOffset param error, offsetPy: {}, sizePy: {}, maxMsgSize: {}", offsetPy, sizePy, storeConfig.getMaxMessageSize());
            return null;
        }
        if (sizePy > bufferLocal.get().limit()) {
            bufferLocal.remove();
            bufferLocal.set(ByteBuffer.allocate(sizePy));
        }
        for (int i = 0; i < MAX_GET_MSG_TIMES; i++) {
            MessageExt msgExt = StoreUtil.getMessage(offsetPy, sizePy, messageStore, bufferLocal.get());
            if (null == msgExt) {
                log.warn("Fail to read msg from commitLog offsetPy: {} sizePy: {}", offsetPy, sizePy);
            } else {
                return msgExt;
            }
        }
        return null;
    }

    private boolean isNeedRoll(MessageExt messageExt) {
        try {
            String property = messageExt.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS);
            if (StringUtils.isEmpty(property)) {
                return false;
            }
            if (!isExpired(Long.parseLong(property))) {
                return true;
            }
        } catch (Exception e) {
            logError.error("isNeedRoll error : {}", e.getMessage());
        }
        return false;
    }

    private Long getDelayTime(MessageExt msgExt) {
        if (null == msgExt) {
            logError.error("getDelayTime msgExt is null");
            return null;
        }
        String delayTimeStr = msgExt.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS);
        if (StringUtils.isEmpty(delayTimeStr)) {
            logError.error("getDelayTime is empty, queueOffset: {}, delayTimeStr: {}", msgExt.getQueueOffset(), delayTimeStr);
            return null;
        }
        try {
            return Long.parseLong(delayTimeStr);
        } catch (Exception e) {
            logError.error("getDelayTime parse to long error : {}", e.getMessage());
        }
        return null;
    }

    private int doPut(MessageExtBrokerInner message) {
        if (null == message) {
            logError.error("doPut message is null");
            return PUT_NO_RETRY;
        }
        if (null != message.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            logError.warn("Trying do put delete timer msg:[{}]", message);
            return PUT_NO_RETRY;
        }
        PutMessageResult putMessageResult = null;
        if (escapeBridgeHook != null) {
            putMessageResult = escapeBridgeHook.apply(message);
        } else {
            putMessageResult = messageStore.putMessage(message);
        }
        if (null != putMessageResult && null != putMessageResult.getPutMessageStatus()) {
            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    if (null != brokerStatsManager) {
                        brokerStatsManager.incTopicPutNums(message.getTopic(), 1, 1);
                        if (null != putMessageResult.getAppendMessageResult()) {
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
                        logError.warn("Skipping message due to unknown error, msg: {}", message);
                        return PUT_NO_RETRY;
                    } else {
                        return PUT_NEED_RETRY;
                    }
            }
        }
        return PUT_NEED_RETRY;
    }

    public static boolean isExpired(long delayedTime) {
        return delayedTime <= System.currentTimeMillis() + expirationThresholdMillis;
    }

    public void registerEscapeBridgeHook(Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook) {
        this.escapeBridgeHook = escapeBridgeHook;
    }

    private String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TimerMessageRocksDBStore.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore)TimerMessageRocksDBStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    private class TimerSysTopicScanService extends ServiceThread {
        private final Logger log = TimerMessageRocksDBStore.log;

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            long waitTime;
            while (!this.isStopped()) {
                try {
                    if (!storeConfig.isTimerRocksDBEnable() || storeConfig.isTimerRocksDBStopScan()) {
                        waitTime = TimeUnit.SECONDS.toMillis(10);
                    } else {
                        scanSysTimerTopic();
                        waitTime = 100L;
                    }
                    waitForRunning(waitTime);
                } catch (Exception e) {
                    logError.error("TimerMessageRocksDBStore error occurred in: {}, error: {}", getServiceName(),
                        e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        private void scanSysTimerTopic() {
            ConsumeQueueInterface cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
            if (null == cq) {
                return;
            }
            if (readOffset.get() < cq.getMinOffsetInQueue()) {
                logError.warn("scanSysTimerTopic readOffset: {} is smaller than minOffsetInQueue: {}, use minOffsetInQueue to scan timer sysTimerTopic", readOffset.get(), cq.getMinOffsetInQueue());
                readOffset.set(cq.getMinOffsetInQueue());
            } else if (readOffset.get() > cq.getMaxOffsetInQueue()) {
                logError.warn("scanSysTimerTopic readOffset: {} is bigger than maxOffsetInQueue: {}, use maxOffsetInQueue to scan timer sysTimerTopic", readOffset.get(), cq.getMaxOffsetInQueue());
                readOffset.set(cq.getMaxOffsetInQueue());
            }
            ReferredIterator<CqUnit> iterator = null;
            try {
                iterator = cq.iterateFrom(readOffset.get());
                if (null == iterator) {
                    return;
                }
                while (iterator.hasNext()) {
                    perfCounterTicks.startTick(SCAN_SYS_TOPIC);
                    try {
                        CqUnit cqUnit = iterator.next();
                        if (null == cqUnit) {
                            logError.error("scanSysTimerTopic cqUnit is null, readOffset: {}", readOffset.get());
                            break;
                        }
                        long offsetPy = cqUnit.getPos();
                        int sizePy = cqUnit.getSize();
                        long queueOffset = cqUnit.getQueueOffset();
                        MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                        if (null == msgExt) {
                            perfCounterTicks.getCounter(SCAN_SYS_TOPIC_MISS);
                            break;
                        }
                        Long delayedTime = getDelayTime(msgExt);
                        if (null == delayedTime) {
                            readOffset.incrementAndGet();
                            continue;
                        }
                        if (isExpired(delayedTime)) {
                            putRealTopicMessage(msgExt);
                            readOffset.incrementAndGet();
                            continue;
                        }
                        TimerRocksDBRecord timerRecord = new TimerRocksDBRecord(delayedTime, MessageClientIDSetter.getUniqID(msgExt), offsetPy, sizePy, queueOffset, msgExt);
                        timeline.putRecord(timerRecord);
                        readOffset.incrementAndGet();

                        StoreMetricsManager metricsManager = messageStore.getStoreMetricsManager();
                        if (metricsManager instanceof DefaultStoreMetricsManager) {
                            DefaultStoreMetricsManager defaultMetricsManager = (DefaultStoreMetricsManager)metricsManager;
                            Attributes attributes = defaultMetricsManager.newAttributesBuilder().put(DefaultStoreMetricsConstant.LABEL_TOPIC, msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC)).build();
                            defaultMetricsManager.getTimerMessageSetLatency().record((delayedTime - msgExt.getBornTimestamp()) / 1000, attributes);
                        }
                    } catch (Exception e) {
                        logError.error("Unknown error in scan the system topic error: {}", e.getMessage());
                    } finally {
                        perfCounterTicks.endTick(SCAN_SYS_TOPIC);
                    }
                }
            } catch (Exception e) {
                logError.error("scanSysTimerTopic throw Unknown exception. {}", e.getMessage());
            } finally {
                if (iterator != null) {
                    iterator.release();
                }
            }
        }
    }

    private class TimerMessageReputService extends ServiceThread {
        private final Logger log = TimerMessageRocksDBStore.log;
        private final BlockingQueue<List<TimerRocksDBRecord>> queue;
        private final RateLimiter rateLimiter;
        private final boolean writeCheckPoint;
        ExecutorService executor = new ThreadPoolExecutor(
            6,
            6,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        public TimerMessageReputService(BlockingQueue<List<TimerRocksDBRecord>> queue, double maxTps, boolean writeCheckPoint) {
            this.queue = queue;
            this.rateLimiter = RateLimiter.create(maxTps);
            this.writeCheckPoint = writeCheckPoint;
        }

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || !queue.isEmpty()) {
                try {
                    List<TimerRocksDBRecord> trs = queue.poll(100L, TimeUnit.MILLISECONDS);
                    if (CollectionUtils.isEmpty(trs)) {
                        continue;
                    }
                    long start = System.currentTimeMillis();
                    CountDownLatch countDownLatch = new CountDownLatch(trs.size());
                    for (TimerRocksDBRecord record : trs) {
                        executor.submit(new Task(countDownLatch, record));
                    }
                    countDownLatch.await();
                    log.info("TimerMessageReputService reput messages to commitlog, cost: {}, trs size: {}, checkPoint: {}", System.currentTimeMillis() - start, trs.size(), trs.get(trs.size() - 1).getCheckPoint());
                    if (this.writeCheckPoint && !CollectionUtils.isEmpty(trs) && trs.get(trs.size() - 1).getCheckPoint() > 0L) {
                        log.info("TimerMessageReputService reput messages to commitlog, checkPoint: {}", trs.get(trs.size() - 1).getCheckPoint());
                        messageRocksDBStorage.writeCheckPointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.TIMELINE_CHECK_POINT, trs.get(trs.size() - 1).getCheckPoint());
                    }
                } catch (Exception e) {
                    logError.error("TimerMessageReputService error: {}", e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        private void putMsgWithRetry(MessageExtBrokerInner msg) throws InterruptedException {
            if (null == msg) {
                return;
            }
            for (int retryCount = 0; !isStopped() && retryCount <= MAX_PUT_MSG_TIMES; retryCount++) {
                int result = doPut(msg);
                switch (result) {
                    case PUT_OK:
                        return;
                    case PUT_NO_RETRY:
                        logError.warn("Skipping message due to unrecoverable error. Msg: {}", msg);
                        return;
                    default:
                        if (retryCount == MAX_PUT_MSG_TIMES) {
                            logError.error("Message processing failed after {} retries. Msg: {}", retryCount, msg);
                            return;
                        } else {
                            Thread.sleep(100L);
                            logError.warn("Retrying to process message. Retry count: {}, Msg: {}", retryCount, msg);
                        }
                }
            }
        }

        class Task implements Callable<Void> {
            private CountDownLatch countDownLatch;
            private TimerRocksDBRecord record;

            public Task(CountDownLatch countDownLatch, TimerRocksDBRecord record) {
                this.countDownLatch = countDownLatch;
                this.record = record;
            }

            @Override
            public Void call() throws Exception {
                try {
                    perfCounterTicks.startTick(OUT_BIZ_MESSAGE);
                    MessageExt messageExt = record.getMessageExt();
                    if (null == messageExt) {
                        messageExt = getMessageByCommitOffset(record.getOffsetPy(), record.getSizePy());
                        if (null == messageExt) {
                            return null;
                        }
                    }
                    MessageExtBrokerInner msg = convertMessage(messageExt);
                    if (null == msg) {
                        return null;
                    }
                    record.setUniqKey(MessageClientIDSetter.getUniqID(msg));
                    putMsgWithRetry(msg);
                    timeline.addMetric(msg, -1);
                    perfCounterTicks.endTick(OUT_BIZ_MESSAGE);
                    rateLimiter.acquire();
                } catch (Exception e) {
                    logError.error("TimerMessageReputService running error: {}", e.getMessage());
                } finally {
                    countDownLatch.countDown();
                }
                return null;
            }
        }
    }

}
