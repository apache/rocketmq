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

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.store.timer.rocksdb.TimerMessageRocksDBStorage.POP_COLUMN_FAMILY;
import static org.apache.rocketmq.store.timer.rocksdb.TimerMessageRocksDBStorage.TRANSACTION_COLUMN_FAMILY;

public class TimerMessageRocksDBStore {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final String ROCKSDB_DIRECTORY = "kvStore";

    public static final int TIMER_WHEEL_TTL_DAY = 30;
    public static final int DAY_SECS = 24 * 3600;
    public static final int DEFAULT_CAPACITY = 1024;
    public static final int INITIAL = 0, RUNNING = 1, SHUTDOWN = 2;
    public static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PUT_NO_RETRY = 2;
    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final String TIMER_OUT_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
    public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;

    private final TimerMessageKVStore timerMessageKVStore;
    private final MessageStore messageStore;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageStoreConfig storeConfig;
    private final TimerMetrics timerMetrics;

    private final int slotSize;
    private final int precisionMs;
    private final long metricsIntervalMs;
    private volatile int state = INITIAL;
    private long lastEnqueueButExpiredTime;
    private long lastEnqueueButExpiredStoreTime;

    private TimerEnqueueGetService timerEnqueueGetService;
    private TimerEnqueuePutService timerEnqueuePutService;
    private List<TimerGetMessageService> timerGetMessageServices;
    private List<TimerWarmService> timerWarmServices;
    private TimerDequeueGetService[] timerDequeueGetServices;
    private TimerDequeuePutService[] timerDequeuePutServices;

    private BlockingQueue<TimerMessageRecord> enqueuePutQueue;
    private BlockingQueue<List<TimerMessageRecord>> dequeueGetQueue;
    private BlockingQueue<List<TimerMessageRecord>> dequeuePutQueue;

    private final AtomicLong commitOffset = new AtomicLong(0);
    private final AtomicLong readOffset = new AtomicLong(0);

    public TimerMessageRocksDBStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
        TimerMetrics timerMetrics, final BrokerStatsManager brokerStatsManager) {
        this.storeConfig = storeConfig;
        this.messageStore = messageStore;
        this.timerMetrics = timerMetrics;
        this.brokerStatsManager = brokerStatsManager;

        this.precisionMs = storeConfig.getTimerPrecisionMs();
        this.slotSize = 1000 * TIMER_WHEEL_TTL_DAY / precisionMs * DAY_SECS;
        this.metricsIntervalMs = 1000L * TIMER_WHEEL_TTL_DAY * DAY_SECS;
        this.timerMessageKVStore = new TimerMessageRocksDBStorage(Paths.get(
            storeConfig.getStorePathRootDir(), ROCKSDB_DIRECTORY).toString());

        this.timerGetMessageServices = new ArrayList<>();
        this.timerWarmServices = new ArrayList<>();
    }

    public boolean load() {
        initService();
        boolean result = timerMessageKVStore.start();
        result &= this.timerMetrics.load();
        calcTimerDistribution();
        return result;
    }

    public void start() {
        if (state == RUNNING) {
            return;
        }
        this.commitOffset.set(timerMessageKVStore.getCommitOffset());
        this.readOffset.set(commitOffset.get());
        this.timerEnqueueGetService.start();
        this.timerEnqueuePutService.start();

        for (TimerDequeueGetService timerDequeueGetService : timerDequeueGetServices) {
            timerDequeueGetService.start();
        }
        for (TimerWarmService timerWarmService : timerWarmServices) {
            timerWarmService.start();
        }
        for (TimerGetMessageService timerGetMessageService : timerGetMessageServices) {
            timerGetMessageService.start();
        }
        for (TimerDequeuePutService timerDequeuePutService : timerDequeuePutServices) {
            timerDequeuePutService.start();
        }
        state = RUNNING;
    }

    public void shutdown() {
        if (state != RUNNING || state == SHUTDOWN) {
            return;
        }
        state = SHUTDOWN;

        this.timerEnqueueGetService.shutdown();
        this.timerEnqueuePutService.shutdown();

        for (TimerDequeueGetService timerDequeueGetService : timerDequeueGetServices) {
            timerDequeueGetService.shutdown();
        }
        for (TimerWarmService timerWarmService : timerWarmServices) {
            timerWarmService.shutdown();
        }
        for (TimerGetMessageService timerGetMessageService : timerGetMessageServices) {
            timerGetMessageService.shutdown();
        }
        for (TimerDequeuePutService timerDequeuePutService : timerDequeuePutServices) {
            timerDequeuePutService.shutdown();
        }

        this.dequeueGetQueue.clear();
        this.enqueuePutQueue.clear();
        this.dequeuePutQueue.clear();
        this.timerMessageKVStore.shutdown();
    }

    public void createTimer(byte[] columnFamily) {
        this.timerGetMessageServices.add(new TimerGetMessageService(columnFamily));
        this.timerWarmServices.add(new TimerWarmService(columnFamily));
    }

    // ----------------------------------------------------------------------------------------------------------------
    private void initService() {
        createTimer(TRANSACTION_COLUMN_FAMILY);
        createTimer(POP_COLUMN_FAMILY);
        createTimer(RocksDB.DEFAULT_COLUMN_FAMILY);
        this.timerEnqueueGetService = new TimerEnqueueGetService();
        this.timerEnqueuePutService = new TimerEnqueuePutService();

        int getThreadNum = Math.max(storeConfig.getTimerGetMessageThreadNum(), 1);
        int putThreadNum = Math.max(storeConfig.getTimerPutMessageThreadNum(), 1);
        this.timerDequeuePutServices = new TimerDequeuePutService[putThreadNum];
        this.timerDequeueGetServices = new TimerDequeueGetService[getThreadNum];
        for (int i = 0; i < timerDequeuePutServices.length; i++) {
            timerDequeuePutServices[i] = new TimerDequeuePutService();
        }
        for (int i = 0; i < timerDequeueGetServices.length; i++) {
            timerDequeueGetServices[i] = new TimerDequeueGetService();
        }

        if (storeConfig.isTimerEnableDisruptor()) {
            this.enqueuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            this.dequeuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            this.dequeueGetQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
        } else {
            this.enqueuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            this.dequeuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            this.dequeueGetQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        }
    }

    private void calcTimerDistribution() {
        List<Integer> timerDist = this.timerMetrics.getTimerDistList();
        long currTime = System.currentTimeMillis() / precisionMs * precisionMs;
        for (int i = 0; i < timerDist.size(); i++) {
            long startTime = (currTime + (i == 0 ? 0 : timerDist.get(i - 1) * 1000)) % metricsIntervalMs;
            long endTime = (currTime + timerDist.get(i) * 1000) % metricsIntervalMs;
            if (endTime > startTime) {
                this.timerMetrics.updateDistPair(timerDist.get(i), timerMessageKVStore.getMetricSize(startTime, endTime));
            } else {
                this.timerMetrics.updateDistPair(timerDist.get(i), timerMessageKVStore.getMetricSize(startTime,
                    metricsIntervalMs) + timerMessageKVStore.getMetricSize(0, endTime));
            }
        }
    }

    public void checkAndReviseMetrics() {
        // TODO topic revise
    }

    public boolean isReject(long deliverTimeMs) {
        // TODO Implement reject logical judgment
        return false;
    }

    private String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TimerMessageRocksDBStore.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) TimerMessageRocksDBStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    private byte[] getColumnFamily(int flag) {
        TimerMessageRecord.Flag tag;
        switch (flag) {
            case 1:
                tag = TimerMessageRecord.Flag.TRANSACTION;
                break;
            case 2:
                tag = TimerMessageRecord.Flag.POP;
                break;
            default:
                tag = TimerMessageRecord.Flag.DEFAULT;
        }
        if (TimerMessageRecord.Flag.TRANSACTION == tag) {
            return TRANSACTION_COLUMN_FAMILY;
        } else if (tag == TimerMessageRecord.Flag.POP) {
            return POP_COLUMN_FAMILY;
        } else {
            return RocksDB.DEFAULT_COLUMN_FAMILY;
        }
    }

    private class TimerEnqueueGetService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (!enqueue(0)) {
                        waitForRunning(100L * precisionMs / 1000);
                    }
                } catch (Throwable e) {
                    TimerMessageRocksDBStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service end");
        }
    }

    private class TimerEnqueuePutService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || !enqueuePutQueue.isEmpty()) {
                try {
                    fetchAndPutTimerRequest();
                } catch (Throwable e) {
                    TimerMessageRocksDBStore.log.error("Unknown error", e);
                }
            }
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service end");
        }

        private List<TimerMessageRecord> fetchTimerMessageRecord() throws InterruptedException {
            List<TimerMessageRecord> trs = null;
            TimerMessageRecord firstReq = enqueuePutQueue.poll(10, TimeUnit.MILLISECONDS);
            if (null != firstReq) {
                trs = new ArrayList<>(16);
                trs.add(firstReq);
                while (true) {
                    TimerMessageRecord tmpReq = enqueuePutQueue.poll(3, TimeUnit.MILLISECONDS);
                    if (null == tmpReq) {
                        break;
                    }
                    trs.add(tmpReq);
                    if (trs.size() > 100) {
                        break;
                    }
                }
            }
            return trs;
        }

        private void fetchAndPutTimerRequest() throws InterruptedException {
            List<TimerMessageRecord> trs = fetchTimerMessageRecord();

            if (null == trs) {
                return;
            }
            Map<Long/* delayTime */, List<TimerMessageRecord>> increaseMetric = new HashMap<>();
            Map<Long/* delayTime */, List<TimerMessageRecord>> deleteMetric = new HashMap<>();
            Map<Integer/* columnFamily */, List<TimerMessageRecord>> increase = new HashMap<>();
            Map<Integer/* columnFamily */, List<TimerMessageRecord>> delete = new HashMap<>();
            List<TimerMessageRecord> expired = new ArrayList<>();

            for (TimerMessageRecord tr : trs) {
                long delayTime = tr.getDelayTime();
                int flag = tr.getMessageExt().getProperty(MessageConst.PROPERTY_TIMER_DEL_FLAG) == null ?
                    0 : Integer.parseInt(tr.getMessageExt().getProperty(MessageConst.PROPERTY_TIMER_DEL_FLAG));
                if (tr.getMessageExt().getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null) {
                    // Construct original message
                    tr.setDelayTime(delayTime / precisionMs % slotSize);
                    tr.setUniqueKey(TimerMessageStore.extractUniqueKey(tr.getMessageExt().
                        getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)));
                    deleteMetric.computeIfAbsent(delayTime % metricsIntervalMs, k -> new ArrayList<>()).add(tr);
                    delete.computeIfAbsent(flag, k -> new ArrayList<>()).add(tr);
                } else if (delayTime <= System.currentTimeMillis()) {
                    expired.add(tr);
                    addMetric(tr.getMessageExt(), 1);
                    addMetric((int) (Long.parseLong(tr.getMessageExt().getProperty(TIMER_OUT_MS)) / precisionMs % slotSize), 1);
                } else {
                    tr.setDelayTime(delayTime / precisionMs % slotSize);
                    increaseMetric.computeIfAbsent(delayTime % metricsIntervalMs, k -> new ArrayList<>()).add(tr);
                    increase.computeIfAbsent(flag, k -> new ArrayList<>()).add(tr);
                }
            }

            while (!expired.isEmpty() && !dequeueGetQueue.offer(expired, 100, TimeUnit.MILLISECONDS)) {
            }
            for (Map.Entry<Integer, List<TimerMessageRecord>> entry : increase.entrySet()) {
                timerMessageKVStore.writeAssignRecords(getColumnFamily(entry.getKey()), entry.getValue(), -1);
            }
            for (Map.Entry<Integer, List<TimerMessageRecord>> entry : delete.entrySet()) {
                timerMessageKVStore.deleteAssignRecords(getColumnFamily(entry.getKey()), entry.getValue(), -1);
            }
            // sync cq read offset
            timerMessageKVStore.writeAssignRecords(getColumnFamily(0), new ArrayList<>(), commitOffset.addAndGet(trs.size()));

            for (Map.Entry<Long, List<TimerMessageRecord>> entry : deleteMetric.entrySet()) {
                long delayTime = entry.getKey();
                for (TimerMessageRecord record : entry.getValue()) {
                    addMetric(record.getMessageExt(), -1);
                    addMetric((int) (Long.parseLong(record.getMessageExt().getProperty(TIMER_OUT_MS)) / precisionMs % slotSize), -1);
                }
                timerMessageKVStore.syncMetric(delayTime, -entry.getValue().size());
            }
            for (Map.Entry<Long, List<TimerMessageRecord>> entry : increaseMetric.entrySet()) {
                long delayTime = entry.getKey();
                for (TimerMessageRecord record : entry.getValue()) {
                    addMetric(record.getMessageExt(), 1);
                    addMetric((int) (Long.parseLong(record.getMessageExt().getProperty(TIMER_OUT_MS)) / precisionMs % slotSize), 1);
                }
                timerMessageKVStore.syncMetric(delayTime, entry.getValue().size());
            }
        }
    }

    private class TimerDequeueGetService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || !dequeueGetQueue.isEmpty()) {
                try {
                    List<TimerMessageRecord> timerMessageRecord = dequeueGetQueue.poll(100L * precisionMs / 1000, TimeUnit.MILLISECONDS);
                    if (null == timerMessageRecord || timerMessageRecord.isEmpty()) {
                        continue;
                    }
                    for (TimerMessageRecord record : timerMessageRecord) {
                        MessageExt messageExt = getMessageByCommitOffset(record.getOffsetPY(), record.getSizeReal());
                        long delayedTime = Long.parseLong(messageExt.getProperty(TIMER_OUT_MS));
                        record.setMessageExt(messageExt);
                        record.setDelayTime(delayedTime);
                        record.setUniqueKey(MessageClientIDSetter.getUniqID(messageExt));
                        record.setRoll(delayedTime >= System.currentTimeMillis() + precisionMs * 3L);
                    }

                    while (!dequeuePutQueue.offer(timerMessageRecord, 3, TimeUnit.SECONDS)) {
                    }
                } catch (InterruptedException e) {
                    TimerMessageRocksDBStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service end");
        }
    }

    private class TimerDequeuePutService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || !dequeuePutQueue.isEmpty()) {
                try {
                    List<TimerMessageRecord> timerMessageRecord = dequeuePutQueue.poll(100L * precisionMs / 1000, TimeUnit.MILLISECONDS);
                    int flag = 0;
                    long delayTime = -1;
                    if (null == timerMessageRecord || timerMessageRecord.isEmpty()) {
                        continue;
                    }
                    for (TimerMessageRecord record : timerMessageRecord) {
                        MessageExt msg = record.getMessageExt();
                        MessageExtBrokerInner messageExtBrokerInner = convert(msg, record.isRoll());
                        if (delayTime == -1) {
                            delayTime = Long.parseLong(record.getMessageExt().getProperty(TIMER_OUT_MS));
                        }
                        flag = record.getMessageExt().getProperty(MessageConst.PROPERTY_TIMER_DEL_FLAG) == null ?
                            0 : Integer.parseInt(record.getMessageExt().getProperty(MessageConst.PROPERTY_TIMER_DEL_FLAG));
                        boolean processed = false;
                        int retryCount = 0;

                        while (!processed && !isStopped()) {
                            int result = doPut(messageExtBrokerInner, record.isRoll());

                            if (result == PUT_OK) {
                                processed = true;
                            } else if (result == PUT_NO_RETRY) {
                                TimerMessageRocksDBStore.log.warn("Skipping message due to unrecoverable error. Msg: {}", msg);
                                processed = true;
                            } else {
                                retryCount++;
                                // Without enabling TimerEnableRetryUntilSuccess, messages will retry up to 3 times before being discarded
                                if (!storeConfig.isTimerEnableRetryUntilSuccess() && retryCount >= 3) {
                                    TimerMessageRocksDBStore.log.error("Message processing failed after {} retries. Msg: {}", retryCount, msg);
                                    processed = true;
                                } else {
                                    Thread.sleep(500L * precisionMs / 1000);
                                    TimerMessageRocksDBStore.log.warn("Retrying to process message. Retry count: {}, Msg: {}", retryCount, msg);
                                }
                            }
                        }
                        addMetric(msg, -1);
                        addMetric((int) (Long.parseLong(msg.getProperty(TIMER_OUT_MS)) / precisionMs % slotSize), -1);
                    }
                    timerMessageKVStore.syncMetric(delayTime % metricsIntervalMs, -timerMessageRecord.size());
                    timerMessageKVStore.deleteAssignRecords(getColumnFamily(flag), timerMessageRecord, timerMessageRecord.get(0).getReadOffset());
                } catch (InterruptedException e) {
                    TimerMessageRocksDBStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service end");
        }
    }

    private class TimerGetMessageService extends ServiceThread {
        private final byte[] columnFamily;
        private long checkpoint;

        public TimerGetMessageService(byte[] columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            this.checkpoint = timerMessageKVStore.getCheckpoint(columnFamily);
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (-1 == dequeue(checkpoint, columnFamily)) {
                        waitForRunning(100L * precisionMs / 1000);
                    } else {
                        checkpoint += precisionMs;
                    }
                } catch (Throwable e) {
                    TimerMessageRocksDBStore.log.error("Error occurred in " + getServiceName(), e);
                    throw new RuntimeException(e);
                }
            }
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service end");
        }
    }

    private class TimerWarmService extends ServiceThread {
        private final byte[] columnFamily;
        private long checkpoint;

        public TimerWarmService(byte[] columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service start");
            this.checkpoint = timerMessageKVStore.getCheckpoint(columnFamily) + precisionMs;
            while (!this.isStopped()) {
                try {
                    int checkpoint = warm(this.checkpoint, columnFamily);
                    if (-1 == checkpoint) {
                        waitForRunning(100L * precisionMs / 1000);
                    } else {
                        this.checkpoint += precisionMs;
                    }
                } catch (Throwable e) {
                    TimerMessageRocksDBStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageRocksDBStore.log.info(this.getServiceName() + " service end");
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    public boolean enqueue(int queueId) {
        if (!storeConfig.getEnableTimerMessageOnRocksDB() || storeConfig.isTimerStopEnqueue()) {
            return false;
        }

        ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
        if (null == cq) {
            return false;
        }
        if (readOffset.get() < cq.getMinOffsetInQueue()) {
            log.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                    readOffset, cq.getMinOffsetInQueue());
            readOffset.set(cq.getMinOffsetInQueue());
        }

        ReferredIterator<CqUnit> iterator = null;
        try {
            iterator = cq.iterateFrom(readOffset.get());
            if (null == iterator) {
                return false;
            }

            int i = 0;
            while (iterator.hasNext()) {
                i++;
                try {
                    CqUnit cqUnit = iterator.next();
                    long offsetPy = cqUnit.getPos();
                    int sizePy = cqUnit.getSize();
                    MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);

                    if (null != msgExt) {
                        lastEnqueueButExpiredTime = System.currentTimeMillis();
                        lastEnqueueButExpiredStoreTime = msgExt.getStoreTimestamp();
                        long delayedTime = Long.parseLong(msgExt.getProperty(TIMER_OUT_MS));
                        TimerMessageRecord timerRequest = new TimerMessageRecord(delayedTime,
                            MessageClientIDSetter.getUniqID(msgExt), offsetPy, sizePy);
                        timerRequest.setMessageExt(msgExt);

                        while (!enqueuePutQueue.offer(timerRequest, 3, TimeUnit.SECONDS)) {
                        }
                        Attributes attributes = DefaultStoreMetricsManager.newAttributesBuilder()
                            .put(DefaultStoreMetricsConstant.LABEL_TOPIC, msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC)).build();
                        DefaultStoreMetricsManager.timerMessageSetLatency.record((delayedTime - msgExt.getBornTimestamp()) / 1000, attributes);
                    }
                } catch (Exception e) {
                    // here may cause the message loss
                    log.warn("Unknown error in skipped in enqueuing", e);
                    throw e;
                }
                readOffset.incrementAndGet();
            }
            return i > 0;
        } catch (Exception e) {
            log.error("Unknown exception in enqueuing", e);
        } finally {
            if (iterator != null) {
                iterator.release();
            }
        }
        return false;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        ThreadLocal<ByteBuffer> bufferLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(sizePy));
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
                log.warn("Fail to read msg from commitLog offsetPy:{} sizePy:{}", offsetPy, sizePy);
            } else {
                return msgExt;
            }
        }
        return null;
    }

    private int dequeue(long checkpoint, byte[] columnFamily) throws InterruptedException {
        if (storeConfig.isTimerStopDequeue()) {
            return -1;
        }
        if (checkpoint > System.currentTimeMillis()) {
            return -1;
        }
        int slot = (int) (checkpoint / precisionMs % slotSize);

        List<TimerMessageRecord> timerMessageRecords = timerMessageKVStore.scanRecords(columnFamily, slot, slot + 1);
        if (timerMessageRecords == null || timerMessageRecords.isEmpty()) {
            return 0;
        }

        for (TimerMessageRecord timerMessageRecord : timerMessageRecords) {
            timerMessageRecord.setReadOffset(checkpoint);
        }
        while (!dequeueGetQueue.offer(timerMessageRecords, 3, TimeUnit.SECONDS)) {
        }
        return 0;
    }

    private int warm(long checkpoint, byte[] columnFamily) {
        if (!storeConfig.isTimerWarmEnable()) {
            return -1;
        }
        if (checkpoint < System.currentTimeMillis() + precisionMs) {
            checkpoint = System.currentTimeMillis() + precisionMs;
        }
        if (checkpoint >= System.currentTimeMillis() + 3L * precisionMs) {
            return -1;
        }

        int slot = (int) (checkpoint / precisionMs % slotSize);
        timerMessageKVStore.scanRecords(columnFamily, slot, slot + 1);
        return 0;
    }

    private MessageExtBrokerInner convert(MessageExt messageExt, boolean needRoll) {
        if (needRoll) {
            if (messageExt.getProperty(TIMER_ROLL_TIMES) != null) {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, Integer.parseInt(messageExt.getProperty(TIMER_ROLL_TIMES)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, 1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TIMER_DEQUEUE_MS, System.currentTimeMillis() + "");
        return convertMessage(messageExt, needRoll);
    }

    private MessageExtBrokerInner convertMessage(MessageExt msgExt, boolean needRoll) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, MessageAccessor.deepCopyProperties(msgExt.getProperties()));
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
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

    private int doPut(MessageExtBrokerInner message, boolean roll) {
        if (!roll && null != message.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            log.warn("Trying do put delete timer msg:[{}] roll:[{}]", message, roll);
            return PUT_NO_RETRY;
        }

        PutMessageResult putMessageResult = messageStore.putMessage(message);
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
                        log.warn("Skipping message due to unknown error, msg: {}", message);
                        return PUT_NO_RETRY;
                    } else {
                        return PUT_NEED_RETRY;
                    }
            }
        }
        return PUT_NEED_RETRY;
    }

    private void addMetric(MessageExt msg, int value) {
        if (null == msg || null == msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC)) {
            return;
        }
        if (msg.getProperty(TIMER_ENQUEUE_MS) != null
            && NumberUtils.toLong(msg.getProperty(TIMER_ENQUEUE_MS)) == Long.MAX_VALUE) {
            return;
        }
        timerMetrics.addAndGet(msg, value);
    }

    private void addMetric(int delayTime, int value) {
        timerMetrics.updateDistPair(delayTime, value);
    }

    public TimerMetrics getTimerMetrics() {
        return this.timerMetrics;
    }

    public long getCommitOffset() {
        return commitOffset.get();
    }

    public long getAllCongestNum() {
        return timerMessageKVStore.getMetricSize(0, metricsIntervalMs);
    }

    public long getCongestNum(long deliverTimeMs) {
        long slot = deliverTimeMs / precisionMs % slotSize;
        return timerMessageKVStore.getMetricSize(slot * precisionMs, (slot + 1) * precisionMs);
    }

    public long getEnqueueBehindMessages() {
        long tmpQueueOffset = readOffset.get();
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

    public long getDequeueBehindMillis() {
        return System.currentTimeMillis() - timerGetMessageServices.get(0).checkpoint;
    }

    public TimerMessageKVStore getTimerMessageKVStore() {
        return timerMessageKVStore;
    }

    public long getMetricsIntervalMs() {
        return metricsIntervalMs;
    }

    public float getEnqueueTps() {
        // TODO get enqueue tps
    }

    public float getDequeueTps() {
        // TODO get dequeue tps
    }
}