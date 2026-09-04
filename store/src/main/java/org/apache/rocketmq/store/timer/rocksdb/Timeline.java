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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TIMER_ROLL_LABEL;
import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TIMER_COLUMN_FAMILY;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_DELETE;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_PUT;
import static org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord.TIMER_ROCKSDB_UPDATE;

public class Timeline {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final String DELETE_KEY_SPLIT = "+";
    private static final int ORIGIN_CAPACITY = 100000;
    private static final int BATCH_SIZE = 1000, MAX_BATCH_SIZE_FROM_ROCKSDB = 8000;
    private static final int INITIAL = 0, RUNNING = 1, SHUTDOWN = 2;
    private volatile int state = INITIAL;
    private final AtomicLong commitOffset = new AtomicLong(0);
    private final MessageStore messageStore;
    private final MessageStoreConfig storeConfig;
    private final TimerMessageStore timerMessageStore;
    private final MessageRocksDBStorage messageRocksDBStorage;
    private final TimerMessageRocksDBStore timerMessageRocksDBStore;
    private final long precisionMs;
    private final TimerMetrics timerMetrics;

    private TimelineIndexBuildService timelineIndexBuildService;
    private TimelineForwardService timelineForwardService;
    private TimelineRollService timelineRollService;
    private TimelineDeleteService timelineDeleteService;
    private BlockingQueue<TimerRocksDBRecord> originTimerMsgQueue;

    public Timeline(final MessageStore messageStore, final MessageRocksDBStorage messageRocksDBStorage, final TimerMessageRocksDBStore timerMessageRocksDBStore, final TimerMetrics timerMetrics) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getMessageStoreConfig();
        this.timerMessageStore = messageStore.getTimerMessageStore();
        this.messageRocksDBStorage = messageRocksDBStorage;
        this.timerMessageRocksDBStore = timerMessageRocksDBStore;
        this.precisionMs = timerMessageRocksDBStore.precisionMs;
        this.timerMetrics = timerMetrics;
        initService();
    }

    private void initService() {
        this.timelineIndexBuildService = new TimelineIndexBuildService();
        this.timelineForwardService = new TimelineForwardService();
        if (storeConfig.isTimerEnableDisruptor()) {
            this.originTimerMsgQueue = new DisruptorBlockingQueue<>(ORIGIN_CAPACITY);
        } else {
            this.originTimerMsgQueue = new LinkedBlockingDeque<>(ORIGIN_CAPACITY);
        }
        this.timelineRollService = new TimelineRollService();
        this.timelineDeleteService = new TimelineDeleteService();
    }

    public void start() {
        if (this.state == RUNNING) {
            return;
        }
        this.commitOffset.set(this.timerMessageRocksDBStore.getReadOffset().get());
        this.timelineIndexBuildService.start();
        this.timelineForwardService.start();
        this.timelineRollService.start();
        this.timelineDeleteService.start();
        this.state = RUNNING;
        log.info("Timeline start success, start commitOffset: {}", this.commitOffset.get());
    }

    public void shutDown() {
        if (this.state != RUNNING || this.state == SHUTDOWN) {
            return;
        }
        if (null != this.timelineIndexBuildService) {
            this.timelineIndexBuildService.shutdown();
        }
        if (null != this.timelineForwardService) {
            this.timelineForwardService.shutdown();
        }
        if (null != this.timelineRollService) {
            this.timelineRollService.shutdown();
        }
        if (null != this.timelineDeleteService) {
            this.timelineDeleteService.shutdown();
        }
        this.state = SHUTDOWN;
        log.info("Timeline shutdown success");
    }

    public void putRecord(TimerRocksDBRecord timerRecord) throws InterruptedException {
        if (null == timerRecord) {
            return;
        }
        while (!originTimerMsgQueue.offer(timerRecord, 3, TimeUnit.SECONDS)) {
            if (null != timerRecord.getMessageExt()) {
                logError.error("Timeline originTimerMsgQueue put record failed, topic: {}, uniqKey: {}", timerRecord.getMessageExt().getTopic(), timerRecord.getUniqKey());
            } else {
                logError.error("Timeline originTimerMsgQueue put record failed, uniqKey: {}", timerRecord.getUniqKey());
            }
        }
    }

    public void putDeleteRecord(long delayTime, String uniqKey, long offsetPy, int sizePy, long queueOffset, MessageExt messageExt) throws InterruptedException {
        if (delayTime <= 0L || StringUtils.isEmpty(uniqKey) || offsetPy < 0L || sizePy < 0 || queueOffset < 0L || null == messageExt) {
            log.info("Timeline putDeleteRecord param error, delayTime: {}, uniqKey: {}, offsetPy: {}, sizePy: {}, queueOffset: {}, messageExt: {}", delayTime, uniqKey, offsetPy, sizePy, queueOffset, messageExt);
            return;
        }
        while (!originTimerMsgQueue.offer(new TimerRocksDBRecord(delayTime, uniqKey, offsetPy, sizePy, queueOffset, messageExt), 3, TimeUnit.SECONDS)) {
            log.error("Timeline putDeleteRecord originTimerMsgQueue put delete record failed, uniqKey: {}", uniqKey);
        }
    }

    public void addMetric(MessageExt msg, int value) {
        if (null == msg || null == msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC)) {
            return;
        }
        if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_ENQUEUE_MS) && NumberUtils.toLong(msg.getProperty(MessageConst.PROPERTY_TIMER_ENQUEUE_MS)) == Long.MAX_VALUE) {
            return;
        }
        timerMetrics.addAndGet(msg, value);
    }

    private String getServiceThreadName() {
        String brokerIdentifier = "";
        if (Timeline.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) Timeline.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    private void recallToTimeWheel(TimerRocksDBRecord tr) {
        if (!messageStore.getMessageStoreConfig().isTimerRecallToTimeWheelEnable()) {
            return;
        }
        if (null == tr || null == tr.getMessageExt()) {
            return;
        }
        try {
            timerMessageStore.doEnqueue(tr.getOffsetPy(), tr.getSizePy(), tr.getDelayTime(), tr.getMessageExt(), true);
        } catch (Exception e) {
            log.error("Timeline recallToTimeWheel error: {}", e.getMessage());
        }
    }

    private boolean scanRecordsToQueue(long checkpoint, long checkRange, BlockingQueue<List<TimerRocksDBRecord>> queue) {
        if (checkpoint <= 0L || checkRange <= 0L || null == queue) {
            logError.error("Timeline scanRecordsToQueue param error, checkpoint: {}, checkRange: {}, queue: {}", checkpoint, checkRange, queue);
            return false;
        }
        if (storeConfig.isTimerStopDequeue()) {
            logError.info("Timeline scanRecordsToQueue storeConfig isTimerStopDequeue is true, stop to scan records to queue");
            return false;
        }
        long count = 0;
        byte[] lastKey = null;
        while (true) {
            try {
                List<TimerRocksDBRecord> trs = messageRocksDBStorage.scanRecordsForTimer(TIMER_COLUMN_FAMILY, checkpoint, checkpoint + checkRange, MAX_BATCH_SIZE_FROM_ROCKSDB, lastKey);
                if (null == trs || CollectionUtils.isEmpty(trs)) {
                    break;
                }
                count += trs.size();
                boolean hasMoreData = trs.size() >= MAX_BATCH_SIZE_FROM_ROCKSDB;
                lastKey = hasMoreData ? trs.get(trs.size() - 1).getKeyBytes() : null;
                if (null == lastKey) {
                    trs.get(trs.size() - 1).setCheckPoint(checkpoint + checkRange);
                }
                while (!queue.offer(trs, 3, TimeUnit.SECONDS)) {
                    log.debug("Timeline scanRecordsToQueue offer to queue error, queue size: {}, records size: {}", queue.size(), trs.size());
                }
                if (!hasMoreData) {
                    break;
                }
            } catch (Exception e) {
                logError.error("Timeline scanRecordsToQueue error: {}", e.getMessage());
                return false;
            }
        }
        log.info("Timeline scan records from rocksdb, checkpoint: {}, records size: {}", checkpoint, count);
        return true;
    }

    public class TimelineIndexBuildService extends ServiceThread {
        private final Logger log = Timeline.log;
        private List<TimerRocksDBRecord> trs;

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            trs = new ArrayList<>(BATCH_SIZE);
            while (!this.isStopped() || !originTimerMsgQueue.isEmpty()) {
                try {
                    buildTimelineIndex();
                } catch (Exception e) {
                    logError.error("Timeline error occurred in: {}, error: {}", getServiceName(), e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        private void buildTimelineIndex() throws InterruptedException {
            pollTimerMessageRecords();
            if (CollectionUtils.isEmpty(trs)) {
                return;
            }
            List<TimerRocksDBRecord> cudlist = new ArrayList<>();
            for (TimerRocksDBRecord tr : trs) {
                try {
                    MessageExt messageExt = tr.getMessageExt();
                    if (null == messageExt) {
                        logError.error("Timeline TimelineIndexBuildService buildTimelineIndex error, messageExt is null");
                        continue;
                    }
                    String timerDelUniqKey = messageExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY);
                    if (!StringUtils.isEmpty(timerDelUniqKey)) {
                        tr.setUniqKey(extractUniqKey(timerDelUniqKey));
                        tr.setActionFlag(TIMER_ROCKSDB_DELETE);
                        cudlist.add(tr);
                        addMetric(messageExt, -1);
                        recallToTimeWheel(tr);
                    } else if (TimerMessageRocksDBStore.isExpired(tr.getDelayTime())) {
                        timerMessageRocksDBStore.putRealTopicMessage(tr.getMessageExt());
                    } else if (!StringUtils.isEmpty(messageExt.getProperty(PROPERTY_TIMER_ROLL_LABEL))) {
                        tr.setActionFlag(TIMER_ROCKSDB_UPDATE);
                        cudlist.add(tr);
                    } else {
                        tr.setActionFlag(TIMER_ROCKSDB_PUT);
                        cudlist.add(tr);
                        addMetric(messageExt, 1);
                    }
                } catch (Exception e) {
                    logError.error("Timeline TimelineIndexBuildService buildTimelineIndex deal trs error", e);
                }
            }
            if (!CollectionUtils.isEmpty(cudlist)) {
                messageRocksDBStorage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, cudlist);
            }
            synCommitOffset(trs);
            trs.clear();
        }

        private String extractUniqKey(String deleteKey) throws IllegalArgumentException {
            if (StringUtils.isEmpty(deleteKey)) {
                throw new IllegalArgumentException("deleteKey is empty");
            }
            int separatorIndex = deleteKey.indexOf(DELETE_KEY_SPLIT);
            if (separatorIndex == -1) {
                return deleteKey;
            }
            return deleteKey.substring(separatorIndex + 1);
        }

        private void pollTimerMessageRecords() throws InterruptedException {
            TimerRocksDBRecord firstReq = originTimerMsgQueue.poll(100, TimeUnit.MILLISECONDS);
            if (null != firstReq) {
                trs.add(firstReq);
                while (true) {
                    TimerRocksDBRecord tmpReq = originTimerMsgQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (null == tmpReq) {
                        break;
                    }
                    trs.add(tmpReq);
                    if (trs.size() >= BATCH_SIZE) {
                        break;
                    }
                }
            }
        }

        private void synCommitOffset(List<TimerRocksDBRecord> trs) {
            if (CollectionUtils.isEmpty(trs)) {
                return;
            }
            long lastQueueOffset = messageRocksDBStorage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.SYS_TOPIC_SCAN_OFFSET_CHECK_POINT);
            long queueOffset = trs.get(trs.size() - 1).getQueueOffset() + 1L;
            if (queueOffset > lastQueueOffset) {
                commitOffset.set(queueOffset);
                messageRocksDBStorage.writeCheckPointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.SYS_TOPIC_SCAN_OFFSET_CHECK_POINT, commitOffset.get());
            }
        }
    }

    private class TimelineForwardService extends ServiceThread {
        private final Logger log = Timeline.log;
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            long checkpoint = messageRocksDBStorage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.TIMELINE_CHECK_POINT);
            log.info(this.getServiceName() + " service start, checkpoint: {}", checkpoint);
            while (!this.isStopped()) {
                try {
                    if (!timelineForward(checkpoint, precisionMs)) {
                        waitForRunning(100L);
                    } else {
                        checkpoint += precisionMs;
                    }
                } catch (Exception e) {
                    logError.error("Timeline error occurred in " + getServiceName(), e);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        private boolean timelineForward(long checkpoint, long checkRange) {
            if (checkpoint > System.currentTimeMillis()) {
                return false;
            }
            try {
                long begin = System.currentTimeMillis();
                boolean result = scanRecordsToQueue(checkpoint, checkRange, timerMessageRocksDBStore.getExpiredMessageQueue());
                log.info("Timeline TimelineForwardService timelineForward scanRecordsToQueue end, result: {}, checkpoint: {}, checkRange: {}, checkDelay: {}, cost: {}", result, checkpoint, checkRange, System.currentTimeMillis() - checkpoint, System.currentTimeMillis() - begin);
                return result;
            } catch (Exception e) {
                logError.error("Timeline TimelineForwardService timelineForward error: {}", e.getMessage());
                return false;
            }
        }
    }

    private class TimelineRollService extends ServiceThread {
        private final Logger log = Timeline.log;
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                int rollIntervalHour = 1;
                int rollRangeHour = 2;
                try {
                    if (storeConfig.getTimerRocksDBRollIntervalHours() > 0) {
                        rollIntervalHour = storeConfig.getTimerRocksDBRollIntervalHours();
                    }
                    if (storeConfig.getTimerRocksDBRollRangeHours() > 0) {
                        rollRangeHour = storeConfig.getTimerRocksDBRollRangeHours();
                    }
                    this.waitForRunning(TimeUnit.HOURS.toMillis(rollIntervalHour));
                    if (stopped) {
                        log.info(this.getServiceName() + " service end");
                        return;
                    }
                } catch (Exception e) {
                    logError.error("Timeline TimelineRollService wait error: {}", e.getMessage());
                }
                long rollCheckpoint = System.currentTimeMillis();
                try {
                    log.info("Timeline TimelineRollService start roll rollCheckpoint: {}", rollCheckpoint);
                    while (!scanRecordsToQueue(rollCheckpoint + TimeUnit.HOURS.toMillis(rollRangeHour),
                            TimeUnit.SECONDS.toMillis(storeConfig.getTimerMaxDelaySec()),
                            timerMessageRocksDBStore.getRollMessageQueue())) {
                        logError.error("Timeline TimelineRollService scanRecordsToQueue error.");
                        Thread.sleep(200);
                    }
                    log.info("Timeline TimelineRollService roll records success, lastRollTime: {}, rollCheckpoint: {}, cost: {}", rollCheckpoint, rollCheckpoint, System.currentTimeMillis() - rollCheckpoint);
                } catch (Exception e) {
                    logError.error("Timeline TimelineRollService failed error: {}", e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }
    }

    private class TimelineDeleteService extends ServiceThread {
        private final Logger log = Timeline.log;
        private long lastDeleteCheckPoint = 0L;
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }
        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    this.waitForRunning(TimeUnit.MINUTES.toMillis(30));
                    if (stopped) {
                        log.info(this.getServiceName() + " service end");
                        return;
                    }
                } catch (Exception e) {
                    logError.error("Timeline TimelineDeleteService wait error: {}", e.getMessage());
                }
                try {
                    long checkpoint = messageRocksDBStorage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, MessageRocksDBStorage.TIMELINE_CHECK_POINT);
                    if (lastDeleteCheckPoint == checkpoint) {
                        continue;
                    }
                    messageRocksDBStorage.deleteRecordsForTimer(TIMER_COLUMN_FAMILY, checkpoint - TimeUnit.HOURS.toMillis(168), checkpoint - TimeUnit.MINUTES.toMillis(30));
                    lastDeleteCheckPoint = checkpoint;
                } catch (Exception e) {
                    logError.error("Timeline TimelineDeleteService delete failed, lastDeleteCheckPoint: {} error: {}", lastDeleteCheckPoint, e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }
    }

}
