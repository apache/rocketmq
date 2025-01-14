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
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TimerMessageRocksDBStore {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final String ROCKSDB_DIRECTORY = "kvStore";

    public static final int TIMER_WHEEL_TTL_DAY = 30;
    public static final int DAY_SECS = 24 * 3600;
    public static final int DEFAULT_CAPACITY = 1024;
    public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;

    private final TimerMessageKVStore timerMessageKVStore;
    private final MessageStore messageStore;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageStoreConfig storeConfig;
    private final TimerMetrics timerMetrics;

    private final int slotSize;
    private final int readCount;
    private final int precisionMs;
    private volatile int state = INITIAL;

    private TimerEnqueueGetService timerEnqueueGetService;
    private TimerEnqueuePutService timerEnqueuePutService;
    private TimerDequeueGetService timerDequeueGetService;
    private TimerDequeuePutService[] timerDequeuePutServices;

    private BlockingQueue<TimerMessageRecord> enqueuePutQueue;
    private BlockingQueue<TimerRocksDBRequest> dequeuePutQueue;

    private long commitOffset;

    public TimerMessageRocksDBStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
        TimerMetrics timerMetrics, final BrokerStatsManager brokerStatsManager) {
        this.storeConfig = storeConfig;
        this.messageStore = messageStore;
        this.timerMetrics = timerMetrics;
        this.brokerStatsManager = brokerStatsManager;

        this.precisionMs = storeConfig.getTimerPrecisionMs();
        this.slotSize = 1000 * TIMER_WHEEL_TTL_DAY / precisionMs * DAY_SECS;
        this.readCount = storeConfig.getReadCountTimerOnRocksDB();
        this.timerMessageKVStore = new TimerMessageRocksDBStorage(Paths.get(
            storeConfig.getStorePathRootDir(), ROCKSDB_DIRECTORY).toString());
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
        this.timerEnqueueGetService.start();
        this.timerEnqueuePutService.start();
        this.timerDequeueGetService.start();
        for (TimerDequeuePutService timerDequeuePutService : timerDequeuePutServices) {
            timerDequeuePutService.start();
        }
        state = RUNNING;
    }

    public void shutdown() {
        if (state == SHUTDOWN) {
            return;
        }
        state = SHUTDOWN;
        this.timerEnqueueGetService.shutdown();
        this.timerEnqueuePutService.shutdown();
        this.timerDequeueGetService.shutdown();
        for (TimerDequeuePutService timerDequeuePutService : timerDequeuePutServices) {
            timerDequeuePutService.shutdown();
        }

        this.enqueuePutQueue.clear();
        this.dequeuePutQueue.clear();
    }

    // ----------------------------------------------------------------------------------------------------------------

    private void initService() {
        this.timerEnqueueGetService = new TimerEnqueueGetService();
        this.timerEnqueuePutService = new TimerEnqueuePutService();
        this.timerDequeueGetService = new TimerDequeueGetService();
        int getThreadNum = Math.max(storeConfig.getTimerGetMessageThreadNum(), 1);
        this.timerDequeuePutServices = new TimerDequeuePutService[getThreadNum];
        for (int i = 0; i < timerDequeuePutServices.length; i++) {
            timerDequeuePutServices[i] = new TimerDequeuePutService();
        }

        if (storeConfig.isTimerEnableDisruptor()) {
            this.enqueuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            this.dequeuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
        } else {
            this.enqueuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            this.dequeuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        }
        this.commitOffset = timerMessageKVStore.getCommitOffset();
    }
    private void calcTimerDistribution() {
        long startTime = System.currentTimeMillis();
        int slotNumber = precisionMs / 100;
        int rocksdbNumber = 0;
        for (int i = 0; i < slotNumber; i++) {
            timerMetrics.resetDistPair(i, timerMessageKVStore.getMetricSize(rocksdbNumber, rocksdbNumber + slotNumber - 1));
            rocksdbNumber += slotNumber;
        }
        long endTime = System.currentTimeMillis();
        log.debug("Total cost Time: {}", endTime - startTime);
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

    private class TimerEnqueueGetService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {

        }
    }

    private class TimerEnqueuePutService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {

        }
    }

    private class TimerDequeueGetService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {

        }
    }

    private class TimerDequeuePutService extends ServiceThread {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {

        }
    }
}
