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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class TimerMessageRocksDBStore {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final String ROCKSDB_DIRECTORY = "kvStore";

    public static final int TIMER_WHEEL_TTL_DAY = 30;
    public static final int DAY_SECS = 24 * 3600;

    private final TimerMessageKVStore timerMessageKVStore;
    private final MessageStore messageStore;
    private final BrokerStatsManager brokerStatsManager;
    private final MessageStoreConfig storeConfig;
    private final TimerMetrics timerMetrics;

    private final int slotSize;
    private final int readCount;
    private final int precisionMs;

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

    public void load() {
        boolean result = timerMessageKVStore.start();
        result &= this.timerMetrics.load();
        calcTimerDistribution();
    }

    private void calcTimerDistribution() {

    }
}
