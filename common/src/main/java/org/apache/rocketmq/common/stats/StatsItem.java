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

package org.apache.rocketmq.common.stats;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

public class StatsItem {
    private final LongAdder value = new LongAdder();

    private final LongAdder times = new LongAdder();

    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<>();

    private final LinkedList<CallSnapshot> csListHour = new LinkedList<>();

    private final LinkedList<CallSnapshot> csListDay = new LinkedList<>();

    private final String statsName;
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Logger logger;

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger logger) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.logger = logger;
    }

    private StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            if (csList.isEmpty()) {
                return statsSnapshot;
            }

            CallSnapshot first = csList.getFirst();
            CallSnapshot last = csList.getLast();
            long sum = last.getValue() - first.getValue();
            double tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

            long timesDiff = last.getTimes() - first.getTimes();
            double avgpt;
            if (timesDiff > 0) {
                avgpt = (sum * 1.0d) / timesDiff;
            } else {
                avgpt = 0;
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
            statsSnapshot.setTimes(timesDiff);
        }

        return statsSnapshot;
    }

    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInSeconds();
            } catch (Throwable ignored) {
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInMinutes();
            } catch (Throwable ignored) {
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInHour();
            } catch (Throwable ignored) {
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtHour();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtDay();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    public void samplingInSeconds() {
        sampling(this.csListMinute, 7, 10 * 1000);
    }

    public void samplingInMinutes() {
        sampling(this.csListHour, 7, 10 * 60 * 1000);
    }

    public void samplingInHour() {
        sampling(this.csListDay, 25, 60 * 60 * 1000);
    }
    
    private void sampling(LinkedList<CallSnapshot> csList, int maxSize, long period) {
        synchronized (csList) {
            CallSnapshot snapshot;
            long currentTimeMillis = System.currentTimeMillis();
            long times = this.times.sum();
            long value = this.value.sum();
            if (csList.size() == maxSize) {
                snapshot = csList.removeFirst();
                snapshot.setTimestamp(currentTimeMillis);
                snapshot.setTimes(times);
                snapshot.setValue(value);
            } else {
                if (csList.size() == 0) {
                    csList.add(new CallSnapshot(currentTimeMillis - period, 0, 0));
                }
                snapshot = new CallSnapshot(currentTimeMillis, times, value);
            }
            csList.add(snapshot);
        }
    }

    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        logger.info(String.format("[%s] [%s] Stats In One Minute, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        logger.info(String.format("[%s] [%s] Stats In One Hour, ", this.statsName, this.statsKey) + statPrintDetail(ss));

    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        logger.info(String.format("[%s] [%s] Stats In One Day, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("SUM: %d TPS: %.2f AVGPT: %.2f",
                ss.getSum(),
                ss.getTps(),
                ss.getAvgpt());
    }

    public LongAdder getValue() {
        return value;
    }

    public String getStatsKey() {
        return statsKey;
    }

    public String getStatsName() {
        return statsName;
    }

    public LongAdder getTimes() {
        return times;
    }
}

class CallSnapshot {
    private long timestamp;
    private long times;

    private long value;

    public CallSnapshot(long timestamp, long times, long value) {
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimes() {
        return times;
    }

    public void setTimes(long times) {
        this.times = times;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
