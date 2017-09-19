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

package org.apache.rocketmq.rpc.impl.metrics;

import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

public class StatsItem {
    private final AtomicLong value = new AtomicLong(0);
    private final AtomicLong times = new AtomicLong(0);
    private final AtomicLong[] valueIncDistributeRegion = new AtomicLong[10];
    private final AtomicLong valueMaxInMinutes = new AtomicLong(0);
    private final AtomicLong valueMaxIn10Minutes = new AtomicLong(0);
    private final AtomicLong valueMaxInHour = new AtomicLong(0);

    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    private final ScheduledExecutorService scheduledExecutorService;
    private final String statsName;
    private final String statsKey;
    private final Logger log;

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;

        for (int i = 0; i < this.valueIncDistributeRegion.length; i++) {
            valueIncDistributeRegion[i] = new AtomicLong(0);
        }
    }

    public static boolean compareAndIncreaseOnly(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                return true;

            prev = target.get();
        }

        return false;
    }

    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            double avgpt = 0;
            long sum = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                sum = last.getValue() - first.getValue();
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                long timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) {
                    avgpt = (sum * 1.0d) / timesDiff;
                }

            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
        }

        return statsSnapshot;
    }

    public static long computNextMinutesTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 0);
        cal.add(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public static long computNextHourTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public static long computNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public void addValue(final int incValue, final int incTimes) {
        this.value.addAndGet(incValue);
        this.times.addAndGet(incTimes);
        this.setValueIncDistributeRegion(incValue);
        StatsItem.compareAndIncreaseOnly(this.valueMaxInMinutes, incValue);
        StatsItem.compareAndIncreaseOnly(this.valueMaxIn10Minutes, incValue);
        StatsItem.compareAndIncreaseOnly(this.valueMaxInHour, incValue);
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
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      samplingInMinutes();
                                                                  } catch (Throwable ignored) {
                                                                  }

                                                              }
                                                          }, Math.abs(StatsItem.computNextMinutesTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 10, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      printAtMinutes();
                                                                  } catch (Throwable ignored) {
                                                                  }
                                                              }
                                                          }, Math.abs(StatsItem.computNextMinutesTimeMillis() - System.currentTimeMillis()), //
            1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      printAtHour();
                                                                  } catch (Throwable ignored) {
                                                                  }

                                                              }
                                                          }, Math.abs(StatsItem.computNextHourTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      printAtDay();
                                                                  } catch (Throwable ignored) {
                                                                  }
                                                              }
                                                          }, Math.abs(StatsItem.computNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, //
            1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }

    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }

        valueMaxIn10Minutes.set(0);
    }

    public void samplingInHour() {
        synchronized (this.csListDay) {
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }

    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String
            .format(
                "[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.4f valueMaxInMinutes: %d valueMaxIn10Minutes: %d valueMaxInHour: %d valueIncDistributeRegion: %s",
                this.statsName,
                this.statsKey,
                ss.getSum(),
                ss.getTps(),
                ss.getAvgpt(),
                this.valueMaxInMinutes.get(),
                this.valueMaxIn10Minutes.get(),
                this.valueMaxInHour.get(),
                Arrays.toString(valueRegion())
            ));

        valueMaxInMinutes.set(0);
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String
            .format(
                "[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.4f valueMaxInMinutes: %d valueMaxIn10Minutes: %d valueMaxInHour: %d valueIncDistributeRegion: %s",
                this.statsName,
                this.statsKey,
                ss.getSum(),
                ss.getTps(),
                ss.getAvgpt(),
                this.valueMaxInMinutes.get(),
                this.valueMaxIn10Minutes.get(),
                this.valueMaxInHour.get(),
                Arrays.toString(valueRegion())
            ));

        valueMaxInHour.set(0);
    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format(
            "[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.4f valueMaxInMinutes: %d valueMaxIn10Minutes: %d valueMaxInHour: %d valueIncDistributeRegion: %s",
            this.statsName,
            this.statsKey,
            ss.getSum(),
            ss.getTps(),
            ss.getAvgpt(),
            this.valueMaxInMinutes.get(),
            this.valueMaxIn10Minutes.get(),
            this.valueMaxInHour.get(),
            Arrays.toString(valueRegion())
        ));
    }

    long[] valueRegion() {
        long[] vrs = new long[this.valueIncDistributeRegion.length];
        for (int i = 0; i < this.valueIncDistributeRegion.length; i++) {
            vrs[i] = this.valueIncDistributeRegion[i].get();
        }
        return vrs;
    }

    public AtomicLong getValue() {
        return value;
    }

    public String getStatsName() {
        return statsName;
    }

    public AtomicLong getTimes() {
        return times;
    }

    public AtomicLong[] getValueDistributeRegion() {
        return valueIncDistributeRegion;
    }

    public AtomicLong[] getValueIncDistributeRegion() {
        return valueIncDistributeRegion;
    }

    private void setValueIncDistributeRegion(long value) {
        // < 1ms
        if (value <= 0) {
            this.valueIncDistributeRegion[0].incrementAndGet();
        }
        // 1ms ~ 10ms
        else if (value < 10) {
            this.valueIncDistributeRegion[1].incrementAndGet();
        }
        // 10ms ~ 100ms
        else if (value < 100) {
            this.valueIncDistributeRegion[2].incrementAndGet();
        }
        // 100ms ~ 500ms
        else if (value < 500) {
            this.valueIncDistributeRegion[3].incrementAndGet();
        }
        // 500ms ~ 1s
        else if (value < 1000) {
            this.valueIncDistributeRegion[4].incrementAndGet();
        }
        // 1s ~ 3s
        else if (value < 3000) {
            this.valueIncDistributeRegion[5].incrementAndGet();
        }
        // 3s ~ 5s
        else if (value < 5000) {
            this.valueIncDistributeRegion[6].incrementAndGet();
        }
        // 5s ~ 10s
        else if (value < 10000) {
            this.valueIncDistributeRegion[7].incrementAndGet();
        }
        // 10s ~ 30s
        else if (value < 30000) {
            this.valueIncDistributeRegion[8].incrementAndGet();
        }
        // >= 30s
        else {
            this.valueIncDistributeRegion[9].incrementAndGet();
        }
    }

    public AtomicLong getValueMaxInHour() {
        return valueMaxInHour;
    }

    public AtomicLong getValueMaxInMinutes() {
        return valueMaxInMinutes;
    }

    public AtomicLong getValueMaxIn10Minutes() {
        return valueMaxIn10Minutes;
    }

    public static class StatsSnapshot {
        private long sum;
        private double tps;
        private double avgpt;

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public double getTps() {
            return tps;
        }

        public void setTps(double tps) {
            this.tps = tps;
        }

        public double getAvgpt() {
            return avgpt;
        }

        public void setAvgpt(double avgpt) {
            this.avgpt = avgpt;
        }
    }

    class CallSnapshot {
        private final long timestamp;
        private final long times;
        private final long value;

        public CallSnapshot(long timestamp, long times, long value) {
            super();
            this.timestamp = timestamp;
            this.times = times;
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getTimes() {
            return times;
        }

        public long getValue() {
            return value;
        }
    }

}
