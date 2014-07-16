package com.alibaba.rocketmq.common.stats;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.common.UtilAll;


public class StatsItem {
    // 具体的统计值
    private final AtomicLong value = new AtomicLong(0);
    // 统计次数
    private final AtomicLong times = new AtomicLong(0);
    // 最近一分钟内的镜像，数量6，10秒钟采样一次
    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();

    // 最近一小时内的镜像，数量6，10分钟采样一次
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();

    // 最近一天内的镜像，数量24，1小时采样一次
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    private final String statsName;
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;


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
                    avgpt = (sum * 1.0d) / (timesDiff);
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
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


    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService,
            Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }


    public void init() {
        // 每隔10s执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                }
                catch (Throwable e) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        // 每隔10分钟执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                }
                catch (Throwable e) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        // 每隔1小时执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                }
                catch (Throwable e) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        // 分钟整点执行
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), //
            1000 * 60, TimeUnit.MILLISECONDS);

        // 小时整点执行
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextHourTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 60, TimeUnit.MILLISECONDS);

        // 每天0点执行
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, //
            1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }


    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String.format("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            ss.getSum(),//
            ss.getTps(),//
            ss.getAvgpt()));
    }


    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String.format("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            ss.getSum(),//
            ss.getTps(),//
            ss.getAvgpt()));
    }


    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            ss.getSum(),//
            ss.getTps(),//
            ss.getAvgpt()));
    }


    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }


    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }


    public void samplingInHour() {
        synchronized (this.csListDay) {
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }


    public AtomicLong getValue() {
        return value;
    }


    public String getStatsKey() {
        return statsKey;
    }


    public String getStatsName() {
        return statsName;
    }


    public AtomicLong getTimes() {
        return times;
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
