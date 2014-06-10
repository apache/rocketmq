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

    private volatile long sumInLastMinutes = 0;
    // 在最近一分钟内，每秒的平均值
    private volatile double avgpsecondInLastMinutes = 0;
    // 在最近一分钟内，每次的平均值
    private volatile double avgptimesInLastMinutes = 0;
    private volatile long sumInLastHour = 0;
    private volatile long sumInLastDay = 0;


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

        // 半小时整点执行
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHalfHour();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextHalfHourTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 30, TimeUnit.MILLISECONDS);

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
        }, Math.abs(UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }


    public void printAtMinutes() {
        double avgpsecond = 0;
        double avgptimes = 0;
        if (!this.csListMinute.isEmpty()) {
            CallSnapshot first = this.csListMinute.getFirst();
            CallSnapshot last = this.csListMinute.getLast();
            sumInLastMinutes = last.getValue() - first.getValue();
            avgpsecond = (sumInLastMinutes * 1000.0d) / (last.getTimestamp() - first.getTimestamp());
            this.avgpsecondInLastMinutes = avgpsecond;

            long timesDiff = last.getTimes() - first.getTimes();
            if (timesDiff > 0) {
                avgptimes = (sumInLastMinutes * 1.0d) / (timesDiff);
            }

            this.avgptimesInLastMinutes = avgptimes;
        }

        log.info(String.format("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            sumInLastMinutes,//
            avgpsecond,//
            avgptimes));
    }


    public void printAtHour() {
        double avgpsecond = 0;
        double avgptimes = 0;
        if (!this.csListHour.isEmpty()) {
            CallSnapshot first = this.csListHour.getFirst();
            CallSnapshot last = this.csListHour.getLast();
            sumInLastHour = last.getValue() - first.getValue();
            avgpsecond = (sumInLastHour * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

            long timesDiff = last.getTimes() - first.getTimes();
            if (timesDiff > 0) {
                avgptimes = (sumInLastHour * 1.0d) / (timesDiff);
            }
        }

        log.info(String.format("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            sumInLastHour,//
            avgpsecond,//
            avgptimes));
    }


    public void printAtHalfHour() {
        double avgpsecond = 0;
        double avgptimes = 0;
        long sumInLastHalfHour = 0;
        if (!this.csListHour.isEmpty()) {
            CallSnapshot first = this.csListHour.get(3);
            if (null == first)
                return;

            CallSnapshot last = this.csListHour.getLast();
            sumInLastHalfHour = last.getValue() - first.getValue();
            avgpsecond = (sumInLastHalfHour * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

            long timesDiff = last.getTimes() - first.getTimes();
            if (timesDiff > 0) {
                avgptimes = (sumInLastHalfHour * 1.0d) / (timesDiff);
            }
        }

        log.info(String.format("[%s] [%s] Stats In Half An Hour, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            sumInLastHalfHour, //
            avgpsecond, //
            avgptimes));
    }


    public void printAtDay() {
        double avgpsecond = 0;
        double avgptimes = 0;
        if (!this.csListDay.isEmpty()) {
            CallSnapshot first = this.csListDay.getFirst();
            CallSnapshot last = this.csListDay.getLast();
            sumInLastDay = last.getValue() - first.getValue();
            avgpsecond = (sumInLastDay * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

            long timesDiff = last.getTimes() - first.getTimes();
            if (timesDiff > 0) {
                avgptimes = (sumInLastDay * 1.0d) / (timesDiff);
            }
        }

        log.info(String.format("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            sumInLastDay,//
            avgpsecond, //
            avgptimes));
    }


    public void samplingInSeconds() {
        this.csListMinute
            .add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
        if (this.csListMinute.size() > 7) {
            this.csListMinute.removeFirst();
        }

        double avgpsecond = 0;
        double avgptimes = 0;
        if (!this.csListMinute.isEmpty()) {
            CallSnapshot first = this.csListMinute.getFirst();
            CallSnapshot last = this.csListMinute.getLast();
            sumInLastMinutes = last.getValue() - first.getValue();
            avgpsecond = (sumInLastMinutes * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

            long timesDiff = last.getTimes() - first.getTimes();
            if (timesDiff > 0) {
                avgptimes = (sumInLastMinutes * 1.0d) / (timesDiff);
            }
        }

        this.avgpsecondInLastMinutes = avgpsecond;
        this.avgptimesInLastMinutes = avgptimes;
    }


    public void samplingInMinutes() {
        this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
        if (this.csListHour.size() > 7) {
            this.csListHour.removeFirst();
        }
    }


    public void samplingInHour() {
        this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
        if (this.csListDay.size() > 25) {
            this.csListDay.removeFirst();
        }
    }


    public AtomicLong getValue() {
        return value;
    }


    public long getSumInLastMinutes() {
        return sumInLastMinutes;
    }


    public void setSumInLastMinutes(long sumInLastMinutes) {
        this.sumInLastMinutes = sumInLastMinutes;
    }


    public long getSumInLastHour() {
        return sumInLastHour;
    }


    public void setSumInLastHour(long sumInLastHour) {
        this.sumInLastHour = sumInLastHour;
    }


    public long getSumInLastDay() {
        return sumInLastDay;
    }


    public void setSumInLastDay(long sumInLastDay) {
        this.sumInLastDay = sumInLastDay;
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


    public double getAvgptimesInLastMinutes() {
        return avgptimesInLastMinutes;
    }


    public void setAvgptimesInLastMinutes(double avgptimesInLastMinutes) {
        this.avgptimesInLastMinutes = avgptimesInLastMinutes;
    }


    public double getAvgpsecondInLastMinutes() {
        return avgpsecondInLastMinutes;
    }


    public void setAvgpsecondInLastMinutes(double avgpsecondInLastMinutes) {
        this.avgpsecondInLastMinutes = avgpsecondInLastMinutes;
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
