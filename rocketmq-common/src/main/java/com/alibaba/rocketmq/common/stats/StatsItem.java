package com.alibaba.rocketmq.common.stats;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.common.SystemClock;
import com.alibaba.rocketmq.common.UtilAll;


public class StatsItem {
    private final AtomicLong value = new AtomicLong(0);
    // 最近一分钟内的镜像，数量6，10秒钟采样一次
    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();

    // 最近一小时内的镜像，数量6，10分钟采样一次
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();

    // 最近一天内的镜像，数量24，1小时采样一次
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    private final String statsName;
    private final SystemClock systemClock;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;

    private volatile long sumInLastMinutes = 0;
    private volatile long sumInLastHour = 0;
    private volatile long sumInLastDay = 0;


    public StatsItem(String statsName, SystemClock systemClock,
            ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.systemClock = systemClock;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }


    private void init() {
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
        }, Math.abs(UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }


    private void printAtMinutes() {
        double avgps = 0;
        if (!this.csListMinute.isEmpty()) {
            CallSnapshot first = this.csListMinute.getFirst();
            CallSnapshot last = this.csListMinute.getLast();
            sumInLastMinutes = last.getCallTimesTotal() - first.getCallTimesTotal();
            avgps = (sumInLastMinutes * 1000.0d) / (last.getTimestamp() - first.getTimestamp());
        }

        log.info(String.format("[%s] Stats In Minutes, SUM: %d AVGPS: %14.4f", //
            this.statsName,//
            sumInLastMinutes, avgps));
    }


    private void printAtHour() {
        double avgps = 0;
        if (!this.csListHour.isEmpty()) {
            CallSnapshot first = this.csListHour.getFirst();
            CallSnapshot last = this.csListHour.getLast();
            sumInLastHour = last.getCallTimesTotal() - first.getCallTimesTotal();
            avgps = (sumInLastHour * 1000.0d) / (last.getTimestamp() - first.getTimestamp());
        }

        log.info(String.format("[%s] Stats In Hours, SUM: %d AVGPS: %14.4f", //
            this.statsName,//
            sumInLastHour, avgps));
    }


    private void printAtDay() {
        double avgps = 0;
        if (!this.csListDay.isEmpty()) {
            CallSnapshot first = this.csListDay.getFirst();
            CallSnapshot last = this.csListDay.getLast();
            sumInLastDay = last.getCallTimesTotal() - first.getCallTimesTotal();
            avgps = (sumInLastDay * 1000.0d) / (last.getTimestamp() - first.getTimestamp());
        }

        log.info(String.format("[%s] Stats In Day, SUM: %d AVGPS: %14.4f", //
            this.statsName,//
            sumInLastDay, avgps));
    }


    private void samplingInSeconds() {
        this.csListMinute.add(new CallSnapshot(this.systemClock.now(), this.value.get()));
        if (this.csListMinute.size() > 6) {
            this.csListMinute.removeFirst();
        }
    }


    private void samplingInMinutes() {
        this.csListHour.add(new CallSnapshot(this.systemClock.now(), this.value.get()));
        if (this.csListHour.size() > 6) {
            this.csListHour.removeFirst();
        }
    }


    private void samplingInHour() {
        this.csListDay.add(new CallSnapshot(this.systemClock.now(), this.value.get()));
        if (this.csListDay.size() > 24) {
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
}


class CallSnapshot {
    private final long timestamp;
    private final long callTimesTotal;


    public CallSnapshot(long timestamp, long callTimesTotal) {
        super();
        this.timestamp = timestamp;
        this.callTimesTotal = callTimesTotal;
    }


    public long getTimestamp() {
        return timestamp;
    }


    public long getCallTimesTotal() {
        return callTimesTotal;
    }
}
