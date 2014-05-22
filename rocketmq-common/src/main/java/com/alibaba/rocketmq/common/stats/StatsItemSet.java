package com.alibaba.rocketmq.common.stats;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.common.UtilAll;


public class StatsItemSet {
    private final ConcurrentHashMap<String/* key */, StatsItem> statsItemTable =
            new ConcurrentHashMap<String, StatsItem>(128);

    private final String statsName;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;


    public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }


    public void addValue(final String statsKey, final int incValue) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            StatsItem prev = this.statsItemTable.put(statsKey, statsItem);
            // 说明是第一次插入
            if (null == prev) {
                // 内部不需要定时，外部统一定时
                // statsItem.init();
            }
        }

        statsItem.getValue().addAndGet(incValue);
    }


    public long getSumInLastMinutes(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getSumInLastMinutes();
        }
        return 0;
    }


    public double getAvgpsInLastMinutes(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getAvgpsInLastMinutes();
        }
        return 0;
    }


    public long getSumInLastHour(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getSumInLastHour();
        }
        return 0;
    }


    public long getSumInLastDay(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getSumInLastDay();
        }
        return 0;
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


    private void printAtMinutes() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtMinutes();
        }
    }


    private void printAtHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtHour();
        }
    }


    private void printAtHalfHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtHalfHour();
        }
    }


    private void printAtDay() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtDay();
        }
    }


    private void samplingInSeconds() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInSeconds();
        }
    }


    private void samplingInMinutes() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInMinutes();
        }
    }


    private void samplingInHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInHour();
        }
    }
}
