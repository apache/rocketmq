package com.alibaba.rocketmq.common.stats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;


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
    }


    public void addValue(final String statsKey, final int incValue) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            StatsItem prev = this.statsItemTable.put(statsKey, statsItem);
            // 说明是第一次插入
            if (null == prev) {
                statsItem.init();
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
}
