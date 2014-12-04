package com.alibaba.rocketmq.common.stats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.common.UtilAll;


public class MomentStatsItem {
    // 具体的统计值
    private final AtomicLong value = new AtomicLong(0);

    private final String statsName;
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;


    public MomentStatsItem(String statsName, String statsKey,
            ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }


    public void init() {
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
            1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }


    public void printAtMinutes() {
        log.info(String.format("[%s] [%s] Stats Every 5 Minutes, Value: %d", //
            this.statsName,//
            this.statsKey,//
            this.value.get()));
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
}
