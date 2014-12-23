package com.alibaba.rocketmq.store.stats;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.stats.MomentStatsItemSet;
import com.alibaba.rocketmq.common.stats.StatsItem;
import com.alibaba.rocketmq.common.stats.StatsItemSet;


public class BrokerStatsManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.RocketmqStatsLoggerName);
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerStatsThread"));

    public static final String TOPIC_PUT_NUMS = "TOPIC_PUT_NUMS";
    public static final String TOPIC_PUT_SIZE = "TOPIC_PUT_SIZE";
    public static final String GROUP_GET_NUMS = "GROUP_GET_NUMS";
    public static final String GROUP_GET_SIZE = "GROUP_GET_SIZE";
    public static final String SNDBCK_PUT_NUMS = "SNDBCK_PUT_NUMS";
    public static final String BROKER_PUT_NUMS = "BROKER_PUT_NUMS";
    public static final String BROKER_GET_NUMS = "BROKER_GET_NUMS";

    private final HashMap<String, StatsItemSet> statsTable = new HashMap<String, StatsItemSet>();
    private final String clusterName;

    /**
     * 读磁盘落后统计
     */
    public static final String GROUP_GET_FALL = "GROUP_GET_FALL";
    private final MomentStatsItemSet momentStatsItemSet = new MomentStatsItemSet(GROUP_GET_FALL,
        scheduledExecutorService, log);


    public BrokerStatsManager(String clusterName) {
        this.clusterName = clusterName;

        this.statsTable.put(TOPIC_PUT_NUMS, new StatsItemSet(TOPIC_PUT_NUMS, this.scheduledExecutorService,
            log));
        this.statsTable.put(TOPIC_PUT_SIZE, new StatsItemSet(TOPIC_PUT_SIZE, this.scheduledExecutorService,
            log));
        this.statsTable.put(GROUP_GET_NUMS, new StatsItemSet(GROUP_GET_NUMS, this.scheduledExecutorService,
            log));
        this.statsTable.put(GROUP_GET_SIZE, new StatsItemSet(GROUP_GET_SIZE, this.scheduledExecutorService,
            log));
        this.statsTable.put(SNDBCK_PUT_NUMS, new StatsItemSet(SNDBCK_PUT_NUMS, this.scheduledExecutorService,
            log));
        this.statsTable.put(BROKER_PUT_NUMS, new StatsItemSet(BROKER_PUT_NUMS, this.scheduledExecutorService,
            log));
        this.statsTable.put(BROKER_GET_NUMS, new StatsItemSet(BROKER_GET_NUMS, this.scheduledExecutorService,
            log));
    }


    public void start() {
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    public StatsItem getStatsItem(final String statsName, final String statsKey) {
        try {
            return this.statsTable.get(statsName).getStatsItem(statsKey);
        }
        catch (Exception e) {
        }

        return null;
    }


    public void incTopicPutNums(final String topic) {
        this.statsTable.get(TOPIC_PUT_NUMS).addValue(topic, 1, 1);
    }


    public void incTopicPutSize(final String topic, final int size) {
        this.statsTable.get(TOPIC_PUT_SIZE).addValue(topic, size, 1);
    }


    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        this.statsTable.get(GROUP_GET_NUMS).addValue(topic + "@" + group, incValue, 1);
    }


    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        this.statsTable.get(GROUP_GET_SIZE).addValue(topic + "@" + group, incValue, 1);
    }


    public void incBrokerPutNums() {
        this.statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue()
            .incrementAndGet();
    }


    public void incBrokerGetNums(final int incValue) {
        this.statsTable.get(BROKER_GET_NUMS).getAndCreateStatsItem(this.clusterName).getValue()
            .addAndGet(incValue);
    }


    public void incSendBackNums(final String group, final String topic) {
        this.statsTable.get(SNDBCK_PUT_NUMS).addValue(topic + "@" + group, 1, 1);
    }


    public double tpsGroupGetNums(final String group, final String topic) {
        return this.statsTable.get(GROUP_GET_NUMS).getStatsDataInMinute(topic + "@" + group).getTps();
    }


    public void recordDiskFallBehind(final String group, final String topic, final int queueId,
            final long fallBehind) {
        final String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.momentStatsItemSet.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }
}
