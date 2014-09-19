package com.alibaba.rocketmq.broker.stats;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
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

    // Topic Put Nums
    private final StatsItemSet topicPutNums = new StatsItemSet(TOPIC_PUT_NUMS, this.scheduledExecutorService,
        log);

    // Topic Put Size
    private final StatsItemSet topicPutSize = new StatsItemSet(TOPIC_PUT_SIZE, this.scheduledExecutorService,
        log);

    // Topic@ConsumerGroup Get Nums
    private final StatsItemSet groupGetNums = new StatsItemSet(GROUP_GET_NUMS, this.scheduledExecutorService,
        log);

    // Topic@ConsumerGroup Get Size
    private final StatsItemSet groupGetSize = new StatsItemSet(GROUP_GET_SIZE, this.scheduledExecutorService,
        log);

    // Broker Put Nums
    private final StatsItem brokerPutNums;

    // Broker Get Nums
    private final StatsItem brokerGetNums;

    // Topic@ConsumerGroup sendback Nums
    private final StatsItemSet sndbckPutNums = new StatsItemSet(SNDBCK_PUT_NUMS,
        this.scheduledExecutorService, log);


    public BrokerStatsManager(BrokerController brokerController) {
        // Broker Put Nums
        this.brokerPutNums = new StatsItem(BROKER_PUT_NUMS, //
            brokerController.getBrokerConfig().getBrokerClusterName(), this.scheduledExecutorService, log);

        // Broker Get Nums
        this.brokerGetNums = new StatsItem(BROKER_GET_NUMS, //
            brokerController.getBrokerConfig().getBrokerClusterName(), this.scheduledExecutorService, log);
    }


    public void start() {
        this.brokerPutNums.init();
        this.brokerGetNums.init();
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    public void incTopicPutNums(final String topic) {
        this.topicPutNums.addValue(topic, 1, 1);
    }


    public void incTopicPutSize(final String topic, final int size) {
        this.topicPutSize.addValue(topic, size, 1);
    }


    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        this.groupGetNums.addValue(topic + "@" + group, incValue, 1);
    }


    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        this.groupGetSize.addValue(topic + "@" + group, incValue, 1);
    }


    public void incBrokerPutNums() {
        this.brokerPutNums.getValue().incrementAndGet();
    }


    public void incBrokerGetNums(final int incValue) {
        this.brokerGetNums.getValue().addAndGet(incValue);
    }


    public void incSendBackNums(final String group, final String topic) {
        this.sndbckPutNums.addValue(topic + "@" + group, 1, 1);
    }


    public double tpsGroupGetNums(final String group, final String topic) {
        return this.groupGetNums.getStatsDataInMinute(topic + "@" + group).getTps();
    }
}
