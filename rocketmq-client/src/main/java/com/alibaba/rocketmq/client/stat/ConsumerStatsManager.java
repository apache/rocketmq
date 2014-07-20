package com.alibaba.rocketmq.client.stat;

import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.stats.StatsItemSet;


public class ConsumerStatsManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ClientLoggerName);

    private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_OK_RT = "CONSUME_OK_RT";
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_OK_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_RT = "CONSUME_OK_RT";
    private static final String TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
    private static final String TOPIC_AND_GROUP_PULL_RT = "PULL_RT";

    private final StatsItemSet topicAndGroupConsumeOKTPS;
    private final StatsItemSet topicAndGroupConsumeRT;
    private final StatsItemSet topicAndGroupConsumeFailedTPS;
    private final StatsItemSet topicAndGroupPullTPS;
    private final StatsItemSet topicAndGroupPullRT;


    public ConsumerStatsManager(final ScheduledExecutorService scheduledExecutorService) {
        this.topicAndGroupConsumeOKTPS =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);

        this.topicAndGroupConsumeRT =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_RT, scheduledExecutorService, log);

        this.topicAndGroupConsumeFailedTPS =
                new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);
    }


    public void start() {
    }


    public void shutdown() {
    }


    public void incPullRT(final String group, final String topic, final long rt) {
        this.topicAndGroupPullRT.addValue(topic + "@" + group, (int) rt, 1);
    }


    public void incPullTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }


    public void incConsumeRT(final String group, final String topic, final long rt) {
        this.topicAndGroupConsumeRT.addValue(topic + "@" + group, (int) rt, 1);
    }


    public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }


    public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }
}
