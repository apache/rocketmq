package org.apache.rocketmq.store.stats;

import org.apache.rocketmq.common.MixAll;

public class LmqBrokerStatsManager extends BrokerStatsManager {

    public LmqBrokerStatsManager(String clusterName, boolean enableQueueStat) {
        super(clusterName, enableQueueStat);
    }

    @Override
    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        super.incGroupGetNums(lmqGroup, lmqTopic, incValue);
    }

    @Override
    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        super.incGroupGetSize(lmqGroup, lmqTopic, incValue);
    }

    @Override
    public void incGroupGetLatency(final String group, final String topic, final int queueId, final int incValue) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        super.incGroupGetLatency(lmqGroup, lmqTopic, queueId, incValue);
    }

    @Override
    public void incSendBackNums(final String group, final String topic) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        super.incSendBackNums(lmqGroup, lmqTopic);
    }

    @Override
    public double tpsGroupGetNums(final String group, final String topic) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        return super.tpsGroupGetNums(lmqGroup, lmqTopic);
    }

    @Override
    public void recordDiskFallBehindTime(final String group, final String topic, final int queueId,
        final long fallBehind) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        super.recordDiskFallBehindTime(lmqGroup, lmqTopic, queueId, fallBehind);
    }

    @Override
    public void recordDiskFallBehindSize(final String group, final String topic, final int queueId,
        final long fallBehind) {
        String lmqGroup = group;
        String lmqTopic = topic;
        if (MixAll.isLmq(group)) {
            lmqGroup = MixAll.LMQ_PREFIX;
        }
        if (MixAll.isLmq(topic)) {
            lmqTopic = MixAll.LMQ_PREFIX;
        }
        super.recordDiskFallBehindSize(lmqGroup, lmqTopic, queueId, fallBehind);
    }

}
