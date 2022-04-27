/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.stats;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.stats.Stats;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.stats.MomentStatsItemSet;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;

public class BrokerStatsManager {

    @Deprecated public static final String QUEUE_PUT_NUMS = Stats.QUEUE_PUT_NUMS;
    @Deprecated public static final String QUEUE_PUT_SIZE = Stats.QUEUE_PUT_SIZE;
    @Deprecated public static final String QUEUE_GET_NUMS = Stats.QUEUE_GET_NUMS;
    @Deprecated public static final String QUEUE_GET_SIZE = Stats.QUEUE_GET_SIZE;
    @Deprecated public static final String TOPIC_PUT_NUMS = Stats.TOPIC_PUT_NUMS;
    @Deprecated public static final String TOPIC_PUT_SIZE = Stats.TOPIC_PUT_SIZE;
    @Deprecated public static final String GROUP_GET_NUMS = Stats.GROUP_GET_NUMS;
    @Deprecated public static final String GROUP_GET_SIZE = Stats.GROUP_GET_SIZE;
    @Deprecated public static final String SNDBCK_PUT_NUMS = Stats.SNDBCK_PUT_NUMS;
    @Deprecated public static final String BROKER_PUT_NUMS = Stats.BROKER_PUT_NUMS;
    @Deprecated public static final String BROKER_GET_NUMS = Stats.BROKER_GET_NUMS;
    @Deprecated public static final String GROUP_GET_FROM_DISK_NUMS = Stats.GROUP_GET_FROM_DISK_NUMS;
    @Deprecated public static final String GROUP_GET_FROM_DISK_SIZE = Stats.GROUP_GET_FROM_DISK_SIZE;
    @Deprecated public static final String BROKER_GET_FROM_DISK_NUMS = Stats.BROKER_GET_FROM_DISK_NUMS;
    @Deprecated public static final String BROKER_GET_FROM_DISK_SIZE = Stats.BROKER_GET_FROM_DISK_SIZE;
    // For commercial
    @Deprecated public static final String COMMERCIAL_SEND_TIMES = Stats.COMMERCIAL_SEND_TIMES;
    @Deprecated public static final String COMMERCIAL_SNDBCK_TIMES = Stats.COMMERCIAL_SNDBCK_TIMES;
    @Deprecated public static final String COMMERCIAL_RCV_TIMES = Stats.COMMERCIAL_RCV_TIMES;
    @Deprecated public static final String COMMERCIAL_RCV_EPOLLS = Stats.COMMERCIAL_RCV_EPOLLS;
    @Deprecated public static final String COMMERCIAL_SEND_SIZE = Stats.COMMERCIAL_SEND_SIZE;
    @Deprecated public static final String COMMERCIAL_RCV_SIZE = Stats.COMMERCIAL_RCV_SIZE;
    @Deprecated public static final String COMMERCIAL_PERM_FAILURES = Stats.COMMERCIAL_PERM_FAILURES;
    public static final String COMMERCIAL_OWNER = "Owner";
    // Message Size limit for one api-calling count.
    public static final double SIZE_PER_COUNT = 64 * 1024;

    @Deprecated public static final String GROUP_GET_FALL_SIZE = Stats.GROUP_GET_FALL_SIZE;
    @Deprecated public static final String GROUP_GET_FALL_TIME = Stats.GROUP_GET_FALL_TIME;
    // Pull Message Latency
    @Deprecated public static final String GROUP_GET_LATENCY = Stats.GROUP_GET_LATENCY;

    /**
     * read disk follow stats
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);
    private static final InternalLogger COMMERCIAL_LOG = InternalLoggerFactory.getLogger(LoggerName.COMMERCIAL_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "BrokerStatsThread"));
    private final ScheduledExecutorService commercialExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "CommercialStatsThread"));
    private final HashMap<String, StatsItemSet> statsTable = new HashMap<String, StatsItemSet>();
    private final String clusterName;
    private final boolean enableQueueStat;
    private final MomentStatsItemSet momentStatsItemSetFallSize = new MomentStatsItemSet(Stats.GROUP_GET_FALL_SIZE, scheduledExecutorService, log);
    private final MomentStatsItemSet momentStatsItemSetFallTime = new MomentStatsItemSet(Stats.GROUP_GET_FALL_TIME, scheduledExecutorService, log);

    public BrokerStatsManager(String clusterName, boolean enableQueueStat) {
        this.clusterName = clusterName;
        this.enableQueueStat = enableQueueStat;

        if (enableQueueStat) {
            this.statsTable.put(Stats.QUEUE_PUT_NUMS, new StatsItemSet(Stats.QUEUE_PUT_NUMS, this.scheduledExecutorService, log));
            this.statsTable.put(Stats.QUEUE_PUT_SIZE, new StatsItemSet(Stats.QUEUE_PUT_SIZE, this.scheduledExecutorService, log));
            this.statsTable.put(Stats.QUEUE_GET_NUMS, new StatsItemSet(Stats.QUEUE_GET_NUMS, this.scheduledExecutorService, log));
            this.statsTable.put(Stats.QUEUE_GET_SIZE, new StatsItemSet(Stats.QUEUE_GET_SIZE, this.scheduledExecutorService, log));
        }
        this.statsTable.put(Stats.TOPIC_PUT_NUMS, new StatsItemSet(Stats.TOPIC_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.TOPIC_PUT_SIZE, new StatsItemSet(Stats.TOPIC_PUT_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_NUMS, new StatsItemSet(Stats.GROUP_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_SIZE, new StatsItemSet(Stats.GROUP_GET_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_LATENCY, new StatsItemSet(Stats.GROUP_GET_LATENCY, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.SNDBCK_PUT_NUMS, new StatsItemSet(Stats.SNDBCK_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_PUT_NUMS, new StatsItemSet(Stats.BROKER_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_GET_NUMS, new StatsItemSet(Stats.BROKER_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_FROM_DISK_NUMS, new StatsItemSet(Stats.GROUP_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_FROM_DISK_SIZE, new StatsItemSet(Stats.GROUP_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_GET_FROM_DISK_NUMS, new StatsItemSet(Stats.BROKER_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_GET_FROM_DISK_SIZE, new StatsItemSet(Stats.BROKER_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));

        this.statsTable.put(Stats.COMMERCIAL_SEND_TIMES, new StatsItemSet(Stats.COMMERCIAL_SEND_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_RCV_TIMES, new StatsItemSet(Stats.COMMERCIAL_RCV_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_SEND_SIZE, new StatsItemSet(Stats.COMMERCIAL_SEND_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_RCV_SIZE, new StatsItemSet(Stats.COMMERCIAL_RCV_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_RCV_EPOLLS, new StatsItemSet(Stats.COMMERCIAL_RCV_EPOLLS, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_SNDBCK_TIMES, new StatsItemSet(Stats.COMMERCIAL_SNDBCK_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_PERM_FAILURES, new StatsItemSet(Stats.COMMERCIAL_PERM_FAILURES, this.commercialExecutor, COMMERCIAL_LOG));
    }

    public MomentStatsItemSet getMomentStatsItemSetFallSize() {
        return momentStatsItemSetFallSize;
    }

    public MomentStatsItemSet getMomentStatsItemSetFallTime() {
        return momentStatsItemSetFallTime;
    }

    public void start() {
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.commercialExecutor.shutdown();
    }

    public StatsItem getStatsItem(final String statsName, final String statsKey) {
        try {
            return this.statsTable.get(statsName).getStatsItem(statsKey);
        } catch (Exception e) {
        }

        return null;
    }

    public void onTopicDeleted(final String topic) {
        this.statsTable.get(Stats.TOPIC_PUT_NUMS).delValue(topic);
        this.statsTable.get(Stats.TOPIC_PUT_SIZE).delValue(topic);
        if (enableQueueStat) {
            this.statsTable.get(Stats.QUEUE_PUT_NUMS).delValueByPrefixKey(topic, "@");
            this.statsTable.get(Stats.QUEUE_PUT_SIZE).delValueByPrefixKey(topic, "@");
        }
        this.statsTable.get(Stats.GROUP_GET_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.GROUP_GET_SIZE).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.QUEUE_GET_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.QUEUE_GET_SIZE).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.SNDBCK_PUT_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.GROUP_GET_LATENCY).delValueByInfixKey(topic, "@");
        this.momentStatsItemSetFallSize.delValueByInfixKey(topic, "@");
        this.momentStatsItemSetFallTime.delValueByInfixKey(topic, "@");
    }

    public void onGroupDeleted(final String group) {
        this.statsTable.get(Stats.GROUP_GET_NUMS).delValueBySuffixKey(group, "@");
        this.statsTable.get(Stats.GROUP_GET_SIZE).delValueBySuffixKey(group, "@");
        if (enableQueueStat) {
            this.statsTable.get(Stats.QUEUE_GET_NUMS).delValueBySuffixKey(group, "@");
            this.statsTable.get(Stats.QUEUE_GET_SIZE).delValueBySuffixKey(group, "@");
        }
        this.statsTable.get(Stats.SNDBCK_PUT_NUMS).delValueBySuffixKey(group, "@");
        this.statsTable.get(Stats.GROUP_GET_LATENCY).delValueBySuffixKey(group, "@");
        this.momentStatsItemSetFallSize.delValueBySuffixKey(group, "@");
        this.momentStatsItemSetFallTime.delValueBySuffixKey(group, "@");
    }

    public void incQueuePutNums(final String topic, final Integer queueId) {
        if (enableQueueStat) {
            this.statsTable.get(Stats.QUEUE_PUT_NUMS).addValue(buildStatsKey(topic, queueId), 1, 1);
        }
    }

    public void incQueuePutNums(final String topic, final Integer queueId, int num, int times) {
        if (enableQueueStat) {
            this.statsTable.get(Stats.QUEUE_PUT_NUMS).addValue(buildStatsKey(topic, queueId), num, times);
        }
    }

    public void incQueuePutSize(final String topic, final Integer queueId, final int size) {
        if (enableQueueStat) {
            this.statsTable.get(Stats.QUEUE_PUT_SIZE).addValue(buildStatsKey(topic, queueId), size, 1);
        }
    }

    public void incQueueGetNums(final String group, final String topic, final Integer queueId, final int incValue) {
        if (enableQueueStat) {
            final String statsKey = buildStatsKey(topic, queueId, group);
            this.statsTable.get(Stats.QUEUE_GET_NUMS).addValue(statsKey, incValue, 1);
        }
    }

    public void incQueueGetSize(final String group, final String topic, final Integer queueId, final int incValue) {
        if (enableQueueStat) {
            final String statsKey = buildStatsKey(topic, queueId, group);
            this.statsTable.get(Stats.QUEUE_GET_SIZE).addValue(statsKey, incValue, 1);
        }
    }

    public void incTopicPutNums(final String topic) {
        this.statsTable.get(Stats.TOPIC_PUT_NUMS).addValue(topic, 1, 1);
    }

    public void incTopicPutNums(final String topic, int num, int times) {
        this.statsTable.get(Stats.TOPIC_PUT_NUMS).addValue(topic, num, times);
    }

    public void incTopicPutSize(final String topic, final int size) {
        this.statsTable.get(Stats.TOPIC_PUT_SIZE).addValue(topic, size, 1);
    }

    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(Stats.GROUP_GET_NUMS).addValue(statsKey, incValue, 1);
    }

    public String buildStatsKey(String topic, String group) {
        StringBuilder strBuilder;
        if (topic != null && group != null) {
            strBuilder = new StringBuilder(topic.length() + group.length() + 1);
        } else {
            strBuilder = new StringBuilder();
        }
        strBuilder.append(topic).append("@").append(group);
        return strBuilder.toString();
    }

    public String buildStatsKey(String topic, int queueId) {
        StringBuilder strBuilder;
        if (topic != null) {
            strBuilder = new StringBuilder(topic.length() + 5);
        } else {
            strBuilder = new StringBuilder();
        }
        strBuilder.append(topic).append("@").append(queueId);
        return strBuilder.toString();
    }

    public String buildStatsKey(String topic, int queueId, String group) {
        StringBuilder strBuilder;
        if (topic != null && group != null) {
            strBuilder = new StringBuilder(topic.length() + group.length() + 6);
        } else {
            strBuilder = new StringBuilder();
        }
        strBuilder.append(topic).append("@").append(queueId).append("@").append(group);
        return strBuilder.toString();
    }

    public String buildStatsKey(int queueId, String topic, String group) {
        StringBuilder strBuilder;
        if (topic != null && group != null) {
            strBuilder = new StringBuilder(topic.length() + group.length() + 6);
        } else {
            strBuilder = new StringBuilder();
        }
        strBuilder.append(queueId).append("@").append(topic).append("@").append(group);
        return strBuilder.toString();
    }

    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(Stats.GROUP_GET_SIZE).addValue(statsKey, incValue, 1);
    }

    public void incGroupGetLatency(final String group, final String topic, final int queueId, final int incValue) {
        String statsKey;
        if (enableQueueStat) {
            statsKey = buildStatsKey(queueId, topic, group);
        } else {
            statsKey = buildStatsKey(topic, group);
        }
        this.statsTable.get(Stats.GROUP_GET_LATENCY).addRTValue(statsKey, incValue, 1);
    }

    public void incBrokerPutNums() {
        this.statsTable.get(Stats.BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(1);
    }

    public void incBrokerPutNums(final int incValue) {
        this.statsTable.get(Stats.BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
    }

    public void incBrokerGetNums(final int incValue) {
        this.statsTable.get(Stats.BROKER_GET_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
    }

    public void incSendBackNums(final String group, final String topic) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(Stats.SNDBCK_PUT_NUMS).addValue(statsKey, 1, 1);
    }

    public double tpsGroupGetNums(final String group, final String topic) {
        final String statsKey = buildStatsKey(topic, group);
        return this.statsTable.get(Stats.GROUP_GET_NUMS).getStatsDataInMinute(statsKey).getTps();
    }

    public void recordDiskFallBehindTime(final String group, final String topic, final int queueId,
        final long fallBehind) {
        final String statsKey = buildStatsKey(queueId, topic, group);
        this.momentStatsItemSetFallTime.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }

    public void recordDiskFallBehindSize(final String group, final String topic, final int queueId,
        final long fallBehind) {
        final String statsKey = buildStatsKey(queueId, topic, group);
        this.momentStatsItemSetFallSize.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }

    public void incCommercialValue(final String key, final String owner, final String group,
        final String topic, final String type, final int incValue) {
        final String statsKey = buildCommercialStatsKey(owner, topic, group, type);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public String buildCommercialStatsKey(String owner, String topic, String group, String type) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(owner);
        strBuilder.append("@");
        strBuilder.append(topic);
        strBuilder.append("@");
        strBuilder.append(group);
        strBuilder.append("@");
        strBuilder.append(type);
        return strBuilder.toString();
    }

    public enum StatsType {
        SEND_SUCCESS,
        SEND_FAILURE,
        SEND_BACK,
        SEND_TIMER,
        SEND_TRANSACTION,
        RCV_SUCCESS,
        RCV_EPOLLS,
        PERM_FAILURE
    }
}
