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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.statistics.StatisticsItem;
import org.apache.rocketmq.common.statistics.StatisticsItemFormatter;
import org.apache.rocketmq.common.statistics.StatisticsItemPrinter;
import org.apache.rocketmq.common.statistics.StatisticsItemScheduledIncrementPrinter;
import org.apache.rocketmq.common.statistics.StatisticsItemScheduledPrinter;
import org.apache.rocketmq.common.statistics.StatisticsItemStateGetter;
import org.apache.rocketmq.common.statistics.StatisticsKindMeta;
import org.apache.rocketmq.common.statistics.StatisticsManager;
import org.apache.rocketmq.common.stats.MomentStatsItemSet;
import org.apache.rocketmq.common.stats.Stats;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

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

    // Send message latency
    @Deprecated public static final String TOPIC_PUT_LATENCY = "TOPIC_PUT_LATENCY";
    @Deprecated public static final String GROUP_ACK_NUMS = "GROUP_ACK_NUMS";
    @Deprecated public static final String GROUP_CK_NUMS = "GROUP_CK_NUMS";
    public static final String DLQ_PUT_NUMS = "DLQ_PUT_NUMS";
    public static final String BROKER_ACK_NUMS = "BROKER_ACK_NUMS";
    public static final String BROKER_CK_NUMS = "BROKER_CK_NUMS";
    public static final String BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC = "BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC";
    public static final String BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC = "BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC";
    public static final String SNDBCK2DLQ_TIMES = "SNDBCK2DLQ_TIMES";

    public static final String COMMERCIAL_OWNER = "Owner";

    public static final String ACCOUNT_OWNER_PARENT = "OWNER_PARENT";
    public static final String ACCOUNT_OWNER_SELF = "OWNER_SELF";

    public static final long ACCOUNT_STAT_INVERTAL = 60 * 1000;
    public static final String ACCOUNT_AUTH_TYPE = "AUTH_TYPE";

    public static final String ACCOUNT_SEND = "SEND";
    public static final String ACCOUNT_RCV = "RCV";
    public static final String ACCOUNT_SEND_BACK = "SEND_BACK";
    public static final String ACCOUNT_SEND_BACK_TO_DLQ = "SEND_BACK_TO_DLQ";
    public static final String ACCOUNT_AUTH_FAILED = "AUTH_FAILED";
    public static final String ACCOUNT_SEND_REJ = "SEND_REJ";
    public static final String ACCOUNT_REV_REJ = "RCV_REJ";

    public static final String MSG_NUM = "MSG_NUM";
    public static final String MSG_SIZE = "MSG_SIZE";
    public static final String SUCCESS_MSG_NUM = "SUCCESS_MSG_NUM";
    public static final String FAILURE_MSG_NUM = "FAILURE_MSG_NUM";
    public static final String COMMERCIAL_MSG_NUM = "COMMERCIAL_MSG_NUM";
    public static final String SUCCESS_REQ_NUM = "SUCCESS_REQ_NUM";
    public static final String FAILURE_REQ_NUM = "FAILURE_REQ_NUM";
    public static final String SUCCESS_MSG_SIZE = "SUCCESS_MSG_SIZE";
    public static final String FAILURE_MSG_SIZE = "FAILURE_MSG_SIZE";
    public static final String RT = "RT";
    public static final String INNER_RT = "INNER_RT";

    @Deprecated public static final String GROUP_GET_FALL_SIZE = Stats.GROUP_GET_FALL_SIZE;
    @Deprecated public static final String GROUP_GET_FALL_TIME = Stats.GROUP_GET_FALL_TIME;
    // Pull Message Latency
    @Deprecated public static final String GROUP_GET_LATENCY = Stats.GROUP_GET_LATENCY;

    // Consumer Register Time
    public static final String CONSUMER_REGISTER_TIME = "CONSUMER_REGISTER_TIME";
    // Producer Register Time
    public static final String PRODUCER_REGISTER_TIME = "PRODUCER_REGISTER_TIME";
    public static final String CHANNEL_ACTIVITY = "CHANNEL_ACTIVITY";
    public static final String CHANNEL_ACTIVITY_CONNECT = "CONNECT";
    public static final String CHANNEL_ACTIVITY_IDLE = "IDLE";
    public static final String CHANNEL_ACTIVITY_EXCEPTION = "EXCEPTION";
    public static final String CHANNEL_ACTIVITY_CLOSE = "CLOSE";

    /**
     * read disk follow stats
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);
    private static final Logger COMMERCIAL_LOG = LoggerFactory.getLogger(
        LoggerName.COMMERCIAL_LOGGER_NAME);
    private static final Logger ACCOUNT_LOG = LoggerFactory.getLogger(LoggerName.ACCOUNT_LOGGER_NAME);
    private static final Logger DLQ_STAT_LOG = LoggerFactory.getLogger(
        LoggerName.DLQ_STATS_LOGGER_NAME);
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledExecutorService commercialExecutor;
    private ScheduledExecutorService accountExecutor;

    private final HashMap<String, StatsItemSet> statsTable = new HashMap<>();
    private final String clusterName;
    private final boolean enableQueueStat;
    private MomentStatsItemSet momentStatsItemSetFallSize;
    private MomentStatsItemSet momentStatsItemSetFallTime;

    private final StatisticsManager accountStatManager = new StatisticsManager();
    private StateGetter producerStateGetter;
    private StateGetter consumerStateGetter;

    private BrokerConfig brokerConfig;

    public BrokerStatsManager(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.enableQueueStat = brokerConfig.isEnableDetailStat();
        initScheduleService();
        this.clusterName = brokerConfig.getBrokerClusterName();
        init();
    }

    public BrokerStatsManager(String clusterName, boolean enableQueueStat) {
        this.clusterName = clusterName;
        this.enableQueueStat = enableQueueStat;
        initScheduleService();
        init();
    }

    public void init() {
        momentStatsItemSetFallSize = new MomentStatsItemSet(GROUP_GET_FALL_SIZE,
            scheduledExecutorService, log);

        momentStatsItemSetFallTime = new MomentStatsItemSet(GROUP_GET_FALL_TIME,
            scheduledExecutorService, log);

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
        this.statsTable.put(Stats.GROUP_ACK_NUMS, new StatsItemSet(Stats.GROUP_ACK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_CK_NUMS, new StatsItemSet(Stats.GROUP_CK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_LATENCY, new StatsItemSet(Stats.GROUP_GET_LATENCY, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.TOPIC_PUT_LATENCY, new StatsItemSet(Stats.TOPIC_PUT_LATENCY, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.SNDBCK_PUT_NUMS, new StatsItemSet(Stats.SNDBCK_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(DLQ_PUT_NUMS, new StatsItemSet(DLQ_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_PUT_NUMS, new StatsItemSet(Stats.BROKER_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_GET_NUMS, new StatsItemSet(Stats.BROKER_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_ACK_NUMS, new StatsItemSet(BROKER_ACK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_CK_NUMS, new StatsItemSet(BROKER_CK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC,
            new StatsItemSet(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC,
            new StatsItemSet(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_FROM_DISK_NUMS,
            new StatsItemSet(Stats.GROUP_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.GROUP_GET_FROM_DISK_SIZE,
            new StatsItemSet(Stats.GROUP_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_GET_FROM_DISK_NUMS,
            new StatsItemSet(Stats.BROKER_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(Stats.BROKER_GET_FROM_DISK_SIZE,
            new StatsItemSet(Stats.BROKER_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));

        this.statsTable.put(SNDBCK2DLQ_TIMES,
            new StatsItemSet(SNDBCK2DLQ_TIMES, this.scheduledExecutorService, DLQ_STAT_LOG));

        this.statsTable.put(Stats.COMMERCIAL_SEND_TIMES,
            new StatsItemSet(Stats.COMMERCIAL_SEND_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_RCV_TIMES,
            new StatsItemSet(Stats.COMMERCIAL_RCV_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_SEND_SIZE,
            new StatsItemSet(Stats.COMMERCIAL_SEND_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_RCV_SIZE,
            new StatsItemSet(Stats.COMMERCIAL_RCV_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_RCV_EPOLLS,
            new StatsItemSet(Stats.COMMERCIAL_RCV_EPOLLS, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_SNDBCK_TIMES,
            new StatsItemSet(Stats.COMMERCIAL_SNDBCK_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(Stats.COMMERCIAL_PERM_FAILURES,
            new StatsItemSet(Stats.COMMERCIAL_PERM_FAILURES, this.commercialExecutor, COMMERCIAL_LOG));

        this.statsTable.put(CONSUMER_REGISTER_TIME,
            new StatsItemSet(CONSUMER_REGISTER_TIME, this.scheduledExecutorService, log));
        this.statsTable.put(PRODUCER_REGISTER_TIME,
            new StatsItemSet(PRODUCER_REGISTER_TIME, this.scheduledExecutorService, log));

        this.statsTable.put(CHANNEL_ACTIVITY, new StatsItemSet(CHANNEL_ACTIVITY, this.scheduledExecutorService, log));

        StatisticsItemFormatter formatter = new StatisticsItemFormatter();
        accountStatManager.setBriefMeta(new Pair[] {
            Pair.of(RT, new long[][] {{50, 50}, {100, 10}, {1000, 10}}),
            Pair.of(INNER_RT, new long[][] {{10, 10}, {100, 10}, {1000, 10}})});
        String[] itemNames = new String[] {
            MSG_NUM, SUCCESS_MSG_NUM, FAILURE_MSG_NUM, COMMERCIAL_MSG_NUM,
            SUCCESS_REQ_NUM, FAILURE_REQ_NUM,
            MSG_SIZE, SUCCESS_MSG_SIZE, FAILURE_MSG_SIZE,
            RT, INNER_RT};
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_SEND, itemNames, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_RCV, itemNames, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_SEND_BACK, itemNames, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_SEND_BACK_TO_DLQ, itemNames, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_SEND_REJ, itemNames, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_REV_REJ, itemNames, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.setStatisticsItemStateGetter(new StatisticsItemStateGetter() {
            @Override
            public boolean online(StatisticsItem item) {
                String[] strArr = null;
                try {
                    strArr = splitAccountStatKey(item.getStatObject());
                } catch (Exception e) {
                    log.warn("parse account stat key failed, key: {}", item.getStatObject());
                    return false;
                }

                // TODO ugly
                if (strArr == null || strArr.length < 4) {
                    return false;
                }

                String instanceId = strArr[1];
                String topic = strArr[2];
                String group = strArr[3];

                String kind = item.getStatKind();
                if (ACCOUNT_SEND.equals(kind) || ACCOUNT_SEND_REJ.equals(kind)) {
                    return producerStateGetter.online(instanceId, group, topic);
                } else if (ACCOUNT_RCV.equals(kind) || ACCOUNT_SEND_BACK.equals(kind) || ACCOUNT_SEND_BACK_TO_DLQ.equals(kind) || ACCOUNT_REV_REJ.equals(kind)) {
                    return consumerStateGetter.online(instanceId, group, topic);
                }
                return false;
            }
        });
    }

    private void initScheduleService() {
        this.scheduledExecutorService =
            ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerStatsThread", true, brokerConfig));
        this.commercialExecutor =
            ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CommercialStatsThread", true, brokerConfig));
        this.accountExecutor =
            ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("AccountStatsThread", true, brokerConfig));
    }

    public MomentStatsItemSet getMomentStatsItemSetFallSize() {
        return momentStatsItemSetFallSize;
    }

    public MomentStatsItemSet getMomentStatsItemSetFallTime() {
        return momentStatsItemSetFallTime;
    }

    public StateGetter getProducerStateGetter() {
        return producerStateGetter;
    }

    public void setProducerStateGetter(StateGetter producerStateGetter) {
        this.producerStateGetter = producerStateGetter;
    }

    public StateGetter getConsumerStateGetter() {
        return consumerStateGetter;
    }

    public void setConsumerStateGetter(StateGetter consumerStateGetter) {
        this.consumerStateGetter = consumerStateGetter;
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
        this.statsTable.get(Stats.GROUP_CK_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.GROUP_ACK_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.QUEUE_GET_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.QUEUE_GET_SIZE).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.SNDBCK_PUT_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(Stats.GROUP_GET_LATENCY).delValueByInfixKey(topic, "@");
        this.statsTable.get(Stats.TOPIC_PUT_LATENCY).delValueBySuffixKey(topic, "@");
        this.momentStatsItemSetFallSize.delValueByInfixKey(topic, "@");
        this.momentStatsItemSetFallTime.delValueByInfixKey(topic, "@");
    }

    public void onGroupDeleted(final String group) {
        this.statsTable.get(Stats.GROUP_GET_NUMS).delValueBySuffixKey(group, "@");
        this.statsTable.get(Stats.GROUP_GET_SIZE).delValueBySuffixKey(group, "@");
        this.statsTable.get(Stats.GROUP_CK_NUMS).delValueBySuffixKey(group, "@");
        this.statsTable.get(Stats.GROUP_ACK_NUMS).delValueBySuffixKey(group, "@");
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

    public void incConsumerRegisterTime(final int incValue) {
        this.statsTable.get(CONSUMER_REGISTER_TIME).addValue(this.clusterName, incValue, 1);
    }

    public void incProducerRegisterTime(final int incValue) {
        this.statsTable.get(PRODUCER_REGISTER_TIME).addValue(this.clusterName, incValue, 1);
    }

    public void incChannelConnectNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_CONNECT, 1, 1);
    }

    public void incChannelCloseNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_CLOSE, 1, 1);
    }

    public void incChannelExceptionNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_EXCEPTION, 1, 1);
    }

    public void incChannelIdleNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_IDLE, 1, 1);
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

    public void incGroupCkNums(final String group, final String topic, final int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(Stats.GROUP_CK_NUMS).addValue(statsKey, incValue, 1);
    }

    public void incGroupAckNums(final String group, final String topic, final int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(Stats.GROUP_ACK_NUMS).addValue(statsKey, incValue, 1);
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

    public void incTopicPutLatency(final String topic, final int queueId, final int incValue) {
        StringBuilder statsKey;
        if (topic != null) {
            statsKey = new StringBuilder(topic.length() + 6);
        } else {
            statsKey = new StringBuilder(6);
        }
        statsKey.append(queueId).append("@").append(topic);
        this.statsTable.get(Stats.TOPIC_PUT_LATENCY).addValue(statsKey.toString(), incValue, 1);
    }
    public void incBrokerPutNums() {
        this.statsTable.get(Stats.BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(1);
    }

    public void incBrokerPutNums(final String topic, final int incValue) {
        this.statsTable.get(Stats.BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
        incBrokerPutNumsWithoutSystemTopic(topic, incValue);
    }

    public void incBrokerGetNums(final String topic, final int incValue) {
        this.statsTable.get(Stats.BROKER_GET_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
        this.incBrokerGetNumsWithoutSystemTopic(topic, incValue);
    }

    public void incBrokerAckNums(final int incValue) {
        this.statsTable.get(BROKER_ACK_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
    }

    public void incBrokerCkNums(final int incValue) {
        this.statsTable.get(BROKER_CK_NUMS).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
    }

    public void incBrokerGetNumsWithoutSystemTopic(final String topic, final int incValue) {
        if (TopicValidator.isSystemTopic(topic)) {
            return;
        }
        this.statsTable.get(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
    }

    public void incBrokerPutNumsWithoutSystemTopic(final String topic, final int incValue) {
        if (TopicValidator.isSystemTopic(topic)) {
            return;
        }
        this.statsTable.get(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC).getAndCreateStatsItem(this.clusterName).getValue().add(incValue);
    }

    public long getBrokerGetNumsWithoutSystemTopic() {
        final StatsItemSet statsItemSet = this.statsTable.get(BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC);
        if (statsItemSet == null) {
            return 0;
        }
        final StatsItem statsItem = statsItemSet.getStatsItem(this.clusterName);
        if (statsItem == null) {
            return 0;
        }
        return statsItem.getValue().longValue();
    }

    public long getBrokerPutNumsWithoutSystemTopic() {
        final StatsItemSet statsItemSet = this.statsTable.get(BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC);
        if (statsItemSet == null) {
            return 0;
        }
        final StatsItem statsItem = statsItemSet.getStatsItem(this.clusterName);
        if (statsItem == null) {
            return 0;
        }
        return statsItem.getValue().longValue();
    }

    public void incSendBackNums(final String group, final String topic) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(Stats.SNDBCK_PUT_NUMS).addValue(statsKey, 1, 1);
    }

    public double tpsTopicPutNums(final String topic){
        return this.statsTable.get(TOPIC_PUT_NUMS).getStatsDataInMinute(topic).getTps();
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

    public void incDLQStatValue(final String key, final String owner, final String group,
        final String topic, final String type, final int incValue) {
        final String statsKey = buildCommercialStatsKey(owner, topic, group, type);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incCommercialValue(final String key, final String owner, final String group,
        final String topic, final String type, final int incValue) {
        final String statsKey = buildCommercialStatsKey(owner, topic, group, type);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incAccountValue(final String key, final String accountOwnerParent, final String accountOwnerSelf,
        final String instanceId, final String group, final String topic,
        final String msgType, final int incValue) {
        final String statsKey = buildAccountStatsKey(accountOwnerParent, accountOwnerSelf, instanceId, topic, group,
            msgType);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incAccountValue(final String key, final String accountOwnerParent, final String accountOwnerSelf,
        final String instanceId, final String group, final String topic,
        final String msgType, final String flowlimitThreshold, final int incValue) {
        final String statsKey = buildAccountStatsKey(accountOwnerParent, accountOwnerSelf, instanceId, topic, group,
            msgType, flowlimitThreshold);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incAccountValue(final String statType, final String owner, final String instanceId, final String topic,
        final String group, final String msgType,
        final long... incValues) {
        final String key = buildAccountStatKey(owner, instanceId, topic, group, msgType);
        this.accountStatManager.inc(statType, key, incValues);
    }

    public void incAccountValue(final String statType, final String owner, final String instanceId, final String topic,
        final String group, final String msgType, final String flowlimitThreshold,
        final long... incValues) {
        final String key = buildAccountStatKey(owner, instanceId, topic, group, msgType, flowlimitThreshold);
        this.accountStatManager.inc(statType, key, incValues);
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

    public String buildAccountStatsKey(String accountOwnerParent, String accountOwnerSelf, String instanceId,
        String topic, String group, String msgType) {
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(accountOwnerParent);
        strBuilder.append("@");
        strBuilder.append(accountOwnerSelf);
        strBuilder.append("@");
        strBuilder.append(instanceId);
        strBuilder.append("@");
        strBuilder.append(topic);
        strBuilder.append("@");
        strBuilder.append(group);
        strBuilder.append("@");
        strBuilder.append(msgType);
        return strBuilder.toString();
    }

    public String buildAccountStatsKey(String accountOwnerParent, String accountOwnerSelf, String instanceId,
        String topic, String group, String msgType, String flowlimitThreshold) {
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(accountOwnerParent);
        strBuilder.append("@");
        strBuilder.append(accountOwnerSelf);
        strBuilder.append("@");
        strBuilder.append(instanceId);
        strBuilder.append("@");
        strBuilder.append(topic);
        strBuilder.append("@");
        strBuilder.append(group);
        strBuilder.append("@");
        strBuilder.append(msgType);
        strBuilder.append("@");
        strBuilder.append(flowlimitThreshold);
        return strBuilder.toString();
    }

    public String buildAccountStatKey(final String owner, final String instanceId,
        final String topic, final String group,
        final String msgType) {
        final String sep = "|";
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(owner).append(sep);
        strBuilder.append(instanceId).append(sep);
        strBuilder.append(topic).append(sep);
        strBuilder.append(group).append(sep);
        strBuilder.append(msgType);
        return strBuilder.toString();
    }

    public String buildAccountStatKey(final String owner, final String instanceId,
        final String topic, final String group,
        final String msgType, String flowlimitThreshold) {
        final String sep = "|";
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(owner).append(sep);
        strBuilder.append(instanceId).append(sep);
        strBuilder.append(topic).append(sep);
        strBuilder.append(group).append(sep);
        strBuilder.append(msgType).append(sep);
        strBuilder.append(flowlimitThreshold);
        return strBuilder.toString();
    }

    public String[] splitAccountStatKey(final String accountStatKey) {
        final String sep = "\\|";
        return accountStatKey.split(sep);
    }

    private StatisticsKindMeta createStatisticsKindMeta(String name,
        String[] itemNames,
        ScheduledExecutorService executorService,
        StatisticsItemFormatter formatter,
        Logger log,
        long interval) {
        final BrokerConfig brokerConfig = this.brokerConfig;
        StatisticsItemPrinter printer = new StatisticsItemPrinter(formatter, log);
        StatisticsKindMeta kindMeta = new StatisticsKindMeta();
        kindMeta.setName(name);
        kindMeta.setItemNames(itemNames);
        kindMeta.setScheduledPrinter(
            new StatisticsItemScheduledIncrementPrinter(
                "Stat In One Minute: ",
                printer,
                executorService,
                new StatisticsItemScheduledPrinter.InitialDelay() {
                    @Override
                    public long get() {
                        return Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis());
                    }
                },
                interval,
                new String[] {MSG_NUM},
                new StatisticsItemScheduledIncrementPrinter.Valve() {
                    @Override
                    public boolean enabled() {
                        return brokerConfig != null ? brokerConfig.isAccountStatsEnable() : true;
                    }

                    @Override
                    public boolean printZeroLine() {
                        return brokerConfig != null ? brokerConfig.isAccountStatsPrintZeroValues() : true;
                    }
                }
            )
        );
        return kindMeta;
    }

    public interface StateGetter {
        boolean online(String instanceId, String group, String topic);
    }

    public enum StatsType {
        SEND_SUCCESS,
        SEND_FAILURE,

        RCV_SUCCESS,
        RCV_EPOLLS,
        SEND_BACK,
        SEND_BACK_TO_DLQ,

        SEND_ORDER,
        SEND_TIMER,
        SEND_TRANSACTION,

        PERM_FAILURE
    }
}
