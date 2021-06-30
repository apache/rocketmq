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

package org.apache.rocketmq.client.stat;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.common.stats.StatsSnapshot;

public class ConsumerStatsManager {
    private static final InternalLogger log = ClientLogger.getLog();

    private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
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
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);

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
        this.topicAndGroupPullRT.addRTValue(topic + "@" + group, (int) rt, 1);
    }

    public void incPullTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeRT(final String group, final String topic, final long rt) {
        this.topicAndGroupConsumeRT.addRTValue(topic + "@" + group, (int) rt, 1);
    }

    public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public ConsumeStatus consumeStatus(final String group, final String topic) {
        ConsumeStatus cs = new ConsumeStatus();
        {
            StatsSnapshot ss = this.getPullRT(group, topic);
            if (ss != null) {
                cs.setPullRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getPullTPS(group, topic);
            if (ss != null) {
                cs.setPullTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeRT(group, topic);
            if (ss != null) {
                cs.setConsumeRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeOKTPS(group, topic);
            if (ss != null) {
                cs.setConsumeOKTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeFailedTPS(group, topic);
            if (ss != null) {
                cs.setConsumeFailedTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
            if (ss != null) {
                cs.setConsumeFailedMsgs(ss.getSum());
            }
        }

        return cs;
    }

    private StatsSnapshot getPullRT(final String group, final String topic) {
        return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getPullTPS(final String group, final String topic) {
        return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getConsumeRT(final String group, final String topic) {
        StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
        if (0 == statsData.getSum()) {
            statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
        }

        return statsData;
    }

    private StatsSnapshot getConsumeOKTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getConsumeFailedTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
    }
}
