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
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ProducerStatsManager {
    private static final Logger log = LoggerFactory.getLogger(ProducerStatsManager.class);

    private static final String TOPIC_SEND_OK_TPS = "SEND_OK_TPS";
    private static final String TOPIC_SEND_FAILED_TPS = "SEND_FAILED_TPS";
    private static final String TOPIC_SEND_RT = "SEND_RT";

    private final StatsItemSet topicSendOKTPS;
    private final StatsItemSet topicSendFailedTPS;
    private final StatsItemSet topicSendRT;

    public ProducerStatsManager(final ScheduledExecutorService scheduledExecutorService) {
        this.topicSendOKTPS = new StatsItemSet(TOPIC_SEND_OK_TPS, scheduledExecutorService, log);
        this.topicSendFailedTPS = new StatsItemSet(TOPIC_SEND_FAILED_TPS, scheduledExecutorService, log);
        this.topicSendRT = new StatsItemSet(TOPIC_SEND_RT, scheduledExecutorService, log);
    }

    public void start() {
    }

    public void shutdown() {
    }

    public void incSendTimes(final String topic, final int times) {
        this.topicSendOKTPS.addValue(topic, times, 1);
    }

    public void incSendFailedTimes(final String topic, final int times) {
        this.topicSendFailedTPS.addValue(topic, times, 1);
    }

    public void incSendRT(final String topic, final long rt) {
        this.topicSendRT.addValue(topic, (int)rt, 1);
    }

    public StatsItemSet getTopicSendOKTPS() {
        return topicSendOKTPS;
    }

    public StatsItemSet getTopicSendFailedTPS() {
        return topicSendFailedTPS;
    }

    public StatsItemSet getTopicSendRT() {
        return topicSendRT;
    }
}
