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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;

public class LmqBrokerStatsManager extends BrokerStatsManager {

    private final BrokerConfig brokerConfig;

    public LmqBrokerStatsManager(BrokerConfig brokerConfig) {
        super(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat());
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        super.incGroupGetNums(getAdjustedGroup(group), getAdjustedTopic(topic), incValue);
    }

    @Override
    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        super.incGroupGetSize(getAdjustedGroup(group), getAdjustedTopic(topic), incValue);
    }

    @Override
    public void incGroupAckNums(final String group, final String topic, final int incValue) {
        super.incGroupAckNums(getAdjustedGroup(group), getAdjustedTopic(topic), incValue);
    }

    @Override
    public void incGroupCkNums(final String group, final String topic, final int incValue) {
        super.incGroupCkNums(getAdjustedGroup(group), getAdjustedTopic(topic), incValue);
    }

    @Override
    public void incGroupGetLatency(final String group, final String topic, final int queueId, final int incValue) {
        super.incGroupGetLatency(getAdjustedGroup(group), getAdjustedTopic(topic), queueId, incValue);
    }

    @Override
    public void incSendBackNums(final String group, final String topic) {
        super.incSendBackNums(getAdjustedGroup(group), getAdjustedTopic(topic));
    }

    @Override
    public double tpsGroupGetNums(final String group, final String topic) {
        return super.tpsGroupGetNums(getAdjustedGroup(group), getAdjustedTopic(topic));
    }

    @Override
    public void recordDiskFallBehindTime(final String group, final String topic, final int queueId,
        final long fallBehind) {
        super.recordDiskFallBehindTime(getAdjustedGroup(group), getAdjustedTopic(topic), queueId, fallBehind);
    }

    @Override
    public void recordDiskFallBehindSize(final String group, final String topic, final int queueId,
        final long fallBehind) {
        super.recordDiskFallBehindSize(getAdjustedGroup(group), getAdjustedTopic(topic), queueId, fallBehind);
    }

    private String getAdjustedGroup(String group) {
        return !brokerConfig.isEnableLmqStats() && MixAll.isLmq(group) ? MixAll.LMQ_PREFIX : group;
    }

    private String getAdjustedTopic(String topic) {
        return !brokerConfig.isEnableLmqStats() && MixAll.isLmq(topic) ? MixAll.LMQ_PREFIX : topic;
    }

}
