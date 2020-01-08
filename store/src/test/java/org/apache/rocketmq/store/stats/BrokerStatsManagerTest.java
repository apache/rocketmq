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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_LATENCY;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.TOPIC_PUT_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.TOPIC_PUT_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

public class BrokerStatsManagerTest {
    private BrokerStatsManager brokerStatsManager;

    private String TOPIC = "TOPIC_TEST";
    private String GROUP_NAME = "GROUP_TEST";

    @Before
    public void init() {
        brokerStatsManager = new BrokerStatsManager("DefaultCluster");
        brokerStatsManager.start();
    }

    @After
    public void destory() {
        brokerStatsManager.shutdown();
    }

    @Test
    public void testGetStatsItem() {
        assertThat(brokerStatsManager.getStatsItem("TEST", "TEST")).isNull();
    }

    @Test
    public void testTopicPutNums() {
        brokerStatsManager.incTopicPutNums(TOPIC);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getTimes().doubleValue()).isEqualTo(1L);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getValue().doubleValue()).isEqualTo(1L);
        brokerStatsManager.incTopicPutNums(TOPIC, 2, 2);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getTimes().doubleValue()).isEqualTo(3L);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getValue().doubleValue()).isEqualTo(3L);
        brokerStatsManager.delPutNumsByTopic(TOPIC);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC)).isNull();
    }

    @Test
    public void testTopicPutSize() {
        brokerStatsManager.incTopicPutSize(TOPIC, 2);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_SIZE, TOPIC).getValue().doubleValue()).isEqualTo(2L);
        brokerStatsManager.delPutSizeByTopic(TOPIC);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_SIZE, TOPIC)).isNull();
    }

    @Test
    public void testGroupGetNums() {
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, GROUP_NAME);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, statsKey).getValue().doubleValue()).isEqualTo(1L);
        brokerStatsManager.delGetNumsByGroup(GROUP_NAME, TOPIC);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, statsKey)).isNull();
    }

    @Test
    public void testGroupGetSize() {
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 1);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, GROUP_NAME);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, statsKey).getValue().doubleValue()).isEqualTo(1L);
        brokerStatsManager.delGetSizeByGroup(GROUP_NAME, TOPIC);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, statsKey)).isNull();
    }

    @Test
    public void testGroupGetLatency() {
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        String statsKey = String.format("%d@%s@%s", 1, TOPIC, GROUP_NAME);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, statsKey).getValue().doubleValue()).isEqualTo(1L);
        brokerStatsManager.delGetLatencyByGroup(GROUP_NAME, TOPIC, 1);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, statsKey)).isNull();
    }

}
