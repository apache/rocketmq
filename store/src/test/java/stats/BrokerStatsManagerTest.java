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

package stats;

import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.store.stats.BrokerStatsManager.BROKER_PUT_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_FALL_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_FALL_TIME;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_LATENCY;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.SNDBCK_PUT_NUMS;
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
    public void testIncTopicPutNums() {
        brokerStatsManager.incTopicPutNums(TOPIC);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getTimes().doubleValue()).isEqualTo(1L);
        brokerStatsManager.incTopicPutNums(TOPIC, 2, 2);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getValue().doubleValue()).isEqualTo(3L);
    }

    @Test
    public void testIncTopicPutSize() {
        brokerStatsManager.incTopicPutSize(TOPIC, 2);
        assertThat(brokerStatsManager.getStatsItem(TOPIC_PUT_SIZE, TOPIC).getValue().doubleValue()).isEqualTo(2L);
    }

    @Test
    public void testIncGroupGetNums() {
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, GROUP_NAME);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, statsKey).getValue().doubleValue()).isEqualTo(1L);
    }

    @Test
    public void testIncGroupGetSize() {
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 1);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, GROUP_NAME);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, statsKey).getValue().doubleValue()).isEqualTo(1L);
    }

    @Test
    public void testIncGroupGetLatency() {
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        String statsKey = String.format("%d@%s@%s", 1, TOPIC, GROUP_NAME);
        assertThat(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, statsKey).getValue().doubleValue()).isEqualTo(1L);
    }

    @Test
    public void testIncBrokerPutNums() {
        brokerStatsManager.incBrokerPutNums();
        assertThat(brokerStatsManager.getStatsItem(BROKER_PUT_NUMS, "DefaultCluster").getValue().doubleValue()).isEqualTo(1L);
    }

    @Test
    public void testOnTopicDeleted() {
        brokerStatsManager.incTopicPutNums(TOPIC);
        brokerStatsManager.incTopicPutSize(TOPIC, 100);
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 100);
        brokerStatsManager.incSendBackNums(GROUP_NAME, TOPIC);
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        brokerStatsManager.recordDiskFallBehindTime(GROUP_NAME, TOPIC, 1, 11L);
        brokerStatsManager.recordDiskFallBehindSize(GROUP_NAME, TOPIC, 1, 11L);

        brokerStatsManager.onTopicDeleted(TOPIC);

        Assert.assertNull(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC));
        Assert.assertNull(brokerStatsManager.getStatsItem(TOPIC_PUT_SIZE, TOPIC));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(SNDBCK_PUT_NUMS, TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, "1@" + TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_SIZE, "1@" + TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_TIME, "1@" + TOPIC + "@" + GROUP_NAME));
    }

    @Test
    public void testOnGroupDeleted(){
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 100);
        brokerStatsManager.incSendBackNums(GROUP_NAME, TOPIC);
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        brokerStatsManager.recordDiskFallBehindTime(GROUP_NAME, TOPIC, 1, 11L);
        brokerStatsManager.recordDiskFallBehindSize(GROUP_NAME, TOPIC, 1, 11L);

        brokerStatsManager.onGroupDeleted(GROUP_NAME);

        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(SNDBCK_PUT_NUMS, TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, "1@" + TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_SIZE, "1@" + TOPIC + "@" + GROUP_NAME));
        Assert.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_TIME, "1@" + TOPIC + "@" + GROUP_NAME));
    }
}
