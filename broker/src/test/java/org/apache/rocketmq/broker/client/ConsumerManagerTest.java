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

package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerTest {

    private ClientChannelInfo clientChannelInfo;

    @Mock
    private Channel channel;

    private ConsumerManager consumerManager;

    @Mock
    private BrokerController brokerController;

    private final BrokerConfig brokerConfig = new BrokerConfig();

    private static final String GROUP = "DEFAULT_GROUP";

    private static final String CLIENT_ID = "1";

    private static final int VERSION = 1;

    private static final String TOPIC = "DEFAULT_TOPIC";

    @Before
    public void before() {
        clientChannelInfo = new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, VERSION);
        DefaultConsumerIdsChangeListener defaultConsumerIdsChangeListener = new DefaultConsumerIdsChangeListener(brokerController);
        BrokerStatsManager brokerStatsManager = new BrokerStatsManager(brokerConfig);
        consumerManager = spy(new ConsumerManager(defaultConsumerIdsChangeListener, brokerStatsManager, brokerConfig, brokerController));
        ConsumerFilterManager consumerFilterManager = mock(ConsumerFilterManager.class);
        when(brokerController.getConsumerFilterManager()).thenReturn(consumerFilterManager);
    }

    @Test
    public void compensateBasicConsumerInfoTest() {
        ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        assertThat(consumerGroupInfo).isNull();

        consumerManager.compensateBasicConsumerInfo(GROUP, ConsumeType.CONSUME_ACTIVELY, MessageModel.BROADCASTING);
        consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        assertThat(consumerGroupInfo).isNotNull();
        assertThat(consumerGroupInfo.getConsumeType()).isEqualTo(ConsumeType.CONSUME_ACTIVELY);
        assertThat(consumerGroupInfo.getMessageModel()).isEqualTo(MessageModel.BROADCASTING);
    }

    @Test
    public void compensateSubscribeDataTest() {
        ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        assertThat(consumerGroupInfo).isNull();

        consumerManager.compensateSubscribeData(GROUP, TOPIC, new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL));
        consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        assertThat(consumerGroupInfo).isNotNull();
        assertThat(consumerGroupInfo.getSubscriptionTable().size()).isEqualTo(1);
        SubscriptionData subscriptionData = consumerGroupInfo.getSubscriptionTable().get(TOPIC);
        assertThat(subscriptionData).isNotNull();
        assertThat(subscriptionData.getTopic()).isEqualTo(TOPIC);
        assertThat(subscriptionData.getSubString()).isEqualTo(SubscriptionData.SUB_ALL);
    }

    @Test
    public void registerConsumerTest() {
        register();
        final Set<SubscriptionData> subList = new HashSet<>();
        SubscriptionData subscriptionData = new SubscriptionData(TOPIC, "*");
        subList.add(subscriptionData);
        consumerManager.registerConsumer(GROUP, clientChannelInfo, ConsumeType.CONSUME_PASSIVELY,
            MessageModel.BROADCASTING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, subList, true);
        verify(consumerManager, never()).callConsumerIdsChangeListener(eq(ConsumerGroupEvent.CHANGE), any(), any());
        assertThat(consumerManager.getConsumerTable().get(GROUP)).isNotNull();
    }

    @Test
    public void unregisterConsumerTest() {
        // register
        register();

        // unregister
        consumerManager.unregisterConsumer(GROUP, clientChannelInfo, true);
        verify(consumerManager, never()).callConsumerIdsChangeListener(eq(ConsumerGroupEvent.CHANGE), any(), any());
        assertThat(consumerManager.getConsumerTable().get(GROUP)).isNull();
    }

    @Test
    public void findChannelTest() {
        register();
        final ClientChannelInfo consumerManagerChannel = consumerManager.findChannel(GROUP, CLIENT_ID);
        assertThat(consumerManagerChannel).isNotNull();
    }

    @Test
    public void findSubscriptionDataTest() {
        register();
        final SubscriptionData subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC);
        assertThat(subscriptionData).isNotNull();
    }

    @Test
    public void findSubscriptionDataCountTest() {
        register();
        final int count = consumerManager.findSubscriptionDataCount(GROUP);
        assertTrue(count > 0);
    }

    @Test
    public void findSubscriptionTest() {
        SubscriptionData subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC, true);
        assertThat(subscriptionData).isNull();

        consumerManager.compensateSubscribeData(GROUP, TOPIC, new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL));
        subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC, true);
        assertThat(subscriptionData).isNotNull();
        assertThat(subscriptionData.getTopic()).isEqualTo(TOPIC);
        assertThat(subscriptionData.getSubString()).isEqualTo(SubscriptionData.SUB_ALL);

        subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC, false);
        assertThat(subscriptionData).isNull();
    }

    @Test
    public void scanNotActiveChannelTest() {
        clientChannelInfo.setLastUpdateTimestamp(System.currentTimeMillis() - brokerConfig.getChannelExpiredTimeout() * 2);
        consumerManager.scanNotActiveChannel();
        assertThat(consumerManager.getConsumerTable().size()).isEqualTo(0);
    }

    @Test
    public void queryTopicConsumeByWhoTest() {
        register();
        final HashSet<String> consumeGroup = consumerManager.queryTopicConsumeByWho(TOPIC);
        assertFalse(consumeGroup.isEmpty());
    }

    @Test
    public void doChannelCloseEventTest() {
        consumerManager.doChannelCloseEvent("127.0.0.1", channel);
        verify(consumerManager, never()).callConsumerIdsChangeListener(eq(ConsumerGroupEvent.CHANGE), any(), any());
        assertEquals(0, consumerManager.getConsumerTable().size());
    }

    private void register() {
        // register
        final Set<SubscriptionData> subList = new HashSet<>();
        SubscriptionData subscriptionData = new SubscriptionData(TOPIC, "*");
        subList.add(subscriptionData);
        consumerManager.registerConsumer(GROUP, clientChannelInfo, ConsumeType.CONSUME_PASSIVELY,
            MessageModel.BROADCASTING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, subList, true);
    }

    @Test
    public void removeExpireConsumerGroupInfo() {
        SubscriptionData subscriptionData = new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL);
        subscriptionData.setSubVersion(System.currentTimeMillis() - brokerConfig.getSubscriptionExpiredTimeout() * 2);
        consumerManager.compensateSubscribeData(GROUP, TOPIC, subscriptionData);
        consumerManager.compensateSubscribeData(GROUP, TOPIC + "_1", new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL));
        consumerManager.removeExpireConsumerGroupInfo();
        assertThat(consumerManager.getConsumerGroupInfo(GROUP, true)).isNotNull();
        assertThat(consumerManager.findSubscriptionData(GROUP, TOPIC)).isNull();
        assertThat(consumerManager.findSubscriptionData(GROUP, TOPIC + "_1")).isNotNull();
    }
}
