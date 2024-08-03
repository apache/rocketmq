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
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerTest {

    private ClientChannelInfo clientChannelInfo;

    @Mock
    private Channel channel;

    private ConsumerManager consumerManager;

    private DefaultConsumerIdsChangeListener defaultConsumerIdsChangeListener;

    @Mock
    private BrokerController brokerController;

    @Mock
    private ConsumerFilterManager consumerFilterManager;

    private BrokerConfig brokerConfig = new BrokerConfig();

    private Broker2Client broker2Client;

    private BrokerStatsManager brokerStatsManager;

    private static final String GROUP = "DEFAULT_GROUP";

    private static final String CLIENT_ID = "1";

    private static final int VERSION = 1;

    private static final String TOPIC = "DEFAULT_TOPIC";

    @Before
    public void before() {
        clientChannelInfo = new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, VERSION);
        defaultConsumerIdsChangeListener = new DefaultConsumerIdsChangeListener(brokerController);
        brokerStatsManager = new BrokerStatsManager(brokerConfig);
        consumerManager = new ConsumerManager(defaultConsumerIdsChangeListener, brokerStatsManager, brokerConfig);
        broker2Client = new Broker2Client(brokerController);
        when(brokerController.getConsumerFilterManager()).thenReturn(consumerFilterManager);
    }

    @Test
    public void compensateBasicConsumerInfoTest() {
        ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        Assertions.assertThat(consumerGroupInfo).isNull();

        consumerManager.compensateBasicConsumerInfo(GROUP, ConsumeType.CONSUME_ACTIVELY, MessageModel.BROADCASTING);
        consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        Assertions.assertThat(consumerGroupInfo).isNotNull();
        Assertions.assertThat(consumerGroupInfo.getConsumeType()).isEqualTo(ConsumeType.CONSUME_ACTIVELY);
        Assertions.assertThat(consumerGroupInfo.getMessageModel()).isEqualTo(MessageModel.BROADCASTING);
    }

    @Test
    public void compensateSubscribeDataTest() {
        ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        Assertions.assertThat(consumerGroupInfo).isNull();

        consumerManager.compensateSubscribeData(GROUP, TOPIC, new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL));
        consumerGroupInfo = consumerManager.getConsumerGroupInfo(GROUP, true);
        Assertions.assertThat(consumerGroupInfo).isNotNull();
        Assertions.assertThat(consumerGroupInfo.getSubscriptionTable().size()).isEqualTo(1);
        SubscriptionData subscriptionData = consumerGroupInfo.getSubscriptionTable().get(TOPIC);
        Assertions.assertThat(subscriptionData).isNotNull();
        Assertions.assertThat(subscriptionData.getTopic()).isEqualTo(TOPIC);
        Assertions.assertThat(subscriptionData.getSubString()).isEqualTo(SubscriptionData.SUB_ALL);
    }

    @Test
    public void registerConsumerTest() {
        register();
        final Set<SubscriptionData> subList = new HashSet<>();
        SubscriptionData subscriptionData = new SubscriptionData(TOPIC, "*");
        subList.add(subscriptionData);
        consumerManager.registerConsumer(GROUP, clientChannelInfo, ConsumeType.CONSUME_PASSIVELY,
            MessageModel.BROADCASTING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, subList, true);
        Assertions.assertThat(consumerManager.getConsumerTable().get(GROUP)).isNotNull();
    }

    @Test
    public void unregisterConsumerTest() {
        // register
        register();

        // unregister
        consumerManager.unregisterConsumer(GROUP, clientChannelInfo, true);
        Assertions.assertThat(consumerManager.getConsumerTable().get(GROUP)).isNull();
    }

    @Test
    public void findChannelTest() {
        register();
        final ClientChannelInfo consumerManagerChannel = consumerManager.findChannel(GROUP, CLIENT_ID);
        Assertions.assertThat(consumerManagerChannel).isNotNull();
    }

    @Test
    public void findSubscriptionDataTest() {
        register();
        final SubscriptionData subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC);
        Assertions.assertThat(subscriptionData).isNotNull();
    }

    @Test
    public void findSubscriptionDataCountTest() {
        register();
        final int count = consumerManager.findSubscriptionDataCount(GROUP);
        assert count > 0;
    }

    @Test
    public void findSubscriptionTest() {
        SubscriptionData subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC, true);
        Assertions.assertThat(subscriptionData).isNull();

        consumerManager.compensateSubscribeData(GROUP, TOPIC, new SubscriptionData(TOPIC, SubscriptionData.SUB_ALL));
        subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC, true);
        Assertions.assertThat(subscriptionData).isNotNull();
        Assertions.assertThat(subscriptionData.getTopic()).isEqualTo(TOPIC);
        Assertions.assertThat(subscriptionData.getSubString()).isEqualTo(SubscriptionData.SUB_ALL);

        subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC, false);
        Assertions.assertThat(subscriptionData).isNull();
    }

    @Test
    public void scanNotActiveChannelTest() {
        clientChannelInfo.setLastUpdateTimestamp(System.currentTimeMillis() - brokerConfig.getChannelExpiredTimeout() * 2);
        consumerManager.scanNotActiveChannel();
        Assertions.assertThat(consumerManager.getConsumerTable().size()).isEqualTo(0);
    }

    @Test
    public void queryTopicConsumeByWhoTest() {
        register();
        final HashSet<String> consumeGroup = consumerManager.queryTopicConsumeByWho(TOPIC);
        assert consumeGroup.size() > 0;
    }

    @Test
    public void doChannelCloseEventTest() {
        consumerManager.doChannelCloseEvent("127.0.0.1", channel);
        assert consumerManager.getConsumerTable().size() == 0;
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
        Assertions.assertThat(consumerManager.getConsumerGroupInfo(GROUP, true)).isNotNull();
        Assertions.assertThat(consumerManager.findSubscriptionData(GROUP, TOPIC)).isNull();
        Assertions.assertThat(consumerManager.findSubscriptionData(GROUP, TOPIC + "_1")).isNotNull();
    }
}
