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
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
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
        consumerManager = new ConsumerManager(defaultConsumerIdsChangeListener, brokerStatsManager);
        broker2Client = new Broker2Client(brokerController);
        when(brokerController.getConsumerFilterManager()).thenReturn(consumerFilterManager);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getBroker2Client()).thenReturn(broker2Client);
        register();
    }

    @Test
    public void registerConsumerTest() {
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

        final ClientChannelInfo consumerManagerChannel = consumerManager.findChannel(GROUP, CLIENT_ID);
        Assertions.assertThat(consumerManagerChannel).isNotNull();
    }

    @Test
    public void findSubscriptionDataTest() {
        final SubscriptionData subscriptionData = consumerManager.findSubscriptionData(GROUP, TOPIC);
        Assertions.assertThat(subscriptionData).isNotNull();
    }

    @Test
    public void findSubscriptionDataCountTest() {
        final int count = consumerManager.findSubscriptionDataCount(GROUP);
        assert count > 0;
    }

    @Test
    public void scanNotActiveChannelTest() {
        clientChannelInfo.setLastUpdateTimestamp(System.currentTimeMillis() - 1000 * 200);
        consumerManager.scanNotActiveChannel();
        assert consumerManager.getConsumerTable().size() == 0;
    }

    @Test
    public void queryTopicConsumeByWhoTest() {
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

}
