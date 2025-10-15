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
package org.apache.rocketmq.broker.slave;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.processor.QueryAssignmentProcessor;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SlaveSynchronizeTest {
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    private SlaveSynchronize slaveSynchronize;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    @Mock
    private TopicConfigManager topicConfigManager;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    @Mock
    private MessageStore messageStore;

    @Mock
    private ScheduleMessageService scheduleMessageService;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private QueryAssignmentProcessor queryAssignmentProcessor;

    @Mock
    private MessageRequestModeManager messageRequestModeManager;

    @Mock
    private TimerMessageStore timerMessageStore;

    @Mock
    private TimerMetrics timerMetrics;

    @Mock
    private TimerCheckpoint timerCheckpoint;

    private static final String BROKER_ADDR = "127.0.0.1:10911";

    @Before
    public void init() {
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getScheduleMessageService()).thenReturn(scheduleMessageService);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getQueryAssignmentProcessor()).thenReturn(queryAssignmentProcessor);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(brokerController.getTimerCheckpoint()).thenReturn(timerCheckpoint);
        when(topicConfigManager.getDataVersion()).thenReturn(new DataVersion());
        when(topicConfigManager.getTopicConfigTable()).thenReturn(new ConcurrentHashMap<>());
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());
        when(consumerOffsetManager.getDataVersion()).thenReturn(new DataVersion());
        when(subscriptionGroupManager.getDataVersion()).thenReturn(new DataVersion());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(new ConcurrentHashMap<>());
        when(queryAssignmentProcessor.getMessageRequestModeManager()).thenReturn(messageRequestModeManager);
        when(messageRequestModeManager.getMessageRequestModeMap()).thenReturn(new ConcurrentHashMap<>());
        when(messageStoreConfig.isTimerWheelEnable()).thenReturn(true);
        when(messageStore.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(timerMessageStore.isShouldRunningDequeue()).thenReturn(false);
        when(timerMessageStore.getTimerMetrics()).thenReturn(timerMetrics);
        when(timerMetrics.getDataVersion()).thenReturn(new DataVersion());
        when(timerCheckpoint.getDataVersion()).thenReturn(new DataVersion());
        slaveSynchronize = new SlaveSynchronize(brokerController);
        slaveSynchronize.setMasterAddr(BROKER_ADDR);
    }

    @Test
    public void testSyncAll() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        MQBrokerException, InterruptedException, UnsupportedEncodingException, RemotingCommandException {
        TopicConfig newTopicConfig = new TopicConfig("NewTopic");
        when(brokerOuterAPI.getAllTopicConfig(anyString())).thenReturn(createTopicConfigWrapper(newTopicConfig));
        when(brokerOuterAPI.getAllConsumerOffset(anyString())).thenReturn(createConsumerOffsetWrapper());
        when(brokerOuterAPI.getAllDelayOffset(anyString())).thenReturn("");
        when(brokerOuterAPI.getAllSubscriptionGroupConfig(anyString())).thenReturn(createSubscriptionGroupWrapper());
        when(brokerOuterAPI.getAllMessageRequestMode(anyString())).thenReturn(createMessageRequestModeWrapper());
        when(brokerOuterAPI.getTimerMetrics(anyString())).thenReturn(createTimerMetricsWrapper());
        slaveSynchronize.syncAll();
        Assert.assertEquals(1, this.brokerController.getTopicConfigManager().getDataVersion().getStateVersion());
        Assert.assertEquals(1, this.brokerController.getTopicQueueMappingManager().getDataVersion().getStateVersion());
        Assert.assertEquals(1, consumerOffsetManager.getDataVersion().getStateVersion());
        Assert.assertEquals(1, subscriptionGroupManager.getDataVersion().getStateVersion());
        Assert.assertEquals(1, timerMetrics.getDataVersion().getStateVersion());
    }

    @Test
    public void testGetMasterAddr() {
        Assert.assertEquals(BROKER_ADDR, slaveSynchronize.getMasterAddr());
    }

    @Test
    public void testSyncTimerCheckPoint() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        when(brokerOuterAPI.getTimerCheckPoint(anyString())).thenReturn(timerCheckpoint);
        slaveSynchronize.syncTimerCheckPoint();
        Assert.assertEquals(0, timerCheckpoint.getDataVersion().getStateVersion());
    }

    private TopicConfigAndMappingSerializeWrapper createTopicConfigWrapper(TopicConfig topicConfig) {
        TopicConfigAndMappingSerializeWrapper wrapper = new TopicConfigAndMappingSerializeWrapper();
        wrapper.setTopicConfigTable(new ConcurrentHashMap<>());
        wrapper.getTopicConfigTable().put(topicConfig.getTopicName(), topicConfig);
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        wrapper.setMappingDataVersion(dataVersion);
        return wrapper;
    }

    private ConsumerOffsetSerializeWrapper createConsumerOffsetWrapper() {
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        wrapper.setOffsetTable(new ConcurrentHashMap<>());
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }

    private SubscriptionGroupWrapper createSubscriptionGroupWrapper() {
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        wrapper.setSubscriptionGroupTable(new ConcurrentHashMap<>());
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }

    private MessageRequestModeSerializeWrapper createMessageRequestModeWrapper() {
        MessageRequestModeSerializeWrapper wrapper = new MessageRequestModeSerializeWrapper();
        wrapper.setMessageRequestModeMap(new ConcurrentHashMap<>());
        return wrapper;
    }

    private TimerMetrics.TimerMetricsSerializeWrapper createTimerMetricsWrapper() {
        TimerMetrics.TimerMetricsSerializeWrapper wrapper = new TimerMetrics.TimerMetricsSerializeWrapper();
        wrapper.setTimingCount(new ConcurrentHashMap<>());
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }
}
