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
package org.apache.rocketmq.client.impl.consumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RebalancePushImplTest {
    @Spy
    private DefaultMQPushConsumerImpl defaultMQPushConsumer = new DefaultMQPushConsumerImpl(new DefaultMQPushConsumer("RebalancePushImplTest"), null);
    @Mock
    private MQClientInstance mqClientInstance;
    @Mock
    private OffsetStore offsetStore;
    private String consumerGroup = "CID_RebalancePushImplTest";
    private String topic = "TopicA";
    private final String brokerName = "BrokerA";

    @Before
    public void before() {
        defaultMQPushConsumer.getDefaultMQPushConsumer().setClientRebalance(false);
        when(mqClientInstance.getTopicRouteTable()).thenReturn(new ConcurrentHashMap<String, TopicRouteData>());
    }

    @Test
    public void testMessageQueueChanged_CountThreshold() {
        RebalancePushImpl rebalancePush = new RebalancePushImpl(consumerGroup, MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely(), mqClientInstance, defaultMQPushConsumer);
        init(rebalancePush);

        // Just set pullThresholdForQueue
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdForQueue(1024);
        Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 0));
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 1));
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.getDefaultMQPushConsumer().getPullThresholdForQueue()).isEqualTo(1024);

        // Set pullThresholdForTopic
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdForTopic(1024);
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.getDefaultMQPushConsumer().getPullThresholdForQueue()).isEqualTo(512);

        // Change message queue allocate result
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 2));
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.getDefaultMQPushConsumer().getPullThresholdForQueue()).isEqualTo(341);
    }

    private void doRebalanceForcibly(RebalancePushImpl rebalancePush, Set<MessageQueue> allocateResultSet) {
        rebalancePush.topicSubscribeInfoTable.put(topic, allocateResultSet);
        rebalancePush.doRebalance(false);
        rebalancePush.messageQueueChanged(topic, allocateResultSet, allocateResultSet);
    }

    private void init(final RebalancePushImpl rebalancePush) {
        rebalancePush.getSubscriptionInner().putIfAbsent(topic, new SubscriptionData());

        rebalancePush.subscriptionInner.putIfAbsent(topic, new SubscriptionData());

        try {
            when(mqClientInstance.queryAssignment(anyString(), anyString(), anyString(), any(MessageModel.class), anyInt())).thenThrow(new RemotingTimeoutException("unsupported"));
        } catch (RemotingException ignored) {
        } catch (InterruptedException ignored) {
        } catch (MQBrokerException ignored) {
        }
        when(mqClientInstance.findConsumerIdList(anyString(), anyString())).thenReturn(Collections.singletonList(consumerGroup));
        when(mqClientInstance.getClientId()).thenReturn(consumerGroup);
        when(defaultMQPushConsumer.getOffsetStore()).thenReturn(offsetStore);
    }

    @Test
    public void testMessageQueueChanged_SizeThreshold() {
        RebalancePushImpl rebalancePush = new RebalancePushImpl(consumerGroup, MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely(), mqClientInstance, defaultMQPushConsumer);
        init(rebalancePush);

        // Just set pullThresholdSizeForQueue
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(1024);
        Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 0));
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 1));
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.getDefaultMQPushConsumer().getPullThresholdSizeForQueue()).isEqualTo(1024);

        // Set pullThresholdSizeForTopic
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdSizeForTopic(1024);
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.getDefaultMQPushConsumer().getPullThresholdSizeForQueue()).isEqualTo(512);

        // Change message queue allocate result
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 2));
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.getDefaultMQPushConsumer().getPullThresholdSizeForQueue()).isEqualTo(341);
    }

    @Test
    public void testMessageQueueChanged_ConsumerRuntimeInfo() throws MQClientException {
        RebalancePushImpl rebalancePush = new RebalancePushImpl(consumerGroup, MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely(), mqClientInstance, defaultMQPushConsumer);
        init(rebalancePush);

        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(1024);
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdForQueue(1024);
        Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
        allocateResultSet.add(new MessageQueue(topic, brokerName, 0));
        allocateResultSet.add(new MessageQueue(topic, brokerName, 1));
        doRebalanceForcibly(rebalancePush, allocateResultSet);

        defaultMQPushConsumer.setConsumeMessageService(new ConsumeMessageConcurrentlyService(defaultMQPushConsumer, null));
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdSizeForQueue")).isEqualTo("1024");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdForQueue")).isEqualTo("1024");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdSizeForTopic")).isEqualTo("-1");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdForTopic")).isEqualTo("-1");

        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdSizeForTopic(1024);
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdForTopic(1024);
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdSizeForQueue")).isEqualTo("512");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdForQueue")).isEqualTo("512");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdSizeForTopic")).isEqualTo("1024");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdForTopic")).isEqualTo("1024");

        // Change message queue allocate result
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 2));
        doRebalanceForcibly(rebalancePush, allocateResultSet);
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdSizeForQueue")).isEqualTo("341");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdForQueue")).isEqualTo("341");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdSizeForTopic")).isEqualTo("1024");
        assertThat(defaultMQPushConsumer.consumerRunningInfo().getProperties().get("pullThresholdForTopic")).isEqualTo("1024");
    }

    @Test
    public void testDoRebalancePull() throws Exception {
        RebalancePushImpl rebalancePush = new RebalancePushImpl(consumerGroup, MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely(), mqClientInstance, defaultMQPushConsumer);
        rebalancePush.getSubscriptionInner().putIfAbsent(topic, new SubscriptionData());
        rebalancePush.subscriptionInner.putIfAbsent(topic, new SubscriptionData());

        when(mqClientInstance.getClientId()).thenReturn(consumerGroup);
        when(defaultMQPushConsumer.getOffsetStore()).thenReturn(offsetStore);
        doNothing().when(defaultMQPushConsumer).executePullRequestLater(any(PullRequest.class), anyLong());
        MessageQueueAssignment queueAssignment = new MessageQueueAssignment();
        queueAssignment.setMode(MessageRequestMode.PULL);
        queueAssignment.setMessageQueue(new MessageQueue(topic, brokerName, 0));
        when(mqClientInstance.queryAssignment(anyString(), anyString(), anyString(), any(MessageModel.class), anyInt())).thenReturn(Collections.singleton(queueAssignment));

        assertThat(rebalancePush.doRebalance(false)).isTrue();
    }
}