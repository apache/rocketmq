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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RebalancePushImplTest {
    @Spy
    private DefaultMQPushConsumerImpl defaultMQPushConsumer = new DefaultMQPushConsumerImpl(new DefaultMQPushConsumer("RebalancePushImplTest"), null);
    @Mock
    private MQClientInstance mqClientInstance;
    private OffsetStore offsetStore = mock(OffsetStore.class);
    private String consumerGroup = "CID_RebalancePushImplTest";
    private String topic = "TopicA";
    private MessageQueue mq = new MessageQueue("topic1", "broker1", 0);
    private MessageQueue retryMq = new MessageQueue(MixAll.RETRY_GROUP_TOPIC_PREFIX + "group", "broker1", 0);
    private DefaultMQPushConsumerImpl consumerImpl = mock(DefaultMQPushConsumerImpl.class);
    private RebalancePushImpl rebalanceImpl = new RebalancePushImpl(consumerImpl);
    private DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
    private MQClientInstance client = mock(MQClientInstance.class);
    private MQAdminImpl admin = mock(MQAdminImpl.class);

    public RebalancePushImplTest() {
        when(consumerImpl.getDefaultMQPushConsumer()).thenReturn(consumer);
        when(consumerImpl.getOffsetStore()).thenReturn(offsetStore);
        rebalanceImpl.setMQClientFactory(client);
        when(client.getMQAdminImpl()).thenReturn(admin);
    }

    @Test
    public void testMessageQueueChanged_CountThreshold() {
        RebalancePushImpl rebalancePush = new RebalancePushImpl(consumerGroup, MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely(), mqClientInstance, defaultMQPushConsumer);
        init(rebalancePush);

        // Just set pullThresholdForQueue
        defaultMQPushConsumer.getDefaultMQPushConsumer().setPullThresholdForQueue(1024);
        Set<MessageQueue> allocateResultSet = new HashSet<>();
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
        Set<MessageQueue> allocateResultSet = new HashSet<>();
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
        Set<MessageQueue> allocateResultSet = new HashSet<>();
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 0));
        allocateResultSet.add(new MessageQueue(topic, "BrokerA", 1));
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
    public void testComputePullFromWhereWithException_ne_minus1() throws MQClientException {
        for (ConsumeFromWhere where : new ConsumeFromWhere[]{
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP}) {
            consumer.setConsumeFromWhere(where);

            when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(0L);
            assertEquals(0, rebalanceImpl.computePullFromWhereWithException(mq));
        }
    }

    @Test
    public void testComputePullFromWhereWithException_eq_minus1_last() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(12345L);

        assertEquals(12345L, rebalanceImpl.computePullFromWhereWithException(mq));

        assertEquals(0L, rebalanceImpl.computePullFromWhereWithException(retryMq));
    }

    @Test
    public void testComputePullFromWhereWithException_eq_minus1_first() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        assertEquals(0, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    @Test
    public void testComputePullFromWhereWithException_eq_minus1_timestamp() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        when(admin.searchOffset(any(MessageQueue.class), anyLong())).thenReturn(12345L);
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(23456L);

        assertEquals(12345L, rebalanceImpl.computePullFromWhereWithException(mq));

        assertEquals(23456L, rebalanceImpl.computePullFromWhereWithException(retryMq));
    }

}
