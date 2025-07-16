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
        rebalanceImpl.setmQClientFactory(client);
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

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: empty message queue (maxOffset=0)
     */
    @Test
    public void testComputePullFromWhereWithException_emptyQueue() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        
        // Mock empty message queue: maxOffset returns 0
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(0L);

        // Verify empty queue should start from offset 0
        assertEquals("Empty message queue should start consuming from offset 0", 0L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: single message queue (maxOffset=1)
     * This is the key test case for fixing the first message ignored issue
     */
    @Test
    public void testComputePullFromWhereWithException_singleMessageQueue() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        
        // Mock single message queue: maxOffset returns 1
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(1L);

        // Verify that when maxOffset=1, should start from 0 to avoid skipping the first message
        assertEquals("Single message queue should start from offset 0 to avoid skipping first message", 0L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: boundary case (maxOffset=2)
     */
    @Test
    public void testComputePullFromWhereWithException_boundaryCase() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        
        // Mock boundary case: maxOffset returns 2
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(2L);

        // Verify when maxOffset=2, should start from maxOffset
        assertEquals("When maxOffset=2, should start consuming from maxOffset", 2L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: multiple messages queue (maxOffset>1)
     */
    @Test
    public void testComputePullFromWhereWithException_multipleMessagesQueue() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        
        // Mock multiple messages queue: maxOffset returns 10
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(10L);

        // Verify multiple messages should start from maxOffset
        assertEquals("Multiple messages queue should start consuming from maxOffset", 10L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Integration test: verify behavior difference before and after fix
     */
    @Test
    public void testComputePullFromWhereWithException_firstMessageNotIgnored() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        
        // Test various maxOffset values to verify fix logic
        long[] testMaxOffsets = {0L, 1L, 2L, 5L, 100L};
        
        for (long maxOffset : testMaxOffsets) {
            when(admin.maxOffset(any(MessageQueue.class))).thenReturn(maxOffset);
            
            long result = rebalanceImpl.computePullFromWhereWithException(mq);
            
            if (maxOffset <= 1) {
                assertEquals("When maxOffset=" + maxOffset + ", should start from 0 to avoid skipping messages", 
                           0L, result);
            } else {
                assertEquals("When maxOffset=" + maxOffset + ", should start from maxOffset", 
                           maxOffset, result);
            }
        }
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: verify handling for CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST
     */
    @Test
    public void testComputePullFromWhereWithException_lastOffsetAndMinWhenBootFirst() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST);
        
        // Test maxOffset=0 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(0L);
        assertEquals("CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST mode, empty queue should start from 0", 
                   0L, rebalanceImpl.computePullFromWhereWithException(mq));
        
        // Test maxOffset=1 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(1L);
        assertEquals("CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST mode, single message queue should start from 0", 
                   0L, rebalanceImpl.computePullFromWhereWithException(mq));
        
        // Test maxOffset>1 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(5L);
        assertEquals("CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST mode, multiple messages queue should start from maxOffset", 
                   5L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: verify handling for CONSUME_FROM_MIN_OFFSET
     */
    @Test
    public void testComputePullFromWhereWithException_minOffset() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET);
        
        // Test maxOffset=0 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(0L);
        assertEquals("CONSUME_FROM_MIN_OFFSET mode, empty queue should start from 0", 
                   0L, rebalanceImpl.computePullFromWhereWithException(mq));
        
        // Test maxOffset=1 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(1L);
        assertEquals("CONSUME_FROM_MIN_OFFSET mode, single message queue should start from 0", 
                   0L, rebalanceImpl.computePullFromWhereWithException(mq));
        
        // Test maxOffset>1 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(5L);
        assertEquals("CONSUME_FROM_MIN_OFFSET mode, multiple messages queue should start from maxOffset", 
                   5L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

    /**
     * Regression test for maxOffset edge cases: verify fix for first message ignored issue
     * Test scenario: verify handling for CONSUME_FROM_MAX_OFFSET
     */
    @Test
    public void testComputePullFromWhereWithException_maxOffset() throws MQClientException {
        when(offsetStore.readOffset(any(MessageQueue.class), any(ReadOffsetType.class))).thenReturn(-1L);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET);
        
        // Test maxOffset=0 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(0L);
        assertEquals("CONSUME_FROM_MAX_OFFSET mode, empty queue should start from 0", 
                   0L, rebalanceImpl.computePullFromWhereWithException(mq));
        
        // Test maxOffset=1 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(1L);
        assertEquals("CONSUME_FROM_MAX_OFFSET mode, single message queue should start from 0", 
                   0L, rebalanceImpl.computePullFromWhereWithException(mq));
        
        // Test maxOffset>1 case
        when(admin.maxOffset(any(MessageQueue.class))).thenReturn(5L);
        assertEquals("CONSUME_FROM_MAX_OFFSET mode, multiple messages queue should start from maxOffset", 
                   5L, rebalanceImpl.computePullFromWhereWithException(mq));
    }

}
