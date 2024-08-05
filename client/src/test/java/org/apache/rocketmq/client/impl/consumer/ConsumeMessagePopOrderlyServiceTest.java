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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumeMessagePopOrderlyServiceTest {

    @Mock
    private DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    @Mock
    private MessageListenerOrderly messageListener;

    @Mock
    private DefaultMQPushConsumer defaultMQPushConsumer;

    @Mock
    private ConsumerStatsManager consumerStatsManager;

    @Mock
    private RebalanceImpl rebalanceImpl;

    private ConsumeMessagePopOrderlyService popService;

    private final String defaultGroup = "defaultGroup";

    private final String defaultBroker = "defaultBroker";

    private final String defaultTopic = "defaultTopic";

    @Before
    public void init() throws Exception {
        when(defaultMQPushConsumer.getConsumerGroup()).thenReturn(defaultGroup);
        when(defaultMQPushConsumer.getConsumeThreadMin()).thenReturn(1);
        when(defaultMQPushConsumer.getConsumeThreadMax()).thenReturn(3);
        when(defaultMQPushConsumerImpl.getDefaultMQPushConsumer()).thenReturn(defaultMQPushConsumer);
        when(defaultMQPushConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        when(defaultMQPushConsumerImpl.getConsumerStatsManager()).thenReturn(consumerStatsManager);
        MQClientInstance mQClientFactory = mock(MQClientInstance.class);
        DefaultMQProducer defaultMQProducer = mock(DefaultMQProducer.class);
        when(mQClientFactory.getDefaultMQProducer()).thenReturn(defaultMQProducer);
        when(defaultMQPushConsumerImpl.getmQClientFactory()).thenReturn(mQClientFactory);
        popService = new ConsumeMessagePopOrderlyService(defaultMQPushConsumerImpl, messageListener);
    }

    @Test
    public void testShutdown() throws IllegalAccessException {
        popService.shutdown(3000L);
        Field scheduledExecutorServiceField = FieldUtils.getDeclaredField(popService.getClass(), "scheduledExecutorService", true);
        Field consumeExecutorField = FieldUtils.getDeclaredField(popService.getClass(), "consumeExecutor", true);
        ScheduledExecutorService scheduledExecutorService = (ScheduledExecutorService) scheduledExecutorServiceField.get(popService);
        ThreadPoolExecutor consumeExecutor = (ThreadPoolExecutor) consumeExecutorField.get(popService);
        assertTrue(scheduledExecutorService.isShutdown());
        assertTrue(scheduledExecutorService.isTerminated());
        assertTrue(consumeExecutor.isShutdown());
        assertTrue(consumeExecutor.isTerminated());
    }

    @Test
    public void testUnlockAllMessageQueues() {
        popService.unlockAllMessageQueues();
        verify(rebalanceImpl, times(1)).unlockAll(eq(false));
    }

    @Test
    public void testUpdateCorePoolSize() {
        popService.updateCorePoolSize(2);
        popService.incCorePoolSize();
        popService.decCorePoolSize();
        assertEquals(2, popService.getCorePoolSize());
    }

    @Test
    public void testConsumeMessageDirectly() {
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.SUCCESS);
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_SUCCESS, actual.getConsumeResult());
        assertTrue(actual.isOrder());
    }

    @Test
    public void testConsumeMessageDirectlyWithCommit() {
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.COMMIT);
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_COMMIT, actual.getConsumeResult());
        assertTrue(actual.isOrder());
    }

    @Test
    public void testConsumeMessageDirectlyWithRollback() {
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.ROLLBACK);
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_ROLLBACK, actual.getConsumeResult());
        assertTrue(actual.isOrder());
    }

    @Test
    public void testConsumeMessageDirectlyWithCrLater() {
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT);
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_LATER, actual.getConsumeResult());
    }

    @Test
    public void testConsumeMessageDirectlyWithCrReturnNull() {
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_RETURN_NULL, actual.getConsumeResult());
    }

    @Test
    public void testConsumeMessageDirectlyWithCrThrowException() {
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenThrow(new RuntimeException("exception"));
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_THROW_EXCEPTION, actual.getConsumeResult());
    }

    @Test
    public void testSubmitConsumeRequest() {
        assertThrows(UnsupportedOperationException.class, () -> {
            List<MessageExt> msgs = mock(List.class);
            ProcessQueue processQueue = mock(ProcessQueue.class);
            MessageQueue messageQueue = mock(MessageQueue.class);
            popService.submitConsumeRequest(msgs, processQueue, messageQueue, false);
        });
    }

    @Test
    public void testSubmitPopConsumeRequest() throws IllegalAccessException {
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = mock(PopProcessQueue.class);
        MessageQueue messageQueue = mock(MessageQueue.class);
        ThreadPoolExecutor consumeExecutor = mock(ThreadPoolExecutor.class);
        FieldUtils.writeDeclaredField(popService, "consumeExecutor", consumeExecutor, true);
        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        verify(consumeExecutor, times(1)).submit(any(Runnable.class));
    }

    @Test
    public void testLockMQPeriodically() {
        popService.lockMQPeriodically();
        verify(defaultMQPushConsumerImpl, times(1)).getRebalanceImpl();
        verify(rebalanceImpl, times(1)).lockAll();
    }

    @Test
    public void testGetConsumerStatsManager() {
        ConsumerStatsManager actual = popService.getConsumerStatsManager();
        assertNotNull(actual);
        assertEquals(consumerStatsManager, actual);
    }

    @Test
    public void testSendMessageBack() {
        assertTrue(popService.sendMessageBack(createMessageExt()));
    }

    @Test
    public void testProcessConsumeResult() {
        ConsumeOrderlyContext context = mock(ConsumeOrderlyContext.class);
        ConsumeMessagePopOrderlyService.ConsumeRequest consumeRequest = mock(ConsumeMessagePopOrderlyService.ConsumeRequest.class);
        assertTrue(popService.processConsumeResult(Collections.singletonList(createMessageExt()), ConsumeOrderlyStatus.SUCCESS, context, consumeRequest));
    }

    @Test
    public void testResetNamespace() {
        when(defaultMQPushConsumer.getNamespace()).thenReturn("defaultNamespace");
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        popService.resetNamespace(msgs);
        assertEquals(defaultTopic, msgs.get(0).getTopic());
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(defaultTopic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, defaultGroup);
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        long curTime = System.currentTimeMillis();
        result.setBornTimestamp(curTime - 1000);
        result.getProperties().put(MessageConst.PROPERTY_POP_CK, curTime + " " + curTime + " " + curTime + " " + curTime);
        result.setKeys("keys");
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setBornHost(bornHost);
        result.setStoreHost(storeHost);
        return result;
    }
}
