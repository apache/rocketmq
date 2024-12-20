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
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumeMessagePopConcurrentlyServiceTest {

    @Mock
    private DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    @Mock
    private MessageListenerConcurrently messageListener;

    @Mock
    private DefaultMQPushConsumer defaultMQPushConsumer;

    private ConsumeMessagePopConcurrentlyService popService;

    private final String defaultGroup = "defaultGroup";

    private final String defaultBroker = "defaultBroker";

    private final String defaultTopic = "defaultTopic";

    @Before
    public void init() throws Exception {
        when(defaultMQPushConsumer.getConsumerGroup()).thenReturn(defaultGroup);
        when(defaultMQPushConsumer.getConsumeThreadMin()).thenReturn(1);
        when(defaultMQPushConsumer.getConsumeThreadMax()).thenReturn(3);
        when(defaultMQPushConsumer.getConsumeMessageBatchMaxSize()).thenReturn(32);
        when(defaultMQPushConsumerImpl.getDefaultMQPushConsumer()).thenReturn(defaultMQPushConsumer);
        ConsumerStatsManager consumerStatsManager = mock(ConsumerStatsManager.class);
        when(defaultMQPushConsumerImpl.getConsumerStatsManager()).thenReturn(consumerStatsManager);
        popService = new ConsumeMessagePopConcurrentlyService(defaultMQPushConsumerImpl, messageListener);
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
        when(messageListener.consumeMessage(any(), any(ConsumeConcurrentlyContext.class))).thenReturn(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_SUCCESS, actual.getConsumeResult());
    }

    @Test
    public void testConsumeMessageDirectlyWithCrLater() {
        when(messageListener.consumeMessage(any(), any(ConsumeConcurrentlyContext.class))).thenReturn(ConsumeConcurrentlyStatus.RECONSUME_LATER);
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
        when(messageListener.consumeMessage(any(), any(ConsumeConcurrentlyContext.class))).thenThrow(new RuntimeException("exception"));
        ConsumeMessageDirectlyResult actual = popService.consumeMessageDirectly(createMessageExt(), defaultBroker);
        assertEquals(CMResult.CR_THROW_EXCEPTION, actual.getConsumeResult());
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
    public void testSubmitPopConsumeRequestWithMultiMsg() throws IllegalAccessException {
        List<MessageExt> msgs = Arrays.asList(createMessageExt(), createMessageExt());
        PopProcessQueue processQueue = mock(PopProcessQueue.class);
        MessageQueue messageQueue = mock(MessageQueue.class);
        ThreadPoolExecutor consumeExecutor = mock(ThreadPoolExecutor.class);
        FieldUtils.writeDeclaredField(popService, "consumeExecutor", consumeExecutor, true);
        when(defaultMQPushConsumer.getConsumeMessageBatchMaxSize()).thenReturn(1);
        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        verify(consumeExecutor, times(2)).submit(any(Runnable.class));
    }

    @Test
    public void testProcessConsumeResult() {
        ConsumeConcurrentlyContext context = mock(ConsumeConcurrentlyContext.class);
        ConsumeMessagePopConcurrentlyService.ConsumeRequest consumeRequest = mock(ConsumeMessagePopConcurrentlyService.ConsumeRequest.class);
        when(consumeRequest.getMsgs()).thenReturn(Arrays.asList(createMessageExt(), createMessageExt()));
        MessageQueue messageQueue = mock(MessageQueue.class);
        when(messageQueue.getTopic()).thenReturn(defaultTopic);
        when(consumeRequest.getMessageQueue()).thenReturn(messageQueue);
        PopProcessQueue processQueue = mock(PopProcessQueue.class);
        when(processQueue.ack()).thenReturn(0);
        when(consumeRequest.getPopProcessQueue()).thenReturn(processQueue);
        when(defaultMQPushConsumerImpl.getPopDelayLevel()).thenReturn(new int[]{1, 10});
        popService.processConsumeResult(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, context, consumeRequest);
        verify(defaultMQPushConsumerImpl, times(1)).ackAsync(any(MessageExt.class), any());
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
