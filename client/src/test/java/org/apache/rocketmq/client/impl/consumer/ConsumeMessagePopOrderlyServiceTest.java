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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.AckCallback;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
        when(defaultMQPushConsumer.getMaxReconsumeTimes()).thenReturn(16);
        when(defaultMQPushConsumer.getConsumeTimeout()).thenReturn(15L);
        when(defaultMQPushConsumerImpl.getDefaultMQPushConsumer()).thenReturn(defaultMQPushConsumer);
        when(defaultMQPushConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        when(defaultMQPushConsumerImpl.getConsumerStatsManager()).thenReturn(consumerStatsManager);
        when(defaultMQPushConsumerImpl.getPopDelayLevel()).thenReturn(new int[]{1, 10});
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
        Field consumeExecutorField = FieldUtils.getField(popService.getClass(), "consumeExecutor", true);
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
        FieldUtils.writeField(popService, "consumeExecutor", consumeExecutor, true);
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
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        ConsumeMessagePopOrderlyService.ConsumeRequest consumeRequest =
            mock(ConsumeMessagePopOrderlyService.ConsumeRequest.class);
        when(consumeRequest.getProcessQueue()).thenReturn(processQueue);
        when(consumeRequest.getMessageQueue()).thenReturn(messageQueue);
        assertTrue(popService.processConsumeResult(msgs, ConsumeOrderlyStatus.SUCCESS,
            new ConsumeOrderlyContext(messageQueue), consumeRequest));
        verify(defaultMQPushConsumerImpl, times(1)).ackAsync(eq(msgs.get(0)), eq(defaultGroup));
        assertEquals(0, processQueue.getWaiAckMsgCount());
    }

    @Test
    public void testConsumeRequestInvokesListenerAndAcksSuccess() throws Exception {
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.SUCCESS);

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);

        Runnable request = captureSubmittedRequest(consumeExecutor, 1);
        assertEquals(1, getConsumeRequestSetSize());
        request.run();

        verify(messageListener, times(1)).consumeMessage(eq(msgs), any(ConsumeOrderlyContext.class));
        verify(defaultMQPushConsumerImpl, times(1)).ackAsync(eq(msgs.get(0)), eq(defaultGroup));
        assertEquals(0, processQueue.getWaiAckMsgCount());
        assertEquals(0, getConsumeRequestSetSize());
    }

    @Test
    public void testCommitAcksMessages() throws Exception {
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.COMMIT);

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(defaultMQPushConsumerImpl, times(1)).ackAsync(eq(msgs.get(0)), eq(defaultGroup));
        assertEquals(0, processQueue.getWaiAckMsgCount());
    }

    @Test
    public void testSuspendChangesInvisibleTimeAndDoesNotAck() throws Exception {
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class)))
            .thenReturn(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT);

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(defaultMQPushConsumerImpl, times(1)).changePopInvisibleTimeAsync(eq(defaultTopic), eq(defaultGroup),
            anyString(), anyLong(), any(AckCallback.class));
        verify(defaultMQPushConsumerImpl, never()).ackAsync(any(MessageExt.class), anyString());
        assertEquals(1, msgs.get(0).getReconsumeTimes());
        assertEquals(0, processQueue.getWaiAckMsgCount());
        assertEquals(0, getConsumeRequestSetSize());
    }

    @Test
    public void testRollbackChangesInvisibleTimeAndDoesNotAck() throws Exception {
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class)))
            .thenReturn(ConsumeOrderlyStatus.ROLLBACK);

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(defaultMQPushConsumerImpl, times(1)).changePopInvisibleTimeAsync(eq(defaultTopic), eq(defaultGroup),
            anyString(), anyLong(), any(AckCallback.class));
        verify(defaultMQPushConsumerImpl, never()).ackAsync(any(MessageExt.class), anyString());
        assertEquals(0, processQueue.getWaiAckMsgCount());
    }

    @Test
    public void testPopTimeoutSkipsListenerAndReleasesWaitCount() throws Exception {
        long now = System.currentTimeMillis();
        List<MessageExt> msgs = Collections.singletonList(createMessageExt(now - 120000L, 60000L));
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(messageListener, never()).consumeMessage(any(), any(ConsumeOrderlyContext.class));
        verify(defaultMQPushConsumerImpl, never()).ackAsync(any(MessageExt.class), anyString());
        assertEquals(0, processQueue.getWaiAckMsgCount());
        assertEquals(0, getConsumeRequestSetSize());
    }

    @Test
    public void testDroppedProcessQueueSkipsListenerAndReleasesWaitCount() throws Exception {
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        processQueue.setDropped(true);
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(messageListener, never()).consumeMessage(any(), any(ConsumeOrderlyContext.class));
        verify(defaultMQPushConsumerImpl, never()).ackAsync(any(MessageExt.class), anyString());
        assertEquals(0, processQueue.getWaiAckMsgCount());
        assertEquals(0, getConsumeRequestSetSize());
    }

    @Test
    public void testRequestRemovedAfterRunAllowsNextSubmission() throws Exception {
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.SUCCESS);

        PopProcessQueue processQueue = new PopProcessQueue();
        List<MessageExt> firstMsgs = Collections.singletonList(createMessageExt());
        processQueue.incFoundMsg(firstMsgs.size());
        popService.submitPopConsumeRequest(firstMsgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();
        assertEquals(0, getConsumeRequestSetSize());

        List<MessageExt> secondMsgs = Collections.singletonList(createMessageExt());
        processQueue.incFoundMsg(secondMsgs.size());
        popService.submitPopConsumeRequest(secondMsgs, processQueue, messageQueue);

        verify(consumeExecutor, times(2)).submit(any(Runnable.class));
        captureSubmittedRequest(consumeExecutor, 2);
    }

    @Test
    public void testLateRemovalKeepsSuccessorRequest() throws Exception {
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.SUCCESS);

        PopProcessQueue processQueue = new PopProcessQueue();
        List<MessageExt> firstMsgs = Collections.singletonList(createMessageExt());
        processQueue.incFoundMsg(firstMsgs.size());
        popService.submitPopConsumeRequest(firstMsgs, processQueue, messageQueue);
        Runnable firstRequest = captureSubmittedRequest(consumeExecutor, 1);
        firstRequest.run();
        assertEquals(0, getConsumeRequestSetSize());

        List<MessageExt> secondMsgs = Collections.singletonList(createMessageExt());
        processQueue.incFoundMsg(secondMsgs.size());
        popService.submitPopConsumeRequest(secondMsgs, processQueue, messageQueue);
        assertEquals(1, getConsumeRequestSetSize());

        // The first request may reach its finally block after a successor for the same queue has
        // been registered. That late cleanup must not unregister the successor by equality.
        Method abort = firstRequest.getClass().getDeclaredMethod("abortRemainingBatches");
        abort.setAccessible(true);
        abort.invoke(firstRequest);
        assertEquals(1, getConsumeRequestSetSize());

        captureSubmittedRequest(consumeExecutor, 2).run();
        verify(messageListener, times(2)).consumeMessage(any(), any(ConsumeOrderlyContext.class));
        assertEquals(0, processQueue.getWaiAckMsgCount());
        assertEquals(0, getConsumeRequestSetSize());
    }

    @Test
    public void testSubmitWhileConsumeRequestIsRunningMergesMessages() throws Exception {
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class))).thenReturn(ConsumeOrderlyStatus.SUCCESS);

        PopProcessQueue processQueue = new PopProcessQueue();
        List<MessageExt> firstMsgs = Collections.singletonList(createMessageExt());
        List<MessageExt> secondMsgs = Collections.singletonList(createMessageExt());
        processQueue.incFoundMsg(firstMsgs.size() + secondMsgs.size());

        popService.submitPopConsumeRequest(firstMsgs, processQueue, messageQueue);
        popService.submitPopConsumeRequest(secondMsgs, processQueue, messageQueue);

        verify(consumeExecutor, times(1)).submit(any(Runnable.class));
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(messageListener, times(2)).consumeMessage(any(), any(ConsumeOrderlyContext.class));
        verify(defaultMQPushConsumerImpl, times(2)).ackAsync(any(MessageExt.class), eq(defaultGroup));
        assertEquals(0, processQueue.getWaiAckMsgCount());
        assertEquals(0, getConsumeRequestSetSize());
    }

    @Test
    public void testMaxReconsumeTimesSendsBackAndAcksOriginal() throws Exception {
        when(defaultMQPushConsumer.getMaxReconsumeTimes()).thenReturn(0);
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        PopProcessQueue processQueue = new PopProcessQueue();
        processQueue.incFoundMsg(msgs.size());
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        ThreadPoolExecutor consumeExecutor = installCaptureExecutor();
        when(messageListener.consumeMessage(any(), any(ConsumeOrderlyContext.class)))
            .thenReturn(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT);

        popService.submitPopConsumeRequest(msgs, processQueue, messageQueue);
        captureSubmittedRequest(consumeExecutor, 1).run();

        verify(defaultMQPushConsumerImpl, times(1)).ackAsync(eq(msgs.get(0)), eq(defaultGroup));
        verify(defaultMQPushConsumerImpl, never()).changePopInvisibleTimeAsync(anyString(), anyString(),
            anyString(), anyLong(), any(AckCallback.class));
        assertEquals(0, processQueue.getWaiAckMsgCount());
    }

    @Test
    public void testResetNamespace() {
        when(defaultMQPushConsumer.getNamespace()).thenReturn("defaultNamespace");
        List<MessageExt> msgs = Collections.singletonList(createMessageExt());
        popService.resetNamespace(msgs);
        assertEquals(defaultTopic, msgs.get(0).getTopic());
    }

    private ThreadPoolExecutor installCaptureExecutor() throws IllegalAccessException {
        ThreadPoolExecutor consumeExecutor = mock(ThreadPoolExecutor.class);
        FieldUtils.writeField(popService, "consumeExecutor", consumeExecutor, true);
        return consumeExecutor;
    }

    private Runnable captureSubmittedRequest(ThreadPoolExecutor consumeExecutor, int expectedSubmissions) {
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(consumeExecutor, times(expectedSubmissions)).submit(captor.capture());
        return captor.getValue();
    }

    private int getConsumeRequestSetSize() throws IllegalAccessException {
        Field field = FieldUtils.getDeclaredField(ConsumeMessagePopOrderlyService.class, "consumeRequestSet", true);
        return ((Set<?>) field.get(popService)).size();
    }

    private MessageExt createMessageExt(long popTime, long invisibleTime) {
        MessageExt result = createMessageExt();
        result.getProperties().put(MessageConst.PROPERTY_POP_CK,
            "0 " + popTime + " " + invisibleTime + " 0");
        return result;
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
