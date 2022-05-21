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

package org.apache.rocketmq.thinclient.impl.consumer;

import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.retry.RetryPolicy;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcessQueueImplTest extends TestBase {
    @Mock
    private PushConsumerImpl pushConsumer;
    @Mock
    private ConsumeService consumeService;
    @Mock
    private PushConsumerSettings pushConsumerSettings;
    @Mock
    private RetryPolicy retryPolicy;

    private AtomicLong consumptionOkQuantity;
    private AtomicLong consumptionErrorQuantity;
    private AtomicLong receivedMessagesQuantity;

    private final FilterExpression filterExpression = FilterExpression.SUB_ALL;

    private ProcessQueueImpl processQueue;

    @Before
    public void setup() throws IllegalAccessException, NoSuchFieldException {
        this.processQueue = new ProcessQueueImpl(pushConsumer, fakeMessageQueueImpl0(), filterExpression);
        when(pushConsumer.isRunning()).thenReturn(true);

        this.consumptionOkQuantity = new AtomicLong(0);
        Field field0 = PushConsumerImpl.class.getDeclaredField("consumptionOkQuantity");
        field0.setAccessible(true);
        field0.set(pushConsumer, consumptionOkQuantity);

        this.consumptionErrorQuantity = new AtomicLong(0);
        Field field1 = PushConsumerImpl.class.getDeclaredField("consumptionErrorQuantity");
        field1.setAccessible(true);
        field1.set(pushConsumer, consumptionErrorQuantity);

        when(pushConsumer.getPushConsumerSettings()).thenReturn(pushConsumerSettings);
        when(pushConsumer.getScheduler()).thenReturn(SCHEDULER);

        this.receivedMessagesQuantity = new AtomicLong(0);
        when(pushConsumer.getReceivedMessagesQuantity()).thenReturn(receivedMessagesQuantity);
        when(pushConsumer.getConsumeService()).thenReturn(consumeService);
    }

    @Test
    public void testExpired() {
        when(pushConsumerSettings.getLongPollingTimeout()).thenReturn(Duration.ofSeconds(3));
        assertFalse(processQueue.expired());
    }

    @Test
    public void testIsCacheFull() {
        when(pushConsumer.cacheMessageCountThresholdPerQueue()).thenReturn(8);
        when(pushConsumer.cacheMessageBytesThresholdPerQueue()).thenReturn(1024);
        assertFalse(processQueue.isCacheFull());
    }

    @Test
    public void testCacheCorruptedMessages() {
        final MessageViewImpl corruptedMessageView0 = fakeMessageViewImpl(true);
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        messageViewList.add(corruptedMessageView0);
        when(retryPolicy.getNextAttemptDelay(anyInt())).thenReturn(Duration.ofSeconds(1));
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(pushConsumerSettings.isFifo()).thenReturn(false);
        when(pushConsumer.changInvisibleDuration(any(MessageViewImpl.class), any(Duration.class))).thenReturn(okChangeInvisibleDurationFuture());
        processQueue.cacheMessages(messageViewList);
        verify(pushConsumer, times(1)).changInvisibleDuration(any(MessageViewImpl.class), any(Duration.class));
    }

    @Test
    public void testCacheCorruptedMessagesFifo() {
        final MessageViewImpl corruptedMessageView0 = fakeMessageViewImpl(true);
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        messageViewList.add(corruptedMessageView0);
        when(pushConsumerSettings.isFifo()).thenReturn(true);
        when(pushConsumer.forwardMessageToDeadLetterQueue(any(MessageViewImpl.class))).thenReturn(okForwardMessageToDeadLetterQueueResponseFuture());
        processQueue.cacheMessages(messageViewList);
        verify(pushConsumer, times(1)).forwardMessageToDeadLetterQueue(any(MessageViewImpl.class));
        final Iterator<MessageViewImpl> iterator = processQueue.tryTakeFifoMessages();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testReceiveMessageImmediately() throws InterruptedException {
        final int cachedMessagesCountThresholdPerQueue = 8;
        when(pushConsumer.cacheMessageCountThresholdPerQueue()).thenReturn(cachedMessagesCountThresholdPerQueue);
        final int cachedMessageBytesThresholdPerQueue = 1024;
        when(pushConsumer.cacheMessageBytesThresholdPerQueue()).thenReturn(cachedMessageBytesThresholdPerQueue);
        Status status = Status.newBuilder().setCode(Code.OK).build();
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl();
        messageViewList.add(messageView);
        ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult(fakeEndpoints(), status, messageViewList);
        SettableFuture<ReceiveMessageResult> future0 = SettableFuture.create();
        future0.set(receiveMessageResult);
        when(pushConsumer.receiveMessage(any(ReceiveMessageRequest.class), any(MessageQueueImpl.class), any(Duration.class))).thenReturn(future0);
        when(pushConsumerSettings.getReceiveBatchSize()).thenReturn(32);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().build();
        when(pushConsumer.wrapReceiveMessageRequest(anyInt(), any(MessageQueueImpl.class), any(FilterExpression.class))).thenReturn(request);
        processQueue.fetchMessageImmediately();
        Thread.sleep(ProcessQueueImpl.RECEIVE_LATER_DELAY.toMillis() / 2);
        verify(pushConsumer, times(cachedMessagesCountThresholdPerQueue))
            .receiveMessage(any(ReceiveMessageRequest.class), any(MessageQueueImpl.class), any(Duration.class));
    }

    @Test
    public void testEraseMessageWithConsumeOk() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        int cachedMessageCount = 8;
        for (int i = 0; i < cachedMessageCount; i++) {
            final MessageViewImpl messageView = fakeMessageViewImpl();
            messageViewList.add(messageView);
        }
        processQueue.cacheMessages(messageViewList);
        assertEquals(cachedMessageCount, processQueue.cachedMessagesCount());
        final Optional<MessageViewImpl> optionalMessageView = processQueue.tryTakeMessage();
        assertTrue(optionalMessageView.isPresent());
        assertEquals(cachedMessageCount, processQueue.cachedMessagesCount());
        assertEquals(1, processQueue.inflightMessagesCount());

        final ListenableFuture<AckMessageResponse> future = okAckMessageResponseFuture();
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future);
        processQueue.eraseMessage(optionalMessageView.get(), ConsumeResult.OK);
        future.addListener(() -> verify(pushConsumer, times(1)).ackMessage(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
        assertEquals(processQueue.cachedMessagesCount(), cachedMessageCount - 1);
        assertEquals(processQueue.inflightMessagesCount(), 0);
        assertEquals(consumptionOkQuantity.get(), 1);
    }

    @Test
    public void testTryTakeFifoMessage() {
        List<MessageViewImpl> messageViewList0 = new ArrayList<>();
        final MessageViewImpl messageView0 = fakeMessageViewImpl(2, false);
        final MessageViewImpl messageView1 = fakeMessageViewImpl(2, false);
        messageViewList0.add(messageView0);
        messageViewList0.add(messageView1);

        List<MessageViewImpl> messageViewList1 = new ArrayList<>();
        final MessageViewImpl messageView2 = fakeMessageViewImpl(2, false);
        messageViewList1.add(messageView2);
        // First take.
        processQueue.cacheMessages(messageViewList0);
        Iterator<MessageViewImpl> iterator = processQueue.tryTakeFifoMessages();
        assertTrue(iterator.hasNext());
        assertEquals(messageView0, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(messageView1, iterator.next());
        assertFalse(iterator.hasNext());
        // Second take.
        processQueue.cacheMessages(messageViewList1);
        iterator = processQueue.tryTakeFifoMessages();
        assertTrue(iterator.hasNext());
        assertEquals(messageView2, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testEraseFifoMessageWithConsumeOk() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        ListenableFuture<AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(1);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        final ListenableFuture<Void> future = processQueue.eraseFifoMessage(messageView, ConsumeResult.OK);
        future.addListener(() -> verify(pushConsumer, times(1)).ackMessage(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
    }

    @Test
    public void testEraseFifoMessageWithConsumeError() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future0 = okForwardMessageToDeadLetterQueueResponseFuture();
        when(pushConsumer.forwardMessageToDeadLetterQueue(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(1);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        final ListenableFuture<Void> future = processQueue.eraseFifoMessage(messageView, ConsumeResult.ERROR);
        future.addListener(() -> verify(pushConsumer, times(1)).forwardMessageToDeadLetterQueue(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
    }

    @Test
    public void testEraseFifoMessageWithConsumeErrorInFirstAttempt() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        ListenableFuture<AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(2);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        SettableFuture<ConsumeResult> consumeFuture = SettableFuture.create();
        consumeFuture.set(ConsumeResult.OK);
        when(consumeService.consume(any(MessageViewImpl.class), any(Duration.class))).thenReturn(consumeFuture);
        when(pushConsumer.getConsumeService()).thenReturn(consumeService);
        final ListenableFuture<Void> future = processQueue.eraseFifoMessage(messageView, ConsumeResult.ERROR);
        future.addListener(() -> verify(pushConsumer, times(1)).ackMessage(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
    }
}