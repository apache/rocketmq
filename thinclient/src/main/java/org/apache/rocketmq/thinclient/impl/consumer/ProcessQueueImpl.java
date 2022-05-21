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
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Iterator;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.thinclient.hook.MessageHookPoints;
import org.apache.rocketmq.thinclient.hook.MessageHookPointsStatus;
import org.apache.rocketmq.thinclient.message.MessageCommon;
import org.apache.rocketmq.thinclient.retry.RetryPolicy;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;

/**
 * Default implementation of {@link ProcessQueue}.
 *
 * <p>Apart from the basic part mentioned in {@link ProcessQueue}, this implementation
 *
 * @see ProcessQueue
 */
@SuppressWarnings({"NullableProblems", "UnstableApiUsage"})
class ProcessQueueImpl implements ProcessQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessQueueImpl.class);

    public static final Duration FORWARD_FIFO_MESSAGE_TO_DLQ_DELAY = Duration.ofMillis(100);
    public static final Duration ACK_FIFO_MESSAGE_DELAY = Duration.ofMillis(100);
    public static final Duration RECEIVE_LATER_DELAY = Duration.ofSeconds(3);

    private final PushConsumerImpl consumer;

    /**
     * Dropped means {@link ProcessQueue} is deprecated, which means no message would be fetched from remote anymore.
     */
    private volatile boolean dropped;
    private final MessageQueueImpl mq;
    private final FilterExpression filterExpression;

    /**
     * Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
     */
    @GuardedBy("pendingMessagesLock")
    private final List<MessageViewImpl> pendingMessages;
    private final ReadWriteLock pendingMessagesLock;

    /**
     * Message which is in-flight means have been dispatched, but the consumption process is not accomplished.
     */
    @GuardedBy("inflightMessagesLock")
    private final List<MessageViewImpl> inflightMessages;
    private final ReadWriteLock inflightMessagesLock;

    private final AtomicLong cachedMessagesBytes;

    private volatile long activityNanoTime = System.nanoTime();

    public ProcessQueueImpl(PushConsumerImpl consumer, MessageQueueImpl mq, FilterExpression filterExpression) {
        this.consumer = consumer;
        this.dropped = false;
        this.mq = mq;
        this.filterExpression = filterExpression;
        this.pendingMessages = new ArrayList<>();
        this.pendingMessagesLock = new ReentrantReadWriteLock();
        this.inflightMessages = new ArrayList<>();
        this.inflightMessagesLock = new ReentrantReadWriteLock();
        this.cachedMessagesBytes = new AtomicLong();
    }

    @Override
    public MessageQueueImpl getMessageQueue() {
        return mq;
    }

    @Override
    public void drop() {
        this.dropped = true;
    }

    @Override
    public boolean expired() {
        Duration maxIdleDuration = Duration.ofNanos(2 * consumer.getPushConsumerSettings().getLongPollingTimeout().toNanos());
        final Duration idleDuration = Duration.ofNanos(System.nanoTime() - activityNanoTime);
        if (idleDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        LOGGER.warn("Process queue is idle, idle duration={}, max idle duration={}, mq={}, clientId={}", idleDuration, maxIdleDuration, mq, consumer.getClientId());
        return true;
    }

    void cacheMessages(List<MessageViewImpl> messageList) {
        List<MessageViewImpl> corrupted = new ArrayList<>();
        pendingMessagesLock.writeLock().lock();
        try {
            MessageViewImpl previous = null;
            for (MessageViewImpl messageView : messageList) {
                // Collect corrupted messages and dispose them individually.
                if (messageView.isCorrupted()) {
                    corrupted.add(messageView);
                    continue;
                }
                if (null != previous) {
                    previous.setNext(messageView);
                }
                previous = messageView;
                pendingMessages.add(messageView);
                cachedMessagesBytes.addAndGet(messageView.getBody().remaining());
            }
        } finally {
            pendingMessagesLock.writeLock().unlock();
            // Dispose corrupted messages.
            corrupted.forEach(messageView -> {
                final MessageId messageId = messageView.getMessageId();
                if (consumer.getPushConsumerSettings().isFifo()) {
                    LOGGER.error("Message is corrupted, forward it to dead letter queue in fifo mode, mq={}, messageId={}, clientId={}", mq, messageId, consumer.getClientId());
                    forwardToDeadLetterQueue(messageView);
                    return;
                }
                LOGGER.error("Message is corrupted, nack it in standard mode, mq={}, messageId={}, clientId={}", mq, messageId, consumer.getClientId());
                nackMessage(messageView);
            });
        }
    }

    private int getReceptionBatchSize() {
        int bufferSize = consumer.cacheMessageCountThresholdPerQueue() - this.cachedMessagesCount();
        bufferSize = Math.max(bufferSize, 1);
        return Math.min(bufferSize, consumer.getPushConsumerSettings().getReceiveBatchSize());
    }

    @Override
    public void fetchMessageImmediately() {
        receiveMessageImmediately();
    }

    /**
     * Receive message later by message queue.
     *
     * <p> Make sure that no exception will be thrown.
     */
    public void receiveMessageLater() {
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(this::receiveMessage, RECEIVE_LATER_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            LOGGER.error("[Bug] Failed to schedule receive message request, mq={}, clientId={}", mq, consumer.getClientId(), t);
            receiveMessageLater();
        }
    }

    public void receiveMessage() {
        if (dropped) {
            LOGGER.info("Process queue has been dropped, no longer receive message, mq={}, clientId={}", mq, consumer.getClientId());
            return;
        }
        if (this.isCacheFull()) {
            LOGGER.warn("Process queue cache is full, would receive message later, mq={}, clientId={}", mq, consumer.getClientId());
            receiveMessageLater();
            return;
        }
        receiveMessageImmediately();
    }

    private void receiveMessageImmediately() {
        if (!consumer.isRunning()) {
            LOGGER.info("Stop to receive message because consumer is not running, mq={}, clientId={}", mq, consumer.getClientId());
            return;
        }
        try {
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final int batchSize = this.getReceptionBatchSize();
            final ReceiveMessageRequest request = consumer.wrapReceiveMessageRequest(batchSize, mq, filterExpression);
            activityNanoTime = System.nanoTime();

            // Intercept before message reception.
            consumer.doBefore(MessageHookPoints.RECEIVE, Collections.emptyList());
            final Stopwatch stopwatch = Stopwatch.createStarted();

            final ListenableFuture<ReceiveMessageResult> future = consumer.receiveMessage(request, mq,
                consumer.getPushConsumerSettings().getLongPollingTimeout());
            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                @Override
                public void onSuccess(ReceiveMessageResult result) {
                    // Intercept after message reception.
                    final Duration duration = stopwatch.elapsed();
                    final List<MessageCommon> messageCommons = result.getMessages().stream().map(MessageViewImpl::getMessageCommon).collect(Collectors.toList());
                    if (result.getStatus().isPresent() && Code.OK.equals(result.getStatus().get().getCode())) {
                        consumer.doAfter(MessageHookPoints.RECEIVE, messageCommons, duration, MessageHookPointsStatus.OK);
                    } else {
                        consumer.doAfter(MessageHookPoints.RECEIVE, messageCommons, duration, MessageHookPointsStatus.ERROR);
                    }

                    try {
                        onReceiveMessageResult(result);
                    } catch (Throwable t) {
                        // Should never reach here.
                        LOGGER.error("[Bug] Exception raised while handling receive result, would receive later, mq={}, endpoints={}, clientId={}",
                            mq, endpoints, consumer.getClientId(), t);
                        receiveMessageLater();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // Intercept after message reception.
                    final Duration duration = stopwatch.elapsed();
                    consumer.doAfter(MessageHookPoints.RECEIVE, Collections.emptyList(), duration, MessageHookPointsStatus.ERROR);

                    LOGGER.error("Exception raised while message reception, would receive later, mq={}, endpoints={}, clientId={}",
                        mq, endpoints, consumer.getClientId(), t);
                    receiveMessageLater();
                }
            }, MoreExecutors.directExecutor());
            consumer.getReceptionTimes().getAndIncrement();
        } catch (Throwable t) {
            LOGGER.error("Exception raised while message reception, would receive later, mq={}, clientId={}", mq, consumer.getClientId(), t);
            receiveMessageLater();
        }
    }

    public boolean isCacheFull() {
        final int cacheMessageCountThresholdPerQueue = consumer.cacheMessageCountThresholdPerQueue();
        final long actualMessagesQuantity = this.cachedMessagesCount();
        if (cacheMessageCountThresholdPerQueue <= actualMessagesQuantity) {
            LOGGER.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}, mq={}, clientId={}",
                cacheMessageCountThresholdPerQueue, actualMessagesQuantity, mq, consumer.getClientId());
            return true;
        }
        final int cacheMessageBytesThresholdPerQueue = consumer.cacheMessageBytesThresholdPerQueue();
        final long actualCachedMessagesBytes = this.cachedMessageBytes();
        if (cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes) {
            LOGGER.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes, actual={} bytes, mq={}, clientId={}",
                cacheMessageBytesThresholdPerQueue, actualCachedMessagesBytes, mq, consumer.getClientId());
            return true;
        }
        return false;
    }

    public int cachedMessagesCount() {
        pendingMessagesLock.readLock().lock();
        inflightMessagesLock.readLock().lock();
        try {
            return pendingMessages.size() + inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
            pendingMessagesLock.readLock().unlock();
        }
    }

    public int inflightMessagesCount() {
        inflightMessagesLock.readLock().lock();
        try {
            return inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
        }
    }

    public long cachedMessageBytes() {
        return cachedMessagesBytes.get();
    }

    private void onReceiveMessageResult(ReceiveMessageResult result) {
        Optional<Status> optionalStatus = result.getStatus();
        final List<MessageViewImpl> messages = result.getMessages();
        final Endpoints endpoints = result.getEndpoints();
        if (!optionalStatus.isPresent()) {
            // Should never reach here.
            LOGGER.error("[Bug] Status in receive message result is not set, mq={}, endpoints={}", mq, endpoints);
            if (messages.isEmpty()) {
                receiveMessage();
            }
            optionalStatus = Optional.of(Status.newBuilder().setCode(Code.OK).build());
            LOGGER.error("[Bug] Status not set but message(s) found in the receive result, fix the status to OK, mq={}, endpoints={}", mq, endpoints);
        }
        final Status status = optionalStatus.get();
        final Code code = status.getCode();
        switch (code) {
            case OK:
                if (!messages.isEmpty()) {
                    cacheMessages(messages);
                    consumer.getReceivedMessagesQuantity().getAndAdd(messages.size());
                    consumer.getConsumeService().signal();
                }
                LOGGER.debug("Receive message with OK, mq={}, endpoints={}, messages found count={}, clientId={}", mq, endpoints, messages.size(), consumer.getClientId());
                receiveMessage();
                break;
            case MESSAGE_NOT_FOUND:
                LOGGER.info("Message not found while receiving message, mq={}, endpoints={}, clientId={}, code={}, status message=[{}]",
                    mq, endpoints, consumer.getClientId(), code.getNumber(), status.getMessage());
                receiveMessage();
                break;
            // Fall through on purpose.
            default:
                LOGGER.error("Failed to receive message from remote, try it later, mq={}, endpoints={}, clientId={}, code={}, status message=[{}]",
                    mq, endpoints, consumer.getClientId(), code.getNumber(), status.getMessage());
                receiveMessageLater();
        }
    }

    @Override
    public Optional<MessageViewImpl> tryTakeMessage() {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            final Optional<MessageViewImpl> first = pendingMessages.stream().findFirst();
            if (!first.isPresent()) {
                return first;
            }
            final MessageViewImpl messageView = first.get();
            inflightMessages.add(messageView);
            pendingMessages.remove(messageView);
            return first;
        } finally {
            inflightMessagesLock.writeLock().unlock();
            pendingMessagesLock.writeLock().unlock();
        }
    }

    private void eraseMessage(MessageViewImpl messageView) {
        inflightMessagesLock.writeLock().lock();
        try {
            if (inflightMessages.remove(messageView)) {
                cachedMessagesBytes.addAndGet(-messageView.getBody().remaining());
            }
        } finally {
            inflightMessagesLock.writeLock().unlock();
        }
    }

    private void statsConsumptionResult(ConsumeResult consumeResult) {
        if (ConsumeResult.OK.equals(consumeResult)) {
            consumer.consumptionOkQuantity.incrementAndGet();
            return;
        }
        consumer.consumptionErrorQuantity.incrementAndGet();
    }

    @Override
    public void eraseMessage(MessageViewImpl messageView, ConsumeResult consumeResult) {
        statsConsumptionResult(consumeResult);
        eraseMessage(messageView);
        if (ConsumeResult.OK.equals(consumeResult)) {
            consumer.ackMessage(messageView);
            return;
        }
        nackMessage(messageView);
    }

    private void nackMessage(MessageViewImpl messageView) {
        final Duration duration = consumer.getRetryPolicy().getNextAttemptDelay(messageView.getDeliveryAttempt());
        consumer.changInvisibleDuration(messageView, duration);
    }

    @Override
    public Iterator<MessageViewImpl> tryTakeFifoMessages() {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            Optional<MessageViewImpl> next = pendingMessages.stream().findFirst();
            // No new message arrived.
            if (!next.isPresent()) {
                return Collections.emptyIterator();
            }
            final MessageViewImpl first = next.get();
            Iterator<MessageViewImpl> iterator = first.iterator();
            iterator.forEachRemaining(messageView -> {
                pendingMessages.remove(messageView);
                inflightMessages.add(messageView);
            });
            return first.iterator();
        } finally {
            inflightMessagesLock.writeLock().unlock();
            pendingMessagesLock.writeLock().unlock();
        }
    }

    @Override
    public ListenableFuture<Void> eraseFifoMessage(MessageViewImpl messageView, ConsumeResult consumeResult) {
        statsConsumptionResult(consumeResult);
        final RetryPolicy retryPolicy = consumer.getRetryPolicy();
        final int maxAttempts = retryPolicy.getMaxAttempts();
        int attempt = messageView.getDeliveryAttempt();
        final MessageId messageId = messageView.getMessageId();
        final ConsumeService service = consumer.getConsumeService();
        if (ConsumeResult.ERROR.equals(consumeResult) && attempt < maxAttempts) {
            final Duration nextAttemptDelay = retryPolicy.getNextAttemptDelay(attempt);
            attempt = messageView.incrementAndGetDeliveryAttempt();
            LOGGER.debug("Prepare to redeliver the fifo message because of the consumption failure, maxAttempt={}, attempt={}, mq={}, messageId={}, nextAttemptDelay={}, clientId={}",
                maxAttempts, attempt, mq, messageId, nextAttemptDelay, consumer.getClientId());
            final ListenableFuture<ConsumeResult> future = service.consume(messageView, nextAttemptDelay);
            return Futures.transformAsync(future, result -> eraseFifoMessage(messageView, result), MoreExecutors.directExecutor());
        }
        boolean ok = ConsumeResult.OK.equals(consumeResult);
        if (!ok) {
            LOGGER.info("Failed to consume fifo message finally, run out of attempt times, maxAttempts={}, attempt={}, mq={}, messageId={}, clientId={}",
                maxAttempts, attempt, mq, messageId, consumer.getClientId());
        }
        // Ack message or forward it to DLQ depends on consumption result.
        ListenableFuture<Void> future = ok ? ackFifoMessage(messageView) : forwardToDeadLetterQueue(messageView);
        future.addListener(() -> eraseMessage(messageView), consumer.getConsumptionExecutor());
        return future;
    }

    private ListenableFuture<Void> forwardToDeadLetterQueue(final MessageViewImpl messageView) {
        final SettableFuture<Void> future = SettableFuture.create();
        forwardToDeadLetterQueue(messageView, 1, future);
        return future;
    }

    private void forwardToDeadLetterQueue(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future =
            consumer.forwardMessageToDeadLetterQueue(messageView);
        final String clientId = consumer.getClientId();
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    LOGGER.error("Failed to forward message to dead letter queue, would attempt to re-forward later, clientId={}, messageId={}, attempt={}, mq={}, code={}, status message={}",
                        clientId, messageView.getMessageId(), attempt, mq, code, status.getMessage());
                    forwardToDeadLetterQueue(messageView, 1 + attempt, future0);
                    return;
                }
                // Log retries.
                if (1 < attempt) {
                    LOGGER.info("Re-forward message to dead letter queue successfully, clientId={}, attempt={}, " +
                        "messageId={}, mq={}", clientId, attempt, messageView.getMessageId(), mq);
                }
                // Set result if message is forwarded successfully.
                future0.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                LOGGER.error("Exception raised while forward message to DLQ, would attempt to re-forward later, " +
                        "clientId={}, attempt={}, messageId={}, mq={}", clientId, attempt,
                    messageView.getMessageId(), mq, t);
                forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void forwardToDeadLetterQueueLater(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final MessageId messageId = messageView.getMessageId();
        final String clientId = consumer.getClientId();
        // Process queue is dropped, no need to proceed.
        if (dropped) {
            LOGGER.info("Process queue was dropped, give up to forward message to dead letter queue, mq={}, messageId={}, clientId={}", mq, messageId, clientId);
            return;
        }
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> forwardToDeadLetterQueue(messageView, attempt, future0),
                FORWARD_FIFO_MESSAGE_TO_DLQ_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            LOGGER.error("[Bug] Failed to schedule DLQ message request, mq={}, messageId={}, clientId={}", mq, messageView.getMessageId(), clientId);
            forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
        }
    }

    private ListenableFuture<Void> ackFifoMessage(final MessageViewImpl messageView) {
        SettableFuture<Void> future = SettableFuture.create();
        ackFifoMessage(messageView, 1, future);
        return future;
    }

    private void ackFifoMessage(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final Endpoints endpoints = messageView.getEndpoints();
        final String clientId = consumer.getClientId();
        final ListenableFuture<AckMessageResponse> future = consumer.ackMessage(messageView);
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    LOGGER.error("Failed to ack fifo message, would attempt to re-ack later, clientId={}, attempt={}, messageId={}, mq={}, code={}, endpoints={}, status message=[{}]",
                        clientId, attempt, messageView.getMessageId(), mq, code, endpoints, status.getMessage());
                    ackFifoMessageLater(messageView, 1 + attempt, future0);
                    return;
                }
                // Log retries.
                if (1 < attempt) {
                    LOGGER.info("Re-ack fifo message successfully, clientId={}, attempt={}, messageId={}, mq={}, endpoints={}",
                        clientId, attempt, messageView.getMessageId(), mq, endpoints);
                }
                // Set result if FIFO message is acknowledged successfully.
                future0.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                LOGGER.error("Exception raised while ack fifo message, clientId={}, would attempt to re-ack later, attempt={}, messageId={}, mq={}, endpoints={}",
                    clientId, attempt, messageView.getMessageId(), mq, endpoints, t);
                ackFifoMessageLater(messageView, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void ackFifoMessageLater(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final MessageId messageId = messageView.getMessageId();
        final String clientId = consumer.getClientId();
        // Process queue is dropped, no need to proceed.
        if (dropped) {
            LOGGER.info("Process queue was dropped, give up to ack message, mq={}, messageId={}, clientId={}", mq, messageId, clientId);
            return;
        }
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> ackFifoMessage(messageView, attempt, future0), ACK_FIFO_MESSAGE_DELAY.toNanos(),
                TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            LOGGER.error("[Bug] Failed to schedule ack fifo message request, mq={}, messageId={}, clientId={}", mq, messageId, clientId);
            ackFifoMessageLater(messageView, 1 + attempt, future0);
        }
    }

    @Override
    public long getPendingMessageCount() {
        pendingMessagesLock.readLock().lock();
        try {
            return pendingMessages.size();
        } finally {
            pendingMessagesLock.readLock().unlock();
        }
    }

    public long getInflightMessageCount() {
        inflightMessagesLock.readLock().lock();
        try {
            return inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
        }
    }

    @Override
    public long getCachedMessageBytes() {
        return cachedMessagesBytes.get();
    }
}
