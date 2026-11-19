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
package org.apache.rocketmq.tools.command.message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Records messages delivered by a temporary push consumer and exposes a stable summary for the command output.
 */
public class PushConsumeMessageObserver implements MessageListenerConcurrently, MessageListenerOrderly {
    private final PushConsumeMessageConfig config;
    private final PushConsumeMessagePrinter printer;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final AtomicLong messageCount = new AtomicLong();
    private final AtomicLong bodyBytes = new AtomicLong();
    private final AtomicLong firstReceiveTimestamp = new AtomicLong();
    private final AtomicLong lastReceiveTimestamp = new AtomicLong();
    private final ConcurrentMap<MessageQueue, QueueStats> queueStatsTable = new ConcurrentHashMap<>();

    public PushConsumeMessageObserver(PushConsumeMessageConfig config, PushConsumeMessagePrinter printer) {
        this.config = config;
        this.printer = printer;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        record(messages, context.getMessageQueue());
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messages, ConsumeOrderlyContext context) {
        record(messages, context.getMessageQueue());
        return ConsumeOrderlyStatus.SUCCESS;
    }

    void record(List<MessageExt> messages, MessageQueue deliveryQueue) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        for (MessageExt message : messages) {
            long receiveTimestamp = System.currentTimeMillis();
            firstReceiveTimestamp.compareAndSet(0, receiveTimestamp);
            lastReceiveTimestamp.set(receiveTimestamp);
            long currentCount = messageCount.incrementAndGet();
            if (message.getBody() != null) {
                bodyBytes.addAndGet(message.getBody().length);
            }

            MessageQueue messageQueue = resolveMessageQueue(message, deliveryQueue);
            queueStatsTable.computeIfAbsent(messageQueue, ignored -> new QueueStats())
                .record(message.getQueueOffset(), message.getStoreTimestamp());
            printer.printMessage(currentCount, receiveTimestamp, messageQueue, message);
        }

        if (config.getMaxMessages() > 0 && messageCount.get() >= config.getMaxMessages()) {
            completionLatch.countDown();
        }
    }

    public CompletionReason awaitCompletion() throws InterruptedException {
        if (config.getMaxMessages() == 0 && config.getMaxWaitMillis() == 0) {
            completionLatch.await();
            return CompletionReason.STOPPED;
        }

        if (config.getMaxWaitMillis() == 0) {
            completionLatch.await();
            return CompletionReason.MESSAGE_LIMIT;
        }

        boolean completed = completionLatch.await(config.getMaxWaitMillis(), TimeUnit.MILLISECONDS);
        return completed ? CompletionReason.MESSAGE_LIMIT : CompletionReason.TIME_LIMIT;
    }

    public void stop() {
        completionLatch.countDown();
    }

    public Snapshot snapshot() {
        List<QueueSnapshot> queues = new ArrayList<>();
        for (Map.Entry<MessageQueue, QueueStats> entry : queueStatsTable.entrySet()) {
            QueueStats stats = entry.getValue();
            queues.add(new QueueSnapshot(entry.getKey(), stats.count.get(), stats.firstOffset.get(),
                stats.lastOffset.get(), stats.lastStoreTimestamp.get()));
        }
        queues.sort(Comparator
            .comparing((QueueSnapshot value) -> value.getMessageQueue().getBrokerName(),
                Comparator.nullsFirst(String::compareTo))
            .thenComparingInt(value -> value.getMessageQueue().getQueueId()));
        return new Snapshot(messageCount.get(), bodyBytes.get(), firstReceiveTimestamp.get(),
            lastReceiveTimestamp.get(), queues);
    }

    private MessageQueue resolveMessageQueue(MessageExt message, MessageQueue deliveryQueue) {
        if (deliveryQueue != null) {
            return deliveryQueue;
        }
        return new MessageQueue(message.getTopic(), message.getBrokerName(), message.getQueueId());
    }

    public enum CompletionReason {
        MESSAGE_LIMIT,
        TIME_LIMIT,
        INTERRUPTED,
        STOPPED
    }

    private static class QueueStats {
        private final AtomicLong count = new AtomicLong();
        private final AtomicLong firstOffset = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong lastOffset = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong lastStoreTimestamp = new AtomicLong();

        private void record(long queueOffset, long storeTimestamp) {
            count.incrementAndGet();
            firstOffset.accumulateAndGet(queueOffset, Math::min);
            lastOffset.accumulateAndGet(queueOffset, Math::max);
            lastStoreTimestamp.accumulateAndGet(storeTimestamp, Math::max);
        }
    }

    public static class Snapshot {
        private final long messageCount;
        private final long bodyBytes;
        private final long firstReceiveTimestamp;
        private final long lastReceiveTimestamp;
        private final List<QueueSnapshot> queueSnapshots;

        Snapshot(long messageCount, long bodyBytes, long firstReceiveTimestamp, long lastReceiveTimestamp,
            List<QueueSnapshot> queueSnapshots) {
            this.messageCount = messageCount;
            this.bodyBytes = bodyBytes;
            this.firstReceiveTimestamp = firstReceiveTimestamp;
            this.lastReceiveTimestamp = lastReceiveTimestamp;
            this.queueSnapshots = queueSnapshots;
        }

        public long getMessageCount() {
            return messageCount;
        }

        public long getBodyBytes() {
            return bodyBytes;
        }

        public long getFirstReceiveTimestamp() {
            return firstReceiveTimestamp;
        }

        public long getLastReceiveTimestamp() {
            return lastReceiveTimestamp;
        }

        public List<QueueSnapshot> getQueueSnapshots() {
            return queueSnapshots;
        }
    }

    public static class QueueSnapshot {
        private final MessageQueue messageQueue;
        private final long messageCount;
        private final long firstOffset;
        private final long lastOffset;
        private final long lastStoreTimestamp;

        QueueSnapshot(MessageQueue messageQueue, long messageCount, long firstOffset, long lastOffset,
            long lastStoreTimestamp) {
            this.messageQueue = messageQueue;
            this.messageCount = messageCount;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.lastStoreTimestamp = lastStoreTimestamp;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public long getMessageCount() {
            return messageCount;
        }

        public long getFirstOffset() {
            return firstOffset;
        }

        public long getLastOffset() {
            return lastOffset;
        }

        public long getLastStoreTimestamp() {
            return lastStoreTimestamp;
        }
    }
}
