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

package org.apache.rocketmq.client.producer;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ProduceAccumulator {
    // totalHoldSize normal value
    private long totalHoldSize = 32 * 1024 * 1024;
    // holdSize normal value
    private long holdSize = 32 * 1024;
    // holdMs normal value
    private int holdMs = 10;
    private final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);
    private final GuardForAsyncSendService guardThreadForAsyncSend;
    private final Map<AggregateKey, MessageAccumulation> syncSendBatchs = new ConcurrentHashMap<AggregateKey, MessageAccumulation>();
    private final Map<AggregateKey, MessageAccumulation> asyncSendBatchs = new ConcurrentHashMap<AggregateKey, MessageAccumulation>();
    private final AtomicLong currentlyHoldSize = new AtomicLong(0);
    private final String instanceName;

    public ProduceAccumulator(String instanceName) {
        this.instanceName = instanceName;
        this.guardThreadForAsyncSend = new GuardForAsyncSendService(this.instanceName);
    }

    /**
     * Sends async batches whose hold time has expired. Sync batches need no guard: their callers wait for the hold
     * time themselves and send the batch on expiry.
     */
    private class GuardForAsyncSendService extends ServiceThread {
        private final String serviceName;

        public GuardForAsyncSendService(String clientInstanceName) {
            serviceName = String.format("Client_%s_GuardForAsyncSend", clientInstanceName);
        }

        @Override
        public String getServiceName() {
            return serviceName;
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.doWork();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        private void doWork() throws InterruptedException {
            for (MessageAccumulation v : asyncSendBatchs.values()) {
                if (v.readyToSend()) {
                    v.send();
                }
            }
            Thread.sleep(Math.max(1, holdMs / 2));
        }
    }

    void start() {
        guardThreadForAsyncSend.start();
    }

    void shutdown() {
        guardThreadForAsyncSend.shutdown();
    }

    int getBatchMaxDelayMs() {
        return holdMs;
    }

    void batchMaxDelayMs(int holdMs) {
        if (holdMs <= 0 || holdMs > 30 * 1000) {
            throw new IllegalArgumentException(String.format("batchMaxDelayMs expect between 1ms and 30s, but get %d!", holdMs));
        }
        this.holdMs = holdMs;
    }

    long getBatchMaxBytes() {
        return holdSize;
    }

    void batchMaxBytes(long holdSize) {
        if (holdSize <= 0 || holdSize > 2 * 1024 * 1024) {
            throw new IllegalArgumentException(String.format("batchMaxBytes expect between 1B and 2MB, but get %d!", holdSize));
        }
        this.holdSize = holdSize;
    }

    long getTotalBatchMaxBytes() {
        return holdSize;
    }

    void totalBatchMaxBytes(long totalHoldSize) {
        if (totalHoldSize <= 0) {
            throw new IllegalArgumentException(String.format("totalBatchMaxBytes must bigger then 0, but get %d!", totalHoldSize));
        }
        this.totalHoldSize = totalHoldSize;
    }

    SendResult send(Message msg, MessageQueue mq,
        DefaultMQProducer defaultMQProducer) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        CompletableFuture<SendResult> result = new CompletableFuture<>();
        MessageAccumulation batch = addToBatch(syncSendBatchs, false, msg, mq, completing(result), defaultMQProducer);
        try {
            try {
                return Uninterruptibles.getUninterruptibly(result, batch.remainingHoldMs(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                batch.send();
            }
            return Uninterruptibles.getUninterruptibly(result);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.throwIfInstanceOf(cause, MQBrokerException.class);
            Throwables.throwIfInstanceOf(cause, RemotingException.class);
            Throwables.throwIfInstanceOf(cause, MQClientException.class);
            Throwables.throwIfInstanceOf(cause, InterruptedException.class);
            Throwables.throwIfUnchecked(cause);
            throw new MQClientException("send message batch failed", cause);
        }
    }

    void send(Message msg, MessageQueue mq, SendCallback sendCallback, DefaultMQProducer defaultMQProducer) {
        addToBatch(asyncSendBatchs, true, msg, mq, sendCallback, defaultMQProducer);
    }

    private MessageAccumulation addToBatch(Map<AggregateKey, MessageAccumulation> batches, boolean asyncSend,
        Message msg, MessageQueue mq, SendCallback sendCallback, DefaultMQProducer defaultMQProducer) {
        AggregateKey aggregateKey = new AggregateKey(msg, mq);
        while (true) {
            MessageAccumulation batch = batches.get(aggregateKey);
            if (batch == null) {
                MessageAccumulation created = new MessageAccumulation(aggregateKey, batches, asyncSend, defaultMQProducer);
                batch = batches.putIfAbsent(aggregateKey, created);
                if (batch == null) {
                    batch = created;
                }
            }
            // a closed batch has already removed itself from the map, so the retry picks up a fresh one
            if (batch.add(msg, sendCallback)) {
                return batch;
            }
        }
    }

    private static SendCallback completing(final CompletableFuture<SendResult> future) {
        return new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                future.complete(sendResult);
            }

            @Override
            public void onException(Throwable e) {
                future.completeExceptionally(e);
            }
        };
    }

    boolean tryAddMessage(Message message) {
        int bodySize = null == message.getBody() ? 0 : message.getBody().length;
        while (true) {
            long current = currentlyHoldSize.get();
            if (current >= totalHoldSize) {
                return false;
            }
            if (bodySize <= 0 || currentlyHoldSize.compareAndSet(current, current + bodySize)) {
                return true;
            }
        }
    }

    long getCurrentlyHoldSize() {
        return currentlyHoldSize.get();
    }

    int pendingBatchCount() {
        return syncSendBatchs.size() + asyncSendBatchs.size();
    }

    private static class AggregateKey {
        private final String topic;
        private final MessageQueue mq;
        private final boolean waitStoreMsgOK;
        private final String tag;

        AggregateKey(Message message, MessageQueue mq) {
            this.topic = message.getTopic();
            this.mq = mq;
            this.waitStoreMsgOK = message.isWaitStoreMsgOK();
            this.tag = message.getTags();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AggregateKey key = (AggregateKey) o;
            return waitStoreMsgOK == key.waitStoreMsgOK && topic.equals(key.topic) && Objects.equals(mq, key.mq) && Objects.equals(tag, key.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, mq, waitStoreMsgOK, tag);
        }
    }

    /**
     * One batch of messages sharing an {@link AggregateKey}. Lifecycle: open (accepting messages) -> closed (removed
     * from its map and handed to sendDirect exactly once) -> completed ({@link #sent} resolved, hold size released and
     * each message's callback invoked in add order).
     */
    private class MessageAccumulation {
        private final AggregateKey aggregateKey;
        private final Map<AggregateKey, MessageAccumulation> batches;
        private final boolean asyncSend;
        private final DefaultMQProducer defaultMQProducer;
        private final long createTime = System.currentTimeMillis();
        private final CompletableFuture<SendResult> sent = new CompletableFuture<>();
        // guarded by this; frozen once closed
        private final List<Message> messages = new ArrayList<>();
        private final List<SendCallback> sendCallbacks = new ArrayList<>();
        private final Set<String> keys = new HashSet<>();
        private long messagesSize;
        private boolean closed;

        MessageAccumulation(AggregateKey aggregateKey, Map<AggregateKey, MessageAccumulation> batches,
            boolean asyncSend, DefaultMQProducer defaultMQProducer) {
            this.aggregateKey = aggregateKey;
            this.batches = batches;
            this.asyncSend = asyncSend;
            this.defaultMQProducer = defaultMQProducer;
            this.sent.whenComplete(this::complete);
        }

        synchronized boolean readyToSend() {
            return !messages.isEmpty() && (messagesSize > holdSize || remainingHoldMs() <= 0);
        }

        long remainingHoldMs() {
            return createTime + holdMs - System.currentTimeMillis();
        }

        /**
         * @return false if the batch is already closed and the message was not added
         */
        boolean add(Message msg, SendCallback sendCallback) {
            boolean ready;
            synchronized (this) {
                if (closed) {
                    return false;
                }
                messages.add(msg);
                sendCallbacks.add(sendCallback);
                messagesSize += null == msg.getBody() ? 0 : msg.getBody().length;
                String msgKeys = msg.getKeys();
                if (msgKeys != null) {
                    keys.addAll(Arrays.asList(msgKeys.split(MessageConst.KEY_SEPARATOR)));
                }
                ready = readyToSend();
            }
            if (ready) {
                send();
            }
            return true;
        }

        /**
         * Closes the batch and sends it. Only the first caller sends; later calls are no-ops.
         */
        void send() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                batches.remove(aggregateKey, this);
            }
            // the batch carries other callers' messages, so the triggering caller's interrupt must not abort it
            boolean interrupted = Thread.interrupted();
            try {
                if (defaultMQProducer == null) {
                    throw new IllegalArgumentException("defaultMQProducer is null, can not send message");
                }
                if (asyncSend) {
                    defaultMQProducer.sendDirect(batch(), aggregateKey.mq, completing(sent));
                } else {
                    sent.complete(defaultMQProducer.sendDirect(batch(), aggregateKey.mq, null));
                }
            } catch (Throwable e) {
                sent.completeExceptionally(e);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void complete(SendResult sendResult, Throwable error) {
            currentlyHoldSize.addAndGet(-messagesSize);
            SendResult[] sendResults = null;
            if (error == null) {
                try {
                    sendResults = splitSendResults(sendResult);
                } catch (Exception e) {
                    error = e;
                }
            }
            for (int i = 0; i < sendCallbacks.size(); i++) {
                try {
                    if (error == null) {
                        sendCallbacks.get(i).onSuccess(sendResults[i]);
                    } else {
                        sendCallbacks.get(i).onException(error);
                    }
                } catch (Throwable e) {
                    log.warn("execute send callback of batch message failed", e);
                }
            }
        }

        private MessageBatch batch() {
            MessageBatch messageBatch = new MessageBatch(this.messages);
            messageBatch.setTopic(this.aggregateKey.topic);
            messageBatch.setWaitStoreMsgOK(this.aggregateKey.waitStoreMsgOK);
            messageBatch.setKeys(this.keys);
            messageBatch.setTags(this.aggregateKey.tag);
            MessageClientIDSetter.setUniqID(messageBatch);
            messageBatch.setBody(MessageDecoder.encodeMessages(this.messages));
            return messageBatch;
        }

        private SendResult[] splitSendResults(SendResult sendResult) {
            if (sendResult == null) {
                throw new IllegalArgumentException("sendResult is null");
            }
            int count = this.messages.size();
            boolean isBatchConsumerQueue = !sendResult.getMsgId().contains(",");
            SendResult[] sendResults = new SendResult[count];
            if (!isBatchConsumerQueue) {
                String[] msgIds = sendResult.getMsgId().split(",");
                String[] offsetMsgIds = sendResult.getOffsetMsgId().split(",");
                if (offsetMsgIds.length != count || msgIds.length != count) {
                    throw new IllegalArgumentException("sendResult is illegal");
                }
                for (int i = 0; i < count; i++) {
                    sendResults[i] = new SendResult(sendResult.getSendStatus(), msgIds[i],
                        sendResult.getMessageQueue(), sendResult.getQueueOffset() + i,
                        sendResult.getTransactionId(), offsetMsgIds[i], sendResult.getRegionId());
                }
            } else {
                for (int i = 0; i < count; i++) {
                    sendResults[i] = sendResult;
                }
            }
            return sendResults;
        }
    }
}
