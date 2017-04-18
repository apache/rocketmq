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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.PropertyKeys;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

class LocalMessageCache {
    private final BlockingQueue<ConsumeRequest> consumeRequestCache;
    private final Map<String, ConsumeRequest> consumedRequest;
    private final ConcurrentHashMap<MessageQueue, Long> pullOffsetTable;
    private final DefaultMQPullConsumer rocketmqPullConsumer;
    private int pullBatchNums = 32;
    private int pollTimeout = -1;
    private final static Logger log = ClientLogger.getLog();

    LocalMessageCache(final DefaultMQPullConsumer rocketmqPullConsumer, final KeyValue properties) {
        int cacheCapacity = 1000;
        if (properties.containsKey(NonStandardKeys.PULL_MESSAGE_CACHE_CAPACITY)) {
            cacheCapacity = properties.getInt(NonStandardKeys.PULL_MESSAGE_CACHE_CAPACITY);
        }
        consumeRequestCache = new LinkedBlockingQueue<>(cacheCapacity);

        if (properties.containsKey(NonStandardKeys.PULL_MESSAGE_BATCH_NUMS)) {
            pullBatchNums = properties.getInt(NonStandardKeys.PULL_MESSAGE_BATCH_NUMS);
        }

        if (properties.containsKey(PropertyKeys.OPERATION_TIMEOUT)) {
            pollTimeout = properties.getInt(PropertyKeys.OPERATION_TIMEOUT);
        }

        this.consumedRequest = new ConcurrentHashMap<>();
        this.pullOffsetTable = new ConcurrentHashMap<>();
        this.rocketmqPullConsumer = rocketmqPullConsumer;
    }

    int nextPullBatchNums() {
        return Math.min(pullBatchNums, consumeRequestCache.remainingCapacity());
    }

    long nextPullOffset(MessageQueue remoteQueue) {
        if (!pullOffsetTable.containsKey(remoteQueue)) {
            try {
                pullOffsetTable.putIfAbsent(remoteQueue,
                    rocketmqPullConsumer.fetchConsumeOffset(remoteQueue, false));
            } catch (MQClientException e) {
                log.error("A error occurred in fetch consume offset process.", e);
            }
        }
        return pullOffsetTable.get(remoteQueue);
    }

    void updatePullOffset(MessageQueue remoteQueue, long nextPullOffset) {
        pullOffsetTable.put(remoteQueue, nextPullOffset);
    }

    void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException ignore) {
        }
    }

    MessageExt poll() {
        try {
            ConsumeRequest consumeRequest = consumeRequestCache.take();
            consumeRequest.setStartConsumeTimeMillis(System.currentTimeMillis());
            consumedRequest.put(consumeRequest.getMessageExt().getMsgId(), consumeRequest);
            return consumeRequest.getMessageExt();
        } catch (InterruptedException ignore) {
        }
        return null;
    }

    MessageExt poll(final KeyValue properties) {
        int currentPollTimeout = pollTimeout;
        if (properties.containsKey(PropertyKeys.OPERATION_TIMEOUT)) {
            currentPollTimeout = properties.getInt(PropertyKeys.OPERATION_TIMEOUT);
        }

        if (currentPollTimeout == -1) {
            return poll();
        }

        try {
            ConsumeRequest consumeRequest = consumeRequestCache.poll(currentPollTimeout, TimeUnit.MILLISECONDS);
            consumeRequest.setStartConsumeTimeMillis(System.currentTimeMillis());
            consumedRequest.put(consumeRequest.getMessageExt().getMsgId(), consumeRequest);
            return consumeRequest.getMessageExt();
        } catch (InterruptedException ignore) {
        }
        return null;
    }

    void ack(final String messageId) {
        ConsumeRequest consumeRequest = consumedRequest.remove(messageId);
        if (consumeRequest != null) {
            long offset = consumeRequest.getProcessQueue().removeMessage(Collections.singletonList(consumeRequest.getMessageExt()));
            try {
                rocketmqPullConsumer.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
            } catch (MQClientException e) {
                log.error("A error occurred in update consume offset process.", e);
            }
        }
    }
}
