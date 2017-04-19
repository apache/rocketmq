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
import io.openmessaging.rocketmq.ClientConfig;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
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
    private final ClientConfig clientConfig;
    private final static Logger log = ClientLogger.getLog();

    LocalMessageCache(final DefaultMQPullConsumer rocketmqPullConsumer, final ClientConfig clientConfig) {
        consumeRequestCache = new LinkedBlockingQueue<>(clientConfig.getRmqPullMessageCacheCapacity());
        this.consumedRequest = new ConcurrentHashMap<>();
        this.pullOffsetTable = new ConcurrentHashMap<>();
        this.rocketmqPullConsumer = rocketmqPullConsumer;
        this.clientConfig = clientConfig;
    }

    int nextPullBatchNums() {
        return Math.min(clientConfig.getRmqPullMessageBatchNums(), consumeRequestCache.remainingCapacity());
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
        return poll(clientConfig.getOmsOperationTimeout());
    }

    MessageExt poll(final KeyValue properties) {
        int currentPollTimeout = clientConfig.getOmsOperationTimeout();
        if (properties.containsKey(PropertyKeys.OPERATION_TIMEOUT)) {
            currentPollTimeout = properties.getInt(PropertyKeys.OPERATION_TIMEOUT);
        }
        return poll(currentPollTimeout);
    }

    private MessageExt poll(long timeout) {
        try {
            ConsumeRequest consumeRequest = consumeRequestCache.poll(timeout, TimeUnit.MILLISECONDS);
            if (consumeRequest != null) {
                consumeRequest.setStartConsumeTimeMillis(System.currentTimeMillis());
                consumedRequest.put(consumeRequest.getMessageExt().getMsgId(), consumeRequest);
                return consumeRequest.getMessageExt();
            }
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
