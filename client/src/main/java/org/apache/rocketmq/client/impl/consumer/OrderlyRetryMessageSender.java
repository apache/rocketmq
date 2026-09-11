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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;

final class OrderlyRetryMessageSender {
    private static final Logger LOG = LoggerFactory.getLogger(OrderlyRetryMessageSender.class);
    // A retry topic created by the broker always has queue 0, even before its route is published.
    private static final int RETRY_QUEUE_ID = 0;

    private OrderlyRetryMessageSender() {
    }

    static void send(DefaultMQPushConsumerImpl consumer, MessageExt originalMessage, Message retryMessage)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        DefaultMQProducer producer = consumer.getmQClientFactory().getDefaultMQProducer();
        List<String> brokerNames = findTopicBrokerNames(consumer, originalMessage);
        if (brokerNames.isEmpty()) {
            producer.send(copyMessage(retryMessage));
            return;
        }

        for (int i = 0; i < brokerNames.size(); i++) {
            String brokerName = brokerNames.get(i);
            MessageQueue retryQueue = new MessageQueue(retryMessage.getTopic(), brokerName, RETRY_QUEUE_ID);
            Message messageToSend = copyMessage(retryMessage);
            try {
                producer.send(messageToSend, retryQueue);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (MQClientException | RemotingException | MQBrokerException e) {
                if (i == brokerNames.size() - 1) {
                    throw e;
                }
                LOG.warn("Failed to send orderly retry message to broker {}, try next topic broker", brokerName, e);
            }
        }
    }

    private static List<String> findTopicBrokerNames(DefaultMQPushConsumerImpl consumer,
        MessageExt originalMessage) {
        Set<MessageQueue> messageQueues;
        String originalTopic = originalMessage.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (originalTopic == null || originalTopic.isEmpty()) {
            originalTopic = originalMessage.getTopic();
        }
        try {
            messageQueues = consumer.fetchSubscribeMessageQueues(originalTopic);
        } catch (MQClientException e) {
            return Collections.emptyList();
        }

        Set<String> brokerNames = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            if (messageQueue.getBrokerName() != null && !messageQueue.getBrokerName().isEmpty()) {
                brokerNames.add(messageQueue.getBrokerName());
            }
        }

        List<String> result = new ArrayList<>();
        String originalBrokerName = originalMessage.getBrokerName();
        if (brokerNames.remove(originalBrokerName)) {
            result.add(originalBrokerName);
        }
        List<String> remainingBrokerNames = new ArrayList<>(brokerNames);
        Collections.sort(remainingBrokerNames);
        result.addAll(remainingBrokerNames);
        return result;
    }

    private static Message copyMessage(Message message) {
        Message result = MessageAccessor.cloneMessage(message);
        MessageAccessor.setProperties(result, MessageAccessor.deepCopyProperties(message.getProperties()));
        result.setTransactionId(message.getTransactionId());
        return result;
    }
}
