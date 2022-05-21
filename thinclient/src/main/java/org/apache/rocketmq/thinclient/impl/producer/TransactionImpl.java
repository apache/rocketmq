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

package org.apache.rocketmq.thinclient.impl.producer;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Transaction;
import org.apache.rocketmq.apis.producer.TransactionResolution;
import org.apache.rocketmq.thinclient.message.PublishingMessageImpl;

class TransactionImpl implements Transaction {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

    private final ProducerImpl producerImpl;

    private static final int MAX_MESSAGE_NUM = 1;

    @GuardedBy("messagesLock")
    private final Set<PublishingMessageImpl> messages;
    private final ReadWriteLock messagesLock;

    private final ConcurrentMap<PublishingMessageImpl, SendReceiptImpl> messageSendReceiptMap;

    public TransactionImpl(ProducerImpl producerImpl) {
        this.producerImpl = producerImpl;
        this.messages = new HashSet<>();
        this.messagesLock = new ReentrantReadWriteLock();
        this.messageSendReceiptMap = new ConcurrentHashMap<>();
    }

    public PublishingMessageImpl tryAddMessage(Message message) throws IOException {
        messagesLock.readLock().lock();
        try {
            if (messages.size() > MAX_MESSAGE_NUM) {
                throw new IllegalArgumentException("Message in transaction has exceeded the threshold: " + MAX_MESSAGE_NUM);
            }
        } finally {
            messagesLock.readLock().unlock();
        }
        messagesLock.writeLock().lock();
        try {
            if (messages.size() >= MAX_MESSAGE_NUM) {
                throw new IllegalArgumentException("Message in transaction has exceeded the threshold: " + MAX_MESSAGE_NUM);
            }
            final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, producerImpl.producerSettings, true);
            messages.add(publishingMessage);
            return publishingMessage;
        } finally {
            messagesLock.writeLock().unlock();
        }
    }

    public void tryAddReceipt(PublishingMessageImpl publishingMessage, SendReceiptImpl sendReceipt) {
        messagesLock.readLock().lock();
        try {
            if (!messages.contains(publishingMessage)) {
                LOGGER.warn("message(s) is not contained in current transaction");
                return;
            }
            messageSendReceiptMap.put(publishingMessage, sendReceipt);
        } finally {
            messagesLock.readLock().unlock();
        }
    }

    @Override
    public void commit() throws ClientException {
        if (messageSendReceiptMap.isEmpty()) {
            throw new IllegalStateException("Transactional message has not been sent yet");
        }
        for (Map.Entry<PublishingMessageImpl, SendReceiptImpl> entry : messageSendReceiptMap.entrySet()) {
            final PublishingMessageImpl publishingMessage = entry.getKey();
            final SendReceiptImpl sendReceipt = entry.getValue();
            producerImpl.endTransaction(sendReceipt.getEndpoints(), publishingMessage.getMessageCommon(), sendReceipt.getMessageId(), sendReceipt.getTransactionId(), TransactionResolution.COMMIT);
        }
    }

    @Override
    public void rollback() throws ClientException {
        if (messageSendReceiptMap.isEmpty()) {
            throw new IllegalStateException("Transactional message has not been sent yet");
        }
        for (Map.Entry<PublishingMessageImpl, SendReceiptImpl> entry : messageSendReceiptMap.entrySet()) {
            final PublishingMessageImpl publishingMessage = entry.getKey();
            final SendReceiptImpl sendReceipt = entry.getValue();
            producerImpl.endTransaction(sendReceipt.getEndpoints(), publishingMessage.getMessageCommon(), sendReceipt.getMessageId(), sendReceipt.getTransactionId(), TransactionResolution.ROLLBACK);
        }
    }
}
