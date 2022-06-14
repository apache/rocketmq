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

package org.apache.rocketmq.apis.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.message.Message;

/**
 * Producer is a thread-safe rocketmq client which is used to publish messages.
 *
 * <p>On account of network timeout or other reasons, rocketmq producer only promised the at-least-once semantics.
 * For producer, at-least-once semantics means potentially attempts are made at sending it, messages may be
 * duplicated but not lost. Especially, potentially attempts are not made using {@link #send(Message, Transaction)}.
 */
public interface Producer extends Closeable {
    /**
     * Sends a message synchronously.
     *
     * <p>This method does not return until it gets the definitive result.
     *
     * @param message message to send.
     * @return send receipt.
     */
    SendReceipt send(Message message) throws ClientException;

    /**
     * Sends a transactional message synchronously.
     *
     * @param message     message to send.
     * @param transaction transaction to bind.
     * @return send receipt.
     */
    SendReceipt send(Message message, Transaction transaction) throws ClientException;

    /**
     * Sends a message asynchronously.
     *
     * <p>This method returns immediately, the result is included in the {@link CompletableFuture};
     *
     * @param message message to send.
     * @return a future that indicates send receipt.
     */
    CompletableFuture<SendReceipt> sendAsync(Message message);

    /**
     * Sends batch messages synchronously.
     *
     * <p>This method does not return until it gets the definitive result.
     *
     * <p>All messages to send should have the same topic.
     *
     * @param messages batch messages to send.
     * @return collection indicates send receipt.
     */
    List<SendReceipt> send(List<Message> messages) throws ClientException;

    /**
     * Begins a transaction.
     *
     * <p>For example:
     *
     * <pre>{@code
     * Transaction transaction = producer.beginTransaction();
     * SendReceipt receipt1 = producer.send(message1, transaction);
     * SendReceipt receipt2 = producer.send(message2, transaction);
     * transaction.commit();
     * }</pre>
     *
     * @return a transaction entity to execute commit/rollback operation.
     */
    Transaction beginTransaction() throws ClientException;

    /**
     * Closes the producer and release all related resources.
     *
     * <p>This method does not return until all related resource is released.
     */
    @Override
    void close() throws IOException;
}
