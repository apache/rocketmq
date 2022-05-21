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

package org.apache.rocketmq.apis.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.message.MessageView;

/**
 * SimpleConsumer is a thread-safe rocketmq client which is used to consume message by group.
 *
 * <p>Simple consumer is lightweight consumer , if you want fully control the message consumption operation by yourself,
 * simple consumer should be your first consideration.
 *
 * <p>Consumers belong to the same consumer group share messages from server,
 * so consumer in the same group must have the same subscription expressions, otherwise the behavior is
 * undefined. If a new consumer group's consumer is started first time, it consumes from the latest position. Once
 * consumer is started, server records its consumption progress and derives it in subsequent startup.
 *
 * <p>You may intend to maintain different consumption progress for different consumer, different consumer group
 * should be set in this case.
 *
 * <p> Simple consumer divide message consumption to 3 parts.
 * Firstly, call receive api get messages from server; Then process message by yourself; At last, your must call Ack api to commit this message.
 * If there is error when process message ,your can reconsume the message later which control by the invisibleDuration parameter.
 * Also, you can change the invisibleDuration by call changeInvisibleDuration api.
 */
public interface SimpleConsumer extends Closeable {
    /**
     * Get the load balancing group for simple consumer.
     *
     * @return consumer load balancing group.
     */
    String getConsumerGroup();

    /**
     * Add subscription expression dynamically.
     *
     * <p>If first subscriptionExpression that contains topicA and tag1 is exists already in consumer, then
     * second subscriptionExpression which contains topicA and tag2, <strong>the result is that the second one
     * replaces the first one instead of integrating them</strong>.
     *
     * @param topic            new topic that need to add or update.
     * @param filterExpression new filter expression to add or update.
     * @return simple consumer instance.
     */
    SimpleConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException;

    /**
     * Remove subscription expression dynamically by topic.
     *
     * <p>It stops the backend task to fetch message from remote, and besides that, the local cached message whose topic
     * was removed before would not be delivered to {@link MessageListener} anymore.
     *
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of push consumer.
     *
     * @param topic the topic to remove subscription.
     * @return simple consumer instance.
     */
    SimpleConsumer unsubscribe(String topic) throws ClientException;

    /**
     * List the existed subscription expressions in simple consumer.
     *
     * @return map of topic to filter expression.
     */
    Map<String, FilterExpression> getSubscriptionExpressions();

    /**
     * Fetch messages from server synchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     *
     * @param maxMessageNum     max message num when server returns.
     * @param invisibleDuration set the invisibleDuration of messages return from server. These messages will be invisible to other consumer unless timout.
     * @return list of messageView
     */
    List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException;

    /**
     * Fetch messages from server asynchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     *
     * @param maxMessageNum     max message num when server returns.
     * @param invisibleDuration set the invisibleDuration of messages return from server. These messages will be invisible to other consumer unless timout.
     * @return list of messageView
     */
    CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum, Duration invisibleDuration);

    /**
     * Ack message to server synchronously, server commit this message.
     *
     * <p> Duplicate ack request does not take effect and throw exception.
     *
     * @param messageView special messageView with handle want to ack.
     */
    void ack(MessageView messageView) throws ClientException;

    /**
     * Ack message to server asynchronously, server commit this message.
     *
     * <p> Duplicate ack request does not take effect and throw exception.
     *
     * @param messageView special messageView with handle want to ack.
     * @return CompletableFuture of this request.
     */
    CompletableFuture<Void> ackAsync(MessageView messageView);

    /**
     * Changes the invisible duration of a specified message synchronously.
     *
     * <p> The origin invisible duration for a message decide by ack request.
     *
     * <p>You must call change request before the origin invisible duration timeout.
     * If called change request later than the origin invisible duration, this request does not take effect and throw exception.
     * Duplicate change request will refresh the next visible time of this message to other consumers.
     *
     * @param messageView       special messageView with handle want to change.
     * @param invisibleDuration new timestamp the message could be visible and reconsume which start from current time.
     */
    void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration) throws ClientException;

    /**
     * Changes the invisible duration of a specified message asynchronously.
     *
     * <p> The origin invisible duration for a message decide by ack request.
     *
     * <p> You must call change request before the origin invisible duration timeout.
     * If called change request later than the origin invisible duration, this request does not take effect and throw exception.
     * Duplicate change request will refresh the next visible time of this message to other consumers.
     *
     * @param messageView       special messageView with handle want to change.
     * @param invisibleDuration new timestamp the message could be visible and reconsume which start from current time.
     * @return CompletableFuture of this request.
     */
    CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration);

    @Override
    void close() throws IOException;
}
