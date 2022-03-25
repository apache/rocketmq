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
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.apis.MessageQueue;
import org.apache.rocketmq.apis.exception.*;
import org.apache.rocketmq.apis.message.MessageView;

/**
 * <p>PullConsumer is a thread-safe rocketmq client which is used to consume message by queue.
 * Unlike push consumer and simple consumer, pull consumer implement load balance based on queue granularity.
 *
 * <p>Pull consumer is lightweight consumer that better suited to streaming scenarios.
 * If you want fully control the message consumption operation by yourself like scan by offset or reconsume repeatedly,
 * pull consumer should be your first consideration.
 *
 * <p>Pull consumer support two load balance mode. First is subscription mode, which full manage the rebalance
 * operation triggered when group membership or cluster and topic metadata change.Another mode is manual assignment mode,which manage the load balance by yourself.
 *
 * <p> Pull consumer divide message consumption to 3 parts.
 * Firstly, determine whether to continue processing from the last consumption or reset the consumption starting point by call seek method;
 * Then, pull message from servers.
 * At last, pull consumer no need to commit message by offset meta.
 * <p> If there is a consumption error, consumer just call seek api to reset the offset for reconsume message again.
 */
public interface PullConsumer extends Closeable {
    /**
     * Get metadata about the message queues for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic message's topic
     * @return message queues of topic.
     */
    Collection<MessageQueue> topicMessageQueues(String topic) throws ClientException;

    /**
     * Manually assign messageQueue collections to this consumer.
     * <p> This interface does not allow for incremental assignment and will replace the previous assignment.
     * If the given collection is empty, it's treated same as unsubscribe().
     * Manual assignment through this interface will disable the consumerGroup management functionality
     * and there will be no rebalance operation triggered when group membership or cluster and topic metadata change.
     * @param messageQueues are the collection for current consumer.
     * @throws ClientException when assign
     */
    void assign(Collection<MessageQueue> messageQueues) throws ClientException;

    /**
     * Add subscription expression dynamically when use subscription mode.
     *
     * <p>If first {@link SubscriptionExpression} that contains topicA and tag1 is exists already in consumer, then
     * second {@link SubscriptionExpression} which contains topicA and tag2, <strong>the result is that the second one
     * replaces the first one instead of integrating them</strong>.
     *
     * @param subscriptionExpression new subscription expression to add.
     * @return pull consumer instance.
     */
    PullConsumer subscribe(SubscriptionExpression subscriptionExpression) throws ClientException;

    /**
     * Remove subscription expression dynamically by topic.
     *
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of pull consumer.
     *
     * @param topic the topic to remove subscription.
     * @return pull consumer instance.
     */
    PullConsumer unsubscribe(String topic) throws ClientException;

    /**
     * Get the collection of messageQueues currently assigned to current consumer.
     * @return the collection of messageQueues currently assigned to current consumer
     */
    Collection<MessageQueue> assignments();

    /**
     * Pull messages for the topics specified by subscribe or assign api synchronously.
     *
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     * An error occurs if you do not subscribe or assign messageQueues before pulling for data.
     * @param maxMessageNum max message num when server returns.
     * @return map of messageQueue to messageViews.
     */
    Map<MessageQueue, Collection<MessageView>> pull(int maxMessageNum) throws ClientException;

    /**
     * Pull messages for the topics specified by subscribe or assign api asynchronously.
     *
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     * An error occurs if you do not subscribe or assign messageQueues before pulling for data.
     * @param maxMessageNum max message num when server returns.
     * @return future of messageViews.
     */
    CompletableFuture<Map<MessageQueue, Collection<MessageView>>> pullAsync(int maxMessageNum) throws ClientException;

    /**
     * Commit offsets synchronously which returned by the last pull api for all the subscribed topic and messageQueues.
     *
     * <p> This offsets meta maintained by kafka, which used for rebalance processing or next startup.
     * If you store the offsets in your own storage, you may need call seek api before pull and no need to call commit.
     */
    void commit() throws ClientException;

    /**
     * Commit offsets asynchronously which returned by the last pull api for all the subscribed topic and messageQueues.
     *
     * <p> This offsets meta maintained by kafka, which used for rebalance processing or next startup.
     * If you store the offsets in your own storage, you may need call seek api before pull and no need to call commit.
     */
    CompletableFuture<Void> commitAsync() throws ClientException;

    /**
     * Commit specified offset for the specified messageQueue synchronously.
     * @param messageQueue the specified messageQueue to commit offset
     * @param committedOffset the specified offset commit to server
     */
    void commit(MessageQueue messageQueue, long committedOffset) throws ClientException;

    /**
     * Commit specified offset for the specified messageQueue asynchronously.
     * @param messageQueue the specified messageQueue to commit offset
     * @param committedOffset the specified offset commit to server
     */
    CompletableFuture<Void> commitAsync(MessageQueue messageQueue, long committedOffset) throws ClientException;

    /**
     * Overrides the fetch offsets that the consumer will use on the next pull operation.
     * Seek operation only apply to local consumption states.
     *
     * @param messageQueue the message queues to seek for.
     * @param offset       offset of message queues.
     */
    void seek(MessageQueue messageQueue, long offset) throws ClientException;

    /**
     * Seek to the first offset for each of the give message queues.
     * Seek operation only apply to local consumption states.
     *
     * @param messageQueue the message queues to seek for.
     */
    void seekToBeginning(MessageQueue messageQueue) throws ClientException;

    /**
     * Seek to the last offset for each of the give message queues.
     * Seek operation only apply to local consumption states.
     *
     * @param messageQueue the message queue to seek for.
     */
    void seekToEnd(MessageQueue messageQueue) throws ClientException;

    /**
     * Seek message queue by timestamp.
     * Seek operation only apply to local consumption states.
     *
     * @param messageQueue the message queue to seek for.
     * @param timestamp
     * @return
     */
    long seekToTimestamp(MessageQueue messageQueue, long timestamp) throws ClientException;

    @Override
    void close();
}
