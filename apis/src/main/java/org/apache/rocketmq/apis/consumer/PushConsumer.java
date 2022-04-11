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

import com.google.common.util.concurrent.Service;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

import org.apache.rocketmq.apis.exception.*;

/**
 * PushConsumer is a thread-safe rocketmq client which is used to consume message by group.
 *
 * <p>Push consumer is fully-managed consumer, if you are confused to choose your consumer, push consumer should be
 * your first consideration.
 *
 * <p>Consumers belong to the same consumer group share messages from server,
 * so consumer in the same group must have the same subscriptionExpressions, otherwise the behavior is
 * undefined. If a new consumer group's consumer is started first time, it consumes from the latest position. Once
 * consumer is started, server records its consumption progress and derives it in subsequent startup.
 *
 * <p>You may intend to maintain different consumption progress for different consumer, different consumer group
 * should be set in this case.
 *
 * <p>To accelerate the message consumption, push consumer applies
 * <a href="https://en.wikipedia.org/wiki/Reactive_Streams">reactive streams</a>
 * . Messages received from server is cached locally before consumption,
 * {@link PushConsumerBuilder#setMaxCacheMessageCount(int)} and
 * {@link PushConsumerBuilder#setMaxCacheMessageSizeInBytes(int)} could be used to set the cache threshold in
 * different dimension.
 */
public interface PushConsumer extends Closeable {
    /**
     * Get the load balancing group for consumer.
     *
     * @return consumer load balancing group.
     */
    String getConsumerGroup();

    /**
     * List the existed subscription expressions in push consumer.
     *
     * @return map of topic to filter expression.
     */
    Map<String, FilterExpression> subscriptionExpressions();

    /**
     * Add subscription expression dynamically.
     *
     * <p>If first subscriptionExpression that contains topicA and tag1 is exists already in consumer, then
     * second subscriptionExpression which contains topicA and tag2, <strong>the result is that the second one
     * replaces the first one instead of integrating them</strong>.
     *
     * @param topic  new topic that need to add or update.
     * @param filterExpression new filter expression to add or update.
     * @return push consumer instance.
     */
    PushConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException;

    /**
     * Remove subscription expression dynamically by topic.
     *
     * <p>It stops the backend task to fetch message from remote, and besides that, the local cached message whose topic
     * was removed before would not be delivered to {@link MessageListener} anymore.
     *
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of push consumer.
     *
     * @param topic the topic to remove subscription.
     * @return push consumer instance.
     */
    PushConsumer unsubscribe(String topic) throws ClientException;

    /**
     * Close the push consumer and release all related resources.
     *
     * <p>Once push consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each producer, which is similar to
     * {@link Service.State}.
     */
    @Override
    void close();
}
