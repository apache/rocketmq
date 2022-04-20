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

import org.apache.rocketmq.apis.exception.ClientException;

import java.io.Closeable;
import java.util.Map;


/**
 * PushConsumer is a managed client which delivers messages to application through {@link MessageListener}.
 *
 * <p>Consumers of the same group are designed to share messages from broker servers. As a result, consumers of the same
 * group must have <strong>exactly identical subscription expressions</strong>, otherwise the behavior is undefined.
 *
 * <p>For a brand-new group, consumers consume messages from head of underlying queues, ignoring existing messages
 * completely. In addition to delivering messages to clients, broker servers also maintain progress in perspective of
 * group. Thus, consumers can safely restart and resume their progress automatically.</p>
 *
 * <p>There are scenarios where <a href="https://en.wikipedia.org/wiki/Fan-out_(software)">fan-out</a> is preferred,
 * recommended solution is to use dedicated group of each client.
 *
 * <p>To mitigate latency, PushConsumer adopts
 * <a href="https://en.wikipedia.org/wiki/Reactive_Streams">reactive streams</a> pattern. Namely,
 * messages received from broker servers are first cached locally, amount of which is controlled by
 * {@link PushConsumerBuilder#setMaxCacheMessageCount(int)} and
 * {@link PushConsumerBuilder#setMaxCacheMessageSizeInBytes(int)}, and then dispatched to thread pool to achieve
 * desirable concurrency.
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
     */
    @Override
    void close();
}
