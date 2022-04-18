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

import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;

import java.util.Map;

public interface PushConsumerBuilder {
    /**
     * Set the client configuration for consumer.
     *
     * @param clientConfiguration client's configuration.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Set the load balancing group for consumer.
     *
     * @param consumerGroup consumer load balancing group.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setConsumerGroup(String consumerGroup);

    /**
     * Add subscriptionExpressions for consumer.
     *
     * @param subscriptionExpressions subscriptions to add which use the map of topic to filterExpression.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions);

    /**
     * Register message listener, all messages meet the subscription expression would across listener here.
     *
     * @param listener message listener.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setMessageListener(MessageListener listener);

    /**
     * Set the maximum number of messages cached locally.
     *
     * @param count message count.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setMaxCacheMessageCount(int count);

    /**
     * Set the maximum bytes of messages cached locally.
     *
     * @param bytes message size.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setMaxCacheMessageSizeInBytes(int bytes);

    /**
     * Set the consumption thread count in parallel.
     *
     * @param count thread count.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setThreadCount(int count);

    /**
     * Finalize the build of {@link PushConsumer}.
     *
     * @return the push consumer instance.
     */
    PushConsumer build() throws ClientException;
}
