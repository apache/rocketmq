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

package org.apache.rocketmq.thinclient.impl.consumer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.apis.consumer.SimpleConsumerBuilder;
import org.apache.rocketmq.apis.ClientException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SimpleConsumerBuilderImpl implements SimpleConsumerBuilder {
    private static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile("^[%|a-zA-Z0-9._-]{1,255}$");

    private ClientConfiguration clientConfiguration = null;
    private String consumerGroup = null;
    private Map<String, FilterExpression> subscriptionExpressions = new ConcurrentHashMap<>();
    private Duration awaitDuration = null;

    /**
     * @see SimpleConsumerBuilder#setClientConfiguration(ClientConfiguration)
     */
    @Override
    public SimpleConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    /**
     * @see SimpleConsumerBuilder#setConsumerGroup(String)
     */
    @Override
    public SimpleConsumerBuilder setConsumerGroup(String consumerGroup) {
        checkNotNull(consumerGroup, "consumerGroup should not be null");
        checkArgument(CONSUMER_GROUP_PATTERN.matcher(consumerGroup).matches(), "consumerGroup does not match the regex [regex=%s]",
            CONSUMER_GROUP_PATTERN.pattern());
        this.consumerGroup = consumerGroup;
        return this;
    }

    @Override
    public SimpleConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions) {
        checkNotNull(subscriptionExpressions, "subscriptionExpressions should not be null");
        checkArgument(!subscriptionExpressions.isEmpty(), "subscriptionExpressions should not be empty");
        this.subscriptionExpressions = subscriptionExpressions;
        return this;
    }

    @Override
    public SimpleConsumerBuilder setAwaitDuration(Duration awaitDuration) {
        this.awaitDuration = checkNotNull(awaitDuration, "awaitDuration should not be null");
        return this;
    }

    @Override
    public SimpleConsumer build() throws ClientException {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        checkNotNull(consumerGroup, "consumerGroup has not been set yet");
        checkArgument(!subscriptionExpressions.isEmpty(), "subscriptionExpressions have not been set yet");
        checkNotNull(awaitDuration, "awaitDuration has not been set yet");
        final SimpleConsumerImpl consumer = new SimpleConsumerImpl(clientConfiguration, consumerGroup, awaitDuration, subscriptionExpressions);
        consumer.startAsync().awaitRunning();
        return consumer;
    }
}
