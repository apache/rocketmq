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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.apis.ClientException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link PushConsumerBuilder}
 */
public class PushConsumerBuilderImpl implements PushConsumerBuilder {
    private static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile("^[%|a-zA-Z0-9._-]{1,255}$");

    private ClientConfiguration clientConfiguration = null;
    private String consumerGroup = null;
    private Map<String, FilterExpression> subscriptionExpressions = new ConcurrentHashMap<>();
    private MessageListener messageListener = null;
    private int maxCacheMessageCount = 5000;
    private int maxCacheMessageSizeInBytes = 500 * 1024 * 1024;
    private int consumptionThreadCount = 20;

    /**
     * @see PushConsumerBuilder#setClientConfiguration(ClientConfiguration)
     */
    @Override
    public PushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    /**
     * @see PushConsumerBuilder#setConsumerGroup(String)
     */
    @Override
    public PushConsumerBuilder setConsumerGroup(String consumerGroup) {
        checkNotNull(consumerGroup, "consumerGroup should not be null");
        checkArgument(CONSUMER_GROUP_PATTERN.matcher(consumerGroup).matches(), "consumerGroup does not match the regex [regex=%s]", CONSUMER_GROUP_PATTERN.pattern());
        this.consumerGroup = consumerGroup;
        return this;
    }

    /**
     * @see PushConsumerBuilder#setSubscriptionExpressions(Map)
     */
    @Override
    public PushConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions) {
        checkNotNull(subscriptionExpressions, "subscriptionExpressions should not be null");
        checkArgument(!subscriptionExpressions.isEmpty(), "subscriptionExpressions should not be empty");
        this.subscriptionExpressions = subscriptionExpressions;
        return this;
    }

    /**
     * @see PushConsumerBuilder#setMessageListener(MessageListener)
     */
    @Override
    public PushConsumerBuilder setMessageListener(MessageListener messageListener) {
        this.messageListener = checkNotNull(messageListener, "messageListener should not be null");
        return this;
    }

    /**
     * @see PushConsumerBuilder#setMaxCacheMessageCount(int)
     */
    @Override
    public PushConsumerBuilder setMaxCacheMessageCount(int maxCachedMessageCount) {
        checkArgument(maxCachedMessageCount > 0, "maxCachedMessageCount should be positive");
        this.maxCacheMessageCount = maxCachedMessageCount;
        return this;
    }

    /**
     * @see PushConsumerBuilder#setMaxCacheMessageSizeInBytes(int)
     */
    @Override
    public PushConsumerBuilder setMaxCacheMessageSizeInBytes(int maxCacheMessageSizeInBytes) {
        checkArgument(maxCacheMessageSizeInBytes > 0, "maxCacheMessageSizeInBytes should be positive");
        this.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;
        return this;
    }

    /**
     * @see PushConsumerBuilder#setConsumptionThreadCount(int)
     */
    @Override
    public PushConsumerBuilder setConsumptionThreadCount(int consumptionThreadCount) {
        checkArgument(consumptionThreadCount > 0, "consumptionThreadCount should be positive");
        this.consumptionThreadCount = consumptionThreadCount;
        return this;
    }

    /**
     * @see PushConsumerBuilder#build()
     */
    @Override
    public PushConsumer build() throws ClientException {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        checkNotNull(consumerGroup, "consumerGroup has not been set yet");
        checkNotNull(messageListener, "messageListener has not been set yet");
        checkArgument(!subscriptionExpressions.isEmpty(), "subscriptionExpressions have not been set yet");
        final PushConsumerImpl pushConsumer = new PushConsumerImpl(clientConfiguration, consumerGroup,
            subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes,
            consumptionThreadCount);
        pushConsumer.startAsync().awaitRunning();
        return pushConsumer;
    }
}
