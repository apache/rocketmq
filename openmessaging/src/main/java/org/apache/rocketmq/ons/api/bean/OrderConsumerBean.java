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
package org.apache.rocketmq.ons.api.bean;

import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.bean.Subscription;
import io.openmessaging.api.order.MessageOrderListener;
import io.openmessaging.api.order.OrderConsumer;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;

/**
 * {@code OrderConsumerBean} is used to integrate {@link OrderConsumer} into Spring Bean
 */
public class OrderConsumerBean implements OrderConsumer {
    /**
     * Need to inject this field, specify the properties of the construct {@code OrderConsumer} instance, the specific
     * supported properties are detailed in {@link PropertyKeyConst}
     *
     * @see OrderConsumerBean#setProperties(Properties)
     */
    private Properties properties;

    /**
     * By injecting this field, complete the subscription for Topic when launching {@code OrderConsumer}
     *
     * @see OrderConsumerBean#setSubscriptionTable(Map)
     */
    private Map<Subscription, MessageOrderListener> subscriptionTable;

    private OrderConsumer orderConsumer;

    @Override
    public boolean isStarted() {
        return this.orderConsumer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.orderConsumer.isClosed();
    }

    /**
     * Start the {@code OrderConsumer} instance, it is recommended to configure the init-method of the bean.
     */
    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        if (null == this.subscriptionTable) {
            throw new ONSClientException("subscriptionTable not set");
        }

        this.orderConsumer = ONSFactory.createOrderedConsumer(this.properties);

        for (final Map.Entry<Subscription, MessageOrderListener> next : this.subscriptionTable.entrySet()) {
            this.subscribe(next.getKey().getTopic(), next.getKey().getExpression(), next.getValue());
        }

        this.orderConsumer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.orderConsumer != null) {
            this.orderConsumer.updateCredential(credentialProperties);
        }
    }

    /**
     * Close the {@code OrderConsumer} instance, it is recommended to configure the destroy-method of the bean.
     */
    @Override
    public void shutdown() {
        if (this.orderConsumer != null) {
            this.orderConsumer.shutdown();
        }
    }

    @Override
    public void subscribe(final String topic, final String subExpression, final MessageOrderListener listener) {
        if (null == this.orderConsumer) {
            throw new ONSClientException("subscribe must be called after OrderConsumerBean started");
        }
        this.orderConsumer.subscribe(topic, subExpression, listener);
    }

    @Override
    public void subscribe(String topic, MessageSelector selector, MessageOrderListener listener) {
        if (null == this.orderConsumer) {
            throw new ONSClientException("subscribe must be called after OrderConsumerBean started");
        }
        this.orderConsumer.subscribe(topic, selector, listener);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public Map<Subscription, MessageOrderListener> getSubscriptionTable() {
        return subscriptionTable;
    }

    public void setSubscriptionTable(
        final Map<Subscription, MessageOrderListener> subscriptionTable) {
        this.subscriptionTable = subscriptionTable;
    }
}
