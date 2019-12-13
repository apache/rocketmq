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

import io.openmessaging.api.Consumer;
import io.openmessaging.api.ExpressionType;
import io.openmessaging.api.MessageListener;
import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.bean.Subscription;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.exception.ONSClientException;

/**
 * {@code ConsumerBean} is used to integrate {@link Consumer} into Spring Bean
 */
public class ConsumerBean implements Consumer {
    /**
     * You need to inject this field to specify the properties of the {@code Consumer} instance. For details, see {@link
     * PropertyKeyConst}.
     *
     * @see ConsumerBean#setProperties(Properties)
     */
    private Properties properties;

    /**
     * By injecting this field, complete the Topic subscription when launching {@code Consumer}
     *
     * @see ConsumerBean#setSubscriptionTable(Map)
     */
    private Map<Subscription, MessageListener> subscriptionTable;

    private Consumer consumer;

    /**
     * Start the {@code Consumer} instance, it is recommended to configure the init-method of the bean.
     */
    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        if (null == this.subscriptionTable) {
            throw new ONSClientException("subscriptionTable not set");
        }

        this.consumer = ONSFactory.createConsumer(this.properties);

        Iterator<Entry<Subscription, MessageListener>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Subscription, MessageListener> next = it.next();

            Subscription subscription = next.getKey();
            if (subscription.getType() == null || ExpressionType.TAG.name().equals(subscription.getType())) {

                this.subscribe(subscription.getTopic(), subscription.getExpression(), next.getValue());

            } else if (ExpressionType.SQL92.name().equals(subscription.getType())) {

                this.subscribe(subscription.getTopic(), MessageSelector.bySql(subscription.getExpression()), next.getValue());
            } else {

                throw new ONSClientException(String.format("Expression type %s is unknown!", subscription.getType()));
            }
        }

        this.consumer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.consumer != null) {
            this.consumer.updateCredential(credentialProperties);
        }
    }

    /**
     * Close the {@code Consumer} instance, it is recommended to configure the destroy-method of the bean.
     */
    @Override
    public void shutdown() {
        if (this.consumer != null) {
            this.consumer.shutdown();
        }
    }

    @Override
    public void subscribe(String topic, String subExpression, MessageListener listener) {
        if (null == this.consumer) {
            throw new ONSClientException("subscribe must be called after consumerBean started");
        }
        this.consumer.subscribe(topic, subExpression, listener);
    }

    @Override
    public void subscribe(final String topic, final MessageSelector selector, final MessageListener listener) {
        if (null == this.consumer) {
            throw new ONSClientException("subscribe must be called after consumerBean started");
        }
        this.consumer.subscribe(topic, selector, listener);
    }

    @Override
    public void unsubscribe(String topic) {
        if (null == this.consumer) {
            throw new ONSClientException("unsubscribe must be called after consumerBean started");
        }
        this.consumer.unsubscribe(topic);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Map<Subscription, MessageListener> getSubscriptionTable() {
        return subscriptionTable;
    }

    public void setSubscriptionTable(Map<Subscription, MessageListener> subscriptionTable) {
        this.subscriptionTable = subscriptionTable;
    }

    @Override
    public boolean isStarted() {
        return this.consumer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.consumer.isClosed();
    }
}
