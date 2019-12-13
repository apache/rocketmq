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

import io.openmessaging.api.batch.BatchConsumer;
import io.openmessaging.api.batch.BatchMessageListener;
import io.openmessaging.api.bean.Subscription;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;

/**
 * {@code BatchConsumerBean} Used to integrate {@link BatchConsumer} into Spring.
 */
public class BatchConsumerBean implements BatchConsumer {
    /**
     * Need to inject this field, specify the properties of the construct {@code BatchConsumer} instance, see the
     * specific supported properties{@link PropertyKeyConst}
     *
     * @see BatchConsumerBean#setProperties(Properties)
     */
    private Properties properties;

    /**
     * By injecting this field, complete the Topic subscription when starting {@code BatchConsumer}
     *
     * @see BatchConsumerBean#setSubscriptionTable(Map)
     */
    private Map<Subscription, BatchMessageListener> subscriptionTable;

    private BatchConsumer batchConsumer;

    @Override
    public boolean isStarted() {
        return this.batchConsumer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.batchConsumer.isClosed();
    }

    /**
     * Start the {@code BatchConsumer} instance, it is recommended to configure the init-method of the bean.
     */
    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        if (null == this.subscriptionTable) {
            throw new ONSClientException("subscriptionTable not set");
        }

        this.batchConsumer = ONSFactory.createBatchConsumer(this.properties);

        for (final Map.Entry<Subscription, BatchMessageListener> next : this.subscriptionTable.entrySet()) {
            this.subscribe(next.getKey().getTopic(), next.getKey().getExpression(), next.getValue());
        }

        this.batchConsumer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.batchConsumer != null) {
            this.batchConsumer.updateCredential(credentialProperties);
        }
    }

    /**
     * Close the {@code BatchConsumer} instance, it is recommended to configure the destroy-method of the bean.
     */
    @Override
    public void shutdown() {
        if (this.batchConsumer != null) {
            this.batchConsumer.shutdown();
        }
    }

    @Override
    public void subscribe(final String topic, final String subExpression, final BatchMessageListener listener) {
        if (null == this.batchConsumer) {
            throw new ONSClientException("subscribe must be called after BatchConsumerBean started");
        }
        this.batchConsumer.subscribe(topic, subExpression, listener);
    }

    @Override
    public void unsubscribe(final String topic) {
        if (null == this.batchConsumer) {
            throw new ONSClientException("unsubscribe must be called after BatchConsumerBean started");
        }
        this.batchConsumer.unsubscribe(topic);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public Map<Subscription, BatchMessageListener> getSubscriptionTable() {
        return subscriptionTable;
    }

    public void setSubscriptionTable(
        final Map<Subscription, BatchMessageListener> subscriptionTable) {
        this.subscriptionTable = subscriptionTable;
    }
}
