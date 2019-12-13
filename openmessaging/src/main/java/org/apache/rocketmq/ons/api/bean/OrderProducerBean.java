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

import io.openmessaging.api.Message;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.order.OrderProducer;
import java.util.Properties;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;

/**
 * {@code OrderProducerBean} is used to integrate {@link OrderProducer} into Spring Bean.
 */
public class OrderProducerBean implements OrderProducer {
    /**
     * Need to inject this field, specify the properties of the construct {@code OrderProducer} instance, the specific
     * supported properties are detailed in {@link PropertyKeyConst}
     *
     * @see OrderProducerBean#setProperties(Properties)
     */
    private Properties properties;

    private OrderProducer orderProducer;

    /**
     * Start the {@code OrderProducer} instance, it is recommended to configure the init-method of the bean.
     */
    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        this.orderProducer = ONSFactory.createOrderProducer(this.properties);
        this.orderProducer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.orderProducer != null) {
            this.orderProducer.updateCredential(credentialProperties);
        }
    }

    /**
     * Close the {@code OrderProducer} instance, it is recommended to configure the destroy-method of the bean.
     */
    @Override
    public void shutdown() {
        if (this.orderProducer != null) {
            this.orderProducer.shutdown();
        }
    }

    @Override
    public boolean isStarted() {
        return this.orderProducer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.orderProducer.isClosed();
    }

    @Override
    public SendResult send(final Message message, final String shardingKey) {
        return this.orderProducer.send(message, shardingKey);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }
}
