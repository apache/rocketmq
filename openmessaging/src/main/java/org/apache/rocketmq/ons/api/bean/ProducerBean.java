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
import io.openmessaging.api.Producer;
import io.openmessaging.api.SendCallback;
import io.openmessaging.api.SendResult;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.exception.ONSClientException;

/**
 * {@code ProducerBean} for integrating {@link Producer} into Spring Bean
 */
public class ProducerBean implements Producer {
    /**
     * You need to inject this field to specify the properties of the {@code Producer} instance. For details, see {@link
     * PropertyKeyConst}
     *
     * @see ProducerBean#setProperties(Properties)
     */
    private Properties properties;
    private Producer producer;

    /**
     * Start the {@code Producer} instance, it is recommended to configure the init-method of the bean.
     */
    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        this.producer = ONSFactory.createProducer(this.properties);
        this.producer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.producer != null) {
            this.producer.updateCredential(credentialProperties);
        }
    }

    /**
     * Close the {@code Producer} instance, it is recommended to configure the destroy-method of the bean
     */
    @Override
    public void shutdown() {
        if (this.producer != null) {
            this.producer.shutdown();
        }
    }

    @Override
    public SendResult send(Message message) {
        return this.producer.send(message);
    }

    @Override
    public void sendOneway(Message message) {
        this.producer.sendOneway(message);
    }

    @Override
    public void sendAsync(Message message, SendCallback sendCallback) {
        this.producer.sendAsync(message, sendCallback);
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.producer.setCallbackExecutor(callbackExecutor);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public boolean isStarted() {
        return this.producer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.producer.isClosed();
    }
}
