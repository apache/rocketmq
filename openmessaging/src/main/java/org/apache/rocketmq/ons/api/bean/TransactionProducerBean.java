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
import io.openmessaging.api.transaction.LocalTransactionChecker;
import io.openmessaging.api.transaction.LocalTransactionExecuter;
import io.openmessaging.api.transaction.TransactionProducer;
import java.util.Properties;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;

/**
 * {@code TransactionProducerBean} is used to integrate {@link TransactionProducer} into Spring Bean
 */
public class TransactionProducerBean implements TransactionProducer {
    /**
     * Need to inject this field, specify the properties of the construct {@code TransactionProducer} instance, the
     * specific supported properties are detailed in {@link PropertyKeyConst}
     *
     * @see TransactionProducerBean#setProperties(Properties)
     */
    private Properties properties;

    /**
     * Need to inject this field, {@code TransactionProducer} will send the transaction message will rely on the object
     * for transaction status checkback
     *
     * @see TransactionProducerBean#setLocalTransactionChecker(LocalTransactionChecker)
     */
    private LocalTransactionChecker localTransactionChecker;

    private TransactionProducer transactionProducer;

    /**
     *
     */
    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        this.transactionProducer = ONSFactory.createTransactionProducer(properties, localTransactionChecker);
        this.transactionProducer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.transactionProducer != null) {
            this.transactionProducer.updateCredential(credentialProperties);
        }
    }

    /**
     * Close the {@code TransactionProducer} instance, it is recommended to configure the destroy-method of the bean.
     */
    @Override
    public void shutdown() {
        if (this.transactionProducer != null) {
            this.transactionProducer.shutdown();
        }
    }

    @Override
    public SendResult send(Message message, LocalTransactionExecuter executer, Object arg) {
        return this.transactionProducer.send(message, executer, arg);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public LocalTransactionChecker getLocalTransactionChecker() {
        return localTransactionChecker;
    }

    public void setLocalTransactionChecker(LocalTransactionChecker localTransactionChecker) {
        this.localTransactionChecker = localTransactionChecker;
    }

    @Override
    public boolean isStarted() {
        return this.transactionProducer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.transactionProducer.isClosed();
    }
}
