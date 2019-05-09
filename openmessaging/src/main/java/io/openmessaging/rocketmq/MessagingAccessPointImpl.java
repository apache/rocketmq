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
package io.openmessaging.rocketmq;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.consumer.Consumer;
import io.openmessaging.exception.OMSUnsupportException;
import io.openmessaging.manager.ResourceManager;
import io.openmessaging.message.MessageFactory;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.TransactionStateCheckListener;
import io.openmessaging.rocketmq.producer.ProducerImpl;

public class MessagingAccessPointImpl implements MessagingAccessPoint {

    private final KeyValue accessPointProperties;

    public MessagingAccessPointImpl(final KeyValue accessPointProperties) {
        this.accessPointProperties = accessPointProperties;
    }

    @Override
    public KeyValue attributes() {
        return accessPointProperties;
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public Producer createProducer() {
        return new ProducerImpl(this.accessPointProperties);
    }

    @Override public Producer createProducer(TransactionStateCheckListener transactionStateCheckListener) {
        return null;
    }

    @Override public Consumer createConsumer() {
        return null;
    }

    @Override
    public ResourceManager resourceManager() {
        throw new OMSUnsupportException(-1, "ResourceManager is not supported in current version.");
    }

    @Override public MessageFactory messageFactory() {
        return null;
    }
}
