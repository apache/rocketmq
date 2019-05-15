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
import io.openmessaging.manager.ResourceManager;
import io.openmessaging.message.MessageFactory;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.TransactionStateCheckListener;
import io.openmessaging.rocketmq.consumer.PushConsumerImpl;
import io.openmessaging.rocketmq.producer.ProducerImpl;

import java.util.HashSet;
import java.util.Set;

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
        return new PushConsumerImpl(accessPointProperties);
    }

    @Override
    public ResourceManager resourceManager() {
       return new ResourceManager() {

            @Override
            public void createNamespace(String nsName) {
                accessPointProperties.put("CONSUMER_ID", nsName);
            }

            @Override
            public void deleteNamespace(String nsName) {
                accessPointProperties.put("CONSUMER_ID", null);
            }

            @Override
            public void switchNamespace(String targetNamespace) {
                accessPointProperties.put("CONSUMER_ID", targetNamespace);
            }

            @Override
            public Set<String> listNamespaces() {
                return new HashSet<String>() {
                    {
                        add(accessPointProperties.getString("CONSUMER_ID"));
                    }
                };
            }

            @Override
            public void createQueue(String queueName) {

            }

            @Override
            public void deleteQueue(String queueName) {

            }

            @Override
            public Set<String> listQueues(String nsName) {
                return null;
            }

            @Override
            public void filter(String queueName, String filterString) {

            }

            @Override
            public void routing(String sourceQueue, String targetQueue) {

            }
        };
    }

    @Override public MessageFactory messageFactory() {
        return null;
    }
}
