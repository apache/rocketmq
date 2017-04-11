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

import io.openmessaging.IterableConsumer;
import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import io.openmessaging.PushConsumer;
import io.openmessaging.ResourceManager;
import io.openmessaging.SequenceProducer;
import io.openmessaging.ServiceEndPoint;
import io.openmessaging.observer.Observer;
import io.openmessaging.rocketmq.producer.ProducerImpl;
import io.openmessaging.rocketmq.producer.SequenceProducerImpl;

public class MessagingAccessPointImpl implements MessagingAccessPoint {
    private final KeyValue accessPointProperties;

    public MessagingAccessPointImpl(final KeyValue accessPointProperties) {
        this.accessPointProperties = accessPointProperties;
    }

    @Override
    public KeyValue properties() {
        return accessPointProperties;
    }

    @Override
    public Producer createProducer() {
        return new ProducerImpl(this.accessPointProperties);
    }

    @Override
    public Producer createProducer(KeyValue properties) {
        return new ProducerImpl(OMSUtil.buildKeyValue(this.accessPointProperties, properties));
    }

    @Override
    public SequenceProducer createSequenceProducer() {
        return new SequenceProducerImpl(this.accessPointProperties);
    }

    @Override
    public SequenceProducer createSequenceProducer(KeyValue properties) {
        return new SequenceProducerImpl(OMSUtil.buildKeyValue(this.accessPointProperties, properties));
    }

    @Override
    public PushConsumer createPushConsumer() {
        return null;
    }

    @Override
    public PushConsumer createPushConsumer(KeyValue properties) {
        return null;
    }

    @Override
    public PullConsumer createPullConsumer(String queueName) {
        return null;
    }

    @Override
    public PullConsumer createPullConsumer(String queueName, KeyValue properties) {
        return null;
    }

    @Override
    public IterableConsumer createIterableConsumer(String queueName) {
        return null;
    }

    @Override
    public IterableConsumer createIterableConsumer(String queueName, KeyValue properties) {
        return null;
    }

    @Override
    public ResourceManager getResourceManager() {
        return null;
    }

    @Override
    public ServiceEndPoint createServiceEndPoint() {
        return null;
    }

    @Override
    public ServiceEndPoint createServiceEndPoint(KeyValue properties) {
        return null;
    }

    @Override
    public void addObserver(Observer observer) {

    }

    @Override
    public void deleteObserver(Observer observer) {

    }

    @Override
    public void startup() {
        //Ignore
    }

    @Override
    public void shutdown() {
        //Ignore
    }
}
