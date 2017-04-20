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
import io.openmessaging.exception.OMSNotSupportedException;
import io.openmessaging.observer.Observer;
import io.openmessaging.rocketmq.consumer.PullConsumerImpl;
import io.openmessaging.rocketmq.consumer.PushConsumerImpl;
import io.openmessaging.rocketmq.producer.ProducerImpl;
import io.openmessaging.rocketmq.producer.SequenceProducerImpl;
import io.openmessaging.rocketmq.utils.OMSUtil;

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
        return new PushConsumerImpl(accessPointProperties);
    }

    @Override
    public PushConsumer createPushConsumer(KeyValue properties) {
        return new PushConsumerImpl(OMSUtil.buildKeyValue(this.accessPointProperties, properties));
    }

    @Override
    public PullConsumer createPullConsumer(String queueName) {
        return new PullConsumerImpl(queueName, accessPointProperties);
    }

    @Override
    public PullConsumer createPullConsumer(String queueName, KeyValue properties) {
        return new PullConsumerImpl(queueName, OMSUtil.buildKeyValue(this.accessPointProperties, properties));
    }

    @Override
    public IterableConsumer createIterableConsumer(String queueName) {
        throw new OMSNotSupportedException("-1", "IterableConsumer is not supported in current version");
    }

    @Override
    public IterableConsumer createIterableConsumer(String queueName, KeyValue properties) {
        throw new OMSNotSupportedException("-1", "IterableConsumer is not supported in current version");
    }

    @Override
    public ResourceManager getResourceManager() {
        throw new OMSNotSupportedException("-1", "ResourceManager is not supported in current version.");
    }

    @Override
    public ServiceEndPoint createServiceEndPoint() {
        throw new OMSNotSupportedException("-1", "ServiceEndPoint is not supported in current version.");
    }

    @Override
    public ServiceEndPoint createServiceEndPoint(KeyValue properties) {
        throw new OMSNotSupportedException("-1", "ServiceEndPoint is not supported in current version.");
    }

    @Override
    public void addObserver(Observer observer) {
        //Ignore
    }

    @Override
    public void deleteObserver(Observer observer) {
        //Ignore
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
