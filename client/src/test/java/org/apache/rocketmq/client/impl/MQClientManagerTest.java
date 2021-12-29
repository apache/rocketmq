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
package org.apache.rocketmq.client.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MQClientManagerTest {

    private ConcurrentMap<String, MQClientInstance> factoryTable;

    @Before
    public void init() throws Exception {
        Field field = MQClientManager.getInstance().getClass().getDeclaredField("factoryTable");
        field.setAccessible(true);
        this.factoryTable = (ConcurrentMap<String, MQClientInstance>) field.get(MQClientManager.getInstance());
        this.factoryTable.clear();

        field = MQClientManager.getInstance().getClass().getDeclaredField("producerCodeTable");
        field.setAccessible(true);
        ConcurrentMap<String, AtomicInteger> producerCodeTable = (ConcurrentMap<String, AtomicInteger>) field.get(MQClientManager.getInstance());
        producerCodeTable.clear();

        field = MQClientManager.getInstance().getClass().getDeclaredField("consumerCodeTable");
        field.setAccessible(true);
        ConcurrentMap<String, AtomicInteger> consumerCodeTable = (ConcurrentMap<String, AtomicInteger>) field.get(MQClientManager.getInstance());
        consumerCodeTable.clear();

        field = MQClientManager.getInstance().getClass().getDeclaredField("adminCodeTable");
        field.setAccessible(true);
        ConcurrentMap<String, AtomicInteger> adminCodeTable = (ConcurrentMap<String, AtomicInteger>) field.get(MQClientManager.getInstance());
        adminCodeTable.clear();
    }

    @Test
    public void repeatInstanceNameTest() throws Exception {
        Assert.assertEquals(0, factoryTable.size());

        DefaultMQProducer producerA1 = new DefaultMQProducer("groupA");
        producerA1.setInstanceName("instanceTest");
        MQClientInstance instanceA1 = MQClientManager.getInstance().getOrCreateMQClientInstance(producerA1);
        Assert.assertEquals(1, factoryTable.size());

        DefaultMQProducer producerA2 = new DefaultMQProducer("groupB");
        producerA2.setInstanceName("instanceTest");
        Assert.assertEquals(instanceA1, MQClientManager.getInstance().getOrCreateMQClientInstance(producerA2));
        Assert.assertEquals(1, factoryTable.size());

        DefaultMQPushConsumer consumerA1 = new DefaultMQPushConsumer("groupA");
        consumerA1.setInstanceName("instanceTest");
        Assert.assertEquals(instanceA1, MQClientManager.getInstance().getOrCreateMQClientInstance(consumerA1));

        DefaultMQPushConsumer consumerA2 = new DefaultMQPushConsumer("groupB");
        consumerA2.setInstanceName("instanceTest");
        Assert.assertEquals(instanceA1, MQClientManager.getInstance().getOrCreateMQClientInstance(consumerA2));

        Assert.assertEquals(1, factoryTable.size());
    }

    @Test
    public void repeatGroupTest() throws Exception {
        Assert.assertEquals(0, factoryTable.size());

        DefaultMQProducer producerA1 = new DefaultMQProducer("groupA1");
        producerA1.setInstanceName("instanceTest");
        MQClientInstance instanceA1 = MQClientManager.getInstance().getOrCreateMQClientInstance(producerA1);

        DefaultMQProducer producerA2 = new DefaultMQProducer("groupA1");
        producerA2.setInstanceName("instanceTest");
        MQClientInstance instanceA2 = MQClientManager.getInstance().getOrCreateMQClientInstance(producerA2);

        Assert.assertNotEquals(instanceA1, instanceA2);
        Assert.assertEquals(2, factoryTable.size());

        DefaultMQPushConsumer consumerA1 = new DefaultMQPushConsumer("groupA1");
        consumerA1.setInstanceName("instanceTest");

        DefaultMQPushConsumer consumerA2 = new DefaultMQPushConsumer("groupA1");
        consumerA2.setInstanceName("instanceTest");

        Assert.assertEquals(instanceA1, MQClientManager.getInstance().getOrCreateMQClientInstance(consumerA1));
        Assert.assertEquals(instanceA2, MQClientManager.getInstance().getOrCreateMQClientInstance(consumerA2));
        Assert.assertEquals(2, factoryTable.size());
    }

    @Test
    public void multipleNamesrvTest() throws Exception {
        Assert.assertEquals(0, factoryTable.size());

        DefaultMQProducer producerA1 = new DefaultMQProducer("groupA");
        producerA1.setInstanceName("instanceTest");
        producerA1.setNamesrvAddr("127.0.0.1:9876");
        MQClientInstance instanceA1 = MQClientManager.getInstance().getOrCreateMQClientInstance(producerA1);

        DefaultMQProducer producerA2 = new DefaultMQProducer("groupA");
        producerA2.setInstanceName("instanceTest");
        producerA2.setNamesrvAddr("127.0.0.2:9876");
        MQClientInstance instanceA2 = MQClientManager.getInstance().getOrCreateMQClientInstance(producerA2);
        Assert.assertNotEquals(instanceA1, instanceA2);
        Assert.assertEquals(2, factoryTable.size());

        DefaultMQPushConsumer consumerA1 = new DefaultMQPushConsumer("groupA");
        consumerA1.setInstanceName("instanceTest");
        consumerA1.setNamesrvAddr("127.0.0.1:9876");
        Assert.assertEquals(instanceA1, MQClientManager.getInstance().getOrCreateMQClientInstance(consumerA1));
        Assert.assertEquals(2, factoryTable.size());

        DefaultMQPushConsumer consumerA2 = new DefaultMQPushConsumer("groupA");
        consumerA2.setInstanceName("instanceTest");
        consumerA2.setNamesrvAddr("127.0.0.2:9876");
        Assert.assertEquals(instanceA2, MQClientManager.getInstance().getOrCreateMQClientInstance(consumerA2));
        Assert.assertEquals(2, factoryTable.size());
    }

    @Test
    public void containerSimulationTest() throws Exception {
        DefaultMQProducer producerA = new DefaultMQProducer("groupTest");
        producerA.setInstanceName("instanceTest");
        producerA.setNamesrvAddr("127.0.0.1:9876");
        MQClientManager mqClientManagerA = createMQClientManager();
        MQClientInstance instanceA = mqClientManagerA.getOrCreateMQClientInstance(producerA);

        DefaultMQProducer producerB = new DefaultMQProducer("groupTest");
        producerB.setInstanceName("instanceTest");
        producerB.setNamesrvAddr("127.0.0.1:9876");
        MQClientManager mqClientManagerB = createMQClientManager();
        MQClientInstance instanceB = mqClientManagerB.getOrCreateMQClientInstance(producerB);

        Assert.assertEquals(instanceA.getClientConfig().getClientIP(), instanceB.getClientConfig().getClientIP());
        Assert.assertEquals(instanceA.getClientConfig().getInstanceName(), instanceB.getClientConfig().getInstanceName());
        Assert.assertEquals(instanceA.getClientConfig().getGroupCode(), instanceB.getClientConfig().getGroupCode());
        Assert.assertNotEquals(instanceA.getClientConfig().getFactoryIndex(), instanceB.getClientConfig().getFactoryIndex());
        Assert.assertNotEquals(instanceA.getClientId(), instanceB.getClientId());
    }

    private MQClientManager createMQClientManager() throws Exception {
        Constructor[] constructors = MQClientManager.class.getDeclaredConstructors();
        Constructor constructor = constructors[0];
        constructor.setAccessible(true);
        return (MQClientManager) constructor.newInstance();
    }
}
