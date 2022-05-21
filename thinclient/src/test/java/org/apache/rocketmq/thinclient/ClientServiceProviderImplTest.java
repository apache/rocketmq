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

package org.apache.rocketmq.thinclient;

import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.thinclient.impl.consumer.PushConsumerBuilderImpl;
import org.apache.rocketmq.thinclient.impl.consumer.SimpleConsumerBuilderImpl;
import org.apache.rocketmq.thinclient.impl.producer.ProducerBuilderImpl;
import org.apache.rocketmq.thinclient.message.MessageBuilderImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientServiceProviderImplTest {

    @Test
    public void testNewProducerBuilder() {
        assertEquals(ProducerBuilderImpl.class, ClientServiceProvider.loadService().newProducerBuilder().getClass());
    }

    @Test
    public void testNewPushConsumerBuilder() {
        assertEquals(PushConsumerBuilderImpl.class, ClientServiceProvider.loadService().newPushConsumerBuilder().getClass());
    }

    @Test
    public void testNewSimpleConsumerBuilder() {
        assertEquals(SimpleConsumerBuilderImpl.class, ClientServiceProvider.loadService().newSimpleConsumerBuilder().getClass());
    }

    @Test
    public void testNewMessageBuilder() {
        assertEquals(MessageBuilderImpl.class, ClientServiceProvider.loadService().newMessageBuilder().getClass());
    }
}