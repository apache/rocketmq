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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerImplTest {

    private String producerGroupPrefix = "RocketMQ";

    @Before
    public void init() throws Exception {

    }

    @After
    public void terminate() {

    }

    @Test
    public void testStart_CheckNameserverConfig() {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        DefaultMQProducerImpl defaultMQProducerImpl = new DefaultMQProducerImpl(producer, null);
        try {
            defaultMQProducerImpl.start(false);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("No name server address");
        }

        producer.setNamesrvAddr("127.0.0.1:9876");
        DefaultMQProducerImpl defaultMQProducerImpl1 = new DefaultMQProducerImpl(producer, null);
        try {
            defaultMQProducerImpl1.start(false);
        } catch (MQClientException e) {
            assertThat(e).hasMessageNotContaining("No name server address");
        }
    }
}
