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

package org.apache.rocketmq.thinclient.impl.producer;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.ProducerBuilder;
import org.apache.rocketmq.thinclient.impl.ClientManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProducerBuilderImplTest {
    @Mock
    private ClientManager clientManager;
    @InjectMocks
    private ProducerBuilder producerBuilder = ClientServiceProvider.loadService().newProducerBuilder();

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @SuppressWarnings("ConfusingArgumentToVarargsMethod")
    @Test(expected = NullPointerException.class)
    public void testSetTopicWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTopics(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetTopicWithTooLong() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        String tooLongTopic = StringUtils.repeat("a", 128);
        builder.setTopics(tooLongTopic);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxAttempts() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setMaxAttempts(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTransactionCheckerWithNull() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.setTransactionChecker(null);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutClientConfiguration() {
        final ProducerBuilderImpl builder = new ProducerBuilderImpl();
        builder.build();
    }

    @Test
    public void testBuildWithoutTopic() throws ClientException, IOException {
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setAccessPoint("127.0.0.1:80").build();
        Producer producer = ClientServiceProvider.loadService().newProducerBuilder().setClientConfiguration(clientConfiguration).build();
        producer.close();
    }
}
