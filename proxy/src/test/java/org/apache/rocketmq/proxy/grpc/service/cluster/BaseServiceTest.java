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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import org.apache.rocketmq.proxy.client.ClientManager;
import org.apache.rocketmq.proxy.client.DefaultClient;
import org.apache.rocketmq.proxy.client.ProducerClient;
import org.apache.rocketmq.proxy.client.ReadConsumerClient;
import org.apache.rocketmq.proxy.client.TopicRouteCache;
import org.apache.rocketmq.proxy.client.WriteConsumerClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@Ignore
@RunWith(MockitoJUnitRunner.Silent.class)
public abstract class BaseServiceTest {

    @Mock
    protected ClientManager clientManager;
    @Mock
    protected DefaultClient defaultClient;
    @Mock
    protected ProducerClient producerClient;
    @Mock
    protected ReadConsumerClient readConsumerClient;
    @Mock
    protected WriteConsumerClient writeConsumerClient;
    @Mock
    protected TopicRouteCache topicRouteCache;

    @Before
    public void before() throws Throwable {
        when(clientManager.getDefaultClient()).thenReturn(defaultClient);
        when(clientManager.getProducerClient()).thenReturn(producerClient);
        when(clientManager.getReadConsumerClient()).thenReturn(readConsumerClient);
        when(clientManager.getWriteConsumerClient()).thenReturn(writeConsumerClient);
        when(clientManager.getTopicRouteCache()).thenReturn(topicRouteCache);

        beforeEach();
    }

    public abstract void beforeEach() throws Throwable;
}
