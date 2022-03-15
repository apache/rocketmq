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

import org.apache.rocketmq.proxy.client.ForwardClientManager;
import org.apache.rocketmq.proxy.client.DefaultForwardClient;
import org.apache.rocketmq.proxy.client.ForwardProducer;
import org.apache.rocketmq.proxy.client.ForwardReadConsumer;
import org.apache.rocketmq.proxy.client.TopicRouteCache;
import org.apache.rocketmq.proxy.client.ForwardWriteConsumer;
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
    protected ForwardClientManager clientManager;
    @Mock
    protected DefaultForwardClient defaultClient;
    @Mock
    protected ForwardProducer producerClient;
    @Mock
    protected ForwardReadConsumer readConsumerClient;
    @Mock
    protected ForwardWriteConsumer writeConsumerClient;
    @Mock
    protected TopicRouteCache topicRouteCache;

    @Before
    public void before() throws Throwable {
        when(clientManager.getDefaultForwardClient()).thenReturn(defaultClient);
        when(clientManager.getForwardProducer()).thenReturn(producerClient);
        when(clientManager.getForwardReadConsumer()).thenReturn(readConsumerClient);
        when(clientManager.getForwardWriteConsumer()).thenReturn(writeConsumerClient);
        when(clientManager.getTopicRouteCache()).thenReturn(topicRouteCache);

        beforeEach();
    }

    public abstract void beforeEach() throws Throwable;
}
