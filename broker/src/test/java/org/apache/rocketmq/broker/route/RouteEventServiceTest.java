/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.route;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.alibaba.fastjson2.JSON;

public class RouteEventServiceTest {
    private BrokerController brokerController;
    private MessageStore mockMessageStore;
    private RouteEventService routeEventService;

    @Before
    public void setUp() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableRouteChangeNotification(true);
        
        brokerController = new BrokerController(
            brokerConfig,
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig()
        );
        
        mockMessageStore = mock(MessageStore.class);
        brokerController.setMessageStore(mockMessageStore);
        
        routeEventService = new RouteEventService(brokerController);
    }

    @Test
    public void testPublishEventSuccessfully() {
        when(mockMessageStore.putMessage(any())).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, null));

        routeEventService.publishEvent(RouteEventType.SHUTDOWN, Collections.singleton("TestTopic"));

        verify(mockMessageStore).putMessage(any(MessageExtBrokerInner.class));
    }

    @Test
    public void testIncludeTopicInEvent() {
        when(mockMessageStore.putMessage(any())).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, null));

        routeEventService.publishEvent(RouteEventType.TOPIC_CHANGE, Collections.singleton("TestTopic"));

        ArgumentCaptor<MessageExtBrokerInner> captor = ArgumentCaptor.forClass(MessageExtBrokerInner.class);
        verify(mockMessageStore).putMessage(captor.capture());

        Map<String, Object> eventData = JSON.parseObject(
            new String(captor.getValue().getBody()), 
            Map.class
        );

        List<String> affectedTopics = (List<String>) eventData.get(RouteEventConstants.AFFECTED_TOPICS);

        List<String> expectedTopics = Collections.singletonList("TestTopic");

        assertEquals(expectedTopics, affectedTopics);
    }
}
