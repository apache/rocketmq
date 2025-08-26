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

package org.apache.rocketmq.proxy.service.route;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson2.JSON;

public class RouteEventSubscriberTest {
    private TopicRouteService mockRouteService;
    private BiConsumer<String, Long> mockDirtyMarker;
    private RouteEventSubscriber subscriber;

    @Before
    public void setUp() {
        mockRouteService = mock(TopicRouteService.class);
        mockDirtyMarker = mock(BiConsumer.class);
        subscriber = new RouteEventSubscriber(mockRouteService, mockDirtyMarker);
    }

    @Test
    public void testHandleShutdownEvent() throws Exception {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("eventType", "SHUTDOWN");
        eventData.put("brokerName", "TestBroker");
        eventData.put("timestamp", System.currentTimeMillis());

        MessageExt msg = new MessageExt();
        msg.setBody(JSON.toJSONString(eventData).getBytes());

        Set<String> topics = new HashSet<>(Arrays.asList("TopicA", "TopicB"));
        when(mockRouteService.getBrokerTopics("TestBroker")).thenReturn(topics);

        invokePrivateMethod(subscriber, "processMessages", Collections.singletonList(msg));

        verify(mockRouteService).removeBrokerToTopics("TestBroker");
        verify(mockDirtyMarker).accept(eq("TopicA"), anyLong());
        verify(mockDirtyMarker).accept(eq("TopicB"), anyLong());
    }

    @Test
    public void testHandleTopicChangeEvent() throws Exception {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("eventType", "TOPIC_CHANGE");
        eventData.put("brokerName", "TestBroker");
        eventData.put("affectedTopic", "TestTopic");
        eventData.put("timestamp", System.currentTimeMillis());

        MessageExt msg = new MessageExt();
        msg.setBody(JSON.toJSONString(eventData).getBytes());

        invokePrivateMethod(subscriber, "processMessages", Collections.singletonList(msg));
        
        verify(mockRouteService).removeBrokerToTopic(eq("TestBroker"), eq("TestTopic"));
        verify(mockDirtyMarker).accept(eq("TestTopic"), anyLong());
    }

    private void invokePrivateMethod(Object obj, String methodName, List<MessageExt> arg) 
        throws Exception {

        java.lang.reflect.Method method = obj.getClass().getDeclaredMethod(methodName, List.class);
        method.setAccessible(true);
        method.invoke(obj, arg);
    }
}
