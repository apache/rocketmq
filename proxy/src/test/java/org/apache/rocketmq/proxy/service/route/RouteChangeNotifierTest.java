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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RouteChangeNotifierTest {
    private LoadingCache<String, MessageQueueView> mockCache;
    private ThreadPoolExecutor mockExecutor;
    private RouteChangeNotifier notifier;

    @Before
    public void setUp() {
        mockCache = mock(LoadingCache.class);
        mockExecutor = mock(ThreadPoolExecutor.class);
        notifier = new RouteChangeNotifier(mockCache);
    }

    @Test
    public void testHandleShutdownEvent() throws Exception {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("eventType", "SHUTDOWN");
        eventData.put("brokerName", "TestBroker");
        eventData.put("timestamp", System.currentTimeMillis());
        eventData.put("affectedTopics", Arrays.asList("TopicA", "TopicB"));

        MessageExt msg = new MessageExt();
        msg.setBody(JSON.toJSONString(eventData).getBytes());

        invokePrivateMethod(notifier, "processEventMessages", Collections.singletonList(msg));

        verifyMarkCacheDirtyCalled("TopicA");
        verifyMarkCacheDirtyCalled("TopicB");
    }

    @Test
    public void testHandleTopicChangeEvent() throws Exception {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("eventType", "TOPIC_CHANGE");
        eventData.put("brokerName", "TestBroker");
        eventData.put("affectedTopics", Collections.singletonList("TestTopic"));
        eventData.put("timestamp", System.currentTimeMillis());

        MessageExt msg = new MessageExt();
        msg.setBody(JSON.toJSONString(eventData).getBytes());

        invokePrivateMethod(notifier, "processEventMessages", Collections.singletonList(msg));

        verifyMarkCacheDirtyCalled("TestTopic");
    }

    @Test
    public void testMarkCacheDirtyAddsToPendingTopics() throws Exception {
        notifier.markCacheDirty("TestTopic", System.currentTimeMillis());

        Field pendingTopicsField = RouteChangeNotifier.class.getDeclaredField("pendingTopics");
        pendingTopicsField.setAccessible(true);
        Queue<String> pendingTopics = (Queue<String>) pendingTopicsField.get(notifier);

        assertTrue(pendingTopics.contains("TestTopic"));
    }

    @Test
    public void testRefreshTriggersCacheRefresh() throws Exception {
        notifier.markCacheDirty("TestTopic", System.currentTimeMillis());

        when(mockCache.getIfPresent("TestTopic")).thenReturn(mock(MessageQueueView.class));

        invokePrivateMethod(notifier, "refreshSingleRoute", "TestTopic");

        verify(mockCache).refresh("TestTopic");
    }

    @Test
    public void testRefreshFailureAddsToRetryQueue() throws Exception {
        notifier.markCacheDirty("TestTopic", System.currentTimeMillis());

        when(mockCache.getIfPresent("TestTopic")).thenReturn(mock(MessageQueueView.class));

        doThrow(new RuntimeException("Refresh error")).when(mockCache).refresh("TestTopic");

        invokePrivateMethod(notifier, "refreshSingleRoute", "TestTopic");

        Field pendingTopicsField = RouteChangeNotifier.class.getDeclaredField("pendingTopics");
        pendingTopicsField.setAccessible(true);
        Queue<String> pendingTopics = (Queue<String>) pendingTopicsField.get(notifier);

        assertTrue(pendingTopics.contains("TestTopic"));
    }

    private void invokePrivateMethod(Object obj, String methodName, Object arg) throws Exception {
        java.lang.reflect.Method method;
        
        if (arg instanceof List) {
            method = obj.getClass().getDeclaredMethod(methodName, List.class);
            method.setAccessible(true);
            method.invoke(obj, arg);
        } else if (arg instanceof String) {
            method = obj.getClass().getDeclaredMethod(methodName, String.class);
            method.setAccessible(true);
            method.invoke(obj, arg);
        }
    }

    private void verifyMarkCacheDirtyCalled(String topic) throws Exception {
        Field pendingTopicsField = RouteChangeNotifier.class.getDeclaredField("pendingTopics");
        pendingTopicsField.setAccessible(true);
        Queue<String> pendingTopics = (Queue<String>) pendingTopicsField.get(notifier);
        
        assertTrue("Topic " + topic + " should be in pending topics", 
                  pendingTopics.contains(topic));
    }
}
