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
import java.util.Queue;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.LoadingCache;

public class RouteCacheRefresherTest {
    private LoadingCache<String, MessageQueueView> mockCache;
    private ThreadPoolExecutor mockExecutor;
    private RouteCacheRefresher refresher;

    @Before
    public void setUp() {
        mockCache = mock(LoadingCache.class);
        mockExecutor = mock(ThreadPoolExecutor.class);
        refresher = new RouteCacheRefresher(mockCache, mockExecutor);
    }

    @Test
    public void testMarkCacheDirtyAddsToPendingTopics() throws Exception {
        refresher.markCacheDirty("TestTopic", System.currentTimeMillis());

        Field pendingTopicsField = RouteCacheRefresher.class.getDeclaredField("pendingTopics");
        pendingTopicsField.setAccessible(true);
        Queue<String> pendingTopics = (Queue<String>) pendingTopicsField.get(refresher);

        assertTrue(pendingTopics.contains("TestTopic"));
    }

    @Test
    public void testRefreshTriggersCacheRefresh() throws Exception {
        refresher.markCacheDirty("TestTopic", System.currentTimeMillis());

        when(mockCache.getIfPresent("TestTopic")).thenReturn(mock(MessageQueueView.class));

        invokePrivateMethod(refresher, "refreshSingleRoute", "TestTopic");

        verify(mockCache).refresh("TestTopic");
    }

    @Test
    public void testRefreshFailureAddsToRetryQueue() throws Exception {
        refresher.markCacheDirty("TestTopic", System.currentTimeMillis());

        when(mockCache.getIfPresent("TestTopic")).thenReturn(mock(MessageQueueView.class));

        doThrow(new RuntimeException("Refresh error")).when(mockCache).refresh("TestTopic");

        invokePrivateMethod(refresher, "refreshSingleRoute", "TestTopic");

        Field pendingTopicsField = RouteCacheRefresher.class.getDeclaredField("pendingTopics");
        pendingTopicsField.setAccessible(true);
        Queue<String> pendingTopics = (Queue<String>) pendingTopicsField.get(refresher);

        assertTrue(pendingTopics.contains("TestTopic"));
    }

    private void invokePrivateMethod(Object obj, String methodName, String arg) 
        throws Exception {

        java.lang.reflect.Method method = obj.getClass().getDeclaredMethod(methodName, String.class);
        method.setAccessible(true);
        method.invoke(obj, arg);
    }
}
