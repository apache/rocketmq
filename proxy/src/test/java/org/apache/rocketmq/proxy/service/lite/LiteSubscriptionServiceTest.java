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

package org.apache.rocketmq.proxy.service.lite;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteSubscriptionServiceTest {

    @Mock
    private TopicRouteService topicRouteService;

    @Mock
    private MQClientAPIFactory mqClientAPIFactory;

    @Mock
    private MQClientAPIExt mqClientAPIExt;

    private LiteSubscriptionService liteSubscriptionService;

    @Before
    public void setUp() {
        liteSubscriptionService = new LiteSubscriptionService(topicRouteService, mqClientAPIFactory);
    }

    /**
     * Test successful case: all brokers sync successfully
     */
    @Test
    public void testSyncLiteSubscription_Success() throws Exception {
        ProxyContext ctx = ProxyContext.create();
        LiteSubscriptionDTO liteSubscriptionDTO = new LiteSubscriptionDTO();
        liteSubscriptionDTO.setTopic("testTopic");
        long timeoutMillis = 3000L;

        MessageQueueView messageQueueView = mock(MessageQueueView.class);
        MessageQueueSelector readSelector = mock(MessageQueueSelector.class);
        when(messageQueueView.getReadSelector()).thenReturn(readSelector);

        AddressableMessageQueue queue1 = mock(AddressableMessageQueue.class);
        AddressableMessageQueue queue2 = mock(AddressableMessageQueue.class);
        when(queue1.getBrokerAddr()).thenReturn("broker1:10911");
        when(queue2.getBrokerAddr()).thenReturn("broker2:10911");
        List<AddressableMessageQueue> readQueues = Arrays.asList(queue1, queue2);
        when(readSelector.getBrokerActingQueues()).thenReturn(readQueues);

        when(topicRouteService.getAllMessageQueueView(ctx, "testTopic")).thenReturn(messageQueueView);

        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);

        when(mqClientAPIExt.syncLiteSubscriptionAsync(anyString(), any(LiteSubscriptionDTO.class), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> future = liteSubscriptionService.syncLiteSubscription(ctx, liteSubscriptionDTO, timeoutMillis);

        assertDoesNotThrow(() -> future.get());
        verify(mqClientAPIExt, times(2)).syncLiteSubscriptionAsync(anyString(), any(LiteSubscriptionDTO.class), anyLong());
    }

    /**
     * Test exception case: topicRouteService throws exception
     */
    @Test
    public void testSyncLiteSubscription_TopicRouteServiceException() throws Exception {
        ProxyContext ctx = ProxyContext.create();
        LiteSubscriptionDTO liteSubscriptionDTO = new LiteSubscriptionDTO();
        liteSubscriptionDTO.setTopic("testTopic");
        long timeoutMillis = 3000L;

        when(topicRouteService.getAllMessageQueueView(ctx, "testTopic"))
            .thenThrow(new RuntimeException("Topic route error"));

        CompletableFuture<Void> future = liteSubscriptionService.syncLiteSubscription(ctx, liteSubscriptionDTO, timeoutMillis);

        assertTrue(future.isCompletedExceptionally());
        verify(mqClientAPIFactory, never()).getClient();
    }

    /**
     * Test exception case: some broker sync fails
     */
    @Test
    public void testSyncLiteSubscription_SomeBrokerFail() throws Exception {
        ProxyContext ctx = ProxyContext.create();
        LiteSubscriptionDTO liteSubscriptionDTO = new LiteSubscriptionDTO();
        liteSubscriptionDTO.setTopic("testTopic");
        long timeoutMillis = 3000L;

        MessageQueueView messageQueueView = mock(MessageQueueView.class);
        MessageQueueSelector readSelector = mock(MessageQueueSelector.class);
        when(messageQueueView.getReadSelector()).thenReturn(readSelector);

        AddressableMessageQueue queue1 = mock(AddressableMessageQueue.class);
        AddressableMessageQueue queue2 = mock(AddressableMessageQueue.class);
        when(queue1.getBrokerAddr()).thenReturn("broker1:10911");
        when(queue2.getBrokerAddr()).thenReturn("broker2:10911");
        List<AddressableMessageQueue> readQueues = Arrays.asList(queue1, queue2);
        when(readSelector.getBrokerActingQueues()).thenReturn(readQueues);

        when(topicRouteService.getAllMessageQueueView(ctx, "testTopic")).thenReturn(messageQueueView);

        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);

        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker sync failed"));

        when(mqClientAPIExt.syncLiteSubscriptionAsync(anyString(), any(LiteSubscriptionDTO.class), anyLong()))
            .thenReturn(failedFuture);

        CompletableFuture<Void> future = liteSubscriptionService.syncLiteSubscription(ctx, liteSubscriptionDTO, timeoutMillis);

        assertTrue(future.isCompletedExceptionally());
        verify(mqClientAPIExt, times(2)).syncLiteSubscriptionAsync(anyString(), any(LiteSubscriptionDTO.class), anyLong());
    }
}
