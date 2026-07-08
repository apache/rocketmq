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

package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RouteChangeNotifierTest {

    @Mock
    private TopicRouteService topicRouteService;

    @Mock
    private ServerCallStreamObserver<SubscribeRouteEventsResponse> streamObserver;

    private RouteChangeNotifier notifier;

    @Before
    public void setUp() {
        notifier = new RouteChangeNotifier(topicRouteService);
        when(topicRouteService.getAllTopicNames()).thenReturn(Collections.emptySet());
    }

    // ========== Subscribe Tests ==========

    @Test
    public void testSubscribe_RegistersSubscription() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        assertEquals(1, notifier.getSubscriberCount());
    }

    @Test
    public void testSubscribe_SetsOnCancelHandler() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        verify(streamObserver).setOnCancelHandler(any(Runnable.class));
    }

    @Test
    public void testSubscribe_OnCancelRemovesSubscription() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        ArgumentCaptor<Runnable> cancelHandlerCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(streamObserver).setOnCancelHandler(cancelHandlerCaptor.capture());

        cancelHandlerCaptor.getValue().run();
        assertEquals(0, notifier.getSubscriberCount());
    }

    @Test
    public void testSubscribe_WithNonServerCallStreamObserver() {
        StreamObserver<SubscribeRouteEventsResponse> plainObserver = mock(StreamObserver.class);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), plainObserver);
        assertEquals(1, notifier.getSubscriberCount());
        // No setOnCancelHandler called for plain StreamObserver
    }

    @Test
    public void testSubscribe_MultipleSubscribers() {
        ServerCallStreamObserver<SubscribeRouteEventsResponse> observer2 = mock(ServerCallStreamObserver.class);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), observer2);
        assertEquals(2, notifier.getSubscriberCount());
    }

    @Test
    public void testSubscribe_SendsInitialSnapshot() {
        Set<String> cachedTopics = new HashSet<>();
        cachedTopics.add("topic-a");
        cachedTopics.add("topic-b");
        when(topicRouteService.getAllTopicNames()).thenReturn(cachedTopics);

        TopicRouteData routeData = createTopicRouteData("topic-a");
        MessageQueueView view = new MessageQueueView("topic-a", routeData, null);
        when(topicRouteService.getCachedTopicRouteData("topic-a")).thenReturn(view);
        when(topicRouteService.getCachedTopicRouteData("topic-b")).thenReturn(null);

        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        // Should send snapshot for topic-a only (topic-b returns null view)
        ArgumentCaptor<SubscribeRouteEventsResponse> responseCaptor = ArgumentCaptor.forClass(SubscribeRouteEventsResponse.class);
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals("topic-a", responseCaptor.getValue().getEvent().getTopic());
    }

    @Test
    public void testSubscribe_WithTopicFilter() {
        Set<String> cachedTopics = new HashSet<>();
        cachedTopics.add("topic-a");
        cachedTopics.add("topic-b");
        when(topicRouteService.getAllTopicNames()).thenReturn(cachedTopics);

        TopicRouteData routeDataA = createTopicRouteData("topic-a");
        TopicRouteData routeDataB = createTopicRouteData("topic-b");
        when(topicRouteService.getCachedTopicRouteData("topic-a")).thenReturn(new MessageQueueView("topic-a", routeDataA, null));
        when(topicRouteService.getCachedTopicRouteData("topic-b")).thenReturn(new MessageQueueView("topic-b", routeDataB, null));

        notifier.subscribe(Arrays.asList("topic-a"), Collections.emptyList(), streamObserver);

        // Should only send snapshot for topic-a (topic-b filtered out)
        ArgumentCaptor<SubscribeRouteEventsResponse> responseCaptor = ArgumentCaptor.forClass(SubscribeRouteEventsResponse.class);
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals("topic-a", responseCaptor.getValue().getEvent().getTopic());
    }

    @Test
    public void testSubscribe_WithEventTypeFilter() {
        Set<String> cachedTopics = new HashSet<>();
        cachedTopics.add("topic-a");
        when(topicRouteService.getAllTopicNames()).thenReturn(cachedTopics);

        TopicRouteData routeData = createTopicRouteData("topic-a");
        when(topicRouteService.getCachedTopicRouteData("topic-a")).thenReturn(new MessageQueueView("topic-a", routeData, null));

        // Subscribe with only BROKER_ONLINE filter - ROUTE_SNAPSHOT should be filtered out
        notifier.subscribe(Collections.emptyList(), Arrays.asList(RouteChangeEventType.BROKER_ONLINE), streamObserver);

        // Should NOT send ROUTE_SNAPSHOT since event type filter doesn't include it
        verify(streamObserver, never()).onNext(any());
    }

    @Test
    public void testSubscribe_NullTopicRouteService() {
        RouteChangeNotifier notifierNoService = new RouteChangeNotifier(null);
        notifierNoService.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        assertEquals(1, notifierNoService.getSubscriberCount());
        // No snapshot sent, no exception thrown
        verify(streamObserver, never()).onNext(any());
    }

    // ========== onRouteRefreshed Tests ==========

    @Test
    public void testOnRouteRefreshed_NoSubscribers() {
        TopicRouteData oldRoute = createTopicRouteData("topic-a");
        TopicRouteData newRoute = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", oldRoute, null);
        MessageQueueView newView = new MessageQueueView("topic-a", newRoute, null);

        // Should not throw when no subscribers
        notifier.onRouteRefreshed("topic-a", oldView, newView);
    }

    @Test
    public void testOnRouteRefreshed_TopicCreate() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        TopicRouteData newRoute = createTopicRouteData("topic-a");
        MessageQueueView newView = new MessageQueueView("topic-a", newRoute, null);

        notifier.onRouteRefreshed("topic-a", null, newView);

        // null -> route with 1 broker produces TOPIC_CREATE + BROKER_ONLINE events
        ArgumentCaptor<SubscribeRouteEventsResponse> responseCaptor = ArgumentCaptor.forClass(SubscribeRouteEventsResponse.class);
        verify(streamObserver, times(2)).onNext(responseCaptor.capture());
        List<SubscribeRouteEventsResponse> responses = responseCaptor.getAllValues();
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_CREATE,
            responses.get(0).getEvent().getEventType());
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.BROKER_ONLINE,
            responses.get(1).getEvent().getEventType());
    }

    @Test
    public void testOnRouteRefreshed_TopicDelete() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        TopicRouteData oldRoute = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", oldRoute, null);

        notifier.onRouteRefreshed("topic-a", oldView, null);

        // route with 1 broker -> null produces BROKER_OFFLINE + TOPIC_DELETE events
        ArgumentCaptor<SubscribeRouteEventsResponse> responseCaptor = ArgumentCaptor.forClass(SubscribeRouteEventsResponse.class);
        verify(streamObserver, times(2)).onNext(responseCaptor.capture());
        List<SubscribeRouteEventsResponse> responses = responseCaptor.getAllValues();
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.BROKER_OFFLINE,
            responses.get(0).getEvent().getEventType());
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_DELETE,
            responses.get(1).getEvent().getEventType());
    }

    @Test
    public void testOnRouteRefreshed_NoChange() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        TopicRouteData route = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", route, null);
        MessageQueueView newView = new MessageQueueView("topic-a", route, null);

        notifier.onRouteRefreshed("topic-a", oldView, newView);

        // No events should be pushed (same route data, no initial snapshot since no cached topics)
        verify(streamObserver, never()).onNext(any(SubscribeRouteEventsResponse.class));
    }

    @Test
    public void testOnRouteRefreshed_WithTopicFilter() {
        notifier.subscribe(Arrays.asList("topic-b"), Collections.emptyList(), streamObserver);

        TopicRouteData newRoute = createTopicRouteData("topic-a");
        MessageQueueView newView = new MessageQueueView("topic-a", newRoute, null);

        notifier.onRouteRefreshed("topic-a", null, newView);

        // Event for topic-a should be filtered out (subscriber only wants topic-b)
        // Only initial snapshot call (if any) should happen, no event for topic-a
        verify(streamObserver, never()).onNext(any(SubscribeRouteEventsResponse.class));
    }

    @Test
    public void testOnRouteRefreshed_WithEventTypeFilter() {
        notifier.subscribe(Collections.emptyList(), Arrays.asList(RouteChangeEventType.BROKER_ONLINE), streamObserver);

        TopicRouteData oldRoute = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", oldRoute, null);

        notifier.onRouteRefreshed("topic-a", oldView, null);

        // TOPIC_DELETE should be filtered out (subscriber only wants BROKER_ONLINE)
        verify(streamObserver, never()).onNext(any(SubscribeRouteEventsResponse.class));
    }

    @Test
    public void testOnRouteRefreshed_BroadcastsToMultipleSubscribers() {
        ServerCallStreamObserver<SubscribeRouteEventsResponse> observer2 = mock(ServerCallStreamObserver.class);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), observer2);

        TopicRouteData newRoute = createTopicRouteData("topic-a");
        MessageQueueView newView = new MessageQueueView("topic-a", newRoute, null);

        notifier.onRouteRefreshed("topic-a", null, newView);

        verify(streamObserver, atLeastOnce()).onNext(any(SubscribeRouteEventsResponse.class));
        verify(observer2, atLeastOnce()).onNext(any(SubscribeRouteEventsResponse.class));
    }

    // ========== Shutdown Tests ==========

    @Test
    public void testShutdown_CompletesAllStreams() {
        ServerCallStreamObserver<SubscribeRouteEventsResponse> observer2 = mock(ServerCallStreamObserver.class);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), observer2);

        notifier.shutdown();

        verify(streamObserver).onCompleted();
        verify(observer2).onCompleted();
        assertEquals(0, notifier.getSubscriberCount());
    }

    @Test
    public void testShutdown_NoSubscribers() {
        notifier.shutdown();
        assertEquals(0, notifier.getSubscriberCount());
    }

    // ========== getSubscriberCount Tests ==========

    @Test
    public void testGetSubscriberCount_Initial() {
        assertEquals(0, notifier.getSubscriberCount());
    }

    @Test
    public void testGetSubscriberCount_AfterSubscribeAndCancel() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        assertEquals(1, notifier.getSubscriberCount());

        ArgumentCaptor<Runnable> cancelHandlerCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(streamObserver).setOnCancelHandler(cancelHandlerCaptor.capture());
        cancelHandlerCaptor.getValue().run();

        assertEquals(0, notifier.getSubscriberCount());
    }

    // ========== Helper Methods ==========

    private TopicRouteData createTopicRouteData(String topic) {
        TopicRouteData routeData = new TopicRouteData();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");

        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("default-cluster");
        brokerData.setBrokerName("broker-a");
        brokerData.setBrokerAddrs(brokerAddrs);

        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setReadQueueNums(4);
        queueData.setWriteQueueNums(4);
        queueData.setPerm(6);

        List<BrokerData> brokerDatas = new ArrayList<>();
        brokerDatas.add(brokerData);
        List<QueueData> queueDatas = new ArrayList<>();
        queueDatas.add(queueData);

        routeData.setBrokerDatas(brokerDatas);
        routeData.setQueueDatas(queueDatas);
        return routeData;
    }
}