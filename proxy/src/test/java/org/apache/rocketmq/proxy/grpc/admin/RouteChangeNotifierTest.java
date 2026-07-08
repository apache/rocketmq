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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for RouteChangeNotifier.
 * <p>
 * Uses sun.misc.Unsafe.allocateInstance() to create TopicRouteService instances
 * without calling the constructor, avoiding Mockito 3.10 / Java 21 Byte Buddy
 * incompatibility and bypassing the complex TopicRouteService constructor that
 * requires MQClientAPIFactory and ConfigurationManager initialization.
 */
public class RouteChangeNotifierTest {

    private TestTopicRouteService topicRouteService;
    private TestServerCallStreamObserver streamObserver;
    private RouteChangeNotifier notifier;

    @Before
    public void setUp() throws Exception {
        topicRouteService = TestTopicRouteService.create();
        topicRouteService.setAllTopicNames(Collections.emptySet());
        streamObserver = new TestServerCallStreamObserver();
        notifier = new RouteChangeNotifier(topicRouteService);
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
        assertNotNull(streamObserver.cancelHandler);
    }

    @Test
    public void testSubscribe_OnCancelRemovesSubscription() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        assertNotNull(streamObserver.cancelHandler);
        streamObserver.cancelHandler.run();
        assertEquals(0, notifier.getSubscriberCount());
    }

    @Test
    public void testSubscribe_WithNonServerCallStreamObserver() {
        TestStreamObserver plainObserver = new TestStreamObserver();
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), plainObserver);
        assertEquals(1, notifier.getSubscriberCount());
        // No setOnCancelHandler called for plain StreamObserver - this is expected
    }

    @Test
    public void testSubscribe_MultipleSubscribers() {
        TestServerCallStreamObserver observer2 = new TestServerCallStreamObserver();
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), observer2);
        assertEquals(2, notifier.getSubscriberCount());
    }

    @Test
    public void testSubscribe_SendsInitialSnapshot() {
        Set<String> cachedTopics = new HashSet<>();
        cachedTopics.add("topic-a");
        cachedTopics.add("topic-b");
        topicRouteService.setAllTopicNames(cachedTopics);

        TopicRouteData routeData = createTopicRouteData("topic-a");
        MessageQueueView view = new MessageQueueView("topic-a", routeData, null);
        topicRouteService.setCachedTopicRouteData("topic-a", view);
        // topic-b returns null view (not set in cache)

        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        // Should send snapshot for topic-a only (topic-b returns null view)
        assertEquals(1, streamObserver.responses.size());
        assertEquals("topic-a", streamObserver.responses.get(0).getEvent().getTopic());
    }

    @Test
    public void testSubscribe_WithTopicFilter() {
        Set<String> cachedTopics = new HashSet<>();
        cachedTopics.add("topic-a");
        cachedTopics.add("topic-b");
        topicRouteService.setAllTopicNames(cachedTopics);

        TopicRouteData routeDataA = createTopicRouteData("topic-a");
        TopicRouteData routeDataB = createTopicRouteData("topic-b");
        topicRouteService.setCachedTopicRouteData("topic-a", new MessageQueueView("topic-a", routeDataA, null));
        topicRouteService.setCachedTopicRouteData("topic-b", new MessageQueueView("topic-b", routeDataB, null));

        notifier.subscribe(Arrays.asList("topic-a"), Collections.emptyList(), streamObserver);

        // Should only send snapshot for topic-a (topic-b filtered out)
        assertEquals(1, streamObserver.responses.size());
        assertEquals("topic-a", streamObserver.responses.get(0).getEvent().getTopic());
    }

    @Test
    public void testSubscribe_WithEventTypeFilter() {
        Set<String> cachedTopics = new HashSet<>();
        cachedTopics.add("topic-a");
        topicRouteService.setAllTopicNames(cachedTopics);

        TopicRouteData routeData = createTopicRouteData("topic-a");
        topicRouteService.setCachedTopicRouteData("topic-a", new MessageQueueView("topic-a", routeData, null));

        // Subscribe with only BROKER_ONLINE filter - ROUTE_SNAPSHOT should be filtered out
        notifier.subscribe(Collections.emptyList(), Arrays.asList(RouteChangeEventType.BROKER_ONLINE), streamObserver);

        // Should NOT send ROUTE_SNAPSHOT since event type filter doesn't include it
        assertTrue(streamObserver.responses.isEmpty());
    }

    @Test
    public void testSubscribe_NullTopicRouteService() {
        RouteChangeNotifier notifierNoService = new RouteChangeNotifier(null);
        notifierNoService.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        assertEquals(1, notifierNoService.getSubscriberCount());
        // No snapshot sent, no exception thrown
        assertTrue(streamObserver.responses.isEmpty());
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
        assertEquals(2, streamObserver.responses.size());
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_CREATE,
            streamObserver.responses.get(0).getEvent().getEventType());
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.BROKER_ONLINE,
            streamObserver.responses.get(1).getEvent().getEventType());
    }

    @Test
    public void testOnRouteRefreshed_TopicDelete() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        TopicRouteData oldRoute = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", oldRoute, null);

        notifier.onRouteRefreshed("topic-a", oldView, null);

        // route with 1 broker -> null produces BROKER_OFFLINE + TOPIC_DELETE events
        assertEquals(2, streamObserver.responses.size());
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_DELETE,
            streamObserver.responses.get(0).getEvent().getEventType());
        assertEquals(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.BROKER_OFFLINE,
            streamObserver.responses.get(1).getEvent().getEventType());
    }

    @Test
    public void testOnRouteRefreshed_NoChange() {
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);

        TopicRouteData route = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", route, null);
        MessageQueueView newView = new MessageQueueView("topic-a", route, null);

        notifier.onRouteRefreshed("topic-a", oldView, newView);

        // No events should be pushed (same route data, no initial snapshot since no cached topics)
        assertTrue(streamObserver.responses.isEmpty());
    }

    @Test
    public void testOnRouteRefreshed_WithTopicFilter() {
        notifier.subscribe(Arrays.asList("topic-b"), Collections.emptyList(), streamObserver);

        TopicRouteData newRoute = createTopicRouteData("topic-a");
        MessageQueueView newView = new MessageQueueView("topic-a", newRoute, null);

        notifier.onRouteRefreshed("topic-a", null, newView);

        // Event for topic-a should be filtered out (subscriber only wants topic-b)
        assertTrue(streamObserver.responses.isEmpty());
    }

    @Test
    public void testOnRouteRefreshed_WithEventTypeFilter() {
        notifier.subscribe(Collections.emptyList(), Arrays.asList(RouteChangeEventType.BROKER_ONLINE), streamObserver);

        TopicRouteData oldRoute = createTopicRouteData("topic-a");
        MessageQueueView oldView = new MessageQueueView("topic-a", oldRoute, null);

        notifier.onRouteRefreshed("topic-a", oldView, null);

        // TOPIC_DELETE should be filtered out (subscriber only wants BROKER_ONLINE)
        assertTrue(streamObserver.responses.isEmpty());
    }

    @Test
    public void testOnRouteRefreshed_BroadcastsToMultipleSubscribers() {
        TestServerCallStreamObserver observer2 = new TestServerCallStreamObserver();
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), observer2);

        TopicRouteData newRoute = createTopicRouteData("topic-a");
        MessageQueueView newView = new MessageQueueView("topic-a", newRoute, null);

        notifier.onRouteRefreshed("topic-a", null, newView);

        assertFalse(streamObserver.responses.isEmpty());
        assertFalse(observer2.responses.isEmpty());
    }

    // ========== Shutdown Tests ==========

    @Test
    public void testShutdown_CompletesAllStreams() {
        TestServerCallStreamObserver observer2 = new TestServerCallStreamObserver();
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), streamObserver);
        notifier.subscribe(Collections.emptyList(), Collections.emptyList(), observer2);

        notifier.shutdown();

        assertTrue(streamObserver.completed);
        assertTrue(observer2.completed);
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

        assertNotNull(streamObserver.cancelHandler);
        streamObserver.cancelHandler.run();

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

    /**
     * Create an instance without calling the constructor using sun.misc.Unsafe.
     * This bypasses constructor logic and avoids Mockito 3.10 / Java 21 Byte Buddy
     * incompatibility with classes extending AbstractStartAndShutdown.
     */
    @SuppressWarnings("unchecked")
    private static <T> T createInstanceWithoutConstructor(Class<T> clazz) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return (T) allocateInstance.invoke(unsafe, clazz);
    }

    // ========== Test Helper Classes ==========

    /**
     * Test implementation of TopicRouteService that uses field-backed overrides
     * for getAllTopicNames() and getCachedTopicRouteData() instead of Mockito mocking.
     * <p>
     * Created via Unsafe.allocateInstance() to bypass the TopicRouteService constructor
     * which requires MQClientAPIFactory and ConfigurationManager initialization.
     * Abstract methods throw UnsupportedOperationException as they are not needed
     * by RouteChangeNotifier tests.
     */
    private static class TestTopicRouteService extends TopicRouteService {
        private Set<String> allTopicNames;
        private Map<String, MessageQueueView> cachedViews;

        /**
         * Constructor exists only to satisfy the Java compiler.
         * Never called at runtime - Unsafe.allocateInstance() bypasses all constructors,
         * avoiding the TopicRouteService constructor that requires MQClientAPIFactory
         * and ConfigurationManager initialization.
         */
        @SuppressWarnings("unused")
        TestTopicRouteService() {
            super(null);
        }

        static TestTopicRouteService create() throws Exception {
            return createInstanceWithoutConstructor(TestTopicRouteService.class);
        }

        void setAllTopicNames(Set<String> topics) {
            this.allTopicNames = topics;
        }

        void setCachedTopicRouteData(String topic, MessageQueueView view) {
            if (this.cachedViews == null) {
                this.cachedViews = new HashMap<>();
            }
            this.cachedViews.put(topic, view);
        }

        @Override
        public Set<String> getAllTopicNames() {
            if (allTopicNames == null) {
                allTopicNames = Collections.emptySet();
            }
            return allTopicNames;
        }

        @Override
        public MessageQueueView getCachedTopicRouteData(String topic) {
            if (cachedViews == null) {
                return null;
            }
            return cachedViews.get(topic);
        }

        @Override
        public MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) {
            throw new UnsupportedOperationException("Not needed for RouteChangeNotifier tests");
        }

        @Override
        public ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx,
            List<Address> requestHostAndPortList, String topicName) {
            throw new UnsupportedOperationException("Not needed for RouteChangeNotifier tests");
        }

        @Override
        public String getBrokerAddr(ProxyContext ctx, String brokerName) {
            throw new UnsupportedOperationException("Not needed for RouteChangeNotifier tests");
        }

        @Override
        public AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx,
            MessageQueue messageQueue) {
            throw new UnsupportedOperationException("Not needed for RouteChangeNotifier tests");
        }
    }

    /**
     * Test implementation of ServerCallStreamObserver that records method calls
     * for verification without requiring Mockito mocking (which is incompatible
     * with Java 21 Byte Buddy).
     */
    private static class TestServerCallStreamObserver extends ServerCallStreamObserver<SubscribeRouteEventsResponse> {
        final List<SubscribeRouteEventsResponse> responses = new ArrayList<>();
        volatile boolean completed = false;
        volatile Throwable error;
        Runnable cancelHandler;
        Runnable readyHandler;
        boolean cancelled = false;

        @Override
        public void setOnCancelHandler(Runnable onCancelHandler) {
            this.cancelHandler = onCancelHandler;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {
            this.readyHandler = onReadyHandler;
        }

        @Override
        public void disableAutoInboundFlowControl() {

        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setCompression(String compression) {
            // no-op for test
        }

        @Override
        public void request(int numMessages) {
            // no-op for test
        }

        @Override
        public void setMessageCompression(boolean b) {

        }

        @Override
        public void onNext(SubscribeRouteEventsResponse value) {
            responses.add(value);
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }
    }

    /**
     * Test implementation of plain StreamObserver for testing non-ServerCallStreamObserver paths.
     */
    private static class TestStreamObserver implements StreamObserver<SubscribeRouteEventsResponse> {
        final List<SubscribeRouteEventsResponse> responses = new ArrayList<>();
        volatile boolean completed = false;
        volatile Throwable error;

        @Override
        public void onNext(SubscribeRouteEventsResponse value) {
            responses.add(value);
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}