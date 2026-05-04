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

package org.apache.rocketmq.broker.lite;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.PopLiteMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@ExtendWith(MockitoExtension.class)
public class LiteEventDispatcherTest {

    private LiteEventDispatcher liteEventDispatcher;

    @Mock
    private BrokerController brokerController;

    @Mock
    private LiteSubscriptionRegistry liteSubscriptionRegistry;

    @Mock
    private AbstractLiteLifecycleManager liteLifecycleManager;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    private final BrokerConfig brokerConfig = new BrokerConfig();

    @Before
    public void setUp() {
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);

        liteEventDispatcher = new LiteEventDispatcher(brokerController, liteSubscriptionRegistry, liteLifecycleManager);
        PopLiteMessageProcessor popLiteMessageProcessor = new PopLiteMessageProcessor(brokerController, liteEventDispatcher);
        when(brokerController.getPopLiteMessageProcessor()).thenReturn(popLiteMessageProcessor);
    }

    @Test
    public void testInitAddsListener() {
        liteEventDispatcher.init();
        verify(liteSubscriptionRegistry).addListener(any(LiteEventDispatcher.LiteCtlListenerImpl.class));
    }

    @Test
    public void testDispatchWhenEventModeDisabled() {
        brokerConfig.setEnableLiteEventMode(false);
        liteEventDispatcher.dispatch("group", "lmqName", 0, 0L, 0L);
        verify(liteSubscriptionRegistry, never()).getAllSubscriber(anyString(), anyString());
    }

    @Test
    public void testDispatchWhenQueueIdNotZero() {
        brokerConfig.setEnableLiteEventMode(true);
        liteEventDispatcher.dispatch("group", "lmqName", 1, 0L, 0L);
        verify(liteSubscriptionRegistry, never()).getAllSubscriber(anyString(), anyString());
    }

    @Test
    public void testDispatchCallsDoDispatch() {
        brokerConfig.setEnableLiteEventMode(true);
        String lmqName = LiteUtil.toLmqName("parentTopic", "lmqName");
        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);
        spyDispatcher.dispatch("group", lmqName, 0, 0L, 0L);
        verify(spyDispatcher).doDispatch("group", lmqName, null);
    }

    @Test
    public void testDoDispatchWhenWrapperIsNull() {
        brokerConfig.setEnableLiteEventMode(true);
        when(liteSubscriptionRegistry.getAllSubscriber("group", "lmqName")).thenReturn(null);

        // Use reflection to access private method
        try {
            java.lang.reflect.Method method = LiteEventDispatcher.class.getDeclaredMethod(
                "doDispatch", String.class, String.class, String.class);
            method.setAccessible(true);
            method.invoke(liteEventDispatcher, "group", "lmqName", null);
        } catch (Exception e) {
            fail("Exception should not be thrown");
        }

        verify(liteSubscriptionRegistry).getAllSubscriber("group", "lmqName");
    }

    @Test
    public void testDoDispatchWithListWrapper() {
        brokerConfig.setEnableLiteEventMode(true);

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setWildcardLiteGroup(false);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("group")).thenReturn(subscriptionGroupConfig);

        SubscriberWrapper.ListWrapper listWrapper = mock(SubscriberWrapper.ListWrapper.class);
        List<ClientGroup> clients = Collections.singletonList(new ClientGroup("clientId", "group"));
        when(listWrapper.asListWrapper()).thenReturn(listWrapper);
        when(listWrapper.getClients()).thenReturn(clients);
        when(liteSubscriptionRegistry.getAllSubscriber("group", "lmqName")).thenReturn(listWrapper);

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);
        spyDispatcher.doDispatch("group", "lmqName", null);
        verify(spyDispatcher).selectAndDispatch("lmqName", clients, null);
    }

    @Test
    public void testDoDispatchWithMapWrapper() {
        brokerConfig.setEnableLiteEventMode(true);

        SubscriberWrapper.MapWrapper mapWrapper = mock(SubscriberWrapper.MapWrapper.class);
        Map<String, List<ClientGroup>> groupMap = new HashMap<>();
        groupMap.put("key", Collections.singletonList(new ClientGroup("clientId", "group")));
        when(mapWrapper.getGroupMap()).thenReturn(groupMap);
        when(mapWrapper.asMapWrapper()).thenReturn(mapWrapper);
        when(liteSubscriptionRegistry.getAllSubscriber("group", "lmqName")).thenReturn(mapWrapper);

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);

        spyDispatcher.doDispatch("group", "lmqName", null);

        verify(spyDispatcher).selectAndDispatch(eq("lmqName"), anyList(), eq(null));
    }

    @Test
    public void testSelectAndDispatchWhenClientsEmpty() {
        List<ClientGroup> clients = new ArrayList<>();
        boolean result = liteEventDispatcher.selectAndDispatch("lmqName", clients, null);
        assertTrue(result);
    }

    @Test
    public void testSelectAndDispatchWhenEventModeDisabled() {
        brokerConfig.setEnableLiteEventMode(false);
        List<ClientGroup> clients = Collections.singletonList(new ClientGroup("clientId", "group"));
        boolean result = liteEventDispatcher.selectAndDispatch("lmqName", clients, null);
        assertTrue(result);
    }

    @Test
    public void testSelectAndDispatchSelectsClientAndDispatches() {
        brokerConfig.setEnableLiteEventMode(true);
        List<ClientGroup> clients = Collections.singletonList(new ClientGroup("clientId", "group"));

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);
        doReturn(true).when(spyDispatcher).tryDispatchToClient(anyString(), anyString(), anyString(), anyBoolean());

        boolean result = spyDispatcher.selectAndDispatch("lmqName", clients, null);
        assertTrue(result);
        verify(spyDispatcher).tryDispatchToClient("lmqName", "clientId", "group", true);
    }

    @Test
    public void testSelectAndDispatchExcludesSpecifiedClient() {
        brokerConfig.setEnableLiteEventMode(true);
        List<ClientGroup> clients = Arrays.asList(
            new ClientGroup("excludeId", "group"),
            new ClientGroup("clientId", "group")
        );

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);
        doReturn(true).when(spyDispatcher).tryDispatchToClient(anyString(), anyString(), anyString(), anyBoolean());

        boolean result = spyDispatcher.selectAndDispatch("lmqName", clients, "excludeId");
        assertTrue(result);
        verify(spyDispatcher).tryDispatchToClient("lmqName", "clientId", "group", true);
        verify(spyDispatcher, never()).tryDispatchToClient("lmqName", "excludeId", "group", true);
    }

    @Test
    public void testTryDispatchToClientWhenQueueHasSpace() {
        String clientId = "clientId";
        String group = "group";
        String lmqName = "lmqName";

        // Create a real ClientEventSet for testing
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);

        // Mock the clientEventMap to return our eventSet
        liteEventDispatcher.clientEventMap.put(clientId, eventSet);

        boolean result = liteEventDispatcher.tryDispatchToClient(lmqName, clientId, group, true);
        assertTrue(result);
        assertEquals(1, eventSet.size());
    }

    @Test
    public void testTryDispatchToClientWhenQueueIsFull() {
        String clientId = "clientId";
        String group = "group";
        String lmqName = "lmqName";

        // Create a ClientEventSet with capacity 1
        LiteEventDispatcher.ClientEventSet eventSet = mock(LiteEventDispatcher.ClientEventSet.class);
        when(eventSet.offer(lmqName)).thenReturn(false);

        liteEventDispatcher.clientEventMap.put(clientId, eventSet);

        boolean result = liteEventDispatcher.tryDispatchToClient(lmqName, clientId, group, true);
        assertFalse(result);
        verify(eventSet).offer(lmqName);
    }

    @Test
    public void testGetEventIteratorInEventMode() {
        brokerConfig.setEnableLiteEventMode(true);
        String clientId = "clientId";
        String group = "group";

        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);
        liteEventDispatcher.clientEventMap.put(clientId, eventSet);

        Iterator<String> iterator = liteEventDispatcher.getEventIterator(clientId);
        assertNotNull(iterator);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetEventIteratorWhenNotInEventMode() {
        brokerConfig.setEnableLiteEventMode(false);
        String clientId = "clientId";
        LiteSubscription subscription = mock(LiteSubscription.class);
        Set<String> topicSet = new HashSet<>();
        topicSet.add("topic1");
        when(subscription.getLiteTopicSet()).thenReturn(topicSet);
        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);

        Iterator<String> iterator = liteEventDispatcher.getEventIterator(clientId);
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertEquals("topic1", iterator.next());
    }

    @Test
    public void testDoFullDispatchForClientWhenSubscriptionIsNull() {
        brokerConfig.setEnableLiteEventMode(true);
        String clientId = "clientId";
        String group = "group";

        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(null);

        liteEventDispatcher.doFullDispatchForClient(clientId, group);
        verify(liteSubscriptionRegistry).getLiteSubscription(clientId);
    }

    @Test
    public void testDoFullDispatchForClientWhenSubscriptionHasNoTopics() {
        brokerConfig.setEnableLiteEventMode(true);
        String clientId = "clientId";
        String group = "group";

        LiteSubscription subscription = mock(LiteSubscription.class);
        when(subscription.getLiteTopicSet()).thenReturn(Collections.emptySet());
        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);

        liteEventDispatcher.doFullDispatchForClient(clientId, group);
        verify(liteSubscriptionRegistry).getLiteSubscription(clientId);
    }

    @Test
    public void testScheduleFullDispatchForClientAddsRequestToSet() {
        String clientId = "clientId";
        String group = "group";
        long delayTime = 1000L;

        liteEventDispatcher.scheduleFullDispatchForClient(clientId, group, delayTime);

        assertEquals(1, liteEventDispatcher.fullDispatchSet.size());
        assertEquals(1, liteEventDispatcher.fullDispatchMap.size());
        assertTrue(liteEventDispatcher.fullDispatchMap.containsKey(clientId));
    }

    @Test
    public void testScheduleFullDispatchForClientDoesNotAddDuplicate() {
        String clientId = "clientId";
        String group = "group";
        long delayTime = 1000L;

        liteEventDispatcher.scheduleFullDispatchForClient(clientId, group, delayTime);
        liteEventDispatcher.scheduleFullDispatchForClient(clientId, group, delayTime);

        assertEquals(1, liteEventDispatcher.fullDispatchSet.size());
        assertEquals(1, liteEventDispatcher.fullDispatchMap.size());
    }

    @Test
    public void testScheduleFullDispatchForWildcardGroup() {
        String group = "group";
        long delayTime = 1000L;

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);
        spyDispatcher.scheduleFullDispatchForWildcardGroup(group, delayTime);

        verify(spyDispatcher).scheduleFullDispatchForClient("$group$", group, delayTime);
    }

    @Test
    public void testClientEventSetOffer() {
        String group = "group";
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);

        boolean result = eventSet.offer("event");
        assertTrue(result);
        assertEquals(1, eventSet.size());
    }

    @Test
    public void testClientEventSetPoll() {
        String group = "group";
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);

        eventSet.offer("event");
        String result = eventSet.poll();
        assertEquals("event", result);
        assertEquals(0, eventSet.size());
    }

    @Test
    public void testClientEventSetMaybeBlock() {
        String group = "group";
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);

        // Initially should not block
        assertFalse(eventSet.maybeBlock());

        // After adding an event and waiting, should block
        eventSet.offer("event");
        // Simulate time passing by manipulating lastAccessTime
        try {
            // Use reflection to access private field
            java.lang.reflect.Field lastAccessTimeField =
                LiteEventDispatcher.ClientEventSet.class.getDeclaredField("lastAccessTime");
            lastAccessTimeField.setAccessible(true);
            lastAccessTimeField.setLong(eventSet, System.currentTimeMillis() -
                LiteEventDispatcher.CLIENT_LONG_POLLING_INTERVAL - 1000);
        } catch (Exception e) {
            fail("Failed to manipulate lastAccessTime");
        }

        assertTrue(eventSet.maybeBlock());
    }

    @Test
    public void testClientEventSetIsLowWaterMark() {
        String group = "group";
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);

        // Empty queue should be low water mark
        assertTrue(eventSet.isLowWaterMark());

        // Add events to exceed low water mark
        for (int i = 0; i < (int) (LiteEventDispatcher.LOW_WATER_MARK * 100) + 1; i++) {
            eventSet.offer("event" + i);
        }

        // Should no longer be low water mark
        assertFalse(eventSet.isLowWaterMark());
    }

    @Test
    public void testClientEventSetIsActiveConsuming() {
        String group = "group";
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);

        // Initially should be active consuming
        assertTrue(eventSet.isActiveConsuming());

        // Simulate time passing
        try {
            java.lang.reflect.Field lastAccessTimeField =
                LiteEventDispatcher.ClientEventSet.class.getDeclaredField("lastAccessTime");
            lastAccessTimeField.setAccessible(true);
            lastAccessTimeField.setLong(eventSet, System.currentTimeMillis() -
                LiteEventDispatcher.ACTIVE_CONSUMING_WINDOW - 1000);
        } catch (Exception e) {
            fail("Failed to manipulate lastAccessTime");
        }

        // Should no longer be active consuming
        assertFalse(eventSet.isActiveConsuming());
    }

    @Test
    public void testEventSetIteratorHasNextAndNext() {
        String group = "group";
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);
        eventSet.offer("event1");
        eventSet.offer("event2");

        LiteEventDispatcher.EventSetIterator iterator = new LiteEventDispatcher.EventSetIterator(eventSet);

        assertTrue(iterator.hasNext());
        assertEquals("event1", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("event2", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testLiteSubscriptionIteratorHasNextAndNext() {
        Set<String> topics = new HashSet<>();
        topics.add("topic1");
        topics.add("topic2");
        Iterator<String> topicIterator = topics.iterator();

        LiteEventDispatcher.LiteSubscriptionIterator iterator =
            new LiteEventDispatcher.LiteSubscriptionIterator("parentTopic", topicIterator);

        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testComparatorComparesTimestampsCorrectly() {
        String clientId1 = "clientId1";
        String clientId2 = "clientId2";
        String group = "group";

        LiteEventDispatcher.FullDispatchRequest request1 =
            new LiteEventDispatcher.FullDispatchRequest(clientId1, group, 1000L);
        LiteEventDispatcher.FullDispatchRequest request2 =
            new LiteEventDispatcher.FullDispatchRequest(clientId2, group, 2000L);

        assertTrue(LiteEventDispatcher.COMPARATOR.compare(request1, request2) < 0);
        assertTrue(LiteEventDispatcher.COMPARATOR.compare(request2, request1) > 0);
        assertEquals(0, LiteEventDispatcher.COMPARATOR.compare(request1, request1));
    }

    @Test
    public void testLiteCtlListenerImplOnRegisterForWildcardGroup() throws NoSuchFieldException, IllegalAccessException {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setWildcardLiteGroup(true);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("group")).thenReturn(subscriptionGroupConfig);

        LiteEventDispatcher.LiteCtlListenerImpl listener =
            liteEventDispatcher.new LiteCtlListenerImpl();

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);

        // Replace the dispatcher in the listener
        java.lang.reflect.Field outerField = listener.getClass().getDeclaredField("this$0");
        outerField.setAccessible(true);
        outerField.set(listener, spyDispatcher);

        listener.onRegister("clientId", "group", "lmqName");

        verify(spyDispatcher).scheduleFullDispatchForWildcardGroup("group", 5000L);
    }

    @Test
    public void testLiteCtlListenerImplOnRegisterForRegularGroupWithExistingLMQ() throws NoSuchFieldException, IllegalAccessException {
        when(liteLifecycleManager.isLmqExist("lmqName")).thenReturn(true);

        LiteEventDispatcher.LiteCtlListenerImpl listener =
            liteEventDispatcher.new LiteCtlListenerImpl();

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);

        // Replace the dispatcher in the listener
        java.lang.reflect.Field outerField = listener.getClass().getDeclaredField("this$0");
        outerField.setAccessible(true);
        outerField.set(listener, spyDispatcher);

        listener.onRegister("clientId", "group", "lmqName");

        verify(spyDispatcher).doDispatch("group", "lmqName", null);

    }

    @Test
    public void testLiteCtlListenerImplOnRemoveAllRemovesClientAndRedispatchesEvents() {
        String clientId = "clientId";
        String group = "group";

        // Add a client event set with an event
        LiteEventDispatcher.ClientEventSet eventSet = liteEventDispatcher.new ClientEventSet(group);
        eventSet.offer("lmqName");
        liteEventDispatcher.clientEventMap.put(clientId, eventSet);

        LiteEventDispatcher.LiteCtlListenerImpl listener =
            liteEventDispatcher.new LiteCtlListenerImpl();

        LiteEventDispatcher spyDispatcher = Mockito.spy(liteEventDispatcher);

        // Replace the dispatcher in the listener
        try {
            java.lang.reflect.Field outerField = listener.getClass().getDeclaredField("this$0");
            outerField.setAccessible(true);
            outerField.set(listener, spyDispatcher);

            listener.onRemoveAll(clientId, group);

            // Verify client was removed
            assertNull(liteEventDispatcher.clientEventMap.get(clientId));

            // Verify doDispatch was called
            verify(spyDispatcher).doDispatch(group, "lmqName", clientId);
        } catch (Exception e) {
            fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    @Test
    public void testDoFullDispatchForClientNormalCase() {
        String clientId = "testClientId";
        String group = "testGroup";
        String lmqName = "testLmq";
        brokerConfig.setEnableLiteEventMode(true);

        LiteSubscription subscription = new LiteSubscription();
        Set<String> topics = new HashSet<>();
        topics.add(lmqName);
        subscription.setLiteTopicSet(topics);

        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);
        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(100L);
        when(consumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(50L);

        LiteEventDispatcher.ClientEventSet eventSet = spy(liteEventDispatcher.new ClientEventSet(group));
        when(eventSet.maybeBlock()).thenReturn(false);
        when(eventSet.isLowWaterMark()).thenReturn(true);
        when(eventSet.offer(lmqName)).thenReturn(true);

        liteEventDispatcher.clientEventMap.put(clientId, eventSet);

        liteEventDispatcher.doFullDispatchForClient(clientId, group);

        verify(liteSubscriptionRegistry).getLiteSubscription(clientId);
        verify(liteLifecycleManager).getMaxOffsetInQueue(lmqName);
        verify(consumerOffsetManager).queryOffset(group, lmqName, 0);
        verify(eventSet).offer(lmqName);
    }

    @Test
    public void testScan_FullDispatch() {
        LiteEventDispatcher.FullDispatchRequest request =
            new LiteEventDispatcher.FullDispatchRequest("testClientId", "testGroup", -1000);
        liteEventDispatcher.fullDispatchSet.add(request);
        liteEventDispatcher.scan();
        assertTrue(liteEventDispatcher.fullDispatchSet.isEmpty());
    }
}