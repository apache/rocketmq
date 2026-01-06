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

import com.google.common.cache.Cache;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PopLiteLongPollingService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.processor.PopLiteMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.apache.rocketmq.broker.lite.LiteEventDispatcher.COMPARATOR;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class LiteEventDispatcherTest {

    @Mock
    private BrokerController brokerController;
    @Mock
    private LiteSubscriptionRegistry liteSubscriptionRegistry;
    @Mock
    private AbstractLiteLifecycleManager liteLifecycleManager;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private PopLiteMessageProcessor popLiteMessageProcessor;
    @Mock
    private PopLiteLongPollingService popLiteLongPollingService;
    @Mock
    private ConsumerOrderInfoManager consumerOrderInfoManager;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    private BrokerConfig brokerConfig;
    private LiteEventDispatcher liteEventDispatcher;
    private ConcurrentMap<String, LiteEventDispatcher.ClientEventSet> clientEventMap;
    private Cache<String, Object> blacklist;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws IllegalAccessException {
        brokerConfig = new BrokerConfig();
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getPopLiteMessageProcessor()).thenReturn(popLiteMessageProcessor);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(popLiteMessageProcessor.getPopLiteLongPollingService()).thenReturn(popLiteLongPollingService);
        when(popLiteMessageProcessor.getConsumerOrderInfoManager()).thenReturn(consumerOrderInfoManager);

        LiteEventDispatcher testObject = new LiteEventDispatcher(brokerController, liteSubscriptionRegistry, liteLifecycleManager);
        liteEventDispatcher = Mockito.spy(testObject);
        liteEventDispatcher.init();

        clientEventMap = (ConcurrentMap<String, LiteEventDispatcher.ClientEventSet>)
            FieldUtils.readDeclaredField(testObject, "clientEventMap", true);
        blacklist = (Cache<String, Object>) FieldUtils.readDeclaredField(testObject, "blacklist", true);
    }

    @After
    public void reset() {
        brokerConfig = new BrokerConfig();
        clientEventMap.clear();
        blacklist.invalidateAll();
    }

    @Test
    public void testFullDispatchRequestComparator() {
        LiteEventDispatcher.FullDispatchRequest request1 =
            new LiteEventDispatcher.FullDispatchRequest("client1", "whatever", 1000);
        LiteEventDispatcher.FullDispatchRequest request2 =
            new LiteEventDispatcher.FullDispatchRequest("client2", "whatever", 2000);
        LiteEventDispatcher.FullDispatchRequest request3 =
            new LiteEventDispatcher.FullDispatchRequest("client1", "whatever", 1000);

        Assert.assertTrue(COMPARATOR.compare(request1, request2) < 0);
        Assert.assertTrue(COMPARATOR.compare(request2, request1) > 0);
        Assert.assertEquals(0, COMPARATOR.compare(request1, request3));
    }

    @Test
    public void testFullDispatchSet() {
        ConcurrentSkipListSet<LiteEventDispatcher.FullDispatchRequest> set =
            new ConcurrentSkipListSet<>(COMPARATOR);

        LiteEventDispatcher.FullDispatchRequest request1 =
            new LiteEventDispatcher.FullDispatchRequest("client1", "whatever", 1000);
        LiteEventDispatcher.FullDispatchRequest request2 =
            new LiteEventDispatcher.FullDispatchRequest("client2", "whatever", 2000);
        LiteEventDispatcher.FullDispatchRequest request3 =
            new LiteEventDispatcher.FullDispatchRequest("client1", "whatever", 1000);
        LiteEventDispatcher.FullDispatchRequest request4 =
            new LiteEventDispatcher.FullDispatchRequest("client3", "whatever", 500);
        LiteEventDispatcher.FullDispatchRequest request5 =
            new LiteEventDispatcher.FullDispatchRequest("client4", "whatever", 1000);
        LiteEventDispatcher.FullDispatchRequest request6 =
            new LiteEventDispatcher.FullDispatchRequest(null, "whatever", 1000);

        set.add(request1);
        set.add(request3);
        set.add(request6);
        Assert.assertEquals(1, set.size());
        Assert.assertEquals(request1, set.pollFirst());

        set.clear();
        set.add(request1);
        set.add(request2);
        set.add(request3);
        set.add(request4);
        set.add(request5);
        Assert.assertEquals(4, set.size());
        Assert.assertEquals(request4, set.pollFirst());
        Assert.assertEquals(request1, set.pollFirst());
        Assert.assertEquals(request5, set.pollFirst());
        Assert.assertEquals(request2, set.pollFirst());
    }

    @Test
    public void testEventSetIterator() {
        LiteEventDispatcher.ClientEventSet clientEventSet = liteEventDispatcher.new ClientEventSet("group");
        clientEventSet.offer("event1");
        clientEventSet.offer("event2");

        LiteEventDispatcher.EventSetIterator iterator = new LiteEventDispatcher.EventSetIterator(clientEventSet);

        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("event1", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("event2", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testLiteSubscriptionIterator() {
        Iterator<String> topicIterator = Arrays.asList("event1", "event2").iterator();

        LiteEventDispatcher.LiteSubscriptionIterator iterator =
            new LiteEventDispatcher.LiteSubscriptionIterator("parentTopic", topicIterator);

        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("event1", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("event2", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testClientEventSet_offerAndPoll() {
        brokerConfig.setMaxClientEventCount(3);
        LiteEventDispatcher.ClientEventSet clientEventSet = liteEventDispatcher.new ClientEventSet("group");

        Assert.assertTrue(clientEventSet.offer("event1"));
        Assert.assertTrue(clientEventSet.offer("event2"));
        Assert.assertTrue(clientEventSet.offer("event1"));
        Assert.assertTrue(clientEventSet.offer("event3"));
        Assert.assertFalse(clientEventSet.offer("event4"));

        Assert.assertEquals(3, clientEventSet.size());
        Assert.assertEquals("event1", clientEventSet.poll());
        Assert.assertEquals("event2", clientEventSet.poll());
        Assert.assertEquals("event3", clientEventSet.poll());
        Assert.assertEquals(0, clientEventSet.size());
        Assert.assertNull(clientEventSet.poll());
    }

    @Test
    public void testClientEventSet_isLowWaterMark() {
        brokerConfig.setMaxClientEventCount(10);
        LiteEventDispatcher.ClientEventSet clientEventSet = liteEventDispatcher.new ClientEventSet("group");
        Assert.assertTrue(clientEventSet.isLowWaterMark());

        for (int i = 0; i < 4; i++) {
            clientEventSet.offer("event" + i);
        }
        Assert.assertFalse(clientEventSet.isLowWaterMark());
    }

    @Test
    public void testClientEventSetMaybeBlock() throws Exception {
        LiteEventDispatcher.ClientEventSet clientEventSet = liteEventDispatcher.new ClientEventSet("group");
        Assert.assertFalse(clientEventSet.maybeBlock());

        clientEventSet.offer("event");
        FieldUtils.writeDeclaredField(clientEventSet, "lastAccessTime", 0L, true);
        Assert.assertTrue(clientEventSet.maybeBlock());
        clientEventSet.poll();
        Assert.assertFalse(clientEventSet.maybeBlock());
    }

    @Test
    public void testGetAllSubscriber_noSubscribers() {
        when(liteSubscriptionRegistry.getSubscriber("event")).thenReturn(null);
        Object result = liteEventDispatcher.getAllSubscriber("group", "event");
        Assert.assertNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetAllSubscriber_singleSubscriber() {
        Set<ClientGroup> subscribers = new HashSet<>();
        subscribers.add(new ClientGroup("clientId", "group"));
        when(liteSubscriptionRegistry.getSubscriber("event")).thenReturn(subscribers);

        Object result = liteEventDispatcher.getAllSubscriber("group", "event"); // specified
        Assert.assertTrue(result instanceof List);
        Assert.assertEquals(1, ((List<?>) result).size());
        Assert.assertEquals("clientId", ((List<ClientGroup>) result).get(0).clientId);

        result = liteEventDispatcher.getAllSubscriber(null, "event"); // not specified
        Assert.assertTrue(result instanceof List);
        Assert.assertEquals(1, ((List<?>) result).size());
        Assert.assertEquals("clientId", ((List<ClientGroup>) result).get(0).clientId);

        result = liteEventDispatcher.getAllSubscriber("otherGroup", "event"); // specified but not match
        Assert.assertNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetAllSubscriber_multipleSubscribers() {
        Set<ClientGroup> subscribers = new HashSet<>();
        subscribers.add(new ClientGroup("clientId1", "group1"));
        subscribers.add(new ClientGroup("clientId2", "group1"));
        subscribers.add(new ClientGroup("clientId3", "group2"));
        when(liteSubscriptionRegistry.getSubscriber("event")).thenReturn(subscribers);

        Object result = liteEventDispatcher.getAllSubscriber("group1", "event"); // specified
        Assert.assertTrue(result instanceof List);
        Assert.assertEquals(2, ((List<?>) result).size());
        Assert.assertEquals("clientId1", ((List<ClientGroup>) result).get(0).clientId);

        result = liteEventDispatcher.getAllSubscriber("group2", "event"); // specified
        Assert.assertTrue(result instanceof List);
        Assert.assertEquals(1, ((List<?>) result).size());
        Assert.assertEquals("clientId3", ((List<ClientGroup>) result).get(0).clientId);

        result = liteEventDispatcher.getAllSubscriber("otherGroup", "event"); // specified but not match
        Assert.assertNull(result);

        result = liteEventDispatcher.getAllSubscriber(null, "event"); // not specified
        Assert.assertTrue(result instanceof Map);
        Assert.assertEquals(2, ((Map<?, ?>) result).size());
        Assert.assertEquals(2, ((Map<String, List<ClientGroup>>) result).get("group1").size());
        Assert.assertEquals(1, ((Map<String, List<ClientGroup>>) result).get("group2").size());
    }

    @Test
    public void testTryDispatchToClient() {
        brokerConfig.setMaxClientEventCount(1);
        String clientId = "clientId";

        boolean result = liteEventDispatcher.tryDispatchToClient("event1", clientId, "group");
        Assert.assertTrue(result);

        // not in blacklist
        result = liteEventDispatcher.tryDispatchToClient("event2", clientId, "group");
        Assert.assertFalse(result);
        verify(liteEventDispatcher).scheduleFullDispatch(clientId, "group", false);

        // in blacklist
        blacklist.put(clientId, Boolean.TRUE);
        result = liteEventDispatcher.tryDispatchToClient("event3", clientId, "group");
        Assert.assertFalse(result);
        verify(liteEventDispatcher).scheduleFullDispatch(clientId, "group", true);

        blacklist.invalidate(clientId);
        result = liteEventDispatcher.tryDispatchToClient("event3", clientId, "group");
        Assert.assertFalse(result);
        verify(liteEventDispatcher, times(2)).scheduleFullDispatch(clientId, "group", false);
    }

    @Test
    public void testSelectAndDispatch_empty_or_singleClient() {
        List<ClientGroup> clients = Collections.singletonList(new ClientGroup("client", "group"));
        // disable event mode
        brokerConfig.setEnableLiteEventMode(false);
        liteEventDispatcher.selectAndDispatch("event", clients, null);
        verify(liteEventDispatcher, never()).tryDispatchToClient(anyString(), anyString(), anyString());

        // empty list
        liteEventDispatcher.selectAndDispatch("event", Collections.emptyList(), null);
        verify(liteEventDispatcher, never()).tryDispatchToClient(anyString(), anyString(), anyString());

        // event mode
        brokerConfig.setMaxClientEventCount(2);
        brokerConfig.setEnableLiteEventMode(true);

        liteEventDispatcher.selectAndDispatch("event1", clients, null);
        liteEventDispatcher.selectAndDispatch("event2", clients, "client"); // exclude
        liteEventDispatcher.selectAndDispatch("event3", clients, null);
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving("client", true, 0, "group");
    }

    @Test
    public void testSelectAndDispatch_multipleClients() {
        brokerConfig.setMaxClientEventCount(2);
        String client1 = UUID.randomUUID().toString();
        String client2 = UUID.randomUUID().toString();
        List<ClientGroup> clients = Arrays.asList(
            new ClientGroup(client1, "group"),
            new ClientGroup(client2, "group"));

        // no fallback
        liteEventDispatcher.selectAndDispatch("event1", clients, client1);
        verify(popLiteLongPollingService).notifyMessageArriving(client2, true, 0, "group");

        // no fallback
        liteEventDispatcher.selectAndDispatch("event2", clients, client2);
        verify(popLiteLongPollingService).notifyMessageArriving(client1, true, 0, "group");

        // fallback
        blacklist.put(client1, Boolean.TRUE);
        liteEventDispatcher.selectAndDispatch("event3", clients, null);
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving(client2, true, 0, "group");

        // fallback
        blacklist.invalidate(client1);
        blacklist.put(client2, Boolean.TRUE);
        liteEventDispatcher.selectAndDispatch("event4", clients, null);
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving(client1, true, 0, "group");

        // queue all full
        liteEventDispatcher.selectAndDispatch("event5", clients, null);
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving(client1, true, 0, "group");
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving(client2, true, 0, "group");
    }

    @Test
    public void testDispatch() {
        // disable event mode
        brokerConfig.setEnableLiteEventMode(false);
        liteEventDispatcher.dispatch("group", "event", 0, 0, System.currentTimeMillis());
        verify(liteEventDispatcher, never()).getAllSubscriber(anyString(), anyString());

        // event mode
        brokerConfig.setEnableLiteEventMode(true);
        liteEventDispatcher.dispatch("group", "event", 1, 0, System.currentTimeMillis()); // queue id not match
        liteEventDispatcher.dispatch("group", "event", 0, 0, System.currentTimeMillis()); // queue name not match
        verify(liteEventDispatcher, never()).getAllSubscriber(anyString(), anyString());

        // do dispatch
        liteEventDispatcher.dispatch("group", LiteUtil.toLmqName("p", "l"), 0, 0, System.currentTimeMillis());
        verify(liteEventDispatcher).getAllSubscriber(anyString(), anyString());
    }

    @Test
    public void testDoFullDispatch_disable_or_emptySubscription() {
        String clientId = "clientId";
        String group = "group";

        // disable event mode
        brokerConfig.setEnableLiteEventMode(false);
        liteEventDispatcher.doFullDispatch(clientId, group);
        verify(liteSubscriptionRegistry, never()).getLiteSubscription(clientId);

        // empty subscription
        brokerConfig.setEnableLiteEventMode(true);
        when(liteSubscriptionRegistry.getLiteSubscription("clientId")).thenReturn(null);
        liteEventDispatcher.doFullDispatch(clientId, group);
        verify(liteLifecycleManager, never()).getMaxOffsetInQueue(anyString());
    }

    @Test
    public void testDoFullDispatch_maybeBlock() throws Exception {
        int num = 10;
        String clientId = "clientId";
        String group = "group";
        LiteSubscription subscription = new LiteSubscription();
        subscription.setTopic("parentTopic");
        for (int i = 0; i < num; i++) {
            subscription.addLiteTopic(LiteUtil.toLmqName(subscription.getTopic(), "l" + i));
        }
        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);

        // maybe block
        liteEventDispatcher.tryDispatchToClient("event", clientId, group);
        Assert.assertNotNull(clientEventMap.get(clientId));
        FieldUtils.writeDeclaredField(clientEventMap.get(clientId), "lastAccessTime", 0L, true);
        liteEventDispatcher.doFullDispatch(clientId, group);
        verify(liteEventDispatcher).scheduleFullDispatch(clientId, group, true);
        verify(liteLifecycleManager, never()).getMaxOffsetInQueue(anyString());
    }

    @Test
    public void testDoFullDispatch_highWaterMark() throws Exception {
        int num = 10;
        String clientId = "clientId";
        String group = "group";
        LiteSubscription subscription = new LiteSubscription();
        subscription.setTopic("parentTopic");
        for (int i = 0; i < num; i++) {
            subscription.addLiteTopic(LiteUtil.toLmqName(subscription.getTopic(), "l" + i));
        }
        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);

        brokerConfig.setMaxClientEventCount(1);

        // active consuming
        liteEventDispatcher.tryDispatchToClient("event", clientId, group);
        liteEventDispatcher.doFullDispatch(clientId, group);

        verify(liteEventDispatcher).scheduleFullDispatch(clientId, group, false);
        verify(liteLifecycleManager, never()).getMaxOffsetInQueue(anyString());

        // not active consuming
        clientEventMap.clear();
        liteEventDispatcher.tryDispatchToClient("event", clientId, group);
        FieldUtils.writeDeclaredField(clientEventMap.get(clientId), "lastAccessTime", System.currentTimeMillis() - 6000L, true);
        liteEventDispatcher.doFullDispatch(clientId, group);

        verify(liteEventDispatcher).scheduleFullDispatch(clientId, group, true);
        verify(liteLifecycleManager, never()).getMaxOffsetInQueue(anyString());
    }

    @Test
    public void testDoFullDispatch_multipleTopics() {
        String clientId = "clientId";
        String group = "group";

        String lmqName1 = "lmqName1";
        String lmqName2 = "lmqName2";
        String lmqName3 = "lmqName2";
        LiteSubscription subscription = new LiteSubscription();
        subscription.setTopic("parentTopic");
        subscription.addLiteTopic(lmqName1);
        subscription.addLiteTopic(lmqName2);
        subscription.addLiteTopic(lmqName3);
        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);


        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName1)).thenReturn(0L);

        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName2)).thenReturn(10L);
        when(consumerOffsetManager.queryOffset(group, lmqName2, 0)).thenReturn(10L);

        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName3)).thenReturn(10L);
        when(consumerOffsetManager.queryOffset(group, lmqName3, 0)).thenReturn(5L);

        liteEventDispatcher.doFullDispatch(clientId, group);

        verify(liteLifecycleManager).getMaxOffsetInQueue(lmqName1);
        verify(liteLifecycleManager).getMaxOffsetInQueue(lmqName2);
        verify(liteLifecycleManager).getMaxOffsetInQueue(lmqName3);
        verify(consumerOffsetManager, never()).queryOffset(group, lmqName1, 0);
        verify(consumerOffsetManager).queryOffset(group, lmqName2, 0);
        verify(consumerOffsetManager).queryOffset(group, lmqName3, 0);

        verify(liteEventDispatcher, never()).scheduleFullDispatch(clientId, group, true);
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving(clientId, true, 0, group);
    }

    @Test
    public void testDoFullDispatch_eventQueueFull() throws IllegalAccessException {
        brokerConfig.setMaxClientEventCount(2);
        String clientId = "clientId";
        String group = "group";

        String lmqName1 = "lmqName1";
        String lmqName2 = "lmqName2";
        String lmqName3 = "lmqName3";
        LiteSubscription subscription = new LiteSubscription();
        subscription.setTopic("parentTopic");
        subscription.addLiteTopic(lmqName1);
        subscription.addLiteTopic(lmqName2);
        subscription.addLiteTopic(lmqName3);
        when(liteSubscriptionRegistry.getLiteSubscription(clientId)).thenReturn(subscription);

        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName1)).thenReturn(10L);
        when(consumerOffsetManager.queryOffset(group, lmqName1, 0)).thenReturn(5L);

        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName2)).thenReturn(10L);
        when(consumerOffsetManager.queryOffset(group, lmqName2, 0)).thenReturn(5L);

        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName3)).thenReturn(10L);
        when(consumerOffsetManager.queryOffset(group, lmqName3, 0)).thenReturn(5L);

        // active consuming
        liteEventDispatcher.doFullDispatch(clientId, group);
        verify(liteEventDispatcher).scheduleFullDispatch(clientId, group, false);
        verify(popLiteLongPollingService, times(2)).notifyMessageArriving(clientId, true, 0, group);
        Assert.assertNotNull(clientEventMap.get(clientId).poll());
        Assert.assertNotNull(clientEventMap.get(clientId).poll());

        // not active consuming
        FieldUtils.writeDeclaredField(clientEventMap.get(clientId), "lastAccessTime", System.currentTimeMillis() - 6000L, true);
        liteEventDispatcher.doFullDispatch(clientId, group);
        verify(liteEventDispatcher).scheduleFullDispatch(clientId, group, true);
        verify(popLiteLongPollingService, times(4)).notifyMessageArriving(clientId, true, 0, group);
    }

    @Test
    public void testDoFullDispatchByGroup() {
        String group = "group";
        String clientId1 = "client1";
        String clientId2 = "client2";
        List<String> clientIds = Arrays.asList(clientId1, clientId2);
        Mockito.when(liteSubscriptionRegistry.getAllClientIdByGroup(group)).thenReturn(clientIds);

        liteEventDispatcher.doFullDispatchByGroup(group);

        verify(liteSubscriptionRegistry, times(1)).getAllClientIdByGroup(group);
        verify(liteEventDispatcher, times(1)).doFullDispatch(clientId1, group);
        verify(liteEventDispatcher, times(1)).doFullDispatch(clientId2, group);
    }

    @Test
    public void testScan() throws Exception {
        String clientId = "clientId";
        String group = "group";
        String event = "event";
        liteEventDispatcher.tryDispatchToClient(event, clientId, group);

        Assert.assertNotNull(clientEventMap.get(clientId));
        FieldUtils.writeDeclaredField(clientEventMap.get(clientId), "lastAccessTime", 0L, true);
        liteEventDispatcher.scan();
        verify(liteEventDispatcher).getAllSubscriber(group, event);
    }

    @Test
    public void testFullDispatchDeduplication() throws InterruptedException {
        String clientId1 = "clientId1";
        String clientId2 = "clientId2";
        String group = "group";
        brokerConfig.setLiteEventFullDispatchDelayTime(10L);
        liteEventDispatcher.scheduleFullDispatch(clientId1, group, false);
        liteEventDispatcher.scheduleFullDispatch(clientId1, group, false);
        liteEventDispatcher.scheduleFullDispatch(clientId1, group, false);
        liteEventDispatcher.scheduleFullDispatch(clientId1, group, false);
        liteEventDispatcher.scheduleFullDispatch(clientId2, group, false);

        Thread.sleep(20L);
        liteEventDispatcher.scan();
        verify(liteEventDispatcher, times(1)).doFullDispatch(clientId1, group);
        verify(liteEventDispatcher, times(1)).doFullDispatch(clientId2, group);
    }
}
