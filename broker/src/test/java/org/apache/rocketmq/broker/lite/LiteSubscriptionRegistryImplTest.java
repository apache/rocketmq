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

import io.netty.channel.Channel;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.QueueLevelConsumerManager;
import org.apache.rocketmq.broker.processor.PopLiteMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.attribute.LiteSubModel;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.common.SubscriptionGroupAttributes.LITE_SUB_MODEL_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LiteSubscriptionRegistryImplTest {

    private LiteSubscriptionRegistryImpl registry;
    private LiteCtlListener mockListener;
    private AbstractLiteLifecycleManager mockLifecycleManager;
    private BrokerConfig mockBrokerConfig;
    private SubscriptionGroupManager mockSubscriptionGroupManager;
    private ConsumerOffsetManager mockConsumerOffsetManager;

    @Before
    public void setUp() {
        BrokerController mockBrokerController = mock(BrokerController.class);
        mockLifecycleManager = mock(AbstractLiteLifecycleManager.class);
        mockBrokerConfig = mock(BrokerConfig.class);
        mockSubscriptionGroupManager = mock(SubscriptionGroupManager.class);
        mockConsumerOffsetManager = mock(ConsumerOffsetManager.class);
        PopLiteMessageProcessor mockPopLiteMessageProcessor = mock(PopLiteMessageProcessor.class);
        QueueLevelConsumerManager mockConsumerOrderInfoManager = mock(QueueLevelConsumerManager.class);

        when(mockBrokerController.getBrokerConfig()).thenReturn(mockBrokerConfig);
        when(mockBrokerController.getSubscriptionGroupManager()).thenReturn(mockSubscriptionGroupManager);
        when(mockBrokerController.getConsumerOffsetManager()).thenReturn(mockConsumerOffsetManager);
        when(mockBrokerController.getPopLiteMessageProcessor()).thenReturn(mockPopLiteMessageProcessor);
        when(mockPopLiteMessageProcessor.getConsumerOrderInfoManager()).thenReturn(mockConsumerOrderInfoManager);
        when(mockConsumerOrderInfoManager.getTable()).thenReturn(new ConcurrentHashMap<>());
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1000L);
        when(mockBrokerConfig.getLiteSubscriptionCheckTimeoutMills()).thenReturn(60000L);
        when(mockBrokerConfig.getLiteSubscriptionCheckInterval()).thenReturn(10000L);

        registry = new LiteSubscriptionRegistryImpl(mockBrokerController, mockLifecycleManager);
        mockListener = mock(LiteCtlListener.class);
        registry.addListener(mockListener);
    }

    // Test addIncremental method
    @Test
    public void testAddPartialSubscription_BasicFunctionality() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add("lmq1");
        liteTopicSet.add("lmq2");

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertEquals(group, subscription.getGroup());
        assertEquals(topic, subscription.getTopic());
        assertTrue(subscription.getLiteTopicSet().containsAll(liteTopicSet));

        assertEquals(liteTopicSet.size(), registry.liteTopic2Group.size());
        Set<ClientGroup> topicGroupSet = registry.liteTopic2Group.get("lmq1");
        assertEquals(1, topicGroupSet.size());
        ClientGroup registeredGroup = topicGroupSet.iterator().next();
        assertEquals(clientId, registeredGroup.clientId);
        assertEquals(group, registeredGroup.group);

        verify(mockListener, times(2)).onRegister(eq(clientId), eq(group), anyString());
    }

    @Test
    public void testAddPartialSubscription_ExclusiveMode() {
        String existingClientId = "existingClient";
        String newClientId = "newClient";
        String group = "group";
        String topic = "topic";
        String liteTopic = "lmq1";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Mock subscription group config for reset offset behavior
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(group);
        subscriptionGroupConfig.getAttributes().put(LITE_SUB_MODEL_ATTRIBUTE.getName(), LiteSubModel.Exclusive.name());
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(subscriptionGroupConfig);

        // Add existing client
        registry.addPartialSubscription(existingClientId, group, topic, liteTopicSet, null);

        // Verify that the existing client is correctly registered
        LiteSubscription existingSubscription = registry.getLiteSubscription(existingClientId);
        assertNotNull(existingSubscription);
        assertTrue(existingSubscription.getLiteTopicSet().contains(liteTopic));

        // Execute exclusive mode addition
        Set<String> newLiteTopicSet = new HashSet<>();
        newLiteTopicSet.add(liteTopic);
        registry.addPartialSubscription(newClientId, group, topic, newLiteTopicSet, null);

        // Verify that new client subscription has been added.
        LiteSubscription newSubscription = registry.getLiteSubscription(newClientId);
        assertNotNull(newSubscription);
        assertTrue(newSubscription.getLiteTopicSet().contains(liteTopic));

        assertEquals(liteTopicSet.size(), registry.liteTopic2Group.size());
        Set<ClientGroup> topicGroupSet = registry.liteTopic2Group.get(liteTopic);
        assertEquals(1, topicGroupSet.size());
        ClientGroup registeredGroup = topicGroupSet.iterator().next();
        assertEquals(newClientId, registeredGroup.clientId);
        assertEquals(group, registeredGroup.group);

        verify(mockListener).onRegister(existingClientId, group, liteTopic);
        verify(mockListener).onRegister(newClientId, group, liteTopic);
        verify(mockListener).onUnregister(existingClientId, group, liteTopic);
    }

    @Test
    public void testAddPartialSubscription_NonExclusiveMode() {
        // Add an existing client subscription first
        String existingClientId = "existingClient";
        String newClientId = "newClient";
        String group = "group1";
        String topic = "topic1";
        String liteTopic = "lmq1";

        Set<String> existingLiteTopicSet = new HashSet<>();
        existingLiteTopicSet.add(liteTopic);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Mock subscription group config
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(subscriptionGroupConfig);

        // Add existing client
        registry.addPartialSubscription(existingClientId, group, topic, existingLiteTopicSet, null);

        // Add new client in non-exclusive mode
        Set<String> newLiteTopicSet = new HashSet<>();
        newLiteTopicSet.add(liteTopic);
        registry.addPartialSubscription(newClientId, group, topic, newLiteTopicSet, null);

        // Verify both client subscriptions exist
        LiteSubscription existingSubscription = registry.getLiteSubscription(existingClientId);
        LiteSubscription newSubscription = registry.getLiteSubscription(newClientId);
        assertNotNull(existingSubscription);
        assertNotNull(newSubscription);
        assertTrue(existingSubscription.getLiteTopicSet().contains(liteTopic));
        assertTrue(newSubscription.getLiteTopicSet().contains(liteTopic));

        // Verify listener was only called for registration, not unregistration
        verify(mockListener, times(2)).onRegister(anyString(), eq(group), eq(liteTopic));
        verify(mockListener, never()).onUnregister(anyString(), anyString(), anyString());
    }

    @Test
    public void testAddPartialSubscription_WithEmptyLiteTopicSet() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        Set<String> liteTopicSet = new HashSet<>();

        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertEquals(group, subscription.getGroup());
        assertEquals(topic, subscription.getTopic());
        assertTrue(subscription.getLiteTopicSet().isEmpty());

        // Verify listener was not called
        verify(mockListener, never()).onRegister(anyString(), anyString(), anyString());
    }

    @Test
    public void testAddPartialSubscription_InactiveSubscription() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        String inactiveLiteTopic = "inactive_lmq1";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(inactiveLiteTopic);

        // Mock inactive subscription
        when(mockLifecycleManager.isSubscriptionActive(topic, inactiveLiteTopic)).thenReturn(false);

        // Should not add inactive subscriptions
        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertFalse(subscription.getLiteTopicSet().contains(inactiveLiteTopic));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    @Test
    public void testAddPartialSubscription_ExclusiveModeDifferentGroups() {
        // Add two clients from different groups
        String client1 = "client1";
        String group1 = "group1";
        String client2 = "client2";
        String group2 = "group2";
        String topic = "topic1";
        String liteTopic = "lmq1";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Mock subscription group configs
        SubscriptionGroupConfig subscriptionGroupConfig1 = new SubscriptionGroupConfig();
        subscriptionGroupConfig1.setGroupName(group1);
        subscriptionGroupConfig1.getAttributes().put(LITE_SUB_MODEL_ATTRIBUTE.getName(), LiteSubModel.Exclusive.name());
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group1)).thenReturn(subscriptionGroupConfig1);

        SubscriptionGroupConfig subscriptionGroupConfig2 = new SubscriptionGroupConfig();
        subscriptionGroupConfig2.setGroupName(group2);
        subscriptionGroupConfig2.getAttributes().put(LITE_SUB_MODEL_ATTRIBUTE.getName(), LiteSubModel.Exclusive.name());
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group2)).thenReturn(subscriptionGroupConfig2);

        // Add first client
        registry.addPartialSubscription(client1, group1, topic, liteTopicSet, null);

        // Add second client
        registry.addPartialSubscription(client2, group2, topic, liteTopicSet, null);

        // Verify both clients are registered for the same topic
        Set<ClientGroup> observers = registry.getSubscriber(liteTopic);
        assertEquals(2, observers.size());

        // Add new client in exclusive mode from the same group as client1
        String client3 = "client3";
        registry.addPartialSubscription(client3, group1, topic, liteTopicSet, null);

        // Verify only client1 was removed (same group), client2 remains (different group)
        observers = registry.getSubscriber(liteTopic);
        assertEquals(2, observers.size()); // client2(group2) and client3(group1)

        boolean hasClient2 = false;
        boolean hasClient3 = false;
        for (ClientGroup cg : observers) {
            if (cg.clientId.equals(client2) && cg.group.equals(group2)) {
                hasClient2 = true;
            }
            if (cg.clientId.equals(client3) && cg.group.equals(group1)) {
                hasClient3 = true;
            }
        }

        assertTrue(hasClient2, "Client2 (group2) should still be registered");
        assertTrue(hasClient3, "Client3 (group1) should be registered");

        // Verify listener calls
        verify(mockListener).onUnregister(client1, group1, liteTopic); // Same group client1 removed
        verify(mockListener, never()).onUnregister(client2, group2, liteTopic); // Different group client2 retained
    }

    @Test
    public void testAddPartialSubscription_QuotaLimit() {
        // Set quota to 1
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1L);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Add first subscription
        String clientId1 = "client1";
        String group1 = "group1";
        String topic1 = "topic1";
        Set<String> liteTopicSet1 = new HashSet<>();
        liteTopicSet1.add("lmq1");

        registry.addPartialSubscription(clientId1, group1, topic1, liteTopicSet1, null);

        // Try to add second subscription, should throw exception
        String clientId2 = "client2";
        String group2 = "group2";
        String topic2 = "topic2";
        Set<String> liteTopicSet2 = new HashSet<>();
        liteTopicSet2.add("lmq2");

        assertThrows(LiteQuotaException.class, () -> {
            registry.addPartialSubscription(clientId2, group2, topic2, liteTopicSet2, null);
        });
    }

    // Test removeIncremental method
    @Test
    public void testRemovePartialSubscription() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        String liteTopic1 = "lmq1";
        String liteTopic2 = "lmq2";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic1);
        liteTopicSet.add(liteTopic2);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Add subscriptions first
        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);

        // Verify subscriptions were added
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic1));
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic2));

        // Remove some subscriptions
        Set<String> toRemove = new HashSet<>();
        toRemove.add(liteTopic1);
        registry.removePartialSubscription(clientId, group, topic, toRemove);

        // Verify removal was successful
        subscription = registry.getLiteSubscription(clientId);
        assertFalse(subscription.getLiteTopicSet().contains(liteTopic1));
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic2));

        verify(mockListener).onUnregister(clientId, group, liteTopic1);
        verify(mockListener, never()).onUnregister(clientId, group, liteTopic2);
    }

    // Test addAll method
    @Test
    public void testAddCompleteSubscription() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        String liteTopic1 = "lmq1";
        String liteTopic2 = "lmq2";
        String liteTopic3 = "lmq3";

        // Initial subscriptions
        Set<String> initialSet = new HashSet<>();
        initialSet.add(liteTopic1);
        initialSet.add(liteTopic2);

        // New full subscription set
        Set<String> newFullSet = new HashSet<>();
        newFullSet.add(liteTopic2);
        newFullSet.add(liteTopic3);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Add initial subscriptions
        registry.addPartialSubscription(clientId, group, topic, initialSet, null);

        // Reset mock to ignore previous interactions
        clearInvocations(mockListener);

        // Update with addAll
        registry.addCompleteSubscription(clientId, group, topic, newFullSet, 1L);

        // Verify update results
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertFalse(subscription.getLiteTopicSet().contains(liteTopic1)); // Should be removed
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic2));  // Should be retained
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic3));  // Should be added

        // Verify that liteTopic1 was unregistered (no longer in new set)
        verify(mockListener).onUnregister(clientId, group, liteTopic1);

        // Verify that liteTopic3 was registered (new in the set)
        verify(mockListener).onRegister(clientId, group, liteTopic3);

        // Verify that liteTopic2 was neither unregistered nor registered again
        // (it was already registered and remains in the new set)
        verify(mockListener, never()).onUnregister(clientId, group, liteTopic2);
    }

    // Test removeAll method
    @Test
    public void testRemoveCompleteSubscription() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        String liteTopic1 = "lmq1";
        String liteTopic2 = "lmq2";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic1);
        liteTopicSet.add(liteTopic2);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Add subscriptions
        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);

        // Verify subscriptions were added
        assertNotNull(registry.getLiteSubscription(clientId));
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Remove all subscriptions
        registry.removeCompleteSubscription(clientId);

        // Verify all subscriptions were removed
        assertNull(registry.getLiteSubscription(clientId));
        assertEquals(0, registry.getActiveSubscriptionNum());

        verify(mockListener).onRemoveAll(clientId, group);
    }

    @Test
    public void testRemoveCompleteSubscription_NonExistentClient() {
        String nonExistentClientId = "nonexistent";

        // Should not throw exception
        registry.removeCompleteSubscription(nonExistentClientId);

        // Verify no changes to registry state
        assertEquals(0, registry.getActiveSubscriptionNum());
        assertNull(registry.getLiteSubscription(nonExistentClientId));
    }

    // Test cleanSubscription method
    @Test
    public void testCleanSubscription() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        String liteTopic1 = "lmq1";
        String liteTopic2 = "lmq2";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic1);
        liteTopicSet.add(liteTopic2);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Add subscription
        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Verify subscription was added
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic1));
        assertTrue(subscription.getLiteTopicSet().contains(liteTopic2));

        // Clean subscription
        registry.cleanSubscription(liteTopic1, true);
        registry.cleanSubscription(liteTopic2, false);

        // Verify subscription was cleaned
        subscription = registry.getLiteSubscription(clientId);
        assertFalse(subscription.getLiteTopicSet().contains(liteTopic1));
        assertFalse(subscription.getLiteTopicSet().contains(liteTopic2));
        assertNull(registry.getSubscriber(liteTopic1));
        assertNull(registry.getSubscriber(liteTopic2));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    // Test getSubscriber method
    @Test
    public void testGetSubscriber() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        String liteTopic = "lmq1";

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        registry.addPartialSubscription(clientId, group, topic, liteTopicSet, null);

        Set<ClientGroup> observers = registry.getSubscriber(liteTopic);
        assertNotNull(observers);
        assertEquals(1, observers.size());
        ClientGroup clientGroup = observers.iterator().next();
        assertEquals(clientId, clientGroup.clientId);
        assertEquals(group, clientGroup.group);
    }

    @Test
    public void testGetSubscriber_NonExistentTopic() {
        String nonExistentTopic = "nonexistent_lmq";

        Set<ClientGroup> result = registry.getSubscriber(nonExistentTopic);

        // Should return null for non-existent topic
        assertNull(result);
    }

    // Test updateClientChannel method
    @Test
    public void testUpdateClientChannel() {
        String clientId = "client1";
        Channel mockChannel = mock(Channel.class);

        registry.updateClientChannel(clientId, mockChannel);

        // Verify channel was updated
        assertEquals(mockChannel, registry.clientChannels.get(clientId));
    }

    // Test getActiveSubscriptionNum method
    @Test
    public void testGetActiveSubscriptionNum() {
        String clientId1 = "client1";
        String clientId2 = "client2";
        String group = "group1";
        String topic = "topic1";
        String liteTopic1 = "lmq1";
        String liteTopic2 = "lmq2";

        Set<String> liteTopicSet1 = new HashSet<>();
        liteTopicSet1.add(liteTopic1);

        Set<String> liteTopicSet2 = new HashSet<>();
        liteTopicSet2.add(liteTopic1); // Same topic
        liteTopicSet2.add(liteTopic2); // New topic

        when(mockLifecycleManager.isSubscriptionActive(anyString(), anyString())).thenReturn(true);

        // Initial state
        assertEquals(0, registry.getActiveSubscriptionNum());

        // Add first client
        registry.addPartialSubscription(clientId1, group, topic, liteTopicSet1, null);
        assertEquals(1, registry.getActiveSubscriptionNum());

        // Add second client
        registry.addPartialSubscription(clientId2, group, topic, liteTopicSet2, null);
        assertEquals(3, registry.getActiveSubscriptionNum()); // 3 references: client1->topic1, client2->topic1, client2->topic2
    }

    // Test cleanupExpiredSubscriptions method
    @Test
    public void testCleanupExpiredSubscriptions_NoExpiredClients() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        Set<String> liteTopics = new HashSet<>();
        liteTopics.add("lmq1");
        liteTopics.add("lmq2");

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.setTopic(topic);
        subscription.addLiteTopic(liteTopics);
        subscription.setUpdateTime(System.currentTimeMillis()); // Not expired

        Channel channel = mock(Channel.class);

        registry.client2Subscription.put(clientId, subscription);
        registry.clientChannels.put(clientId, channel);

        // Initialize liteTopic2Group
        for (String lmq : liteTopics) {
            registry.liteTopic2Group.computeIfAbsent(lmq, k -> ConcurrentHashMap.newKeySet())
                .add(new ClientGroup(clientId, group));
        }

        registry.activeNum.set(liteTopics.size());

        // Perform cleanup with a timeout of 10 seconds
        registry.cleanupExpiredSubscriptions(10000);

        // Verify that the client has not been cleaned up
        assertNotNull(registry.client2Subscription.get(clientId));
        assertNotNull(registry.clientChannels.get(clientId));
        assertEquals(liteTopics.size(), registry.activeNum.get());
    }

    @Test
    public void testCleanupExpiredSubscriptions_WithExpiredClients() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        Set<String> liteTopics = new HashSet<>();
        liteTopics.add("lmq1");
        liteTopics.add("lmq2");

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.setTopic(topic);
        subscription.addLiteTopic(liteTopics);
        subscription.setUpdateTime(System.currentTimeMillis() - 20000);

        Channel channel = mock(Channel.class);

        registry.client2Subscription.put(clientId, subscription);
        registry.clientChannels.put(clientId, channel);

        // Initialize liteTopic2Group
        for (String lmq : liteTopics) {
            registry.liteTopic2Group.computeIfAbsent(lmq, k -> ConcurrentHashMap.newKeySet())
                .add(new ClientGroup(clientId, group));
        }

        registry.activeNum.set(liteTopics.size());

        LiteCtlListener mockListener = mock(LiteCtlListener.class);
        registry.addListener(mockListener);

        // Perform cleanup with a timeout of 10 seconds
        registry.cleanupExpiredSubscriptions(10000);

        // Verify that the client has been cleaned up
        assertNull(registry.client2Subscription.get(clientId));
        assertNull(registry.clientChannels.get(clientId));
        assertEquals(0, registry.activeNum.get());

        // Verify that the listener was called
        verify(mockListener, times(1)).onUnregister(eq(clientId), eq(group), eq("lmq1"));
        verify(mockListener, times(1)).onUnregister(eq(clientId), eq(group), eq("lmq2"));
        verify(mockListener, times(1)).onRemoveAll(eq(clientId), eq(group));

        // Verify that topics in liteTopic2Group have been removed
        assertNull(registry.liteTopic2Group.get("lmq1"));
        assertNull(registry.liteTopic2Group.get("lmq2"));
    }

    @Test
    public void testCleanupExpiredSubscriptions_ExpiredClientWithNoSubscriptions() {
        String clientId = "client1";
        String group = "group1";
        String topic = "topic1";
        Set<String> liteTopics = new HashSet<>();

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.setTopic(topic);
        subscription.addLiteTopic(liteTopics);
        subscription.setUpdateTime(System.currentTimeMillis() - 20000); // Expired

        Channel channel = mock(Channel.class);

        registry.client2Subscription.put(clientId, subscription);
        registry.clientChannels.put(clientId, channel);

        registry.activeNum.set(0);

        LiteCtlListener mockListener = mock(LiteCtlListener.class);
        registry.addListener(mockListener);

        // Perform cleanup with 10 second timeout
        registry.cleanupExpiredSubscriptions(10000);

        // Verify that the client has been cleaned up
        assertNull(registry.client2Subscription.get(clientId));
        assertNull(registry.clientChannels.get(clientId));
        assertEquals(0, registry.activeNum.get());

        // Verify that the listener was not called
        verify(mockListener, never()).onUnregister(anyString(), anyString(), anyString());
    }

    // Test removeTopicGroup method
    @Test
    public void testRemoveTopicGroup_EmptyTopicGroupSet() {
        String clientId = "client1";
        String group = "group1";
        String liteTopic = "lmq1";

        ClientGroup clientGroup = new ClientGroup(clientId, group);

        // Initialize with a single client
        Set<ClientGroup> topicGroupSet = ConcurrentHashMap.newKeySet();
        topicGroupSet.add(clientGroup);
        registry.liteTopic2Group.put(liteTopic, topicGroupSet);
        registry.activeNum.set(1);

        // Remove the only client
        registry.removeTopicGroup(clientGroup, liteTopic, false);

        // Verify that the topic is completely removed from liteTopic2Group
        assertNull(registry.liteTopic2Group.get(liteTopic));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    // Test excludeClientByLmqName method
    @Test
    public void testExcludeClientByLmqName_EmptyClientSet() {
        String newClientId = "newClient";
        String group = "group1";
        String lmqName = "lmq1";

        // Ensure the liteTopic2Group map exists but is empty
        registry.liteTopic2Group.put(lmqName, ConcurrentHashMap.newKeySet());

        // Should not throw any exception
        registry.excludeClientByLmqName(newClientId, group, lmqName);

        // Verify no changes
        assertTrue(registry.liteTopic2Group.get(lmqName).isEmpty());
    }

    @Test
    public void testGetAllClientIdByGroup() {
        String group1 = "group1";
        String group2 = "group2";
        String clientId1 = "client1";
        String clientId2 = "client2";
        String clientId3 = "client3";
        String topic = "parentTopic";

        LiteSubscription sub1 = new LiteSubscription();
        sub1.setGroup(group1);
        sub1.setTopic(topic);

        LiteSubscription sub2 = new LiteSubscription();
        sub2.setGroup(group1);
        sub2.setTopic(topic);

        LiteSubscription sub3 = new LiteSubscription();
        sub3.setGroup(group2);
        sub3.setTopic(topic);

        registry.client2Subscription.put(clientId1, sub1);
        registry.client2Subscription.put(clientId2, sub2);
        registry.client2Subscription.put(clientId3, sub3);

        List<String> result;

        // group1
        result = registry.getAllClientIdByGroup(group1);
        assertEquals(2, result.size());
        assertTrue(result.contains(clientId1));
        assertTrue(result.contains(clientId2));

        // group2
        result = registry.getAllClientIdByGroup(group2);
        assertEquals(1, result.size());
        assertTrue(result.contains(clientId3));

        // not exist
        result = registry.getAllClientIdByGroup("notExistGroup");
        assertTrue(result.isEmpty());

        // null
        result = registry.getAllClientIdByGroup(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testResetOffset_minOffset() {
        String lmqName = "lmq1";
        String group = "group1";
        String clientId = "client1";

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.POLICY, OffsetOption.POLICY_MIN_VALUE);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, 0L);
    }

    @Test
    public void testResetOffset_maxOffset() {
        String lmqName = "lmq1";
        String group = "group1";
        String clientId = "client1";
        long maxOffset = 500L;

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);
        when(mockLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(maxOffset);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.POLICY, OffsetOption.POLICY_MAX_VALUE);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, maxOffset);
    }

    @Test
    public void testResetOffset_absolute() {
        String lmqName = "lmq1";
        String group = "group1";
        String clientId = "client1";
        long specifiedOffset = 250L;

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.OFFSET, specifiedOffset);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, specifiedOffset);
    }

    @Test
    public void testResetOffset_LastN() {
        String lmqName = "lmq1";
        String group1 = "group1";
        String group2 = "group2";
        String clientId = "client1";
        long currentOffset = 100L;
        long lastN = 20L;
        long expectedTargetOffset = 80L;

        when(mockConsumerOffsetManager.queryOffset(group1, lmqName, 0)).thenReturn(currentOffset);
        when(mockConsumerOffsetManager.queryOffset(group2, lmqName, 0)).thenReturn(-1L);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.TAIL_N, lastN);

        registry.resetOffset(lmqName, group1, clientId, offsetOption);
        registry.resetOffset(lmqName, group2, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group1, 0, expectedTargetOffset);
        verify(mockConsumerOffsetManager, never()).assignResetOffset(lmqName, group2, 0, expectedTargetOffset);
    }

    @Test
    public void testResetOffset_timestamp_not_supported() {
        String lmqName = "lmq1";
        String group = "group1";
        String clientId = "client1";
        long timestamp = System.currentTimeMillis();

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.TIMESTAMP, timestamp);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager, never()).assignResetOffset(anyString(), anyString(), anyInt(), anyLong());
    }
}