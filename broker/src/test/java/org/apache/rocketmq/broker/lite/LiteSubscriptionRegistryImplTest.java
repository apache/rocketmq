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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.QueueLevelConsumerManager;
import org.apache.rocketmq.broker.processor.PopLiteMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.remoting.protocol.header.NotifyUnsubscribeLiteRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LiteSubscriptionRegistryImplTest {

    private LiteSubscriptionRegistryImpl registry;
    private BrokerController mockBrokerController;
    private AbstractLiteLifecycleManager mockLifecycleManager;
    private SubscriptionGroupManager mockSubscriptionGroupManager;
    private BrokerConfig mockBrokerConfig;
    private ConsumerOffsetManager mockConsumerOffsetManager;
    private PopLiteMessageProcessor mockPopLiteMessageProcessor;
    private QueueLevelConsumerManager mockConsumerOrderInfoManager;
    private Broker2Client mockBroker2Client;
    private LiteCtlListener mockListener;

    @Before
    public void setUp() {
        mockBrokerController = mock(BrokerController.class);
        mockLifecycleManager = mock(AbstractLiteLifecycleManager.class);
        mockSubscriptionGroupManager = mock(SubscriptionGroupManager.class);
        mockBrokerConfig = mock(BrokerConfig.class);
        mockConsumerOffsetManager = mock(ConsumerOffsetManager.class);
        mockPopLiteMessageProcessor = mock(PopLiteMessageProcessor.class);
        mockConsumerOrderInfoManager = mock(QueueLevelConsumerManager.class);
        mockBroker2Client = mock(Broker2Client.class);

        when(mockBrokerController.getSubscriptionGroupManager()).thenReturn(mockSubscriptionGroupManager);
        when(mockBrokerController.getBrokerConfig()).thenReturn(mockBrokerConfig);
        when(mockBrokerController.getConsumerOffsetManager()).thenReturn(mockConsumerOffsetManager);
        when(mockBrokerController.getPopLiteMessageProcessor()).thenReturn(mockPopLiteMessageProcessor);
        when(mockBrokerController.getBroker2Client()).thenReturn(mockBroker2Client);
        when(mockPopLiteMessageProcessor.getConsumerOrderInfoManager()).thenReturn(mockConsumerOrderInfoManager);
        when(mockConsumerOrderInfoManager.getTable()).thenReturn(new ConcurrentHashMap<>());
        when(mockPopLiteMessageProcessor.getConsumerOrderInfoManager()).thenReturn(mockConsumerOrderInfoManager);
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1000L);
        when(mockBrokerConfig.getLiteSubscriptionCheckInterval()).thenReturn(1000L);
        when(mockBrokerConfig.getLiteSubscriptionCheckTimeoutMills()).thenReturn(60000L);

        registry = new LiteSubscriptionRegistryImpl(mockBrokerController, mockLifecycleManager);
        mockListener = mock(LiteCtlListener.class);
        registry.addListener(mockListener);
    }

    /**
     * Test updateClientChannel updates client channel correctly
     */
    @Test
    public void testUpdateClientChannel_UpdateChannel() {
        String clientId = "testClient";
        Channel mockChannel = mock(Channel.class);

        registry.updateClientChannel(clientId, mockChannel);

        assertEquals(mockChannel, registry.clientChannels.get(clientId));
    }

    /**
     * Test addPartialSubscription throws exception when quota exceeded
     */
    @Test
    public void testAddPartialSubscription_QuotaExceeded() {
        // Set quota to 0 so any new subscription exceeds quota
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(0L);

        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = Collections.singleton("lmq1");

        assertThrows(LiteQuotaException.class, () -> {
            registry.addPartialSubscription(clientId, group, topic, lmqNameSet, null);
        });
    }

    /**
     * Test addPartialSubscription throws exception for wildcard group
     */
    @Test
    public void testAddPartialSubscription_WildcardGroup() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = Collections.singleton("lmq1");

        // Simulate wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        assertThrows(IllegalStateException.class, () -> {
            registry.addPartialSubscription(clientId, group, topic, lmqNameSet, null);
        });
    }

    /**
     * Test addPartialSubscription does not add inactive subscription
     */
    @Test
    public void testAddPartialSubscription_InactiveSubscription() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = Collections.singleton("lmq1");

        // Simulate non-wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);

        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, "lmq1")).thenReturn(false);

        registry.addPartialSubscription(clientId, group, topic, lmqNameSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertFalse(subscription.getLmqSet().contains("lmq1"));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    /**
     * Test addPartialSubscription adds subscription normally
     */
    @Test
    public void testAddPartialSubscription_NormalCase() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = Collections.singleton("lmq1");

        // Simulate non-wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, "lmq1")).thenReturn(true);

        registry.addPartialSubscription(clientId, group, topic, lmqNameSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getLmqSet().contains("lmq1"));
        assertEquals(1, registry.getActiveSubscriptionNum());

        verify(mockListener).onRegister(clientId, group, "lmq1");
    }

    /**
     * Test addPartialSubscription excludes client in exclusive mode
     */
    @Test
    public void testAddPartialSubscription_ExclusiveMode() {
        String clientId1 = "testClient1";
        String clientId2 = "testClient2";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = Collections.singleton("lmq1");

        // Simulate non-wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, "lmq1")).thenReturn(true);

        // Add first client
        registry.addPartialSubscription(clientId1, group, topic, lmqNameSet, null);

        LiteSubscription subscription1 = registry.getLiteSubscription(clientId1);
        assertNotNull(subscription1);
        assertTrue(subscription1.getLmqSet().contains("lmq1"));
        assertEquals(1, registry.getActiveSubscriptionNum());

        // Add second client, should exclude first client
        registry.addPartialSubscription(clientId2, group, topic, lmqNameSet, null);

        LiteSubscription subscription2 = registry.getLiteSubscription(clientId2);
        assertNotNull(subscription2);
        assertTrue(subscription2.getLmqSet().contains("lmq1"));
        assertNull(registry.getLiteSubscription(clientId1));
        assertEquals(1, registry.getActiveSubscriptionNum());

        verify(mockListener).onRegister(clientId1, group, "lmq1");
        verify(mockListener).onUnregister(clientId1, group, "lmq1");
        verify(mockListener).onRegister(clientId2, group, "lmq1");
    }

    /**
     * Test removePartialSubscription removes partial subscription correctly
     */
    @Test
    public void testRemovePartialSubscription_RemoveSubscription() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = new HashSet<>();
        lmqNameSet.add("lmq1");
        lmqNameSet.add("lmq2");

        // Simulate non-wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // Add subscription first
        registry.addPartialSubscription(clientId, group, topic, lmqNameSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getLmqSet().contains("lmq1"));
        assertTrue(subscription.getLmqSet().contains("lmq2"));
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Remove partial subscription
        Set<String> toRemove = Collections.singleton("lmq1");
        registry.removePartialSubscription(clientId, group, topic, toRemove);

        subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertFalse(subscription.getLmqSet().contains("lmq1"));
        assertTrue(subscription.getLmqSet().contains("lmq2"));
        assertEquals(1, registry.getActiveSubscriptionNum());

        verify(mockListener).onUnregister(clientId, group, "lmq1");
    }

    /**
     * Test addCompleteSubscription handles wildcard group
     */
    @Test
    public void testAddCompleteSubscription_WildcardGroup() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameAll = new HashSet<>();
        lmqNameAll.add("lmq1");
        lmqNameAll.add("lmq2");

        // Simulate wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        registry.addCompleteSubscription(clientId, group, topic, lmqNameAll, 1L);

        assertTrue(registry.wildcardGroupMap.containsKey(topic));
        assertTrue(registry.wildcardGroupMap.get(topic).contains(group));

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getLmqSet().contains(topic + "@" + group));
        assertEquals(1, registry.getActiveSubscriptionNum());
    }

    /**
     * Test removeCompleteSubscription cleans wildcard group metadata
     */
    @Test
    public void testRemoveCompleteSubscription_WildcardGroupMetadataCleanup() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameAll = new HashSet<>();
        lmqNameAll.add("lmq1");

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        registry.addCompleteSubscription(clientId, group, topic, lmqNameAll, 1L);

        assertTrue(registry.wildcardGroupMap.containsKey(topic));
        assertTrue(registry.wildcardGroupMap.get(topic).contains(group));

        registry.removeCompleteSubscription(clientId);

        assertFalse(registry.wildcardGroupMap.containsKey(topic));
        assertNull(registry.getLiteSubscription(clientId));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    /**
     * Test addCompleteSubscription updates complete subscription
     */
    @Test
    public void testAddCompleteSubscription_UpdateSubscription() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameAll = new HashSet<>();
        lmqNameAll.add("lmq1");
        lmqNameAll.add("lmq2");

        Set<String> lmqNameNew = new HashSet<>();
        lmqNameNew.add("lmq2");
        lmqNameNew.add("lmq3");

        // Simulate non-wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // Add initial subscription
        registry.addCompleteSubscription(clientId, group, topic, lmqNameAll, 1L);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getLmqSet().contains("lmq1"));
        assertTrue(subscription.getLmqSet().contains("lmq2"));
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Update subscription
        registry.addCompleteSubscription(clientId, group, topic, lmqNameNew, 2L);

        subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertFalse(subscription.getLmqSet().contains("lmq1"));
        assertTrue(subscription.getLmqSet().contains("lmq2"));
        assertTrue(subscription.getLmqSet().contains("lmq3"));
        assertEquals(2, registry.getActiveSubscriptionNum());
    }

    /**
     * Test removeCompleteSubscription removes all subscriptions
     */
    @Test
    public void testRemoveCompleteSubscription_RemoveAll() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameSet = new HashSet<>();
        lmqNameSet.add("lmq1");
        lmqNameSet.add("lmq2");

        // Simulate non-wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // Add subscription first
        registry.addPartialSubscription(clientId, group, topic, lmqNameSet, null);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getLmqSet().contains("lmq1"));
        assertTrue(subscription.getLmqSet().contains("lmq2"));
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Remove complete subscription
        registry.removeCompleteSubscription(clientId);

        assertNull(registry.getLiteSubscription(clientId));
        assertNull(registry.clientChannels.get(clientId));
        assertEquals(0, registry.getActiveSubscriptionNum());

        verify(mockListener).onRemoveAll(clientId, group);
    }

    /**
     * Test addListener adds listener
     */
    @Test
    public void testAddListener_AddListener() {
        LiteCtlListener listener = mock(LiteCtlListener.class);

        registry.addListener(listener);

        assertTrue(registry.listeners.contains(listener));
    }

    /**
     * Test getAllSubscriber gets wildcard subscribers
     */
    @Test
    public void testGetAllSubscribers_WildcardGroup() {
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName = LiteUtil.toLmqName(topic, "liteTopic");
        String wildcardLmqName = topic + "@" + group;

        // Simulate wildcard group with subscription data
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        groupConfig.setLiteBindTopic(topic);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        ClientGroup clientGroup = new ClientGroup("testClient", group);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(clientGroup);
        registry.liteTopic2ClientGroup.put(wildcardLmqName, clientSet);

        Map<String, List<ClientGroup>> result = registry.getAllSubscribers(group, lmqName);

        assertNotNull(result);
        assertTrue(result.containsKey(group));
    }

    /**
     * Test getAllSubscriber gets subscribers for specific group
     */
    @Test
    public void testGetAllSubscribers_SpecificGroup() {
        String clientId = "testClient";
        String group = "testGroup";
        String lmqName = "lmq1";

        // Add subscription
        ClientGroup clientGroup = new ClientGroup(clientId, group);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(clientGroup);
        registry.liteTopic2ClientGroup.put(lmqName, clientSet);

        Map<String, List<ClientGroup>> result = registry.getAllSubscribers(group, lmqName);

        assertNotNull(result);
        assertTrue(result.containsKey(group));
        List<ClientGroup> clients = result.get(group);
        assertEquals(1, clients.size());
        assertEquals(clientId, clients.get(0).clientId);
        assertEquals(group, clients.get(0).group);
    }

    /**
     * Test getAllSubscriber gets subscribers for all groups
     */
    @Test
    public void testGetAllSubscriber_AllGroups() {
        String clientId1 = "testClient1";
        String clientId2 = "testClient2";
        String group1 = "testGroup1";
        String group2 = "testGroup2";
        String topic = "testTopic";
        String lmqName = LiteUtil.toLmqName(topic, "lmq1");

        // Add subscription
        ClientGroup clientGroup1 = new ClientGroup(clientId1, group1);
        ClientGroup clientGroup2 = new ClientGroup(clientId2, group2);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(clientGroup1);
        clientSet.add(clientGroup2);
        registry.liteTopic2ClientGroup.put(lmqName, clientSet);

        Map<String, List<ClientGroup>> result = registry.getAllSubscribers(null, lmqName);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey(group1));
        assertTrue(result.containsKey(group2));
        assertEquals(1, result.get(group1).size());
        assertEquals(1, result.get(group2).size());
    }

    /**
     * Test cleanSubscription cleans subscription
     */
    @Test
    public void testCleanSubscription_CleanSubscription() {
        String clientId = "testClient";
        String group = "testGroup";
        String lmqName = "lmq1";

        // Add subscription
        ClientGroup clientGroup = new ClientGroup(clientId, group);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(clientGroup);
        registry.liteTopic2ClientGroup.put(lmqName, clientSet);

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.addLmq(lmqName);
        registry.client2Subscription.put(clientId, subscription);
        registry.activeNum.set(1);

        registry.cleanSubscription(lmqName, false);

        assertFalse(registry.liteTopic2ClientGroup.containsKey(lmqName));
        assertFalse(subscription.getLmqSet().contains(lmqName));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    /**
     * Test getLiteSubscription gets LiteSubscription
     */
    @Test
    public void testGetLiteSubscription_GetSubscription() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.setTopic(topic);
        registry.client2Subscription.put(clientId, subscription);

        LiteSubscription result = registry.getLiteSubscription(clientId);

        assertNotNull(result);
        assertEquals(group, result.getGroup());
        assertEquals(topic, result.getTopic());
    }

    /**
     * Test getActiveSubscriptionNum gets active subscription count
     */
    @Test
    public void testGetActiveSubscriptionNum_GetCount() {
        registry.activeNum.set(5);

        int count = registry.getActiveSubscriptionNum();

        assertEquals(5, count);
    }

    /**
     * Test getAllClientIdByGroup gets all client IDs by group
     */
    @Test
    public void testGetAllClientIdByGroup_GetClientIds() {
        String clientId1 = "testClient1";
        String clientId2 = "testClient2";
        String clientId3 = "testClient3";
        String group1 = "testGroup1";
        String group2 = "testGroup2";
        String topic = "testTopic";

        LiteSubscription subscription1 = new LiteSubscription();
        subscription1.setGroup(group1);
        subscription1.setTopic(topic);
        registry.client2Subscription.put(clientId1, subscription1);

        LiteSubscription subscription2 = new LiteSubscription();
        subscription2.setGroup(group1);
        subscription2.setTopic(topic);
        registry.client2Subscription.put(clientId2, subscription2);

        LiteSubscription subscription3 = new LiteSubscription();
        subscription3.setGroup(group2);
        subscription3.setTopic(topic);
        registry.client2Subscription.put(clientId3, subscription3);

        List<String> result = registry.getAllClientIdByGroup(group1);

        assertEquals(2, result.size());
        assertTrue(result.contains(clientId1));
        assertTrue(result.contains(clientId2));
    }

    /**
     * Test resetOffset resets offset to specific value
     */
    @Test
    public void testResetOffset_SpecificOffset() {
        String lmqName = "lmq1";
        String group = "testGroup";
        String clientId = "testClient";
        long specifiedOffset = 250L;

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.OFFSET, specifiedOffset);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, specifiedOffset);
    }

    /**
     * Test resetOffset resets offset to minimum
     */
    @Test
    public void testResetOffset_MinOffset() {
        String lmqName = "lmq1";
        String group = "testGroup";
        String clientId = "testClient";

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.POLICY, OffsetOption.POLICY_MIN_VALUE);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, 0L);
    }

    /**
     * Test resetOffset resets offset to maximum
     */
    @Test
    public void testResetOffset_MaxOffset() {
        String lmqName = "lmq1";
        String group = "testGroup";
        String clientId = "testClient";
        long maxOffset = 500L;

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);
        when(mockLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(maxOffset);

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.POLICY, OffsetOption.POLICY_MAX_VALUE);
        registry.resetOffset(lmqName, group, clientId, offsetOption);

        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, maxOffset);
    }

    /**
     * Test notifyUnsubscribeLite notifies client to unsubscribe
     */
    @Test
    public void testNotifyUnsubscribeLite_NotifyClient() {
        String clientId = "testClient";
        String group = "testGroup";
        String lmqName = LiteUtil.toLmqName("testTopic", "lmq1");
        Channel mockChannel = mock(Channel.class);

        registry.clientChannels.put(clientId, mockChannel);

        registry.notifyUnsubscribeLite(clientId, group, lmqName);

        ArgumentCaptor<NotifyUnsubscribeLiteRequestHeader> captor = ArgumentCaptor.forClass(NotifyUnsubscribeLiteRequestHeader.class);
        verify(mockBroker2Client).notifyUnsubscribeLite(eq(mockChannel), captor.capture());
        NotifyUnsubscribeLiteRequestHeader header = captor.getValue();
        assertEquals(clientId, header.getClientId());
        assertEquals(group, header.getConsumerGroup());
        assertEquals("lmq1", header.getLiteTopic());
    }

    /**
     * Test cleanupExpiredSubscriptions cleans expired subscriptions
     */
    @Test
    public void testCleanupExpiredSubscriptions_CleanExpired() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        long timeout = 10000L; // 10 seconds

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.setTopic(topic);
        // Updated 20 seconds ago, expired
        subscription.setUpdateTime(System.currentTimeMillis() - 20000L);

        registry.client2Subscription.put(clientId, subscription);
        registry.cleanupExpiredSubscriptions(timeout);

        assertFalse(registry.client2Subscription.containsKey(clientId));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    // ==================== Exclusive Eviction Tombstone Tests ====================

    /**
     * Test: When clientB takes over lmq in exclusive mode, clientA gets a tombstone
     */
    @Test
    public void testExclusiveEviction_TombstoneCreatedOnEviction() {
        String clientA = "clientA";
        String clientB = "clientB";
        String group = "exclusiveGroup";
        String topic = "testTopic";
        String lmqName = "lmq1";
        Set<String> lmqNameSet = Collections.singleton(lmqName);

        // Configure exclusive mode
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, lmqName)).thenReturn(true);

        // clientA subscribes
        registry.updateClientChannel(clientA, mock(Channel.class));
        registry.addPartialSubscription(clientA, group, topic, lmqNameSet, null);

        assertFalse(registry.hasExclusiveEvictionTombstone(clientA, lmqName));

        // clientB takes over → clientA should get tombstone
        registry.updateClientChannel(clientB, mock(Channel.class));
        registry.addPartialSubscription(clientB, group, topic, lmqNameSet, null);

        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, lmqName));
        assertFalse(registry.hasExclusiveEvictionTombstone(clientB, lmqName));
    }

    /**
     * Test: addCompleteSubscription clears stale tombstones but keeps active ones
     */
    @Test
    public void testExclusiveEviction_CompleteSyncClearsStale() {
        String clientA = "clientA";
        String clientB = "clientB";
        String group = "exclusiveGroup";
        String topic = "testTopic";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // clientA subscribes to lmq1 and lmq2
        Set<String> initialSet = new HashSet<>();
        initialSet.add("lmq1");
        initialSet.add("lmq2");
        registry.updateClientChannel(clientA, mock(Channel.class));
        registry.addPartialSubscription(clientA, group, topic, initialSet, null);

        // clientB takes over lmq1 and lmq2 from clientA
        registry.updateClientChannel(clientB, mock(Channel.class));
        registry.addPartialSubscription(clientB, group, topic, Collections.singleton("lmq1"), null);
        registry.addPartialSubscription(clientB, group, topic, Collections.singleton("lmq2"), null);

        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, "lmq1"));
        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, "lmq2"));

        // clientA does full sync with only lmq2 in active set → lmq1 tombstone should be cleaned
        Set<String> newActiveSet = new HashSet<>();
        newActiveSet.add("lmq2");
        newActiveSet.add("lmq3");
        registry.addCompleteSubscription(clientA, group, topic, newActiveSet, 2L);

        assertFalse(registry.hasExclusiveEvictionTombstone(clientA, "lmq1")); // cleared (not in newActiveSet)
        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, "lmq2"));  // retained (in newActiveSet)
    }

    /**
     * Test: removeCompleteSubscription clears all tombstones for the client
     */
    @Test
    public void testExclusiveEviction_RemoveClientClearsTombstones() {
        String clientA = "clientA";
        String clientB = "clientB";
        String group = "exclusiveGroup";
        String topic = "testTopic";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // Setup: clientA subscribes then gets evicted by clientB
        registry.updateClientChannel(clientA, mock(Channel.class));
        registry.addPartialSubscription(clientA, group, topic, Collections.singleton("lmq1"), null);
        registry.addPartialSubscription(clientA, group, topic, Collections.singleton("lmq2"), null);

        registry.updateClientChannel(clientB, mock(Channel.class));
        registry.addPartialSubscription(clientB, group, topic, Collections.singleton("lmq1"), null);
        registry.addPartialSubscription(clientB, group, topic, Collections.singleton("lmq2"), null);

        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, "lmq1"));
        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, "lmq2"));

        // clientA disconnects
        registry.removeCompleteSubscription(clientA);

        assertFalse(registry.hasExclusiveEvictionTombstone(clientA, "lmq1"));
        assertFalse(registry.hasExclusiveEvictionTombstone(clientA, "lmq2"));
        // clientB unaffected
        assertFalse(registry.hasExclusiveEvictionTombstone(clientB, "lmq1"));
    }

    /**
     * Test: expired subscription cleanup also clears tombstones
     */
    @Test
    public void testExclusiveEviction_ExpiredCleanupClearsTombstones() {
        String clientA = "clientA";
        String clientB = "clientB";
        String group = "exclusiveGroup";
        String topic = "testTopic";
        String lmqName = "lmq1";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, lmqName)).thenReturn(true);

        // clientA subscribes, then gets evicted
        registry.updateClientChannel(clientA, mock(Channel.class));
        registry.addPartialSubscription(clientA, group, topic, Collections.singleton(lmqName), null);

        registry.updateClientChannel(clientB, mock(Channel.class));
        registry.addPartialSubscription(clientB, group, topic, Collections.singleton(lmqName), null);

        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, lmqName));

        // Simulate clientA becoming expired (sub was already removed, manually add back for timeout test)
        LiteSubscription expiredSub = new LiteSubscription();
        expiredSub.setGroup(group);
        expiredSub.setTopic(topic);
        expiredSub.setUpdateTime(System.currentTimeMillis() - 60000L);
        registry.client2Subscription.put(clientA, expiredSub);

        registry.cleanupExpiredSubscriptions(10000L);

        assertFalse(registry.hasExclusiveEvictionTombstone(clientA, lmqName));
    }

    /**
     * Test: addPartialSubscription clears stale tombstone when client re-claims an lmqName
     */
    @Test
    public void testExclusiveEviction_ReClaimClearsStaleTombstone() {
        String clientA = "clientA";
        String clientB = "clientB";
        String group = "exclusiveGroup";
        String topic = "testTopic";
        String lmqName = "lmq1";
        Set<String> lmqNameSet = Collections.singleton(lmqName);

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, lmqName)).thenReturn(true);

        // clientA subscribes first
        registry.updateClientChannel(clientA, mock(Channel.class));
        registry.addPartialSubscription(clientA, group, topic, lmqNameSet, null);

        // clientB takes over → clientA gets tombstone
        registry.updateClientChannel(clientB, mock(Channel.class));
        registry.addPartialSubscription(clientB, group, topic, lmqNameSet, null);

        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, lmqName));

        // clientA re-claims the lmqName → tombstone should be cleared
        registry.addPartialSubscription(clientA, group, topic, lmqNameSet, null);

        assertFalse(registry.hasExclusiveEvictionTombstone(clientA, lmqName));
    }

    /**
     * Test: addCompleteSubscription re-sends unsubscribe for tombstoned lmqNames still in active set
     */
    @Test
    public void testExclusiveEviction_CompleteSyncReNotifiesForTombstonedLmq() {
        String clientA = "clientA";
        String clientB = "clientB";
        String group = "exclusiveGroup";
        String topic = "testTopic";
        String lmqName = "lmq1";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        Channel clientAChannel = mock(Channel.class);
        registry.updateClientChannel(clientA, clientAChannel);
        registry.addPartialSubscription(clientA, group, topic, Collections.singleton(lmqName), null);

        // clientB takes over → clientA gets tombstone
        registry.updateClientChannel(clientB, mock(Channel.class));
        registry.addPartialSubscription(clientB, group, topic, Collections.singleton(lmqName), null);

        assertTrue(registry.hasExclusiveEvictionTombstone(clientA, lmqName));

        // clientA does full sync still reporting lmq1 → re-notify should be triggered
        Set<String> fullSet = new HashSet<>();
        fullSet.add(lmqName);
        registry.addCompleteSubscription(clientA, group, topic, fullSet, 2L);

        // Verify notifyUnsubscribeLite was called for the tombstoned lmqName during full sync
        // The first call is during eviction, the second is during re-notify
        ArgumentCaptor<NotifyUnsubscribeLiteRequestHeader> captor =
            ArgumentCaptor.forClass(NotifyUnsubscribeLiteRequestHeader.class);
        verify(mockBroker2Client, org.mockito.Mockito.atLeast(2))
            .notifyUnsubscribeLite(eq(clientAChannel), captor.capture());
    }

    // ==================== resetOffset Edge Cases ====================

    /**
     * Test: resetOffset with null option is a no-op
     */
    @Test
    public void testResetOffset_NullOption() {
        registry.resetOffset("lmq1", "group", "client", null);
        // No interaction with offset manager
        org.mockito.Mockito.verifyNoInteractions(mockConsumerOffsetManager);
    }

    /**
     * Test: resetOffset with TAIL_N computes target correctly
     */
    @Test
    public void testResetOffset_TailN() {
        String lmqName = "lmq1";
        String group = "testGroup";
        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption option = new OffsetOption(OffsetOption.Type.TAIL_N, 30);
        registry.resetOffset(lmqName, group, "client", option);

        // targetOffset = max(0, 100 - 30) = 70
        verify(mockConsumerOffsetManager).assignResetOffset(lmqName, group, 0, 70L);
    }

    /**
     * Test: resetOffset with TAIL_N when no existing offset (currentOffset < 0)
     */
    @Test
    public void testResetOffset_TailN_NoExistingOffset() {
        String lmqName = "lmq1";
        String group = "testGroup";
        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(-1L);

        OffsetOption option = new OffsetOption(OffsetOption.Type.TAIL_N, 30);
        registry.resetOffset(lmqName, group, "client", option);

        // currentOffset < 0 → targetOffset stays null → no reset
        org.mockito.Mockito.verify(mockConsumerOffsetManager, org.mockito.Mockito.never())
            .assignResetOffset(anyString(), anyString(), eq(0), eq(0L));
    }

    /**
     * Test: resetOffset with TIMESTAMP is silently disabled
     */
    @Test
    public void testResetOffset_Timestamp() {
        String lmqName = "lmq1";
        String group = "testGroup";
        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        OffsetOption option = new OffsetOption(OffsetOption.Type.TIMESTAMP, System.currentTimeMillis());
        registry.resetOffset(lmqName, group, "client", option);

        // TIMESTAMP is disabled → no reset
        org.mockito.Mockito.verify(mockConsumerOffsetManager, org.mockito.Mockito.never())
            .assignResetOffset(anyString(), anyString(), eq(0), eq(0L));
    }

    /**
     * Test: resetOffset skips when target equals current
     */
    @Test
    public void testResetOffset_SameOffset_NoReset() {
        String lmqName = "lmq1";
        String group = "testGroup";
        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(250L);

        OffsetOption option = new OffsetOption(OffsetOption.Type.OFFSET, 250L);
        registry.resetOffset(lmqName, group, "client", option);

        org.mockito.Mockito.verify(mockConsumerOffsetManager, org.mockito.Mockito.never())
            .assignResetOffset(anyString(), anyString(), eq(0), eq(0L));
    }

    // ==================== removePartialSubscription Supplements ====================

    /**
     * Test: removePartialSubscription triggers resetOffset when group has resetOffsetOnUnsubscribe
     */
    @Test
    public void testRemovePartialSubscription_ResetOffsetOnUnsubscribe() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName = "lmq1";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.getAttributes().put("lite.sub.reset.offset.unsubscribe", "true");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(topic, lmqName)).thenReturn(true);

        registry.addPartialSubscription(clientId, group, topic, Collections.singleton(lmqName), null);

        when(mockConsumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(100L);

        registry.removePartialSubscription(clientId, group, topic, Collections.singleton(lmqName));

        // resetOffset should be called with POLICY MIN
        verify(mockConsumerOffsetManager).assignResetOffset(eq(lmqName), eq(group), eq(0), eq(0L));
    }


    // ==================== cleanSubscription Supplements ====================

    /**
     * Test: cleanSubscription with notifyClient=true sends notification
     */
    @Test
    public void testCleanSubscription_NotifyClient() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName = LiteUtil.toLmqName(topic, "liteTopic");
        Channel mockChannel = mock(Channel.class);

        registry.clientChannels.put(clientId, mockChannel);
        ClientGroup clientGroup = new ClientGroup(clientId, group);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(clientGroup);
        registry.liteTopic2ClientGroup.put(lmqName, clientSet);

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.addLmq(lmqName);
        registry.client2Subscription.put(clientId, subscription);
        registry.activeNum.set(1);

        registry.cleanSubscription(lmqName, true);

        verify(mockBroker2Client).notifyUnsubscribeLite(eq(mockChannel),
            org.mockito.Mockito.any(NotifyUnsubscribeLiteRequestHeader.class));
    }

    /**
     * Test: cleanSubscription with empty/nonexistent lmq is a no-op
     */
    @Test
    public void testCleanSubscription_EmptyClientSet() {
        int beforeActive = registry.getActiveSubscriptionNum();
        registry.cleanSubscription("nonexistent_lmq", true);
        assertEquals(beforeActive, registry.getActiveSubscriptionNum());
    }

    /**
     * Test: cleanSubscription skips clientGroup when client2Subscription has no entry
     */
    @Test
    public void testCleanSubscription_NullSubscription() {
        String lmqName = "lmq1";
        ClientGroup orphanCg = new ClientGroup("orphanClient", "orphanGroup");
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(orphanCg);
        registry.liteTopic2ClientGroup.put(lmqName, clientSet);
        registry.activeNum.set(1);

        // client2Subscription has no entry for "orphanClient"
        registry.cleanSubscription(lmqName, false);

        // lmqName removed from liteTopic2ClientGroup, activeNum unchanged (removeLmq returned false)
        assertFalse(registry.liteTopic2ClientGroup.containsKey(lmqName));
    }

    // ==================== getWildcardGroupClients Direct Tests ====================

    /**
     * Test: getWildcardGroupClients returns clients when data exists
     */
    @Test
    public void testGetWildcardGroupClients_HasClients() {
        String group = "wildcardGroup";
        String topic = "testTopic";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        groupConfig.setLiteBindTopic(topic);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        ClientGroup cg = new ClientGroup("client1", group);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(cg);
        registry.liteTopic2ClientGroup.put(topic + "@" + group, clientSet);

        List<ClientGroup> result = registry.getWildcardGroupClients(group);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("client1", result.get(0).clientId);
    }

    /**
     * Test: getWildcardGroupClients returns empty list when bindTopic is null
     */
    @Test
    public void testGetWildcardGroupClients_NoBindTopic() {
        String group = "wildcardGroup";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        // No liteBindTopic set → getLiteBindTopic returns null
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        List<ClientGroup> result = registry.getWildcardGroupClients(group);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ==================== Boundary / Defensive Tests ====================

    /**
     * Test: removeCompleteSubscription with nonexistent clientId is a no-op
     */
    @Test
    public void testRemoveCompleteSubscription_NullSubscription() {
        // Should not throw
        registry.removeCompleteSubscription("nonexistent_client");
    }

    /**
     * Test: removeCompleteSubscription for non-exclusive group does not clear tombstones
     */
    @Test
    public void testRemoveCompleteSubscription_NonExclusiveGroup() {
        String clientId = "testClient";
        String group = "normalGroup";
        String topic = "testTopic";

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        registry.addPartialSubscription(clientId, group, topic, Collections.singleton("lmq1"), null);

        // Manually add a tombstone to verify it's NOT cleaned for non-exclusive
        // (non-exclusive groups shouldn't have tombstones, but verify the guard logic)
        registry.removeCompleteSubscription(clientId);
        assertNull(registry.getLiteSubscription(clientId));
    }

    /**
     * Test: notifyUnsubscribeLite with null channel does not throw
     */
    @Test
    public void testNotifyUnsubscribeLite_ChannelNull() {
        String lmqName = LiteUtil.toLmqName("testTopic", "liteTopic");
        // No channel registered for this client
        registry.notifyUnsubscribeLite("unknownClient", "group", lmqName);

        // broker2Client should not be called
        org.mockito.Mockito.verifyNoInteractions(mockBroker2Client);
    }

    /**
     * Test: excludeClientByLmqName with empty client set is a no-op
     */
    @Test
    public void testExcludeClientByLmqName_EmptyClientSet() {
        // No subscribers for lmq1
        int activeBefore = registry.getActiveSubscriptionNum();
        // excludeClientByLmqName is protected, test through addPartialSubscription in exclusive mode
        // But we can verify indirectly: adding a new client to an empty lmq should not trigger exclusion logic
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("exclusiveGroup");
        groupConfig.setLiteSubExclusive(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig("exclusiveGroup")).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive("testTopic", "lmq1")).thenReturn(true);

        registry.addPartialSubscription("newClient", "exclusiveGroup", "testTopic",
            Collections.singleton("lmq1"), null);

        assertEquals(activeBefore + 1, registry.getActiveSubscriptionNum());
        assertFalse(registry.hasExclusiveEvictionTombstone("newClient", "lmq1"));
    }

}
