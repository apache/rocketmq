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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
        assertFalse(subscription.getLiteTopicSet().contains("lmq1"));
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
        assertTrue(subscription.getLiteTopicSet().contains("lmq1"));
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
        assertTrue(subscription1.getLiteTopicSet().contains("lmq1"));
        assertEquals(1, registry.getActiveSubscriptionNum());

        // Add second client, should exclude first client
        registry.addPartialSubscription(clientId2, group, topic, lmqNameSet, null);

        LiteSubscription subscription2 = registry.getLiteSubscription(clientId2);
        assertNotNull(subscription2);
        assertTrue(subscription2.getLiteTopicSet().contains("lmq1"));
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
        assertTrue(subscription.getLiteTopicSet().contains("lmq1"));
        assertTrue(subscription.getLiteTopicSet().contains("lmq2"));
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Remove partial subscription
        Set<String> toRemove = Collections.singleton("lmq1");
        registry.removePartialSubscription(clientId, group, topic, toRemove);

        subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertFalse(subscription.getLiteTopicSet().contains("lmq1"));
        assertTrue(subscription.getLiteTopicSet().contains("lmq2"));
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
        assertTrue(subscription.getLiteTopicSet().contains(topic + "@" + group));
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

    // ==================== Pattern-mode Wildcard Group Tests ====================

    private SubscriptionGroupConfig wildcardGroupConfig(String group) {
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        return groupConfig;
    }

    /**
     * Pattern-mode wildcard group: patterns are eagerly expanded against existing lite-topics
     * under the parent topic, and matched lmqNames are registered as a normal subscription.
     */
    @Test
    public void testAddCompleteSubscription_PatternWildcardGroup_ExpandsAgainstExistingLmq() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        // candidates returned by collectByParentTopic (full lmqNames)
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success");
        String payRefundNotify = LiteUtil.toLmqName(topic, "pay__refund__notify");
        String orderCreated = LiteUtil.toLmqName(topic, "order__created");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(
            Arrays.asList(payRefund, paySuccess, payRefundNotify, orderCreated));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        Set<String> patterns = new HashSet<>(Collections.singletonList("pay__*"));
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(), patterns, 1L);

        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertEquals(patterns, subscription.getWildcardPatterns());
        // pay__* matches pay__refund and pay__success only (single-level)
        assertTrue(subscription.getLiteTopicSet().contains(payRefund));
        assertTrue(subscription.getLiteTopicSet().contains(paySuccess));
        assertFalse(subscription.getLiteTopicSet().contains(payRefundNotify));
        assertFalse(subscription.getLiteTopicSet().contains(orderCreated));
        // No synthetic topic@group key for pattern-mode groups
        assertFalse(subscription.getLiteTopicSet().contains(topic + "@" + group));
        // marked in wildcardGroupMap for fan-out enumeration
        assertTrue(registry.wildcardGroupMap.containsKey(topic));
        assertTrue(registry.wildcardGroupMap.get(topic).contains(group));
        assertEquals(2, registry.getActiveSubscriptionNum());
    }

    /**
     * Re-subscribing with changed patterns removes old matched lmqNames and adds new ones.
     */
    @Test
    public void testAddCompleteSubscription_PatternWildcardGroup_DiffOnResubscribe() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String notifyRefundSms = LiteUtil.toLmqName(topic, "notify__refund__sms");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Arrays.asList(payRefund, notifyRefundSms));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // first subscribe: pay__*
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getLiteTopicSet().contains(payRefund));
        assertFalse(subscription.getLiteTopicSet().contains(notifyRefundSms));

        // re-subscribe: notify__**
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("notify__**")), 2L);
        subscription = registry.getLiteSubscription(clientId);
        assertFalse(subscription.getLiteTopicSet().contains(payRefund));
        assertTrue(subscription.getLiteTopicSet().contains(notifyRefundSms));
        assertEquals(1, registry.getActiveSubscriptionNum());
    }

    /**
     * Re-subscribing from pattern mode back to legacy mode (empty patterns) must clear the stored
     * patterns. Otherwise the client is misclassified as pattern-mode by
     * doFullDispatchForWildcardGroup / reexpandWildcardPatterns, which key off a non-empty
     * wildcardPatterns set, and legacy receive-all behavior is broken.
     */
    @Test
    public void testAddCompleteSubscription_PatternToLegacyTransition_ClearsPatterns() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // first subscribe in pattern mode
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertFalse(subscription.getWildcardPatterns().isEmpty());
        assertTrue(subscription.getLiteTopicSet().contains(payRefund));

        // re-subscribe in legacy mode (empty patterns): must drop patterns and revert to the
        // synthetic topic@group key.
        registry.addCompleteSubscription(clientId, group, topic,
            new HashSet<>(Collections.singletonList(payRefund)), Collections.emptySet(), 2L);
        subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getWildcardPatterns().isEmpty());
        assertFalse(subscription.getLiteTopicSet().contains(payRefund));
        assertTrue(subscription.getLiteTopicSet().contains(topic + "@" + group));
        assertEquals(1, registry.getActiveSubscriptionNum());
    }

    /**
     * getAllSubscriber for a pattern-mode wildcard group uses the normal liteTopic2Group path.
     */
    @Test
    public void testGetAllSubscriber_PatternWildcardGroup_UsesNormalPath() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        when(mockLifecycleManager.isSubscriptionActive(topic, payRefund)).thenReturn(true);

        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);

        SubscriberWrapper result = registry.getAllSubscriber(group, payRefund);
        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.ListWrapper.class, result);
        SubscriberWrapper.ListWrapper listWrapper = (SubscriberWrapper.ListWrapper) result;
        assertEquals(1, listWrapper.getClients().size());
        assertEquals(clientId, listWrapper.getClients().get(0).clientId);
        assertEquals(group, listWrapper.getClients().get(0).group);
    }

    /**
     * removeCompleteSubscription cleans up wildcardGroupMap for pattern-mode groups when the last
     * client leaves (the synthetic-key path does not fire for them).
     */
    @Test
    public void testRemoveCompleteSubscription_PatternWildcardGroupMetadataCleanup() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        when(mockLifecycleManager.isSubscriptionActive(topic, payRefund)).thenReturn(true);

        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        assertTrue(registry.wildcardGroupMap.containsKey(topic));

        registry.removeCompleteSubscription(clientId);

        assertFalse(registry.wildcardGroupMap.containsKey(topic));
        assertNull(registry.getLiteSubscription(clientId));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }

    /**
     * Backward compat: a legacy wildcard group (empty patterns) still uses the synthetic topic@group
     * key and receives all lite-topics under the parent topic.
     */
    @Test
    public void testAddCompleteSubscription_LegacyWildcardGroup_StillUsesSyntheticKey() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        Set<String> lmqNameAll = new HashSet<>(Arrays.asList("lmq1", "lmq2"));
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));

        // 5-arg overload delegates with empty patterns -> legacy mode
        registry.addCompleteSubscription(clientId, group, topic, lmqNameAll, 1L);

        assertTrue(registry.wildcardGroupMap.containsKey(topic));
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getWildcardPatterns().isEmpty());
        assertTrue(subscription.getLiteTopicSet().contains(topic + "@" + group));
        assertEquals(1, registry.getActiveSubscriptionNum());
    }

    // ==================== Subscription Quota on All Registration Paths ====================

    /**
     * addCompleteSubscription (non-wildcard group) must respect maxLiteSubscriptionCount: once the
     * broker is at the limit, a second client's complete-add is rejected with LiteQuotaException
     * (mapped to LITE_SUBSCRIPTION_QUOTA_EXCEEDED by the processor) and mutates nothing.
     */
    @Test
    public void testAddCompleteSubscription_NonWildcard_QuotaExceeded() {
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1L);
        String group = "testGroup";
        String topic = "testTopic";
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        String clientA = "clientA";
        String lmq1 = "lmq1";
        registry.addCompleteSubscription(clientA, group, topic, new HashSet<>(Collections.singletonList(lmq1)), 1L);
        assertEquals(1, registry.getActiveSubscriptionNum());
        assertTrue(registry.getLiteSubscription(clientA).getLiteTopicSet().contains(lmq1));

        // clientB's complete-add would push activeNum to 2 (> maxCount=1) -> rejected, no mutation.
        // (addCompleteSubscription creates a placeholder LiteSubscription for clientB before the
        // pre-flight check fires, but registers no lmqNames and never increments activeNum.)
        String clientB = "clientB";
        String lmq2 = "lmq2";
        assertThrows(LiteQuotaException.class, () ->
            registry.addCompleteSubscription(clientB, group, topic, new HashSet<>(Collections.singletonList(lmq2)), 1L));
        assertEquals(1, registry.getActiveSubscriptionNum());
        assertTrue(registry.getLiteSubscription(clientB).getLiteTopicSet().isEmpty());
        // clientA's subscription is intact
        assertTrue(registry.getLiteSubscription(clientA).getLiteTopicSet().contains(lmq1));
    }

    /**
     * addCompleteSubscription (pattern-mode wildcard group) must respect maxLiteSubscriptionCount:
     * when eager pattern expansion would register more lmqNames than the quota allows, the whole
     * operation is rejected up front (pre-mutation) with LiteQuotaException and registers nothing.
     */
    @Test
    public void testAddCompleteSubscription_PatternWildcard_QuotaExceeded() {
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1L);
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Arrays.asList(payRefund, paySuccess));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        Set<String> patterns = new HashSet<>(Collections.singletonList("pay__*"));
        // Both pay__refund and pay__success match -> wouldAdd=2 > maxCount=1 -> rejected pre-mutation.
        assertThrows(LiteQuotaException.class, () ->
            registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(), patterns, 1L));

        assertEquals(0, registry.getActiveSubscriptionNum());
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getLiteTopicSet().isEmpty());
        // The quota check runs before any state mutation, so a rejected complete-add must NOT leave
        // behind wildcard patterns or a wildcard-group mark that later re-expansion would mistake
        // for a live (deferred-effect) subscription.
        assertTrue(subscription.getWildcardPatterns().isEmpty());
        assertFalse(registry.wildcardGroupMap.containsKey(topic));
    }

    /**
     * reexpandWildcardPatterns caps at the quota instead of throwing: it registers newly-matched
     * lmqNames up to the limit, then stops and logs. Runs on the dispatcher background thread with
     * no client to respond to, so it must not throw. The cap guard precedes addLiteTopic so the
     * subscription's LiteTopicSet stays consistent with liteTopic2Group on early exit.
     */
    @Test
    public void testReexpandWildcardPatterns_CapsAtQuota() {
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(2L);
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success");
        String payCancelled = LiteUtil.toLmqName(topic, "pay__cancelled");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // initial subscription: only pay__refund exists -> activeNum=1 (room for 1 more)
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        assertEquals(1, registry.getActiveSubscriptionNum());

        // two new matching lite-topics now exist; re-expand has room for only one more
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(
            Arrays.asList(payRefund, paySuccess, payCancelled));
        int added = registry.reexpandWildcardPatterns(clientId);

        assertEquals(1, added); // added paySuccess, then capped before payCancelled
        assertEquals(2, registry.getActiveSubscriptionNum());
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getLiteTopicSet().contains(payRefund));
        assertTrue(subscription.getLiteTopicSet().contains(paySuccess));
        assertFalse(subscription.getLiteTopicSet().contains(payCancelled));
    }

    /**
     * registerArrivingLmqForPatternClients caps at the quota instead of throwing: when the broker is
     * already at the limit on the message-arriving hot path, it registers nothing and logs rather than
     * throwing (an exception would abort per-message dispatch). Existing subscriptions are unaffected.
     */
    @Test
    public void testRegisterArrivingLmqForPatternClients_CapsAtQuota() {
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(2L);
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // two pattern clients subscribed when only pay__refund exists -> activeNum=2 (at limit)
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        Set<String> patterns = new HashSet<>(Collections.singletonList("pay__*"));
        registry.addCompleteSubscription("clientA", group, topic, Collections.emptySet(), patterns, 1L);
        registry.addCompleteSubscription("clientB", group, topic, Collections.emptySet(), patterns, 1L);
        assertEquals(2, registry.getActiveSubscriptionNum());

        // a new matching lite-topic arrives while at the limit -> nothing registered, no throw
        int added = registry.registerArrivingLmqForPatternClients(paySuccess);

        assertEquals(0, added);
        assertEquals(2, registry.getActiveSubscriptionNum());
        assertFalse(registry.getLiteSubscription("clientA").getLiteTopicSet().contains(paySuccess));
        assertFalse(registry.getLiteSubscription("clientB").getLiteTopicSet().contains(paySuccess));
    }

    /**
     * reexpandWildcardPatterns picks up lite-topics created after the initial subscription.
     */
    @Test
    public void testReexpandWildcardPatterns_PicksUpNewlyCreatedLmq() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // initial subscription: only pay__refund exists
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        assertEquals(1, registry.getActiveSubscriptionNum());

        // a new matching lite-topic pay__success is created
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Arrays.asList(payRefund, paySuccess));
        int added = registry.reexpandWildcardPatterns(clientId);

        assertEquals(1, added);
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getLiteTopicSet().contains(payRefund));
        assertTrue(subscription.getLiteTopicSet().contains(paySuccess));
        assertEquals(2, registry.getActiveSubscriptionNum());
    }

    /**
     * registerArrivingLmqForPatternClients is the dispatch-time single-lmqName counterpart to
     * reexpandWildcardPatterns. When a message arrives on a newly-created lite-topic that a
     * pattern-mode client matches but was not expanded to at subscribe time, it registers the
     * (client, lmqName) pair so the dispatch can reach the client immediately — without the O(M)
     * collectByParentTopic scan.
     */
    @Test
    public void testRegisterArrivingLmqForPatternClients_RegistersNewlyCreatedMatchingLmq() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success"); // created after subscribe
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // initial subscription: only pay__refund exists at expand time
        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        assertEquals(1, registry.getActiveSubscriptionNum());

        // a new matching lite-topic pay__success arrives — single-lmqName match, no re-scan
        int added = registry.registerArrivingLmqForPatternClients(paySuccess);

        assertEquals(1, added);
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertTrue(subscription.getLiteTopicSet().contains(payRefund));
        assertTrue(subscription.getLiteTopicSet().contains(paySuccess));
        assertEquals(2, registry.getActiveSubscriptionNum());
        // the new lmqName is now reachable via the normal message-arriving (group == null) path
        SubscriberWrapper result = registry.getAllSubscriber(null, paySuccess);
        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.MapWrapper.class, result);
        assertTrue(((SubscriberWrapper.MapWrapper) result).getGroupMap().containsKey(group));
    }

    /**
     * registerArrivingLmqForPatternClients does not register a lmqName whose child does not match
     * any of the pattern client's patterns.
     */
    @Test
    public void testRegisterArrivingLmqForPatternClients_SkipsNonMatchingLmq() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String orderCreated = LiteUtil.toLmqName(topic, "order__created"); // does not match pay__*
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);

        int added = registry.registerArrivingLmqForPatternClients(orderCreated);

        assertEquals(0, added);
        assertFalse(registry.getLiteSubscription(clientId).getLiteTopicSet().contains(orderCreated));
        assertEquals(1, registry.getActiveSubscriptionNum());
    }

    /**
     * registerArrivingLmqForPatternClients is idempotent: re-arrival on an lmqName already
     * registered at subscribe time registers nothing new.
     */
    @Test
    public void testRegisterArrivingLmqForPatternClients_IdempotentOnRepeatArrival() {
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Collections.singletonList(payRefund));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);

        int added = registry.registerArrivingLmqForPatternClients(payRefund);
        assertEquals(0, added); // already registered at subscribe time
        assertEquals(1, registry.getActiveSubscriptionNum());
    }

    /**
     * registerArrivingLmqForPatternClients ignores legacy wildcard clients (empty patterns); they
     * are reachable via the synthetic topic@group key, not the pattern path, and must not be
     * double-registered.
     */
    @Test
    public void testRegisterArrivingLmqForPatternClients_IgnoresLegacyClients() {
        String legacyClientId = "legacyClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // legacy wildcard client (empty patterns -> synthetic key, receives all)
        registry.addCompleteSubscription(legacyClientId, group, topic, Collections.emptySet(), 1L);

        int added = registry.registerArrivingLmqForPatternClients(payRefund);
        assertEquals(0, added); // legacy client has empty wildcardPatterns, skipped
        // legacy client still reachable via the synthetic key, unaffected
        assertEquals(1, registry.getActiveSubscriptionNum());
        SubscriberWrapper result = registry.getAllSubscriber(group, payRefund);
        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.ListWrapper.class, result);
        Set<String> deliveredClientIds = new HashSet<>();
        for (ClientGroup cg : ((SubscriberWrapper.ListWrapper) result).getClients()) {
            deliveredClientIds.add(cg.clientId);
        }
        assertTrue(deliveredClientIds.contains(legacyClientId));
    }

    /**
     * A mixed wildcard group (one pattern-mode client + one legacy client) must deliver to BOTH:
     * the pattern-mode client (via its real lmqName in liteTopic2Group) and the legacy client (via
     * the synthetic topic@group key). getAllSubscriber must merge the two sets.
     */
    @Test
    public void testGetAllSubscriber_MixedWildcardGroup_DeliversToBothPatternAndLegacy() {
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String unmatched = LiteUtil.toLmqName(topic, "order__created");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Arrays.asList(payRefund, unmatched));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // pattern-mode client subscribes pay__*
        registry.addCompleteSubscription("patternClient", group, topic, Collections.emptySet(),
            new HashSet<>(Collections.singletonList("pay__*")), 1L);
        // legacy client subscribes (empty patterns -> synthetic key, receives all)
        registry.addCompleteSubscription("legacyClient", group, topic, Collections.emptySet(), 1L);

        // For a topic the pattern client matched: both clients should be returned.
        SubscriberWrapper result = registry.getAllSubscriber(group, payRefund);
        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.ListWrapper.class, result);
        Set<String> deliveredClientIds = new HashSet<>();
        for (ClientGroup cg : ((SubscriberWrapper.ListWrapper) result).getClients()) {
            deliveredClientIds.add(cg.clientId);
        }
        assertTrue("pattern-mode client must be delivered", deliveredClientIds.contains("patternClient"));
        assertTrue("legacy client must be delivered", deliveredClientIds.contains("legacyClient"));

        // For a topic the pattern client did NOT match: only the legacy client (receive-all).
        SubscriberWrapper unmatchedResult = registry.getAllSubscriber(group, unmatched);
        Set<String> unmatchedIds = new HashSet<>();
        for (ClientGroup cg : ((SubscriberWrapper.ListWrapper) unmatchedResult).getClients()) {
            unmatchedIds.add(cg.clientId);
        }
        assertFalse("pattern client must not receive unmatched topic", unmatchedIds.contains("patternClient"));
        assertTrue("legacy client must still receive unmatched topic", unmatchedIds.contains("legacyClient"));
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
        assertTrue(subscription.getLiteTopicSet().contains("lmq1"));
        assertTrue(subscription.getLiteTopicSet().contains("lmq2"));
        assertEquals(2, registry.getActiveSubscriptionNum());

        // Update subscription
        registry.addCompleteSubscription(clientId, group, topic, lmqNameNew, 2L);

        subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertFalse(subscription.getLiteTopicSet().contains("lmq1"));
        assertTrue(subscription.getLiteTopicSet().contains("lmq2"));
        assertTrue(subscription.getLiteTopicSet().contains("lmq3"));
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
        assertTrue(subscription.getLiteTopicSet().contains("lmq1"));
        assertTrue(subscription.getLiteTopicSet().contains("lmq2"));
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
    public void testGetAllSubscriber_WildcardGroup() {
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName = topic + "@" + group;

        // Simulate wildcard group
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setWildcardLiteGroup(true);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        SubscriberWrapper result = registry.getAllSubscriber(group, lmqName);

        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.ListWrapper.class, result);
    }

    /**
     * Test getAllSubscriber gets subscribers for specific group
     */
    @Test
    public void testGetAllSubscriber_SpecificGroup() {
        String clientId = "testClient";
        String group = "testGroup";
        String lmqName = "lmq1";

        // Add subscription
        ClientGroup clientGroup = new ClientGroup(clientId, group);
        Set<ClientGroup> clientSet = ConcurrentHashMap.newKeySet();
        clientSet.add(clientGroup);
        registry.liteTopic2Group.put(lmqName, clientSet);

        SubscriberWrapper result = registry.getAllSubscriber(group, lmqName);

        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.ListWrapper.class, result);
        SubscriberWrapper.ListWrapper listWrapper = (SubscriberWrapper.ListWrapper) result;
        assertEquals(1, listWrapper.getClients().size());
        assertEquals(clientId, listWrapper.getClients().get(0).clientId);
        assertEquals(group, listWrapper.getClients().get(0).group);
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
        registry.liteTopic2Group.put(lmqName, clientSet);

        SubscriberWrapper result = registry.getAllSubscriber(null, lmqName);

        assertNotNull(result);
        assertInstanceOf(SubscriberWrapper.MapWrapper.class, result);
        SubscriberWrapper.MapWrapper mapWrapper = (SubscriberWrapper.MapWrapper) result;
        assertEquals(2, mapWrapper.getGroupMap().size());
        assertTrue(mapWrapper.getGroupMap().containsKey(group1));
        assertTrue(mapWrapper.getGroupMap().containsKey(group2));
        assertEquals(1, mapWrapper.getGroupMap().get(group1).size());
        assertEquals(1, mapWrapper.getGroupMap().get(group2).size());
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
        registry.liteTopic2Group.put(lmqName, clientSet);

        LiteSubscription subscription = new LiteSubscription();
        subscription.setGroup(group);
        subscription.addLiteTopic(lmqName);
        registry.client2Subscription.put(clientId, subscription);
        registry.activeNum.set(1);

        registry.cleanSubscription(lmqName, false);

        assertFalse(registry.liteTopic2Group.containsKey(lmqName));
        assertFalse(subscription.getLiteTopicSet().contains(lmqName));
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

    // ==================== Quota projection: net delta & no state leak on failure ====================

    /**
     * wouldAdd must credit pending removals: a complete-add that removes one lmqName and adds a
     * different one has a NET delta of zero, so it must NOT be rejected at the quota limit even
     * though the gross "add" count is 1. Before crediting removals this net-zero replace was
     * falsely rejected when the broker was full.
     */
    @Test
    public void testAddCompleteSubscription_NetZeroReplaceAtQuotaNotRejected() {
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1L);
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "testTopic";
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        // client holds lmq1 -> activeNum=1 (at the limit)
        String lmq1 = "lmq1";
        String lmq2 = "lmq2";
        registry.addCompleteSubscription(clientId, group, topic, new HashSet<>(Collections.singletonList(lmq1)), 1L);
        assertEquals(1, registry.getActiveSubscriptionNum());

        // Full re-sync swaps lmq1 -> lmq2. Gross add = 1, but net delta = 0 (lmq1 removed). Must pass.
        registry.addCompleteSubscription(clientId, group, topic, new HashSet<>(Collections.singletonList(lmq2)), 2L);

        // Still at the limit, now holding lmq2 (lmq1 dropped).
        assertEquals(1, registry.getActiveSubscriptionNum());
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertFalse(subscription.getLiteTopicSet().contains(lmq1));
        assertTrue(subscription.getLiteTopicSet().contains(lmq2));
    }

    /**
     * A rejected pattern-mode complete-add must leave NO live subscription state behind — not just
     * an empty liteTopicSet, but empty wildcardPatterns and no wildcard-group mark — so the periodic
     * reexpandWildcardPatterns / message-arriving registerArrivingLmqForPatternClients paths do not
     * pick up the failed client and turn it into a deferred-effect subscription. This is the core of
     * the review's "reexpand may turn a failed subscription into a delayed-effect subscription".
     */
    @Test
    public void testAddCompleteSubscription_PatternQuotaFailure_NoDeferredState() {
        when(mockBrokerConfig.getMaxLiteSubscriptionCount()).thenReturn(1L);
        String clientId = "testClient";
        String group = "testGroup";
        String topic = "order_events";
        String payRefund = LiteUtil.toLmqName(topic, "pay__refund");
        String paySuccess = LiteUtil.toLmqName(topic, "pay__success");
        when(mockSubscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(wildcardGroupConfig(group));
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Arrays.asList(payRefund, paySuccess));
        when(mockLifecycleManager.isSubscriptionActive(eq(topic), anyString())).thenReturn(true);

        Set<String> patterns = new HashSet<>(Collections.singletonList("pay__*"));
        // wouldAdd=2 > maxCount=1 -> rejected. Critically, this runs before setWildcardPatterns /
        // markWildcardGroup, so no patterns or group mark are persisted.
        assertThrows(LiteQuotaException.class, () ->
            registry.addCompleteSubscription(clientId, group, topic, Collections.emptySet(), patterns, 1L));

        // No active references, and no pattern/group state a later re-expand could act on.
        assertEquals(0, registry.getActiveSubscriptionNum());
        LiteSubscription subscription = registry.getLiteSubscription(clientId);
        assertNotNull(subscription);
        assertTrue(subscription.getWildcardPatterns().isEmpty());
        assertFalse(registry.wildcardGroupMap.containsKey(topic));

        // Consequence: a later re-expand of this client registers nothing (it short-circuits on
        // empty wildcardPatterns), and an arriving matching lmqName does not pick the client up.
        when(mockLifecycleManager.collectByParentTopic(topic)).thenReturn(Arrays.asList(payRefund, paySuccess));
        assertEquals(0, registry.reexpandWildcardPatterns(clientId));
        assertEquals(0, registry.registerArrivingLmqForPatternClients(paySuccess));
        assertEquals(0, registry.getActiveSubscriptionNum());
    }
}
