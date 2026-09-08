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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import java.util.function.Function;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.processor.PopLiteMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager.MAX_INVALID_SCAN_COUNT;
import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeastOnce;

@RunWith(MockitoJUnitRunner.class)
public class AbstractLiteLifecycleManagerTest {
    private static final String PARENT_TOPIC = "parentTopic";
    private static final String EXIST_LMQ_NAME = LiteUtil.toLmqName(PARENT_TOPIC, "HW");
    private static final String GROUP = "group";

    @Mock
    private BrokerController brokerController;
    @Mock
    private LiteSharding liteSharding;
    @Mock
    private MessageStore messageStore;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;
    @Mock
    private RocksDBConsumerOffsetManager consumerOffsetManager;
    @Mock
    private PopLiteMessageProcessor popLiteMessageProcessor;
    @Mock
    private ConsumerOrderInfoManager consumerOrderInfoManager;
    @Mock
    private LiteSubscriptionRegistry liteSubscriptionRegistry;

    private TestLiteLifecycleManager lifecycleManager;
    private BrokerConfig brokerConfig;

    private final TopicConfig topicConfig = new TopicConfig(PARENT_TOPIC, 1, 1);
    private final SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
    private final ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

    @Before
    public void setUp() {
        brokerConfig = new BrokerConfig();
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getPopLiteMessageProcessor()).thenReturn(popLiteMessageProcessor);
        when(popLiteMessageProcessor.getConsumerOrderInfoManager()).thenReturn(consumerOrderInfoManager);
        when(brokerController.getLiteSubscriptionRegistry()).thenReturn(liteSubscriptionRegistry);

        topicConfig.getAttributes().put(
            TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.LITE.getValue());
        topicConfig.setLiteTopicExpiration(1);
        topicConfigTable.put(PARENT_TOPIC, topicConfig);
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicConfigTable);
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);

        groupConfig.setGroupName(GROUP);
        groupConfig.setLiteBindTopic(PARENT_TOPIC);
        ConcurrentMap<String, SubscriptionGroupConfig> groupTable = new ConcurrentHashMap<>();
        groupTable.put(GROUP, groupConfig);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(groupTable);

        when(consumerOffsetManager.getOffsetTable()).thenReturn(offsetTable);
        when(consumerOffsetManager.getPullOffsetTable()).thenReturn(offsetTable);

        TestLiteLifecycleManager testObject = new TestLiteLifecycleManager(brokerController, liteSharding);
        lifecycleManager = Mockito.spy(testObject);
        lifecycleManager.init();
        lifecycleManager.lmqPrefixIndex.add(EXIST_LMQ_NAME);
    }

    @After
    public void reset() {
        topicConfig.getAttributes().clear();
        groupConfig.getAttributes().clear();
        offsetTable.clear();
        topicConfigTable.clear();
    }

    @Test
    public void testIsSubscriptionActive() {
        when(liteSharding.shardingByLmqName(PARENT_TOPIC, EXIST_LMQ_NAME)).thenReturn(brokerConfig.getBrokerName());
        Assert.assertTrue(lifecycleManager.isSubscriptionActive(PARENT_TOPIC, EXIST_LMQ_NAME));
        Assert.assertFalse(lifecycleManager.isSubscriptionActive("whatever", "whatever"));

        when(liteSharding.shardingByLmqName(anyString(), anyString())).thenReturn(brokerConfig.getBrokerName());
        Assert.assertTrue(lifecycleManager.isSubscriptionActive(PARENT_TOPIC, EXIST_LMQ_NAME));
        Assert.assertTrue(lifecycleManager.isSubscriptionActive("whatever", "whatever"));

        when(liteSharding.shardingByLmqName(anyString(), anyString())).thenReturn("otherBrokerName");
        Assert.assertTrue(lifecycleManager.isSubscriptionActive(PARENT_TOPIC, EXIST_LMQ_NAME));
        Assert.assertFalse(lifecycleManager.isSubscriptionActive("whatever", "whatever"));
    }

    @Test
    public void testIsLmqExist() {
        Assert.assertTrue(lifecycleManager.isLmqExist(EXIST_LMQ_NAME));
        Assert.assertFalse(lifecycleManager.isLmqExist("whatever"));
    }

    @Test
    public void testGetLiteTopicCount() {
        Assert.assertEquals(1, lifecycleManager.getLiteTopicCount(PARENT_TOPIC));
        Assert.assertEquals(0, lifecycleManager.getLiteTopicCount("whatever"));

        // parentTopic1: 2 liteTopics, parentTopic2: 3 liteTopics
        String parent1 = "parentTopic1";
        String parent2 = "parentTopic2";
        registerLiteTopicConfig(parent1);
        registerLiteTopicConfig(parent2);
        lifecycleManager.lmqPrefixIndex.add(LiteUtil.toLmqName(parent1, "sub1"));
        lifecycleManager.lmqPrefixIndex.add(LiteUtil.toLmqName(parent1, "sub2"));
        lifecycleManager.lmqPrefixIndex.add(LiteUtil.toLmqName(parent2, "sub1"));
        lifecycleManager.lmqPrefixIndex.add(LiteUtil.toLmqName(parent2, "sub2"));
        lifecycleManager.lmqPrefixIndex.add(LiteUtil.toLmqName(parent2, "sub3"));

        Assert.assertEquals(2, lifecycleManager.getLiteTopicCount(parent1));
        Assert.assertEquals(3, lifecycleManager.getLiteTopicCount(parent2));
        // PARENT_TOPIC count unchanged
        Assert.assertEquals(1, lifecycleManager.getLiteTopicCount(PARENT_TOPIC));
    }

    @Test
    public void testIsLiteTopicExpired() {
        // not lite topic queue
        Assert.assertFalse(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, "whatever", 10L));

        // maxOffset invalid
        for (int i = 0; i < MAX_INVALID_SCAN_COUNT; i++) {
            Assert.assertFalse(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 0L));
        }
        Assert.assertTrue(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 0L));

        // storeTime invalid
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(-1L);
        for (int i = 0; i < MAX_INVALID_SCAN_COUNT; i++) {
            Assert.assertFalse(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 100L));
        }
        Assert.assertTrue(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 100L));

        // less than minLiteTTl
        long mockStoreTime = System.currentTimeMillis();
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(mockStoreTime);
        Assert.assertFalse(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 100L));

        // topic ttl not found
        mockStoreTime = System.currentTimeMillis() - brokerConfig.getMinLiteTTl() - 2000;
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(mockStoreTime);
        Assert.assertFalse(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 100L));

        // topic ttl no expiration
        topicConfig.getAttributes().put(TopicAttributes.LITE_EXPIRATION_ATTRIBUTE.getName(), "-1");
        lifecycleManager.updateMetadata();
        mockStoreTime = System.currentTimeMillis() - brokerConfig.getMinLiteTTl() - 2000;
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(mockStoreTime);
        Assert.assertFalse(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 100L));

        // topic ttl expired
        topicConfig.getAttributes().put(
            TopicAttributes.LITE_EXPIRATION_ATTRIBUTE.getName(), "" + brokerConfig.getMinLiteTTl() / 1000 / 60);
        lifecycleManager.updateMetadata();
        mockStoreTime = System.currentTimeMillis() - brokerConfig.getMinLiteTTl() - 2000;
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong())).thenReturn(mockStoreTime);
        Assert.assertTrue(lifecycleManager.isLiteTopicExpired(PARENT_TOPIC, EXIST_LMQ_NAME, 100L));
    }

    @Test
    public void testDeleteLmq() {
        lifecycleManager.updateMetadata();
        String otherKey = "otherTopic@otherGroup";
        String removeKey = EXIST_LMQ_NAME + TOPIC_GROUP_SEPARATOR + GROUP;
        offsetTable.put(otherKey, new ConcurrentHashMap<>());
        offsetTable.put(removeKey, new ConcurrentHashMap<>());

        // sharding to this broker
        when(liteSharding.shardingByLmqName(PARENT_TOPIC, EXIST_LMQ_NAME)).thenReturn(brokerConfig.getBrokerName());
        lifecycleManager.deleteLmq(PARENT_TOPIC, EXIST_LMQ_NAME);

        Assert.assertTrue(offsetTable.containsKey(otherKey));
        Assert.assertFalse(offsetTable.containsKey(removeKey));
        verify(consumerOffsetManager).removeConsumerOffset(removeKey);
        verify(messageStore).deleteTopics(Collections.singleton(EXIST_LMQ_NAME));
        verify(liteSubscriptionRegistry).cleanSubscription(EXIST_LMQ_NAME, false);
        verify(consumerOrderInfoManager, times(1)).remove(EXIST_LMQ_NAME, GROUP);

        // not sharding to this broker
        when(liteSharding.shardingByLmqName(PARENT_TOPIC, EXIST_LMQ_NAME)).thenReturn("otherBrokerName");
        lifecycleManager.deleteLmq(PARENT_TOPIC, EXIST_LMQ_NAME);

        Assert.assertTrue(offsetTable.containsKey(otherKey));
        Assert.assertFalse(offsetTable.containsKey(removeKey));
        verify(consumerOffsetManager, times(2)).removeConsumerOffset(removeKey);
        verify(messageStore, times(2)).deleteTopics(Collections.singleton(EXIST_LMQ_NAME));
        verify(liteSubscriptionRegistry, times(2)).cleanSubscription(EXIST_LMQ_NAME, false);
    }

    @Test
    public void testCleanExpiredLiteTopic() {
        String removeKey = EXIST_LMQ_NAME + TOPIC_GROUP_SEPARATOR + GROUP;
        when(liteSharding.shardingByLmqName(PARENT_TOPIC, EXIST_LMQ_NAME)).thenReturn(brokerConfig.getBrokerName());
        brokerConfig.setMinLiteTTl(0);
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong()))
            .thenReturn(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10));

        lifecycleManager.cleanExpiredLiteTopic();
        verify(consumerOffsetManager).removeConsumerOffset(removeKey);
        verify(messageStore).deleteTopics(Collections.singleton(EXIST_LMQ_NAME));
        verify(liteSubscriptionRegistry).cleanSubscription(EXIST_LMQ_NAME, false);
    }

    @Test
    public void testCleanByParentTopic() {
        String lmq1 = LiteUtil.toLmqName(PARENT_TOPIC, "sub1");
        String lmq2 = LiteUtil.toLmqName(PARENT_TOPIC, "sub2");
        String lmq3 = LiteUtil.toLmqName(PARENT_TOPIC, "sub3");

        String otherLmq1 = LiteUtil.toLmqName("otherParentTopic", "sub1");
        String otherLmq2 = LiteUtil.toLmqName("otherParentTopic", "sub2");

        // multiple LMQs: deleteLmq called only for LMQs under PARENT_TOPIC
        lifecycleManager.lmqPrefixIndex.remove(EXIST_LMQ_NAME);
        lifecycleManager.lmqPrefixIndex.add(lmq1);
        lifecycleManager.lmqPrefixIndex.add(lmq2);
        lifecycleManager.lmqPrefixIndex.add(lmq3);
        lifecycleManager.lmqPrefixIndex.add(otherLmq1);
        lifecycleManager.lmqPrefixIndex.add(otherLmq2);

        ArgumentCaptor<String> parentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> lmqCaptor = ArgumentCaptor.forClass(String.class);
        lifecycleManager.cleanByParentTopic(PARENT_TOPIC);
        verify(lifecycleManager, times(3)).deleteLmq(parentCaptor.capture(), lmqCaptor.capture());
        Assert.assertTrue(parentCaptor.getAllValues().stream().allMatch(PARENT_TOPIC::equals));
        Assert.assertEquals(new HashSet<>(Arrays.asList(lmq1, lmq2, lmq3)), new HashSet<>(lmqCaptor.getAllValues()));

        // other parent's LMQs remain untouched
        List<String> otherResult = lifecycleManager.collectByParentTopic("otherParentTopic");
        Assert.assertEquals(new HashSet<>(Arrays.asList(otherLmq1, otherLmq2)), new HashSet<>(otherResult));

        // zero LMQs: deleteLmq not called
        Mockito.clearInvocations(lifecycleManager);
        lifecycleManager.cleanByParentTopic(PARENT_TOPIC);
        verify(lifecycleManager, never()).deleteLmq(anyString(), anyString());

        // guard: non-lite topic and null both return early
        Mockito.clearInvocations(lifecycleManager);
        lifecycleManager.lmqPrefixIndex.add(EXIST_LMQ_NAME);
        lifecycleManager.cleanByParentTopic("nonExistentTopic");
        verify(lifecycleManager, never()).deleteLmq(anyString(), anyString());
        lifecycleManager.cleanByParentTopic(null);
        verify(lifecycleManager, never()).deleteLmq(anyString(), anyString());
    }

    @Test
    public void testCollectByParentTopic() {
        String lmq1 = LiteUtil.toLmqName(PARENT_TOPIC, "sub1");
        String lmq2 = LiteUtil.toLmqName(PARENT_TOPIC, "sub2");
        String lmq3 = LiteUtil.toLmqName(PARENT_TOPIC, "sub3");

        String otherLmq1 = LiteUtil.toLmqName("otherParentTopic", "sub1");
        String otherLmq2 = LiteUtil.toLmqName("otherParentTopic", "sub2");

        lifecycleManager.lmqPrefixIndex.remove(EXIST_LMQ_NAME);
        lifecycleManager.lmqPrefixIndex.add(lmq1);
        lifecycleManager.lmqPrefixIndex.add(lmq2);
        lifecycleManager.lmqPrefixIndex.add(lmq3);
        lifecycleManager.lmqPrefixIndex.add(otherLmq1);
        lifecycleManager.lmqPrefixIndex.add(otherLmq2);

        // multiple LMQs: returns only those under PARENT_TOPIC, excluding other parent's
        List<String> result = lifecycleManager.collectByParentTopic(PARENT_TOPIC);
        Assert.assertEquals(new HashSet<>(Arrays.asList(lmq1, lmq2, lmq3)), new HashSet<>(result));

        // no LMQs under parent: returns empty list
        result = lifecycleManager.collectByParentTopic("nonExistentTopic");
        Assert.assertTrue(result.isEmpty());

        // guard: null and empty both return empty list
        result = lifecycleManager.collectByParentTopic(null);
        Assert.assertTrue(result.isEmpty());
        result = lifecycleManager.collectByParentTopic("");
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testRun() throws InterruptedException {
        brokerConfig.setLiteTtlCheckInterval(100L);
        brokerConfig.setMinLiteTTl(0);
        when(liteSharding.shardingByLmqName(PARENT_TOPIC, EXIST_LMQ_NAME)).thenReturn(brokerConfig.getBrokerName());
        when(messageStore.getMessageStoreTimeStamp(anyString(), anyInt(), anyLong()))
            .thenReturn(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10));
        lifecycleManager.start();
        Thread.sleep(300);
        lifecycleManager.shutdown();

        verify(consumerOffsetManager, atLeastOnce()).removeConsumerOffset(anyString());
        verify(messageStore, atLeastOnce()).deleteTopics(Collections.singleton(EXIST_LMQ_NAME));
        verify(liteSubscriptionRegistry, atLeastOnce()).cleanSubscription(EXIST_LMQ_NAME, false);
    }

    private void registerLiteTopicConfig(String parentTopic) {
        TopicConfig config = new TopicConfig(parentTopic, 1, 1);
        config.getAttributes().put(
            TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.LITE.getValue());
        topicConfigTable.put(parentTopic, config);
        when(topicConfigManager.selectTopicConfig(parentTopic)).thenReturn(config);
    }

    private static class TestLiteLifecycleManager extends AbstractLiteLifecycleManager {

        public TestLiteLifecycleManager(BrokerController brokerController, LiteSharding liteSharding) {
            super(brokerController, liteSharding);
        }

        @Override
        public long getMaxOffsetInQueue(String lmqName) {
            return LiteUtil.isLiteTopicQueue(lmqName) ? 100 : -1;
        }

        @Override
        public void forEachLiteTopic(Function<Triple<String, Long, Long>, Boolean> function) {
            List<Triple<String, Long, Long>> triples = new ArrayList<>();
            lmqPrefixIndex.forEachLmqByPrefix(LiteUtil.LITE_TOPIC_PREFIX, lmqName -> {
                long maxOffset = getMaxOffsetInQueue(lmqName);
                if (maxOffset > 0) {
                    triples.add(Triple.of(lmqName, maxOffset, null));
                }
                return true;
            });
            for (Triple<String, Long, Long> triple : triples) {
                if (!function.apply(triple)) {
                    break;
                }
            }
        }
    }
}
