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
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.store.MessageStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteLifecycleManagerTest {

    private final static BrokerConfig BROKER_CONFIG = new BrokerConfig();
    private final static ConcurrentMap<String, TopicConfig> TOPIC_CONFIG_TABLE = new ConcurrentHashMap<>();
    private static String storePathRootDir;
    private static MessageStore messageStore;
    private static LiteLifecycleManager liteLifecycleManager;
    private static TopicConfig mockTopicConfig = new TopicConfig();

    @BeforeClass
    public static void setUp() throws Exception {
        storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-lifecycleTest";
        UtilAll.deleteFile(new File(storePathRootDir));

        messageStore = LiteTestUtil.buildMessageStore(storePathRootDir, BROKER_CONFIG, TOPIC_CONFIG_TABLE, false);
        messageStore.load();
        messageStore.start();

        BrokerController brokerController = Mockito.mock(BrokerController.class);
        LiteSharding liteSharding = Mockito.mock(LiteSharding.class);
        TopicConfigManager topicConfigManager = Mockito.mock(TopicConfigManager.class);
        SubscriptionGroupManager subscriptionGroupManager = Mockito.mock(SubscriptionGroupManager.class);

        when(brokerController.getBrokerConfig()).thenReturn(BROKER_CONFIG);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(topicConfigManager.getTopicConfigTable()).thenReturn(TOPIC_CONFIG_TABLE);
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(mockTopicConfig);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(new ConcurrentHashMap<>());

        LiteLifecycleManager testObject = new LiteLifecycleManager(brokerController, liteSharding);
        liteLifecycleManager = Mockito.spy(testObject);
        liteLifecycleManager.init();
    }

    @AfterClass
    public static void reset() {
        messageStore.shutdown();
        messageStore.destroy();
        UtilAll.deleteFile(new File(storePathRootDir));
    }

    @Test
    public void testGetMaxOffsetInQueue() {
        int num = 3;
        String topic = UUID.randomUUID().toString();
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(topic, null));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);
        Assert.assertEquals(num, liteLifecycleManager.getMaxOffsetInQueue(topic));
        Assert.assertEquals(0, liteLifecycleManager.getMaxOffsetInQueue(UUID.randomUUID().toString()));
    }

    @Test
    public void testCollectByParentTopic() {
        int num = 3;
        String parentTopic = UUID.randomUUID().toString();
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(parentTopic, UUID.randomUUID().toString()));
            messageStore.putMessage(LiteTestUtil.buildMessage(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);
        List<String> result = liteLifecycleManager.collectByParentTopic(parentTopic);
        Assert.assertEquals(num, result.size());
        for (String lmqName : result) {
            Assert.assertTrue(LiteUtil.belongsTo(lmqName, parentTopic));
        }

        result = liteLifecycleManager.collectByParentTopic(UUID.randomUUID().toString());
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testCollectExpiredLiteTopic() {
        int num = 3;
        String parentTopic = UUID.randomUUID().toString();
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(parentTopic, UUID.randomUUID().toString()));
            messageStore.putMessage(LiteTestUtil.buildMessage(UUID.randomUUID().toString(), null));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);

        when(liteLifecycleManager.isLiteTopicExpired(anyString(), anyString(), anyLong())).thenReturn(false);
        List<Pair<String, String>> result = liteLifecycleManager.collectExpiredLiteTopic();
        Assert.assertEquals(0, result.size());

        when(liteLifecycleManager.isLiteTopicExpired(eq(parentTopic), anyString(), anyLong())).thenReturn(true);
        result = liteLifecycleManager.collectExpiredLiteTopic();
        Assert.assertEquals(num, result.size());
        for (Pair<String, String> pair : result) {
            Assert.assertEquals(parentTopic, pair.getObject1());
            Assert.assertTrue(LiteUtil.belongsTo(pair.getObject2(), parentTopic));
        }
    }

    @Ignore
    @Test
    public void testCleanExpiredLiteTopic() {
        int num = 3;
        String parentTopic = UUID.randomUUID().toString();
        List<String> liteTopics =
            IntStream.range(0, 3).mapToObj(i -> UUID.randomUUID().toString()).collect(Collectors.toList());
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(parentTopic, liteTopics.get(i)));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertTrue(messageStore.getQueueStore().getConsumeQueueTable().containsKey(lmqName));
        }

        when(liteLifecycleManager.isLiteTopicExpired(eq(parentTopic), anyString(), anyLong())).thenReturn(true);
        liteLifecycleManager.cleanExpiredLiteTopic();

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertFalse(messageStore.getQueueStore().getConsumeQueueTable().containsKey(lmqName));
        }
    }

    @Test
    public void testCleanByParentTopic() {
        int num = 3;
        String parentTopic = UUID.randomUUID().toString();
        mockTopicConfig.getAttributes().put(
            TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.LITE.getValue());

        List<String> liteTopics =
            IntStream.range(0, 3).mapToObj(i -> UUID.randomUUID().toString()).collect(Collectors.toList());
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(parentTopic, liteTopics.get(i)));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertTrue(messageStore.getQueueStore().getConsumeQueueTable().containsKey(lmqName));
        }

        liteLifecycleManager.cleanByParentTopic(parentTopic);

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertFalse(messageStore.getQueueStore().getConsumeQueueTable().containsKey(lmqName));
        }
    }
}
