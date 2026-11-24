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
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.plugin.AbstractPluginMessageStore;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.store.queue.AbstractConsumeQueueStore;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.Collections;
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
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RocksDBLiteLifecycleManagerTest {

    private final static BrokerConfig BROKER_CONFIG = new BrokerConfig();
    private final static ConcurrentMap<String, TopicConfig> TOPIC_CONFIG_TABLE = new ConcurrentHashMap<>();
    private static String storePathRootDir;
    private static MessageStore messageStore;
    private static RocksDBLiteLifecycleManager liteLifecycleManager;
    private static LiteEventDispatcher liteEventDispatcher;
    private static TopicConfig mockTopicConfig = new TopicConfig();

    @BeforeClass
    public static void setUp() throws Exception {
        storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-rocksDBLifecycleTest";
        UtilAll.deleteFile(new File(storePathRootDir));

        BrokerController brokerController = Mockito.mock(BrokerController.class);
        LiteSharding liteSharding = Mockito.mock(LiteSharding.class);
        TopicConfigManager topicConfigManager = Mockito.mock(TopicConfigManager.class);
        SubscriptionGroupManager subscriptionGroupManager = Mockito.mock(SubscriptionGroupManager.class);
        LiteSubscriptionRegistry liteSubscriptionRegistry = Mockito.mock(LiteSubscriptionRegistry.class);
        ConsumerOffsetManager consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());
        when(consumerOffsetManager.getPullOffsetTable()).thenReturn(new ConcurrentHashMap<>());
        // dispatch() fans out to subscribers right after maintaining the prefix index; no subscriber here
        when(liteSubscriptionRegistry.getAllSubscribers(nullable(String.class), anyString()))
            .thenReturn(Collections.emptyMap());

        when(brokerController.getBrokerConfig()).thenReturn(BROKER_CONFIG);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getLiteSubscriptionRegistry()).thenReturn(liteSubscriptionRegistry);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(topicConfigManager.getTopicConfigTable()).thenReturn(TOPIC_CONFIG_TABLE);
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(mockTopicConfig);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(new ConcurrentHashMap<>());

        RocksDBLiteLifecycleManager testObject = new RocksDBLiteLifecycleManager(brokerController, liteSharding);
        liteLifecycleManager = Mockito.spy(testObject);

        // Wire the real notify path so putMessage drives the prefix index. RocksDB CQ is committed by
        // RocksGroupCommitService (isNotifyMessageArriveWhenReput()==false), but it still lands on the same
        // MessageArrivingListener -> LiteEventDispatcher.dispatch -> onLmqCreate path.
        liteEventDispatcher = new LiteEventDispatcher(brokerController, liteSubscriptionRegistry, liteLifecycleManager);
        MessageArrivingListener listener = new NotifyMessageArrivingListener(
            Mockito.mock(PullRequestHoldService.class), Mockito.mock(PopMessageProcessor.class),
            Mockito.mock(NotificationProcessor.class), liteEventDispatcher);

        messageStore = LiteTestUtil.buildMessageStore(storePathRootDir, BROKER_CONFIG, TOPIC_CONFIG_TABLE, true, listener);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        messageStore.load();
        messageStore.start();

        liteLifecycleManager.init();
    }

    @AfterClass
    public static void reset() {
        messageStore.shutdown();
        messageStore.destroy();
        UtilAll.deleteFile(new File(storePathRootDir));
        mockTopicConfig = new TopicConfig();
    }

    @Ignore
    @Test
    public void testInit_tieredStore() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        LiteSharding liteSharding = Mockito.mock(LiteSharding.class);
        MessageStorePluginContext context = Mockito.mock(MessageStorePluginContext.class);

        TieredMessageStore tieredMessageStore = new TieredMessageStore(context, messageStore);
        when(brokerController.getBrokerConfig()).thenReturn(BROKER_CONFIG);
        when(brokerController.getMessageStore()).thenReturn(tieredMessageStore);

        RocksDBLiteLifecycleManager manager = new RocksDBLiteLifecycleManager(brokerController, liteSharding);
        manager.init();
        Assert.assertEquals(0, manager.getMaxOffsetInQueue(UUID.randomUUID().toString()));
    }

    @Test
    public void testInit_otherStore() {
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        LiteSharding liteSharding = Mockito.mock(LiteSharding.class);
        AbstractPluginMessageStore pluginMessageStore = Mockito.mock(AbstractPluginMessageStore.class);

        when(brokerController.getBrokerConfig()).thenReturn(BROKER_CONFIG);
        when(brokerController.getMessageStore()).thenReturn(pluginMessageStore);
        when(pluginMessageStore.getQueueStore()).thenReturn(Mockito.mock(AbstractConsumeQueueStore.class));

        RocksDBLiteLifecycleManager manager = new RocksDBLiteLifecycleManager(brokerController, liteSharding);

        Assert.assertFalse(manager.init());
        Assert.assertThrows(NullPointerException.class, () -> manager.getMaxOffsetInQueue("HW"));
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
    public void testGetLiteTopicCount() {
        int num = 3;
        String parentTopic = UUID.randomUUID().toString();
        mockTopicConfig.getAttributes().put(
            TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.LITE.getValue());
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(parentTopic, UUID.randomUUID().toString()));
            messageStore.putMessage(LiteTestUtil.buildMessage(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);

        Assert.assertEquals(num, liteLifecycleManager.getLiteTopicCount(parentTopic));
        Assert.assertEquals(0, liteLifecycleManager.getLiteTopicCount(UUID.randomUUID().toString()));
    }

    @Test
    public void testCleanByParentTopic() throws Exception {
        int num = 3;
        String parentTopic = UUID.randomUUID().toString();
        mockTopicConfig.getAttributes().put(
            TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), TopicMessageType.LITE.getValue());
        List<String> liteTopics =
            IntStream.range(0, num).mapToObj(i -> UUID.randomUUID().toString()).collect(Collectors.toList());
        for (int i = 0; i < num; i++) {
            messageStore.putMessage(LiteTestUtil.buildMessage(parentTopic, liteTopics.get(i)));
        }
        await().atMost(5, SECONDS).pollInterval(200, MILLISECONDS).until(() -> messageStore.dispatchBehindBytes() <= 0);

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertEquals(1, (long) messageStore.getQueueStore().getMaxOffset(lmqName, 0));
            Assert.assertEquals(1, liteLifecycleManager.getMaxOffsetInQueue(lmqName));
        }

        liteLifecycleManager.cleanByParentTopic(parentTopic);

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertEquals(0, (long) messageStore.getQueueStore().getMaxOffset(lmqName, 0));
            Assert.assertEquals(0, liteLifecycleManager.getMaxOffsetInQueue(lmqName));
        }
    }

    @Test
    public void testCleanExpiredLiteTopic() throws Exception {
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
            Assert.assertEquals(1, (long) messageStore.getQueueStore().getMaxOffset(lmqName, 0));
            Assert.assertEquals(1, liteLifecycleManager.getMaxOffsetInQueue(lmqName));
        }

        when(liteLifecycleManager.isLiteTopicExpired(eq(parentTopic), anyString(), anyLong())).thenReturn(true);
        liteLifecycleManager.cleanExpiredLiteTopic();

        for (int i = 0; i < num; i++) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopics.get(i));
            Assert.assertEquals(0, (long) messageStore.getQueueStore().getMaxOffset(lmqName, 0));
            Assert.assertEquals(0, liteLifecycleManager.getMaxOffsetInQueue(lmqName));
        }
    }

    @Test
    public void testInit_combineConsumeQueueStore() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(
            System.getProperty("java.io.tmpdir") + File.separator + "store-rocksDBLifecycleTest-" + UUID.randomUUID());
        storeConfig.setRocksdbCQDoubleWriteEnable(true);
        MessageStore messageStore = LiteTestUtil.buildMessageStore(BROKER_CONFIG, storeConfig, TOPIC_CONFIG_TABLE, false);
        BrokerController brokerController = Mockito.mock(BrokerController.class);
        LiteSharding liteSharding = Mockito.mock(LiteSharding.class);
        when(brokerController.getBrokerConfig()).thenReturn(BROKER_CONFIG);
        when(brokerController.getMessageStore()).thenReturn(messageStore);

        // enable
        storeConfig.setCombineCQUseRocksdbForLmq(true);
        RocksDBLiteLifecycleManager manager = new RocksDBLiteLifecycleManager(brokerController, liteSharding);
        Assert.assertTrue(manager.init());
        Assert.assertEquals(0, manager.getMaxOffsetInQueue(UUID.randomUUID().toString()));

        // disable
        storeConfig.setCombineCQUseRocksdbForLmq(false);
        RocksDBLiteLifecycleManager manager2 = new RocksDBLiteLifecycleManager(brokerController, liteSharding);
        Assert.assertFalse(manager2.init());
        Assert.assertThrows(NullPointerException.class, () -> manager2.getMaxOffsetInQueue("HW"));

        messageStore.shutdown();
        messageStore.destroy();
        UtilAll.deleteFile(new File(storeConfig.getStorePathRootDir()));
    }
}
