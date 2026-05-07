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
package org.apache.rocketmq.store.queue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.LmqDispatch;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

public class RocksDBConsumeQueueStoreTest extends QueueTestBase {

    private MessageStore messageStore;
    private ConcurrentMap<String, TopicConfig> topicConfigTableMap;

    @Before
    public void init() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
        storeConfig.setEnableCompaction(false);
        storeConfig.setEnableLmq(true);
        storeConfig.setEnableMultiDispatch(true);
        this.topicConfigTableMap = new ConcurrentHashMap<>();
        messageStore = createMessageStore(null, false, topicConfigTableMap, storeConfig);
        messageStore.load();
        messageStore.start();
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();
    }

    @Test
    public void testStorePath_correctConfig() {
        String root = messageStore.getMessageStoreConfig().getStorePathRootDir();
        String originalPath = StorePathConfigHelper.getStorePathConsumeQueue(root);
        File dir = new File(originalPath);
        File checkFile = new File(StorePathConfigHelper.getStorePathRocksDBConsumeQueue(root) + File.separator + "CURRENT");
        assertTrue(dir.exists() || !checkFile.isFile());
    }

    @Test
    public void testStorePath_incompatibleConfig() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
        storeConfig.setUseSeparateStorePathForRocksdbCQ(true);
        storeConfig.setEnableCompaction(false);
        this.topicConfigTableMap = new ConcurrentHashMap<>();

        String root = createBaseDir();
        makeSureFileExists(StorePathConfigHelper.getStorePathConsumeQueue(root) + File.separator + "CURRENT");
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            createMessageStore(root, false, topicConfigTableMap, storeConfig)
        );
        assertTrue(exception.getMessage().contains("incompatible config"));

        storeConfig.setUseSeparateStorePathForRocksdbCQ(false);
        String root2 = createBaseDir();
        makeSureFileExists(StorePathConfigHelper.getStorePathRocksDBConsumeQueue(root2) + File.separator + "CURRENT");
        exception = assertThrows(IllegalStateException.class, () ->
            createMessageStore(root2, false, topicConfigTableMap, storeConfig)
        );
        assertTrue(exception.getMessage().contains("incompatible config"));
    }

    @Test
    public void testFindOrCreateConsumeQueue() {
        String topic = "test-topic-" + UUID.randomUUID();
        ConsumeQueueInterface cq = messageStore.getQueueStore().findOrCreateConsumeQueue(topic, 0);
        assertNotNull(cq);
        assertEquals(CQType.RocksDBCQ, cq.getCQType());
    }

    @Test
    public void testPutMessagePositionInfoWrapper_basic() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID();
        int msgNum = 10;
        int msgSize = 100;
        int queueId = 0;

        for (int i = 0; i < msgNum; i++) {
            DispatchRequest request = new DispatchRequest(topic, queueId, (long) i * msgSize, msgSize, i,
                System.currentTimeMillis(), i, "key", "uk", 0, 0, null);
            messageStore.getQueueStore().putMessagePositionInfoWrapper(request);
        }

        RocksDBConsumeQueueStore store = (RocksDBConsumeQueueStore) messageStore.getQueueStore();
        await().atMost(5, SECONDS).untilAsserted(() ->
            assertEquals(msgNum, store.getMaxOffsetInQueue(topic, queueId))
        );
    }

    @Test
    public void testPutMessagePositionInfoWrapper_lmq() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID();
        int msgNum = 10;
        int msgSize = 100;
        int queueId = 0;

        String lmqName = MixAll.LMQ_PREFIX + UUID.randomUUID();
        for (int i = 0; i < msgNum; i++) {
            Map<String, String> propertyMap = new HashMap<>();
            propertyMap.put(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
            propertyMap.put(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, String.valueOf(i));
            DispatchRequest request = new DispatchRequest(topic, queueId, (long) i * msgSize, msgSize, i,
                System.currentTimeMillis(), i, "key", "uk", 0, 0, propertyMap);
            messageStore.getQueueStore().putMessagePositionInfoWrapper(request);
        }

        RocksDBConsumeQueueStore store = (RocksDBConsumeQueueStore) messageStore.getQueueStore();
        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertEquals(msgNum, store.getMaxOffsetInQueue(topic, queueId));
            assertTrue(store.isLmqExist(lmqName));
            assertEquals(msgNum, store.getMaxOffsetInQueue(lmqName, MixAll.LMQ_QUEUE_ID));
        });
    }

    @Test
    public void testGetMaxOffset_emptyQueue() throws ConsumeQueueException {
        String topic = "test-topic-" + UUID.randomUUID();
        long maxOffset = messageStore.getQueueStore().getMaxOffset(topic, 0);
        assertEquals(0L, maxOffset);
    }

    @Test
    public void testGetMinOffsetInQueue_emptyQueue() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID();
        long minOffset = messageStore.getQueueStore().getMinOffsetInQueue(topic, 0);
        assertEquals(0L, minOffset);
    }

    @Test
    public void testDeleteTopic() throws Exception {
        RocksDBConsumeQueueStore store = (RocksDBConsumeQueueStore) messageStore.getQueueStore();
        String topic = "test-topic-" + UUID.randomUUID();
        String lmqName = MixAll.LMQ_PREFIX + UUID.randomUUID();

        MessageExtBrokerInner msg = buildMessage(topic, -1);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
        LmqDispatch.wrapLmqDispatch(messageStore, msg);
        messageStore.putMessage(msg);

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));
        assertEquals(1, store.getLmqNum());
        assertEquals(1, store.getMaxOffsetInQueue(topic, 0));
        assertEquals(1, store.getMaxOffsetInQueue(lmqName, 0));
        assertTrue(messageStore.getQueueStore().isLmqExist(lmqName));

        messageStore.deleteTopics(java.util.Collections.singleton(topic));
        messageStore.deleteTopics(java.util.Collections.singleton(lmqName));
        assertEquals(0, messageStore.getQueueStore().getLmqNum());
        assertFalse(messageStore.getQueueStore().isLmqExist(lmqName));
    }

    @Test
    public void testGetLmqNum_reload() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID();
        String lmqName = MixAll.LMQ_PREFIX + UUID.randomUUID();

        MessageExtBrokerInner msg = buildMessage(topic, -1);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
        LmqDispatch.wrapLmqDispatch(messageStore, msg);
        messageStore.putMessage(msg);

        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));
        assertEquals(1, messageStore.getQueueStore().getLmqNum());

        String root = messageStore.getMessageStoreConfig().getStorePathRootDir();
        MessageStoreConfig config = messageStore.getMessageStoreConfig();
        messageStore.shutdown();

        MessageStore reloadStore = createMessageStore(root, false, topicConfigTableMap, config);
        reloadStore.load();
        reloadStore.start();

        assertEquals(1, reloadStore.getQueueStore().getLmqNum());
        assertTrue(messageStore.getQueueStore().isLmqExist(lmqName));
        assertNull(reloadStore.getQueueStore().getConsumeQueueTable().get(lmqName));
        messageStore = reloadStore;
    }
}