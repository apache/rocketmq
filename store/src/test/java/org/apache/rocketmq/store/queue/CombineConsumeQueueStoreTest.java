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
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.common.TopicFilterType.SINGLE_TAG;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class CombineConsumeQueueStoreTest extends QueueTestBase {
    private DefaultMessageStore messageStore;
    private MessageStoreConfig messageStoreConfig;
    private ConcurrentMap<String, TopicConfig> topicConfigTableMap;

    String topic = UUID.randomUUID().toString();
    final int queueId = 0;
    final int msgNum = 100;
    final int msgSize = 1000;

    @Before
    public void init() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.topicConfigTableMap = new ConcurrentHashMap<>();
        messageStoreConfig = new MessageStoreConfig();
    }

    @After
    public void destroy() {
        if (MixAll.isMac()) {
            return;
        }
        if (!messageStore.isShutdown()) {
            messageStore.shutdown();
        }
        messageStore.destroy();

        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    @Test(expected = IllegalArgumentException.class)
    public void CombineConsumeQueueStore_EmptyLoadingCQTypes_ThrowsException() throws Exception {
        Assume.assumeFalse(MixAll.isMac());
        messageStore = (DefaultMessageStore) createMessageStore(null, false, topicConfigTableMap, messageStoreConfig);

        messageStoreConfig.setCombineCQLoadingCQTypes("");
        new CombineConsumeQueueStore(messageStore);
    }

    @Test
    public void CombineConsumeQueueStore_InitializesConsumeQueueStore() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        messageStore = (DefaultMessageStore) createMessageStore(null, false, topicConfigTableMap, messageStoreConfig);
        {
            messageStoreConfig.setCombineCQLoadingCQTypes("default");
            messageStoreConfig.setCombineCQPreferCQType("default");
            CombineConsumeQueueStore store = new CombineConsumeQueueStore(messageStore);
            assertNotNull(store.getConsumeQueueStore());
            assertNull(store.getRocksDBConsumeQueueStore());
        }

        {
            messageStoreConfig.setCombineCQLoadingCQTypes("defaultRocksDB");
            messageStoreConfig.setCombineCQPreferCQType("defaultRocksDB");
            messageStoreConfig.setCombineAssignOffsetCQType("defaultRocksDB");
            CombineConsumeQueueStore store = new CombineConsumeQueueStore(messageStore);
            assertNull(store.getConsumeQueueStore());
            assertNotNull(store.getRocksDBConsumeQueueStore());
            assertTrue(store.getCurrentReadStore() instanceof RocksDBConsumeQueueStore);
        }

        {
            messageStoreConfig.setCombineCQLoadingCQTypes(";;default;defaultRocksDB;");
            messageStoreConfig.setCombineCQPreferCQType("defaultRocksDB");
            CombineConsumeQueueStore store = new CombineConsumeQueueStore(messageStore);
            assertNotNull(store.getConsumeQueueStore());
            assertNotNull(store.getRocksDBConsumeQueueStore());
            assertTrue(store.getCurrentReadStore() instanceof RocksDBConsumeQueueStore);
        }

        {
            messageStoreConfig.setCombineCQLoadingCQTypes("default;defaultRocksDB");
            messageStoreConfig.setCombineCQPreferCQType("defaultRocksDB");
            CombineConsumeQueueStore store = new CombineConsumeQueueStore(messageStore);
            assertTrue(store.getCurrentReadStore() instanceof RocksDBConsumeQueueStore);
        }

    }

    @Test
    public void testIterator() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        messageStoreConfig.setRocksdbCQDoubleWriteEnable(true);
        messageStore = (DefaultMessageStore) createMessageStore(null, false, topicConfigTableMap, messageStoreConfig);
        messageStore.load();
        messageStore.start();

        //The initial min max offset, before and after the creation of consume queue
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, queueId));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, queueId));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, queueId);
        Assert.assertEquals(CQType.SimpleCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, queueId));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, queueId));

        for (int i = 0; i < msgNum; i++) {
            DispatchRequest request = new DispatchRequest(topic, queueId, i * msgSize, msgSize, i,
                System.currentTimeMillis(), i, null, null, 0, 0, null);
            messageStore.getQueueStore().putMessagePositionInfoWrapper(request);
        }

        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            checkCQ(consumeQueue, msgNum, msgSize);

            CombineConsumeQueueStore combineConsumeQueueStore = (CombineConsumeQueueStore) messageStore.getQueueStore();
            ConsumeQueueInterface rocksDBConsumeQueue = combineConsumeQueueStore.getRocksDBConsumeQueueStore().getConsumeQueue(topic, queueId);
            Assert.assertEquals(CQType.RocksDBCQ, rocksDBConsumeQueue.getCQType());
            Assert.assertEquals(msgNum, rocksDBConsumeQueue.getMaxOffsetInQueue());
            checkCQ(rocksDBConsumeQueue, msgNum, msgSize);
        });
    }

    private void checkCQ(ConsumeQueueInterface consumeQueue, int msgNum,
        int msgSize) {
        Assert.assertEquals(0, consumeQueue.getMinLogicOffset());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(msgNum, consumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(msgNum, consumeQueue.getMessageTotalInQueue());

        assertNull(consumeQueue.iterateFrom(-1));
        assertNull(consumeQueue.iterateFrom(msgNum));

        {
            CqUnit first = consumeQueue.getEarliestUnit();
            assertNotNull(first);
            Assert.assertEquals(0, first.getQueueOffset());
            Assert.assertEquals(msgSize, first.getSize());
            assertTrue(first.isTagsCodeValid());
        }
        {
            CqUnit last = consumeQueue.getLatestUnit();
            assertNotNull(last);
            Assert.assertEquals(msgNum - 1, last.getQueueOffset());
            Assert.assertEquals(msgSize, last.getSize());
            assertTrue(last.isTagsCodeValid());
        }

        for (int i = 0; i < msgNum; i++) {
            ReferredIterator<CqUnit> iterator = consumeQueue.iterateFrom(i);
            assertNotNull(iterator);
            long queueOffset = i;
            while (iterator.hasNext()) {
                CqUnit cqUnit = iterator.next();
                Assert.assertEquals(queueOffset, cqUnit.getQueueOffset());
                Assert.assertEquals(queueOffset * msgSize, cqUnit.getPos());
                Assert.assertEquals(msgSize, cqUnit.getSize());
                assertTrue(cqUnit.isTagsCodeValid());
                Assert.assertEquals(queueOffset, cqUnit.getTagsCode());
                Assert.assertEquals(queueOffset, cqUnit.getValidTagsCodeAsLong().longValue());
                Assert.assertEquals(1, cqUnit.getBatchNum());
                assertNull(cqUnit.getCqExtUnit());
                queueOffset++;
            }
            Assert.assertEquals(msgNum, queueOffset);
        }
    }

    @Test
    public void testInitializeWithOffset() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        final String path = createBaseDir();
        FileUtils.deleteDirectory(new File(path));
        topicConfigTableMap.put(topic, new TopicConfig(topic, 1, 1, PermName.PERM_WRITE | PermName.PERM_READ));

        {
            messageStoreConfig.setRocksdbCQDoubleWriteEnable(true);
            messageStore = (DefaultMessageStore) createMessageStore(path, false, topicConfigTableMap, messageStoreConfig);
            messageStore.load();

            ConsumeQueueStoreInterface consumeQueueStoreInterface = messageStore.getQueueStore();
            assertTrue(consumeQueueStoreInterface instanceof CombineConsumeQueueStore);
            CombineConsumeQueueStore combineConsumeQueueStore = (CombineConsumeQueueStore) consumeQueueStoreInterface;
            ConsumeQueueStore consumeQueueStore = combineConsumeQueueStore.getConsumeQueueStore();
            RocksDBConsumeQueueStore rocksDBConsumeQueueStore = combineConsumeQueueStore.getRocksDBConsumeQueueStore();
            assertNotNull(consumeQueueStore);
            assertNotNull(rocksDBConsumeQueueStore);

            ConsumeQueueInterface rocksDBConsumeQueue = rocksDBConsumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
            consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
            rocksDBConsumeQueue.initializeWithOffset(100, 0);

            Assert.assertEquals(100, rocksDBConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(100, rocksDBConsumeQueue.getMinOffsetInQueue());

            rocksDBConsumeQueue.initializeWithOffset(200, 0);

            Assert.assertEquals(200, rocksDBConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(200, rocksDBConsumeQueue.getMinOffsetInQueue());

            messageStore.start();

            Assert.assertEquals(0, rocksDBConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(0, rocksDBConsumeQueue.getMinOffsetInQueue());

            ConsumeQueue consumeQueue = (ConsumeQueue) consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);

            for (int i = 0; i < msgNum; i++) {
                MessageExtBrokerInner msg = new MessageExtBrokerInner();

                msg.setQueueId(queueId);
                msg.setBody(new byte[msgSize]);
                msg.setTopic(topic);
                msg.setTags("TAG1");

                msg.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(SINGLE_TAG, msg.getTags()));
                msg.setBornTimestamp(System.currentTimeMillis());
                msg.setReconsumeTimes(0);

                msg.setBornHost(new InetSocketAddress(9999));
                msg.setStoreHost(new InetSocketAddress(8888));

                messageStore.putMessage(msg);
            }

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                Assert.assertEquals(msgNum, consumeQueue.getMaxOffsetInQueue());
                Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
            });

            messageStore.shutdown();
        }

        {
            messageStoreConfig.setRocksdbCQDoubleWriteEnable(true);
            messageStore = (DefaultMessageStore) createMessageStore(path, false, topicConfigTableMap, messageStoreConfig);
            messageStore.load();

            ConsumeQueueStoreInterface consumeQueueStoreInterface = messageStore.getQueueStore();
            assertTrue(consumeQueueStoreInterface instanceof CombineConsumeQueueStore);
            CombineConsumeQueueStore combineConsumeQueueStore = (CombineConsumeQueueStore) consumeQueueStoreInterface;
            ConsumeQueueStore consumeQueueStore = combineConsumeQueueStore.getConsumeQueueStore();
            RocksDBConsumeQueueStore rocksDBConsumeQueueStore = combineConsumeQueueStore.getRocksDBConsumeQueueStore();
            assertNotNull(consumeQueueStore);
            assertNotNull(rocksDBConsumeQueueStore);

            consumeQueueStore.findOrCreateConsumeQueue(topic, queueId).initializeWithOffset(200, 0);

            ConsumeQueueInterface cq = rocksDBConsumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
            Assert.assertEquals(msgNum, cq.getMaxOffsetInQueue());
            Assert.assertEquals(0, cq.getMinOffsetInQueue());

            combineConsumeQueueStore.verifyAndInitOffsetForAllStore(true);

            Assert.assertEquals(200, cq.getMaxOffsetInQueue());
            Assert.assertEquals(200, cq.getMinOffsetInQueue());

            messageStore.shutdown();
        }
    }

    @Test
    public void testVerifyAndInitOffsetForAllStore() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        final String path = createBaseDir();
        topicConfigTableMap.put(topic, new TopicConfig(topic, 1, 1, PermName.PERM_WRITE | PermName.PERM_READ));

        {
            messageStoreConfig.setRocksdbCQDoubleWriteEnable(false);
            messageStore = (DefaultMessageStore) createMessageStore(path, false, topicConfigTableMap, messageStoreConfig);
            messageStore.load();
            messageStore.start();

            assertTrue(messageStore.getQueueStore() instanceof ConsumeQueueStore);

            for (int i = 0; i < msgNum; i++) {
                MessageExtBrokerInner msg = new MessageExtBrokerInner();

                msg.setQueueId(queueId);
                msg.setBody(new byte[msgSize]);
                msg.setTopic(topic);
                msg.setTags("TAG1");

                msg.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(SINGLE_TAG, msg.getTags()));
                msg.setBornTimestamp(System.currentTimeMillis());
                msg.setReconsumeTimes(0);

                msg.setBornHost(new InetSocketAddress(9999));
                msg.setStoreHost(new InetSocketAddress(8888));

                messageStore.putMessage(msg);
            }

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                File cq = new File(path + File.separator + "consumequeue" + File.separator + topic + File.separator + queueId + File.separator + "00000000000000000000");
                assertTrue(cq.exists());
                Assert.assertEquals(msgNum, (long) messageStore.getQueueStore().getMaxOffset(topic, queueId));
                Assert.assertEquals(0, messageStore.getQueueStore().getMinOffsetInQueue(topic, queueId));
            });

            messageStore.shutdown();
        }

        {
            messageStoreConfig.setRocksdbCQDoubleWriteEnable(true);
            messageStore = (DefaultMessageStore) createMessageStore(path, false, topicConfigTableMap, messageStoreConfig);
            messageStore.load();
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                assertTrue(((CombineConsumeQueueStore) messageStore.getQueueStore()).verifyAndInitOffsetForAllStore(false));
            });
            messageStore.start();
            messageStore.shutdown();
        }

        {
            messageStoreConfig.setRocksdbCQDoubleWriteEnable(true);
            messageStore = (DefaultMessageStore) createMessageStore(path, false, topicConfigTableMap, messageStoreConfig);
            messageStore.load();
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                assertTrue(((CombineConsumeQueueStore) messageStore.getQueueStore()).verifyAndInitOffsetForAllStore(false));
            });
            messageStore.start();
            messageStore.shutdown();
        }
    }
}
