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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

public class ConsumeQueueTest extends QueueTestBase {

    private static final String TOPIC = "StoreTest";
    private static final int QUEUE_ID = 0;
    private static final String STORE_PATH = "." + File.separator + "unit_test_store";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 8;
    private static final int CQ_FILE_SIZE = 10 * 20;
    private static final int CQ_EXT_FILE_SIZE = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    public MessageStoreConfig buildStoreConfig(int commitLogFileSize, int cqFileSize,
        boolean enableCqExt, int cqExtFileSize) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(enableCqExt);

        messageStoreConfig.setStorePathRootDir(STORE_PATH);
        messageStoreConfig.setStorePathCommitLog(STORE_PATH + File.separator + "commitlog");

        return messageStoreConfig;
    }

    protected DefaultMessageStore gen() throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
            COMMIT_LOG_FILE_SIZE, CQ_FILE_SIZE, true, CQ_EXT_FILE_SIZE
        );

        BrokerConfig brokerConfig = new BrokerConfig();

        DefaultMessageStore master = new DefaultMessageStore(
            messageStoreConfig, new BrokerStatsManager(brokerConfig),
            (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {
            }, brokerConfig, new ConcurrentHashMap<>());

        assertThat(master.load()).isTrue();

        master.start();

        return master;
    }

    protected RocksDBMessageStore genRocksdbMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
            COMMIT_LOG_FILE_SIZE, CQ_FILE_SIZE, true, CQ_EXT_FILE_SIZE
        );

        BrokerConfig brokerConfig = new BrokerConfig();

        RocksDBMessageStore master = new RocksDBMessageStore(
            messageStoreConfig, new BrokerStatsManager(brokerConfig),
            (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {
            }, brokerConfig, new ConcurrentHashMap<>());

        assertThat(master.load()).isTrue();

        master.start();

        return master;
    }

    protected void putMsg(MessageStore messageStore) {
        int totalMsgs = 200;
        for (int i = 0; i < totalMsgs; i++) {
            MessageExtBrokerInner message = buildMessage();
            message.setQueueId(0);
            switch (i % 3) {
                case 0:
                    message.setTags("TagA");
                    break;

                case 1:
                    message.setTags("TagB");
                    break;

                case 2:
                    message.setTags("TagC");
                    break;
            }
            message.setTagsCode(message.getTags().hashCode());
            message.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
            messageStore.putMessage(message);
        }
        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));
    }

    @Test
    public void testIterator() throws Exception {
        final int msgNum = 100;
        final int msgSize = 1000;
        MessageStore messageStore =  createMessageStore(null, true, null);
        messageStore.load();
        String topic = UUID.randomUUID().toString();
        //The initial min max offset, before and after the creation of consume queue
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));

        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, 0);
        Assert.assertEquals(CQType.SimpleCQ, consumeQueue.getCQType());
        Assert.assertEquals(0, consumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        for (int i = 0; i < msgNum; i++) {
            DispatchRequest request = new DispatchRequest(consumeQueue.getTopic(), consumeQueue.getQueueId(), i * msgSize, msgSize, i,
                System.currentTimeMillis(), i, null, null, 0, 0, null);
            request.setBitMap(new byte[10]);
            messageStore.getQueueStore().putMessagePositionInfoWrapper(consumeQueue, request);
        }
        Assert.assertEquals(0, consumeQueue.getMinLogicOffset());
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());
        Assert.assertEquals(msgNum, consumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(msgNum, consumeQueue.getMessageTotalInQueue());
        //TO DO Should test it
        //Assert.assertEquals(100 * 100, consumeQueue.getMaxPhysicOffset());


        Assert.assertNull(consumeQueue.iterateFrom(-1));
        Assert.assertNull(consumeQueue.iterateFrom(msgNum));

        {
            CqUnit first = consumeQueue.getEarliestUnit();
            Assert.assertNotNull(first);
            Assert.assertEquals(0, first.getQueueOffset());
            Assert.assertEquals(msgSize, first.getSize());
            Assert.assertTrue(first.isTagsCodeValid());
        }
        {
            CqUnit last = consumeQueue.getLatestUnit();
            Assert.assertNotNull(last);
            Assert.assertEquals(msgNum - 1, last.getQueueOffset());
            Assert.assertEquals(msgSize, last.getSize());
            Assert.assertTrue(last.isTagsCodeValid());
        }

        for (int i = 0; i < msgNum; i++) {
            ReferredIterator<CqUnit> iterator = consumeQueue.iterateFrom(i);
            Assert.assertNotNull(iterator);
            long queueOffset = i;
            while (iterator.hasNext()) {
                CqUnit cqUnit =  iterator.next();
                Assert.assertEquals(queueOffset, cqUnit.getQueueOffset());
                Assert.assertEquals(queueOffset * msgSize, cqUnit.getPos());
                Assert.assertEquals(msgSize, cqUnit.getSize());
                Assert.assertTrue(cqUnit.isTagsCodeValid());
                Assert.assertEquals(queueOffset, cqUnit.getTagsCode());
                Assert.assertEquals(queueOffset, cqUnit.getValidTagsCodeAsLong().longValue());
                Assert.assertEquals(1, cqUnit.getBatchNum());
                Assert.assertNotNull(cqUnit.getCqExtUnit());
                ConsumeQueueExt.CqExtUnit cqExtUnit =  cqUnit.getCqExtUnit();
                Assert.assertEquals(queueOffset, cqExtUnit.getTagsCode());
                Assert.assertArrayEquals(new byte[10], cqExtUnit.getFilterBitMap());
                queueOffset++;
            }
            Assert.assertEquals(msgNum, queueOffset);
        }
        messageStore.getQueueStore().destroy(consumeQueue);
    }

    @Test
    public void testEstimateMessageCountInEmptyConsumeQueue() {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = gen();
            doTestEstimateMessageCountInEmptyConsumeQueue(messageStore);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test
    public void testEstimateRocksdbMessageCountInEmptyConsumeQueue() {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = genRocksdbMessageStore();
            doTestEstimateMessageCountInEmptyConsumeQueue(messageStore);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    public void doTestEstimateMessageCountInEmptyConsumeQueue(MessageStore master) {
        try {
            ConsumeQueueInterface consumeQueue = master.findConsumeQueue(TOPIC, QUEUE_ID);
            MessageFilter filter = new MessageFilter() {
                @Override
                public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                    return tagsCode == "TagA".hashCode();
                }

                @Override
                public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                    return false;
                }
            };
            long estimation = consumeQueue.estimateMessageCount(0, 0, filter);
            Assert.assertEquals(0, estimation);

            // test for illegal offset
            estimation = consumeQueue.estimateMessageCount(0, 100, filter);
            Assert.assertEquals(0, estimation);
            estimation = consumeQueue.estimateMessageCount(100, 1000, filter);
            Assert.assertEquals(0, estimation);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        } finally {
            if (master != null) {
                master.shutdown();
                master.destroy();
            }
            UtilAll.deleteFile(new File(STORE_PATH));
        }
    }

    @Test
    public void testEstimateRocksdbMessageCount() {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = genRocksdbMessageStore();
            doTestEstimateMessageCount(messageStore);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test
    public void testEstimateMessageCount() {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = gen();
            doTestEstimateMessageCount(messageStore);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    public void doTestEstimateMessageCount(MessageStore messageStore) {
        try {
            try {
                putMsg(messageStore);
            } catch (Exception e) {
                fail("Failed to put message", e);
            }

            ConsumeQueueInterface cq = messageStore.findConsumeQueue(TOPIC, QUEUE_ID);
            MessageFilter filter = new MessageFilter() {
                @Override
                public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                    return tagsCode == "TagA".hashCode();
                }

                @Override
                public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                    return false;
                }
            };
            long estimation = cq.estimateMessageCount(0, 199, filter);
            Assert.assertEquals(67, estimation);

            // test for illegal offset
            estimation = cq.estimateMessageCount(0, 1000, filter);
            Assert.assertEquals(67, estimation);
            estimation = cq.estimateMessageCount(1000, 10000, filter);
            Assert.assertEquals(0, estimation);
            estimation = cq.estimateMessageCount(100, 0, filter);
            Assert.assertEquals(0, estimation);
        } finally {
            messageStore.shutdown();
            messageStore.destroy();
            UtilAll.deleteFile(new File(STORE_PATH));
        }
    }

    @Test
    public void testEstimateRocksdbMessageCountSample() {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = genRocksdbMessageStore();
            doTestEstimateMessageCountSample(messageStore);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test
    public void testEstimateMessageCountSample() {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = gen();
            doTestEstimateMessageCountSample(messageStore);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    public void doTestEstimateMessageCountSample(MessageStore messageStore) {

        try {
            try {
                putMsg(messageStore);
            } catch (Exception e) {
                fail("Failed to put message", e);
            }
            messageStore.getMessageStoreConfig().setSampleCountThreshold(10);
            messageStore.getMessageStoreConfig().setMaxConsumeQueueScan(20);
            ConsumeQueueInterface cq = messageStore.findConsumeQueue(TOPIC, QUEUE_ID);
            MessageFilter filter = new MessageFilter() {
                @Override
                public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                    return tagsCode == "TagA".hashCode();
                }

                @Override
                public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                    return false;
                }
            };
            long estimation = cq.estimateMessageCount(100, 150, filter);
            Assert.assertEquals(15, estimation);
        } finally {
            messageStore.shutdown();
            messageStore.destroy();
            UtilAll.deleteFile(new File(STORE_PATH));
        }
    }
}
