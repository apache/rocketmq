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

package org.apache.rocketmq.broker.offset;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.config.v1.RocksDBConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.WriteBatch;

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class RocksDBConsumerOffsetManagerTest {

    private static final String SKIP_MAC_KEY = "skipMac";

    private static final String KEY = "FooBar@FooBarGroup";
    private static final long TIMEOUT_SECONDS = 10;

    private BrokerController brokerController;

    private ConsumerOffsetManager consumerOffsetManager;

    private BrokerConfig brokerConfig;

    @Before
    public void init() {
//        System.setProperty(SKIP_MAC_KEY, "false");
        skipMacIfNecessary();
        brokerController = Mockito.mock(BrokerController.class);
        brokerConfig = new BrokerConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        consumerOffsetManager = new RocksDBConsumerOffsetManager(brokerController);
        consumerOffsetManager.load();

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);
        ConcurrentHashMap<Integer, Long> innerMap = new ConcurrentHashMap<>();
        innerMap.put(1, 2L);
        innerMap.put(2, 3L);
        offsetTable.put(KEY, innerMap);
        consumerOffsetManager.setOffsetTable(offsetTable);
    }

    @After
    public void destroy() {
        if (consumerOffsetManager != null) {
            consumerOffsetManager.stop();
            File file = new File(((RocksDBConsumerOffsetManager) consumerOffsetManager).rocksdbConfigFilePath(null, false));
            UtilAll.deleteFile(file);
        }
    }

    @Test
    public void cleanOffsetByTopic_NotExist() {
        consumerOffsetManager.cleanOffsetByTopic("InvalidTopic");
        assertThat(consumerOffsetManager.getOffsetTable().containsKey(KEY)).isTrue();
    }

    @Test
    public void cleanOffsetByTopic_Exist() {
        consumerOffsetManager.cleanOffsetByTopic("FooBar");
        assertThat(!consumerOffsetManager.getOffsetTable().containsKey(KEY)).isTrue();
    }

    @Test
    public void testOffsetPersistInMemory() {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = consumerOffsetManager.getOffsetTable();
        ConcurrentMap<Integer, Long> table = new ConcurrentHashMap<>();
        table.put(0, 1L);
        table.put(1, 3L);
        String group = "G1";
        offsetTable.put(group, table);

        consumerOffsetManager.persist();
        consumerOffsetManager.stop();
        consumerOffsetManager.load();

        ConcurrentMap<Integer, Long> offsetTableLoaded = consumerOffsetManager.getOffsetTable().get(group);
        Assert.assertEquals(table, offsetTableLoaded);
    }

    @Test
    public void testCommitOffset_persist_periodically() {
        brokerConfig.setPersistConsumerOffsetIncrementally(false);
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        // 1. commit but not persist
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key));
        consumerOffsetManager.commitOffset("ClientID", group, topic, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key));

        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key)); // not in kv

        // 2. commit and persist
        consumerOffsetManager.commitOffset("ClientID", group, topic, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key));
        consumerOffsetManager.persist();
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key)); // load from kv
    }

    @Test
    public void testCommitOffset_persist_incrementally() {
        brokerConfig.setPersistConsumerOffsetIncrementally(true);
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        // commit but not persist
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key));
        consumerOffsetManager.commitOffset("ClientID", group, topic, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key));

        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key)); // reload from kv
    }

    @Test
    public void testConcurrentFirstCommitsPersistAllLmqQueuesPeriodically() throws Exception {
        brokerConfig.setPersistConsumerOffsetIncrementally(false);
        String group = UUID.randomUUID().toString();
        String topic = MixAll.LMQ_PREFIX + UUID.randomUUID();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        FirstReadBarrierMap offsetTable = new FirstReadBarrierMap(key);
        consumerOffsetManager.setOffsetTable(offsetTable);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstCommit = executor.submit(
                () -> consumerOffsetManager.commitOffset("ClientA", group, topic, 0, 100L));
            Future<?> secondCommit = executor.submit(
                () -> consumerOffsetManager.commitOffset("ClientB", group, topic, 1, 200L));

            firstCommit.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            secondCommit.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            offsetTable.releaseReaders();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        assertOffsets(offsetTable.get(key));
        consumerOffsetManager.persist();
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        Assert.assertTrue(consumerOffsetManager.load());
        assertOffsets(consumerOffsetManager.getOffsetTable().get(key));
    }

    @Test
    public void testIncrementalConcurrentCommitsPersistLatestSnapshot() throws Exception {
        brokerConfig.setPersistConsumerOffsetIncrementally(true);
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        offsetTable.put(key, offsets);
        consumerOffsetManager.setOffsetTable(offsetTable);

        RocksDBConsumerOffsetManager rocksDBConsumerOffsetManager =
            (RocksDBConsumerOffsetManager) consumerOffsetManager;
        RocksDBConfigManager realConfigManager = (RocksDBConfigManager) FieldUtils.readDeclaredField(
            rocksDBConsumerOffsetManager, "rocksDBConfigManager", true);
        RocksDBConfigManager configManagerSpy = Mockito.spy(realConfigManager);
        FieldUtils.writeDeclaredField(
            rocksDBConsumerOffsetManager, "rocksDBConfigManager", configManagerSpy, true);

        CountDownLatch firstBatchReady = new CountDownLatch(1);
        CountDownLatch releaseFirstBatch = new CountDownLatch(1);
        CountDownLatch secondBatchWritten = new CountDownLatch(1);
        AtomicInteger batchWriteCount = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            int batchNumber = batchWriteCount.incrementAndGet();
            if (batchNumber == 1) {
                firstBatchReady.countDown();
                try {
                    Assert.assertTrue("Timed out waiting to release the first batch",
                        releaseFirstBatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            Object result = invocation.callRealMethod();
            if (batchNumber == 2) {
                secondBatchWritten.countDown();
            }
            return result;
        }).when(configManagerSpy).batchPutWithWal(Mockito.any(WriteBatch.class));

        FutureTask<Void> firstCommit = new FutureTask<>(() -> {
            consumerOffsetManager.commitOffset("ClientA", group, topic, 0, 100L);
            return null;
        });
        FutureTask<Void> secondCommit = new FutureTask<>(() -> {
            consumerOffsetManager.commitOffset("ClientB", group, topic, 1, 200L);
            return null;
        });
        Thread firstThread = new Thread(firstCommit, "first-incremental-offset-commit");
        Thread secondThread = new Thread(secondCommit, "second-incremental-offset-commit");
        boolean secondWriteCompletedBeforeRelease;
        try {
            firstThread.start();
            Assert.assertTrue("First batch was not ready",
                firstBatchReady.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            secondThread.start();
            awaitSecondWriteOrBlocked(secondThread, secondBatchWritten);
            secondWriteCompletedBeforeRelease = secondBatchWritten.getCount() == 0;
            if (!secondWriteCompletedBeforeRelease) {
                Assert.assertEquals(Thread.State.BLOCKED, secondThread.getState());
            }

            releaseFirstBatch.countDown();
            firstCommit.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            secondCommit.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            releaseFirstBatch.countDown();
            firstThread.interrupt();
            secondThread.interrupt();
            firstThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            secondThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        }

        Assert.assertEquals(2, batchWriteCount.get());
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        Assert.assertTrue(consumerOffsetManager.load());
        assertOffsets(consumerOffsetManager.getOffsetTable().get(key));
        Assert.assertFalse("Commits for one topic and group must be serialized",
            secondWriteCompletedBeforeRelease);
    }

    @Test
    public void testLoadAndMerge_persist_periodically() {
        brokerConfig.setPersistConsumerOffsetIncrementally(false);
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        ConsumerOffsetManager jsonConsumerOffsetManager = new ConsumerOffsetManager(brokerController);
        jsonConsumerOffsetManager.commitOffset("ClientID", group, topic, 0, 1);
        jsonConsumerOffsetManager.updateDataVersion();
        jsonConsumerOffsetManager.persist();

        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key));

        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load(); // merge from json file
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key));

        UtilAll.deleteFile(new File(jsonConsumerOffsetManager.configFilePath()));
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key)); // already persisted in kv
    }

    @Test
    public void testLoadAndMerge_persist_incrementally() {
        brokerConfig.setPersistConsumerOffsetIncrementally(true);
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        ConsumerOffsetManager jsonConsumerOffsetManager = new ConsumerOffsetManager(brokerController);
        jsonConsumerOffsetManager.commitOffset("ClientID", group, topic, 0, 1);
        jsonConsumerOffsetManager.updateDataVersion();
        jsonConsumerOffsetManager.persist();

        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key));

        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load(); // merge from json file
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key));

        UtilAll.deleteFile(new File(jsonConsumerOffsetManager.configFilePath()));
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key)); // already persisted in kv
    }

    @Test
    public void testRemoveConsumerOffset() {
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        // commit and persist
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key));
        consumerOffsetManager.commitOffset("ClientID", group, topic, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key));
        consumerOffsetManager.persist();

        consumerOffsetManager.removeConsumerOffset(key);
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key)); // removed from kv
    }

    @Test
    public void testRemoveOffset() {
        String group = UUID.randomUUID().toString();
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String key1 = topic1 + TOPIC_GROUP_SEPARATOR + group;
        String key2 = topic2 + TOPIC_GROUP_SEPARATOR + group;

        // commit and persist
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.commitOffset("ClientID", group, topic1, 0, 1);
        consumerOffsetManager.commitOffset("ClientID", group, topic2, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.persist();

        // remove all offsets by group
        consumerOffsetManager.removeOffset(group);
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1)); // removed from kv
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2)); // removed from kv
    }

    @Test
    // similar to testRemoveOffset()
    public void testCleanOffset() {
        String group = UUID.randomUUID().toString();
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String key1 = topic1 + TOPIC_GROUP_SEPARATOR + group;
        String key2 = topic2 + TOPIC_GROUP_SEPARATOR + group;

        // commit and persist
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.commitOffset("ClientID", group, topic1, 0, 1);
        consumerOffsetManager.commitOffset("ClientID", group, topic2, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.persist();

        // remove all offsets by group
        consumerOffsetManager.cleanOffset(group);
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1)); // removed from kv
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2)); // removed from kv
    }

    @Test
    public void testCleanOffsetByTopic() {
        String group1 = UUID.randomUUID().toString();
        String group2 = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        String key1 = topic + TOPIC_GROUP_SEPARATOR + group1;
        String key2 = topic + TOPIC_GROUP_SEPARATOR + group2;

        // commit and persist
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.commitOffset("ClientID", group1, topic, 0, 1);
        consumerOffsetManager.commitOffset("ClientID", group2, topic, 0, 1);
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertTrue(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.persist();

        // remove all offsets by group
        consumerOffsetManager.cleanOffsetByTopic(topic);
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1));
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2));
        consumerOffsetManager.stop();
        consumerOffsetManager.getOffsetTable().clear();
        consumerOffsetManager.load();
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key1)); // removed from kv
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(key2)); // removed from kv
    }

    @Test
    public void testUpdateDataVersion() {
        Assert.assertEquals(0, consumerOffsetManager.getDataVersion().getCounter().get());
        for (int i = 0; i < 10; i++) {
            ((RocksDBConsumerOffsetManager) consumerOffsetManager).updateDataVersion();
        }
        Assert.assertEquals(10, consumerOffsetManager.getDataVersion().getCounter().get());
    }

    @Test
    public void testLoadDataVersion() {
        for (int i = 0; i < 10; i++) {
            ((RocksDBConsumerOffsetManager) consumerOffsetManager).updateDataVersion();
        }
        consumerOffsetManager.stop();
        consumerOffsetManager.load();
        Assert.assertEquals(10, consumerOffsetManager.getDataVersion().getCounter().get());
    }

    private static void awaitSecondWriteOrBlocked(Thread secondThread, CountDownLatch secondBatchWritten)
        throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (secondBatchWritten.await(10, TimeUnit.MILLISECONDS)
                || secondThread.getState() == Thread.State.BLOCKED) {
                return;
            }
        }
        Assert.fail("Second commit neither wrote its batch nor blocked on the offset map");
    }

    private static void assertOffsets(ConcurrentMap<Integer, Long> offsets) {
        Assert.assertNotNull(offsets);
        Assert.assertEquals(2, offsets.size());
        Assert.assertEquals(Long.valueOf(100L), offsets.get(0));
        Assert.assertEquals(Long.valueOf(200L), offsets.get(1));
    }

    private static class FirstReadBarrierMap
        extends ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> {
        private final String targetKey;
        private final AtomicInteger targetReads = new AtomicInteger();
        private final CountDownLatch firstReads = new CountDownLatch(2);

        FirstReadBarrierMap(String targetKey) {
            this.targetKey = targetKey;
        }

        @Override
        public ConcurrentMap<Integer, Long> get(Object key) {
            ConcurrentMap<Integer, Long> storedValue = super.get(key);
            if (targetKey.equals(key) && targetReads.getAndIncrement() < 2) {
                firstReads.countDown();
                try {
                    Assert.assertTrue("Timed out waiting for both initial reads",
                        firstReads.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return storedValue;
        }

        void releaseReaders() {
            while (firstReads.getCount() > 0) {
                firstReads.countDown();
            }
        }
    }

    private static void skipMacIfNecessary() {
        boolean skipMac = Boolean.parseBoolean(System.getProperty(SKIP_MAC_KEY, "true"));
        Assume.assumeFalse(MixAll.isMac() && skipMac);
    }
}
