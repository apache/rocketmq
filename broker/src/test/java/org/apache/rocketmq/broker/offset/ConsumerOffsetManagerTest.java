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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {

    private static final String KEY = "FooBar@FooBarGroup";
    private static final long TIMEOUT_SECONDS = 10;

    private BrokerController brokerController;

    private ConsumerOffsetManager consumerOffsetManager;

    @Before
    @SuppressWarnings("DoubleBraceInitialization")
    public void init() {
        brokerController = Mockito.mock(BrokerController.class);
        consumerOffsetManager = new ConsumerOffsetManager(brokerController);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);
        offsetTable.put(KEY,new ConcurrentHashMap<Integer, Long>() {{
                put(1,2L);
                put(2,3L);
            }});
        consumerOffsetManager.setOffsetTable(offsetTable);
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
    public void removeOffsetByGroupTest() {
        String topic = "TopicName";
        String group = "GroupName";
        consumerOffsetManager.commitOffset("Commit", group, topic, 0, 100);
        consumerOffsetManager.assignResetOffset(topic, group, 0, 100);
        consumerOffsetManager.commitPullOffset("Pull", group, topic, 0, 100);
        consumerOffsetManager.removeOffset(group);
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(topic + TOPIC_GROUP_SEPARATOR + group));

        consumerOffsetManager.commitPullOffset("Pull", group, topic, 0, 100);
        consumerOffsetManager.clearPullOffset(group, topic);
        Assert.assertEquals(-1L, consumerOffsetManager.queryPullOffset(group, topic, 0));
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
        ConsumerOffsetManager manager = new ConsumerOffsetManager(brokerController);
        manager.load();

        ConcurrentMap<Integer, Long> offsetTableLoaded = manager.getOffsetTable().get(group);
        Assert.assertEquals(table, offsetTableLoaded);
    }

    @Test
    public void testEraseResetOffset() {
        String topic = "Topic";
        String group = "Group";
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        consumerOffsetManager.assignResetOffset(topic, group, 0, 100L);
        consumerOffsetManager.assignResetOffset(topic, group, 1, 200L);

        Assert.assertTrue(consumerOffsetManager.hasOffsetReset(topic, group, 0));
        Assert.assertTrue(consumerOffsetManager.hasOffsetReset(topic, group, 1));

        consumerOffsetManager.eraseResetOffset(topic, group, 0);
        Assert.assertFalse(consumerOffsetManager.hasOffsetReset(topic, group, 0));
        Assert.assertTrue(consumerOffsetManager.hasOffsetReset(topic, group, 1));
        Assert.assertTrue(consumerOffsetManager.resetOffsetTable.containsKey(key));

        consumerOffsetManager.eraseResetOffset(topic, group, 1);
        Assert.assertFalse(consumerOffsetManager.hasOffsetReset(topic, group, 1));
        Assert.assertFalse(consumerOffsetManager.resetOffsetTable.containsKey(key));
    }

    @Test
    public void testConcurrentFirstCommitsPreserveAllQueuesDuringJsonRoundTrip() throws Exception {
        String topic = "ConcurrentTopic";
        String group = "ConcurrentGroup";
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

        ConsumerOffsetManager decodedManager = new ConsumerOffsetManager(brokerController);
        decodedManager.decode(consumerOffsetManager.encode());
        assertOffsets(decodedManager.getOffsetTable().get(key));
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
}
