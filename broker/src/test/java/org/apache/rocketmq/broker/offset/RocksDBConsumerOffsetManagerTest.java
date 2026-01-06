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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBConsumerOffsetManager;
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

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class RocksDBConsumerOffsetManagerTest {

    private static final String SKIP_MAC_KEY = "skipMac";

    private static final String KEY = "FooBar@FooBarGroup";

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

    private static void skipMacIfNecessary() {
        boolean skipMac = Boolean.parseBoolean(System.getProperty(SKIP_MAC_KEY, "true"));
        Assume.assumeFalse(MixAll.isMac() && skipMac);
    }
}