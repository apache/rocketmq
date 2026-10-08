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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.mockito.Mockito;

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {

    private static final String KEY = "FooBar@FooBarGroup";

    private BrokerController brokerController;

    private ConsumerOffsetManager consumerOffsetManager;

    @Before
    @SuppressWarnings("DoubleBraceInitialization")
    public void init() {
        brokerController = Mockito.mock(BrokerController.class);
        consumerOffsetManager = new ConsumerOffsetManager(brokerController);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

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
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
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
    public void testQueryMinOffsetInAllGroupDoesNotDeleteOffsets() {
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Mockito.when(messageStore.getMinOffsetInQueue(Mockito.anyString(), Mockito.anyInt())).thenReturn(0L);

        String topic = "Topic";
        String group1 = "G1";
        String group2 = "G2";
        ConcurrentHashMap<Integer, Long> offsets1 = new ConcurrentHashMap<>();
        offsets1.put(0, 50L);
        ConcurrentHashMap<Integer, Long> offsets2 = new ConcurrentHashMap<>();
        offsets2.put(0, 30L);
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + group1, offsets1);
        offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + group2, offsets2);
        consumerOffsetManager.setOffsetTable(offsetTable);

        // filtering out G2 must exclude its offsets from the min computation
        Map<Integer, Long> result = consumerOffsetManager.queryMinOffsetInAllGroup(topic, group2);
        assertThat(result).containsEntry(0, 50L);

        // but the query must not destroy the filtered group's offsets
        assertThat(offsetTable).containsKey(topic + TOPIC_GROUP_SEPARATOR + group2);
        assertThat(consumerOffsetManager.queryOffset(group2, topic, 0)).isEqualTo(30L);

        // without filter, the min across all groups is returned
        result = consumerOffsetManager.queryMinOffsetInAllGroup(topic, "");
        assertThat(result).containsEntry(0, 30L);
    }

    @Test
    public void testQueryMinOffsetInAllGroupToleratesMalformedKeys() {
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Mockito.when(messageStore.getMinOffsetInQueue(Mockito.anyString(), Mockito.anyInt())).thenReturn(0L);

        String topic = "Topic";
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + "G1", new ConcurrentHashMap<>());
        // malformed key without '@' must not break the query
        offsetTable.put("MalformedKey", new ConcurrentHashMap<>());
        consumerOffsetManager.setOffsetTable(offsetTable);

        assertThat(consumerOffsetManager.queryMinOffsetInAllGroup(topic, "G1")).isEmpty();
        assertThat(consumerOffsetManager.queryMinOffsetInAllGroup(topic, "")).isEmpty();
    }
}
