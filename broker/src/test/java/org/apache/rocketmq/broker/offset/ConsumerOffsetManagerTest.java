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
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {

    private static final String KEY = "FooBar@FooBarGroup";

    private BrokerController brokerController;

    private ConsumerOffsetManager consumerOffsetManager;

    @Before
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
}
