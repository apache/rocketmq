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
package org.apache.rocketmq.broker.config.v1;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.RocksDBConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RocksDBTopicConfigManagerTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    @Mock
    private RocksDBConfigManager rocksDBConfigManager;

    private RocksDBTopicConfigManager rocksDBTopicConfigManager;

    @Before
    public void init() throws Exception {
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStoreConfig.getMemTableFlushIntervalMs()).thenReturn(1000L);
        when(messageStoreConfig.getRocksdbCompressionType()).thenReturn("LZ4_COMPRESSION");
        when(messageStoreConfig.getStorePathRootDir()).thenReturn("/");
        rocksDBTopicConfigManager = new RocksDBTopicConfigManager(brokerController);
        FieldUtils.writeDeclaredField(rocksDBTopicConfigManager, "rocksDBConfigManager", rocksDBConfigManager, true);
    }

    @Test
    public void testDecodeTopicConfig() {
        String topicName = "testTopic";
        String topicConfigJson = "{\"topicName\":\"testTopic\",\"readQueueNums\":10,\"writeQueueNums\":10}";
        byte[] key = topicName.getBytes(StandardCharsets.UTF_8);
        byte[] body = topicConfigJson.getBytes(StandardCharsets.UTF_8);

        rocksDBTopicConfigManager.decodeTopicConfig(key, body);

        ConcurrentMap<String, TopicConfig> topicConfigTable = rocksDBTopicConfigManager.getTopicConfigTable();
        assertNotNull(topicConfigTable);
        assertEquals(1, topicConfigTable.size());
        TopicConfig topicConfig = topicConfigTable.get(topicName);
        assertNotNull(topicConfig);
        assertEquals(topicName, topicConfig.getTopicName());
        assertEquals(10, topicConfig.getReadQueueNums());
        assertEquals(10, topicConfig.getWriteQueueNums());
    }

    @Test
    public void testPutTopicConfig() throws Exception {
        TopicConfig newTopicConfig = new TopicConfig("newTopic");
        newTopicConfig.setReadQueueNums(10);
        newTopicConfig.setWriteQueueNums(10);

        assertNull(rocksDBTopicConfigManager.putTopicConfig(newTopicConfig));
        verify(rocksDBConfigManager, times(1)).put(any(byte[].class), anyInt(), any(byte[].class));
    }
}
