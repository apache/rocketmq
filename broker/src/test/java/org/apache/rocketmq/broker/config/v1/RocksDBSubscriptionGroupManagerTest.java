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
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RocksDBSubscriptionGroupManagerTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private RocksDBConfigManager rocksDBConfigManager;

    private RocksDBSubscriptionGroupManager rocksDBSubscriptionGroupManager;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    @Before
    public void init() throws IllegalAccessException {
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStoreConfig.getMemTableFlushIntervalMs()).thenReturn(1000L);
        when(messageStoreConfig.getRocksdbCompressionType()).thenReturn("LZ4_COMPRESSION");
        when(messageStoreConfig.getStorePathRootDir()).thenReturn("/");
        rocksDBSubscriptionGroupManager = new RocksDBSubscriptionGroupManager(brokerController);
        FieldUtils.writeDeclaredField(rocksDBSubscriptionGroupManager, "rocksDBConfigManager", rocksDBConfigManager, true);
    }

    @Test
    public void testPutSubscriptionGroupConfig() {
        SubscriptionGroupConfig newConfig = new SubscriptionGroupConfig();
        newConfig.setGroupName("group");
        SubscriptionGroupConfig oldConfig = new SubscriptionGroupConfig();
        oldConfig.setGroupName("group");
        rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().put("group", oldConfig);

        assertEquals(oldConfig, rocksDBSubscriptionGroupManager.putSubscriptionGroupConfig(newConfig));
        assertEquals(newConfig, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().get("group"));
    }

    @Test
    public void testPutSubscriptionGroupConfigIfAbsent() {
        SubscriptionGroupConfig newConfig = new SubscriptionGroupConfig();
        newConfig.setGroupName("group");
        SubscriptionGroupConfig oldConfig = new SubscriptionGroupConfig();
        oldConfig.setGroupName("group");

        assertNull(rocksDBSubscriptionGroupManager.putSubscriptionGroupConfigIfAbsent(newConfig));
        assertEquals(newConfig, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().get("group"));
    }

    @Test
    public void testDecodeForbidden() {
        String forbiddenGroupName = "group";
        String bodyJson = "{\"topic1\":1,\"topic2\":2}";
        byte[] key = forbiddenGroupName.getBytes(StandardCharsets.UTF_8);
        byte[] body = bodyJson.getBytes(StandardCharsets.UTF_8);

        rocksDBSubscriptionGroupManager.decodeForbidden(key, body);
        ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable = rocksDBSubscriptionGroupManager.getForbiddenTable();
        assertTrue(forbiddenTable.containsKey(forbiddenGroupName));

        ConcurrentMap<String, Integer> forbiddenGroup = forbiddenTable.get(forbiddenGroupName);
        assertEquals(2, forbiddenGroup.size());
        assertEquals(Integer.valueOf(1), forbiddenGroup.get("topic1"));
        assertEquals(Integer.valueOf(2), forbiddenGroup.get("topic2"));
    }

    @Test
    public void testDecodeSubscriptionGroup() {
        String groupName = "group";
        String bodyJson = "{\"groupName\":\"group\",\"consumeEnable\":true}";
        byte[] key = groupName.getBytes(StandardCharsets.UTF_8);
        byte[] body = bodyJson.getBytes(StandardCharsets.UTF_8);

        rocksDBSubscriptionGroupManager.decodeSubscriptionGroup(key, body);
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = rocksDBSubscriptionGroupManager.getSubscriptionGroupTable();
        assertEquals(1, subscriptionGroupTable.size());
        SubscriptionGroupConfig config = subscriptionGroupTable.get(groupName);
        assertEquals(groupName, config.getGroupName());
        assertTrue(config.isConsumeEnable());
    }
}
