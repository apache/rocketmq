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

package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBTopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RocksdbTopicConfigTransferTest {

    private final String basePath = Paths.get(System.getProperty("user.home"),
            "unit-test-store", UUID.randomUUID().toString().substring(0, 16).toUpperCase()).toString();

    private RocksDBTopicConfigManager rocksdbTopicConfigManager;

    private TopicConfigManager jsonTopicConfigManager;
    @Mock
    private BrokerController brokerController;

    @Mock
    private DefaultMessageStore defaultMessageStore;

    @Before
    public void init() {
        if (notToBeExecuted()) {
            return;
        }
        BrokerConfig brokerConfig = new BrokerConfig();
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(basePath);
        Mockito.lenient().when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(defaultMessageStore.getStateMachineVersion()).thenReturn(0L);
    }

    @After
    public void destroy() {
        if (notToBeExecuted()) {
            return;
        }
        Path pathToBeDeleted = Paths.get(basePath);
        try {
            Files.walk(pathToBeDeleted)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // ignore
                        }
                    });
        } catch (IOException e) {
            // ignore
        }
        if (rocksdbTopicConfigManager != null) {
            rocksdbTopicConfigManager.stop();
        }
    }

    public void initRocksdbTopicConfigManager() {
        if (rocksdbTopicConfigManager == null) {
            rocksdbTopicConfigManager = new RocksDBTopicConfigManager(brokerController);
            rocksdbTopicConfigManager.load();
        }
    }

    public void initJsonTopicConfigManager() {
        if (jsonTopicConfigManager == null) {
            jsonTopicConfigManager = new TopicConfigManager(brokerController);
            jsonTopicConfigManager.load();
        }
    }

    @Test
    public void theFirstTimeLoadJsonTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        initJsonTopicConfigManager();
        DataVersion dataVersion = jsonTopicConfigManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(0L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, jsonTopicConfigManager.getTopicConfigTable().size());
    }

    @Test
    public void theFirstTimeLoadRocksdbTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicConfigManager();
        DataVersion dataVersion = rocksdbTopicConfigManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(0L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksdbTopicConfigManager.getTopicConfigTable().size());
    }


    @Test
    public void addTopicLoadJsonTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        initJsonTopicConfigManager();
        String topicName = "testAddTopicConfig-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);
        DataVersion beforeDataVersion = jsonTopicConfigManager.getDataVersion();
        long beforeDataVersionCounter = beforeDataVersion.getCounter().get();
        long beforeTimestamp = beforeDataVersion.getTimestamp();

        jsonTopicConfigManager.updateTopicConfig(topicConfig);

        DataVersion afterDataVersion = jsonTopicConfigManager.getDataVersion();
        long afterDataVersionCounter = afterDataVersion.getCounter().get();
        long afterTimestamp = afterDataVersion.getTimestamp();

        Assert.assertEquals(0, beforeDataVersionCounter);
        Assert.assertEquals(1, afterDataVersionCounter);
        Assert.assertTrue(afterTimestamp >= beforeTimestamp);
    }

    @Test
    public void addTopicLoadRocksdbTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicConfigManager();
        String topicName = "testAddTopicConfig-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setAttributes(attributes);
        DataVersion beforeDataVersion = rocksdbTopicConfigManager.getDataVersion();
        long beforeDataVersionCounter = beforeDataVersion.getCounter().get();
        long beforeTimestamp = beforeDataVersion.getTimestamp();

        rocksdbTopicConfigManager.updateTopicConfig(topicConfig);

        DataVersion afterDataVersion = rocksdbTopicConfigManager.getDataVersion();
        long afterDataVersionCounter = afterDataVersion.getCounter().get();
        long afterTimestamp = afterDataVersion.getTimestamp();
        Assert.assertEquals(0, beforeDataVersionCounter);
        Assert.assertEquals(1, afterDataVersionCounter);
        Assert.assertTrue(afterTimestamp >= beforeTimestamp);
    }

    @Test
    public void theSecondTimeLoadJsonTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        addTopicLoadJsonTopicConfigManager();
        jsonTopicConfigManager.stop();
        jsonTopicConfigManager = new TopicConfigManager(brokerController);
        jsonTopicConfigManager.load();
        DataVersion dataVersion = jsonTopicConfigManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(1L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, jsonTopicConfigManager.getTopicConfigTable().size());
    }

    @Test
    public void theSecondTimeLoadRocksdbTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        addTopicLoadRocksdbTopicConfigManager();
        rocksdbTopicConfigManager.stop();
        rocksdbTopicConfigManager = null;
        rocksdbTopicConfigManager = new RocksDBTopicConfigManager(brokerController);
        rocksdbTopicConfigManager.load();
        DataVersion dataVersion = rocksdbTopicConfigManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(1L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksdbTopicConfigManager.getTopicConfigTable().size());
    }

    @Test
    public void jsonUpgradeToRocksdb() {
        if (notToBeExecuted()) {
            return;
        }
        addTopicLoadJsonTopicConfigManager();
        initRocksdbTopicConfigManager();
        DataVersion dataVersion = rocksdbTopicConfigManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(2L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksdbTopicConfigManager.getTopicConfigTable().size());
        Assert.assertEquals(rocksdbTopicConfigManager.getTopicConfigTable().size(), jsonTopicConfigManager.getTopicConfigTable().size());

        rocksdbTopicConfigManager.stop();
        rocksdbTopicConfigManager = new RocksDBTopicConfigManager(brokerController);
        rocksdbTopicConfigManager.load();
        dataVersion = rocksdbTopicConfigManager.getDataVersion();
        Assert.assertEquals(2L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksdbTopicConfigManager.getTopicConfigTable().size());
        Assert.assertEquals(rocksdbTopicConfigManager.getTopicConfigTable().size(), rocksdbTopicConfigManager.getTopicConfigTable().size());
    }


    private boolean notToBeExecuted() {
        return MixAll.isMac();
    }

}
