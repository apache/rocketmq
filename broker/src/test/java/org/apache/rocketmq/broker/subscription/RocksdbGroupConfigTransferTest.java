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

package org.apache.rocketmq.broker.subscription;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
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
public class RocksdbGroupConfigTransferTest {
    private final String basePath = Paths.get(System.getProperty("user.home"),
            "unit-test-store", UUID.randomUUID().toString().substring(0, 16).toUpperCase()).toString();

    private RocksDBSubscriptionGroupManager rocksDBSubscriptionGroupManager;

    private SubscriptionGroupManager jsonSubscriptionGroupManager;
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
        Mockito.lenient().when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(basePath);
        Mockito.lenient().when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.lenient().when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
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
        if (rocksDBSubscriptionGroupManager != null) {
            rocksDBSubscriptionGroupManager.stop();
        }
    }


    public void initRocksDBSubscriptionGroupManager() {
        if (rocksDBSubscriptionGroupManager == null) {
            rocksDBSubscriptionGroupManager = new RocksDBSubscriptionGroupManager(brokerController);
            rocksDBSubscriptionGroupManager.load();
        }
    }

    public void initJsonSubscriptionGroupManager() {
        if (jsonSubscriptionGroupManager == null) {
            jsonSubscriptionGroupManager = new SubscriptionGroupManager(brokerController);
            jsonSubscriptionGroupManager.load();
        }
    }

    @Test
    public void theFirstTimeLoadJsonSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        initJsonSubscriptionGroupManager();
        DataVersion dataVersion = jsonSubscriptionGroupManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(0L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, jsonSubscriptionGroupManager.getSubscriptionGroupTable().size());
    }

    @Test
    public void theFirstTimeLoadRocksDBSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        initRocksDBSubscriptionGroupManager();
        DataVersion dataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(0L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size());
    }


    @Test
    public void addGroupLoadJsonSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        initJsonSubscriptionGroupManager();
        int beforeSize = jsonSubscriptionGroupManager.getSubscriptionGroupTable().size();
        String groupName = "testAddGroupConfig-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(groupName);
        subscriptionGroupConfig.setAttributes(attributes);
        DataVersion beforeDataVersion = jsonSubscriptionGroupManager.getDataVersion();
        long beforeDataVersionCounter = beforeDataVersion.getCounter().get();
        long beforeTimestamp = beforeDataVersion.getTimestamp();

        jsonSubscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);

        int afterSize = jsonSubscriptionGroupManager.getSubscriptionGroupTable().size();
        DataVersion afterDataVersion = jsonSubscriptionGroupManager.getDataVersion();
        long afterDataVersionCounter = afterDataVersion.getCounter().get();
        long afterTimestamp = afterDataVersion.getTimestamp();

        Assert.assertEquals(0, beforeDataVersionCounter);
        Assert.assertEquals(1, afterDataVersionCounter);
        Assert.assertEquals(1, afterSize - beforeSize);
        Assert.assertTrue(afterTimestamp >= beforeTimestamp);
    }

    @Test
    public void addForbiddenGroupLoadJsonSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        initJsonSubscriptionGroupManager();
        int beforeSize = jsonSubscriptionGroupManager.getForbiddenTable().size();
        String groupName = "testAddGroupConfig-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(groupName);
        subscriptionGroupConfig.setAttributes(attributes);
        DataVersion beforeDataVersion = jsonSubscriptionGroupManager.getDataVersion();
        long beforeDataVersionCounter = beforeDataVersion.getCounter().get();
        long beforeTimestamp = beforeDataVersion.getTimestamp();

        jsonSubscriptionGroupManager.setForbidden(groupName, "topic", 0);
        int afterSize = jsonSubscriptionGroupManager.getForbiddenTable().size();
        DataVersion afterDataVersion = jsonSubscriptionGroupManager.getDataVersion();
        long afterDataVersionCounter = afterDataVersion.getCounter().get();
        long afterTimestamp = afterDataVersion.getTimestamp();

        Assert.assertEquals(1, afterDataVersionCounter - beforeDataVersionCounter);
        Assert.assertEquals(1, afterSize - beforeSize);
        Assert.assertTrue(afterTimestamp >= beforeTimestamp);
    }

    @Test
    public void addGroupLoadRocksdbSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        initRocksDBSubscriptionGroupManager();
        int beforeSize = rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size();
        String groupName = "testAddGroupConfig-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(groupName);
        subscriptionGroupConfig.setAttributes(attributes);
        DataVersion beforeDataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        long beforeDataVersionCounter = beforeDataVersion.getCounter().get();
        long beforeTimestamp = beforeDataVersion.getTimestamp();

        rocksDBSubscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);
        int afterSize = rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size();
        DataVersion afterDataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        long afterDataVersionCounter = afterDataVersion.getCounter().get();
        long afterTimestamp = afterDataVersion.getTimestamp();
        Assert.assertEquals(1, afterDataVersionCounter);
        Assert.assertEquals(0, beforeDataVersionCounter);
        Assert.assertEquals(1, afterSize - beforeSize);
        Assert.assertTrue(afterTimestamp >= beforeTimestamp);
    }

    @Test
    public void addForbiddenLoadRocksdbSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        initRocksDBSubscriptionGroupManager();
        int beforeSize = rocksDBSubscriptionGroupManager.getForbiddenTable().size();
        String groupName = "testAddGroupConfig-" + System.currentTimeMillis();

        Map<String, String> attributes = new HashMap<>();

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(groupName);
        subscriptionGroupConfig.setAttributes(attributes);
        DataVersion beforeDataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        long beforeDataVersionCounter = beforeDataVersion.getCounter().get();
        long beforeTimestamp = beforeDataVersion.getTimestamp();

        rocksDBSubscriptionGroupManager.updateForbidden(groupName, "topic", 0, true);

        int afterSize = rocksDBSubscriptionGroupManager.getForbiddenTable().size();
        DataVersion afterDataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        long afterDataVersionCounter = afterDataVersion.getCounter().get();
        long afterTimestamp = afterDataVersion.getTimestamp();
        Assert.assertEquals(1, afterDataVersionCounter - beforeDataVersionCounter);
        Assert.assertEquals(1, afterSize - beforeSize);
        Assert.assertTrue(afterTimestamp >= beforeTimestamp);
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size());
    }

    @Test
    public void theSecondTimeLoadJsonSubscriptionGroupManager() {
        if (notToBeExecuted()) {
            return;
        }
        addGroupLoadJsonSubscriptionGroupManager();
        jsonSubscriptionGroupManager.stop();
        rocksDBSubscriptionGroupManager = null;
        addForbiddenGroupLoadJsonSubscriptionGroupManager();
        jsonSubscriptionGroupManager.stop();
        rocksDBSubscriptionGroupManager = null;
        jsonSubscriptionGroupManager = new SubscriptionGroupManager(brokerController);
        jsonSubscriptionGroupManager.load();
        DataVersion dataVersion = jsonSubscriptionGroupManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(2L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, jsonSubscriptionGroupManager.getSubscriptionGroupTable().size());
        Assert.assertNotEquals(0, jsonSubscriptionGroupManager.getForbiddenTable().size());
        Assert.assertNotEquals(0, jsonSubscriptionGroupManager.getSubscriptionGroupTable().size());
    }

    @Test
    public void theSecondTimeLoadRocksdbTopicConfigManager() {
        if (notToBeExecuted()) {
            return;
        }
        addGroupLoadRocksdbSubscriptionGroupManager();
        rocksDBSubscriptionGroupManager.stop();
        rocksDBSubscriptionGroupManager = null;
        addForbiddenLoadRocksdbSubscriptionGroupManager();
        rocksDBSubscriptionGroupManager.stop();
        rocksDBSubscriptionGroupManager = null;
        rocksDBSubscriptionGroupManager = new RocksDBSubscriptionGroupManager(brokerController);
        rocksDBSubscriptionGroupManager.load();
        DataVersion dataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(2L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getForbiddenTable().size());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size());
    }


    @Test
    public void jsonUpgradeToRocksdb() {
        if (notToBeExecuted()) {
            return;
        }
        addGroupLoadJsonSubscriptionGroupManager();
        addForbiddenGroupLoadJsonSubscriptionGroupManager();
        initRocksDBSubscriptionGroupManager();
        DataVersion dataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        Assert.assertNotNull(dataVersion);
        Assert.assertEquals(3L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getForbiddenTable().size());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size());
        Assert.assertEquals(rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size(), jsonSubscriptionGroupManager.getSubscriptionGroupTable().size());
        Assert.assertEquals(rocksDBSubscriptionGroupManager.getForbiddenTable().size(), jsonSubscriptionGroupManager.getForbiddenTable().size());

        rocksDBSubscriptionGroupManager.stop();
        rocksDBSubscriptionGroupManager = new RocksDBSubscriptionGroupManager(brokerController);
        rocksDBSubscriptionGroupManager.load();
        dataVersion = rocksDBSubscriptionGroupManager.getDataVersion();
        Assert.assertEquals(3L, dataVersion.getCounter().get());
        Assert.assertEquals(0L, dataVersion.getStateVersion());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getForbiddenTable().size());
        Assert.assertNotEquals(0, rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size());
        Assert.assertEquals(rocksDBSubscriptionGroupManager.getSubscriptionGroupTable().size(), jsonSubscriptionGroupManager.getSubscriptionGroupTable().size());
        Assert.assertEquals(rocksDBSubscriptionGroupManager.getForbiddenTable().size(), jsonSubscriptionGroupManager.getForbiddenTable().size());
    }

    private boolean notToBeExecuted() {
        return MixAll.isMac() || MixAll.isWindows();
    }

}
