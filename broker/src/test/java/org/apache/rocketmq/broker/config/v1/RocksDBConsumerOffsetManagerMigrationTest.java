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

import java.io.File;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RocksDBConsumerOffsetManagerMigrationTest {

    private static final String TEST_GROUP = "TestGroup";
    private static final String TEST_TOPIC = "TestTopic";
    private static final String TEST_KEY = TEST_TOPIC + "@" + TEST_GROUP;
    
    private BrokerController brokerController;
    private String storePath;
    private String separateRocksDBPath;
    private String unifiedRocksDBPath;

    @Before
    public void init() {
        brokerController = Mockito.mock(BrokerController.class);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        storePath = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test-" + System.currentTimeMillis();
        messageStoreConfig.setStorePathRootDir(storePath);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setConsumerOffsetUpdateVersionStep(1);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        
        separateRocksDBPath = storePath + File.separator + "config" + File.separator + "consumerOffsets" + File.separator;
        unifiedRocksDBPath = storePath + File.separator + "config" + File.separator + "metadata" + File.separator;
        
        // Create directories
        UtilAll.ensureDirOK(separateRocksDBPath);
        UtilAll.ensureDirOK(unifiedRocksDBPath);
    }

    @After
    public void destroy() {
        // Clean up test directories
        UtilAll.deleteFile(new File(storePath));
    }

    @Test
    public void testMigrationFromSeparateToUnifiedRocksDB() {
        
        // First, create data in separate RocksDB mode
        RocksDBConsumerOffsetManager separateManager = new RocksDBConsumerOffsetManager(brokerController, false);
        separateManager.load();
        
        // Add some consumer offsets
        separateManager.commitOffset("client", TEST_GROUP, TEST_TOPIC, 0, 100L);
        separateManager.commitOffset("client", TEST_GROUP, TEST_TOPIC, 1, 200L);
        separateManager.persist();
        separateManager.stop();
        
        // Now create unified RocksDB manager which should migrate data
        RocksDBConsumerOffsetManager unifiedManager = new RocksDBConsumerOffsetManager(brokerController, true);
        boolean loaded = unifiedManager.load();
        Assert.assertTrue("Unified manager should load successfully", loaded);
        
        // Verify that data was migrated
        ConcurrentMap<Integer, Long> migratedOffsetMap = unifiedManager.getOffsetTable().get(TEST_KEY);
        Assert.assertNotNull("Consumer offset should be migrated", migratedOffsetMap);
        Assert.assertEquals("Offset for queue 0 should match", Long.valueOf(100L), migratedOffsetMap.get(0));
        Assert.assertEquals("Offset for queue 1 should match", Long.valueOf(200L), migratedOffsetMap.get(1));

        unifiedManager.commitOffset("client", TEST_GROUP, TEST_TOPIC, 0, 300L);
        unifiedManager.commitOffset("client", TEST_GROUP, TEST_TOPIC, 1, 400L);
        unifiedManager.persist();
        unifiedManager.stop();

        // reload unified RocksDB manager which should not migrate data
        unifiedManager = new RocksDBConsumerOffsetManager(brokerController, true);
        unifiedManager.load();

        // Verify that data was new
        migratedOffsetMap = unifiedManager.getOffsetTable().get(TEST_KEY);
        Assert.assertEquals("Offset for queue 0 should match", Long.valueOf(300L), migratedOffsetMap.get(0));
        Assert.assertEquals("Offset for queue 1 should match", Long.valueOf(400L), migratedOffsetMap.get(1));
        unifiedManager.stop();
    }

    @Test
    public void testMigrationWithNoSeparateRocksDB() {
        
        // Ensure separate RocksDB doesn't exist
        UtilAll.deleteFile(new File(separateRocksDBPath));
        
        // Create unified RocksDB manager - should not fail even without separate DB
        RocksDBConsumerOffsetManager unifiedManager = new RocksDBConsumerOffsetManager(brokerController, true);
        boolean loaded = unifiedManager.load();
        Assert.assertTrue("Unified manager should load successfully even without separate DB", loaded);
        
        unifiedManager.stop();
    }

    @Test
    public void testNoMigrationWhenDisabled() {
        
        // Create data in separate RocksDB mode
        RocksDBConsumerOffsetManager separateManager = new RocksDBConsumerOffsetManager(brokerController, false);
        separateManager.load();

        separateManager.commitOffset("client", TEST_GROUP, TEST_TOPIC, 0, 100L);
        separateManager.commitOffset("client", TEST_GROUP, TEST_TOPIC, 1, 200L);
        separateManager.persist();
        separateManager.stop();

        long version = separateManager.getDataVersion().getCounter().get();
        Assert.assertEquals(2, version);

        // Create another separate manager - should not trigger migration
        RocksDBConsumerOffsetManager anotherSeparateManager = new RocksDBConsumerOffsetManager(brokerController, false);
        boolean loaded = anotherSeparateManager.load();
        Assert.assertTrue("Separate manager should load successfully", loaded);

        anotherSeparateManager.loadDataVersion();
        Assert.assertEquals(version, anotherSeparateManager.getDataVersion().getCounter().get());
        anotherSeparateManager.stop();
    }
}