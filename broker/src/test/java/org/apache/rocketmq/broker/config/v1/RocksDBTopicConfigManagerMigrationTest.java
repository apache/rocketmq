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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RocksDBTopicConfigManagerMigrationTest {

    private static final String TEST_TOPIC = "TestTopic";
    
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
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        
        separateRocksDBPath = storePath + File.separator + "config" + File.separator + "topics" + File.separator;
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
        RocksDBTopicConfigManager separateManager = new RocksDBTopicConfigManager(brokerController, false);
        separateManager.load();
        
        // Add some topic configs
        TopicConfig topicConfig = new TopicConfig(TEST_TOPIC, 4, 4);
        separateManager.updateTopicConfig(topicConfig);
        separateManager.persist();
        separateManager.stop();

        {
            // Now create unified RocksDB manager which should migrate data
            RocksDBTopicConfigManager unifiedManager = new RocksDBTopicConfigManager(brokerController, true);
            boolean loaded = unifiedManager.load();
            Assert.assertTrue("Unified manager should load successfully", loaded);

            // Verify that data was migrated
            TopicConfig migratedConfig = unifiedManager.selectTopicConfig(TEST_TOPIC);
            Assert.assertNotNull("Topic config should be migrated", migratedConfig);
            Assert.assertEquals("Topic name should match", TEST_TOPIC, migratedConfig.getTopicName());
            Assert.assertEquals("Read queue num should match", 4, migratedConfig.getReadQueueNums());
            Assert.assertEquals("Write queue num should match", 4, migratedConfig.getWriteQueueNums());

            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);
            unifiedManager.updateTopicConfig(topicConfig);
            unifiedManager.persist();
            unifiedManager.stop();
        }

        {
            // Now create unified RocksDB manager which should migrate data
            RocksDBTopicConfigManager unifiedManager = new RocksDBTopicConfigManager(brokerController, true);
            boolean loaded = unifiedManager.load();
            Assert.assertTrue("Unified manager should load successfully", loaded);

            // Verify that data was migrated
            TopicConfig migratedConfig = unifiedManager.selectTopicConfig(TEST_TOPIC);
            Assert.assertNotNull("Topic config should be migrated", migratedConfig);
            Assert.assertEquals("Topic name should match", TEST_TOPIC, migratedConfig.getTopicName());
            Assert.assertEquals("Read queue num should match", 8, migratedConfig.getReadQueueNums());
            Assert.assertEquals("Write queue num should match", 8, migratedConfig.getWriteQueueNums());

            unifiedManager.stop();
        }

    }

    @Test
    public void testMigrationWithNoSeparateRocksDB() {
        
        // Ensure separate RocksDB doesn't exist
        UtilAll.deleteFile(new File(separateRocksDBPath));
        
        // Create unified RocksDB manager - should not fail even without separate DB
        RocksDBTopicConfigManager unifiedManager = new RocksDBTopicConfigManager(brokerController, true);
        boolean loaded = unifiedManager.load();
        Assert.assertTrue("Unified manager should load successfully even without separate DB", loaded);
        
        unifiedManager.stop();
    }

    @Test
    public void testNoMigrationWhenDisabled() {
        // Create data in separate RocksDB mode
        RocksDBTopicConfigManager separateManager = new RocksDBTopicConfigManager(brokerController, false);
        separateManager.load();
        
        TopicConfig topicConfig = new TopicConfig(TEST_TOPIC, 4, 4);
        separateManager.putTopicConfig(topicConfig);
        separateManager.persist();
        separateManager.stop();
        
        // Create another separate manager - should not trigger migration
        RocksDBTopicConfigManager anotherSeparateManager = new RocksDBTopicConfigManager(brokerController, false);
        boolean loaded = anotherSeparateManager.load();
        Assert.assertTrue("Separate manager should load successfully", loaded);
        
        anotherSeparateManager.stop();
    }
}