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

import java.util.Arrays;
import java.util.Collection;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class RocksDBTopicConfigManagerPersistenceTest {
    @Parameterized.Parameters(name = "singleRocksDB={0}, realTimePersist={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {false, false}, {false, true}, {true, false}, {true, true}
        });
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean useSingleRocksDB;
    private final boolean realTimePersist;
    private BrokerController brokerController;
    private RocksDBTopicConfigManager manager;

    public RocksDBTopicConfigManagerPersistenceTest(boolean useSingleRocksDB, boolean realTimePersist) {
        this.useSingleRocksDB = useSingleRocksDB;
        this.realTimePersist = realTimePersist;
    }

    @Before
    public void setUp() throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setUseSingleRocksDBForAllConfigs(useSingleRocksDB);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(temporaryFolder.newFolder("store").getAbsolutePath());
        storeConfig.setRealTimePersistRocksDBConfig(realTimePersist);
        brokerController = mock(BrokerController.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getMessageStoreConfig()).thenReturn(storeConfig);
        manager = new RocksDBTopicConfigManager(brokerController);
        assertTrue(manager.load());
    }

    @After
    public void tearDown() {
        if (manager != null) {
            manager.stop();
        }
    }

    @Test
    public void testPersistMemoryOnlyDataVersionAcrossRestart() {
        manager.updateTopicConfig(new TopicConfig("test-topic"));
        // Broker permission updates and role changes can advance the returned version directly.
        manager.getDataVersion().nextVersion(42L);
        DataVersion expected = new DataVersion();
        expected.assignNewOne(manager.getDataVersion());

        manager.persist();
        manager.persist();
        assertEquals("Persist must not advance the version", expected, manager.getDataVersion());

        manager.stop();
        manager = new RocksDBTopicConfigManager(brokerController);
        assertTrue(manager.load());
        assertEquals(expected.getCounter().get(), manager.getDataVersion().getCounter().get());
        assertEquals(expected.getTimestamp(), manager.getDataVersion().getTimestamp());
        assertEquals(expected.getStateVersion(), manager.getDataVersion().getStateVersion());
        assertNotNull(manager.selectTopicConfig("test-topic"));
    }
}
