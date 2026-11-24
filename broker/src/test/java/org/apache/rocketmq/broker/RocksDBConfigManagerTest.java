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

package org.apache.rocketmq.broker;

import org.apache.rocketmq.broker.config.v1.RocksDBConfigManager;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class RocksDBConfigManagerTest {

    private ConfigRocksDBStorage configRocksDBStorage;

    private RocksDBConfigManager rocksDBConfigManager;

    @Before
    public void setUp() throws IllegalAccessException {
        configRocksDBStorage = mock(ConfigRocksDBStorage.class);
        rocksDBConfigManager = spy(new RocksDBConfigManager("testPath", 1000L, null));
        rocksDBConfigManager.configRocksDBStorage = configRocksDBStorage;
    }

    @Test
    public void testLoadDataVersion() throws Exception {
        DataVersion expected = new DataVersion();
        expected.nextVersion();

        when(rocksDBConfigManager.getKvDataVersion()).thenReturn(expected);

        boolean result = rocksDBConfigManager.loadDataVersion();

        assertTrue(result);
        assertEquals(expected.getCounter().get(), rocksDBConfigManager.getKvDataVersion().getCounter().get());
        assertEquals(expected.getTimestamp(), rocksDBConfigManager.getKvDataVersion().getTimestamp());
    }

    @Test
    public void testUpdateKvDataVersion() throws Exception {
        rocksDBConfigManager.updateKvDataVersion();

        verify(rocksDBConfigManager, times(1)).updateKvDataVersion();
    }
}
