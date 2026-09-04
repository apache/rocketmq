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
package org.apache.rocketmq.remoting;

import java.util.Properties;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;

public class ConfigurationTest {

    @Test
    public void testGetAllConfigsSnapshotRefreshesAndCopiesProperties() {
        TestConfig testConfig = new TestConfig();
        Configuration configuration = new Configuration(mock(Logger.class), testConfig);
        testConfig.customPath = "C:\\rocketmq\\store";

        Properties snapshot = configuration.getAllConfigsSnapshot();

        assertEquals("C:\\rocketmq\\store", snapshot.getProperty("customPath"));
        assertNotSame(configuration.getAllConfigs(), snapshot);
        snapshot.remove("customPath");
        assertEquals("C:\\rocketmq\\store", configuration.getAllConfigs().getProperty("customPath"));
    }

    private static class TestConfig {
        private String customPath = "initial";
    }
}
