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

package org.apache.rocketmq.tieredstore;

import java.io.File;
import java.util.Properties;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MessageStoreConfigLogTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testObjectStoreKeyUpdatesMaskLogsAndPreserveValues() throws Exception {
        Logger logger = mock(Logger.class);
        File configFile = temporaryFolder.newFile();
        Configuration configuration = new Configuration(logger, configFile.getAbsolutePath());
        Properties initial = new Properties();
        initial.setProperty("objectStoreAccessKey", "old-object-ak");
        initial.setProperty("objectStoreSecretKey", "old-object-sk");
        configuration.registerConfig(initial);
        MessageStoreConfig config = new MessageStoreConfig();
        MessageStorePluginContext context = new MessageStorePluginContext(null, null, null, null, configuration);
        context.registerConfiguration(config);
        Properties update = new Properties();
        update.setProperty("objectStoreAccessKey", "new-object-ak");
        update.setProperty("objectStoreSecretKey", "new-object-sk");

        configuration.update(update);

        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "objectStoreAccessKey", "******", "******");
        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "objectStoreSecretKey", "******", "******");
        verify(logger, never()).info("Replace, key: {}, value: {} -> {}",
            "objectStoreAccessKey", "old-object-ak", "new-object-ak");
        verify(logger, never()).info("Replace, key: {}, value: {} -> {}",
            "objectStoreSecretKey", "old-object-sk", "new-object-sk");
        assertThat(config.getObjectStoreAccessKey()).isEqualTo("new-object-ak");
        assertThat(config.getObjectStoreSecretKey()).isEqualTo("new-object-sk");
        assertThat(configuration.getAllConfigsSnapshot()).containsAllEntriesOf(update);
        Properties persisted = MixAll.string2Properties(MixAll.file2String(configFile));
        assertThat(persisted).containsAllEntriesOf(update);
    }
}
