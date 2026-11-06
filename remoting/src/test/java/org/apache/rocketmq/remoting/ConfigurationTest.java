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

import java.io.File;
import java.util.Properties;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.Sensitive;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static class AnnotatedConfig {
        @Sensitive
        private String opaqueValue = "old-secret";
        private String databasePassword = "old-password";
        private String unchangedValue = "same-value";

        public void setOpaqueValue(String opaqueValue) {
            this.opaqueValue = opaqueValue;
        }

        public void setDatabasePassword(String databasePassword) {
            this.databasePassword = databasePassword;
        }

        public void setUnchangedValue(String unchangedValue) {
            this.unchangedValue = unchangedValue;
        }
    }

    @Test
    public void testUpdateLogsOnlyChangedPropertiesAndMasksExplicitAnnotations() throws Exception {
        Logger logger = mock(Logger.class);
        AnnotatedConfig config = new AnnotatedConfig();
        Configuration configuration = new Configuration(logger,
            temporaryFolder.newFile().getAbsolutePath(), config);
        Properties properties = new Properties();
        properties.setProperty("opaqueValue", "new-secret");
        properties.setProperty("databasePassword", "new-password");
        properties.setProperty("unchangedValue", "same-value");

        configuration.update(properties);

        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "opaqueValue", "******", "******");
        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "databasePassword", "old-password", "new-password");
        verify(logger, never()).info("Replace, key: {}, value: {} -> {}",
            "unchangedValue", "same-value", "same-value");
        assertThat(config.opaqueValue).isEqualTo("new-secret");
        assertThat(config.databasePassword).isEqualTo("new-password");
        assertThat(configuration.getAllConfigs().getProperty("opaqueValue")).isEqualTo("new-secret");
    }

    @Test
    public void testPropertiesForLogMasksRegisteredFieldsWithoutChangingInput() {
        Configuration configuration = new Configuration(mock(Logger.class), new AnnotatedConfig());
        Properties properties = new Properties();
        properties.setProperty("opaqueValue", "request-secret");
        properties.setProperty("databasePassword", "ordinary-value");
        properties.setProperty("unknownProperty", "unknown-value");

        Properties masked = configuration.getPropertiesForLog(properties);

        assertThat(masked).hasSize(3)
            .containsEntry("opaqueValue", "******")
            .containsEntry("databasePassword", "ordinary-value")
            .containsEntry("unknownProperty", "unknown-value");
        assertThat(properties).containsEntry("opaqueValue", "request-secret");
        masked.setProperty("databasePassword", "changed-copy");
        assertThat(properties).containsEntry("databasePassword", "ordinary-value");
        assertThat(configuration.getAllConfigs()).containsEntry("opaqueValue", "old-secret");
    }

    @Test
    public void testInterruptedRegistrationAndUpdateDoNotLogProperties() {
        Logger logger = mock(Logger.class);
        Configuration configuration = new Configuration(logger, new AnnotatedConfig());
        Properties properties = new Properties();
        properties.setProperty("opaqueValue", "request-secret");
        String originalVersion = configuration.getDataVersionJson();

        try {
            Thread.currentThread().interrupt();
            configuration.registerConfig(properties);
            Thread.currentThread().interrupt();
            configuration.update(properties);
        } finally {
            Thread.interrupted();
        }

        verify(logger).error("register config interrupted while waiting for lock");
        verify(logger).error("update config interrupted while waiting for lock");
        verifyNoMoreInteractions(logger);
        assertThat(configuration.getAllConfigs()).containsEntry("opaqueValue", "old-secret");
        assertThat(configuration.getDataVersionJson()).isEqualTo(originalVersion);
    }

    @Test
    public void testRegistrationMasksReplacementUsingIncomingObjectMetadata() {
        Logger logger = mock(Logger.class);
        Configuration configuration = new Configuration(logger);
        Properties initial = new Properties();
        initial.setProperty("opaqueValue", "initial-secret");
        configuration.registerConfig(initial);

        configuration.registerConfig(new AnnotatedConfig());

        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "opaqueValue", "******", "******");
    }

    @Test
    public void testReadOnlyConfigKeepsMemoryUpdateAndCanPersistAfterPermissionRestored() throws Exception {
        Logger logger = mock(Logger.class);
        File configFile = temporaryFolder.newFile("broker.properties");
        String original = "opaqueValue=old-secret\n";
        MixAll.string2FileNotSafe(original, configFile.getAbsolutePath());
        AnnotatedConfig config = new AnnotatedConfig();
        Configuration configuration = new Configuration(logger, configFile.getAbsolutePath(), config);
        String originalVersion = configuration.getDataVersionJson();
        Properties update = new Properties();
        update.setProperty("opaqueValue", "new-secret");

        try {
            makeReadOnly(configFile);
            configuration.update(update);

            assertThat(config.opaqueValue).isEqualTo("new-secret");
            assertThat(configuration.getAllConfigs()).containsEntry("opaqueValue", "new-secret");
            assertThat(configuration.getDataVersionJson()).isNotEqualTo(originalVersion);
            assertThat(MixAll.file2String(configFile)).isEqualTo(original);
            assertThat(new File(configFile + ".bak")).doesNotExist();
            verify(logger).warn("Skip persisting configuration to {}: {} is not writable", configFile, configFile);
            verify(logger, never()).error(anyString(), any(Throwable.class));
        } finally {
            configFile.setWritable(true, false);
        }

        configuration.persist();
        assertThat(MixAll.string2Properties(MixAll.file2String(configFile)))
            .containsEntry("opaqueValue", "new-secret");
        assertThat(MixAll.file2String(configFile + ".bak")).isEqualTo(original);
    }

    @Test
    public void testReadOnlyBackupSkipsPersistence() throws Exception {
        Logger logger = mock(Logger.class);
        File configFile = temporaryFolder.newFile("broker.properties");
        File backupFile = temporaryFolder.newFile("broker.properties.bak");
        MixAll.string2FileNotSafe("original", configFile.getAbsolutePath());
        MixAll.string2FileNotSafe("backup", backupFile.getAbsolutePath());
        Configuration configuration = new Configuration(logger, configFile.getAbsolutePath(), new AnnotatedConfig());

        try {
            makeReadOnly(backupFile);
            configuration.persist();

            assertThat(MixAll.file2String(configFile)).isEqualTo("original");
            assertThat(MixAll.file2String(backupFile)).isEqualTo("backup");
            verify(logger).warn("Skip persisting configuration to {}: {} is not writable", backupFile, backupFile);
            verify(logger, never()).error(anyString(), any(Throwable.class));
        } finally {
            backupFile.setWritable(true, false);
        }
    }

    @Test
    public void testReadOnlyDirectoryPreventsCreatingBackup() throws Exception {
        Logger logger = mock(Logger.class);
        File directory = temporaryFolder.newFolder("config");
        File configFile = new File(directory, "broker.properties");
        MixAll.string2FileNotSafe("original", configFile.getAbsolutePath());
        Configuration configuration = new Configuration(logger, configFile.getAbsolutePath(), new AnnotatedConfig());

        try {
            makeReadOnly(directory);
            configuration.persist();

            assertThat(MixAll.file2String(configFile)).isEqualTo("original");
            File backupFile = new File(configFile + ".bak");
            assertThat(backupFile).doesNotExist();
            verify(logger).warn("Skip persisting configuration to {}: {} is not writable", backupFile, directory);
            verify(logger, never()).error(anyString(), any(Throwable.class));
        } finally {
            directory.setWritable(true, false);
        }
    }

    @Test
    public void testNewConfigChecksAncestorPermissionAndCreatesMissingDirectories() throws Exception {
        Logger logger = mock(Logger.class);
        File directory = temporaryFolder.newFolder("config");
        File configFile = new File(directory, "nested/broker.properties");
        Configuration configuration = new Configuration(logger, configFile.getAbsolutePath(), new AnnotatedConfig());

        try {
            makeReadOnly(directory);
            configuration.persist();

            assertThat(configFile).doesNotExist();
            verify(logger).warn("Skip persisting configuration to {}: {} is not writable", configFile, directory);
            verify(logger, never()).error(anyString(), any(Throwable.class));
        } finally {
            directory.setWritable(true, false);
        }

        configuration.persist();
        assertThat(MixAll.string2Properties(MixAll.file2String(configFile)))
            .containsEntry("opaqueValue", "old-secret");
    }

    private void makeReadOnly(File file) {
        assumeTrue(file.setWritable(false, false));
        // Privileged users and some file systems can still write despite the permission bits.
        assumeFalse(file.canWrite());
    }
}
