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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.rocketmq.broker.schedule.DelayOffsetSerializeWrapper;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BrokerInitializationFailureTest {
    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    private BrokerController brokerController;
    private Path delayOffsetPath;
    private Path backupPath;
    private final byte[] offsets = "{\"offsetTable\":{\"1\":42}}".getBytes(StandardCharsets.UTF_8);
    private final byte[] backupOffsets = "{\"offsetTable\":{\"1\":40}}".getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(temporaryFolder.newFolder("store").getAbsolutePath());
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(0);
        brokerController = new BrokerController(new BrokerConfig(), serverConfig, new NettyClientConfig(), storeConfig);
        delayOffsetPath = Paths.get(StorePathConfigHelper.getDelayOffsetStorePath(storeConfig.getStorePathRootDir()));
        backupPath = Paths.get(delayOffsetPath + ".bak");
    }

    @After
    public void tearDown() {
        brokerController.shutdown();
    }

    @Test
    public void testDelayOffsetPathBeforeStoreInitialization() {
        assertThat(brokerController.getMessageStore()).isNull();
        assertThat(brokerController.getScheduleMessageService().configFilePath()).isEqualTo(delayOffsetPath.toString());
    }

    @Test
    public void testShutdownPreservesOffsetsBeforeStoreInitialization() throws Exception {
        writeDelayOffsets();
        assertOffsetsPreservedOnShutdown();
    }

    @Test
    public void testShutdownPreservesOffsetsBeforeScheduleInitialization() throws Exception {
        createMessageStore();
        writeDelayOffsets();
        assertOffsetsPreservedOnShutdown();
    }

    @Test
    public void testShutdownPreservesOffsetsAfterDelayLevelLoadFails() throws Exception {
        createMessageStore();
        writeDelayOffsets();
        brokerController.getMessageStoreConfig().setMessageDelayLevel("invalid");

        assertThat(brokerController.getScheduleMessageService().load()).isFalse();

        assertOffsetsPreservedOnShutdown();
    }

    @Test
    public void testShutdownPreservesOffsetsAfterCorrectionFailsOnReload() throws Exception {
        MessageStore messageStore = createMessageStore();
        writeDelayOffsets();
        assertThat(brokerController.getScheduleMessageService().load()).isTrue();
        when(messageStore.findConsumeQueue(anyString(), anyInt())).thenThrow(new IllegalStateException("Store is unavailable"));

        assertThat(brokerController.getScheduleMessageService().load()).isFalse();

        assertOffsetsPreservedOnShutdown();
    }

    @Test
    public void testShutdownDoesNotPersistAfterOffsetFilesFailToLoad() throws Exception {
        createMessageStore();
        writeDelayOffsets();
        byte[] invalidOffsets = "{invalid".getBytes(StandardCharsets.UTF_8);
        Files.write(delayOffsetPath, invalidOffsets);
        Files.write(backupPath, invalidOffsets);

        assertThat(brokerController.getScheduleMessageService().load()).isFalse();
        assertThat(Files.exists(delayOffsetPath)).isFalse();

        for (int i = 0; i < 2; i++) {
            brokerController.shutdown();
            assertThat(Files.exists(delayOffsetPath)).isFalse();
            assertThat(Files.readAllBytes(backupPath)).isEqualTo(invalidOffsets);
        }
    }

    @Test
    public void testShutdownPersistsOffsetsAfterSuccessfulLoad() throws Exception {
        createMessageStore();
        writeDelayOffsets();
        assertThat(brokerController.getScheduleMessageService().load()).isTrue();

        assertLoadedOffsetsPersistOnShutdown();
    }

    @Test
    public void testShutdownPersistsSyncedOffsetsOnSlave() throws Exception {
        createMessageStore();
        writeDelayOffsets();
        brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
        assertThat(brokerController.getScheduleMessageService().loadWhenSyncDelayOffset()).isTrue();

        assertLoadedOffsetsPersistOnShutdown();
    }

    @Test
    public void testShutdownPreservesOffsetsAfterSlaveSyncReloadFails() throws Exception {
        createMessageStore();
        writeDelayOffsets();
        brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
        assertThat(brokerController.getScheduleMessageService().loadWhenSyncDelayOffset()).isTrue();
        brokerController.getMessageStoreConfig().setMessageDelayLevel("invalid");

        assertThat(brokerController.getScheduleMessageService().loadWhenSyncDelayOffset()).isFalse();

        assertOffsetsPreservedOnShutdown();
    }

    private MessageStore createMessageStore() {
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(brokerController.getMessageStoreConfig());
        brokerController.setMessageStore(messageStore);
        return messageStore;
    }

    private void writeDelayOffsets() throws Exception {
        Files.createDirectories(delayOffsetPath.getParent());
        Files.write(delayOffsetPath, offsets);
        Files.write(backupPath, backupOffsets);
    }

    private void assertOffsetsPreservedOnShutdown() throws Exception {
        for (int i = 0; i < 2; i++) {
            brokerController.shutdown();
            assertThat(Files.readAllBytes(delayOffsetPath)).isEqualTo(offsets);
            assertThat(Files.readAllBytes(backupPath)).isEqualTo(backupOffsets);
        }
    }

    private void assertLoadedOffsetsPersistOnShutdown() throws Exception {
        assertThat(brokerController.getScheduleMessageService().isStarted()).isFalse();
        assertThat(brokerController.getScheduleMessageService().getOffsetTable()).containsEntry(1, 42L);
        brokerController.getScheduleMessageService().getOffsetTable().put(1, 84L);

        brokerController.shutdown();

        String json = new String(Files.readAllBytes(delayOffsetPath), StandardCharsets.UTF_8);
        DelayOffsetSerializeWrapper offsets = DelayOffsetSerializeWrapper.fromJson(json, DelayOffsetSerializeWrapper.class);
        assertThat(offsets.getOffsetTable()).containsEntry(1, 84L);
    }
}
