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
package org.apache.rocketmq.store.ha.autoswitch;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoSwitchHAServiceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testEpochCheckpointUnderSymlinkedDirectory() throws Exception {
        final Path directory = tempFolder.newFolder("store").toPath();
        final Path link = directory.resolveSibling("link");
        try {
            Files.createSymbolicLink(link, directory);
        } catch (IOException | UnsupportedOperationException e) {
            Assume.assumeNoException("Symbolic links are unavailable on this filesystem", e);
        }
        final AutoSwitchHAService service = new AutoSwitchHAService();
        service.init(mockMessageStore(link.resolve("epoch").toString(), 0));
        try {
            assertTrue(service.changeToMaster(1));
        } finally {
            service.shutdown();
        }
        final EpochFileCache reloaded = new EpochFileCache(directory.resolve("epoch").toString());
        assertTrue(reloaded.initCacheFromFile());
        assertEquals(new EpochEntry(1, 0), reloaded.lastEntry());
    }

    @Test
    public void testChangeToMasterWithSameEpochKeepsOriginalStartOffset() throws Exception {
        final String epochFilePath = epochFilePath();

        AutoSwitchHAService haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 0L));
        try {
            assertTrue(haService.changeToMaster(1));
            assertEpochEntry(haService, 1, 0L);
        } finally {
            haService.shutdown();
        }

        // Reload the persisted entry and replay its grant after the commitlog has grown. This tests
        // the store's idempotence; controller registration must grant a new epoch for a new startup.
        haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 5802L));
        try {
            assertTrue(haService.changeToMaster(1));
            assertEpochEntry(haService, 1, 0L);
        } finally {
            haService.shutdown();
        }
    }

    @Test
    public void testChangeToMasterWithNewEpochAppendsEntryAtMaxPhyOffset() throws Exception {
        final String epochFilePath = epochFilePath();

        AutoSwitchHAService haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 0L));
        try {
            assertTrue(haService.changeToMaster(1));
        } finally {
            haService.shutdown();
        }

        haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 5802L));
        try {
            assertTrue(haService.changeToMaster(2));
            final List<EpochEntry> epochEntries = haService.getEpochEntries();
            assertEquals(2, epochEntries.size());
            assertEquals(1, epochEntries.get(0).getEpoch());
            assertEquals(0L, epochEntries.get(0).getStartOffset());
            assertEquals(2, epochEntries.get(1).getEpoch());
            assertEquals(5802L, epochEntries.get(1).getStartOffset());
        } finally {
            haService.shutdown();
        }
    }

    @Test
    public void testChangeToMasterWhenLastRoleIsMasterWithSameEpochKeepsOriginalStartOffset() throws Exception {
        final String epochFilePath = epochFilePath();

        AutoSwitchHAService haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 0L));
        try {
            assertTrue(haService.changeToMaster(1));
        } finally {
            haService.shutdown();
        }

        haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 5802L));
        try {
            assertTrue(haService.changeToMasterWhenLastRoleIsMaster(1));
            assertEpochEntry(haService, 1, 0L);
        } finally {
            haService.shutdown();
        }
    }

    @Test
    public void testChangeToMasterRewritesEpochWhenStartOffsetExceedsMaxPhyOffset() throws Exception {
        final String epochFilePath = epochFilePath();

        final DefaultMessageStore messageStore = mockMessageStore(epochFilePath, 0L);
        AutoSwitchHAService haService = new AutoSwitchHAService();
        haService.init(messageStore);
        try {
            assertTrue(haService.changeToMaster(1));
            when(messageStore.getMaxPhyOffset()).thenReturn(5000L);
            assertTrue(haService.changeToMaster(2));
            final EpochEntry secondEntry = haService.getEpochEntries().get(1);
            assertEquals(2, secondEntry.getEpoch());
            assertEquals(5000L, secondEntry.getStartOffset());
        } finally {
            haService.shutdown();
        }

        // An invalid empty tail cannot be reused after commitlog recovery. The controller normally
        // supplies a newer epoch; retain coverage of the existing store-level same-epoch behavior.
        haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(epochFilePath, 4800L));
        try {
            assertTrue(haService.changeToMaster(2));
            final List<EpochEntry> epochEntries = haService.getEpochEntries();
            assertEquals(2, epochEntries.size());
            assertEquals(1, epochEntries.get(0).getEpoch());
            assertEquals(0L, epochEntries.get(0).getStartOffset());
            assertEquals(2, epochEntries.get(1).getEpoch());
            assertEquals(4800L, epochEntries.get(1).getStartOffset());
        } finally {
            haService.shutdown();
        }
    }

    @Test
    public void testBothMasterRolePathsFailWithoutPublishingAnUnpersistedEpoch() throws Exception {
        for (boolean alreadyMaster : new boolean[] {false, true}) {
            final Path path = tempFolder.getRoot().toPath().resolve("epoch-" + alreadyMaster);
            final DefaultMessageStore messageStore = mockMessageStore(path.toString(), 0);
            final AutoSwitchHAService haService = new AutoSwitchHAService();
            haService.init(messageStore);
            try {
                assertTrue(haService.changeToMaster(1));
                when(messageStore.getMaxPhyOffset()).thenReturn(100L);
                Files.delete(path);
                Files.createDirectory(path);

                final boolean changed = alreadyMaster ? haService.changeToMasterWhenLastRoleIsMaster(2)
                    : haService.changeToMaster(2);

                assertFalse(changed);
                verify(messageStore, never()).setStateMachineVersion(2);
                assertEpochEntry(haService, 1, 0);
            } finally {
                haService.shutdown();
            }
        }
    }

    @Test
    public void testInitRejectsCorruptEpochHistoryEvenWithAValidBackup() throws Exception {
        final Path path = new File(epochFilePath()).toPath();
        final EpochFileCache cache = new EpochFileCache(path.toString());
        cache.prepareMasterEpoch(1, 0);
        Files.copy(path, path.resolveSibling(path.getFileName() + ".bak"));
        Files.write(path, "broken".getBytes(StandardCharsets.UTF_8));

        try {
            new AutoSwitchHAService().init(mockMessageStore(path.toString(), 100));
            fail("Invalid epoch history must prevent HA initialization");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("epoch history"));
        }
    }

    @Test
    public void testRefusedRemoteHistorySurvivesLocalPromotionAndRestart() throws Exception {
        final Path path = tempFolder.getRoot().toPath().resolve("nested").resolve("epoch");
        final EpochFileCache history = new EpochFileCache(path.toString());
        assertEquals(1, history.prepareMasterEpoch(1, 0).getEpoch());
        AutoSwitchHAService haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(path.toString(), 3280));
        try {
            haService.getConfirmedOffsetGuard().recordRefusal("old-master", 2, 1580, 3280);
            // A refused remote history does not make this replica's own confirmed data invalid.
            assertTrue(haService.changeToMaster(3));
            assertTrue(haService.getConfirmedOffsetGuard().isBlocked());
        } finally {
            haService.shutdown();
        }

        haService = new AutoSwitchHAService();
        haService.init(mockMessageStore(path.toString(), 3280));
        try {
            assertTrue(haService.getConfirmedOffsetGuard().isBlocked());
            assertTrue(haService.changeToMasterWhenLastRoleIsMaster(4));
            assertTrue(haService.getConfirmedOffsetGuard().isBlocked());
            assertEquals(2, haService.getEpochEntries().size());
            assertEquals(1, haService.getEpochEntries().get(0).getEpoch());
            assertEquals(0, haService.getEpochEntries().get(0).getStartOffset());
            assertEquals(4, haService.getEpochEntries().get(1).getEpoch());
            assertEquals(3280, haService.getEpochEntries().get(1).getStartOffset());
        } finally {
            haService.shutdown();
        }
    }

    @Test
    public void testNonemptyLogWithoutEpochHistoryCannotInitializeHA() throws Exception {
        for (boolean emptyCheckpoint : new boolean[] {false, true}) {
            final Path path = tempFolder.getRoot().toPath().resolve("missing-history-" + emptyCheckpoint);
            if (emptyCheckpoint) {
                final EpochFileCache cache = new EpochFileCache(path.toString());
                assertTrue(cache.truncateSuffixByOffset(0));
            }
            try {
                new AutoSwitchHAService().init(mockMessageStore(path.toString(), 1000));
                fail("A nonempty log without its history must not initialize HA");
            } catch (IOException expected) {
                assertTrue(expected.getMessage().contains("no epoch history"));
            }
        }
    }

    @Test
    public void testSlaveRolePathsPublishExpectedEpochBeforeStartingTheClient() throws Exception {
        for (boolean sameMaster : new boolean[] {false, true}) {
            final DefaultMessageStore messageStore = mockMessageStore(epochFilePath(), 0);
            final AutoSwitchHAService haService = new AutoSwitchHAService();
            haService.init(messageStore);
            final AutoSwitchHAClient client = mock(AutoSwitchHAClient.class);
            final Field clientField = AutoSwitchHAService.class.getDeclaredField("haClient");
            clientField.setAccessible(true);
            clientField.set(haService, client);
            try {
                final boolean changed = sameMaster ? haService.changeToSlaveWhenMasterNotChange("new-master", 2)
                    : haService.changeToSlave("new-master", 2, 1L);
                assertTrue(changed);

                final InOrder order = inOrder(client, messageStore);
                order.verify(client).reOpen();
                order.verify(client).updateMasterAddress("new-master");
                order.verify(client).updateHaMasterAddress(null);
                order.verify(messageStore).setStateMachineVersion(2);
                order.verify(client).start();
            } finally {
                haService.shutdown();
            }
        }
    }

    private String epochFilePath() {
        return new File(tempFolder.getRoot(), "EpochFileCache").getAbsolutePath();
    }

    private DefaultMessageStore mockMessageStore(final String epochFilePath, final long maxPhyOffset) {
        final DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathEpochFile(epochFilePath);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMaxPhyOffset()).thenReturn(maxPhyOffset);
        return messageStore;
    }

    private void assertEpochEntry(final AutoSwitchHAService haService, final int epoch, final long startOffset) {
        final List<EpochEntry> epochEntries = haService.getEpochEntries();
        assertEquals(1, epochEntries.size());
        assertEquals(epoch, epochEntries.get(0).getEpoch());
        assertEquals(startOffset, epochEntries.get(0).getStartOffset());
    }
}
