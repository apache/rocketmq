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
import java.util.List;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoSwitchHAServiceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

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

        // Simulate the broker restart: the epoch file is recovered from disk and the broker is elected
        // as the master again with the same epoch, while the commitlog has grown to 5802.
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

        // Restart after an unclean shutdown: the commitlog recovers to 4800, before the recorded epoch start,
        // so the recorded entry must be rewritten instead of being reused.
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

    private String epochFilePath() {
        return new File(tempFolder.getRoot(), "EpochFileCache").getAbsolutePath();
    }

    private DefaultMessageStore mockMessageStore(final String epochFilePath, final long maxPhyOffset) {
        final DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(tempFolder.getRoot().getAbsolutePath());
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
