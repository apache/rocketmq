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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoSwitchHAClientTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testHandshakeMustRetainRefusalAfterClientRestart() throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(temporaryFolder.newFolder().getAbsolutePath());
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        when(store.getMessageStoreConfig()).thenReturn(config);
        when(store.getMaxPhyOffset()).thenReturn(3280L);
        when(store.getConfirmOffset()).thenReturn(3280L);
        when(store.truncateFiles(3280L)).thenReturn(true);

        AutoSwitchHAClient first = createClient(store);
        try {
            first.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(first.new HAClientReader().processReadResult(handshake(1580L)));
            assertEquals(HAConnectionState.HANDSHAKE, first.getCurrentState());
            verify(store, never()).truncateFiles(anyLong());
        } finally {
            first.shutdown();
        }

        // Recreate the client against the same storage directory. A higher end offset
        // in the same epoch cannot prove that the previously rejected history is safe.
        AutoSwitchHAClient restarted = createClient(store);
        try {
            restarted.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(restarted.new HAClientReader().processReadResult(handshake(4190L)));
            assertEquals(HAConnectionState.HANDSHAKE, restarted.getCurrentState());
            verify(store, never()).truncateFiles(anyLong());
        } finally {
            restarted.shutdown();
        }
    }

    private ByteBuffer handshake(long masterEndOffset) {
        ByteBuffer frame = ByteBuffer.allocate(AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE
            + AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
        frame.putInt(HAConnectionState.HANDSHAKE.ordinal());
        frame.putInt(AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
        frame.putLong(masterEndOffset);
        frame.putInt(1);
        frame.putInt(1);
        frame.putLong(0L);
        return frame;
    }

    @Test
    public void testSmallerObservationInvalidatesApprovalEvenIfDiskBecomesUnavailable() throws Exception {
        Path root = temporaryFolder.newFolder().toPath();
        Path offline = root.resolveSibling("unmounted");
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(root.toString());
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        when(store.getMessageStoreConfig()).thenReturn(config);
        when(store.getMaxPhyOffset()).thenReturn(200L);
        when(store.getConfirmOffset()).thenReturn(200L);
        AutoSwitchHAClient first = createClient(store);
        try {
            first.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(first.new HAClientReader().processReadResult(handshake(100)));
        } finally {
            first.shutdown();
        }
        Files.copy(root.resolve("ha-history-incident"), root.resolve("ha-history-approval"));
        AutoSwitchHAClient second = createClient(store);
        try {
            second.changeCurrentState(HAConnectionState.HANDSHAKE);
            // The selected source has regressed further. Lose the filesystem
            // between computing the new point and publishing the restriction.
            when(store.getConfirmOffset()).thenAnswer(invocation -> {
                Files.move(root, offline);
                return 200L;
            });
            assertFalse(second.new HAClientReader().processReadResult(handshake(60)));
            assertTrue(Files.isDirectory(offline));
        } finally {
            second.shutdown();
            Files.move(offline, root);
        }
        HistoryIncident restarted = new HistoryIncident(root);
        assertNull(restarted.authorize(200, 200, 200, "null 1@0", false));
    }

    @Test
    public void testCrashBeforeFirstReplyAllowsPositiveRetainedMinimum() throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(temporaryFolder.newFolder().getAbsolutePath());
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        when(store.getMessageStoreConfig()).thenReturn(config);
        when(store.getMinPhyOffset()).thenReturn(1024L);
        when(store.getMaxPhyOffset()).thenReturn(2048L);
        when(store.getConfirmOffset()).thenReturn(2048L);
        when(store.truncateFiles(2048L)).thenReturn(true);
        AutoSwitchHAClient waiting = createClient(store);
        AutoSwitchHAClient restarted = createClient(store);
        try {
            Method prepare = AutoSwitchHAClient.class.getDeclaredMethod("prepareHistoryCheck");
            prepare.setAccessible(true);
            prepare.invoke(waiting);
            // A fresh process sees only durable state. Never close the first
            // connection gracefully before the recreated client has checked.
            restarted.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertTrue(restarted.new HAClientReader().processReadResult(handshake(2048)));
            assertEquals(HAConnectionState.TRANSFER, restarted.getCurrentState());
        } finally {
            restarted.shutdown();
            waiting.shutdown();
        }
    }

    @Test
    public void testInterruptedReceiptBlocksHealthyHandshakeAndRestart() throws Exception {
        Path root = temporaryFolder.newFolder().toPath();
        Path received = root.resolve("ha-history-received");
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(root.toString());
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        when(store.getMessageStoreConfig()).thenReturn(config);
        when(store.getMaxPhyOffset()).thenReturn(2048L);
        when(store.getConfirmOffset()).thenThrow(new IllegalStateException("Interrupted after reserving receipt"))
            .thenReturn(2048L);
        when(store.truncateFiles(2048L)).thenReturn(true);

        for (int attempt = 0; attempt < 2; attempt++) {
            AutoSwitchHAClient client = createClient(store);
            try {
                client.changeCurrentState(HAConnectionState.HANDSHAKE);
                assertFalse(client.new HAClientReader().processReadResult(handshake(2048L)));
                assertEquals(HAConnectionState.HANDSHAKE, client.getCurrentState());
                assertThatThrownBy(client::reservePromotion).isInstanceOf(IOException.class)
                    .hasMessageContaining(received.toString());
                assertEquals(0, Files.size(received));
            } finally {
                client.shutdown();
            }
        }
        verify(store).getConfirmOffset();
        verify(store, never()).truncateFiles(anyLong());
    }

    @Test
    public void testCorruptIsolationStateRefusesHandshake() throws Exception {
        File directory = temporaryFolder.newFolder();
        Files.write(new File(directory, "ha-history-incident").toPath(), new byte[] {1, 2});
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(directory.getAbsolutePath());
        when(store.getMessageStoreConfig()).thenReturn(config);
        when(store.getMaxPhyOffset()).thenReturn(100L);
        when(store.truncateFiles(100L)).thenReturn(true);
        AutoSwitchHAClient client = createClient(store);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(client.new HAClientReader().processReadResult(handshake(100L)));
            assertEquals(HAConnectionState.HANDSHAKE, client.getCurrentState());
            verify(store, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testDoTruncateRejectsOffsetBelowConfirmOffset() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(temporaryFolder.newFolder().getAbsolutePath());
        when(messageStore.getMessageStoreConfig()).thenReturn(config);
        when(messageStore.getMaxPhyOffset()).thenReturn(200L, 100L);
        when(messageStore.getConfirmOffset()).thenReturn(150L);
        when(messageStore.truncateFiles(100L)).thenReturn(true);

        AutoSwitchHAClient client = createClient(messageStore);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(client.new HAClientReader().processReadResult(handshake(100L)));
            assertEquals(HAConnectionState.HANDSHAKE, client.getCurrentState());
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testDoTruncateAllowsOffsetAtConfirmOffset() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(temporaryFolder.newFolder().getAbsolutePath());
        when(messageStore.getMessageStoreConfig()).thenReturn(config);
        when(messageStore.getMaxPhyOffset()).thenReturn(200L, 100L);
        when(messageStore.getConfirmOffset()).thenReturn(100L);
        when(messageStore.truncateFiles(100L)).thenReturn(true);

        AutoSwitchHAClient client = createClient(messageStore);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertTrue(client.new HAClientReader().processReadResult(handshake(100L)));
            assertEquals(HAConnectionState.TRANSFER, client.getCurrentState());
            verify(messageStore).truncateFiles(100L);
        } finally {
            client.shutdown();
        }
    }

    private AutoSwitchHAClient createClient(DefaultMessageStore messageStore) throws Exception {
        EpochFileCache epochCache = new EpochFileCache();
        assertTrue(epochCache.appendEntry(new EpochEntry(1, 0L)));
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        return new AutoSwitchHAClient(haService, messageStore, epochCache, 1L);
    }

}
