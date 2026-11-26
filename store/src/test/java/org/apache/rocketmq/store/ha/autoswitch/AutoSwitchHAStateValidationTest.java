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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoSwitchHAStateValidationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testServerReaderRejectsUnknownSlaveStateOrdinal() throws Exception {
        assertFalse(processSlaveState(Integer.MAX_VALUE));
    }

    @Test
    public void testServerReaderAcceptsKnownSlaveStateOrdinal() throws Exception {
        assertTrue(processSlaveState(HAConnectionState.HANDSHAKE.ordinal()));
    }

    private boolean processSlaveState(int stateOrdinal) throws Exception {
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getConnectionCount()).thenReturn(new AtomicInteger());
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());

        try (ServerSocketChannel serverSocket = ServerSocketChannel.open();
             SocketChannel slaveSocket = SocketChannel.open()) {
            serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            slaveSocket.connect(serverSocket.getLocalAddress());

            try (SocketChannel masterSocket = serverSocket.accept()) {
                AutoSwitchHAConnection connection = new AutoSwitchHAConnection(
                    haService, masterSocket, mock(EpochFileCache.class));
                try {
                    AutoSwitchHAConnection.ReadSocketService readSocketService = getReadSocketService(connection);
                    AutoSwitchHAConnection.ReadSocketService.HAServerReader reader = readSocketService.new HAServerReader();
                    ByteBuffer frame = ByteBuffer.allocate(AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE);
                    frame.putInt(stateOrdinal);
                    frame.putShort((short) 0);
                    frame.putShort((short) 0);
                    frame.putLong(1L);

                    return reader.processReadResult(frame);
                } finally {
                    connection.shutdown();
                }
            }
        }
    }

    @Test
    public void testClientReaderRejectsUnknownMasterStateOrdinal() throws Exception {
        assertFalse(processMasterState(Integer.MAX_VALUE, AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE));
    }

    @Test
    public void testClientReaderHandlesKnownMasterStateOrdinals() throws Exception {
        assertFalse(processMasterState(
            HAConnectionState.HANDSHAKE.ordinal(), AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE));
        assertFalse(processMasterState(
            HAConnectionState.TRANSFER.ordinal(), AutoSwitchHAConnection.TRANSFER_HEADER_SIZE));
    }

    @Test
    public void testTruncateSlaveLogProtectsConfirmOffset() throws Exception {
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        when(haService.getConfirmedOffsetGuard()).thenReturn(new ConfirmedOffsetGuard(
            temporaryFolder.getRoot().toPath().resolve("confirmed-offset-guard")));
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getConfirmOffset()).thenReturn(1024L);

        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, new EpochFileCache(), 1L);
        try {
            assertFalse(client.truncateSlaveLog(1023L));
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testClientHandshakeAtConfirmOffset() throws Exception {
        long confirmOffset = 1024L;
        int epoch = 1;
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        when(haService.getConfirmedOffsetGuard()).thenReturn(new ConfirmedOffsetGuard(
            temporaryFolder.getRoot().toPath().resolve("confirmed-offset-guard")));
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getConfirmOffset()).thenReturn(confirmOffset);
        when(messageStore.getMaxPhyOffset()).thenReturn(confirmOffset);
        when(messageStore.truncateFiles(confirmOffset)).thenReturn(true);
        EpochFileCache epochCache = new EpochFileCache();
        epochCache.initCacheFromEntries(Collections.singletonList(new EpochEntry(epoch, 0L)));

        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, epochCache, 1L);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            ByteBuffer frame = ByteBuffer.allocate(AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE
                + AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
            frame.putInt(HAConnectionState.HANDSHAKE.ordinal());
            frame.putInt(AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
            frame.putLong(confirmOffset);
            frame.putInt(epoch);
            frame.putInt(epoch);
            frame.putLong(0L);

            assertTrue(client.new HAClientReader().processReadResult(frame));
            assertEquals(HAConnectionState.TRANSFER, client.getCurrentState());
            verify(messageStore).truncateFiles(confirmOffset);
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testHandshakeRefusalSurvivesClientReplacementAndMasterGrowth() throws Exception {
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getConfirmOffset()).thenReturn(1024L);
        when(messageStore.getMaxPhyOffset()).thenReturn(1024L);
        java.nio.file.Path guardFile = temporaryFolder.getRoot().toPath().resolve("confirmed-offset-guard");
        when(haService.getConfirmedOffsetGuard()).thenReturn(new ConfirmedOffsetGuard(guardFile));
        EpochFileCache epochCache = new EpochFileCache();
        epochCache.initCacheFromEntries(Collections.singletonList(new EpochEntry(1, 0L)));

        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, epochCache, 1L);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(client.new HAClientReader().processReadResult(handshake(1, 512L)));
            assertEquals(HAConnectionState.HANDSHAKE, client.getCurrentState());
            assertEquals(0L, reportedOffset(client));
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }

        when(haService.getConfirmedOffsetGuard()).thenReturn(new ConfirmedOffsetGuard(guardFile));
        client = new AutoSwitchHAClient(haService, messageStore, epochCache, 1L);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(client.new HAClientReader().processReadResult(handshake(1, 2048L)));
            assertEquals(HAConnectionState.HANDSHAKE, client.getCurrentState());
            assertEquals(0L, reportedOffset(client));
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testEmptyEpochCacheDoesNotBypassPersistedRefusal() throws Exception {
        ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(
            temporaryFolder.getRoot().toPath().resolve("confirmed-offset-guard"));
        guard.recordRefusal("127.0.0.1:10001", 1, 512, 1024);
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(haService.getConfirmedOffsetGuard()).thenReturn(guard);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, new EpochFileCache(), 1L);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertFalse(client.new HAClientReader().processReadResult(handshake(2, 2048)));
            assertEquals(HAConnectionState.HANDSHAKE, client.getCurrentState());
            assertEquals(0L, reportedOffset(client));
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    private ByteBuffer handshake(int epoch, long maxOffset) {
        ByteBuffer frame = ByteBuffer.allocate(AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE
            + AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
        frame.putInt(HAConnectionState.HANDSHAKE.ordinal());
        frame.putInt(AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
        frame.putLong(maxOffset);
        frame.putInt(epoch);
        frame.putInt(epoch);
        frame.putLong(0L);
        return frame;
    }

    @Test
    public void testEmptyReplicaCanJoinAfterAnEmptyMasterEpochWasReplaced() throws Exception {
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(haService.getConfirmedOffsetGuard()).thenReturn(new ConfirmedOffsetGuard(
            temporaryFolder.getRoot().toPath().resolve("confirmed-offset-guard")));
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        String epochPath = temporaryFolder.getRoot().toPath().resolve("epoch").toString();
        EpochFileCache cache = new EpochFileCache(epochPath);
        assertTrue(cache.appendEntry(new EpochEntry(1, 0)));
        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, cache, 1L);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            assertTrue(client.new HAClientReader().processReadResult(handshake(2, 1024)));
            assertEquals(HAConnectionState.TRANSFER, client.getCurrentState());
            EpochFileCache recovered = new EpochFileCache(epochPath);
            assertTrue(recovered.initCacheFromFile());
            assertEquals(0, recovered.getEntrySize());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testEpochPersistedAheadOfDataCannotAdvanceAcknowledgedOffset() throws Exception {
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(haService.getConfirmedOffsetGuard()).thenReturn(new ConfirmedOffsetGuard(
            temporaryFolder.getRoot().toPath().resolve("guard")));
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMaxPhyOffset()).thenReturn(1000L);
        when(messageStore.getConfirmOffset()).thenReturn(1000L);
        when(messageStore.truncateFiles(1000L)).thenReturn(true);
        String epochPath = temporaryFolder.getRoot().toPath().resolve("epoch").toString();
        EpochFileCache cache = new EpochFileCache(epochPath);
        cache.initCacheFromEntries(Arrays.asList(new EpochEntry(1, 0), new EpochEntry(2, 1580)));
        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, cache, 1L);
        try {
            client.changeCurrentState(HAConnectionState.HANDSHAKE);
            ByteBuffer frame = ByteBuffer.allocate(AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE
                + 2 * AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
            frame.putInt(HAConnectionState.HANDSHAKE.ordinal());
            frame.putInt(2 * AutoSwitchHAConnection.EPOCH_ENTRY_SIZE);
            frame.putLong(4190L);
            frame.putInt(2);
            frame.putInt(1).putLong(0);
            frame.putInt(2).putLong(1580);

            assertTrue(client.new HAClientReader().processReadResult(frame));
            assertEquals(HAConnectionState.TRANSFER, client.getCurrentState());
            assertEquals(1000L, reportedOffset(client));
            verify(messageStore).truncateFiles(1000L);
            verify(messageStore, never()).truncateFiles(4190L);
            EpochFileCache restored = new EpochFileCache(epochPath);
            assertTrue(restored.initCacheFromFile());
            assertEquals(1, restored.getEntrySize());
            assertEquals(1, restored.lastEpoch());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testBlockedClientRetriesPersistingTheFirstRefusal() throws Exception {
        Path parent = temporaryFolder.getRoot().toPath().resolve("guard-parent");
        Path guardPath = parent.resolve("guard");
        ConfirmedOffsetGuard guard = new ConfirmedOffsetGuard(guardPath);
        Files.createFile(parent);
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(haService.getConfirmedOffsetGuard()).thenReturn(guard);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getConfirmOffset()).thenReturn(1024L);
        AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, new EpochFileCache(), 1L);
        try {
            try {
                client.truncateSlaveLog(512L);
                fail("The first refusal cannot be persisted below a regular file");
            } catch (IOException expected) {
                assertTrue(guard.isBlocked());
            }
            Files.delete(parent);
            Files.createDirectory(parent);

            assertFalse(client.truncateSlaveLog(2048L));
            ConfirmedOffsetGuard restored = new ConfirmedOffsetGuard(guardPath);
            assertTrue(restored.isBlocked());
            assertTrue(restored.describe().contains("observedOffset=512"));
            assertTrue(restored.describe().contains("confirmOffset=1024"));
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    private long reportedOffset(AutoSwitchHAClient client) throws Exception {
        Field field = AutoSwitchHAClient.class.getDeclaredField("currentReportedOffset");
        field.setAccessible(true);
        return field.getLong(client);
    }

    private boolean processMasterState(int stateOrdinal, int headerSize) throws Exception {
        AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        AutoSwitchHAClient client = new AutoSwitchHAClient(
            haService, messageStore, mock(EpochFileCache.class), 1L);
        try {
            ByteBuffer frame = ByteBuffer.allocate(headerSize);
            frame.putInt(stateOrdinal);
            frame.putInt(0);
            frame.putLong(0);
            frame.putInt(0);
            if (headerSize == AutoSwitchHAConnection.TRANSFER_HEADER_SIZE) {
                frame.putLong(0);
                frame.putLong(0);
            }

            return client.new HAClientReader().processReadResult(frame);
        } finally {
            client.shutdown();
        }
    }

    private AutoSwitchHAConnection.ReadSocketService getReadSocketService(
        AutoSwitchHAConnection connection) throws ReflectiveOperationException {
        Field field = AutoSwitchHAConnection.class.getDeclaredField("readSocketService");
        field.setAccessible(true);
        return (AutoSwitchHAConnection.ReadSocketService) field.get(connection);
    }
}
