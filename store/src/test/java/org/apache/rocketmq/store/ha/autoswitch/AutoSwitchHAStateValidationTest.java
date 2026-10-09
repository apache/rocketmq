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

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoSwitchHAStateValidationTest {

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
