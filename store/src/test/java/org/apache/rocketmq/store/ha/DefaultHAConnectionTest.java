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
package org.apache.rocketmq.store.ha;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultHAConnectionTest {

    private SocketChannel socketChannel;
    private DefaultHAConnection connection;

    @After
    public void tearDown() throws Exception {
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
    }

    @Test
    public void testConstructorWithNullRemoteAddress() throws Exception {
        DefaultHAService haService = mock(DefaultHAService.class);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);
        when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        when(haService.getConnectionCount()).thenReturn(new AtomicInteger(0));

        // SocketChannel.open() creates a non-connected channel,
        // so getRemoteSocketAddress() returns null
        socketChannel = SocketChannel.open();
        connection = new DefaultHAConnection(haService, socketChannel);

        assertNotNull(connection);
        assertEquals("", connection.getClientAddress());
    }
}
