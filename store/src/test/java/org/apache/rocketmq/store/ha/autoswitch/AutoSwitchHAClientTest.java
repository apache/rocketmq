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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoSwitchHAClientTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testDoTruncateRefusesBelowConfirmOffset() throws Exception {
        final ClientFixture fixture = createFixture(3280L, 3280L, false);
        try {
            assertFalse(invokeDoTruncate(fixture.client,
                Collections.singletonList(new EpochEntry(1, 0)), 1580L));
            verify(fixture.messageStore, never()).truncateFiles(anyLong());
            verify(fixture.messageStore).getConfirmOffsetDirectly();
        } finally {
            shutdown(fixture.client);
        }
    }

    @Test
    public void testDoTruncateAllowsTruncateAtConfirmOffset() throws Exception {
        final ClientFixture fixture = createFixture(3280L, 1580L, false);
        try {
            invokeDoTruncate(fixture.client, Collections.singletonList(new EpochEntry(1, 0)), 1580L);
            verify(fixture.messageStore).truncateFiles(1580L);
        } finally {
            shutdown(fixture.client);
        }
    }

    @Test
    public void testDoTruncateAllowsBelowConfirmOffsetWhenEnabled() throws Exception {
        final ClientFixture fixture = createFixture(3280L, 3280L, true);
        try {
            invokeDoTruncate(fixture.client, Collections.singletonList(new EpochEntry(1, 0)), 1580L);
            verify(fixture.messageStore).truncateFiles(1580L);
        } finally {
            shutdown(fixture.client);
        }
    }

    private ClientFixture createFixture(final long maxPhyOffset, final long confirmOffset,
        final boolean enableTruncateBelowConfirmOffset) throws Exception {
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setEnableTruncateBelowConfirmOffset(enableTruncateBelowConfirmOffset);

        final DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStore.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(messageStore.getMaxPhyOffset()).thenReturn(maxPhyOffset);
        when(messageStore.getConfirmOffsetDirectly()).thenReturn(confirmOffset);
        when(messageStore.truncateFiles(anyLong())).thenReturn(true);

        final AutoSwitchHAService haService = mock(AutoSwitchHAService.class);
        when(haService.getDefaultMessageStore()).thenReturn(messageStore);

        final String epochFilePath = new File(tempFolder.getRoot(),
            "EpochFileCache-" + System.nanoTime()).getAbsolutePath();
        final EpochFileCache epochCache = new EpochFileCache(epochFilePath);
        assertTrue(epochCache.appendEntry(new EpochEntry(1, 0)));

        final AutoSwitchHAClient client = new AutoSwitchHAClient(haService, messageStore, epochCache, 1L);
        final Field socketChannelField = AutoSwitchHAClient.class.getDeclaredField("socketChannel");
        socketChannelField.setAccessible(true);
        socketChannelField.set(client, mock(SocketChannel.class));

        final ClientFixture fixture = new ClientFixture();
        fixture.client = client;
        fixture.messageStore = messageStore;
        return fixture;
    }

    private void shutdown(final AutoSwitchHAClient client) throws Exception {
        final Field socketChannelField = AutoSwitchHAClient.class.getDeclaredField("socketChannel");
        socketChannelField.setAccessible(true);
        socketChannelField.set(client, null);
        client.shutdown();
    }

    private boolean invokeDoTruncate(final AutoSwitchHAClient client, final List<EpochEntry> masterEpochEntries,
        final long masterEndOffset) throws Exception {
        final Method doTruncate = AutoSwitchHAClient.class.getDeclaredMethod("doTruncate", List.class, long.class);
        doTruncate.setAccessible(true);
        return (boolean) doTruncate.invoke(client, masterEpochEntries, masterEndOffset);
    }

    private static class ClientFixture {
        private AutoSwitchHAClient client;
        private DefaultMessageStore messageStore;
    }
}