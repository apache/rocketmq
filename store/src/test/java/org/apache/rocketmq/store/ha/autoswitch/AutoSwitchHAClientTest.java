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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoSwitchHAClientTest {

    @Test
    public void testDoTruncateRejectsOffsetBelowConfirmOffset() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMaxPhyOffset()).thenReturn(200L, 100L);
        when(messageStore.getConfirmOffset()).thenReturn(150L);
        when(messageStore.truncateFiles(100L)).thenReturn(true);

        AutoSwitchHAClient client = createClient(messageStore);
        try {
            assertFalse(invokeDoTruncate(client, 100L));
            assertEquals(HAConnectionState.READY, client.getCurrentState());
            verify(messageStore, never()).truncateFiles(anyLong());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testDoTruncateAllowsOffsetAtConfirmOffset() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMaxPhyOffset()).thenReturn(200L, 100L);
        when(messageStore.getConfirmOffset()).thenReturn(100L);
        when(messageStore.truncateFiles(100L)).thenReturn(true);

        AutoSwitchHAClient client = createClient(messageStore);
        try {
            assertTrue(invokeDoTruncate(client, 100L));
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

    private boolean invokeDoTruncate(AutoSwitchHAClient client, long masterEndOffset) throws Exception {
        Method method = AutoSwitchHAClient.class.getDeclaredMethod("doTruncate", List.class, long.class);
        method.setAccessible(true);
        return (boolean) method.invoke(client, Collections.singletonList(new EpochEntry(1, 0L)), masterEndOffset);
    }
}
