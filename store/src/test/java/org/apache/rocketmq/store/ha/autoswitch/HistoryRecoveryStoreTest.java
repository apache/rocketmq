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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HistoryRecoveryStoreTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private DefaultMessageStore source;
    private DefaultMessageStore replica;
    private AutoSwitchHAClient client;

    @After
    public void shutdown() {
        if (client != null) {
            client.shutdown();
        }
        if (replica != null) {
            replica.shutdown();
        }
        if (source != null) {
            source.shutdown();
        }
    }

    @Test
    public void sameEpochRewriteAcrossStoreReload() throws Exception {
        verifyRecovery(false);
    }

    @Test
    public void formerMasterAfterUncleanElection() throws Exception {
        verifyRecovery(true);
    }

    private void verifyRecovery(boolean newEpoch) throws Exception {
        Path directory = temporaryFolder.newFolder().toPath();
        source = open(temporaryFolder.newFolder().toPath(), 1);
        replica = open(directory, 2);
        AutoSwitchHAService master = (AutoSwitchHAService) source.getHaService();
        master.setLocalBrokerId(1L);
        master.setSyncStateSet(Collections.singleton(1L));
        assertTrue(master.changeToMaster(1));
        write(source, "common");
        long cut = source.getMaxPhyOffset();
        byte[] common = bytes(source, 0);
        assertTrue(((AutoSwitchHAService) replica.getHaService()).changeToMaster(1));
        assertTrue(replica.appendToCommitLog(0, common, 0, common.length));
        write(replica, "former");
        long confirmed = replica.getMaxPhyOffset();
        replica.setConfirmOffset(confirmed);
        replica.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
        byte[] former = bytes(replica, cut);
        if (newEpoch) {
            assertTrue(master.changeToMaster(2));
            write(source, "chosen");
        }
        client = connect();
        assertFalse(client.new HAClientReader().processReadResult(handshake(newEpoch, cut)));
        assertArrayEquals(former, bytes(replica, cut));
        assertArrayEquals("former".getBytes(StandardCharsets.UTF_8), replica.lookMessageByOffset(cut).getBody());
        assertFalse(replica.getHaService().changeToMasterWhenLastRoleIsMaster(3));
        assertFalse(replica.getHaService().changeToMaster(3));
        client.shutdown();
        client = null;
        replica.shutdown();
        replica = open(directory, 2, BrokerRole.SLAVE);
        assertEquals(confirmed, replica.getConfirmOffset());
        if (!newEpoch) {
            write(source, "chosen");
        }
        write(source, "beyond");
        assertTrue(source.getMaxPhyOffset() > confirmed);
        byte[] elected = bytes(source, cut);
        assertFalse(Arrays.equals(former, Arrays.copyOf(elected, former.length)));
        client = connect();
        assertFalse(client.new HAClientReader().processReadResult(handshake(newEpoch, cut)));
        assertArrayEquals(former, bytes(replica, cut));
        client.shutdown();

        Files.copy(directory.resolve("ha-history-incident"), directory.resolve("ha-history-approval"));
        client = connect();
        ByteBuffer frames = handshake(newEpoch, cut);
        AutoSwitchHAClient.HAClientReader reader = client.new HAClientReader();
        assertTrue(reader.processReadResult(frames));
        assertEquals(HAConnectionState.TRANSFER, client.getCurrentState());
        assertEquals(cut, replica.getMaxPhyOffset());
        assertEquals(cut, replica.getConfirmOffset());
        assertFalse(Files.exists(directory.resolve("ha-history-approval")));
        // Reload before any replacement writes. Old commit-log bytes cannot
        // reappear merely because the authorized session lost its connection.
        client.shutdown();
        client = null;
        replica.shutdown();
        replica = open(directory, 2, BrokerRole.SLAVE);
        assertEquals(cut, replica.getMaxPhyOffset());
        assertTrue(replica.getHaService().changeToSlave("127.0.0.1", newEpoch ? 2 : 1, 2L));
        replica.getHaService().updateHaMasterAddress("127.0.0.1:" + source.getMessageStoreConfig().getHaListenPort());
        await().atMost(30, TimeUnit.SECONDS).until(() -> replica.getConfirmOffset() == source.getMaxPhyOffset());
        assertArrayEquals(bytes(source, 0), bytes(replica, 0));
        assertArrayEquals("chosen".getBytes(StandardCharsets.UTF_8), replica.lookMessageByOffset(cut).getBody());
    }

    private AutoSwitchHAClient connect() throws Exception {
        EpochFileCache epochs = new EpochFileCache(replica.getMessageStoreConfig().getStorePathEpochFile());
        assertTrue(epochs.initCacheFromFile());
        AutoSwitchHAClient result = new AutoSwitchHAClient((AutoSwitchHAService) replica.getHaService(), replica, epochs, 2L);
        result.updateHaMasterAddress("127.0.0.1:" + source.getMessageStoreConfig().getHaListenPort());
        assertTrue(result.connectMaster());
        return result;
    }

    private ByteBuffer handshake(boolean newEpoch, long cut) {
        ByteBuffer frame = ByteBuffer.allocate(4096);
        frame.putInt(HAConnectionState.HANDSHAKE.ordinal());
        frame.putInt(AutoSwitchHAConnection.EPOCH_ENTRY_SIZE * (newEpoch ? 2 : 1));
        frame.putLong(source.getMaxPhyOffset()).putInt(newEpoch ? 2 : 1);
        frame.putInt(1).putLong(0);
        if (newEpoch) {
            frame.putInt(2).putLong(cut);
        }
        return frame;
    }

    private DefaultMessageStore open(Path directory, int id) throws Exception {
        return open(directory, id, BrokerRole.SYNC_MASTER);
    }

    private DefaultMessageStore open(Path directory, int id, BrokerRole role) throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setBrokerRole(role);
        config.setStorePathRootDir(directory.toString());
        config.setHaListenPort(0);
        config.setHaSendHeartbeatInterval(100);
        config.setMappedFileSizeCommitLog(64 * 1024);
        config.setMappedFileSizeConsumeQueue(1024 * 20);
        config.setMaxHashSlotNum(100);
        config.setMaxIndexNum(1000);
        BrokerConfig broker = new BrokerConfig();
        broker.setBrokerId(id);
        broker.setEnableControllerMode(true);
        DefaultMessageStore store = new DefaultMessageStore(config, new BrokerStatsManager("test", true),
            null, broker, new ConcurrentHashMap<>());
        assertTrue(store.load());
        store.start();
        return store;
    }

    private void write(DefaultMessageStore store, String body) {
        MessageExtBrokerInner message = new MessageExtBrokerInner();
        message.setTopic("History");
        message.setQueueId(0);
        message.setBody(body.getBytes(StandardCharsets.UTF_8));
        message.setBornHost(new InetSocketAddress("127.0.0.1", 1000));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 1001));
        message.setWaitStoreMsgOK(false);
        assertTrue(store.putMessage(message).isOk());
    }

    private byte[] bytes(DefaultMessageStore store, long start) {
        SelectMappedBufferResult result = store.getCommitLogData(start);
        try {
            byte[] data = new byte[result.getSize()];
            result.getByteBuffer().get(data);
            return data;
        } finally {
            result.release();
        }
    }
}
