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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Store-level scenarios using real commitlogs and HA sockets. Epoch assignments are explicit inputs;
 * these tests do not run a controller or simulate a machine crash.
 */
public class AutoSwitchHAHistoryRecoveryTest {
    private static final String TOPIC = "HistoryRecovery";
    private static final int TIMEOUT_SECONDS = 30;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final List<TestStore> stores = new ArrayList<>();
    private BrokerStatsManager brokerStatsManager;

    @Before
    public void setUp() {
        this.brokerStatsManager = new BrokerStatsManager("HistoryRecovery", true);
    }

    @After
    public void tearDown() {
        try {
            for (int i = this.stores.size() - 1; i >= 0; i--) {
                final TestStore store = this.stores.get(i);
                store.stop();
                store.messageStore.destroy();
            }
        } finally {
            this.brokerStatsManager.shutdown();
        }
    }

    @Test(timeout = 180000)
    public void testRefusalSurvivesRegrowthReconnectAndReplicaRestart() throws Exception {
        final Path masterPath = newStorePath("master");
        final Path replicaPath = newStorePath("replica");
        final TestStore master = startStore(masterPath, 1);
        TestStore replica = startStore(replicaPath, 2);
        master.becomeMaster(1);
        replica.follow(master, 1);

        final List<String> original = appendMessages(master, "shared", 10);
        final long recoveredEnd = master.messageStore.getMaxPhyOffset();
        original.addAll(appendMessages(master, "original", 10));
        awaitReplication(master, replica, original);
        final long protectedEnd = replica.messageStore.getMaxPhyOffset();
        replica.stop();

        // Isolate the store and use its truncation API to provide the shorter source history.
        // Keeping epoch 1 deliberately reproduces the unsafe source behavior under review.
        truncateIsolatedMaster(master, recoveredEnd);
        replica = startStore(replicaPath, 2);
        replica.follow(master, 1);
        awaitBlocked(replica);
        assertPreserved(replica, protectedEnd, original);

        appendMessages(master, "replacement", 30);
        assertTrue(master.messageStore.getMaxPhyOffset() > protectedEnd);
        replica.follow(master, 1);
        assertRemainsRejected(replica, protectedEnd, original);

        replica.stop();
        replica = startStore(replicaPath, 2);
        assertTrue(replica.haService().getConfirmedOffsetGuard().isBlocked());
        replica.follow(master, 1);
        assertRemainsRejected(replica, protectedEnd, original);
    }

    @Test(timeout = 180000)
    public void testFirstHandshakeAfterRegrowthUsesNewEpochBoundary() throws Exception {
        final Path masterPath = newStorePath("master");
        final Path replicaPath = newStorePath("replica");
        final TestStore master = startStore(masterPath, 1);
        TestStore replica = startStore(replicaPath, 2);
        master.becomeMaster(1);
        replica.follow(master, 1);

        final List<String> original = appendMessages(master, "shared", 10);
        final long recoveredEnd = master.messageStore.getMaxPhyOffset();
        original.addAll(appendMessages(master, "original", 10));
        awaitReplication(master, replica, original);
        final long protectedEnd = replica.messageStore.getMaxPhyOffset();
        replica.stop();

        truncateIsolatedMaster(master, recoveredEnd);
        // Supply the new epoch before any replacement write, not after regrowth.
        master.becomeMaster(2);
        assertEquals(recoveredEnd, master.haService().getEpochEntries().get(1).getStartOffset());
        appendMessages(master, "replacement", 30);
        assertTrue(master.messageStore.getMaxPhyOffset() > protectedEnd);

        replica = startStore(replicaPath, 2);
        assertFalse(replica.haService().getConfirmedOffsetGuard().isBlocked());
        replica.follow(master, 2);
        awaitBlocked(replica);
        assertRemainsRejected(replica, protectedEnd, original);
    }

    @Test(timeout = 180000)
    public void testUncleanSelectionRequiresExplicitRebuildAndPreservesOldStore() throws Exception {
        final Path oldMasterPath = newStorePath("old-master");
        final Path selectedMasterPath = newStorePath("selected-master");
        TestStore oldMaster = startStore(oldMasterPath, 1);
        TestStore selectedMaster = startStore(selectedMasterPath, 2);
        oldMaster.becomeMaster(1);
        selectedMaster.follow(oldMaster, 1);

        final List<String> common = appendMessages(oldMaster, "shared", 10);
        awaitReplication(oldMaster, selectedMaster, common);
        final long commonEnd = selectedMaster.messageStore.getMaxPhyOffset();
        selectedMaster.stop();

        final List<String> abandonedHistory = new ArrayList<>(common);
        abandonedHistory.addAll(appendMessages(oldMaster, "old-only", 10));
        oldMaster.haService().setSyncStateSet(Collections.singleton(1L));
        final long protectedEnd = oldMaster.messageStore.getConfirmOffset();
        assertTrue(protectedEnd > commonEnd);
        awaitContents(oldMaster, abandonedHistory);
        oldMaster.stop();

        selectedMaster = startStore(selectedMasterPath, 2);
        selectedMaster.becomeMaster(2);
        final List<String> selectedHistory = new ArrayList<>(common);
        selectedHistory.addAll(appendMessages(selectedMaster, "selected", 20));

        oldMaster = startStore(oldMasterPath, 1);
        assertEquals(protectedEnd, oldMaster.messageStore.getConfirmOffset());
        oldMaster.follow(selectedMaster, 2);
        awaitBlocked(oldMaster);
        assertPreserved(oldMaster, protectedEnd, abandonedHistory);
        oldMaster.stop();
        final Map<Path, byte[]> preservedCommitlog = readCommitlogFiles(oldMasterPath);

        // The data frames must carry every epoch themselves. A heartbeat carrying epoch 1 before
        // the first data frame could hide a sender that labels the old tail with epoch 2.
        selectedMaster.config.setHaSendHeartbeatInterval(60000);

        // The operator explicitly chooses the new master and a fresh store directory. The old
        // store and its guard are retained; no offset is guessed from the refused handshake.
        TestStore rebuilt = startStore(newStorePath("rebuilt-old-master"), 3);
        assertFalse(rebuilt.config.isSyncFromLastFile());
        assertEquals(0, rebuilt.messageStore.getMaxPhyOffset());
        rebuilt.follow(selectedMaster, 2);
        awaitReplication(selectedMaster, rebuilt, selectedHistory);
        assertEquals(selectedMaster.haService().getEpochEntries(), rebuilt.haService().getEpochEntries());
        assertEquals(0, rebuilt.haService().getEpochEntries().get(0).getStartOffset());
        assertEquals(commonEnd, rebuilt.haService().getEpochEntries().get(0).getEndOffset());

        selectedHistory.addAll(appendMessages(selectedMaster, "after-rebuild", 5));
        awaitReplication(selectedMaster, rebuilt, selectedHistory);
        assertFalse(rebuilt.haService().getConfirmedOffsetGuard().isBlocked());
        assertCommitlogUnchanged(preservedCommitlog, oldMasterPath);

        final Path rebuiltPath = rebuilt.root;
        rebuilt.stop();
        rebuilt = startStore(rebuiltPath, 3);
        rebuilt.follow(selectedMaster, 2);
        awaitReplication(selectedMaster, rebuilt, selectedHistory);
        assertEquals(selectedMaster.haService().getEpochEntries(), rebuilt.haService().getEpochEntries());

        // A fully rebuilt replica must retain enough epoch history to become a source itself.
        // Stop the previous source, then supply the next controller grant explicitly to this fixture.
        selectedMaster.stop();
        rebuilt.becomeMaster(3);
        rebuilt.config.setHaSendHeartbeatInterval(60000);
        selectedHistory.addAll(appendMessages(rebuilt, "after-promotion", 5));
        final TestStore nextReplica = startStore(newStorePath("replica-from-rebuilt"), 4);
        nextReplica.follow(rebuilt, 3);
        awaitReplication(rebuilt, nextReplica, selectedHistory);
        assertEquals(rebuilt.haService().getEpochEntries(), nextReplica.haService().getEpochEntries());

        selectedHistory.addAll(appendMessages(rebuilt, "after-second-copy", 5));
        awaitReplication(rebuilt, nextReplica, selectedHistory);
        assertCommitlogUnchanged(preservedCommitlog, oldMasterPath);
    }

    private Path newStorePath(String name) throws Exception {
        return this.tempFolder.newFolder(name).toPath();
    }

    private TestStore startStore(Path root, long brokerId) throws Exception {
        final TestStore store = new TestStore(root, brokerId);
        this.stores.add(store);
        store.messageStore.getRunningFlags().makeFenced(true);
        assertTrue("Store load failed: " + root, store.messageStore.load());
        store.messageStore.start();
        store.haService().setLocalBrokerId(brokerId);
        return store;
    }

    private void truncateIsolatedMaster(TestStore master, long recoveredEnd) throws Exception {
        master.haService().destroyConnections();
        master.haService().setSyncStateSet(Collections.singleton(master.brokerId));
        await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .until(() -> master.messageStore.dispatchBehindBytes() == 0);
        master.messageStore.getRunningFlags().makeFenced(true);
        assertTrue(master.messageStore.truncateFiles(recoveredEnd));
        assertEquals(recoveredEnd, master.messageStore.getMaxPhyOffset());
        assertEquals(recoveredEnd, master.messageStore.getConfirmOffset());
        master.messageStore.getRunningFlags().makeFenced(false);
    }

    private List<String> appendMessages(TestStore store, String prefix, int count) {
        final List<String> payloads = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final String payload = String.format(Locale.ROOT, "%s-%03d", prefix, i);
            final MessageExtBrokerInner message = new MessageExtBrokerInner();
            message.setTopic(TOPIC);
            message.setTags("history");
            message.setQueueId(0);
            message.setBody(payload.getBytes(StandardCharsets.UTF_8));
            message.setBornTimestamp(System.currentTimeMillis());
            message.setBornHost(new InetSocketAddress("127.0.0.1", 0));
            message.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
            message.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
            final PutMessageResult result = store.messageStore.putMessage(message);
            assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
            payloads.add(payload);
        }
        return payloads;
    }

    private void awaitReplication(TestStore master, TestStore replica, List<String> expected) {
        await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(master.messageStore.getMaxPhyOffset(), replica.messageStore.getMaxPhyOffset());
            assertEquals(master.messageStore.getConfirmOffset(), replica.messageStore.getConfirmOffset());
            assertContents(master, expected);
            assertContents(replica, expected);
        });
    }

    private void awaitContents(TestStore store, List<String> expected) {
        await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertContents(store, expected));
    }

    private void awaitBlocked(TestStore replica) {
        await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .until(() -> replica.haService().getConfirmedOffsetGuard().isBlocked());
    }

    private void assertRemainsRejected(TestStore replica, long protectedEnd, List<String> expected) {
        // Cover a full rejection/reconnect interval as well as the first response.
        await().during(8, TimeUnit.SECONDS).atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(replica.haService().getConfirmedOffsetGuard().isBlocked());
            assertFalse(HAConnectionState.TRANSFER == replica.haService().getHAClient().getCurrentState());
            assertPreserved(replica, protectedEnd, expected);
        });
    }

    private void assertPreserved(TestStore replica, long protectedEnd, List<String> expected) {
        assertEquals(protectedEnd, replica.messageStore.getMaxPhyOffset());
        assertEquals(protectedEnd, replica.messageStore.getConfirmOffset());
        assertContents(replica, expected);
    }

    private void assertContents(TestStore store, List<String> expected) {
        long offset = 0;
        int index = 0;
        while (index < expected.size()) {
            final GetMessageResult result = store.messageStore.getMessage("history-group", TOPIC, 0, offset, 1000, null);
            assertNotNull(result);
            try {
                assertEquals(GetMessageStatus.FOUND, result.getStatus());
                assertTrue(result.getMessageCount() > 0);
                assertEquals((long) expected.size(), result.getMaxOffset());
                for (ByteBuffer buffer : result.getMessageBufferList()) {
                    final MessageExt message = MessageDecoder.decode(buffer.duplicate());
                    assertNotNull(message);
                    assertTrue(index < expected.size());
                    assertEquals((long) index, message.getQueueOffset());
                    assertEquals(expected.get(index), new String(message.getBody(), StandardCharsets.UTF_8));
                    index++;
                }
                assertTrue(result.getNextBeginOffset() > offset);
                offset = result.getNextBeginOffset();
            } finally {
                result.release();
            }
        }
        assertEquals(expected.size(), index);
        assertEquals((long) expected.size(), offset);
    }

    private Map<Path, byte[]> readCommitlogFiles(Path root) throws Exception {
        final Map<Path, byte[]> result = new LinkedHashMap<>();
        try (Stream<Path> paths = Files.list(root.resolve("commitlog"))) {
            for (Path path : paths.filter(Files::isRegularFile).sorted().collect(Collectors.toList())) {
                result.put(path.getFileName(), Files.readAllBytes(path));
            }
        }
        assertFalse(result.isEmpty());
        return result;
    }

    private void assertCommitlogUnchanged(Map<Path, byte[]> expected, Path root) throws Exception {
        final Map<Path, byte[]> actual = readCommitlogFiles(root);
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<Path, byte[]> entry : expected.entrySet()) {
            assertArrayEquals("Retained commitlog changed: " + entry.getKey(), entry.getValue(), actual.get(entry.getKey()));
        }
    }

    private class TestStore {
        private final Path root;
        private final long brokerId;
        private final MessageStoreConfig config;
        private final DefaultMessageStore messageStore;
        private boolean stopped;

        private TestStore(Path root, long brokerId) throws Exception {
            this.root = root;
            this.brokerId = brokerId;
            this.config = new MessageStoreConfig();
            this.config.setStorePathRootDir(root.toString());
            this.config.setMappedFileSizeCommitLog(2048);
            this.config.setMappedFileSizeConsumeQueue(20 * 1024);
            this.config.setMaxHashSlotNum(1000);
            this.config.setMaxIndexNum(1000);
            this.config.setMessageIndexEnable(false);
            this.config.setEnableCompaction(false);
            this.config.setTimerWheelEnable(false);
            this.config.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
            this.config.setFlushIntervalConsumeQueue(10);
            this.config.setBrokerRole(BrokerRole.SLAVE);
            this.config.setHaListenPort(0);
            this.config.setHaSendHeartbeatInterval(200);
            this.config.setSyncFromLastFile(false);
            this.config.setTotalReplicas(2);
            this.config.setInSyncReplicas(1);
            this.config.setMinInSyncReplicas(1);
            this.config.setAllAckInSyncStateSet(false);

            final BrokerConfig brokerConfig = new BrokerConfig();
            brokerConfig.setBrokerName("history-recovery");
            brokerConfig.setBrokerId(brokerId);
            brokerConfig.setEnableControllerMode(true);
            this.messageStore = new DefaultMessageStore(this.config, brokerStatsManager, null,
                brokerConfig, new ConcurrentHashMap<>());
        }

        private AutoSwitchHAService haService() {
            return (AutoSwitchHAService) this.messageStore.getHaService();
        }

        private void becomeMaster(int epoch) throws Exception {
            this.messageStore.getRunningFlags().makeFenced(true);
            this.config.setBrokerRole(BrokerRole.SYNC_MASTER);
            haService().setSyncStateSet(Collections.singleton(this.brokerId));
            assertTrue(haService().changeToMaster(epoch));
            this.messageStore.getRunningFlags().makeFenced(false);
        }

        private void follow(TestStore master, int epoch) {
            this.config.setBrokerRole(BrokerRole.SLAVE);
            final String address = "127.0.0.1:" + master.config.getHaListenPort();
            assertTrue(master.config.getHaListenPort() > 0);
            assertTrue(haService().changeToSlave(address, epoch, this.brokerId));
            haService().updateHaMasterAddress(address);
            this.messageStore.getRunningFlags().makeFenced(false);
        }

        private void stop() {
            if (!this.stopped) {
                this.messageStore.shutdown();
                this.stopped = true;
            }
        }
    }
}
