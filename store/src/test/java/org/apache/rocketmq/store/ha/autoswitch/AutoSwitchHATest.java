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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.StoreCheckpoint;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assume;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoSwitchHATest {
    private final String storeMessage = "Once, there was a chance for me!";
    private final int defaultMappedFileSize = 1024 * 1024;
    private int queueTotal = 100;
    private AtomicInteger queueId = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private byte[] messageBody;

    private DefaultMessageStore messageStore1;
    private DefaultMessageStore messageStore2;
    private DefaultMessageStore messageStore3;
    private MessageStoreConfig storeConfig1;
    private MessageStoreConfig storeConfig2;
    private MessageStoreConfig storeConfig3;
    private String store1HaAddress;
    private String store2HaAddress;

    private BrokerStatsManager brokerStatsManager = new BrokerStatsManager("simpleTest", true);
    private String tmpdir = System.getProperty("java.io.tmpdir");
    private String storePathRootParentDir = (StringUtils.endsWith(tmpdir, File.separator) ? tmpdir : tmpdir + File.separator) + UUID.randomUUID();
    private String storePathRootDir = storePathRootParentDir + File.separator + "store";
    private Random random = new Random();

    public void init(int mappedFileSize) throws Exception {
        String brokerName = "AutoSwitchHATest_" + random.nextInt(65535);
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        storeConfig1 = new MessageStoreConfig();
        storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        storeConfig1.setHaSendHeartbeatInterval(1000);
        storeConfig1.setStorePathRootDir(storePathRootDir + File.separator + brokerName + "#1");
        storeConfig1.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + "#1" + File.separator + "commitlog");
        storeConfig1.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + "#1" + File.separator + "EpochFileCache");
        storeConfig1.setTotalReplicas(3);
        storeConfig1.setInSyncReplicas(2);
        buildMessageStoreConfig(storeConfig1, mappedFileSize);
        this.store1HaAddress = "127.0.0.1:10912";

        storeConfig2 = new MessageStoreConfig();
        storeConfig2.setBrokerRole(BrokerRole.SLAVE);
        storeConfig2.setHaSendHeartbeatInterval(1000);
        storeConfig2.setStorePathRootDir(storePathRootDir + File.separator + brokerName + "#2");
        storeConfig2.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + "#2" + File.separator + "commitlog");
        storeConfig2.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + "#2" + File.separator + "EpochFileCache");
        storeConfig2.setHaListenPort(10943);
        storeConfig2.setTotalReplicas(3);
        storeConfig2.setInSyncReplicas(2);
        buildMessageStoreConfig(storeConfig2, mappedFileSize);
        this.store2HaAddress = "127.0.0.1:10943";

        messageStore1 = buildMessageStore(storeConfig1, 1L);
        messageStore2 = buildMessageStore(storeConfig2, 2L);

        storeConfig3 = new MessageStoreConfig();
        storeConfig3.setBrokerRole(BrokerRole.SLAVE);
        storeConfig3.setHaSendHeartbeatInterval(1000);
        storeConfig3.setStorePathRootDir(storePathRootDir + File.separator + brokerName + "#3");
        storeConfig3.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + "#3" + File.separator + "commitlog");
        storeConfig3.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + "#3" + File.separator + "EpochFileCache");
        storeConfig3.setHaListenPort(10980);
        storeConfig3.setTotalReplicas(3);
        storeConfig3.setInSyncReplicas(2);
        buildMessageStoreConfig(storeConfig3, mappedFileSize);
        messageStore3 = buildMessageStore(storeConfig3, 3L);

        assertTrue(messageStore1.load());
        assertTrue(messageStore2.load());
        assertTrue(messageStore3.load());
        messageStore1.start();
        messageStore2.start();
        messageStore3.start();

//        ((AutoSwitchHAService) this.messageStore1.getHaService()).("127.0.0.1:8000");
//        ((AutoSwitchHAService) this.messageStore2.getHaService()).setLocalAddress("127.0.0.1:8001");
//        ((AutoSwitchHAService) this.messageStore3.getHaService()).setLocalAddress("127.0.0.1:8002");
    }

    public void init(int mappedFileSize, boolean allAckInSyncStateSet) throws Exception {
        String brokerName = "AutoSwitchHATest_" + random.nextInt(65535);
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        storeConfig1 = new MessageStoreConfig();
        storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        storeConfig1.setStorePathRootDir(storePathRootDir + File.separator + brokerName + "#1");
        storeConfig1.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + "#1" + File.separator + "commitlog");
        storeConfig1.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + "#1" + File.separator + "EpochFileCache");
        storeConfig1.setAllAckInSyncStateSet(allAckInSyncStateSet);
        buildMessageStoreConfig(storeConfig1, mappedFileSize);
        this.store1HaAddress = "127.0.0.1:10912";

        storeConfig2 = new MessageStoreConfig();
        storeConfig2.setBrokerRole(BrokerRole.SLAVE);
        storeConfig2.setStorePathRootDir(storePathRootDir + File.separator + brokerName + "#2");
        storeConfig2.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + "#2" + File.separator + "commitlog");
        storeConfig2.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + "#2" + File.separator + "EpochFileCache");
        storeConfig2.setHaListenPort(10943);
        storeConfig2.setAllAckInSyncStateSet(allAckInSyncStateSet);
        buildMessageStoreConfig(storeConfig2, mappedFileSize);
        this.store2HaAddress = "127.0.0.1:10943";

        messageStore1 = buildMessageStore(storeConfig1, 1L);
        messageStore2 = buildMessageStore(storeConfig2, 2L);

        assertTrue(messageStore1.load());
        assertTrue(messageStore2.load());
        messageStore1.start();
        messageStore2.start();

//        ((AutoSwitchHAService) this.messageStore1.getHaService()).setLocalAddress("127.0.0.1:8000");
//        ((AutoSwitchHAService) this.messageStore2.getHaService()).setLocalAddress("127.0.0.1:8001");
    }

    private boolean changeMasterAndPutMessage(DefaultMessageStore master, MessageStoreConfig masterConfig,
        DefaultMessageStore slave, long slaveId, MessageStoreConfig slaveConfig, int epoch, String masterHaAddress,
        int totalPutMessageNums) {

        boolean flag = true;
        // Change role
        slaveConfig.setBrokerRole(BrokerRole.SLAVE);
        masterConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        flag &= slave.getHaService().changeToSlave("", epoch, slaveId);
        slave.getHaService().updateHaMasterAddress(masterHaAddress);
        flag &= master.getHaService().changeToMaster(epoch);
        // Put message on master
        for (int i = 0; i < totalPutMessageNums; i++) {
            PutMessageResult result = master.putMessage(buildMessage());
            flag &= result.isOk();
        }
        return flag;
    }

    private void checkMessage(final DefaultMessageStore messageStore, int totalNums, int startOffset) {
        await().atMost(30, TimeUnit.SECONDS)
            .until(() -> {
                GetMessageResult result = messageStore.getMessage("GROUP_A", "FooBar", 0, startOffset, 1024, null);
//                System.out.printf(result + "%n");
                return result != null && result.getStatus() == GetMessageStatus.FOUND && result.getMessageCount() >= totalNums;
            });
    }

    @Test
    public void testConfirmOffset() throws Exception {
        init(defaultMappedFileSize, true);
        // Step1, set syncStateSet, if both broker1 and broker2 are in syncStateSet, the confirmOffset will be computed as the min slaveAckOffset(broker2's ack)
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Arrays.asList(1L, 2L)));
        boolean masterAndPutMessage = changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        assertTrue(masterAndPutMessage);
        checkMessage(this.messageStore2, 10, 0);

        final long confirmOffset = this.messageStore1.getConfirmOffset();

        // Step2, shutdown store2
        this.messageStore2.shutdown();

        // Put message, which should put failed.
        final PutMessageResult putMessageResult = this.messageStore1.putMessage(buildMessage());
        assertEquals(putMessageResult.getPutMessageStatus(), PutMessageStatus.FLUSH_SLAVE_TIMEOUT);

        // The confirmOffset still don't change, because syncStateSet contains broker2, but broker2 shutdown
        assertEquals(confirmOffset, this.messageStore1.getConfirmOffset());

        // Step3, shutdown store1, start store2, change store2 to master, epoch = 2
        this.messageStore1.shutdown();

        storeConfig2.setBrokerRole(BrokerRole.SYNC_MASTER);
        messageStore2 = buildMessageStore(storeConfig2, 2L);
        messageStore2.getRunningFlags().makeFenced(true);
        assertTrue(messageStore2.load());
        messageStore2.start();
        messageStore2.getHaService().changeToMaster(2);
        messageStore2.getRunningFlags().makeFenced(false);
        ((AutoSwitchHAService) messageStore2.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(2L)));

        // Put message on master
        for (int i = 0; i < 10; i++) {
            messageStore2.putMessage(buildMessage());
        }

        // Step4, start store1, it should truncate dirty logs and syncLog from store2
        storeConfig1.setBrokerRole(BrokerRole.SLAVE);
        messageStore1 = buildMessageStore(storeConfig1, 1L);
        assertTrue(messageStore1.load());
        messageStore1.start();
        messageStore1.getHaService().changeToSlave("", 2, 1L);
        messageStore1.getHaService().updateHaMasterAddress(this.store2HaAddress);

        checkMessage(this.messageStore1, 20, 0);
    }

    @Test
    public void testAsyncLearnerBrokerRole() throws Exception {
        init(defaultMappedFileSize);
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));

        storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        storeConfig2.setBrokerRole(BrokerRole.SLAVE);
        storeConfig2.setAsyncLearner(true);
        messageStore1.getHaService().changeToMaster(1);
        messageStore2.getHaService().changeToSlave("", 1, 2L);
        messageStore2.getHaService().updateHaMasterAddress(store1HaAddress);
        // Put message on master
        for (int i = 0; i < 10; i++) {
            messageStore1.putMessage(buildMessage());
        }
        checkMessage(messageStore2, 10, 0);
        final Set<Long> syncStateSet = ((AutoSwitchHAService) this.messageStore1.getHaService()).getSyncStateSet();
        assertFalse(syncStateSet.contains(2L));
    }

    @Test
    public void testOptionAllAckInSyncStateSet() throws Exception {
        init(defaultMappedFileSize, true);
        AtomicReference<Set<Long>> syncStateSet = new AtomicReference<>();
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));
        ((AutoSwitchHAService) this.messageStore1.getHaService()).registerSyncStateSetChangedListener(newSyncStateSet -> {
            syncStateSet.set(newSyncStateSet);
        });

        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);
        // Check syncStateSet
        final Set<Long> result = syncStateSet.get();
        assertTrue(result.contains(1L));
        assertTrue(result.contains(2L));

        // Now, shutdown store2
        this.messageStore2.shutdown();
        this.messageStore2.destroy();

        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(result);

        final PutMessageResult putMessageResult = this.messageStore1.putMessage(buildMessage());
        assertEquals(putMessageResult.getPutMessageStatus(), PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
    }

    @Ignore
    @Test
    public void testChangeRoleManyTimes() throws Exception {

        // Skip MacOSX platform for now as this test case is not stable on it.
        Assume.assumeFalse(MixAll.isMac());

        // Step1, change store1 to master, store2 to follower
        init(defaultMappedFileSize);
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);

        // Step2, change store1 to follower, store2 to master, epoch = 2
        ((AutoSwitchHAService) this.messageStore2.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(2L)));
        changeMasterAndPutMessage(this.messageStore2, this.storeConfig2, this.messageStore1, 1, this.storeConfig1, 2, store2HaAddress, 10);
        checkMessage(this.messageStore1, 20, 0);

        // Step3, change store2 to follower, store1 to master, epoch = 3
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 3, store1HaAddress, 10);
        checkMessage(this.messageStore2, 30, 0);
    }

    @Test
    public void testAddBroker() throws Exception {
        // Step1: broker1 as leader, broker2 as follower
        init(defaultMappedFileSize);
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));

        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);

        // Step2: add new broker3, link to broker1
        messageStore3.getHaService().changeToSlave("", 1, 3L);
        messageStore3.getHaService().updateHaMasterAddress(store1HaAddress);
        checkMessage(messageStore3, 10, 0);
    }

    @Test
    public void testTruncateEpochLogAndAddBroker() throws Exception {
        // Noted that 10 msg 's total size = 1570, and if init the mappedFileSize = 1700, one file only be used to store 10 msg.
        init(1700);

        // Step1: broker1 as leader, broker2 as follower, append 2 epoch, each epoch will be stored on one file(Because fileSize = 1700, which only can hold 10 msgs);
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>

        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);

        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 2, store1HaAddress, 10);
        checkMessage(this.messageStore2, 20, 0);

        // Step2: Check file position, each epoch will be stored on one file(Because fileSize = 1700, which equal to 10 msg size);
        // So epoch1 was stored in firstFile, epoch2 was stored in second file, the lastFile was empty.
        final MappedFileQueue fileQueue = this.messageStore1.getCommitLog().getMappedFileQueue();
        assertEquals(2, fileQueue.getTotalFileSize() / 1700);

        // Step3: truncate epoch1's log (truncateEndOffset = 1570), which means we should delete the first file directly.
        final MappedFile firstFile = this.messageStore1.getCommitLog().getMappedFileQueue().getFirstMappedFile();
        firstFile.shutdown(1000);
        fileQueue.retryDeleteFirstFile(1000);
        assertEquals(this.messageStore1.getCommitLog().getMinOffset(), 1700);
        checkMessage(this.messageStore1, 10, 10);

        final AutoSwitchHAService haService = (AutoSwitchHAService) this.messageStore1.getHaService();
        haService.truncateEpochFilePrefix(1570);

        // Step4: add broker3 as slave, only have 10 msg from offset 10;
        messageStore3.getHaService().changeToSlave("", 2, 3L);
        messageStore3.getHaService().updateHaMasterAddress(store1HaAddress);

        checkMessage(messageStore3, 10, 10);
    }

    @Test
    public void testTruncateEpochLogAndChangeMaster() throws Exception {
        // Noted that 10 msg 's total size = 1570, and if init the mappedFileSize = 1700, one file only be used to store 10 msg.
        init(1700);

        // Step1: broker1 as leader, broker2 as follower, append 2 epoch, each epoch will be stored on one file(Because fileSize = 1700, which only can hold 10 msgs);
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>

        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 2, store1HaAddress, 10);
        checkMessage(this.messageStore2, 20, 0);

        // Step2: Check file position, each epoch will be stored on one file(Because fileSize = 1700, which equal to 10 msg size);
        // So epoch1 was stored in firstFile, epoch2 was stored in second file, the lastFile was empty.
        final MappedFileQueue fileQueue = this.messageStore1.getCommitLog().getMappedFileQueue();
        assertEquals(2, fileQueue.getTotalFileSize() / 1700);

        // Step3: truncate epoch1's log (truncateEndOffset = 1570), which means we should delete the first file directly.
        final MappedFile firstFile = this.messageStore1.getCommitLog().getMappedFileQueue().getFirstMappedFile();
        firstFile.shutdown(1000);
        fileQueue.retryDeleteFirstFile(1000);
        assertEquals(this.messageStore1.getCommitLog().getMinOffset(), 1700);

        final AutoSwitchHAService haService = (AutoSwitchHAService) this.messageStore1.getHaService();
        haService.truncateEpochFilePrefix(1570);
        checkMessage(this.messageStore1, 10, 10);

        // Step4: add broker3 as slave
        messageStore3.getHaService().changeToSlave("", 2, 3L);
        messageStore3.getHaService().updateHaMasterAddress(store1HaAddress);

        checkMessage(messageStore3, 10, 10);

        // Step5: change broker2 as leader, broker3 as follower
        ((AutoSwitchHAService) this.messageStore2.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(2L)));
        changeMasterAndPutMessage(this.messageStore2, this.storeConfig2, this.messageStore3, 3, this.storeConfig3, 3, this.store2HaAddress, 10);
        checkMessage(messageStore3, 20, 10);

        // Step6, let broker1 link to broker2, it should sync log from epoch3.
        this.storeConfig1.setBrokerRole(BrokerRole.SLAVE);
        this.messageStore1.getHaService().changeToSlave("", 3, 1L);
        this.messageStore1.getHaService().updateHaMasterAddress(this.store2HaAddress);

        checkMessage(messageStore1, 20, 0);
    }

    @Test
    public void testAddBrokerAndSyncFromLastFile() throws Exception {
        init(1700);

        // Step1: broker1 as leader, broker2 as follower, append 2 epoch, each epoch will be stored on one file(Because fileSize = 1700, which only can hold 10 msgs);
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 2, store1HaAddress, 10);
        checkMessage(this.messageStore2, 20, 0);

        // Step2: restart broker3
        messageStore3.shutdown();
        messageStore3.destroy();

        storeConfig3.setSyncFromLastFile(true);
        messageStore3 = buildMessageStore(storeConfig3, 3L);
        assertTrue(messageStore3.load());
        messageStore3.start();

        // Step2: add new broker3, link to broker1. because broker3 request sync from lastFile, so it only synced 10 msg from offset 10;
        messageStore3.getHaService().changeToSlave("", 2, 3L);
        messageStore3.getHaService().updateHaMasterAddress("127.0.0.1:10912");

        checkMessage(messageStore3, 10, 10);
    }

    @Test
    public void testCheckSynchronizingSyncStateSetFlag() throws Exception {
        // Step1: broker1 as leader, broker2 as follower
        init(defaultMappedFileSize);
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));

        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);
        AutoSwitchHAService masterHAService = (AutoSwitchHAService) this.messageStore1.getHaService();

        // Step2: check flag SynchronizingSyncStateSet
        Assert.assertTrue(masterHAService.isSynchronizingSyncStateSet());
        Assert.assertEquals(this.messageStore1.getConfirmOffset(), 1570);
        Set<Long> syncStateSet = masterHAService.getSyncStateSet();
        Assert.assertEquals(syncStateSet.size(), 2);
        Assert.assertTrue(syncStateSet.contains(1L));

        // Step3: set new syncStateSet
        HashSet<Long> newSyncStateSet = new HashSet<Long>() {{
                add(1L);
                add(2L);
                }};
        masterHAService.setSyncStateSet(newSyncStateSet);
        Assert.assertFalse(masterHAService.isSynchronizingSyncStateSet());
    }

    @Test
    public void testBuildConsumeQueueNotExceedConfirmOffset() throws Exception {
        init(defaultMappedFileSize);
        ((AutoSwitchHAService) this.messageStore1.getHaService()).setSyncStateSet(new HashSet<>(Collections.singletonList(1L)));
        changeMasterAndPutMessage(this.messageStore1, this.storeConfig1, this.messageStore2, 2, this.storeConfig2, 1, store1HaAddress, 10);
        checkMessage(this.messageStore2, 10, 0);

        long tmpConfirmOffset = this.messageStore2.getConfirmOffset();
        long setConfirmOffset = this.messageStore2.getConfirmOffset() - this.messageStore2.getConfirmOffset() / 2;
        messageStore2.shutdown();
        StoreCheckpoint storeCheckpoint = new StoreCheckpoint(storeConfig2.getStorePathRootDir() + File.separator + "checkpoint");
        assertEquals(tmpConfirmOffset, storeCheckpoint.getConfirmPhyOffset());
        storeCheckpoint.setConfirmPhyOffset(setConfirmOffset);
        storeCheckpoint.shutdown();
        messageStore2 = buildMessageStore(storeConfig2, 2L);
        messageStore2.getRunningFlags().makeFenced(true);
        assertTrue(messageStore2.load());
        messageStore2.start();
        messageStore2.getRunningFlags().makeFenced(false);
        assertEquals(setConfirmOffset, messageStore2.getConfirmOffset());
        checkMessage(this.messageStore2, 5, 0);
    }

    @After
    public void destroy() throws Exception {
        if (this.messageStore2 != null) {
            messageStore2.shutdown();
            messageStore2.destroy();
        }
        if (this.messageStore1 != null) {
            messageStore1.shutdown();
            messageStore1.destroy();
        }
        if (this.messageStore3 != null) {
            messageStore3.shutdown();
            messageStore3.destroy();
        }
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }

    private DefaultMessageStore buildMessageStore(MessageStoreConfig messageStoreConfig,
        long brokerId) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        brokerConfig.setEnableControllerMode(true);
        return new DefaultMessageStore(messageStoreConfig, brokerStatsManager, null, brokerConfig, new ConcurrentHashMap<>());
    }

    private void buildMessageStoreConfig(MessageStoreConfig messageStoreConfig, int mappedFileSize) {
        messageStoreConfig.setMappedFileSizeCommitLog(mappedFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }
}
