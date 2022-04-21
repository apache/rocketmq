package org.apache.rocketmq.namesrv.controller.impl;

import io.openmessaging.storage.dledger.DLedgerConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ErrorCodes;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerResponseHeader;
import org.apache.rocketmq.namesrv.controller.Controller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/20 11:05
 */
public class DledgerControllerTest {
    private List<String> baseDirs;
    private List<DledgerController> controllers;

    public DledgerController launchController(final String group, final String peers, final String selfId,
        final String leaderId, String storeType, final boolean isEnableElectUncleanMaster) {
        final String path = "/tmp" + File.separator + group + File.separator + selfId;
        baseDirs.add(path);

        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(path);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);

        final DledgerController controller = new DledgerController(config, isEnableElectUncleanMaster);

        controller.startup();
        return controller;
    }

    @Before
    public void startup() {
        this.baseDirs = new ArrayList<>();
        this.controllers = new ArrayList<>();
    }

    @After
    public void tearDown() {
        for (Controller controller : this.controllers) {
            controller.shutdown();
        }
        for (String dir : this.baseDirs) {
            System.out.println("Delete file " + dir);
            new File(dir).delete();
        }
    }

    public boolean registerNewBroker(Controller leader, String clusterName, String brokerName, String brokerAddress,
        boolean isFirstRegisteredBroker) throws Exception {
        // Register new broker
        final RegisterBrokerRequestHeader registerRequest =
            new RegisterBrokerRequestHeader(clusterName, brokerName, brokerAddress);
        final CompletableFuture<RegisterBrokerResponseHeader> response = leader.registerBroker(registerRequest);
        final RegisterBrokerResponseHeader registerResult = response.get(10, TimeUnit.SECONDS);
        System.out.println("------------- Register broker done, the result is :" + registerResult);

        if (!isFirstRegisteredBroker) {
            assertTrue(registerResult.getBrokerId() > 0);
        }
        Thread.sleep(500);
        return true;
    }

    private boolean alterNewInSyncSet(Controller leader, String brokerName, String masterAddress, int masterEpoch,
        Set<String> newSyncStateSet, int syncStateSetEpoch) throws Exception {
        final AlterInSyncReplicasRequestHeader alterRequest =
            new AlterInSyncReplicasRequestHeader(brokerName, masterAddress, masterEpoch, newSyncStateSet, syncStateSetEpoch);
        final CompletableFuture<AlterInSyncReplicasResponseHeader> response = leader.alterInSyncReplicas(alterRequest);
        response.get(10, TimeUnit.SECONDS);
        Thread.sleep(500);

        final CompletableFuture<GetReplicaInfoResponseHeader> getInfoResponse = leader.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));
        final GetReplicaInfoResponseHeader replicaInfo = getInfoResponse.get();
        if (replicaInfo.getErrorCode() != ErrorCodes.NONE.getCode()) {
            return false;
        }
        assertArrayEquals(replicaInfo.getSyncStateSet().toArray(), newSyncStateSet.toArray());
        assertEquals(replicaInfo.getSyncStateSetEpoch(), syncStateSetEpoch + 1);
        return true;
    }

    public DledgerController waitLeader(final List<DledgerController> controllers) throws Exception {
        if (controllers.isEmpty()) {
            return null;
        }
        DledgerController c1 = controllers.get(0);
        while (c1.getMemberState().getLeaderId() == null) {
            Thread.sleep(1000);
        }
        String leaderId = c1.getMemberState().getLeaderId();
        System.out.println("New leader " + leaderId);
        for (DledgerController controller : controllers) {
            if (controller.getMemberState().getSelfId().equals(leaderId)) {
                return controller;
            }
        }
        return null;
    }

    public DledgerController mockMetaData() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", 30000, 30001, 30002);
        DledgerController c0 = launchController(group, peers, "n0", "n1", DLedgerConfig.MEMORY, true);
        DledgerController c1 = launchController(group, peers, "n1", "n1", DLedgerConfig.MEMORY, true);
        DledgerController c2 = launchController(group, peers, "n2", "n1", DLedgerConfig.MEMORY, true);
        controllers.add(c0);
        controllers.add(c1);
        controllers.add(c2);

        DledgerController leader = waitLeader(controllers);
        Thread.sleep(2000);

        assertTrue(registerNewBroker(leader, "cluster1", "broker1", "127.0.0.1:9000", true));
        assertTrue(registerNewBroker(leader, "cluster1", "broker1", "127.0.0.1:9001", true));
        assertTrue(registerNewBroker(leader, "cluster1", "broker1", "127.0.0.1:9002", true));
        final CompletableFuture<GetReplicaInfoResponseHeader> getInfoResponse = leader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1"));
        final GetReplicaInfoResponseHeader replicaInfo = getInfoResponse.get();
        assertEquals(replicaInfo.getMasterEpoch(), 1);
        assertEquals(replicaInfo.getMasterAddress(), "127.0.0.1:9000");

        // Try alter sync state set
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        newSyncStateSet.add("127.0.0.1:9001");
        newSyncStateSet.add("127.0.0.1:9002");
        assertTrue(alterNewInSyncSet(leader, "broker1", "127.0.0.1:9000", 1, newSyncStateSet, 1));
        return leader;
    }

    @Test
    public void testElectMaster() throws Exception {
        final DledgerController leader = mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        final CompletableFuture<ElectMasterResponseHeader> future = leader.electMaster(request);
        final ElectMasterResponseHeader response = future.get(10, TimeUnit.SECONDS);
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertNotEquals(response.getNewMasterAddress(), "127.0.0.1:9000");
    }

    @Test
    public void testAllReplicasShutdownAndRestart() throws Exception {
        final DledgerController leader = mockMetaData();
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");

        assertTrue(alterNewInSyncSet(leader, "broker1", "127.0.0.1:9000", 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {"127.0.0.1:9000"}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = new ElectMasterRequestHeader("broker1");
        final CompletableFuture<ElectMasterResponseHeader> future = leader.electMaster(electRequest);
        final ElectMasterResponseHeader response = future.get(10, TimeUnit.SECONDS);
        Thread.sleep(500);
        assertEquals(response.getErrorCode(), ErrorCodes.MASTER_NOT_AVAILABLE.getCode());

        final GetReplicaInfoResponseHeader replicaInfo = leader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).get(10, TimeUnit.SECONDS);
        assertEquals(replicaInfo.getMasterAddress(), "");
        assertEquals(replicaInfo.getMasterEpoch(), 2);
    }

    @Test
    public void testChangeControllerLeader() throws Exception {
        final DledgerController leader = mockMetaData();
        leader.shutdown();
        Thread.sleep(2000);
        this.controllers.remove(leader);
        // Wait leader again
        final DledgerController newLeader = waitLeader(this.controllers);
        assertNotNull(newLeader);

        final GetReplicaInfoResponseHeader response = newLeader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).get(10, TimeUnit.SECONDS);
        assertEquals(response.getMasterAddress(), "127.0.0.1:9000");
        assertEquals(response.getMasterEpoch(), 1);

        final HashSet<String> syncStateSet = new HashSet<>();
        syncStateSet.add("127.0.0.1:9000");
        syncStateSet.add("127.0.0.1:9001");
        syncStateSet.add("127.0.0.1:9002");
        assertEquals(response.getSyncStateSet(), syncStateSet);

    }
}