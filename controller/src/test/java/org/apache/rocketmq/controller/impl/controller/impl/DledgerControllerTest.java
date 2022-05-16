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
package org.apache.rocketmq.controller.impl.controller.impl;

import io.openmessaging.storage.dledger.DLedgerConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.BrokerRegisterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.BrokerRegisterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.impl.DledgerController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DledgerControllerTest {
    private List<String> baseDirs;
    private List<DledgerController> controllers;

    public DledgerController launchController(final String group, final String peers, final String selfId, String storeType, final boolean isEnableElectUncleanMaster) {
        final String path = "/tmp" + File.separator + group + File.separator + selfId;
        baseDirs.add(path);

        final ControllerConfig config = new ControllerConfig();
        config.setControllerDLegerGroup(group);
        config.setControllerDLegerPeers(peers);
        config.setControllerDLegerSelfId(selfId);
        config.setControllerStorePath(path);
        config.setMappedFileSize(10 * 1024 * 1024);
        config.setEnableElectUncleanMaster(isEnableElectUncleanMaster);

        final DledgerController controller = new DledgerController(config);

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
        final BrokerRegisterRequestHeader registerRequest =
            new BrokerRegisterRequestHeader(clusterName, brokerName, brokerAddress);
        final RemotingCommand response = leader.registerBroker(registerRequest).get(10, TimeUnit.SECONDS);
        final BrokerRegisterResponseHeader registerResult = (BrokerRegisterResponseHeader) response.readCustomHeader();
        System.out.println("------------- Register broker done, the result is :" + registerResult);

        if (!isFirstRegisteredBroker) {
            assertTrue(registerResult.getBrokerId() > 0);
        }
        return true;
    }

    private boolean alterNewInSyncSet(Controller leader, String brokerName, String masterAddress, int masterEpoch,
        Set<String> newSyncStateSet, int syncStateSetEpoch) throws Exception {
        final AlterSyncStateSetRequestHeader alterRequest =
            new AlterSyncStateSetRequestHeader(brokerName, masterAddress, masterEpoch);
        final RemotingCommand response = leader.alterSyncStateSet(alterRequest, new SyncStateSet(newSyncStateSet, syncStateSetEpoch)).get(10, TimeUnit.SECONDS);

        final RemotingCommand getInfoResponse = leader.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName)).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) getInfoResponse.readCustomHeader();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(getInfoResponse.getBody(), SyncStateSet.class);
        assertArrayEquals(syncStateSet.getSyncStateSet().toArray(), newSyncStateSet.toArray());
        assertEquals(syncStateSet.getSyncStateSetEpoch(), syncStateSetEpoch + 1);
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

    public DledgerController mockMetaData(boolean enableElectUncleanMaster) throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", 30000, 30001, 30002);
        DledgerController c0 = launchController(group, peers, "n0", DLedgerConfig.MEMORY, enableElectUncleanMaster);
        DledgerController c1 = launchController(group, peers, "n1", DLedgerConfig.MEMORY, enableElectUncleanMaster);
        DledgerController c2 = launchController(group, peers, "n2", DLedgerConfig.MEMORY, enableElectUncleanMaster);
        controllers.add(c0);
        controllers.add(c1);
        controllers.add(c2);

        DledgerController leader = waitLeader(controllers);

        assertTrue(registerNewBroker(leader, "cluster1", "broker1", "127.0.0.1:9000", true));
        assertTrue(registerNewBroker(leader, "cluster1", "broker1", "127.0.0.1:9001", true));
        assertTrue(registerNewBroker(leader, "cluster1", "broker1", "127.0.0.1:9002", true));
        final RemotingCommand getInfoResponse = leader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) getInfoResponse.readCustomHeader();
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
        final DledgerController leader = mockMetaData(false);
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        final RemotingCommand resp = leader.electMaster(request).get(10, TimeUnit.SECONDS);
        final ElectMasterResponseHeader response = (ElectMasterResponseHeader) resp.readCustomHeader();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertNotEquals(response.getNewMasterAddress(), "127.0.0.1:9000");
    }

    @Test
    public void testAllReplicasShutdownAndRestartWithUnEnableElectUnCleanMaster() throws Exception {
        final DledgerController leader = mockMetaData(false);
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");

        assertTrue(alterNewInSyncSet(leader, "broker1", "127.0.0.1:9000", 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {"127.0.0.1:9000"}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = new ElectMasterRequestHeader("broker1");
        leader.electMaster(electRequest).get(10, TimeUnit.SECONDS);

        final RemotingCommand resp = leader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).
            get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) resp.readCustomHeader();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);
        assertEquals(syncStateSet.getSyncStateSet(), newSyncStateSet);
        assertEquals(replicaInfo.getMasterAddress(), "");
        assertEquals(replicaInfo.getMasterEpoch(), 2);

        // Now, we start broker1 - 127.0.0.1:9001, but it was not in syncStateSet, so it will not be elected as master.
        final BrokerRegisterRequestHeader request1 =
            new BrokerRegisterRequestHeader("cluster1", "broker1", "127.0.0.1:9001");
        final BrokerRegisterResponseHeader r1 = (BrokerRegisterResponseHeader) leader.registerBroker(request1).get(10, TimeUnit.SECONDS).readCustomHeader();
        assertEquals(r1.getBrokerId(), 2);
        assertEquals(r1.getMasterAddress(), "");
        assertEquals(r1.getMasterEpoch(), 2);

        // Now, we start broker1 - 127.0.0.1:9000, it will be elected as master
        final BrokerRegisterRequestHeader request2 =
            new BrokerRegisterRequestHeader("cluster1", "broker1", "127.0.0.1:9000");
        final BrokerRegisterResponseHeader r2 = (BrokerRegisterResponseHeader) leader.registerBroker(request2).get(10, TimeUnit.SECONDS).readCustomHeader();
        assertEquals(r2.getBrokerId(), 0);
        assertEquals(r2.getMasterAddress(), "127.0.0.1:9000");
        assertEquals(r2.getMasterEpoch(), 3);
    }

    @Test
    public void testEnableElectUnCleanMaster() throws Exception {
        final DledgerController leader = mockMetaData(true);
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");

        assertTrue(alterNewInSyncSet(leader, "broker1", "127.0.0.1:9000", 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, event if the syncStateSet in statemachine is {"127.0.0.1:9000"}
        // the option {enableElectUncleanMaster = true}, so the controller sill can elect a new master
        final ElectMasterRequestHeader electRequest = new ElectMasterRequestHeader("broker1");
        final CompletableFuture<RemotingCommand> future = leader.electMaster(electRequest);
        future.get(10, TimeUnit.SECONDS);

        final RemotingCommand resp = leader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) resp.readCustomHeader();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);

         final HashSet<String> newSyncStateSet2 = new HashSet<>();
        newSyncStateSet2.add(replicaInfo.getMasterAddress());
        assertEquals(syncStateSet.getSyncStateSet(), newSyncStateSet2);
        assertNotEquals(replicaInfo.getMasterAddress(), "");
        assertNotEquals(replicaInfo.getMasterAddress(), "127.0.0.1:9000");
        assertEquals(replicaInfo.getMasterEpoch(), 2);
    }

    @Test
    public void testChangeControllerLeader() throws Exception {
        final DledgerController leader = mockMetaData(false);
        leader.shutdown();
        Thread.sleep(2000);
        this.controllers.remove(leader);
        // Wait leader again
        final DledgerController newLeader = waitLeader(this.controllers);
        assertNotNull(newLeader);

        final RemotingCommand resp = newLeader.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) resp.readCustomHeader();
        final SyncStateSet syncStateSetResult = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);

        assertEquals(replicaInfo.getMasterAddress(), "127.0.0.1:9000");
        assertEquals(replicaInfo.getMasterEpoch(), 1);

        final HashSet<String> syncStateSet = new HashSet<>();
        syncStateSet.add("127.0.0.1:9000");
        syncStateSet.add("127.0.0.1:9001");
        syncStateSet.add("127.0.0.1:9002");
        assertEquals(syncStateSetResult.getSyncStateSet(), syncStateSet);
    }
}