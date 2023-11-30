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
package org.apache.rocketmq.controller.impl;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_BROKER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_CLUSTER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_IP;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DLedgerControllerTest {
    private List<String> baseDirs;
    private List<DLedgerController> controllers;

    public DLedgerController launchController(final String group, final String peers, final String selfId,
        final boolean isEnableElectUncleanMaster) {
        String tmpdir = System.getProperty("java.io.tmpdir");
        final String path = (StringUtils.endsWith(tmpdir, File.separator) ? tmpdir : tmpdir + File.separator) + group + File.separator + selfId;
        baseDirs.add(path);

        final ControllerConfig config = new ControllerConfig();
        config.setControllerDLegerGroup(group);
        config.setControllerDLegerPeers(peers);
        config.setControllerDLegerSelfId(selfId);
        config.setControllerStorePath(path);
        config.setMappedFileSize(10 * 1024 * 1024);
        config.setEnableElectUncleanMaster(isEnableElectUncleanMaster);
        config.setScanInactiveMasterInterval(1000);
        final DLedgerController controller = new DLedgerController(config, (str1, str2, str3) -> true);

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
            new File(dir).delete();
        }
    }

    public void registerNewBroker(Controller leader, String clusterName, String brokerName, String brokerAddress,
        Long expectBrokerId) throws Exception {
        // Get next brokerId
        final GetNextBrokerIdRequestHeader getNextBrokerIdRequest = new GetNextBrokerIdRequestHeader(clusterName, brokerName);
        RemotingCommand remotingCommand = leader.getNextBrokerId(getNextBrokerIdRequest).get(2, TimeUnit.SECONDS);
        GetNextBrokerIdResponseHeader getNextBrokerIdResp = (GetNextBrokerIdResponseHeader) remotingCommand.readCustomHeader();
        Long nextBrokerId = getNextBrokerIdResp.getNextBrokerId();
        String registerCheckCode = brokerAddress + ";" + System.currentTimeMillis();

        // Check response
        assertEquals(expectBrokerId, nextBrokerId);

        // Apply brokerId
        final ApplyBrokerIdRequestHeader applyBrokerIdRequestHeader = new ApplyBrokerIdRequestHeader(clusterName, brokerName, nextBrokerId, registerCheckCode);
        RemotingCommand remotingCommand1 = leader.applyBrokerId(applyBrokerIdRequestHeader).get(2, TimeUnit.SECONDS);

        // Check response
        assertEquals(ResponseCode.SUCCESS, remotingCommand1.getCode());

        // Register success
        final RegisterBrokerToControllerRequestHeader registerBrokerToControllerRequestHeader = new RegisterBrokerToControllerRequestHeader(clusterName, brokerName, nextBrokerId, brokerAddress);
        RemotingCommand remotingCommand2 = leader.registerBroker(registerBrokerToControllerRequestHeader).get(2, TimeUnit.SECONDS);

        assertEquals(ResponseCode.SUCCESS, remotingCommand2.getCode());
    }

    public void brokerTryElectMaster(Controller leader, String clusterName, String brokerName, String brokerAddress,
        Long brokerId,
        boolean exceptSuccess) throws Exception {
        final ElectMasterRequestHeader electMasterRequestHeader = ElectMasterRequestHeader.ofBrokerTrigger(clusterName, brokerName, brokerId);
        RemotingCommand command = leader.electMaster(electMasterRequestHeader).get(2, TimeUnit.SECONDS);
        ElectMasterResponseHeader header = (ElectMasterResponseHeader) command.readCustomHeader();
        assertEquals(exceptSuccess, ResponseCode.SUCCESS == command.getCode());
    }

    private boolean alterNewInSyncSet(Controller leader, String brokerName, Long masterBrokerId, Integer masterEpoch,
        Set<Long> newSyncStateSet, Integer syncStateSetEpoch) throws Exception {
        final AlterSyncStateSetRequestHeader alterRequest =
            new AlterSyncStateSetRequestHeader(brokerName, masterBrokerId, masterEpoch);
        final RemotingCommand response = leader.alterSyncStateSet(alterRequest, new SyncStateSet(newSyncStateSet, syncStateSetEpoch)).get(10, TimeUnit.SECONDS);
        if (null == response || response.getCode() != ResponseCode.SUCCESS) {
            return false;
        }
        final RemotingCommand getInfoResponse = leader.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName)).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) getInfoResponse.readCustomHeader();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(getInfoResponse.getBody(), SyncStateSet.class);
        assertArrayEquals(syncStateSet.getSyncStateSet().toArray(), newSyncStateSet.toArray());
        assertEquals(syncStateSet.getSyncStateSetEpoch(), syncStateSetEpoch + 1);
        return true;
    }

    public DLedgerController waitLeader(final List<DLedgerController> controllers) throws Exception {
        if (controllers.isEmpty()) {
            return null;
        }
        DLedgerController c1 = controllers.get(0);
        DLedgerController dLedgerController = await().atMost(Duration.ofSeconds(10)).until(() -> {
            String leaderId = c1.getMemberState().getLeaderId();
            if (null == leaderId) {
                return null;
            }
            for (DLedgerController controller : controllers) {
                if (controller.getMemberState().getSelfId().equals(leaderId) && controller.isLeaderState()) {
                    return controller;
                }
            }
            return null;
        }, item -> item != null);
        return dLedgerController;
    }

    public DLedgerController mockMetaData(boolean enableElectUncleanMaster) throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", 30000, 30001, 30002);
        DLedgerController c0 = launchController(group, peers, "n0", enableElectUncleanMaster);
        DLedgerController c1 = launchController(group, peers, "n1", enableElectUncleanMaster);
        DLedgerController c2 = launchController(group, peers, "n2", enableElectUncleanMaster);
        controllers.add(c0);
        controllers.add(c1);
        controllers.add(c2);

        DLedgerController leader = waitLeader(controllers);

        // register
        registerNewBroker(leader, DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L);
        registerNewBroker(leader, DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L);
        registerNewBroker(leader, DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L);
        // try elect
        brokerTryElectMaster(leader, DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, true);
        brokerTryElectMaster(leader, DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, false);
        brokerTryElectMaster(leader, DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, false);
        final RemotingCommand getInfoResponse = leader.getReplicaInfo(new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) getInfoResponse.readCustomHeader();
        assertEquals(1, replicaInfo.getMasterEpoch().intValue());
        assertEquals(DEFAULT_IP[0], replicaInfo.getMasterAddress());
        // Try alter SyncStateSet
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);
        newSyncStateSet.add(2L);
        newSyncStateSet.add(3L);
        assertTrue(alterNewInSyncSet(leader, DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 1));
        return leader;
    }

    public void setBrokerAlivePredicate(DLedgerController controller, Long... deathBroker) {
        controller.setBrokerAlivePredicate((clusterName, brokerName, brokerId) -> {
            for (Long broker : deathBroker) {
                if (broker.equals(brokerId)) {
                    return false;
                }
            }
            return true;
        });
    }

    public void setBrokerElectPolicy(DLedgerController controller, Long... deathBroker) {
        controller.setElectPolicy(new DefaultElectPolicy((clusterName, brokerName, brokerId) -> {
            for (Long broker : deathBroker) {
                if (broker.equals(brokerId)) {
                    return false;
                }
            }
            return true;
        }, null));
    }

    @Test
    public void testElectMaster() throws Exception {
        final DLedgerController leader = mockMetaData(false);
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        setBrokerElectPolicy(leader, 1L);
        final RemotingCommand resp = leader.electMaster(request).get(10, TimeUnit.SECONDS);
        final ElectMasterResponseHeader response = (ElectMasterResponseHeader) resp.readCustomHeader();
        assertEquals(2, response.getMasterEpoch().intValue());
        assertNotEquals(1L, response.getMasterBrokerId().longValue());
        assertNotEquals(DEFAULT_IP[0], response.getMasterAddress());
    }

    @Test
    public void testBrokerLifecycleListener() throws Exception {
        final DLedgerController leader = mockMetaData(false);

        assertTrue(leader.isLeaderState());
        // Mock that master broker has been inactive, and try to elect a new master from sync-state-set
        // But we shut down two controller, so the ElectMasterEvent will be appended to DLedger failed.
        // So the statemachine still keep the stale master's information
        List<DLedgerController> removed = controllers.stream().filter(controller -> controller != leader).collect(Collectors.toList());
        for (DLedgerController dLedgerController : removed) {
            dLedgerController.shutdown();
            controllers.remove(dLedgerController);
        }

        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        setBrokerElectPolicy(leader, 1L);
        Exception exception = null;
        RemotingCommand remotingCommand = null;
        try {
            remotingCommand = leader.electMaster(request).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            exception = e;
        }

        assertTrue(exception != null ||
            remotingCommand != null && remotingCommand.getCode() == ResponseCode.CONTROLLER_NOT_LEADER);

        // Shut down leader controller
        leader.shutdown();
        controllers.remove(leader);
        // Restart two controller
        for (DLedgerController controller : removed) {
            if (controller != leader) {
                ControllerConfig config = controller.getControllerConfig();
                DLedgerController newController = launchController(config.getControllerDLegerGroup(), config.getControllerDLegerPeers(), config.getControllerDLegerSelfId(), false);
                controllers.add(newController);
                newController.startup();
            }
        }
        DLedgerController newLeader = waitLeader(controllers);
        setBrokerAlivePredicate(newLeader, 1L);
        // Check if the statemachine is stale
        final RemotingCommand resp = newLeader.getReplicaInfo(new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).
            get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) resp.readCustomHeader();
        assertEquals(1, replicaInfo.getMasterBrokerId().longValue());
        assertEquals(1, replicaInfo.getMasterEpoch().intValue());

        // Register broker's lifecycle listener
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        newLeader.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            assertEquals(DEFAULT_BROKER_NAME, brokerName);
            atomicBoolean.set(true);
        });
        Awaitility.await().pollDelay(Duration.ofMillis(2000)).until(() -> true);
        assertTrue(atomicBoolean.get());
    }

    @Test
    public void testAllReplicasShutdownAndRestartWithUnEnableElectUnCleanMaster() throws Exception {
        final DLedgerController leader = mockMetaData(false);
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);

        assertTrue(alterNewInSyncSet(leader, DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {1}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        setBrokerElectPolicy(leader, 1L);
        leader.electMaster(electRequest).get(10, TimeUnit.SECONDS);

        final RemotingCommand resp = leader.getReplicaInfo(new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).
            get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) resp.readCustomHeader();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);
        assertEquals(syncStateSet.getSyncStateSet(), newSyncStateSet);
        assertEquals(null, replicaInfo.getMasterAddress());
        assertEquals(2, replicaInfo.getMasterEpoch().intValue());

        // Now, we start broker - id[2]address[127.0.0.1:9001] to try elect, but it was not in syncStateSet, so it will not be elected as master.
        final ElectMasterRequestHeader request1 =
            ElectMasterRequestHeader.ofBrokerTrigger(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 2L);
        final ElectMasterResponseHeader r1 = (ElectMasterResponseHeader) leader.electMaster(request1).get(10, TimeUnit.SECONDS).readCustomHeader();
        assertEquals(null, r1.getMasterBrokerId());
        assertEquals(null, r1.getMasterAddress());

        // Now, we start broker - id[1]address[127.0.0.1:9000] to try elect, it will be elected as master
        setBrokerElectPolicy(leader);
        final ElectMasterRequestHeader request2 =
            ElectMasterRequestHeader.ofBrokerTrigger(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L);
        final ElectMasterResponseHeader r2 = (ElectMasterResponseHeader) leader.electMaster(request2).get(10, TimeUnit.SECONDS).readCustomHeader();
        assertEquals(1L, r2.getMasterBrokerId().longValue());
        assertEquals(DEFAULT_IP[0], r2.getMasterAddress());
        assertEquals(3, r2.getMasterEpoch().intValue());
    }

    @Test
    public void testEnableElectUnCleanMaster() throws Exception {
        final DLedgerController leader = mockMetaData(true);
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);

        assertTrue(alterNewInSyncSet(leader, DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, event if the syncStateSet in statemachine is {DEFAULT_IP[0]}
        // the option {enableElectUncleanMaster = true}, so the controller sill can elect a new master
        final ElectMasterRequestHeader electRequest = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        setBrokerElectPolicy(leader, 1L);
        final CompletableFuture<RemotingCommand> future = leader.electMaster(electRequest);
        future.get(10, TimeUnit.SECONDS);

        final RemotingCommand resp = leader.getReplicaInfo(new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).get(10, TimeUnit.SECONDS);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) resp.readCustomHeader();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);

        final HashSet<Long> newSyncStateSet2 = new HashSet<>();
        newSyncStateSet2.add(replicaInfo.getMasterBrokerId());
        assertEquals(syncStateSet.getSyncStateSet(), newSyncStateSet2);
        assertNotEquals(1L, replicaInfo.getMasterBrokerId().longValue());
        assertNotEquals(DEFAULT_IP[0], replicaInfo.getMasterAddress());
        assertEquals(2, replicaInfo.getMasterEpoch().intValue());
    }

    @Test
    public void testChangeControllerLeader() throws Exception {
        final DLedgerController leader = mockMetaData(false);
        leader.shutdown();
        this.controllers.remove(leader);
        // Wait leader again
        final DLedgerController newLeader = waitLeader(this.controllers);
        assertNotNull(newLeader);

        RemotingCommand response = await().atMost(Duration.ofSeconds(10)).until(() -> {
            final RemotingCommand resp = newLeader.getReplicaInfo(new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).get(10, TimeUnit.SECONDS);
            if (resp.getCode() == ResponseCode.SUCCESS) {

                return resp;
            }
            return null;

        }, item -> item != null);
        final GetReplicaInfoResponseHeader replicaInfo = (GetReplicaInfoResponseHeader) response.readCustomHeader();
        final SyncStateSet syncStateSetResult = RemotingSerializable.decode(response.getBody(), SyncStateSet.class);
        assertEquals(replicaInfo.getMasterAddress(), DEFAULT_IP[0]);
        assertEquals(1, replicaInfo.getMasterEpoch().intValue());

        final HashSet<Long> syncStateSet = new HashSet<>();
        syncStateSet.add(1L);
        syncStateSet.add(2L);
        syncStateSet.add(3L);
        assertEquals(syncStateSetResult.getSyncStateSet(), syncStateSet);
    }
}
