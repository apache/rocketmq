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
package org.apache.rocketmq.controller.impl.controller.impl.manager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.ElectMasterEvent;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ReplicasInfoManagerTest {
    private ReplicasInfoManager replicasInfoManager;

    private DefaultBrokerHeartbeatManager heartbeatManager;

    @Before
    public void init() {
        final ControllerConfig config = new ControllerConfig();
        config.setEnableElectUncleanMaster(false);
        config.setScanNotActiveBrokerInterval(300000000);
        this.replicasInfoManager = new ReplicasInfoManager(config);
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(config);
        this.heartbeatManager.start();
    }

    @After
    public void destroy() {
        this.replicasInfoManager = null;
        this.heartbeatManager.shutdown();
        this.heartbeatManager = null;
    }

    public boolean registerNewBroker(String clusterName, String brokerName, String brokerAddress,
        boolean isFirstRegisteredBroker) {
        // Register new broker
        final RegisterBrokerToControllerRequestHeader registerRequest =
            new RegisterBrokerToControllerRequestHeader(clusterName, brokerName, brokerAddress);
        final ControllerResult<RegisterBrokerToControllerResponseHeader> registerResult = this.replicasInfoManager.registerBroker(registerRequest);
        apply(registerResult.getEvents());

        if (isFirstRegisteredBroker) {
            final ControllerResult<GetReplicaInfoResponseHeader> getInfoResult = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));
            final GetReplicaInfoResponseHeader replicaInfo = getInfoResult.getResponse();
            assertEquals(replicaInfo.getMasterAddress(), brokerAddress);
            assertEquals(replicaInfo.getMasterEpoch(), 1);
        } else {
            final RegisterBrokerToControllerResponseHeader response = registerResult.getResponse();
            assertTrue(response.getBrokerId() > 0);
        }
        return true;
    }

    private boolean alterNewInSyncSet(String brokerName, String masterAddress, int masterEpoch,
        Set<String> newSyncStateSet, int syncStateSetEpoch) {
        final AlterSyncStateSetRequestHeader alterRequest =
            new AlterSyncStateSetRequestHeader(brokerName, masterAddress, masterEpoch);
        final ControllerResult<AlterSyncStateSetResponseHeader> result = this.replicasInfoManager.alterSyncStateSet(alterRequest, new SyncStateSet(newSyncStateSet, syncStateSetEpoch), (va1, va2) -> true);
        apply(result.getEvents());

        final ControllerResult<GetReplicaInfoResponseHeader> resp = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));
        final GetReplicaInfoResponseHeader replicaInfo = resp.getResponse();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);

        assertArrayEquals(syncStateSet.getSyncStateSet().toArray(), newSyncStateSet.toArray());
        assertEquals(syncStateSet.getSyncStateSetEpoch(), syncStateSetEpoch + 1);
        return true;
    }

    private void apply(final List<EventMessage> events) {
        for (EventMessage event : events) {
            this.replicasInfoManager.applyEvent(event);
        }
    }

    public void mockMetaData() {
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9000", true);
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9001", false);
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9002", false);
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        newSyncStateSet.add("127.0.0.1:9001");
        newSyncStateSet.add("127.0.0.1:9002");
        assertTrue(alterNewInSyncSet("broker1", "127.0.0.1:9000", 1, newSyncStateSet, 1));
    }

    public void mockHeartbeatDataMasterStillAlive() {
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9000", 1L, 10000000000L, null,
            1, 3L);
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 2L);
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            1, 3L);
    }

    public void mockHeartbeatDataHigherEpoch() {
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9000", 1L, -10000L, null,
            1, 3L);
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 2L);
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            0, 3L);
    }

    public void mockHeartbeatDataHigherOffset() {
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9000", 1L, -10000L, null,
            1, 3L);
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 2L);
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            1, 3L);
    }

    @Test
    public void testElectMasterOldMasterStillAlive() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataMasterStillAlive();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        assertEquals(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, cResult.getResponseCode());
    }

    @Test
    public void testElectMasterPreferHigherEpoch() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherEpoch();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        System.out.println(cResult.getResponseCode());
        final ElectMasterResponseHeader response = cResult.getResponse();
        System.out.println(response);
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertEquals("127.0.0.1:9001", response.getNewMasterAddress());
    }

    @Test
    public void testElectMasterPreferHigherOffsetWhenEpochEquals() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherOffset();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        System.out.println(cResult.getResponseCode());
        final ElectMasterResponseHeader response = cResult.getResponse();
        System.out.println(response);
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertEquals("127.0.0.1:9002", response.getNewMasterAddress());
    }

    @Test
    public void testElectMaster() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"), null));
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertNotEquals(response.getNewMasterAddress(), "127.0.0.1:9000");

        final Set<String> brokerSet = new HashSet<>();
        brokerSet.add("127.0.0.1:9000");
        brokerSet.add("127.0.0.1:9001");
        brokerSet.add("127.0.0.1:9002");
        final ElectMasterRequestHeader assignRequest = new ElectMasterRequestHeader("cluster1", "broker1", "127.0.0.1:9000");
        final ControllerResult<ElectMasterResponseHeader> cResult1 = this.replicasInfoManager.electMaster(assignRequest,
            new DefaultElectPolicy((clusterName, brokerAddress) -> brokerAddress.contains("127.0.0.1:9000"), null));
        assertEquals(cResult1.getResponseCode(), ResponseCode.CONTROLLER_ELECT_MASTER_FAILED);

        final ElectMasterRequestHeader assignRequest1 = new ElectMasterRequestHeader("cluster1", "broker1", "127.0.0.1:9001");
        final ControllerResult<ElectMasterResponseHeader> cResult2 = this.replicasInfoManager.electMaster(assignRequest1,
            new DefaultElectPolicy((clusterName, brokerAddress) -> brokerAddress.equals("127.0.0.1:9000"), null));
        assertEquals(cResult2.getResponseCode(), ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE);

        final ElectMasterRequestHeader assignRequest2 = new ElectMasterRequestHeader("cluster1", "broker1", "127.0.0.1:9001");
        final ControllerResult<ElectMasterResponseHeader> cResult3 = this.replicasInfoManager.electMaster(assignRequest2,
            new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"), null));
        assertEquals(cResult3.getResponseCode(), ResponseCode.SUCCESS);
        final ElectMasterResponseHeader response3 = cResult3.getResponse();
        assertEquals(response3.getNewMasterAddress(), "127.0.0.1:9001");
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertNotEquals(response.getNewMasterAddress(), "127.0.0.1:9000");

    }

    @Test
    public void testAllReplicasShutdownAndRestart() {
        mockMetaData();
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        assertTrue(alterNewInSyncSet("broker1", "127.0.0.1:9000", 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {"127.0.0.1:9000"}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = new ElectMasterRequestHeader("broker1");
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(electRequest,
            new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"), null));
        final List<EventMessage> events = cResult.getEvents();
        assertEquals(events.size(), 1);
        final ElectMasterEvent event = (ElectMasterEvent) events.get(0);
        assertFalse(event.getNewMasterElected());

        apply(cResult.getEvents());

        final GetReplicaInfoResponseHeader replicaInfo = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).getResponse();
        assertEquals(replicaInfo.getMasterAddress(), "");
        assertEquals(replicaInfo.getMasterEpoch(), 2);
    }

    @Test
    public void testCleanBrokerData() {
        mockMetaData();
        CleanControllerBrokerDataRequestHeader header1 = new CleanControllerBrokerDataRequestHeader("cluster1", "broker1", "127.0.0.1:9000");
        ControllerResult<Void> result1 = this.replicasInfoManager.cleanBrokerData(header1, (cluster, brokerAddr) -> true);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result1.getResponseCode());

        CleanControllerBrokerDataRequestHeader header2 = new CleanControllerBrokerDataRequestHeader("cluster1", "broker1", null);
        ControllerResult<Void> result2 = this.replicasInfoManager.cleanBrokerData(header2, (cluster, brokerAddr) -> true);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result2.getResponseCode());
        assertEquals("Broker broker1 is still alive, clean up failure", result2.getRemark());

        CleanControllerBrokerDataRequestHeader header3 = new CleanControllerBrokerDataRequestHeader("cluster1", "broker1", "127.0.0.1:9000");
        ControllerResult<Void> result3 = this.replicasInfoManager.cleanBrokerData(header3, (cluster, brokerAddr) -> false);
        assertEquals(ResponseCode.SUCCESS, result3.getResponseCode());

        CleanControllerBrokerDataRequestHeader header4 = new CleanControllerBrokerDataRequestHeader("cluster1", "broker1", "127.0.0.1:9000;127.0.0.1:9001;127.0.0.1:9002");
        ControllerResult<Void> result4 = this.replicasInfoManager.cleanBrokerData(header4, (cluster, brokerAddr) -> false);
        assertEquals(ResponseCode.SUCCESS, result4.getResponseCode());

        CleanControllerBrokerDataRequestHeader header5 = new CleanControllerBrokerDataRequestHeader("cluster1", "broker12", "127.0.0.1:9000;127.0.0.1:9001;127.0.0.1:9002", true);
        ControllerResult<Void> result5 = this.replicasInfoManager.cleanBrokerData(header5, (cluster, brokerAddr) -> false);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result5.getResponseCode());
        assertEquals("Broker broker12 is not existed,clean broker data failure.", result5.getRemark());

        CleanControllerBrokerDataRequestHeader header6 = new CleanControllerBrokerDataRequestHeader(null, "broker12", "127.0.0.1:9000;127.0.0.1:9001;127.0.0.1:9002", true);
        ControllerResult<Void> result6 = this.replicasInfoManager.cleanBrokerData(header6, (cluster, brokerAddr) -> cluster != null);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result6.getResponseCode());

        CleanControllerBrokerDataRequestHeader header7 = new CleanControllerBrokerDataRequestHeader(null, "broker1", "127.0.0.1:9000;127.0.0.1:9001;127.0.0.1:9002", true);
        ControllerResult<Void> result7 = this.replicasInfoManager.cleanBrokerData(header7, (cluster, brokerAddr) -> false);
        assertEquals(ResponseCode.SUCCESS, result7.getResponseCode());

    }
}
