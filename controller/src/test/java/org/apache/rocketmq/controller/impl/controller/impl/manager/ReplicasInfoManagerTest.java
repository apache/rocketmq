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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.ElectMasterEvent;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.RegisterBrokerToControllerResponseHeader;
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

    private ControllerConfig config;

    private ElectPolicy electPolicy;

    @Before
    public void init() {
        this.electPolicy = new DefaultElectPolicy((clusterName, brokerAddr) -> true, null);
        this.config = new ControllerConfig();
        this.config.setEnableElectUncleanMaster(false);
        this.config.setScanNotActiveBrokerInterval(300000000);
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

    public void registerNewBroker(String clusterName, String brokerName, String brokerAddress,
        long exceptBrokerId, String exceptMasterAddress) {
        // Register new broker
        final RegisterBrokerToControllerRequestHeader registerRequest =
            new RegisterBrokerToControllerRequestHeader(clusterName, brokerName, brokerAddress);
        final ControllerResult<RegisterBrokerToControllerResponseHeader> registerResult = this.replicasInfoManager.registerBroker(registerRequest, (s, v) -> true);
        apply(registerResult.getEvents());
        // check response
        assertEquals(ResponseCode.SUCCESS, registerResult.getResponseCode());
        assertEquals(exceptBrokerId, registerResult.getResponse().getBrokerId());
        assertEquals(exceptMasterAddress, registerResult.getResponse().getMasterAddress());
        // check it in state machine
        final GetReplicaInfoResponseHeader replicaInfo = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName, brokerAddress)).getResponse();
        assertEquals(exceptBrokerId, replicaInfo.getBrokerId());
    }

    public void brokerElectMaster(String clusterName, long brokerId, String brokerName, String brokerAddress,
        boolean isFirstTryElect) {

        final GetReplicaInfoResponseHeader replicaInfoBefore = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName, brokerAddress)).getResponse();
        byte[] body = this.replicasInfoManager.getSyncStateData(Arrays.asList(brokerName)).getBody();
        BrokerReplicasInfo syncStateDataBefore = RemotingSerializable.decode(body, BrokerReplicasInfo.class);
        // Try elect itself as a master
        ElectMasterRequestHeader requestHeader = ElectMasterRequestHeader.ofBrokerTrigger(clusterName, brokerName, brokerAddress);
        final ControllerResult<ElectMasterResponseHeader> result = this.replicasInfoManager.electMaster(requestHeader, this.electPolicy);
        apply(result.getEvents());

        final GetReplicaInfoResponseHeader replicaInfoAfter = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName, brokerAddress)).getResponse();
        final ElectMasterResponseHeader response = result.getResponse();

        if (isFirstTryElect) {
            // it should be elected
            // check response
            assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
            assertEquals(1, response.getMasterEpoch());
            assertEquals(1, response.getSyncStateSetEpoch());
            assertEquals(brokerAddress, response.getMasterAddress());
            // check it in state machine
            assertEquals(brokerAddress, replicaInfoAfter.getMasterAddress());
            assertEquals(1, replicaInfoAfter.getMasterEpoch());
            assertEquals(brokerId, replicaInfoAfter.getBrokerId());
        } else {

            // failed because now master still exist
            if (StringUtils.isNotEmpty(replicaInfoBefore.getMasterAddress())) {
                assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, result.getResponseCode());
                assertEquals(replicaInfoBefore.getMasterAddress(), response.getMasterAddress());
                assertEquals(replicaInfoBefore.getMasterEpoch(), response.getMasterEpoch());
                assertEquals(brokerId, replicaInfoAfter.getBrokerId());
                return;
            }
            if (syncStateDataBefore.getReplicasInfoTable().containsKey(brokerAddress) || this.config.isEnableElectUncleanMaster()) {
                // can be elected successfully
                assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
                assertEquals(MixAll.MASTER_ID, replicaInfoAfter.getBrokerId());
                assertEquals(brokerId, replicaInfoAfter.getBrokerId());
            } else {
                // failed because elect nothing
                assertEquals(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, result.getResponseCode());
            }
        }
    }

    @Test
    public void testRegisterNewBroker() {
        final RegisterBrokerToControllerRequestHeader registerRequest =
            new RegisterBrokerToControllerRequestHeader("default", "brokerName-a", "127.0.0.1:9000");
        final ControllerResult<RegisterBrokerToControllerResponseHeader> registerResult = this.replicasInfoManager.registerBroker(registerRequest, (s, v) -> true);
        apply(registerResult.getEvents());
        final RegisterBrokerToControllerRequestHeader registerRequest0 =
            new RegisterBrokerToControllerRequestHeader("default", "brokerName-a", "127.0.0.1:9001");
        final ControllerResult<RegisterBrokerToControllerResponseHeader> registerResult0 = this.replicasInfoManager.registerBroker(registerRequest0, (s, v) -> true);
        apply(registerResult0.getEvents());
        final ElectMasterRequestHeader electMasterRequest = ElectMasterRequestHeader.ofBrokerTrigger("default", "brokerName-a", "127.0.0.1:9000");
        ControllerResult<ElectMasterResponseHeader> electMasterResponseHeaderControllerResult = this.replicasInfoManager.electMaster(electMasterRequest, new DefaultElectPolicy());
        apply(electMasterResponseHeaderControllerResult.getEvents());
        final ControllerResult<GetReplicaInfoResponseHeader> getInfoResult = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader("brokerName-a"));
        final GetReplicaInfoResponseHeader replicaInfo = getInfoResult.getResponse();
        assertEquals("127.0.0.1:9000", replicaInfo.getMasterAddress());
        assertEquals(1, replicaInfo.getMasterEpoch());
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        newSyncStateSet.add("127.0.0.1:9001");
        alterNewInSyncSet("brokerName-a", "127.0.0.1:9000", 1, newSyncStateSet, 1);
        final RegisterBrokerToControllerRequestHeader registerRequest1 =
            new RegisterBrokerToControllerRequestHeader("default", "brokerName-a", "127.0.0.1:9002");
        final ControllerResult<RegisterBrokerToControllerResponseHeader> registerResult1 = this.replicasInfoManager.registerBroker(registerRequest1, (s, v) -> StringUtils.equals(v, "127.0.0.1:9001"));
        apply(registerResult1.getEvents());
        assertEquals(3, registerResult1.getResponse().getBrokerId());
        assertEquals("", registerResult1.getResponse().getMasterAddress());
        ElectPolicy electPolicy1 = new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"),null);
        final ElectMasterRequestHeader electMasterRequest1 = ElectMasterRequestHeader.ofBrokerTrigger("default", "brokerName-a", "127.0.0.1:9002");
        ControllerResult<ElectMasterResponseHeader> electMasterResponseHeaderControllerResult1 = this.replicasInfoManager.electMaster(electMasterRequest1, electPolicy1);
        apply(electMasterResponseHeaderControllerResult1.getEvents());
        final ControllerResult<GetReplicaInfoResponseHeader> getInfoResult0 = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader("brokerName-a"));
        final GetReplicaInfoResponseHeader replicaInfo0 = getInfoResult0.getResponse();
        assertEquals(replicaInfo0.getMasterAddress(), "127.0.0.1:9001");
        assertTrue(replicaInfo0.getMasterAddress().equals("127.0.0.1:9001") || replicaInfo0.getMasterAddress().equals("127.0.0.1:9002"));
        assertEquals(replicaInfo0.getMasterEpoch(), 2);
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
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9000", 1L, "");
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9001", 2L, "");
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9002", 3L, "");
        brokerElectMaster("cluster1", 1L, "broker1", "127.0.0.1:9000", true);
        brokerElectMaster("cluster1", 2L, "broker1", "127.0.0.1:9001", false);
        brokerElectMaster("cluster1", 3L, "broker1", "127.0.0.1:9002", false);
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        newSyncStateSet.add("127.0.0.1:9001");
        newSyncStateSet.add("127.0.0.1:9002");
        assertTrue(alterNewInSyncSet("broker1", "127.0.0.1:9000", 1, newSyncStateSet, 1));
    }

    public void mockHeartbeatDataMasterStillAlive() {
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9000", 1L, 10000000000L, null,
            1, 1L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            1, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherEpoch() {
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9000", 1L, -10000L, null,
            1, 3L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            0, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherOffset() {
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9000", 1L, -10000L, null,
            1, 3L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            1, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherPriority() {
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9000", 1L, -10000L, null,
            1, 3L, -1L, 3);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9001", 1L, 10000000000L, null,
            1, 3L, -1L, 2);
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:9002", 1L, 10000000000L, null,
            1, 3L, -1L, 1);
    }

    @Test
    public void testRegisterBrokerSuccess() {
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9000", 1L, "");
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9001", 2L, "");
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9002", 3L, "");
        brokerElectMaster("cluster1", 1L, "broker1", "127.0.0.1:9000", true);
        brokerElectMaster("cluster1", 2L, "broker1", "127.0.0.1:9001", false);
        brokerElectMaster("cluster1", 3L, "broker1", "127.0.0.1:9002", false);
    }

    @Test
    public void testRegisterWithMasterExistResp() {
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9000", 1L, "");
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9001", 2L, "");
        brokerElectMaster("cluster1", 1L, "broker1", "127.0.0.1:9000", true);
        brokerElectMaster("cluster1", 2L, "broker1", "127.0.0.1:9001", false);
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9002", 3L, "127.0.0.1:9000");
        brokerElectMaster("cluster1", 3L, "broker1", "127.0.0.1:9002", false);
    }

    @Test
    public void testElectMasterOldMasterStillAlive() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataMasterStillAlive();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, cResult.getResponseCode());
    }

    @Test
    public void testElectMasterPreferHigherEpoch() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherEpoch();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getMasterAddress().isEmpty());
        assertEquals("127.0.0.1:9001", response.getMasterAddress());
    }

    @Test
    public void testElectMasterPreferHigherOffsetWhenEpochEquals() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherOffset();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getMasterAddress().isEmpty());
        assertEquals("127.0.0.1:9002", response.getMasterAddress());
    }

    @Test
    public void testElectMasterPreferHigherPriorityWhenEpochAndOffsetEquals() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherPriority();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getMasterAddress().isEmpty());
        assertEquals("127.0.0.1:9002", response.getMasterAddress());
    }

    @Test
    public void testElectMaster() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger("broker1");
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"), null));
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getMasterAddress().isEmpty());
        assertNotEquals(response.getMasterAddress(), "127.0.0.1:9000");

        apply(cResult.getEvents());

        final Set<String> brokerSet = new HashSet<>();
        brokerSet.add("127.0.0.1:9000");
        brokerSet.add("127.0.0.1:9001");
        brokerSet.add("127.0.0.1:9002");
        assertTrue(alterNewInSyncSet("broker1", response.getMasterAddress(), response.getMasterEpoch(), brokerSet, response.getSyncStateSetEpoch()));

        // test admin try to elect a assignedMaster, but it isn't alive
        final ElectMasterRequestHeader assignRequest = ElectMasterRequestHeader.ofAdminTrigger("cluster1", "broker1", "127.0.0.1:9000");
        final ControllerResult<ElectMasterResponseHeader> cResult1 = this.replicasInfoManager.electMaster(assignRequest,
            new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"), null));

        assertEquals(cResult1.getResponseCode(), ResponseCode.CONTROLLER_ELECT_MASTER_FAILED);

        // test admin try to elect a assignedMaster but old master still alive, and the old master is equals to assignedMaster
        final ElectMasterRequestHeader assignRequest1 = ElectMasterRequestHeader.ofAdminTrigger("cluster1", "broker1", response.getMasterAddress());
        final ControllerResult<ElectMasterResponseHeader> cResult2 = this.replicasInfoManager.electMaster(assignRequest1,
            new DefaultElectPolicy((clusterName, brokerAddress) -> true, null));
        assertEquals(cResult2.getResponseCode(), ResponseCode.CONTROLLER_MASTER_STILL_EXIST);

        // admin successful elect a assignedMaster.
        final ElectMasterRequestHeader assignRequest2 = ElectMasterRequestHeader.ofAdminTrigger("cluster1", "broker1", "127.0.0.1:9000");
        final ControllerResult<ElectMasterResponseHeader> cResult3 = this.replicasInfoManager.electMaster(assignRequest2,
            new DefaultElectPolicy((clusterName, brokerAddress) -> !brokerAddress.equals(response.getMasterAddress()), null));
        assertEquals(cResult3.getResponseCode(), ResponseCode.SUCCESS);

        final ElectMasterResponseHeader response3 = cResult3.getResponse();
        assertEquals(response3.getMasterAddress(), "127.0.0.1:9000");
        assertEquals(response3.getMasterEpoch(), 3);
    }

    @Test
    public void testAllReplicasShutdownAndRestart() {
        mockMetaData();
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        assertTrue(alterNewInSyncSet("broker1", "127.0.0.1:9000", 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {"127.0.0.1:9000"}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = ElectMasterRequestHeader.ofControllerTrigger("broker1");
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
