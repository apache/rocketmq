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
package org.apache.rocketmq.controller.impl.manager;

import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.ElectMasterEvent;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.heartbeat.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_BROKER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_CLUSTER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_IP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReplicasInfoManagerTest {
    private ReplicasInfoManager replicasInfoManager;

    private DefaultBrokerHeartbeatManager heartbeatManager;

    private ControllerConfig config;

    @Before
    public void init() {
        this.config = new ControllerConfig();
        this.config.setEnableElectUncleanMaster(false);
        this.config.setScanNotActiveBrokerInterval(300000000);
        this.replicasInfoManager = new ReplicasInfoManager(config);
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(config);
        this.heartbeatManager.initialize();
        this.heartbeatManager.start();
    }

    @After
    public void destroy() {
        this.replicasInfoManager = null;
        this.heartbeatManager.shutdown();
        this.heartbeatManager = null;
    }

    private BrokerReplicasInfo.ReplicasInfo getReplicasInfo(String brokerName) {
        ControllerResult<Void> syncStateData = this.replicasInfoManager.getSyncStateData(Arrays.asList(brokerName), (a, b, c) -> true);
        BrokerReplicasInfo replicasInfo = RemotingSerializable.decode(syncStateData.getBody(), BrokerReplicasInfo.class);
        return replicasInfo.getReplicasInfoTable().get(brokerName);
    }

    public void registerNewBroker(String clusterName, String brokerName, String brokerAddress,
        Long exceptBrokerId, Long exceptMasterBrokerId) {

        // Get next brokerId
        final GetNextBrokerIdRequestHeader getNextBrokerIdRequestHeader = new GetNextBrokerIdRequestHeader(clusterName, brokerName);
        final ControllerResult<GetNextBrokerIdResponseHeader> nextBrokerIdResult = this.replicasInfoManager.getNextBrokerId(getNextBrokerIdRequestHeader);
        Long nextBrokerId = nextBrokerIdResult.getResponse().getNextBrokerId();
        String registerCheckCode = brokerAddress + ";" + System.currentTimeMillis();

        // check response
        assertEquals(ResponseCode.SUCCESS, nextBrokerIdResult.getResponseCode());
        assertEquals(exceptBrokerId, nextBrokerId);

        // Apply brokerId
        final ApplyBrokerIdRequestHeader applyBrokerIdRequestHeader = new ApplyBrokerIdRequestHeader(clusterName, brokerName, nextBrokerId, registerCheckCode);
        final ControllerResult<ApplyBrokerIdResponseHeader> applyBrokerIdResult = this.replicasInfoManager.applyBrokerId(applyBrokerIdRequestHeader);
        apply(applyBrokerIdResult.getEvents());

        // check response
        assertEquals(ResponseCode.SUCCESS, applyBrokerIdResult.getResponseCode());

        // check it in state machine
        BrokerReplicasInfo.ReplicasInfo replicasInfo = getReplicasInfo(brokerName);
        BrokerReplicasInfo.ReplicaIdentity replicaIdentity = replicasInfo.getNotInSyncReplicas().stream().filter(x -> x.getBrokerId().equals(nextBrokerId)).findFirst().get();
        assertNotNull(replicaIdentity);
        assertEquals(brokerName, replicaIdentity.getBrokerName());
        assertEquals(exceptBrokerId, replicaIdentity.getBrokerId());
        assertEquals(brokerAddress, replicaIdentity.getBrokerAddress());

        // register success
        final RegisterBrokerToControllerRequestHeader registerBrokerToControllerRequestHeader = new RegisterBrokerToControllerRequestHeader(clusterName, brokerName, exceptBrokerId, brokerAddress);
        ControllerResult<RegisterBrokerToControllerResponseHeader> registerSuccessResult = this.replicasInfoManager.registerBroker(registerBrokerToControllerRequestHeader, (a, b, c) -> true);
        apply(registerSuccessResult.getEvents());

        // check response
        assertEquals(ResponseCode.SUCCESS, registerSuccessResult.getResponseCode());
        assertEquals(exceptMasterBrokerId, registerSuccessResult.getResponse().getMasterBrokerId());

    }

    public void brokerElectMaster(String clusterName, Long brokerId, String brokerName, String brokerAddress,
        boolean isFirstTryElect, boolean expectToBeElected) {
        this.brokerElectMaster(clusterName, brokerId, brokerName, brokerAddress, isFirstTryElect, expectToBeElected, (a, b, c) -> true);
    }

    public void brokerElectMaster(String clusterName, Long brokerId, String brokerName, String brokerAddress,
        boolean isFirstTryElect, boolean expectToBeElected, BrokerValidPredicate validPredicate) {

        final GetReplicaInfoResponseHeader replicaInfoBefore = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName)).getResponse();
        BrokerReplicasInfo.ReplicasInfo syncStateSetInfo = getReplicasInfo(brokerName);
        // Try elect itself as a master
        ElectMasterRequestHeader requestHeader = ElectMasterRequestHeader.ofBrokerTrigger(clusterName, brokerName, brokerId);
        final ControllerResult<ElectMasterResponseHeader> result = this.replicasInfoManager.electMaster(requestHeader, new DefaultElectPolicy(validPredicate, null));
        apply(result.getEvents());

        final GetReplicaInfoResponseHeader replicaInfoAfter = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName)).getResponse();
        final ElectMasterResponseHeader response = result.getResponse();

        if (isFirstTryElect) {
            // it should be elected
            // check response
            assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
            assertEquals(1, response.getMasterEpoch().intValue());
            assertEquals(1, response.getSyncStateSetEpoch().intValue());
            assertEquals(brokerAddress, response.getMasterAddress());
            assertEquals(brokerId, response.getMasterBrokerId());
            // check it in state machine
            assertEquals(brokerAddress, replicaInfoAfter.getMasterAddress());
            assertEquals(1, replicaInfoAfter.getMasterEpoch().intValue());
            assertEquals(brokerId, replicaInfoAfter.getMasterBrokerId());
        } else {

            // failed because now master still exist
            if (replicaInfoBefore.getMasterBrokerId() != null && validPredicate.check(clusterName, brokerName, replicaInfoBefore.getMasterBrokerId())) {
                assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, result.getResponseCode());
                assertEquals(replicaInfoBefore.getMasterAddress(), response.getMasterAddress());
                assertEquals(replicaInfoBefore.getMasterEpoch(), response.getMasterEpoch());
                assertEquals(replicaInfoBefore.getMasterBrokerId(), response.getMasterBrokerId());
                assertEquals(replicaInfoBefore.getMasterBrokerId(), replicaInfoAfter.getMasterBrokerId());
                return;
            }
            if (syncStateSetInfo.isExistInSync(brokerName, brokerId, brokerAddress) || this.config.isEnableElectUncleanMaster()) {
                // a new master can be elected successfully
                assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
                assertEquals(replicaInfoBefore.getMasterEpoch() + 1, replicaInfoAfter.getMasterEpoch().intValue());

                if (expectToBeElected) {
                    assertEquals(brokerAddress, response.getMasterAddress());
                    assertEquals(brokerId, response.getMasterBrokerId());
                    assertEquals(brokerAddress, replicaInfoAfter.getMasterAddress());
                    assertEquals(brokerId, replicaInfoAfter.getMasterBrokerId());
                }

            } else {
                // failed because elect nothing
                assertEquals(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, result.getResponseCode());
            }
        }
    }

    private boolean alterNewInSyncSet(String brokerName, Long brokerId, Integer masterEpoch,
        Set<Long> newSyncStateSet, Integer syncStateSetEpoch) {
        final AlterSyncStateSetRequestHeader alterRequest =
            new AlterSyncStateSetRequestHeader(brokerName, brokerId, masterEpoch);
        final ControllerResult<AlterSyncStateSetResponseHeader> result = this.replicasInfoManager.alterSyncStateSet(alterRequest,
            new SyncStateSet(newSyncStateSet, syncStateSetEpoch), (cluster, brokerName1, brokerId1) -> true);
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
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, null);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, null);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, null);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 1L, DEFAULT_BROKER_NAME, DEFAULT_IP[0], true, true);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 2L, DEFAULT_BROKER_NAME, DEFAULT_IP[1], false, false);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 3L, DEFAULT_BROKER_NAME, DEFAULT_IP[2], false, false);
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);
        newSyncStateSet.add(2L);
        newSyncStateSet.add(3L);
        assertTrue(alterNewInSyncSet(DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 1));
    }

    public void mockHeartbeatDataMasterStillAlive() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, 10000000000L, null,
            1, 1L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, 10000000000L, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, 10000000000L, null,
            1, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherEpoch() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, -10000L, null,
            1, 3L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, 10000000000L, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, 10000000000L, null,
            0, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherOffset() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, -10000L, null,
            1, 3L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, 10000000000L, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, 10000000000L, null,
            1, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherPriority() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, -10000L, null,
            1, 3L, -1L, 3);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, 10000000000L, null,
            1, 3L, -1L, 2);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, 10000000000L, null,
            1, 3L, -1L, 1);
    }

    @Test
    public void testRegisterBrokerSuccess() {
        mockMetaData();

        BrokerReplicasInfo.ReplicasInfo replicasInfo = getReplicasInfo(DEFAULT_BROKER_NAME);
        assertEquals(1L, replicasInfo.getMasterBrokerId().longValue());
        assertEquals(DEFAULT_IP[0], replicasInfo.getMasterAddress());
        assertEquals(1, replicasInfo.getMasterEpoch());
        assertEquals(2, replicasInfo.getSyncStateSetEpoch());
        assertEquals(3, replicasInfo.getInSyncReplicas().size());
        assertEquals(0, replicasInfo.getNotInSyncReplicas().size());
    }

    @Test
    public void testRegisterWithMasterExistResp() {
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, null);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, null);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 1L, DEFAULT_BROKER_NAME, DEFAULT_IP[0], true, true);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 2L, DEFAULT_BROKER_NAME, DEFAULT_IP[1], false, false);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, 1L);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 3L, DEFAULT_BROKER_NAME, DEFAULT_IP[2], false, false);

        BrokerReplicasInfo.ReplicasInfo replicasInfo = getReplicasInfo(DEFAULT_BROKER_NAME);
        assertEquals(1L, replicasInfo.getMasterBrokerId().longValue());
        assertEquals(DEFAULT_IP[0], replicasInfo.getMasterAddress());
        assertEquals(1, replicasInfo.getMasterEpoch());
        assertEquals(1, replicasInfo.getSyncStateSetEpoch());
        assertEquals(1, replicasInfo.getInSyncReplicas().size());
        assertEquals(2, replicasInfo.getNotInSyncReplicas().size());
    }

    @Test
    public void testRegisterWithOldMasterInactive() {
        mockMetaData();
        // If now only broker-3 alive, it will be elected to be a new master
        brokerElectMaster(DEFAULT_CLUSTER_NAME, 3L, DEFAULT_BROKER_NAME, DEFAULT_IP[2], false, true, (a, b, c) -> c.equals(3L));

        // Check in statemachine
        BrokerReplicasInfo.ReplicasInfo replicasInfo = getReplicasInfo(DEFAULT_BROKER_NAME);
        assertEquals(3L, replicasInfo.getMasterBrokerId().longValue());
        assertEquals(DEFAULT_IP[2], replicasInfo.getMasterAddress());
        assertEquals(2, replicasInfo.getMasterEpoch());
        assertEquals(3, replicasInfo.getSyncStateSetEpoch());
        assertEquals(1, replicasInfo.getInSyncReplicas().size());
        assertEquals(2, replicasInfo.getNotInSyncReplicas().size());
    }

    @Test
    public void testElectMasterOldMasterStillAlive() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataMasterStillAlive();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, cResult.getResponseCode());
    }

    @Test
    public void testElectMasterPreferHigherEpoch() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherEpoch();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(DEFAULT_IP[1], response.getMasterAddress());
        assertEquals(2L, response.getMasterBrokerId().longValue());
        assertEquals(2, response.getMasterEpoch().intValue());
    }

    @Test
    public void testElectMasterPreferHigherOffsetWhenEpochEquals() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherOffset();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(DEFAULT_IP[2], response.getMasterAddress());
        assertEquals(3L, response.getMasterBrokerId().longValue());
        assertEquals(2, response.getMasterEpoch().intValue());
    }

    @Test
    public void testElectMasterPreferHigherPriorityWhenEpochAndOffsetEquals() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader(DEFAULT_BROKER_NAME);
        ElectPolicy electPolicy = new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo);
        mockHeartbeatDataHigherPriority();
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            electPolicy);
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(DEFAULT_IP[2], response.getMasterAddress());
        assertEquals(3L, response.getMasterBrokerId().longValue());
        assertEquals(2, response.getMasterEpoch().intValue());
    }

    @Test
    public void testElectMaster() {
        mockMetaData();
        final ElectMasterRequestHeader request = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request,
            new DefaultElectPolicy((cluster, brokerName, brokerId) -> !brokerId.equals(1L), null));
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(2, response.getMasterEpoch().intValue());
        assertNotEquals(1L, response.getMasterBrokerId().longValue());
        assertNotEquals(DEFAULT_IP[0], response.getMasterAddress());
        apply(cResult.getEvents());

        final Set<Long> brokerSet = new HashSet<>();
        brokerSet.add(1L);
        brokerSet.add(2L);
        brokerSet.add(3L);
        assertTrue(alterNewInSyncSet(DEFAULT_BROKER_NAME, response.getMasterBrokerId(), response.getMasterEpoch(), brokerSet, response.getSyncStateSetEpoch()));

        // test admin try to elect a assignedMaster, but it isn't alive
        final ElectMasterRequestHeader assignRequest = ElectMasterRequestHeader.ofAdminTrigger(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L);
        final ControllerResult<ElectMasterResponseHeader> cResult1 = this.replicasInfoManager.electMaster(assignRequest,
            new DefaultElectPolicy((cluster, brokerName, brokerId) -> !brokerId.equals(1L), null));

        assertEquals(cResult1.getResponseCode(), ResponseCode.CONTROLLER_ELECT_MASTER_FAILED);

        // test admin try to elect a assignedMaster but old master still alive, and the old master is equals to assignedMaster
        final ElectMasterRequestHeader assignRequest1 = ElectMasterRequestHeader.ofAdminTrigger(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, response.getMasterBrokerId());
        final ControllerResult<ElectMasterResponseHeader> cResult2 = this.replicasInfoManager.electMaster(assignRequest1,
            new DefaultElectPolicy((cluster, brokerName, brokerId) -> true, null));
        assertEquals(cResult2.getResponseCode(), ResponseCode.CONTROLLER_MASTER_STILL_EXIST);

        // admin successful elect a assignedMaster.
        final ElectMasterRequestHeader assignRequest2 = ElectMasterRequestHeader.ofAdminTrigger(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L);
        final ControllerResult<ElectMasterResponseHeader> cResult3 = this.replicasInfoManager.electMaster(assignRequest2,
            new DefaultElectPolicy((cluster, brokerName, brokerId) -> !brokerId.equals(response.getMasterBrokerId()), null));
        assertEquals(cResult3.getResponseCode(), ResponseCode.SUCCESS);

        final ElectMasterResponseHeader response3 = cResult3.getResponse();
        assertEquals(1L, response3.getMasterBrokerId().longValue());
        assertEquals(DEFAULT_IP[0], response3.getMasterAddress());
        assertEquals(3, response3.getMasterEpoch().intValue());
    }

    @Test
    public void testAllReplicasShutdownAndRestart() {
        mockMetaData();
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);
        assertTrue(alterNewInSyncSet(DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {DEFAULT_IP[0]}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = ElectMasterRequestHeader.ofControllerTrigger(DEFAULT_BROKER_NAME);
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(electRequest,
            new DefaultElectPolicy((cluster, brokerName, brokerId) -> !brokerId.equals(1L), null));
        final List<EventMessage> events = cResult.getEvents();
        assertEquals(events.size(), 1);
        final ElectMasterEvent event = (ElectMasterEvent) events.get(0);
        assertFalse(event.getNewMasterElected());

        apply(cResult.getEvents());

        final GetReplicaInfoResponseHeader replicaInfo = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).getResponse();
        assertEquals(replicaInfo.getMasterAddress(), null);
        assertEquals(2, replicaInfo.getMasterEpoch().intValue());
    }

    @Test
    public void testCleanBrokerData() {
        mockMetaData();
        CleanControllerBrokerDataRequestHeader header1 = new CleanControllerBrokerDataRequestHeader(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, "1");
        ControllerResult<Void> result1 = this.replicasInfoManager.cleanBrokerData(header1, (cluster, brokerName, brokerId) -> true);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result1.getResponseCode());

        CleanControllerBrokerDataRequestHeader header2 = new CleanControllerBrokerDataRequestHeader(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null);
        ControllerResult<Void> result2 = this.replicasInfoManager.cleanBrokerData(header2, (cluster, brokerName, brokerId) -> true);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result2.getResponseCode());
        assertEquals("Broker broker-set-a is still alive, clean up failure", result2.getRemark());

        CleanControllerBrokerDataRequestHeader header3 = new CleanControllerBrokerDataRequestHeader(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, "1");
        ControllerResult<Void> result3 = this.replicasInfoManager.cleanBrokerData(header3, (cluster, brokerName, brokerId) -> false);
        assertEquals(ResponseCode.SUCCESS, result3.getResponseCode());

        CleanControllerBrokerDataRequestHeader header4 = new CleanControllerBrokerDataRequestHeader(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, "1;2;3");
        ControllerResult<Void> result4 = this.replicasInfoManager.cleanBrokerData(header4, (cluster, brokerName, brokerId) -> false);
        assertEquals(ResponseCode.SUCCESS, result4.getResponseCode());

        CleanControllerBrokerDataRequestHeader header5 = new CleanControllerBrokerDataRequestHeader(DEFAULT_CLUSTER_NAME, "broker12", "1;2;3", true);
        ControllerResult<Void> result5 = this.replicasInfoManager.cleanBrokerData(header5, (cluster, brokerName, brokerId) -> false);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result5.getResponseCode());
        assertEquals("Broker broker12 is not existed,clean broker data failure.", result5.getRemark());

        CleanControllerBrokerDataRequestHeader header6 = new CleanControllerBrokerDataRequestHeader(null, "broker12", "1;2;3", true);
        ControllerResult<Void> result6 = this.replicasInfoManager.cleanBrokerData(header6, (cluster, brokerName, brokerId) -> cluster != null);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, result6.getResponseCode());

        CleanControllerBrokerDataRequestHeader header7 = new CleanControllerBrokerDataRequestHeader(null, DEFAULT_BROKER_NAME, "1;2;3", true);
        ControllerResult<Void> result7 = this.replicasInfoManager.cleanBrokerData(header7, (cluster, brokerName, brokerId) -> false);
        assertEquals(ResponseCode.SUCCESS, result7.getResponseCode());

    }

    @Test
    public void testSerialize() {
        mockMetaData();
        byte[] data;
        try {
            data = this.replicasInfoManager.serialize();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        final ReplicasInfoManager newReplicasInfoManager = new ReplicasInfoManager(config);
        try {
            newReplicasInfoManager.deserializeFrom(data);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        Map<String, BrokerReplicaInfo> oldReplicaInfoTable = new TreeMap<>();
        Map<String, BrokerReplicaInfo> newReplicaInfoTable = new TreeMap<>();
        Map<String/* brokerName */, SyncStateInfo> oldSyncStateTable = new TreeMap<>();
        Map<String/* brokerName */, SyncStateInfo> newSyncStateTable = new TreeMap<>();
        try {
            Field field = ReplicasInfoManager.class.getDeclaredField("replicaInfoTable");
            field.setAccessible(true);
            oldReplicaInfoTable.putAll((Map<String, BrokerReplicaInfo>) field.get(this.replicasInfoManager));
            newReplicaInfoTable.putAll((Map<String, BrokerReplicaInfo>) field.get(newReplicasInfoManager));
            field = ReplicasInfoManager.class.getDeclaredField("syncStateSetInfoTable");
            field.setAccessible(true);
            oldSyncStateTable.putAll((Map<String, SyncStateInfo>) field.get(this.replicasInfoManager));
            newSyncStateTable.putAll((Map<String, SyncStateInfo>) field.get(newReplicasInfoManager));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        assertArrayEquals(oldReplicaInfoTable.keySet().toArray(), newReplicaInfoTable.keySet().toArray());
        assertArrayEquals(oldSyncStateTable.keySet().toArray(), newSyncStateTable.keySet().toArray());
        for (String brokerName : oldReplicaInfoTable.keySet()) {
            BrokerReplicaInfo oldReplicaInfo = oldReplicaInfoTable.get(brokerName);
            BrokerReplicaInfo newReplicaInfo = newReplicaInfoTable.get(brokerName);
            Field[] fields = oldReplicaInfo.getClass().getFields();
        }
    }

    @Test
    public void testScanNeedReelectBrokerSets(){
        mockMetaData();
        List<String> strings = this.replicasInfoManager.scanNeedReelectBrokerSets((clusterName, brokerName, brokerId) -> mockValidPredicate1(brokerId));
        assertArrayEquals(strings.toArray(),new String[]{DEFAULT_BROKER_NAME});
    }

    private boolean mockValidPredicate1(Long brokerId) {
        // mock only broker1(id=2) & broker2(id=3) is alive,master(id=1) is inactive
        List<Long> inactiveBroker = Arrays.asList(1L);
        return inactiveBroker.contains(brokerId);
    }
}
