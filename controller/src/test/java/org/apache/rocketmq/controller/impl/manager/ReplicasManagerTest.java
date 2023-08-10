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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.dledger.manager.BrokerReplicaInfo;
import org.apache.rocketmq.controller.dledger.manager.ReplicasManager;
import org.apache.rocketmq.controller.dledger.manager.SyncStateInfo;
import org.apache.rocketmq.controller.dledger.statemachine.EventResponse;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNextBrokerIdEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNextBrokerIdResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetReplicaInfoEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetReplicaInfoResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetSyncStateSetEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetSyncStateSetResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.AlterSyncStateSetEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.AlterSyncStateSetResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.ApplyBrokerIdEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.ApplyBrokerIdResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.CleanBrokerDataEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.CleanBrokerDataResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.ElectMasterEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.ElectMasterResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.RegisterBrokerEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.RegisterBrokerResult;
import org.apache.rocketmq.controller.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.controller.heartbeat.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.helper.BrokerLiveInfoGetter;
import org.apache.rocketmq.controller.helper.ValidBrokersGetter;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_BROKER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_CLUSTER_NAME;
import static org.apache.rocketmq.controller.ControllerTestBase.DEFAULT_IP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class ReplicasManagerTest {

    private ReplicasManager replicasManager;

    private DefaultBrokerHeartbeatManager heartbeatManager;

    private ControllerConfig config;

    private ValidBrokersGetter validBrokersGetter;

    private BrokerLiveInfoGetter brokerLiveInfoGetter;

    private final static Long TIMEOUT_NEVER = 1000000000000000000L;

    private final static Long TIMEOUT_NOW = -1L;

    @Before
    public void init() {
        this.config = new ControllerConfig();
        this.config.setEnableElectUncleanMaster(false);
        this.config.setScanNotActiveBrokerInterval(300000000);
        this.replicasManager = new ReplicasManager(config);
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(config);
        this.validBrokersGetter = this.heartbeatManager::getActiveBrokerIds;
        this.brokerLiveInfoGetter = this.heartbeatManager::getBrokerLiveInfo;
        this.heartbeatManager.initialize();
        this.heartbeatManager.start();
    }

    @After
    public void destroy() {
        this.replicasManager = null;
        this.heartbeatManager.shutdown();
        this.heartbeatManager = null;
    }

    private BrokerReplicasInfo.ReplicasInfo getReplicasInfo(String brokerName) {
        EventResponse<GetSyncStateSetResult> response = (EventResponse<GetSyncStateSetResult>) this.replicasManager.applyEvent(new GetSyncStateSetEvent(Arrays.asList(brokerName)));
        final GetSyncStateSetResult result = response.getResponseResult();
        final BrokerReplicasInfo replicasInfo = new BrokerReplicasInfo();

        result.getBrokerSyncStateInfoMap().forEach((broker, pair) -> {
            final List<BrokerReplicasInfo.ReplicaIdentity> inSyncReplicas = new ArrayList<>();
            final List<BrokerReplicasInfo.ReplicaIdentity> outSyncReplicas = new ArrayList<>();
            final BrokerReplicaInfo replicaInfo = pair.getObject1();
            final SyncStateInfo syncStateInfo = pair.getObject2();

            replicaInfo.getBrokerIdTable().forEach((brokerId, brokerAddress) -> {
                boolean isAlive = true;
                BrokerReplicasInfo.ReplicaIdentity replica = new BrokerReplicasInfo.ReplicaIdentity(broker, brokerId, brokerAddress, isAlive);
                if (syncStateInfo.getSyncStateSet().contains(brokerId)) {
                    inSyncReplicas.add(replica);
                } else {
                    outSyncReplicas.add(replica);
                }
            });
            final Long masterBrokerId = syncStateInfo.getMasterBrokerId();
            int masterEpoch = syncStateInfo.getMasterEpoch();
            int syncStateSetEpoch = syncStateInfo.getSyncStateSetEpoch();
            final BrokerReplicasInfo.ReplicasInfo syncState = new BrokerReplicasInfo.ReplicasInfo(masterBrokerId, replicaInfo.getBrokerAddress(masterBrokerId), masterEpoch,
                syncStateSetEpoch, inSyncReplicas, outSyncReplicas);
            replicasInfo.addReplicaInfo(broker, syncState);
        });
        return replicasInfo.getReplicasInfoTable().get(brokerName);
    }

    public void registerNewBroker(String clusterName, String brokerName, String brokerAddress,
        Long exceptBrokerId, Long exceptMasterBrokerId) {

        // Get next brokerId
        final GetNextBrokerIdEvent getNextBrokerIdEvent = new GetNextBrokerIdEvent(clusterName, brokerName);
        final EventResponse<GetNextBrokerIdResult> response = (EventResponse<GetNextBrokerIdResult>) this.replicasManager.applyEvent(getNextBrokerIdEvent);
        final GetNextBrokerIdResult nextBrokerIdResult = response.getResponseResult();
        Long nextBrokerId = nextBrokerIdResult.getNextBrokerId();
        String registerCheckCode = brokerAddress + ";" + System.currentTimeMillis();

        // check response
        assertEquals(ResponseCode.SUCCESS, response.getResponseCode());
        assertEquals(exceptBrokerId, nextBrokerId);

        // Apply brokerId
        final ApplyBrokerIdEvent applyBrokerIdEvent = new ApplyBrokerIdEvent(clusterName, brokerName, nextBrokerId, registerCheckCode);
        final EventResponse<ApplyBrokerIdResult> applyBrokerIdResponse = (EventResponse<ApplyBrokerIdResult>) this.replicasManager.applyEvent(applyBrokerIdEvent);

        // check response
        assertEquals(ResponseCode.SUCCESS, applyBrokerIdResponse.getResponseCode());

        // check it in state machine
        BrokerReplicasInfo.ReplicasInfo replicasInfo = getReplicasInfo(brokerName);
        BrokerReplicasInfo.ReplicaIdentity replicaIdentity = replicasInfo.getNotInSyncReplicas().stream().filter(x -> x.getBrokerId().equals(exceptBrokerId)).findFirst().get();
        assertNotNull(replicaIdentity);
        assertEquals(brokerName, replicaIdentity.getBrokerName());
        assertEquals(exceptBrokerId, replicaIdentity.getBrokerId());
        assertEquals(brokerAddress, replicaIdentity.getBrokerAddress());

        // register success
        final RegisterBrokerEvent registerBrokerEvent = new RegisterBrokerEvent(clusterName, brokerName, brokerAddress, nextBrokerId);
        final EventResponse<RegisterBrokerResult> registerBrokerResultEventResponse = (EventResponse<RegisterBrokerResult>) this.replicasManager.applyEvent(registerBrokerEvent);
        final RegisterBrokerResult registerSuccessResult = registerBrokerResultEventResponse.getResponseResult();

        // check response
        assertEquals(ResponseCode.SUCCESS, registerBrokerResultEventResponse.getResponseCode());
        assertEquals(exceptMasterBrokerId, registerSuccessResult.getMasterBrokerId());

    }

    private Map<Long, BrokerLiveInfo> getBrokersLiveInfo(String clusterName, String brokerName) {
        return this.validBrokersGetter.get(clusterName, brokerName).stream().
            map(broker -> this.brokerLiveInfoGetter.get(clusterName, brokerName, broker)).
            collect(Collectors.toMap(brokerLiveInfo -> brokerLiveInfo.getBrokerId(), brokerLiveInfo -> brokerLiveInfo));
    }

    public void brokerElectMaster(String clusterName, String brokerName, Long brokerId, String brokerAddress,
        boolean isFirstTryElect, boolean expectToBeElected) {

        final Map<Long, BrokerLiveInfo> aliveBrokers = getBrokersLiveInfo(clusterName, brokerName);

        final GetReplicaInfoEvent getReplicaInfoEvent = new GetReplicaInfoEvent(clusterName, brokerName);
        EventResponse<GetReplicaInfoResult> getReplicaInfoResultEventResponse = (EventResponse<GetReplicaInfoResult>) this.replicasManager.applyEvent(getReplicaInfoEvent);
        final GetReplicaInfoResult replicaInfoBefore = getReplicaInfoResultEventResponse.getResponseResult();
        final BrokerReplicasInfo.ReplicasInfo syncStateSetInfo = getReplicasInfo(brokerName);

        // Try elect itself as a master
        final ElectMasterEvent electMasterEvent = new ElectMasterEvent(clusterName, brokerName, brokerId, false, aliveBrokers);
        final EventResponse<ElectMasterResult> electMasterResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        final ElectMasterResult electMasterResult = electMasterResponse.getResponseResult();

        getReplicaInfoResultEventResponse = (EventResponse<GetReplicaInfoResult>) this.replicasManager.applyEvent(getReplicaInfoEvent);
        final GetReplicaInfoResult replicaInfoAfter = getReplicaInfoResultEventResponse.getResponseResult();

        if (isFirstTryElect) {
            // it should be elected
            // check response
            assertEquals(ResponseCode.SUCCESS, electMasterResponse.getResponseCode());
            assertEquals(1, electMasterResult.getMasterEpoch().intValue());
            assertEquals(1, electMasterResult.getSyncStateSetEpoch().intValue());
            assertEquals(brokerAddress, electMasterResult.getMasterAddress());
            assertEquals(brokerId, electMasterResult.getMasterBrokerId());
            // check it in state machine
            assertEquals(brokerAddress, replicaInfoAfter.getMasterAddress());
            assertEquals(1, replicaInfoAfter.getMasterEpoch().intValue());
            assertEquals(brokerId, replicaInfoAfter.getMasterBrokerId());
            return;
        }

        // failed because now master still exist
        if (replicaInfoBefore.getMasterBrokerId() != null && aliveBrokers.containsKey(replicaInfoBefore.getMasterBrokerId())) {
            assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, electMasterResponse.getResponseCode());
            assertEquals(replicaInfoBefore.getMasterAddress(), electMasterResult.getMasterAddress());
            assertEquals(replicaInfoBefore.getMasterEpoch(), electMasterResult.getMasterEpoch());
            assertEquals(replicaInfoBefore.getMasterBrokerId(), electMasterResult.getMasterBrokerId());
            assertEquals(replicaInfoBefore.getMasterBrokerId(), replicaInfoAfter.getMasterBrokerId());
            return;
        }
        if (syncStateSetInfo.isExistInSync(brokerName, brokerId, brokerAddress) || this.config.isEnableElectUncleanMaster()) {
            // a new master can be elected successfully
            assertEquals(ResponseCode.SUCCESS, electMasterResponse.getResponseCode());
            assertEquals(replicaInfoBefore.getMasterEpoch() + 1, replicaInfoAfter.getMasterEpoch().intValue());

            if (expectToBeElected) {
                assertEquals(brokerAddress, electMasterResult.getMasterAddress());
                assertEquals(brokerId, electMasterResult.getMasterBrokerId());
                assertEquals(brokerAddress, replicaInfoAfter.getMasterAddress());
                assertEquals(brokerId, replicaInfoAfter.getMasterBrokerId());
            }
            return;
        }
        // failed because elect nothing
        assertEquals(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, electMasterResponse.getResponseCode());
    }

    private void alterNewInSyncSet(String clusterName, String brokerName, Long brokerId, Integer masterEpoch,
        Set<Long> newSyncStateSet, Integer syncStateSetEpoch) {
        final Set<Long> aliveBrokerIds = this.validBrokersGetter.get(clusterName, brokerName);
        final AlterSyncStateSetEvent alterSyncStateEvent = new AlterSyncStateSetEvent(clusterName, brokerName, brokerId, masterEpoch, newSyncStateSet, syncStateSetEpoch, aliveBrokerIds);
        final EventResponse<AlterSyncStateSetResult> alterSyncStateSetResultEventResponse = (EventResponse<AlterSyncStateSetResult>) this.replicasManager.applyEvent(alterSyncStateEvent);

        assertEquals(ResponseCode.SUCCESS, alterSyncStateSetResultEventResponse.getResponseCode());
        assertArrayEquals(alterSyncStateSetResultEventResponse.getResponseResult().getNewSyncStateSet().getSyncStateSet().toArray(), newSyncStateSet.toArray());

        final GetReplicaInfoEvent getReplicaInfoEvent = new GetReplicaInfoEvent(clusterName, brokerName);
        EventResponse<GetReplicaInfoResult> getReplicaInfoResultEventResponse = (EventResponse<GetReplicaInfoResult>) this.replicasManager.applyEvent(getReplicaInfoEvent);
        final GetReplicaInfoResult replicaInfoResult = getReplicaInfoResultEventResponse.getResponseResult();

        SyncStateSet syncStateSet = replicaInfoResult.getSyncStateSet();
        assertArrayEquals(syncStateSet.getSyncStateSet().toArray(), newSyncStateSet.toArray());
        assertEquals(syncStateSet.getSyncStateSetEpoch(), syncStateSetEpoch + 1);
    }

    public void mockMetaData() {
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, null);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, null);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, null);
        // all brokers alive
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NEVER, null,
            1, 0L, 0L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, TIMEOUT_NEVER, null,
            1, 0L, 0L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, TIMEOUT_NEVER, null,
            1, 0L, 0L, 0);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L, DEFAULT_IP[0], true, true);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 2L, DEFAULT_IP[1], false, false);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 3L, DEFAULT_IP[2], false, false);
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);
        newSyncStateSet.add(2L);
        newSyncStateSet.add(3L);
        alterNewInSyncSet(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 1);
    }

    public void mockHeartbeatDataMasterStillAlive() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NEVER, null,
            1, 1L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, TIMEOUT_NEVER, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, TIMEOUT_NEVER, null,
            1, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherEpoch() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NOW, null,
            1, 3L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, TIMEOUT_NEVER, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, TIMEOUT_NEVER, null,
            0, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherOffset() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NOW, null,
            1, 3L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, TIMEOUT_NEVER, null,
            1, 2L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, TIMEOUT_NEVER, null,
            1, 3L, -1L, 0);
    }

    public void mockHeartbeatDataHigherPriority() {
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NOW, null,
            1, 3L, -1L, 3);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, TIMEOUT_NEVER, null,
            1, 3L, -1L, 2);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, TIMEOUT_NEVER, null,
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
        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L, DEFAULT_IP[0], true, true);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 2L, DEFAULT_IP[1], false, false);
        registerNewBroker(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[2], 3L, 1L);
        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 3L, DEFAULT_IP[2], false, false);

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

        // mock broker-1 and broker-2 inactive
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NOW, null,
            1, 1L, -1L, 0);
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[1], 2L, TIMEOUT_NOW, null,
            1, 1L, -1L, 0);

        brokerElectMaster(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 3L, DEFAULT_IP[2], false, true);

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
        mockHeartbeatDataMasterStillAlive();

        final Map<Long, BrokerLiveInfo> liveInfo = getBrokersLiveInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        final ElectMasterEvent electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null, false, liveInfo);
        final EventResponse<ElectMasterResult> eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, eventResponse.getResponseCode());
    }

    @Test
    public void testElectMasterPreferHigherEpoch() {
        mockMetaData();
        mockHeartbeatDataHigherEpoch();

        final Map<Long, BrokerLiveInfo> liveInfo = getBrokersLiveInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        final ElectMasterEvent electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null, false, liveInfo);
        final EventResponse<ElectMasterResult> eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        final ElectMasterResult electMasterResult = eventResponse.getResponseResult();
        assertEquals(DEFAULT_IP[1], electMasterResult.getMasterAddress());
        assertEquals(2L, electMasterResult.getMasterBrokerId().longValue());
        assertEquals(2, electMasterResult.getMasterEpoch().intValue());
    }

    @Test
    public void testElectMasterPreferHigherOffsetWhenEpochEquals() {
        mockMetaData();
        mockHeartbeatDataHigherOffset();

        final Map<Long, BrokerLiveInfo> liveInfo = getBrokersLiveInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        final ElectMasterEvent electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null, false, liveInfo);
        final EventResponse<ElectMasterResult> eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        final ElectMasterResult electMasterResult = eventResponse.getResponseResult();
        assertEquals(DEFAULT_IP[2], electMasterResult.getMasterAddress());
        assertEquals(3L, electMasterResult.getMasterBrokerId().longValue());
        assertEquals(2, electMasterResult.getMasterEpoch().intValue());
    }

    @Test
    public void testElectMasterPreferHigherPriorityWhenEpochAndOffsetEquals() {
        mockMetaData();
        mockHeartbeatDataHigherPriority();

        final Map<Long, BrokerLiveInfo> liveInfo = getBrokersLiveInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        final ElectMasterEvent electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null, false, liveInfo);
        final EventResponse<ElectMasterResult> eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        final ElectMasterResult electMasterResult = eventResponse.getResponseResult();
        assertEquals(DEFAULT_IP[2], electMasterResult.getMasterAddress());
        assertEquals(3L, electMasterResult.getMasterBrokerId().longValue());
        assertEquals(2, electMasterResult.getMasterEpoch().intValue());
    }

    @Test
    public void testElectMaster() {
        mockMetaData();

        // mock broker-1 is offline
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NOW, null,
            1, 1L, -1L, 0);

        final Map<Long, BrokerLiveInfo> liveInfo = getBrokersLiveInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        ElectMasterEvent electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null, false, liveInfo);
        EventResponse<ElectMasterResult> eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        ElectMasterResult electMasterResult = eventResponse.getResponseResult();
        Long masterBrokerId = electMasterResult.getMasterBrokerId();
        String masterBrokerAddress = DEFAULT_IP[(int) (masterBrokerId - 1)];

        assertEquals(2, electMasterResult.getMasterEpoch().intValue());
        assertNotEquals(1L, electMasterResult.getMasterBrokerId().longValue());
        assertNotEquals(DEFAULT_IP[0], electMasterResult.getMasterAddress());

        final Set<Long> brokerSet = new HashSet<>();
        brokerSet.add(1L);
        brokerSet.add(2L);
        brokerSet.add(3L);
        alterNewInSyncSet(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, electMasterResult.getMasterBrokerId(), electMasterResult.getMasterEpoch(), brokerSet, electMasterResult.getSyncStateSetEpoch());

        // test admin try to elect a assignedMaster, but it isn't alive
        electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L, true, liveInfo);
        eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        electMasterResult = eventResponse.getResponseResult();

        assertEquals(eventResponse.getResponseCode(), ResponseCode.CONTROLLER_ELECT_MASTER_FAILED);

        // test admin try to elect a assignedMaster but old master still alive, and the old master is equals to assignedMaster
        electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, masterBrokerId, true, liveInfo);
        eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        electMasterResult = eventResponse.getResponseResult();

        assertEquals(eventResponse.getResponseCode(), ResponseCode.CONTROLLER_MASTER_STILL_EXIST);

        // admin successful elect a assignedMaster.
        // mock master is offline
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, masterBrokerAddress, masterBrokerId, TIMEOUT_NOW, null,
            1, 1L, -1L, 0);
        // mock broker-1 is online
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NEVER, null,
            1, 1L, -1L, 0);

        electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L, true, liveInfo);
        eventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        electMasterResult = eventResponse.getResponseResult();
        assertEquals(eventResponse.getResponseCode(), ResponseCode.SUCCESS);

        assertEquals(1L, electMasterResult.getMasterBrokerId().longValue());
        assertEquals(DEFAULT_IP[0], electMasterResult.getMasterAddress());
        assertEquals(3, electMasterResult.getMasterEpoch().intValue());
    }

    @Test
    public void testAllReplicasShutdownAndRestart() {
        mockMetaData();
        final HashSet<Long> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(1L);
        alterNewInSyncSet(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, 1L, 1, newSyncStateSet, 2);

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {DEFAULT_IP[0]}, not more replicas can be elected as master, it will be failed.

        // mock broker-1 is offline
        this.heartbeatManager.onBrokerHeartbeat(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, DEFAULT_IP[0], 1L, TIMEOUT_NOW, null,
            1, 1L, -1L, 0);
        final Map<Long, BrokerLiveInfo> liveInfo = getBrokersLiveInfo(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        ElectMasterEvent electMasterEvent = new ElectMasterEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, null, false, liveInfo);
        EventResponse<ElectMasterResult> electMasterResultEventResponse = (EventResponse<ElectMasterResult>) this.replicasManager.applyEvent(electMasterEvent);
        ElectMasterResult electMasterResult = electMasterResultEventResponse.getResponseResult();

        assertEquals(ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE, electMasterResultEventResponse.getResponseCode());

        final GetReplicaInfoEvent getReplicaInfoEvent = new GetReplicaInfoEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME);
        EventResponse<GetReplicaInfoResult> getReplicaInfoResultEventResponse = (EventResponse<GetReplicaInfoResult>) this.replicasManager.applyEvent(getReplicaInfoEvent);
        final GetReplicaInfoResult replicaInfoResult = getReplicaInfoResultEventResponse.getResponseResult();

        assertEquals(replicaInfoResult.getMasterAddress(), null);
        assertEquals(2, replicaInfoResult.getMasterEpoch().intValue());
    }

    @Test
    public void testCleanBrokerData() {
        mockMetaData();

        // clean broker-1 should success
        CleanBrokerDataEvent cleanBrokerDataEvent = new CleanBrokerDataEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, Sets.newHashSet(1L));
        EventResponse<CleanBrokerDataResult> cleanBrokerDataResultEventResponse = (EventResponse<CleanBrokerDataResult>) this.replicasManager.applyEvent(cleanBrokerDataEvent);
        assertEquals(ResponseCode.SUCCESS, cleanBrokerDataResultEventResponse.getResponseCode());

        GetSyncStateSetEvent getSyncStateSetEvent = new GetSyncStateSetEvent(Arrays.asList(DEFAULT_BROKER_NAME));
        EventResponse<GetSyncStateSetResult> getSyncStateSetResultEventResponse = (EventResponse<GetSyncStateSetResult>) this.replicasManager.applyEvent(getSyncStateSetEvent);
        GetSyncStateSetResult getSyncStateSetResult = getSyncStateSetResultEventResponse.getResponseResult();
        assertEquals(ResponseCode.SUCCESS, getSyncStateSetResultEventResponse.getResponseCode());
        assertEquals(1, getSyncStateSetResult.getBrokerSyncStateInfoMap().size());
        BrokerReplicaInfo replicaInfo = getSyncStateSetResult.getBrokerSyncStateInfoMap().get(DEFAULT_BROKER_NAME).getObject1();
        SyncStateInfo syncStateInfo = getSyncStateSetResult.getBrokerSyncStateInfoMap().get(DEFAULT_BROKER_NAME).getObject2();
        assertFalse(replicaInfo.isBrokerExist(1L));
        assertEquals(2, replicaInfo.getAllBroker().size());

        // clean broker-4 should be nothing change
        cleanBrokerDataEvent = new CleanBrokerDataEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME, Sets.newHashSet(4L));
        cleanBrokerDataResultEventResponse = (EventResponse<CleanBrokerDataResult>) this.replicasManager.applyEvent(cleanBrokerDataEvent);
        assertEquals(ResponseCode.SUCCESS, cleanBrokerDataResultEventResponse.getResponseCode());

        getSyncStateSetEvent = new GetSyncStateSetEvent(Arrays.asList(DEFAULT_BROKER_NAME));
        getSyncStateSetResultEventResponse = (EventResponse<GetSyncStateSetResult>) this.replicasManager.applyEvent(getSyncStateSetEvent);
        getSyncStateSetResult = getSyncStateSetResultEventResponse.getResponseResult();
        assertEquals(ResponseCode.SUCCESS, getSyncStateSetResultEventResponse.getResponseCode());
        assertEquals(1, getSyncStateSetResult.getBrokerSyncStateInfoMap().size());
        replicaInfo = getSyncStateSetResult.getBrokerSyncStateInfoMap().get(DEFAULT_BROKER_NAME).getObject1();
        syncStateInfo = getSyncStateSetResult.getBrokerSyncStateInfoMap().get(DEFAULT_BROKER_NAME).getObject2();
        assertFalse(replicaInfo.isBrokerExist(1L));
        assertEquals(2, replicaInfo.getAllBroker().size());

        // clean broker with unregistered broker-set should fail
        cleanBrokerDataEvent = new CleanBrokerDataEvent(DEFAULT_CLUSTER_NAME, DEFAULT_BROKER_NAME + "invalid", Sets.newHashSet(2L));
        cleanBrokerDataResultEventResponse = (EventResponse<CleanBrokerDataResult>) this.replicasManager.applyEvent(cleanBrokerDataEvent);
        assertEquals(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, cleanBrokerDataResultEventResponse.getResponseCode());

    }
}
