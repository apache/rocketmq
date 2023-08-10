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

package org.apache.rocketmq.controller.dledger.manager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.dledger.statemachine.EventResponse;
import org.apache.rocketmq.controller.dledger.statemachine.event.EventMessage;
import org.apache.rocketmq.controller.dledger.statemachine.event.EventResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNeedReElectBrokerSetsEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNeedReElectBrokerSetsResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNextBrokerIdEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNextBrokerIdResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetReplicaInfoEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetReplicaInfoResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetSyncStateSetEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetSyncStateSetResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.ReadEventMessage;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.ReadEventType;
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
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventMessage;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventType;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.ElectMasterResponseBody;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;

public class ReplicasManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ControllerConfig controllerConfig;

    private final ElectPolicy electPolicy = new DefaultElectPolicy();
    private final Map<String/* brokerName */, BrokerReplicaInfo> replicaInfoTable;
    private final Map<String/* brokerName */, SyncStateInfo> syncStateSetInfoTable;

    public ReplicasManager(ControllerConfig controllerConfig) {
        this.controllerConfig = controllerConfig;
        this.replicaInfoTable = new ConcurrentHashMap<>();
        this.syncStateSetInfoTable = new ConcurrentHashMap<>();
    }

    /**
     * Apply events to memory statemachine.
     *
     * @param event event message
     */
    public EventResponse<? extends EventResult> applyEvent(final EventMessage event) {
        if (event == null) {
            return new EventResponse<>();
        }
        if (event instanceof WriteEventMessage) {
            WriteEventMessage writeEvent = (WriteEventMessage) event;
            final WriteEventType type = writeEvent.getEventType();
            switch (type) {
                case APPLY_BROKER_ID:
                    return dealApplyBrokerId((ApplyBrokerIdEvent) writeEvent);
                case REGISTER_BROKER:
                    return dealRegisterBroker((RegisterBrokerEvent) writeEvent);
                case ELECT_MASTER:
                    return dealElectMaster((ElectMasterEvent) writeEvent);
                case ALTER_SYNC_STATE_SET:
                    return dealAlterSyncStateSet((AlterSyncStateSetEvent) writeEvent);
                case CLEAN_BROKER_DATA:
                    return dealCleanBrokerData((CleanBrokerDataEvent) writeEvent);
                default:
                    break;
            }
        } else if (event instanceof ReadEventMessage) {
            ReadEventMessage readEvent = (ReadEventMessage) event;
            final ReadEventType type = readEvent.getEventType();
            switch (type) {
                case GET_NEXT_BROKER_ID:
                    return dealGetNextBrokerId((GetNextBrokerIdEvent) readEvent);
                case GET_REPLICA_INFO:
                    return dealGetReplicaInfo((GetReplicaInfoEvent) readEvent);
                case GET_SYNC_STATE_SET:
                    return dealGetSyncStateSet((GetSyncStateSetEvent) readEvent);
                case GET_NEED_RE_ELECT_BROKER_SETS:
                    return dealGetNeedReElectBrokerSets((GetNeedReElectBrokerSetsEvent) readEvent);
                default:
                    break;
            }
        }
        EventResponse result = new EventResponse<>();
        result.setResponse(ResponseCode.CONTROLLER_INNER_ERROR, "Unknown event : " + event);
        return result;
    }

    private EventResponse<ApplyBrokerIdResult> dealApplyBrokerId(final ApplyBrokerIdEvent event) {
        final String clusterName = event.getClusterName();
        final String brokerName = event.getBrokerName();
        final Long brokerId = event.getAppliedBrokerId();
        final String registerCheckCode = event.getRegisterCheckCode();
        final String brokerAddress = registerCheckCode.split(";")[0];
        BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final ApplyBrokerIdResult result = new ApplyBrokerIdResult(clusterName, brokerName);
        final EventResponse<ApplyBrokerIdResult> resp = new EventResponse<>(result);
        // broker-set unregistered
        if (brokerReplicaInfo == null) {
            if (brokerId != MixAll.FIRST_BROKER_CONTROLLER_ID) {
                String msg = String.format("Broker-set: %s hasn't been registered in controller, but broker try to apply brokerId: %d", brokerName, brokerId);
                LOGGER.error(msg);
                resp.setResponse(ResponseCode.CONTROLLER_BROKER_ID_INVALID, msg);
                return resp;
            }
            // First time to register in this broker set
            // Initialize the replicaInfo about this broker set
            brokerReplicaInfo = new BrokerReplicaInfo(clusterName, brokerName);
            brokerReplicaInfo.addBroker(brokerId, brokerAddress, registerCheckCode);
            this.replicaInfoTable.put(brokerName, brokerReplicaInfo);
            final SyncStateInfo syncStateInfo = new SyncStateInfo(clusterName, brokerName);
            // Initialize an empty syncStateInfo for this broker set
            this.syncStateSetInfoTable.put(brokerName, syncStateInfo);
            return resp;
        }
        // broker-set registered
        if (!brokerReplicaInfo.isBrokerExist(brokerId)) {
            // if brokerId hasn't been applied
            brokerReplicaInfo.addBroker(brokerId, brokerAddress, registerCheckCode);
            return resp;
        }
        // if brokerId has been applied and its registerCheckCode has changed
        if (!registerCheckCode.equals(brokerReplicaInfo.getBrokerRegisterCheckCode(brokerId))) {
            String msg = String.format("Broker-set: %s has been registered in controller, but broker try to apply brokerId: %d with different registerCheckCode", brokerName, brokerId);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_BROKER_ID_INVALID, msg);
            return resp;
        }
        return resp;
    }

    private EventResponse<RegisterBrokerResult> dealRegisterBroker(final RegisterBrokerEvent event) {
        final String clusterName = event.getClusterName();
        final String brokerName = event.getBrokerName();
        final Long brokerId = event.getBrokerId();
        final String brokerAddress = event.getBrokerAddress();
        final EventResponse<RegisterBrokerResult> resp = new EventResponse<>();
        if (!isContainsBroker(brokerName)) {
            String msg = String.format("BrokerSet[%s]: broker set hasn't been registered in controller, but broker try to register brokerId: %d", brokerName, brokerId);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, msg);
            return resp;
        }
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        if (!brokerReplicaInfo.isBrokerExist(brokerId)) {
            String msg = String.format("BrokerSet[%s]: brokerId: %d hasn't been applied in controller, but broker try to register", brokerName, brokerId);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, msg);
            return resp;

        }
        if (!brokerAddress.equals(brokerReplicaInfo.getBrokerAddress(brokerId))) {
            // brokerAddress has been changed, update it
            brokerReplicaInfo.updateBrokerAddress(brokerId, brokerAddress);
        }
        RegisterBrokerResult result = new RegisterBrokerResult();
        result.setClusterName(clusterName);
        result.setBrokerName(brokerName);
        result.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
        result.setSyncStateBrokerSet(syncStateInfo.getSyncStateSet());
        result.setMasterEpoch(syncStateInfo.getMasterEpoch());
        if (syncStateInfo.isMasterExist()) {
            final Long masterBrokerId = syncStateInfo.getMasterBrokerId();
            result.setMasterBrokerId(masterBrokerId);
            result.setMasterAddress(brokerReplicaInfo.getBrokerAddress(masterBrokerId));
        }
        resp.setResponseResult(result);
        return resp;
    }

    private EventResponse<ElectMasterResult> dealElectMaster(final ElectMasterEvent event) {
        final String clusterName = event.getClusterName();
        final String brokerName = event.getBrokerName();
        final Long brokerId = event.getBrokerId();
        final boolean designateElect = event.isDesignateElect();
        final Map<Long, BrokerLiveInfo> aliveBrokers = event.getAliveBrokers();
        final EventResponse<ElectMasterResult> resp = new EventResponse<>();
        final ElectMasterResult result = new ElectMasterResult();
        resp.setResponseResult(result);

        if (!isContainsBroker(brokerName)) {
            String msg = String.format("BrokerSet[%s]: broker set hasn't been registered in controller, but broker try to elect master", brokerName);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, msg);
            return resp;
        }

        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        final Set<Long> syncStateSet = syncStateInfo.getSyncStateSet();
        final Long oldMaster = syncStateInfo.getMasterBrokerId();
        Set<Long> allReplicaBrokers = controllerConfig.isEnableElectUncleanMaster() ? brokerReplicaInfo.getAllBroker() : Collections.emptySet();
        Long newMaster = null;

        if (syncStateInfo.isFirstTimeForElect()) {
            // if never have a master in this broker set, in other words, it is the first time to elect a master
            // elect it as the first master
            newMaster = brokerId;
        }

        // elect by policy
        if (newMaster == null) {
            // we should assign this assignedBrokerId when the brokerAddress need to be elected by force
            Long assignedBrokerId = designateElect ? brokerId : null;
            newMaster = electPolicy.elect(syncStateSet, allReplicaBrokers, aliveBrokers, oldMaster, assignedBrokerId);
        }

        if (newMaster != null && newMaster.equals(oldMaster)) {
            // old master still valid, change nothing
            String msg = String.format("BrokerSet[%s]: old master: %d still valid, change nothing", brokerName, oldMaster);
            LOGGER.warn(msg);
            // the master still exist
            result.setMasterEpoch(syncStateInfo.getMasterEpoch());
            result.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
            result.setMasterBrokerId(oldMaster);
            result.setMasterAddress(brokerReplicaInfo.getBrokerAddress(oldMaster));
            result.setElectMasterResponseBody(new ElectMasterResponseBody(syncStateSet));
            resp.setResponse(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, msg);
            return resp;
        }

        // a new master is elected
        if (newMaster != null) {
            // update master info
            syncStateInfo.updateMasterInfo(newMaster);

            // record new syncStateSet
            final Set<Long> newSyncStateSet = new HashSet<>();
            newSyncStateSet.add(newMaster);
            syncStateInfo.updateSyncStateSetInfo(newSyncStateSet);

            final BrokerMemberGroup brokerMemberGroup = buildBrokerMemberGroup(brokerReplicaInfo);
            final ElectMasterResponseBody responseBody = new ElectMasterResponseBody(brokerMemberGroup, newSyncStateSet);
            result.setElectMasterResponseBody(responseBody);
            result.setMasterBrokerId(newMaster);
            result.setMasterAddress(brokerReplicaInfo.getBrokerAddress(newMaster));
            result.setMasterEpoch(syncStateInfo.getMasterEpoch());
            result.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
            return resp;
        }
        if (brokerId == null) {
            // elect nothing and the election is triggered by controller itself, we should clear the stale master info
            syncStateInfo.updateMasterInfo(null);
            String msg = String.format("BrokerSet[%s]: no broker is elected as master, clear master info", brokerName);
            LOGGER.warn(msg);
            resp.setResponse(ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE, msg);
        } else {
            String msg = String.format("BrokerSet[%s]: no broker is elected as master, brokerId: %d", brokerName, brokerId);
            LOGGER.warn(msg);
            resp.setResponse(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, msg);
        }
        return resp;
    }

    private EventResponse<AlterSyncStateSetResult> dealAlterSyncStateSet(final AlterSyncStateSetEvent event) {
        final String brokerName = event.getBrokerName();
        final Long masterBrokerId = event.getMasterBrokerId();
        final Integer masterEpoch = event.getMasterEpoch();
        final Integer newSyncStateSetEpoch = event.getNewSyncStateSetEpoch();
        final Set<Long> newSyncStateSet = event.getNewSyncStateSet();
        final Set<Long> aliveBrokers = event.getAliveBrokerSet();
        final EventResponse<AlterSyncStateSetResult> resp = new EventResponse<>();
        if (!isContainsBroker(brokerName)) {
            String msg = String.format("BrokerSet[%s]: broker set hasn't been registered in controller, but broker try to alter syncStateSet", brokerName);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, msg);
            return resp;
        }
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);

        // Check whether the oldSyncStateSet is equal with newSyncStateSet
        final Set<Long> oldSyncStateSet = syncStateInfo.getSyncStateSet();
        if (oldSyncStateSet.size() == newSyncStateSet.size() && oldSyncStateSet.containsAll(newSyncStateSet)) {
            String msg = String.format("BrokerSet[%s]: syncStateSet hasn't been changed, no need to alter", brokerName);
            LOGGER.warn(msg);
            resp.setResponse(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, msg);
            return resp;
        }

        // Check master
        if (!syncStateInfo.getMasterBrokerId().equals(masterBrokerId)) {
            String msg = String.format("BrokerSet[%s]: masterBrokerId: %d is not equal with current masterBrokerId: %d", brokerName, masterBrokerId, syncStateInfo.getMasterBrokerId());
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_INVALID_MASTER, msg);
            return resp;
        }

        // Check master epoch
        if (masterEpoch != syncStateInfo.getMasterEpoch()) {
            String msg = String.format("BrokerSet[%s]: masterEpoch: %d is not equal with current masterEpoch: %d", brokerName, masterEpoch, syncStateInfo.getMasterEpoch());
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_FENCED_MASTER_EPOCH, msg);
            return resp;
        }

        // Check syncStateSet epoch
        if (newSyncStateSetEpoch != syncStateInfo.getSyncStateSetEpoch()) {
            String msg = String.format("BrokerSet[%s]: newSyncStateSetEpoch: %d is not equal with current syncStateSetEpoch: %d", brokerName, newSyncStateSetEpoch, syncStateInfo.getSyncStateSetEpoch());
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH, msg);
            return resp;
        }

        // Check newSyncStateSet correctness
        for (Long replica : newSyncStateSet) {
            if (!brokerReplicaInfo.isBrokerExist(replica)) {
                String msg = String.format("BrokerSet[%s]: replica: %d is not exist in broker set", brokerName, replica);
                LOGGER.error(msg);
                resp.setResponse(ResponseCode.CONTROLLER_INVALID_REPLICAS, msg);
                return resp;
            }
            if (!aliveBrokers.contains(replica)) {
                // replica is not alive
                String msg = String.format("BrokerSet[%s]: replica: %d is not alive", brokerName, replica);
                LOGGER.error(msg);
                resp.setResponse(ResponseCode.CONTROLLER_BROKER_NOT_ALIVE, msg);
                return resp;
            }
        }

        // Update syncStateSet
        syncStateInfo.updateSyncStateSetInfo(newSyncStateSet);
        final AlterSyncStateSetResult result = new AlterSyncStateSetResult(new SyncStateSet(newSyncStateSet, syncStateInfo.getSyncStateSetEpoch()));
        resp.setResponseResult(result);
        return resp;
    }

    private EventResponse<CleanBrokerDataResult> dealCleanBrokerData(final CleanBrokerDataEvent event) {
        final String brokerName = event.getBrokerName();
        final Set<Long> brokerIds = event.getNeedCleanBrokerIdSet();
        final EventResponse<CleanBrokerDataResult> resp = new EventResponse<>();
        final CleanBrokerDataResult result = new CleanBrokerDataResult();
        resp.setResponseResult(result);
        if (brokerIds == null || brokerIds.isEmpty()) {
            // empty means clean all broker data
            this.replicaInfoTable.remove(brokerName);
            this.syncStateSetInfoTable.remove(brokerName);
            return resp;
        }
        if (!isContainsBroker(brokerName)) {
            String msg = String.format("BrokerSet[%s]: broker set hasn't been registered in controller, but broker try to clean broker data", brokerName);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, msg);
            return resp;
        }
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        for (Long brokerId : brokerIds) {
            brokerReplicaInfo.removeBrokerId(brokerId);
            syncStateInfo.removeFromSyncState(brokerId);
        }
        if (brokerReplicaInfo.getBrokerIdTable().isEmpty()) {
            this.replicaInfoTable.remove(brokerName);
        }
        if (syncStateInfo.getSyncStateSet().isEmpty()) {
            this.syncStateSetInfoTable.remove(brokerName);
        }
        return resp;
    }

    private EventResponse<GetNextBrokerIdResult> dealGetNextBrokerId(final GetNextBrokerIdEvent event) {
        final String clusterName = event.getClusterName();
        final String brokerName = event.getBrokerName();
        BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final EventResponse<GetNextBrokerIdResult> resp = new EventResponse<>();
        Long nextBrokerId = null;
        if (brokerReplicaInfo == null) {
            nextBrokerId = MixAll.FIRST_BROKER_CONTROLLER_ID;
        } else {
            nextBrokerId = brokerReplicaInfo.getNextAssignBrokerId();
        }
        final GetNextBrokerIdResult result = new GetNextBrokerIdResult(clusterName, brokerName, nextBrokerId);
        resp.setResponseResult(result);
        return resp;
    }

    private EventResponse<GetReplicaInfoResult> dealGetReplicaInfo(final GetReplicaInfoEvent event) {
        final String brokerName = event.getBrokerName();
        final EventResponse<GetReplicaInfoResult> resp = new EventResponse<>();
        if (!isContainsBroker(brokerName)) {
            String msg = String.format("BrokerSet[%s]: broker set hasn't been registered in controller, but broker try to get replica info", brokerName);
            LOGGER.error(msg);
            resp.setResponse(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, msg);
            return resp;
        }
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        final Long masterBrokerId = syncStateInfo.getMasterBrokerId();
        final Integer masterEpoch = syncStateInfo.getMasterEpoch();
        final String masterAddress = brokerReplicaInfo.getBrokerAddress(masterBrokerId);
        final SyncStateSet syncStateSet = new SyncStateSet(syncStateInfo.getSyncStateSet(), syncStateInfo.getSyncStateSetEpoch());
        final GetReplicaInfoResult result = new GetReplicaInfoResult(masterBrokerId, masterAddress, masterEpoch, syncStateSet);
        resp.setResponseResult(result);
        return resp;
    }

    private EventResponse<GetSyncStateSetResult> dealGetSyncStateSet(final GetSyncStateSetEvent event) {
        final List<String> brokerNames = event.getBrokerNames();
        final EventResponse<GetSyncStateSetResult> resp = new EventResponse<>();
        final GetSyncStateSetResult result = new GetSyncStateSetResult();
        resp.setResponseResult(result);
        for (String brokerName : brokerNames) {
            if (isContainsBroker(brokerName)) {
                final BrokerReplicaInfo replicaInfo = this.replicaInfoTable.get(brokerName);
                final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
                result.addBrokerSyncStateInfo(brokerName, replicaInfo, syncStateInfo);
            }
        }
        return resp;
    }

    private EventResponse<GetNeedReElectBrokerSetsResult> dealGetNeedReElectBrokerSets(
        final GetNeedReElectBrokerSetsEvent event) {
        return null;
    }

    /**
     * Is the broker existed in the memory metadata
     *
     * @return true if both existed in replicaInfoTable and inSyncReplicasInfoTable
     */
    private boolean isContainsBroker(final String brokerName) {
        return this.replicaInfoTable.containsKey(brokerName) && this.syncStateSetInfoTable.containsKey(brokerName);
    }

    private BrokerMemberGroup buildBrokerMemberGroup(final BrokerReplicaInfo brokerReplicaInfo) {
        if (brokerReplicaInfo != null) {
            final BrokerMemberGroup group = new BrokerMemberGroup(brokerReplicaInfo.getClusterName(), brokerReplicaInfo.getBrokerName());
            final Map<Long, String> brokerIdTable = brokerReplicaInfo.getBrokerIdTable();
            final Map<Long, String> memberGroup = new HashMap<>();
            brokerIdTable.forEach((id, addr) -> memberGroup.put(id, addr));
            group.setBrokerAddrs(memberGroup);
            return group;
        }
        return null;
    }

}
