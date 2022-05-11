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
package org.apache.rocketmq.namesrv.controller.manager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.BrokerRegisterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.BrokerRegisterResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.controller.manager.event.AlterSyncStateSetEvent;
import org.apache.rocketmq.namesrv.controller.manager.event.ApplyBrokerIdEvent;
import org.apache.rocketmq.namesrv.controller.manager.event.ControllerResult;
import org.apache.rocketmq.namesrv.controller.manager.event.ElectMasterEvent;
import org.apache.rocketmq.namesrv.controller.manager.event.EventMessage;
import org.apache.rocketmq.namesrv.controller.manager.event.EventType;

/**
 * The manager that manages the replicas info for all brokers.
 * We can think of this class as the controller's memory state machine
 * It should be noted that this class is not thread safe,
 * and the upper layer needs to ensure that it can be called sequentially
 */
public class ReplicasInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final boolean enableElectUncleanMaster;
    private final Map<String/* brokerName */, BrokerInfo> replicaInfoTable;
    private final Map<String/* brokerName */, InSyncReplicasInfo> inSyncReplicasInfoTable;

    public ReplicasInfoManager(final ControllerConfig config) {
        this.enableElectUncleanMaster = config.isEnableElectUncleanMaster();
        this.replicaInfoTable = new HashMap<>();
        this.inSyncReplicasInfoTable = new HashMap<>();
    }

    public ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(
        final AlterSyncStateSetRequestHeader request, final SyncStateSet syncStateSet,
        final BiPredicate<String, String> brokerAlivePredicate) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<AlterSyncStateSetResponseHeader> result = new ControllerResult<>(new AlterSyncStateSetResponseHeader());
        final AlterSyncStateSetResponseHeader response = result.getResponse();

        if (isContainsBroker(brokerName)) {
            final Set<String> newSyncStateSet = syncStateSet.getSyncStateSet();
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);

            // Check master
            if (!replicasInfo.getMasterAddress().equals(request.getMasterAddress())) {
                String err = String.format("Rejecting alter syncStateSet request because the current leader is:{%s}, not {%s}",
                    replicasInfo.getMasterAddress(), request.getMasterAddress());
                log.error("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_MASTER, err);
                return result;
            }

            // Check master epoch
            if (request.getMasterEpoch() != replicasInfo.getMasterEpoch()) {
                String err = String.format("Rejecting alter syncStateSet request because the current master epoch is:{%d}, not {%d}",
                    replicasInfo.getMasterEpoch(), request.getMasterEpoch());
                log.error("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_MASTER_EPOCH, err);
                return result;
            }

            // Check syncStateSet epoch
            if (syncStateSet.getSyncStateSetEpoch() != replicasInfo.getSyncStateSetEpoch()) {
                String err = String.format("Rejecting alter syncStateSet request because the current syncStateSet epoch is:{%d}, not {%d}",
                    replicasInfo.getSyncStateSetEpoch(), syncStateSet.getSyncStateSetEpoch());
                log.error("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH, err);
                return result;
            }

            // Check newSyncStateSet correctness
            for (String replicas : newSyncStateSet) {
                if (!brokerInfo.isBrokerExist(replicas)) {
                    String err = String.format("Rejecting alter syncStateSet request because the replicas {%s} don't exist", replicas);
                    log.error("{}", err);
                    result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REPLICAS, err);
                    return result;
                }
                if (!brokerAlivePredicate.test(brokerInfo.getClusterName(), replicas)) {
                    String err = String.format("Rejecting alter syncStateSet request because the replicas {%s} don't alive", replicas);
                    log.error(err);
                    result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NOT_ALIVE, err);
                    return result;
                }
            }

            if (!newSyncStateSet.contains(replicasInfo.getMasterAddress())) {
                String err = String.format("Rejecting alter syncStateSet request because the newSyncStateSet don't contains origin leader {%s}", replicasInfo.getMasterAddress());
                log.error(err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REQUEST, err);
                return result;
            }

            // Generate event
            int epoch = replicasInfo.getSyncStateSetEpoch() + 1;
            response.setNewSyncStateSetEpoch(epoch);
            result.setBody(new SyncStateSet(newSyncStateSet, epoch).encode());
            final AlterSyncStateSetEvent event = new AlterSyncStateSetEvent(brokerName, newSyncStateSet);
            result.addEvent(event);
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REQUEST, "Broker metadata is not existed");
        return result;
    }

    public ControllerResult<ElectMasterResponseHeader> electMaster(
        final ElectMasterRequestHeader request, final BiPredicate<String, String> brokerAlivePredicate) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<ElectMasterResponseHeader> result = new ControllerResult<>(new ElectMasterResponseHeader());
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final Set<String> syncStateSet = replicasInfo.getSyncStateSet();
            // Try elect a master in syncStateSet
            if (syncStateSet.size() > 1) {
                boolean electSuccess = tryElectMaster(result, brokerName, syncStateSet, (candidate) ->
                    !candidate.equals(replicasInfo.getMasterAddress()) && brokerAlivePredicate.test(brokerInfo.getClusterName(), candidate));
                if (electSuccess) {
                    return result;
                }
            }

            // Try elect a master in lagging replicas if enableElectUncleanMaster = true
            if (enableElectUncleanMaster) {
                boolean electSuccess = tryElectMaster(result, brokerName, brokerInfo.getAllBroker(), (candidate) ->
                    !candidate.equals(replicasInfo.getMasterAddress()) && brokerAlivePredicate.test(brokerInfo.getClusterName(), candidate));
                if (electSuccess) {
                    return result;
                }
            }

            // If elect failed, we still need to apply an ElectMasterEvent to tell the statemachine
            // that the master was shutdown and no new master was elected.
            final ElectMasterEvent event = new ElectMasterEvent(false, brokerName);
            result.addEvent(event);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE, "Failed to elect a new broker master");
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REQUEST, "Broker metadata is not existed");
        return result;
    }

    /**
     * Try elect a new master in candidates
     *
     * @param filter return true if the candidate is available
     * @return true if elect success
     */
    private boolean tryElectMaster(final ControllerResult<ElectMasterResponseHeader> result, final String brokerName,
        final Set<String> candidates, final Predicate<String> filter) {
        final int masterEpoch = this.inSyncReplicasInfoTable.get(brokerName).getMasterEpoch();
        for (final String candidate : candidates) {
            if (filter.test(candidate)) {
                final ElectMasterResponseHeader response = result.getResponse();
                response.setNewMasterAddress(candidate);
                response.setMasterEpoch(masterEpoch + 1);

                final ElectMasterEvent event = new ElectMasterEvent(brokerName, candidate);
                result.addEvent(event);
                return true;
            }
        }
        return false;
    }

    public ControllerResult<BrokerRegisterResponseHeader> registerBroker(final BrokerRegisterRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final String brokerAddress = request.getBrokerAddress();
        final ControllerResult<BrokerRegisterResponseHeader> result = new ControllerResult<>(new BrokerRegisterResponseHeader());
        final BrokerRegisterResponseHeader response = result.getResponse();
        boolean canBeElectedAsMaster;
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);

            // Get brokerId.
            long brokerId;
            if (!brokerInfo.isBrokerExist(brokerAddress)) {
                // If this broker replicas is first time come online, we need to apply a new id for this replicas.
                brokerId = brokerInfo.newBrokerId();
                final ApplyBrokerIdEvent applyIdEvent = new ApplyBrokerIdEvent(request.getBrokerName(), brokerAddress, brokerId);
                result.addEvent(applyIdEvent);
            } else {
                brokerId = brokerInfo.getBrokerId(brokerAddress);
            }
            response.setBrokerId(brokerId);
            response.setMasterEpoch(replicasInfo.getMasterEpoch());
            response.setSyncStateSetEpoch(replicasInfo.getSyncStateSetEpoch());

            if (replicasInfo.isMasterExist()) {
                // If the master is alive, just return master info.
                final String masterAddress = replicasInfo.getMasterAddress();
                response.setMasterAddress(masterAddress);
                return result;
            } else {
                // If the master is not alive, we should elect a new master:
                // Case1: This replicas was in sync state set list
                // Case2: The option {EnableElectUncleanMaster} is true
                canBeElectedAsMaster = replicasInfo.getSyncStateSet().contains(brokerAddress) || this.enableElectUncleanMaster;
            }
        } else {
            // If the broker's metadata does not exist in the state machine, the replicas can be elected as master directly.
            canBeElectedAsMaster = true;
        }

        if (canBeElectedAsMaster) {
            final boolean isBrokerExist = isContainsBroker(brokerName);
            int masterEpoch = isBrokerExist ? this.inSyncReplicasInfoTable.get(brokerName).getMasterEpoch() + 1 : 1;
            int syncStateSetEpoch = isBrokerExist ? this.inSyncReplicasInfoTable.get(brokerName).getSyncStateSetEpoch() + 1 : 1;
            response.setMasterAddress(request.getBrokerAddress());
            response.setMasterEpoch(masterEpoch);
            response.setSyncStateSetEpoch(syncStateSetEpoch);
            response.setBrokerId(MixAll.MASTER_ID);

            final ElectMasterEvent event = new ElectMasterEvent(true, brokerName, brokerAddress, request.getClusterName());
            result.addEvent(event);
            return result;
        }

        response.setMasterAddress("");
        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REQUEST, "The broker has not master, and this new registered broker can't not be elected as master");
        return result;
    }

    public ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<GetReplicaInfoResponseHeader> result = new ControllerResult<>(new GetReplicaInfoResponseHeader());
        final GetReplicaInfoResponseHeader response = result.getResponse();
        if (isContainsBroker(brokerName)) {
            // If exist broker metadata, just return metadata
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final String masterAddress = replicasInfo.getMasterAddress();
            response.setMasterAddress(masterAddress);
            response.setMasterEpoch(replicasInfo.getMasterEpoch());
            result.setBody(new SyncStateSet(replicasInfo.getSyncStateSet(), replicasInfo.getSyncStateSetEpoch()).encode());
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REQUEST, "Broker metadata is not existed");
        return result;
    }

    /**
     * Apply events to memory statemachine.
     *
     * @param event event message
     */
    public void applyEvent(final EventMessage event) {
        final EventType type = event.getEventType();
        switch (type) {
            case ALTER_SYNC_STATE_SET_EVENT:
                handleAlterSyncStateSet((AlterSyncStateSetEvent) event);
                break;
            case APPLY_BROKER_ID_EVENT:
                handleApplyBrokerId((ApplyBrokerIdEvent) event);
                break;
            case ELECT_MASTER_EVENT:
                handleElectMaster((ElectMasterEvent) event);
                break;
            default:
                break;
        }
    }

    private void handleAlterSyncStateSet(final AlterSyncStateSetEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            replicasInfo.updateSyncStateSetInfo(event.getNewSyncStateSet());
        }
    }

    private void handleApplyBrokerId(final ApplyBrokerIdEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            if (!brokerInfo.isBrokerExist(event.getBrokerAddress())) {
                brokerInfo.addBroker(event.getBrokerAddress(), event.getNewBrokerId());
            }
        }
    }

    private void handleElectMaster(final ElectMasterEvent event) {
        final String brokerName = event.getBrokerName();
        final String newMaster = event.getNewMasterAddress();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);

            if (event.getNewMasterElected()) {
                // Record new master
                replicasInfo.updateMasterInfo(newMaster);

                // Record new newSyncStateSet list
                final HashSet<String> newSyncStateSet = new HashSet<>();
                newSyncStateSet.add(newMaster);
                replicasInfo.updateSyncStateSetInfo(newSyncStateSet);
            } else {
                // If new master was not elected, which means old master was shutdown and the newSyncStateSet list had no more replicas
                // So we should delete old master, but retain newSyncStateSet list.
                replicasInfo.updateMasterInfo("");
            }
        } else {
            // When the first replicas of a broker come online,
            // we can create memory meta information for the broker, and regard it as master
            final String clusterName = event.getClusterName();
            final BrokerInfo brokerInfo = new BrokerInfo(clusterName, brokerName);
            brokerInfo.addBroker(newMaster, 1L);
            final InSyncReplicasInfo replicasInfo = new InSyncReplicasInfo(clusterName, brokerName, newMaster);
            this.inSyncReplicasInfoTable.put(brokerName, replicasInfo);
            this.replicaInfoTable.put(brokerName, brokerInfo);
        }
    }

    /**
     * Is the broker existed in the memory metadata
     *
     * @return true if both existed in replicaInfoTable and inSyncReplicasInfoTable
     */
    private boolean isContainsBroker(final String brokerName) {
        return this.replicaInfoTable.containsKey(brokerName) && this.inSyncReplicasInfoTable.containsKey(brokerName);
    }
}
