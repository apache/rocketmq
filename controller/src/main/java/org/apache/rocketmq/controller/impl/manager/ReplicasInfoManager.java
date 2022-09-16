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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.common.protocol.body.InSyncStateData;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.impl.event.AlterSyncStateSetEvent;
import org.apache.rocketmq.controller.impl.event.ApplyBrokerIdEvent;
import org.apache.rocketmq.controller.impl.event.CleanBrokerDataEvent;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.ElectMasterEvent;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.event.EventType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * The manager that manages the replicas info for all brokers. We can think of this class as the controller's memory
 * state machine It should be noted that this class is not thread safe, and the upper layer needs to ensure that it can
 * be called sequentially
 */
public class ReplicasInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ControllerConfig controllerConfig;
    private final Map<String/* brokerName */, BrokerInfo> replicaInfoTable;
    private final Map<String/* brokerName */, SyncStateInfo> syncStateSetInfoTable;

    public ReplicasInfoManager(final ControllerConfig config) {
        this.controllerConfig = config;
        this.replicaInfoTable = new HashMap<>();
        this.syncStateSetInfoTable = new HashMap<>();
    }

    public ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(
        final AlterSyncStateSetRequestHeader request, final SyncStateSet syncStateSet,
        final BiPredicate<String, String> brokerAlivePredicate) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<AlterSyncStateSetResponseHeader> result = new ControllerResult<>(new AlterSyncStateSetResponseHeader());
        final AlterSyncStateSetResponseHeader response = result.getResponse();

        if (isContainsBroker(brokerName)) {
            final Set<String> newSyncStateSet = syncStateSet.getSyncStateSet();
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);

            // Check whether the oldSyncStateSet is equal with newSyncStateSet
            final Set<String> oldSyncStateSet = syncStateInfo.getSyncStateSet();
            if (oldSyncStateSet.size() == newSyncStateSet.size() && oldSyncStateSet.containsAll(newSyncStateSet)) {
                String err = "The newSyncStateSet is equal with oldSyncStateSet, no needed to update syncStateSet";
                log.warn("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, err);
                return result;
            }

            // Check master
            if (!syncStateInfo.getMasterAddress().equals(request.getMasterAddress())) {
                String err = String.format("Rejecting alter syncStateSet request because the current leader is:{%s}, not {%s}",
                    syncStateInfo.getMasterAddress(), request.getMasterAddress());
                log.error("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_MASTER, err);
                return result;
            }

            // Check master epoch
            if (request.getMasterEpoch() != syncStateInfo.getMasterEpoch()) {
                String err = String.format("Rejecting alter syncStateSet request because the current master epoch is:{%d}, not {%d}",
                    syncStateInfo.getMasterEpoch(), request.getMasterEpoch());
                log.error("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_MASTER_EPOCH, err);
                return result;
            }

            // Check syncStateSet epoch
            if (syncStateSet.getSyncStateSetEpoch() != syncStateInfo.getSyncStateSetEpoch()) {
                String err = String.format("Rejecting alter syncStateSet request because the current syncStateSet epoch is:{%d}, not {%d}",
                    syncStateInfo.getSyncStateSetEpoch(), syncStateSet.getSyncStateSetEpoch());
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

            if (!newSyncStateSet.contains(syncStateInfo.getMasterAddress())) {
                String err = String.format("Rejecting alter syncStateSet request because the newSyncStateSet don't contains origin leader {%s}", syncStateInfo.getMasterAddress());
                log.error(err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, err);
                return result;
            }

            // Generate event
            int epoch = syncStateInfo.getSyncStateSetEpoch() + 1;
            response.setNewSyncStateSetEpoch(epoch);
            result.setBody(new SyncStateSet(newSyncStateSet, epoch).encode());
            final AlterSyncStateSetEvent event = new AlterSyncStateSetEvent(brokerName, newSyncStateSet);
            result.addEvent(event);
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, "Broker metadata is not existed");
        return result;
    }

    public ControllerResult<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request,
        final ElectPolicy electPolicy) {
        final String brokerName = request.getBrokerName();
        final String assignBrokerAddress = request.getBrokerAddress();
        final ControllerResult<ElectMasterResponseHeader> result = new ControllerResult<>(new ElectMasterResponseHeader());
        if (isContainsBroker(brokerName)) {
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final Set<String> syncStateSet = syncStateInfo.getSyncStateSet();
            final String oldMaster = syncStateInfo.getMasterAddress();
            Set<String> allReplicaBrokers = controllerConfig.isEnableElectUncleanMaster() ? brokerInfo.getAllBroker() : null;

            // elect by policy
            String newMaster = electPolicy.elect(brokerInfo.getClusterName(), syncStateSet, allReplicaBrokers, oldMaster, assignBrokerAddress);
            if (StringUtils.isNotEmpty(newMaster) && newMaster.equals(oldMaster)) {
                // old master still valid, change nothing
                String err = String.format("The old master %s is still alive, not need to elect new master for broker %s", oldMaster, brokerInfo.getBrokerName());
                log.warn("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, err);
                return result;
            }
            // a new master is elected
            if (StringUtils.isNotEmpty(newMaster)) {
                final int masterEpoch = this.syncStateSetInfoTable.get(brokerName).getMasterEpoch();
                final int syncStateSetEpoch = this.syncStateSetInfoTable.get(brokerName).getSyncStateSetEpoch();
                final ElectMasterResponseHeader response = result.getResponse();
                response.setNewMasterAddress(newMaster);
                response.setMasterEpoch(masterEpoch + 1);
                response.setSyncStateSetEpoch(syncStateSetEpoch);
                BrokerMemberGroup brokerMemberGroup = buildBrokerMemberGroup(brokerName);
                if (null != brokerMemberGroup) {
                    response.setBrokerMemberGroup(brokerMemberGroup);
                    result.setBody(brokerMemberGroup.encode());
                }
                final ElectMasterEvent event = new ElectMasterEvent(brokerName, newMaster);
                result.addEvent(event);
                return result;
            }
            // If elect failed, we still need to apply an ElectMasterEvent to tell the statemachine
            // that the master was shutdown and no new master was elected.
            final ElectMasterEvent event = new ElectMasterEvent(false, brokerName);
            result.addEvent(event);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE, "Failed to elect a new broker master");
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, "Broker metadata is not existed");
        return result;
    }

    private BrokerMemberGroup buildBrokerMemberGroup(final String brokerName) {
        if (isContainsBroker(brokerName)) {
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final BrokerMemberGroup group = new BrokerMemberGroup(brokerInfo.getClusterName(), brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();
            final HashMap<Long, String> memberGroup = new HashMap<>();
            brokerIdTable.forEach((addr, id) -> memberGroup.put(id, addr));
            group.setBrokerAddrs(memberGroup);
            return group;
        }
        return null;
    }

    public ControllerResult<RegisterBrokerToControllerResponseHeader> registerBroker(
        final RegisterBrokerToControllerRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final String brokerAddress = request.getBrokerAddress();
        final ControllerResult<RegisterBrokerToControllerResponseHeader> result = new ControllerResult<>(new RegisterBrokerToControllerResponseHeader());
        final RegisterBrokerToControllerResponseHeader response = result.getResponse();
        boolean canBeElectedAsMaster;
        if (isContainsBroker(brokerName)) {
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
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
            response.setMasterEpoch(syncStateInfo.getMasterEpoch());
            response.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());

            if (syncStateInfo.isMasterExist()) {
                // If the master is alive, just return master info.
                final String masterAddress = syncStateInfo.getMasterAddress();
                response.setMasterAddress(masterAddress);
                return result;
            } else {
                // If the master is not alive, we should elect a new master:
                // Case1: This replicas was in sync state set list
                // Case2: The option {EnableElectUncleanMaster} is true
                canBeElectedAsMaster = syncStateInfo.getSyncStateSet().contains(brokerAddress) || this.controllerConfig.isEnableElectUncleanMaster();
            }
        } else {
            // If the broker's metadata does not exist in the state machine, the replicas can be elected as master directly.
            canBeElectedAsMaster = true;
        }

        if (canBeElectedAsMaster) {
            final boolean isBrokerExist = isContainsBroker(brokerName);
            int masterEpoch = isBrokerExist ? this.syncStateSetInfoTable.get(brokerName).getMasterEpoch() + 1 : 1;
            int syncStateSetEpoch = isBrokerExist ? this.syncStateSetInfoTable.get(brokerName).getSyncStateSetEpoch() + 1 : 1;
            response.setMasterAddress(request.getBrokerAddress());
            response.setMasterEpoch(masterEpoch);
            response.setSyncStateSetEpoch(syncStateSetEpoch);
            response.setBrokerId(MixAll.MASTER_ID);

            final ElectMasterEvent event = new ElectMasterEvent(true, brokerName, brokerAddress, request.getClusterName());
            result.addEvent(event);
            return result;
        }

        response.setMasterAddress("");
        result.setCodeAndRemark(ResponseCode.CONTROLLER_REGISTER_BROKER_FAILED, "The broker has not master, and this new registered broker can't not be elected as master");
        return result;
    }

    public ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<GetReplicaInfoResponseHeader> result = new ControllerResult<>(new GetReplicaInfoResponseHeader());
        final GetReplicaInfoResponseHeader response = result.getResponse();
        if (isContainsBroker(brokerName)) {
            // If exist broker metadata, just return metadata
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final String masterAddress = syncStateInfo.getMasterAddress();
            response.setMasterAddress(masterAddress);
            response.setMasterEpoch(syncStateInfo.getMasterEpoch());
            if (StringUtils.isNotEmpty(request.getBrokerAddress())) {
                response.setBrokerId(brokerInfo.getBrokerId(request.getBrokerAddress()));
            }
            result.setBody(new SyncStateSet(syncStateInfo.getSyncStateSet(), syncStateInfo.getSyncStateSetEpoch()).encode());
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST, "Broker metadata is not existed");
        return result;
    }

    public ControllerResult<Void> getSyncStateData(final List<String> brokerNames) {
        final ControllerResult<Void> result = new ControllerResult<>();
        final InSyncStateData inSyncStateData = new InSyncStateData();
        for (String brokerName : brokerNames) {
            if (isContainsBroker(brokerName)) {
                // If exist broker metadata, just return metadata
                final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
                final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
                final Set<String> syncStateSet = syncStateInfo.getSyncStateSet();
                final String master = syncStateInfo.getMasterAddress();
                final ArrayList<InSyncStateData.InSyncMember> inSyncMembers = new ArrayList<>();
                syncStateSet.forEach(replicas -> {
                    long brokerId = StringUtils.equals(master, replicas) ? MixAll.MASTER_ID : brokerInfo.getBrokerId(replicas);
                    inSyncMembers.add(new InSyncStateData.InSyncMember(replicas, brokerId));
                });

                final InSyncStateData.InSyncStateSet inSyncState = new InSyncStateData.InSyncStateSet(master, syncStateInfo.getMasterEpoch(), syncStateInfo.getSyncStateSetEpoch(), inSyncMembers);
                inSyncStateData.addInSyncState(brokerName, inSyncState);
            }
        }
        result.setBody(inSyncStateData.encode());
        return result;
    }

    public ControllerResult<Void> cleanBrokerData(final CleanControllerBrokerDataRequestHeader requestHeader,
        final BiPredicate<String, String> brokerAlivePredicate) {
        final ControllerResult<Void> result = new ControllerResult<>();

        final String clusterName = requestHeader.getClusterName();
        final String brokerName = requestHeader.getBrokerName();
        final String brokerAddrs = requestHeader.getBrokerAddress();

        Set<String> brokerAddressSet = null;
        if (!requestHeader.isCleanLivingBroker()) {
            //if SyncStateInfo.masterAddress is not empty, at least one broker with the same BrokerName is alive
            SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            if (StringUtils.isBlank(brokerAddrs) && null != syncStateInfo && StringUtils.isNotEmpty(syncStateInfo.getMasterAddress())) {
                String remark = String.format("Broker %s is still alive, clean up failure", requestHeader.getBrokerName());
                result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, remark);
                return result;
            }
            if (StringUtils.isNotBlank(brokerAddrs)) {
                brokerAddressSet = Stream.of(brokerAddrs.split(";")).collect(Collectors.toSet());
                for (String brokerAddr : brokerAddressSet) {
                    if (brokerAlivePredicate.test(clusterName, brokerAddr)) {
                        String remark = String.format("Broker [%s,  %s] is still alive, clean up failure", requestHeader.getBrokerName(), brokerAddr);
                        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, remark);
                        return result;
                    }
                }
            }
        }
        if (isContainsBroker(brokerName)) {
            final CleanBrokerDataEvent event = new CleanBrokerDataEvent(brokerName, brokerAddressSet);
            result.addEvent(event);
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, String.format("Broker %s is not existed,clean broker data failure.", brokerName));
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
            case CLEAN_BROKER_DATA_EVENT:
                handleCleanBrokerDataEvent((CleanBrokerDataEvent) event);
                break;
            default:
                break;
        }
    }

    private void handleAlterSyncStateSet(final AlterSyncStateSetEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            syncStateInfo.updateSyncStateSetInfo(event.getNewSyncStateSet());
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
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);

            if (event.getNewMasterElected()) {
                // Record new master
                syncStateInfo.updateMasterInfo(newMaster);

                // Record new newSyncStateSet list
                final HashSet<String> newSyncStateSet = new HashSet<>();
                newSyncStateSet.add(newMaster);
                syncStateInfo.updateSyncStateSetInfo(newSyncStateSet);
            } else {
                // If new master was not elected, which means old master was shutdown and the newSyncStateSet list had no more replicas
                // So we should delete old master, but retain newSyncStateSet list.
                syncStateInfo.updateMasterInfo("");
            }
        } else {
            // When the first replicas of a broker come online,
            // we can create memory meta information for the broker, and regard it as master
            final String clusterName = event.getClusterName();
            final BrokerInfo brokerInfo = new BrokerInfo(clusterName, brokerName);
            brokerInfo.addBroker(newMaster, 1L);
            final SyncStateInfo syncStateInfo = new SyncStateInfo(clusterName, brokerName, newMaster);
            this.syncStateSetInfoTable.put(brokerName, syncStateInfo);
            this.replicaInfoTable.put(brokerName, brokerInfo);
        }
    }

    private void handleCleanBrokerDataEvent(final CleanBrokerDataEvent event) {

        final String brokerName = event.getBrokerName();
        final Set<String> brokerAddressSet = event.getBrokerAddressSet();

        if (null == brokerAddressSet || brokerAddressSet.isEmpty()) {
            this.replicaInfoTable.remove(brokerName);
            this.syncStateSetInfoTable.remove(brokerName);
            return;
        }
        if (!isContainsBroker(brokerName)) {
            return;
        }
        final BrokerInfo brokerInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        for (String brokerAddress : brokerAddressSet) {
            brokerInfo.removeBrokerAddress(brokerAddress);
            syncStateInfo.removeSyncState(brokerAddress);
        }
        if (brokerInfo.getBrokerIdTable().isEmpty()) {
            this.replicaInfoTable.remove(brokerName);
        }
        if (syncStateInfo.getSyncStateSet().isEmpty()) {
            this.syncStateSetInfoTable.remove(brokerName);
        }
    }

    /**
     * Is the broker existed in the memory metadata
     *
     * @return true if both existed in replicaInfoTable and inSyncReplicasInfoTable
     */
    private boolean isContainsBroker(final String brokerName) {
        return this.replicaInfoTable.containsKey(brokerName) && this.syncStateSetInfoTable.containsKey(brokerName);
    }
}
