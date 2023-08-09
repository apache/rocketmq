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

package org.apache.rocketmq.controller.dledger;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.common.ReadClosure;
import io.openmessaging.storage.dledger.common.ReadMode;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.common.WriteClosure;
import io.openmessaging.storage.dledger.common.WriteTask;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.dledger.manager.BrokerReplicaInfo;
import org.apache.rocketmq.controller.dledger.manager.ReplicasInfoManager;
import org.apache.rocketmq.controller.dledger.manager.SyncStateInfo;
import org.apache.rocketmq.controller.dledger.statemachine.DLedgerControllerStateMachine;
import org.apache.rocketmq.controller.dledger.statemachine.EventResponse;
import org.apache.rocketmq.controller.dledger.statemachine.event.EventResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNextBrokerIdEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetNextBrokerIdResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetReplicaInfoEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetReplicaInfoResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetSyncStateSetEvent;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.GetSyncStateSetResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.ReadEventMessage;
import org.apache.rocketmq.controller.dledger.statemachine.event.read.ReadEventResult;
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
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventResult;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventSerializer;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;
import org.apache.rocketmq.controller.helper.ValidBrokerGetter;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewDLedgerController implements Controller {

    private static final Logger log = LoggerFactory.getLogger(NewDLedgerController.class);

    private final DLedgerServer dLedgerServer;

    private final DLedgerConfig dLedgerConfig;

    private final ControllerConfig controllerConfig;

    private final ReplicasInfoManager replicasInfoManager;

    private final DLedgerControllerStateMachine stateMachine;

    private final List<BrokerLifecycleListener> brokerLifecycleListeners;

    private final RoleChangeHandler roleChangeHandler;

    private final String selfId;

    private BrokerValidPredicate brokerValidPredicate;

    private ValidBrokerGetter validBrokerGetter;

    private ElectPolicy electPolicy;

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final WriteEventSerializer writeEventSerializer = new WriteEventSerializer();

    public NewDLedgerController(final ControllerConfig controllerConfig,
        final BrokerValidPredicate brokerValidPredicate, final ValidBrokerGetter validBrokerGetter,
        final NettyServerConfig nettyServerConfig, final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener, final ElectPolicy electPolicy) {
        this.controllerConfig = controllerConfig;
        this.brokerValidPredicate = brokerValidPredicate;
        this.validBrokerGetter = validBrokerGetter;
        this.dLedgerConfig = buildDLedgerConfig(controllerConfig);
        this.selfId = this.dLedgerConfig.getSelfId();
        this.roleChangeHandler = new RoleChangeHandler();
        this.replicasInfoManager = new ReplicasInfoManager(controllerConfig);
        this.stateMachine = new DLedgerControllerStateMachine(this.replicasInfoManager, writeEventSerializer, this.dLedgerConfig);

        this.dLedgerServer = new DLedgerServer(this.dLedgerConfig, nettyServerConfig, nettyClientConfig, channelEventListener, this.stateMachine);
        this.dLedgerServer.getDLedgerLeaderElector().addRoleChangeHandler(this.roleChangeHandler);
        this.brokerLifecycleListeners = new LinkedList<>();
    }

    private DLedgerConfig buildDLedgerConfig(final ControllerConfig controllerConfig) {
        DLedgerConfig dLedgerConfig = new DLedgerConfig();
        dLedgerConfig.setGroup(controllerConfig.getControllerDLegerGroup());
        dLedgerConfig.setPeers(controllerConfig.getControllerDLegerPeers());
        dLedgerConfig.setSelfId(controllerConfig.getControllerDLegerSelfId());
        dLedgerConfig.setStoreBaseDir(controllerConfig.getControllerStorePath());
        dLedgerConfig.setMappedFileSizeForEntryData(controllerConfig.getMappedFileSize());
        return dLedgerConfig;
    }

    @Override
    public void startup() {
        if (this.start.compareAndSet(false, true)) {
            this.dLedgerServer.startup();
        }
    }

    @Override
    public void shutdown() {
        if (this.start.compareAndSet(true, false)) {
            this.dLedgerServer.shutdown();
        }
    }

    @Override
    public void startScheduling() {

    }

    @Override
    public void stopScheduling() {

    }

    @Override
    public boolean isLeaderState() {
        return false;
    }

    @Override
    public CompletableFuture<RemotingCommand> alterSyncStateSet(AlterSyncStateSetRequestHeader request,
        SyncStateSet syncStateSet) {
        final String brokerName = request.getBrokerName();
        final Long masterBrokerId = request.getMasterBrokerId();
        final Integer masterEpoch = request.getMasterEpoch();
        final Integer syncStateSetEpoch = syncStateSet.getSyncStateSetEpoch();
        final Set<Long> newSyncStateSets = syncStateSet.getSyncStateSet();
        final AlterSyncStateSetEvent event = new AlterSyncStateSetEvent(brokerName, masterBrokerId, masterEpoch, newSyncStateSets, syncStateSetEpoch);
        byte[] data = writeEventSerializer.serialize(event);
        final WriteTask task = new WriteTask();
        task.setBody(data);
        final CompletableFuture<EventResponse<AlterSyncStateSetResult>> future = new CompletableFuture<>();
        final ControllerWriteClosure writeClosure = new ControllerWriteClosure(future, event);
        dLedgerServer.handleWrite(task, writeClosure);
        return future.thenApply(resp -> {
            AlterSyncStateSetResult alterSyncStateSetResult = resp.getResponseResult();
            AlterSyncStateSetResponseHeader alterSyncStateSetResponseHeader = new AlterSyncStateSetResponseHeader();
            alterSyncStateSetResponseHeader.setNewSyncStateSetEpoch(alterSyncStateSetResult.getNewSyncStateSetEpoch());
            RemotingCommand command = RemotingCommand.createResponseCommandWithHeader(resp.getResponseCode(), alterSyncStateSetResponseHeader);
            command.setRemark(resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> electMaster(ElectMasterRequestHeader request) {
        final String clusterName = request.getClusterName();
        final String brokerName = request.getBrokerName();
        final Long brokerId = request.getBrokerId();
        final boolean designateElect = request.getDesignateElect();
        final List<Long> aliveBrokers = validBrokerGetter.get(clusterName, brokerName);
        final ElectMasterEvent event = new ElectMasterEvent(clusterName, brokerName, brokerId, designateElect, aliveBrokers);
        byte[] data = writeEventSerializer.serialize(event);
        final WriteTask task = new WriteTask();
        task.setBody(data);
        final CompletableFuture<EventResponse<ElectMasterResult>> future = new CompletableFuture<>();
        final ControllerWriteClosure writeClosure = new ControllerWriteClosure(future, event);
        dLedgerServer.handleWrite(task, writeClosure);
        return future.thenApply(resp -> {
            ElectMasterResult electMasterResult = resp.getResponseResult();
            ElectMasterResponseHeader electMasterResponseHeader = new ElectMasterResponseHeader();
            electMasterResponseHeader.setMasterBrokerId(electMasterResult.getMasterBrokerId());
            electMasterResponseHeader.setMasterAddress(electMasterResult.getMasterAddress());
            electMasterResponseHeader.setMasterEpoch(electMasterResult.getMasterEpoch());
            electMasterResponseHeader.setSyncStateSetEpoch(electMasterResult.getSyncStateSetEpoch());
            RemotingCommand command = RemotingCommand.createResponseCommandWithHeader(resp.getResponseCode(), electMasterResponseHeader);
            command.setRemark(resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> getNextBrokerId(GetNextBrokerIdRequestHeader request) {
        final String clusterName = request.getClusterName();
        final String brokerName = request.getBrokerName();
        final CompletableFuture<EventResponse<GetNextBrokerIdResult>> future = new CompletableFuture<>();
        final GetNextBrokerIdEvent event = new GetNextBrokerIdEvent(clusterName, brokerName);
        final ControllerReadClosure closure = new ControllerReadClosure(future, event);
        dLedgerServer.handleRead(ReadMode.RAFT_LOG_READ, closure);
        return future.thenApply(resp -> {
            GetNextBrokerIdResult getNextBrokerIdResult = resp.getResponseResult();
            GetNextBrokerIdResponseHeader getNextBrokerIdResponseHeader = new GetNextBrokerIdResponseHeader();
            getNextBrokerIdResponseHeader.setNextBrokerId(getNextBrokerIdResult.getNextBrokerId());
            RemotingCommand command = RemotingCommand.createResponseCommandWithHeader(resp.getResponseCode(), getNextBrokerIdResponseHeader);
            command.setRemark(resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> applyBrokerId(ApplyBrokerIdRequestHeader request) {
        final String clusterName = request.getClusterName();
        final String brokerName = request.getBrokerName();
        final Long appliedBrokerId = request.getAppliedBrokerId();
        final String registerCheckCode = request.getRegisterCheckCode();
        final ApplyBrokerIdEvent event = new ApplyBrokerIdEvent(clusterName, brokerName, appliedBrokerId, registerCheckCode);
        byte[] data = writeEventSerializer.serialize(event);
        final WriteTask task = new WriteTask();
        task.setBody(data);
        final CompletableFuture<EventResponse<ApplyBrokerIdResult>> future = new CompletableFuture<>();
        final ControllerWriteClosure writeClosure = new ControllerWriteClosure(future, event);
        dLedgerServer.handleWrite(task, writeClosure);
        return future.thenApply(resp -> {
            ApplyBrokerIdResult applyBrokerIdResult = resp.getResponseResult();
            ApplyBrokerIdResponseHeader applyBrokerIdResponseHeader = new ApplyBrokerIdResponseHeader();
            applyBrokerIdResponseHeader.setClusterName(applyBrokerIdResult.getClusterName());
            applyBrokerIdResponseHeader.setBrokerName(applyBrokerIdResult.getBrokerName());
            RemotingCommand command = RemotingCommand.createResponseCommandWithHeader(resp.getResponseCode(), applyBrokerIdResponseHeader);
            command.setRemark(resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        final String clusterName = request.getClusterName();
        final String brokerName = request.getBrokerName();
        final String brokerAddress = request.getBrokerAddress();
        final Long brokerId = request.getBrokerId();
        final RegisterBrokerEvent event = new RegisterBrokerEvent(clusterName, brokerName, brokerAddress, brokerId);
        byte[] data = writeEventSerializer.serialize(event);
        final WriteTask task = new WriteTask();
        task.setBody(data);
        final CompletableFuture<EventResponse<RegisterBrokerResult>> future = new CompletableFuture<>();
        final ControllerWriteClosure writeClosure = new ControllerWriteClosure(future, event);
        dLedgerServer.handleWrite(task, writeClosure);
        return future.thenApply(resp -> {
            RegisterBrokerResult registerBrokerResult = resp.getResponseResult();
            RegisterBrokerToControllerResponseHeader registerBrokerResponseHeader = new RegisterBrokerToControllerResponseHeader();
            registerBrokerResponseHeader.setClusterName(registerBrokerResult.getClusterName());
            registerBrokerResponseHeader.setBrokerName(registerBrokerResult.getBrokerName());
            registerBrokerResponseHeader.setMasterBrokerId(registerBrokerResult.getMasterBrokerId());
            registerBrokerResponseHeader.setMasterAddress(registerBrokerResult.getMasterAddress());
            registerBrokerResponseHeader.setMasterEpoch(registerBrokerResult.getMasterEpoch());
            registerBrokerResponseHeader.setSyncStateSetEpoch(registerBrokerResult.getSyncStateSetEpoch());
            RemotingCommand command = RemotingCommand.createResponseCommandWithHeader(resp.getResponseCode(), registerBrokerResponseHeader);
            command.setRemark(resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> getReplicaInfo(GetReplicaInfoRequestHeader request) {
        String brokerName = request.getBrokerName();
        CompletableFuture<EventResponse<GetReplicaInfoResult>> future = new CompletableFuture<>();
        GetReplicaInfoEvent event = new GetReplicaInfoEvent(brokerName);
        ControllerReadClosure closure = new ControllerReadClosure(future, event);
        dLedgerServer.handleRead(ReadMode.RAFT_LOG_READ, closure);
        return future.thenApply(resp -> {
            GetReplicaInfoResult getReplicaInfoResult = resp.getResponseResult();
            GetReplicaInfoResponseHeader getReplicaInfoResponseHeader = new GetReplicaInfoResponseHeader();
            getReplicaInfoResponseHeader.setMasterAddress(getReplicaInfoResult.getMasterAddress());
            getReplicaInfoResponseHeader.setMasterBrokerId(getReplicaInfoResult.getMasterBrokerId());
            getReplicaInfoResponseHeader.setMasterEpoch(getReplicaInfoResult.getMasterEpoch());
            RemotingCommand command = RemotingCommand.createResponseCommandWithHeader(resp.getResponseCode(), getReplicaInfoResponseHeader);
            command.setRemark(resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public RemotingCommand getControllerMetadata() {
        final MemberState state = this.dLedgerServer.getMemberState();
        final Map<String, String> peers = state.getPeerMap();
        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : peers.entrySet()) {
            final String peer = entry.getKey() + ":" + entry.getValue();
            sb.append(peer).append(";");
        }
        return RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, new GetMetaDataResponseHeader(
            state.getGroup(), state.getLeaderId(), state.getLeaderAddr(), state.isLeader(), sb.toString()));
    }

    @Override
    public CompletableFuture<RemotingCommand> getSyncStateData(List<String> brokerNames) {
        GetSyncStateSetEvent event = new GetSyncStateSetEvent(brokerNames);
        CompletableFuture<EventResponse<GetSyncStateSetResult>> future = new CompletableFuture<>();
        ControllerReadClosure closure = new ControllerReadClosure(future, event);
        dLedgerServer.handleRead(ReadMode.RAFT_LOG_READ, closure);
        return future.thenApply(resp -> {
            final GetSyncStateSetResult result = resp.getResponseResult();
            final BrokerReplicasInfo replicasInfo = new BrokerReplicasInfo();

            result.getBrokerSyncStateInfoMap().forEach((brokerName, pair) -> {
                final List<BrokerReplicasInfo.ReplicaIdentity> inSyncReplicas = new ArrayList<>();
                final List<BrokerReplicasInfo.ReplicaIdentity> outSyncReplicas = new ArrayList<>();
                final BrokerReplicaInfo replicaInfo = pair.getObject1();
                final SyncStateInfo syncStateInfo = pair.getObject2();

                replicaInfo.getBrokerIdTable().forEach((brokerId, brokerAddress) -> {
                    boolean isAlive = brokerValidPredicate.check(replicaInfo.getClusterName(), brokerName, brokerId);
                    BrokerReplicasInfo.ReplicaIdentity replica = new BrokerReplicasInfo.ReplicaIdentity(brokerName, brokerId, brokerAddress, isAlive);
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
                replicasInfo.addReplicaInfo(brokerName, syncState);
            });
            RemotingCommand command = RemotingCommand.createResponseCommand(resp.getResponseCode(), resp.getResponseMsg());
            command.setBody(replicasInfo.encode());
            return command;
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> cleanBrokerData(CleanControllerBrokerDataRequestHeader requestHeader) {
        final String clusterName = requestHeader.getClusterName();
        final String brokerName = requestHeader.getBrokerName();
        final String brokerIds = requestHeader.getBrokerControllerIdsToClean();
        Set<Long> brokerIdSet = new HashSet<>();
        if (StringUtils.isNotBlank(brokerIds)) {
            // check if brokerId is valid
            try {
                brokerIdSet = Stream.of(brokerIds.split(";")).map(Long::parseLong).collect(Collectors.toSet());
            } catch (NumberFormatException numberFormatException) {
                String remark = String.format("Please set the option <brokerControllerIdsToClean> according to the format, exception: %s", numberFormatException);
                return CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, remark));
            }
        }
        final CleanBrokerDataEvent event = new CleanBrokerDataEvent(clusterName, brokerName, brokerIdSet);
        byte[] data = writeEventSerializer.serialize(event);
        WriteTask task = new WriteTask();
        task.setBody(data);
        final CompletableFuture<EventResponse<CleanBrokerDataResult>> future = new CompletableFuture<>();
        final ControllerWriteClosure closure = new ControllerWriteClosure(future, event);
        dLedgerServer.handleWrite(task, closure);
        return future.thenApply(resp -> {
            RemotingCommand command = RemotingCommand.createResponseCommand(resp.getResponseCode(), resp.getResponseMsg());
            return command;
        });
    }

    @Override
    public void registerBrokerLifecycleListener(BrokerLifecycleListener listener) {

    }

    @Override
    public RemotingServer getRemotingServer() {
        return null;
    }

    class RoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

        @Override
        public void handle(long term, MemberState.Role role) {

        }

        @Override
        public void startup() {

        }

        @Override
        public void shutdown() {

        }
    }

    class ControllerWriteClosure<T extends WriteEventResult> extends WriteClosure<EventResponse<T>> {

        private final WriteEventMessage event;

        private EventResponse<T> resp;

        private final CompletableFuture<EventResponse<T>> future;

        public ControllerWriteClosure(CompletableFuture<EventResponse<T>> future, WriteEventMessage event) {
            this.future = future;
            this.event = event;
        }

        @Override
        public void setResp(EventResponse<T> resp) {
            this.resp = resp;
        }

        @Override
        public EventResponse<T> getResp() {
            return resp;
        }

        @Override
        public void done(Status status) {
            if (!status.isOk()) {
                String msg = String.format("Failed to write event: %s, code: %s", event, status.code);
                log.error(msg);
                resp.setResponse(ResponseCode.CONTROLLER_INNER_ERROR, msg);
            }
            future.complete(resp);
        }
    }

    class ControllerReadClosure<T extends ReadEventResult> extends ReadClosure {

        private final ReadEventMessage event;

        private final CompletableFuture<EventResponse<T>> future;

        private EventResponse<T> resp;

        public ControllerReadClosure(CompletableFuture<EventResponse<T>> future, ReadEventMessage event) {
            this.future = future;
            this.event = event;
        }

        @Override
        public void done(Status status) {
            if (!status.isOk()) {
                String msg = String.format("Failed to read event: %s, code: %s", event, status.code);
                log.error(msg);
                resp.setResponse(ResponseCode.CONTROLLER_INNER_ERROR, msg);
            } else {
                EventResponse<? extends EventResult> response = replicasInfoManager.applyEvent(event);
                if (!(response.getResponseResult() instanceof ReadEventResult)) {
                    String msg = String.format("Failed to read event: %s, invalid result type: %s", event, response.getResponseResult().getClass().getSimpleName());
                    log.error(msg);
                    resp.setResponse(ResponseCode.CONTROLLER_INNER_ERROR, msg);
                } else {
                    resp = (EventResponse<T>) response;
                }
            }
            future.complete(resp);
        }
    }
}
