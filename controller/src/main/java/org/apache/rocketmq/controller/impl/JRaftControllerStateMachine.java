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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.closure.ControllerClosure;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.manager.RaftReplicasInfoManager;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetSyncStateDataRequest;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.controller.metrics.ControllerMetricsConstant;
import org.apache.rocketmq.controller.metrics.ControllerMetricsManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_BROKER_SET;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_CLUSTER_NAME;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_ELECTION_RESULT;

public class JRaftControllerStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final List<Consumer<Long>> onLeaderStartCallbacks;
    private final List<Consumer<Status>> onLeaderStopCallbacks;
    private final RaftReplicasInfoManager replicasInfoManager;
    private final NodeId nodeId;

    public JRaftControllerStateMachine(ControllerConfig controllerConfig, NodeId nodeId) {
        this.replicasInfoManager = new RaftReplicasInfoManager(controllerConfig);
        this.nodeId = nodeId;
        this.onLeaderStartCallbacks = new ArrayList<>();
        this.onLeaderStopCallbacks = new ArrayList<>();
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            byte[] data = iter.getData().array();
            ControllerClosure controllerClosure = (ControllerClosure) iter.done();
            processEvent(controllerClosure, data, iter.getTerm(), iter.getIndex());

            iter.next();
        }
    }

    private void processEvent(ControllerClosure controllerClosure, byte[] data, long term, long index) {
        RemotingCommand request;
        ControllerResult<?> result;
        try {
            if (controllerClosure != null) {
                request = controllerClosure.getRequestEvent();
            } else {
                request = RemotingCommand.decode(Arrays.copyOfRange(data, 4, data.length));
            }
            log.info("process event: term {}, index {}, request code {}", term, index, request.getCode());
            switch (request.getCode()) {
                case RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET:
                    AlterSyncStateSetRequestHeader requestHeader = (AlterSyncStateSetRequestHeader) request.decodeCommandCustomHeader(AlterSyncStateSetRequestHeader.class);
                    SyncStateSet syncStateSet = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);
                    result = alterSyncStateSet(requestHeader, syncStateSet);
                    break;
                case RequestCode.CONTROLLER_ELECT_MASTER:
                    ElectMasterRequestHeader electMasterRequestHeader = (ElectMasterRequestHeader) request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
                    result = electMaster(electMasterRequestHeader);
                    break;
                case RequestCode.CONTROLLER_GET_NEXT_BROKER_ID:
                    GetNextBrokerIdRequestHeader getNextBrokerIdRequestHeader = (GetNextBrokerIdRequestHeader) request.decodeCommandCustomHeader(GetNextBrokerIdRequestHeader.class);
                    result = getNextBrokerId(getNextBrokerIdRequestHeader);
                    break;
                case RequestCode.CONTROLLER_APPLY_BROKER_ID:
                    ApplyBrokerIdRequestHeader applyBrokerIdRequestHeader = (ApplyBrokerIdRequestHeader) request.decodeCommandCustomHeader(ApplyBrokerIdRequestHeader.class);
                    result = applyBrokerId(applyBrokerIdRequestHeader);
                    break;
                case RequestCode.CONTROLLER_REGISTER_BROKER:
                    RegisterBrokerToControllerRequestHeader registerBrokerToControllerRequestHeader = (RegisterBrokerToControllerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerToControllerRequestHeader.class);
                    result = registerBroker(registerBrokerToControllerRequestHeader);
                    break;
                case RequestCode.CONTROLLER_GET_REPLICA_INFO:
                    GetReplicaInfoRequestHeader getReplicaInfoRequestHeader = (GetReplicaInfoRequestHeader) request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
                    result = getReplicaInfo(getReplicaInfoRequestHeader);
                    break;
                case RequestCode.CONTROLLER_GET_SYNC_STATE_DATA:
                    List<String> brokerNames = RemotingSerializable.decode(request.getBody(), List.class);
                    GetSyncStateDataRequest getSyncStateDataRequest = (GetSyncStateDataRequest) request.decodeCommandCustomHeader(GetSyncStateDataRequest.class);
                    result = getSyncStateData(brokerNames, getSyncStateDataRequest.getInvokeTime());
                    break;
                case RequestCode.CLEAN_BROKER_DATA:
                    CleanControllerBrokerDataRequestHeader cleanBrokerDataRequestHeader = (CleanControllerBrokerDataRequestHeader) request.decodeCommandCustomHeader(CleanControllerBrokerDataRequestHeader.class);
                    result = cleanBrokerData(cleanBrokerDataRequestHeader);
                    break;
                case RequestCode.GET_BROKER_LIVE_INFO_REQUEST:
                    GetBrokerLiveInfoRequest getBrokerLiveInfoRequest = (GetBrokerLiveInfoRequest) request.decodeCommandCustomHeader(GetBrokerLiveInfoRequest.class);
                    result = replicasInfoManager.getBrokerLiveInfo(getBrokerLiveInfoRequest);
                    break;
                case RequestCode.RAFT_BROKER_HEART_BEAT_EVENT_REQUEST:
                    RaftBrokerHeartBeatEventRequest brokerHeartbeatRequestHeader = (RaftBrokerHeartBeatEventRequest) request.decodeCommandCustomHeader(RaftBrokerHeartBeatEventRequest.class);
                    result = replicasInfoManager.onBrokerHeartBeat(brokerHeartbeatRequestHeader);
                    break;
                case RequestCode.BROKER_CLOSE_CHANNEL_REQUEST:
                    BrokerCloseChannelRequest brokerCloseChannelRequest = (BrokerCloseChannelRequest) request.decodeCommandCustomHeader(BrokerCloseChannelRequest.class);
                    result = replicasInfoManager.onBrokerCloseChannel(brokerCloseChannelRequest);
                    break;
                case RequestCode.CHECK_NOT_ACTIVE_BROKER_REQUEST:
                    CheckNotActiveBrokerRequest checkNotActiveBrokerRequest = (CheckNotActiveBrokerRequest) request.decodeCommandCustomHeader(CheckNotActiveBrokerRequest.class);
                    result = replicasInfoManager.checkNotActiveBroker(checkNotActiveBrokerRequest);
                    break;
                default:
                    throw new RemotingCommandException("Unknown request code: " + request.getCode());
            }
            result.getEvents().forEach(replicasInfoManager::applyEvent);
        } catch (RemotingCommandException e) {
            log.error("Fail to process event", e);
            if (controllerClosure != null) {
                controllerClosure.run(new Status(RaftError.EINTERNAL, e.getMessage()));
            }
            return;
        }
        log.info("process event: term {}, index {}, request code {} success with result {}", term, index, request.getCode(), result.toString());
        if (controllerClosure != null) {
            controllerClosure.setControllerResult(result);
            controllerClosure.run(Status.OK());
        }
    }

    private ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(
        AlterSyncStateSetRequestHeader requestHeader, SyncStateSet syncStateSet) {
        return replicasInfoManager.alterSyncStateSet(requestHeader, syncStateSet, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), this.replicasInfoManager));
    }

    private ControllerResult<ElectMasterResponseHeader> electMaster(ElectMasterRequestHeader request) {
        ControllerResult<ElectMasterResponseHeader> electResult = this.replicasInfoManager.electMaster(request, new DefaultElectPolicy(
            (clusterName, brokerName, brokerId) -> replicasInfoManager.isBrokerActive(clusterName, brokerName, brokerId, request.getInvokeTime()),
            replicasInfoManager::getBrokerLiveInfo
        ));
        log.info("elect master, request :{}, result: {}", request.toString(), electResult.toString());
        AttributesBuilder attributesBuilder = ControllerMetricsManager.newAttributesBuilder()
            .put(LABEL_CLUSTER_NAME, request.getClusterName())
            .put(LABEL_BROKER_SET, request.getBrokerName());
        switch (electResult.getResponseCode()) {
            case ResponseCode.SUCCESS:
                ControllerMetricsManager.electionTotal.add(1,
                    attributesBuilder.put(LABEL_ELECTION_RESULT, ControllerMetricsConstant.ElectionResult.NEW_MASTER_ELECTED.getLowerCaseName()).build());
                break;
            case ResponseCode.CONTROLLER_MASTER_STILL_EXIST:
                ControllerMetricsManager.electionTotal.add(1,
                    attributesBuilder.put(LABEL_ELECTION_RESULT, ControllerMetricsConstant.ElectionResult.KEEP_CURRENT_MASTER.getLowerCaseName()).build());
                break;
            case ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE:
            case ResponseCode.CONTROLLER_ELECT_MASTER_FAILED:
                ControllerMetricsManager.electionTotal.add(1,
                    attributesBuilder.put(LABEL_ELECTION_RESULT, ControllerMetricsConstant.ElectionResult.NO_MASTER_ELECTED.getLowerCaseName()).build());
                break;
            default:
                break;
        }
        return electResult;
    }

    private ControllerResult<GetNextBrokerIdResponseHeader> getNextBrokerId(
        GetNextBrokerIdRequestHeader requestHeader) {
        return replicasInfoManager.getNextBrokerId(requestHeader);
    }

    private ControllerResult<ApplyBrokerIdResponseHeader> applyBrokerId(ApplyBrokerIdRequestHeader requestHeader) {
        return replicasInfoManager.applyBrokerId(requestHeader);
    }

    private ControllerResult<?> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        return replicasInfoManager.registerBroker(request, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(request.getInvokeTime(), this.replicasInfoManager));
    }

    private ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(GetReplicaInfoRequestHeader request) {
        return replicasInfoManager.getReplicaInfo(request);
    }

    private ControllerResult<Void> getSyncStateData(List<String> brokerNames, long invokeTile) {
        return replicasInfoManager.getSyncStateData(brokerNames, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(invokeTile, this.replicasInfoManager));
    }

    private ControllerResult<Void> cleanBrokerData(CleanControllerBrokerDataRequestHeader requestHeader) {
        return replicasInfoManager.cleanBrokerData(requestHeader, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), this.replicasInfoManager));
    }

    @Override
    public void onShutdown() {
        log.info("StateMachine {} node {} onShutdown", getClass().getName(), nodeId.toString());
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        byte[] data;
        try {
            data = this.replicasInfoManager.serialize();
        } catch (Throwable e) {
            done.run(new Status(RaftError.EIO, "Fail to serialize replicasInfoManager state machine data"));
            return;
        }
        Utils.runInThread(() -> {
            try {
                FileUtils.writeByteArrayToFile(new File(writer.getPath() + File.separator + "data"), data);
                if (writer.addFile("data")) {
                    log.info("Save snapshot, path={}", writer.getPath());
                    done.run(Status.OK());
                } else {
                    throw new IOException("Fail to add file to writer");
                }
            } catch (IOException e) {
                log.error("Fail to save snapshot", e);
                done.run(new Status(RaftError.EIO, "Fail to save snapshot"));
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (reader.getFileMeta("data") == null) {
            log.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        try {
            byte[] data = FileUtils.readFileToByteArray(new File(reader.getPath() + File.separator + "data"));
            this.replicasInfoManager.deserializeFrom(data);
            log.info("Load snapshot from {}", reader.getPath());
            return true;
        } catch (Throwable e) {
            log.error("Fail to load snapshot from {}", reader.getPath(), e);
            return false;
        }
    }

    @Override
    public void onLeaderStart(long term) {
        for (Consumer<Long> callback : onLeaderStartCallbacks) {
            callback.accept(term);
        }
        log.info("node {} Start Leader, term={}", nodeId.toString(), term);
    }

    @Override
    public void onLeaderStop(Status status) {
        for (Consumer<Status> callback : onLeaderStopCallbacks) {
            callback.accept(status);
        }
        log.info("node {} Stop Leader, status={}", nodeId.toString(), status);
    }

    public void registerOnLeaderStart(Consumer<Long> callback) {
        onLeaderStartCallbacks.add(callback);
    }

    public void registerOnLeaderStop(Consumer<Status> callback) {
        onLeaderStopCallbacks.add(callback);
    }

    @Override
    public void onError(RaftException e) {
        log.error("Encountered an error={} on StateMachine {}, node {}, raft may stop working since some error occurs, you should figure out the cause and repair or remove this node.", e.getStatus(), this.getClass().getName(), nodeId.toString(), e);
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        log.info("Configuration committed, conf={}", conf);
    }

    @Override
    public void onStopFollowing(LeaderChangeContext ctx) {
        log.info("Stop following, ctx={}", ctx);
    }

    @Override
    public void onStartFollowing(LeaderChangeContext ctx) {
        log.info("Start following, ctx={}", ctx);
    }
}
