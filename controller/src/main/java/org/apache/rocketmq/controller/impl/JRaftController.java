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

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.controller.impl.closure.ControllerClosure;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetSyncStateDataRequest;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class JRaftController implements Controller {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final RaftGroupService raftGroupService;
    private Node node;
    private final JRaftControllerStateMachine stateMachine;
    private final ControllerConfig controllerConfig;
    private final List<BrokerLifecycleListener> brokerLifecycleListeners;
    private final Map<PeerId/* jRaft peerId */, String/* Controller RPC Server Addr */> peerIdToAddr;
    private final NettyRemotingServer remotingServer;

    public JRaftController(ControllerConfig controllerConfig,
        final ChannelEventListener channelEventListener) throws IOException {
        this.controllerConfig = controllerConfig;
        this.brokerLifecycleListeners = new ArrayList<>();

        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(controllerConfig.getjRaftElectionTimeoutMs());
        nodeOptions.setSnapshotIntervalSecs(controllerConfig.getjRaftSnapshotIntervalSecs());
        final PeerId serverId = new PeerId();
        if (!serverId.parse(controllerConfig.getjRaftServerId())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + controllerConfig.getjRaftServerId());
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(controllerConfig.getjRaftInitConf())) {
            throw new IllegalArgumentException("Fail to parse initConf:" + controllerConfig.getjRaftInitConf());
        }
        nodeOptions.setInitialConf(initConf);

        FileUtils.forceMkdir(new File(controllerConfig.getControllerStorePath()));
        nodeOptions.setLogUri(controllerConfig.getControllerStorePath() + File.separator + "log");
        nodeOptions.setRaftMetaUri(controllerConfig.getControllerStorePath() + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(controllerConfig.getControllerStorePath() + File.separator + "snapshot");

        this.stateMachine = new JRaftControllerStateMachine(controllerConfig, new NodeId(controllerConfig.getjRaftGroupId(), serverId));
        this.stateMachine.registerOnLeaderStart(this::onLeaderStart);
        this.stateMachine.registerOnLeaderStop(this::onLeaderStop);
        nodeOptions.setFsm(this.stateMachine);

        this.raftGroupService = new RaftGroupService(controllerConfig.getjRaftGroupId(), serverId, nodeOptions);

        this.peerIdToAddr = new HashMap<>();
        initPeerIdMap();

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(Integer.parseInt(this.peerIdToAddr.get(serverId).split(":")[1]));
        remotingServer = new NettyRemotingServer(nettyServerConfig, channelEventListener);
    }

    private void initPeerIdMap() {
        String[] peers = this.controllerConfig.getjRaftInitConf().split(",");
        String[] rpcAddrs = this.controllerConfig.getjRaftControllerRPCAddr().split(",");
        for (int i = 0; i < peers.length; i++) {
            PeerId peerId = new PeerId();
            if (!peerId.parse(peers[i])) {
                throw new IllegalArgumentException("Fail to parse peerId:" + peers[i]);
            }
            this.peerIdToAddr.put(peerId, rpcAddrs[i]);
        }
    }

    @Override
    public void startup() {
        this.remotingServer.start();
        this.node = this.raftGroupService.start();
        log.info("Controller {} started.", node.getNodeId());
    }

    @Override
    public void shutdown() {
        this.stopScheduling();
        this.raftGroupService.shutdown();
        this.remotingServer.shutdown();
        log.info("Controller {} stopped.", node.getNodeId());
    }

    @Override
    public void startScheduling() {
    }

    @Override
    public void stopScheduling() {
    }

    @Override
    public boolean isLeaderState() {
        return node.isLeader();
    }

    private <T extends CommandCustomHeader> CompletableFuture<RemotingCommand> applyToJRaft(RemotingCommand request) {
        if (!isLeaderState()) {
            final RemotingCommand command = RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_NOT_LEADER, "The controller is not in leader state");
            final CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            future.complete(command);
            log.warn("Apply to none leader controller, controller state is {}", node.getNodeState());
            return future;
        }
        ControllerClosure closure = new ControllerClosure(request);
        Task task = closure.taskWithThisClosure();
        if (task != null) {
            node.apply(task);
            return closure.getFuture();
        } else {
            log.error("Apply task failed, task is null.");
            return CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR, "Apply task failed, Please see the server log."));
        }
    }

    @Override
    public CompletableFuture<RemotingCommand> alterSyncStateSet(AlterSyncStateSetRequestHeader request,
        SyncStateSet syncStateSet) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, request);
        requestCommand.setBody(syncStateSet.encode());
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> electMaster(ElectMasterRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ELECT_MASTER, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> getNextBrokerId(GetNextBrokerIdRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_NEXT_BROKER_ID, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> applyBrokerId(ApplyBrokerIdRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_APPLY_BROKER_ID, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_REGISTER_BROKER, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> getReplicaInfo(GetReplicaInfoRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_REPLICA_INFO, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> getSyncStateData(List<String> brokerNames) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, new GetSyncStateDataRequest());
        requestCommand.setBody(RemotingSerializable.encode(brokerNames));
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> cleanBrokerData(CleanControllerBrokerDataRequestHeader requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CLEAN_BROKER_DATA, requestHeader);
        return applyToJRaft(requestCommand);
    }

    @Override
    public void registerBrokerLifecycleListener(BrokerLifecycleListener listener) {
        this.brokerLifecycleListeners.add(listener);
    }

    @Override
    public RemotingCommand getControllerMetadata() {
        List<PeerId> peers = node.getOptions().getInitialConf().getPeers();
        final StringBuilder sb = new StringBuilder();
        for (PeerId peer : peers) {
            sb.append(peerIdToAddr.get(peer)).append(";");
        }
        return RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, new GetMetaDataResponseHeader(
            node.getGroupId(),
            node.getLeaderId() == null ? "" : node.getLeaderId().toString(),
            this.peerIdToAddr.get(node.getLeaderId()),
            node.isLeader(),
            sb.toString()
        ));
    }

    @Override
    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void onLeaderStart(long term) {
        log.info("Controller start leadership, term: {}.", term);
    }

    public void onLeaderStop(Status status) {
        log.info("Controller {} stop leadership, status: {}.", node.getNodeId(), status);
        this.stopScheduling();
    }

    public CompletableFuture<RemotingCommand> getBrokerLiveInfo(GetBrokerLiveInfoRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_LIVE_INFO_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }

    public CompletableFuture<RemotingCommand> onBrokerHeartBeat(RaftBrokerHeartBeatEventRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.RAFT_BROKER_HEART_BEAT_EVENT_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }

    public CompletableFuture<RemotingCommand> onBrokerCloseChannel(BrokerCloseChannelRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.BROKER_CLOSE_CHANNEL_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }

    public CompletableFuture<RemotingCommand> checkNotActiveBroker(CheckNotActiveBrokerRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CHECK_NOT_ACTIVE_BROKER_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }
}
