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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.dledger.manager.ReplicasInfoManager;
import org.apache.rocketmq.controller.dledger.statemachine.DLedgerControllerStateMachine;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventSerializer;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
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

    private ElectPolicy electPolicy;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public NewDLedgerController(final ControllerConfig controllerConfig, final BrokerValidPredicate brokerValidPredicate,
        final NettyServerConfig nettyServerConfig, final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener, final ElectPolicy electPolicy) {
        this.controllerConfig = controllerConfig;
        this.brokerValidPredicate = brokerValidPredicate;
        this.dLedgerConfig = buildDLedgerConfig(controllerConfig);
        this.selfId = this.dLedgerConfig.getSelfId();
        this.roleChangeHandler = new RoleChangeHandler();
        this.replicasInfoManager = new ReplicasInfoManager(controllerConfig);
        this.stateMachine = new DLedgerControllerStateMachine(this.replicasInfoManager, new WriteEventSerializer(), this.dLedgerConfig);

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
        this.start.compareAndSet(false, true) {
            this.dLedgerServer.startup();
        }
    }

    @Override
    public void shutdown() {
        this.start.compareAndSet(true, false) {
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
    public CompletableFuture<AlterSyncStateSetResponseHeader> alterSyncStateSet(AlterSyncStateSetRequestHeader request,
        SyncStateSet syncStateSet) {
        final String brokerName = request.getBrokerName();
        final Long masterBrokerId = request.getMasterBrokerId();
        final Integer masterEpoch = request.getMasterEpoch();
        final Integer syncStateSetEpoch = syncStateSet.getSyncStateSetEpoch();
        final Set<Long> newSyncStateSets = syncStateSet.getSyncStateSet();

        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> electMaster(ElectMasterRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> getNextBrokerId(GetNextBrokerIdRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> applyBrokerId(ApplyBrokerIdRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> getReplicaInfo(GetReplicaInfoRequestHeader request) {
        return null;
    }

    @Override
    public RemotingCommand getControllerMetadata() {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> getSyncStateData(List<String> brokerNames) {
        return null;
    }

    @Override
    public void registerBrokerLifecycleListener(BrokerLifecycleListener listener) {

    }

    @Override
    public RemotingServer getRemotingServer() {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> cleanBrokerData(
        CleanControllerBrokerDataRequestHeader requestHeader) {
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
}
