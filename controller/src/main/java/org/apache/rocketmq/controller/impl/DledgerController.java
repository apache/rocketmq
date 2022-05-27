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

import io.openmessaging.storage.dledger.AppendFuture;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.BrokerRegisterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.event.EventSerializer;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * The implementation of controller, based on dledger (raft).
 */
public class DledgerController implements Controller {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final DLedgerServer dLedgerServer;
    private final ControllerConfig controllerConfig;
    private final DLedgerConfig dLedgerConfig;
    // Usr for checking whether the broker is alive
    private final BiPredicate<String, String> brokerAlivePredicate;
    private final ReplicasInfoManager replicasInfoManager;
    private final EventScheduler scheduler;
    private final EventSerializer eventSerializer;
    private final RoleChangeHandler roleHandler;
    private final DledgerControllerStateMachine statemachine;
    private volatile boolean isScheduling = false;

    public DledgerController(final ControllerConfig config, final BiPredicate<String, String> brokerAlivePredicate) {
        this.controllerConfig = config;
        this.eventSerializer = new EventSerializer();
        this.scheduler = new EventScheduler();
        this.brokerAlivePredicate = brokerAlivePredicate;

        this.dLedgerConfig = new DLedgerConfig();
        this.dLedgerConfig.setGroup(config.getControllerDLegerGroup());
        this.dLedgerConfig.setPeers(config.getControllerDLegerPeers());
        this.dLedgerConfig.setSelfId(config.getControllerDLegerSelfId());
        this.dLedgerConfig.setStoreBaseDir(config.getControllerStorePath());
        this.dLedgerConfig.setMappedFileSizeForEntryData(config.getMappedFileSize());

        this.roleHandler = new RoleChangeHandler(dLedgerConfig.getSelfId());
        this.replicasInfoManager = new ReplicasInfoManager(config);
        this.statemachine = new DledgerControllerStateMachine(replicasInfoManager, this.eventSerializer, dLedgerConfig.getSelfId());

        // Register statemachine and role handler.
        this.dLedgerServer = new DLedgerServer(dLedgerConfig);
        this.dLedgerServer.registerStateMachine(this.statemachine);
        this.dLedgerServer.getdLedgerLeaderElector().addRoleChangeHandler(this.roleHandler);
    }

    @Override
    public void startup() {
        this.dLedgerServer.startup();
    }

    @Override
    public void shutdown() {
        this.dLedgerServer.shutdown();
    }

    @Override
    public void startScheduling() {
        if (!this.isScheduling) {
            log.info("Start scheduling controller events");
            this.isScheduling = true;
            this.scheduler.start();
        }
    }

    @Override
    public void stopScheduling() {
        if (this.isScheduling) {
            log.info("Stop scheduling controller events");
            this.isScheduling = false;
            this.scheduler.shutdown(true);
        }
    }

    @Override
    public boolean isLeaderState() {
        return this.roleHandler.isLeaderState();
    }

    @Override
    public CompletableFuture<RemotingCommand> alterSyncStateSet(AlterSyncStateSetRequestHeader request,
        final SyncStateSet syncStateSet) {
        return this.scheduler.appendEvent("alterSyncStateSet",
            () -> this.replicasInfoManager.alterSyncStateSet(request, syncStateSet, this.brokerAlivePredicate), true);
    }

    @Override
    public CompletableFuture<RemotingCommand> electMaster(final ElectMasterRequestHeader request) {
        return this.scheduler.appendEvent("electMaster",
            () -> this.replicasInfoManager.electMaster(request, this.brokerAlivePredicate), true);
    }

    @Override
    public CompletableFuture<RemotingCommand> registerBroker(BrokerRegisterRequestHeader request) {
        return this.scheduler.appendEvent("registerBroker",
            () -> this.replicasInfoManager.registerBroker(request), true);
    }

    @Override
    public CompletableFuture<RemotingCommand> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
        return this.scheduler.appendEvent("getReplicaInfo",
            () -> this.replicasInfoManager.getReplicaInfo(request), false);
    }

    @Override
    public CompletableFuture<RemotingCommand> getSyncStateData(List<String> brokerNames) {
        return this.scheduler.appendEvent("getSyncStateData",
            () -> this.replicasInfoManager.getInSyncStateData(brokerNames), false);
    }

    @Override
    public RemotingCommand getControllerMetadata() {
        final MemberState state = getMemberState();
        return RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, new GetMetaDataResponseHeader(state.getLeaderId(), state.getLeaderAddr(), state.isLeader()));
    }

    /**
     * Append the request to dledger, wait the dledger to commit the request.
     */
    private boolean appendToDledgerAndWait(final AppendEntryRequest request) throws Throwable {
        if (request != null) {
            request.setGroup(this.dLedgerConfig.getGroup());
            request.setRemoteId(this.dLedgerConfig.getSelfId());

            final AppendFuture<AppendEntryResponse> dledgerFuture = (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
            if (dledgerFuture.getPos() == -1) {
                return false;
            }
            dledgerFuture.get(5, TimeUnit.SECONDS);
            return true;
        }
        return false;
    }

    // Only for test
    public MemberState getMemberState() {
        return this.dLedgerServer.getMemberState();
    }

    /**
     * Event handler that handle event
     */
    interface EventHandler<T> {
        /**
         * Run the controller event
         */
        void run() throws Throwable;

        /**
         * Return the completableFuture
         */
        CompletableFuture<RemotingCommand> future();

        /**
         * Handle Exception.
         */
        void handleException(final Throwable t);
    }

    /**
     * Event scheduler, schedule event handler from event queue
     */
    class EventScheduler extends ServiceThread {
        private final BlockingQueue<EventHandler> eventQueue;

        public EventScheduler() {
            this.eventQueue = new LinkedBlockingQueue<>(1024);
        }

        @Override
        public String getServiceName() {
            return EventScheduler.class.getName();
        }

        @Override
        public void run() {
            log.info("Start event scheduler.");
            while (!isStopped()) {
                EventHandler handler;
                try {
                    handler = this.eventQueue.poll(5, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    continue;
                }
                try {
                    if (handler != null) {
                        handler.run();
                    }
                } catch (final Throwable e) {
                    handler.handleException(e);
                }
            }
        }

        public <T> CompletableFuture<RemotingCommand> appendEvent(final String name,
            final Supplier<ControllerResult<T>> supplier, boolean isWriteEvent) {
            if (isStopped() || !DledgerController.this.roleHandler.isLeaderState()) {
                final RemotingCommand command = RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_NOT_LEADER, "The controller is not in leader state");
                final CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
                future.complete(command);
                return future;
            }

            final EventHandler<T> event = new ControllerEventHandler<>(name, supplier, isWriteEvent);
            int tryTimes = 0;
            while (true) {
                try {
                    if (!this.eventQueue.offer(event, 5, TimeUnit.SECONDS)) {
                        continue;
                    }
                    return event.future();
                } catch (final InterruptedException e) {
                    log.error("Error happen in EventScheduler when append event", e);
                    tryTimes++;
                    if (tryTimes > 3) {
                        return null;
                    }
                }
            }
        }
    }

    /**
     * Event handler, get events from supplier, and append events to dledger
     */
    class ControllerEventHandler<T> implements EventHandler<T> {
        private final String name;
        private final Supplier<ControllerResult<T>> supplier;
        private final CompletableFuture<RemotingCommand> future;
        private final boolean isWriteEvent;

        ControllerEventHandler(final String name, final Supplier<ControllerResult<T>> supplier,
            final boolean isWriteEvent) {
            this.name = name;
            this.supplier = supplier;
            this.future = new CompletableFuture<>();
            this.isWriteEvent = isWriteEvent;
        }

        @Override
        public void run() throws Throwable {
            final ControllerResult<T> result = this.supplier.get();
            log.info("Event queue run event {}, get the result {}", this.name, result);
            boolean appendSuccess = true;
            if (this.isWriteEvent) {
                final List<EventMessage> events = result.getEvents();
                final List<byte[]> eventBytes = new ArrayList<>(events.size());
                for (final EventMessage event : events) {
                    if (event != null) {
                        final byte[] data = DledgerController.this.eventSerializer.serialize(event);
                        if (data != null && data.length > 0) {
                            eventBytes.add(data);
                        }
                    }
                }
                // Append events to dledger
                if (!eventBytes.isEmpty()) {
                    final BatchAppendEntryRequest request = new BatchAppendEntryRequest();
                    request.setBatchMsgs(eventBytes);
                    appendSuccess = appendToDledgerAndWait(request);
                }
            } else {
                if (DledgerController.this.controllerConfig.isProcessReadEvent()) {
                    // Now the dledger don't have the function of Read-Index or Lease-Read,
                    // So we still need to propose an empty request to dledger.
                    final AppendEntryRequest request = new AppendEntryRequest();
                    request.setBody(new byte[0]);
                    appendSuccess = appendToDledgerAndWait(request);
                }
            }
            if (appendSuccess) {
                final RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(result.getResponseCode(), (CommandCustomHeader) result.getResponse());
                if (result.getBody() != null) {
                    response.setBody(result.getBody());
                }
                if (result.getRemark() != null) {
                    response.setRemark(result.getRemark());
                }
                this.future.complete(response);
            } else {
                log.error("Failed to append event to dledger, the response is {}, try cancel the future", result.getResponse());
                this.future.cancel(true);
            }
        }

        @Override
        public CompletableFuture<RemotingCommand> future() {
            return this.future;
        }

        @Override
        public void handleException(final Throwable t) {
            log.error("Error happen when handle event {}", this.name, t);
            this.future.completeExceptionally(t);
        }
    }

    /**
     * Role change handler, trigger the startScheduling() and stopScheduling() when role change.
     */
    class RoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

        private final String selfId;
        private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("DLedgerControllerRoleChangeHandler_"));
        private volatile MemberState.Role currentRole = MemberState.Role.FOLLOWER;

        public RoleChangeHandler(final String selfId) {
            this.selfId = selfId;
        }

        @Override
        public void handle(long term, MemberState.Role role) {
            Runnable runnable = () -> {
                switch (role) {
                    case CANDIDATE:
                        this.currentRole = MemberState.Role.CANDIDATE;
                        log.info("Controller {} change role to candidate", this.selfId);
                        DledgerController.this.stopScheduling();
                        break;
                    case FOLLOWER:
                        this.currentRole = MemberState.Role.FOLLOWER;
                        log.info("Controller {} change role to Follower, leaderId:{}", this.selfId, getMemberState().getLeaderId());
                        DledgerController.this.stopScheduling();
                        break;
                    case LEADER: {
                        this.currentRole = MemberState.Role.LEADER;
                        log.info("Controller {} change role to leader, try process a initial proposal", this.selfId);
                        // Because the role becomes to leader, but the memory statemachine of the controller is still in the old point,
                        // some committed logs have not been applied. Therefore, we must first process an empty request to dledger,
                        // and after the request is committed, the controller can provide services(startScheduling).
                        int tryTimes = 0;
                        while (true) {
                            final AppendEntryRequest request = new AppendEntryRequest();
                            request.setBody(new byte[0]);
                            try {
                                if (appendToDledgerAndWait(request)) {
                                    DledgerController.this.startScheduling();
                                    break;
                                }
                            } catch (final Throwable e) {
                                log.error("Error happen when controller leader append initial request to dledger", e);
                                tryTimes++;
                                if (tryTimes > 2) {
                                    log.warn("Controller leader append initial log failed too many times, please wait a while");
                                    tryTimes = 0;
                                }
                            }
                        }
                        break;
                    }
                }
            };
            this.executorService.submit(runnable);
        }

        @Override
        public void startup() {
        }

        @Override
        public void shutdown() {
            if (this.currentRole == MemberState.Role.LEADER) {
                DledgerController.this.stopScheduling();
            }
            this.executorService.shutdown();
        }

        public boolean isLeaderState() {
            return this.currentRole == MemberState.Role.LEADER;
        }
    }
}
