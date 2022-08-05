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
package org.apache.rocketmq.controller.processor;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.BrokerHeartbeatRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import static org.apache.rocketmq.common.protocol.RequestCode.BROKER_HEARTBEAT;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_ELECT_MASTER;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_METADATA_INFO;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_REPLICA_INFO;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_SYNC_STATE_DATA;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_REGISTER_BROKER;

/**
 * Processor for controller request
 */
public class ControllerRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final int WAIT_TIMEOUT_OUT = 5;
    private final Controller controller;
    private final BrokerHeartbeatManager heartbeatManager;

    public ControllerRequestProcessor(final ControllerManager controllerManager) {
        this.controller = controllerManager.getController();
        this.heartbeatManager = controllerManager.getHeartbeatManager();
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (ctx != null) {
            log.debug("Receive request, {} {} {}",
                request.getCode(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                request);
        }
        switch (request.getCode()) {
            case CONTROLLER_ALTER_SYNC_STATE_SET: {
                final AlterSyncStateSetRequestHeader controllerRequest = (AlterSyncStateSetRequestHeader) request.decodeCommandCustomHeader(AlterSyncStateSetRequestHeader.class);
                final SyncStateSet syncStateSet = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);
                final CompletableFuture<RemotingCommand> future = this.controller.alterSyncStateSet(controllerRequest, syncStateSet);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_ELECT_MASTER: {
                final ElectMasterRequestHeader controllerRequest = (ElectMasterRequestHeader) request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controller.electMaster(controllerRequest);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_REGISTER_BROKER: {
                final RegisterBrokerToControllerRequestHeader controllerRequest = (RegisterBrokerToControllerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerToControllerRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controller.registerBroker(controllerRequest);
                if (future != null) {
                    final RemotingCommand response = future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                    final RegisterBrokerToControllerResponseHeader responseHeader = (RegisterBrokerToControllerResponseHeader) response.readCustomHeader();
                    if (responseHeader != null && responseHeader.getBrokerId() >= 0) {
                        this.heartbeatManager.registerBroker(controllerRequest.getClusterName(), controllerRequest.getBrokerName(), controllerRequest.getBrokerAddress(),
                            responseHeader.getBrokerId(), controllerRequest.getHeartbeatTimeoutMillis(), ctx.channel());
                    }
                    return response;
                }
                break;
            }
            case CONTROLLER_GET_REPLICA_INFO: {
                final GetReplicaInfoRequestHeader controllerRequest = (GetReplicaInfoRequestHeader) request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controller.getReplicaInfo(controllerRequest);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_GET_METADATA_INFO: {
                return this.controller.getControllerMetadata();
            }
            case BROKER_HEARTBEAT: {
                final BrokerHeartbeatRequestHeader requestHeader = (BrokerHeartbeatRequestHeader) request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);
                this.heartbeatManager.onBrokerHeartbeat(requestHeader.getClusterName(), requestHeader.getBrokerAddr());
                return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Heart beat success");
            }
            case CONTROLLER_GET_SYNC_STATE_DATA: {
                if (request.getBody() != null) {
                    final List<String> brokerNames = RemotingSerializable.decode(request.getBody(), List.class);
                    if (brokerNames != null && brokerNames.size() > 0) {
                        final CompletableFuture<RemotingCommand> future = this.controller.getSyncStateData(brokerNames);
                        if (future != null) {
                            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                        }
                    }
                }
                break;
            }
            default: {
                final String error = " request type " + request.getCode() + " not supported";
                return RemotingCommand.createResponseCommand(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            }
        }
        return RemotingCommand.createResponseCommand(null);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
