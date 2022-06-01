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
package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.BrokerRegisterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

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
    private final NamesrvController namesrvController;
    private final Controller controller;

    public ControllerRequestProcessor(final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.controller = namesrvController.getController();
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
                final AlterSyncStateSetRequestHeader controllerRequest = request.decodeCommandCustomHeader(AlterSyncStateSetRequestHeader.class);
                final SyncStateSet syncStateSet = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);
                final CompletableFuture<RemotingCommand> future = this.controller.alterSyncStateSet(controllerRequest, syncStateSet);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_ELECT_MASTER: {
                final ElectMasterRequestHeader controllerRequest = request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controller.electMaster(controllerRequest);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_REGISTER_BROKER: {
                final BrokerRegisterRequestHeader controllerRequest = request.decodeCommandCustomHeader(BrokerRegisterRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controller.registerBroker(controllerRequest);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_GET_REPLICA_INFO: {
                final GetReplicaInfoRequestHeader controllerRequest = request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controller.getReplicaInfo(controllerRequest);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_GET_METADATA_INFO: {
                final RemotingCommand response = this.controller.getControllerMetadata();
                final GetMetaDataResponseHeader responseHeader = (GetMetaDataResponseHeader) response.readCustomHeader();
                if (StringUtils.isNoneEmpty(responseHeader.getControllerLeaderAddress())) {
                    final String leaderAddress = responseHeader.getControllerLeaderAddress();
                    // Because the controller is proxy by namesrv, so we should replace the controllerAddress to namesrvAddress.
                    final int splitIndex = StringUtils.lastIndexOf(leaderAddress, ":");
                    final String namesrvAddress = leaderAddress.substring(0, splitIndex + 1) + this.namesrvController.getNettyServerConfig().getListenPort();
                    responseHeader.setControllerLeaderAddress(namesrvAddress);
                }
                return response;
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
