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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.controller.Controller;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_ELECT_MASTER;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_METADATA_INFO;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_REPLICA_INFO;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_REGISTER_BROKER;

/**
 * Processor for controller request
 */
public class ControllerRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final int WAIT_TIMEOUT_OUT = 5;
    private final Controller controller;

    public ControllerRequestProcessor(final Controller controller) {
        this.controller = controller;
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
                final CompletableFuture<RemotingCommand> future = this.controller.alterSyncStateSet(controllerRequest);
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
                final RegisterBrokerRequestHeader controllerRequest = request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
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
                return this.controller.getControllerMetadata();
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
