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
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.BrokerHeartbeatRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import static org.apache.rocketmq.common.protocol.RequestCode.BROKER_HEARTBEAT;
import static org.apache.rocketmq.common.protocol.RequestCode.CLEAN_BROKER_DATA;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_ELECT_MASTER;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_METADATA_INFO;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_REPLICA_INFO;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_GET_SYNC_STATE_DATA;
import static org.apache.rocketmq.common.protocol.RequestCode.CONTROLLER_REGISTER_BROKER;
import static org.apache.rocketmq.common.protocol.RequestCode.GET_CONTROLLER_CONFIG;
import static org.apache.rocketmq.common.protocol.RequestCode.UPDATE_CONTROLLER_CONFIG;

/**
 * Processor for controller request
 */
public class ControllerRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final int WAIT_TIMEOUT_OUT = 5;
    private final ControllerManager controllerManager;
    private final BrokerHeartbeatManager heartbeatManager;

    public ControllerRequestProcessor(final ControllerManager controllerManager) {
        this.controllerManager = controllerManager;
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
                final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().alterSyncStateSet(controllerRequest, syncStateSet);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_ELECT_MASTER: {
                final ElectMasterRequestHeader electMasterRequest = (ElectMasterRequestHeader) request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().electMaster(electMasterRequest);
                if (future != null) {
                    final RemotingCommand response = future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                    final ElectMasterResponseHeader responseHeader = (ElectMasterResponseHeader) response.readCustomHeader();

                    if (null != responseHeader) {
                        if (StringUtils.isNotEmpty(responseHeader.getNewMasterAddress())) {
                            heartbeatManager.changeBrokerMetadata(electMasterRequest.getClusterName(), responseHeader.getNewMasterAddress(), MixAll.MASTER_ID);
                        }
                        if (this.controllerManager.getControllerConfig().isNotifyBrokerRoleChanged()) {
                            this.controllerManager.notifyBrokerRoleChanged(responseHeader, electMasterRequest.getClusterName());
                        }
                    }
                    return response;
                }
                break;
            }
            case CONTROLLER_REGISTER_BROKER: {
                final RegisterBrokerToControllerRequestHeader controllerRequest = (RegisterBrokerToControllerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerToControllerRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().registerBroker(controllerRequest);
                if (future != null) {
                    final RemotingCommand response = future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                    final RegisterBrokerToControllerResponseHeader responseHeader = (RegisterBrokerToControllerResponseHeader) response.readCustomHeader();
                    if (responseHeader != null && responseHeader.getBrokerId() >= 0) {
                        this.heartbeatManager.registerBroker(controllerRequest.getClusterName(), controllerRequest.getBrokerName(), controllerRequest.getBrokerAddress(),
                            responseHeader.getBrokerId(), controllerRequest.getHeartbeatTimeoutMillis(), ctx.channel(), controllerRequest.getEpoch(), controllerRequest.getMaxOffset());
                    }
                    return response;
                }
                break;
            }
            case CONTROLLER_GET_REPLICA_INFO: {
                final GetReplicaInfoRequestHeader controllerRequest = (GetReplicaInfoRequestHeader) request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().getReplicaInfo(controllerRequest);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
            }
            case CONTROLLER_GET_METADATA_INFO: {
                return this.controllerManager.getController().getControllerMetadata();
            }
            case BROKER_HEARTBEAT: {
                final BrokerHeartbeatRequestHeader requestHeader = (BrokerHeartbeatRequestHeader) request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);
                this.heartbeatManager.onBrokerHeartbeat(requestHeader.getClusterName(), requestHeader.getBrokerAddr(),
                        requestHeader.getEpoch(), requestHeader.getMaxOffset(), requestHeader.getConfirmOffset());
                return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Heart beat success");
            }
            case CONTROLLER_GET_SYNC_STATE_DATA: {
                if (request.getBody() != null) {
                    final List<String> brokerNames = RemotingSerializable.decode(request.getBody(), List.class);
                    if (brokerNames != null && brokerNames.size() > 0) {
                        final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().getSyncStateData(brokerNames);
                        if (future != null) {
                            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                        }
                    }
                }
                break;
            }
            case UPDATE_CONTROLLER_CONFIG:
                return this.updateControllerConfig(ctx, request);
            case GET_CONTROLLER_CONFIG:
                return this.getControllerConfig(ctx, request);
            case CLEAN_BROKER_DATA:
                final CleanControllerBrokerDataRequestHeader requestHeader = (CleanControllerBrokerDataRequestHeader) request.decodeCommandCustomHeader(CleanControllerBrokerDataRequestHeader.class);
                final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().cleanBrokerData(requestHeader);
                if (null != future) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
                break;
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

    private RemotingCommand updateControllerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        if (ctx != null) {
            log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }

            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            this.controllerManager.getConfiguration().update(properties);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getControllerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.controllerManager.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}
