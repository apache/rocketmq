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

import com.google.common.base.Stopwatch;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.controller.metrics.ControllerMetricsConstant;
import org.apache.rocketmq.controller.metrics.ControllerMetricsManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.RoleChangeNotifyEntry;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.BrokerHeartbeatRequestHeader;

import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_REQUEST_HANDLE_STATUS;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_REQUEST_TYPE;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_APPLY_BROKER_ID;
import static org.apache.rocketmq.remoting.protocol.RequestCode.BROKER_HEARTBEAT;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CLEAN_BROKER_DATA;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_ELECT_MASTER;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_GET_METADATA_INFO;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_GET_REPLICA_INFO;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_GET_SYNC_STATE_DATA;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_REGISTER_BROKER;
import static org.apache.rocketmq.remoting.protocol.RequestCode.GET_CONTROLLER_CONFIG;
import static org.apache.rocketmq.remoting.protocol.RequestCode.CONTROLLER_GET_NEXT_BROKER_ID;
import static org.apache.rocketmq.remoting.protocol.RequestCode.UPDATE_CONTROLLER_CONFIG;

/**
 * Processor for controller request
 */
public class ControllerRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final int WAIT_TIMEOUT_OUT = 5;
    private final ControllerManager controllerManager;
    private final BrokerHeartbeatManager heartbeatManager;
    protected Set<String> configBlackList = new HashSet<>();

    public ControllerRequestProcessor(final ControllerManager controllerManager) {
        this.controllerManager = controllerManager;
        this.heartbeatManager = controllerManager.getHeartbeatManager();
        initConfigBlackList();
    }
    private void initConfigBlackList() {
        configBlackList.add("configBlackList");
        configBlackList.add("configStorePath");
        configBlackList.add("rocketmqHome");
        String[] configArray = controllerManager.getControllerConfig().getConfigBlackList().split(";");
        configBlackList.addAll(Arrays.asList(configArray));
    }
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (ctx != null) {
            log.debug("Receive request, {} {} {}",
                request.getCode(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                request);
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            RemotingCommand resp = handleRequest(ctx, request);
            Attributes attributes = ControllerMetricsManager.newAttributesBuilder()
                .put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
                .put(LABEL_REQUEST_HANDLE_STATUS, ControllerMetricsConstant.RequestHandleStatus.SUCCESS.getLowerCaseName())
                .build();
            ControllerMetricsManager.requestTotal.add(1, attributes);
            attributes = ControllerMetricsManager.newAttributesBuilder()
                .put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
                .build();
            ControllerMetricsManager.requestLatency.record(stopwatch.elapsed(TimeUnit.MICROSECONDS), attributes);
            return resp;
        } catch (Exception e) {
            log.error("process request: {} error, ", request, e);
            Attributes attributes;
            if (e instanceof TimeoutException) {
                attributes = ControllerMetricsManager.newAttributesBuilder()
                    .put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
                    .put(LABEL_REQUEST_HANDLE_STATUS, ControllerMetricsConstant.RequestHandleStatus.TIMEOUT.getLowerCaseName())
                    .build();
            } else {
                attributes = ControllerMetricsManager.newAttributesBuilder()
                    .put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
                    .put(LABEL_REQUEST_HANDLE_STATUS, ControllerMetricsConstant.RequestHandleStatus.FAILED.getLowerCaseName())
                    .build();
            }
            ControllerMetricsManager.requestTotal.add(1, attributes);
            throw e;
        }
    }

    private RemotingCommand handleRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case CONTROLLER_ALTER_SYNC_STATE_SET:
                return this.handleAlterSyncStateSet(ctx, request);
            case CONTROLLER_ELECT_MASTER:
                return this.handleControllerElectMaster(ctx, request);
            case CONTROLLER_GET_REPLICA_INFO:
                return this.handleControllerGetReplicaInfo(ctx, request);
            case CONTROLLER_GET_METADATA_INFO:
                return this.handleControllerGetMetadataInfo(ctx, request);
            case BROKER_HEARTBEAT:
                return this.handleBrokerHeartbeat(ctx, request);
            case CONTROLLER_GET_SYNC_STATE_DATA:
                return this.handleControllerGetSyncStateData(ctx, request);
            case UPDATE_CONTROLLER_CONFIG:
                return this.handleUpdateControllerConfig(ctx, request);
            case GET_CONTROLLER_CONFIG:
                return this.handleGetControllerConfig(ctx, request);
            case CLEAN_BROKER_DATA:
                return this.handleCleanBrokerData(ctx, request);
            case CONTROLLER_GET_NEXT_BROKER_ID:
                return this.handleGetNextBrokerId(ctx, request);
            case CONTROLLER_APPLY_BROKER_ID:
                return this.handleApplyBrokerId(ctx, request);
            case CONTROLLER_REGISTER_BROKER:
                return this.handleRegisterBroker(ctx, request);
            default: {
                final String error = " request type " + request.getCode() + " not supported";
                return RemotingCommand.createResponseCommand(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            }
        }
    }

    private RemotingCommand handleAlterSyncStateSet(ChannelHandlerContext ctx,
        RemotingCommand request) throws Exception {
        final AlterSyncStateSetRequestHeader controllerRequest = (AlterSyncStateSetRequestHeader) request.decodeCommandCustomHeader(AlterSyncStateSetRequestHeader.class);
        final SyncStateSet syncStateSet = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);
        final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().alterSyncStateSet(controllerRequest, syncStateSet);
        if (future != null) {
            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleControllerElectMaster(ChannelHandlerContext ctx,
        RemotingCommand request) throws Exception {
        final ElectMasterRequestHeader electMasterRequest = (ElectMasterRequestHeader) request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
        final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().electMaster(electMasterRequest);
        if (future != null) {
            final RemotingCommand response = future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);

            if (response.getCode() == ResponseCode.SUCCESS) {
                if (this.controllerManager.getControllerConfig().isNotifyBrokerRoleChanged()) {
                    this.controllerManager.notifyBrokerRoleChanged(RoleChangeNotifyEntry.convert(response));
                }
            }
            return response;
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleControllerGetReplicaInfo(ChannelHandlerContext ctx,
                                                           RemotingCommand request) throws Exception {
        final GetReplicaInfoRequestHeader controllerRequest = (GetReplicaInfoRequestHeader) request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
        final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().getReplicaInfo(controllerRequest);
        if (future != null) {
            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleControllerGetMetadataInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        return this.controllerManager.getController().getControllerMetadata();
    }

    private RemotingCommand handleBrokerHeartbeat(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final BrokerHeartbeatRequestHeader requestHeader = (BrokerHeartbeatRequestHeader) request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);
        if (requestHeader.getBrokerId() == null) {
            return RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_INVALID_REQUEST, "Heart beat with empty brokerId");
        }
        this.heartbeatManager.onBrokerHeartbeat(requestHeader.getClusterName(), requestHeader.getBrokerName(), requestHeader.getBrokerAddr(), requestHeader.getBrokerId(),
                requestHeader.getHeartbeatTimeoutMills(), ctx.channel(), requestHeader.getEpoch(), requestHeader.getMaxOffset(), requestHeader.getConfirmOffset(), requestHeader.getElectionPriority());
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Heart beat success");
    }

    private RemotingCommand handleControllerGetSyncStateData(ChannelHandlerContext ctx,
                                                             RemotingCommand request) throws Exception {
        if (request.getBody() != null) {
            final List<String> brokerNames = RemotingSerializable.decode(request.getBody(), List.class);
            if (brokerNames != null && brokerNames.size() > 0) {
                final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().getSyncStateData(brokerNames);
                if (future != null) {
                    return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
                }
            }
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleCleanBrokerData(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final CleanControllerBrokerDataRequestHeader requestHeader = (CleanControllerBrokerDataRequestHeader) request.decodeCommandCustomHeader(CleanControllerBrokerDataRequestHeader.class);
        final CompletableFuture<RemotingCommand> future = this.controllerManager.getController().cleanBrokerData(requestHeader);
        if (null != future) {
            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleGetNextBrokerId(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final GetNextBrokerIdRequestHeader requestHeader = (GetNextBrokerIdRequestHeader) request.decodeCommandCustomHeader(GetNextBrokerIdRequestHeader.class);
        CompletableFuture<RemotingCommand> future = this.controllerManager.getController().getNextBrokerId(requestHeader);
        if (future != null) {
            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleApplyBrokerId(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final ApplyBrokerIdRequestHeader requestHeader = (ApplyBrokerIdRequestHeader) request.decodeCommandCustomHeader(ApplyBrokerIdRequestHeader.class);
        CompletableFuture<RemotingCommand> future = this.controllerManager.getController().applyBrokerId(requestHeader);
        if (future != null) {
            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleRegisterBroker(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RegisterBrokerToControllerRequestHeader requestHeader = (RegisterBrokerToControllerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerToControllerRequestHeader.class);
        CompletableFuture<RemotingCommand> future = this.controllerManager.getController().registerBroker(requestHeader);
        if (future != null) {
            return future.get(WAIT_TIMEOUT_OUT, TimeUnit.SECONDS);
        }
        return RemotingCommand.createResponseCommand(null);
    }

    private RemotingCommand handleUpdateControllerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
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
            if (validateBlackListConfigExist(properties)) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("Can not update config in black list.");
                return response;
            }

            this.controllerManager.getConfiguration().update(properties);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand handleGetControllerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
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

    @Override
    public boolean rejectRequest() {
        return false;
    }
    private boolean validateBlackListConfigExist(Properties properties) {
        for (String blackConfig : configBlackList) {
            if (properties.containsKey(blackConfig)) {
                return true;
            }
        }
        return false;
    }
}
