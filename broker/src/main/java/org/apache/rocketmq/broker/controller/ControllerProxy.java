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

package org.apache.rocketmq.broker.controller;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.AlterSyncStateSetResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.protocol.ResponseCode.CONTROLLER_NOT_LEADER;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

/**
 * The proxy of controller api.
 */
public class ControllerProxy {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final RemotingClient remotingClient;
    private final List<String> controllerAddresses;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ControllerProxy_"));
    private volatile String controllerLeaderAddress;

    public ControllerProxy(final NettyClientConfig nettyClientConfig, final List<String> controllerAddresses) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.controllerAddresses = controllerAddresses;

        // Update controller metadata.
        this.executorService.scheduleAtFixedRate(this::updateControllerMetadata, 0, 2, TimeUnit.SECONDS);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.executorService.shutdown();
    }

    /**
     * Update controller metadata(leaderAddress)
     */
    public void updateControllerMetadata() {
        for (final String address : this.controllerAddresses) {
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_METADATA_INFO, null);
            try {
                final RemotingCommand response = this.remotingClient.invokeSync(address, request, 3000);
                if (response.getCode() == SUCCESS) {
                    final GetMetaDataResponseHeader responseHeader = response.decodeCommandCustomHeader(GetMetaDataResponseHeader.class);
                    LOGGER.info("Change controller leader address from {} to {}", this.controllerLeaderAddress, responseHeader.getControllerLeaderAddress());
                    this.controllerLeaderAddress = responseHeader.getControllerLeaderAddress();
                    return;
                }
            } catch (final Exception e) {
                LOGGER.error("Error happen when pull controller metadata", e);
            }
        }
    }

    /**
     * Alter syncStateSet
     */
    public AlterSyncStateSetResult alterSyncStateSet(String brokerName,
        final String masterAddress, final int masterEpoch,
        final Set<String> newSyncStateSet, final int newSyncStateSetEpoch) throws Exception {

        final AlterSyncStateSetRequestHeader requestHeader = new AlterSyncStateSetRequestHeader(brokerName, masterAddress, masterEpoch, newSyncStateSet, newSyncStateSetEpoch);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, requestHeader);
        final RemotingCommand response = this.remotingClient.invokeSync(this.controllerLeaderAddress, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case SUCCESS: {
                final AlterSyncStateSetResponseHeader responseHeader = response.decodeCommandCustomHeader(AlterSyncStateSetResponseHeader.class);
                return new AlterSyncStateSetResult(ResponseCode.SUCCESS, responseHeader.getNewSyncStateSet(), responseHeader.getNewSyncStateSetEpoch());
            }
            case CONTROLLER_NOT_LEADER: {
                throw new MQBrokerException(response.getCode(), "Controller leader was changed");
            }
            default: {
                // Because alterSyncStateSet api has many response code, we should let the upper application to handle it.
                return new AlterSyncStateSetResult(response.getCode());
            }
        }
    }

    /**
     * Register broker to controller
     */
    public RegisterBrokerResponseHeader registerBroker(final String clusterName, final String brokerName,
        final String address) throws Exception {

        final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader(clusterName, brokerName, address);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_REGISTER_BROKER, requestHeader);
        final RemotingCommand response = this.remotingClient.invokeSync(this.controllerLeaderAddress, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case SUCCESS: {
                return response.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
            }
            case CONTROLLER_NOT_LEADER: {
                throw new MQBrokerException(response.getCode(), "Controller leader was changed");
            }
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * Get broker replica info
     */
    public GetReplicaInfoResponseHeader getReplicaInfo(final String brokerName) throws Exception {
        final GetReplicaInfoRequestHeader requestHeader = new GetReplicaInfoRequestHeader(brokerName);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_REPLICA_INFO, requestHeader);
        final RemotingCommand response = this.remotingClient.invokeSync(this.controllerLeaderAddress, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case SUCCESS: {
                return response.decodeCommandCustomHeader(GetReplicaInfoResponseHeader.class);
            }
            case CONTROLLER_NOT_LEADER: {
                throw new MQBrokerException(response.getCode(), "Controller leader was changed");
            }
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }
}
