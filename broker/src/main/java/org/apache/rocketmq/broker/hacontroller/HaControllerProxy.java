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

package org.apache.rocketmq.broker.hacontroller;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
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
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import static org.apache.rocketmq.common.protocol.ResponseCode.CONTROLLER_NOT_LEADER;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

/**
 * The proxy of controller api.
 */
public class HaControllerProxy {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final int RPC_TIME_OUT = 3000;
    private final RemotingClient remotingClient;
    private final List<String> controllerAddresses;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ControllerProxy_"));
    private volatile String controllerLeaderAddress = "";

    public HaControllerProxy(final NettyClientConfig nettyClientConfig, final List<String> controllerAddresses) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.controllerAddresses = controllerAddresses;
    }

    public boolean start() {
        this.remotingClient.start();
        // Get controller metadata first.
        int tryTimes = 0;
        while (tryTimes < 3) {
            boolean flag = updateControllerMetadata();
            if (flag) {
                this.executorService.scheduleAtFixedRate(this::updateControllerMetadata, 0, 2, TimeUnit.SECONDS);
                return true;
            }
            tryTimes ++;
        }
        LOGGER.error("Failed to init controller metadata, maybe the controllers in {} is not available", this.controllerAddresses);
        return false;
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.executorService.shutdown();
    }

    /**
     * Update controller metadata(leaderAddress)
     */
    private boolean updateControllerMetadata() {
        for (final String address : this.controllerAddresses) {
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_METADATA_INFO, null);
            try {
                final RemotingCommand response = this.remotingClient.invokeSync(address, request, RPC_TIME_OUT);
                if (response.getCode() == SUCCESS) {
                    final GetMetaDataResponseHeader responseHeader = response.decodeCommandCustomHeader(GetMetaDataResponseHeader.class);
                    if (responseHeader != null && responseHeader.isLeader()) {
                        // Because the controller is served externally with the help of name-srv
                        this.controllerLeaderAddress = address;
                        LOGGER.info("Change controller leader address to {}", this.controllerAddresses);
                        return true;
                    }
                }
            } catch (final Exception e) {
                LOGGER.error("Error happen when pull controller metadata", e);
                return false;
            }
        }
        return false;
    }

    /**
     * Alter syncStateSet
     */
    public SyncStateSet alterSyncStateSet(String brokerName,
        final String masterAddress, final int masterEpoch,
        final Set<String> newSyncStateSet, final int syncStateSetEpoch) throws Exception {

        final AlterSyncStateSetRequestHeader requestHeader = new AlterSyncStateSetRequestHeader(brokerName, masterAddress, masterEpoch);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, requestHeader);
        request.setBody(new SyncStateSet(newSyncStateSet, syncStateSetEpoch).encode());
        final RemotingCommand response = this.remotingClient.invokeSync(this.controllerLeaderAddress, request, RPC_TIME_OUT);
        assert response != null;
        switch (response.getCode()) {
            case SUCCESS: {
                assert response.getBody() != null;
                return RemotingSerializable.decode(response.getBody(), SyncStateSet.class);
            }
            case CONTROLLER_NOT_LEADER: {
                throw new MQBrokerException(response.getCode(), "Controller leader was changed");
            }
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * Register broker to controller
     */
    public RegisterBrokerResponseHeader registerBroker(final String clusterName, final String brokerName,
        final String address, final String haAddress) throws Exception {

        final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader(clusterName, brokerName, address, haAddress);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_REGISTER_BROKER, requestHeader);
        final RemotingCommand response = this.remotingClient.invokeSync(this.controllerLeaderAddress, request, RPC_TIME_OUT);
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
    public Pair<GetReplicaInfoResponseHeader, SyncStateSet> getReplicaInfo(final String brokerName) throws Exception {
        final GetReplicaInfoRequestHeader requestHeader = new GetReplicaInfoRequestHeader(brokerName);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_REPLICA_INFO, requestHeader);
        final RemotingCommand response = this.remotingClient.invokeSync(this.controllerLeaderAddress, request, RPC_TIME_OUT);
        assert response != null;
        switch (response.getCode()) {
            case SUCCESS: {
                final GetReplicaInfoResponseHeader header = response.decodeCommandCustomHeader(GetReplicaInfoResponseHeader.class);
                assert response.getBody() != null;
                final SyncStateSet stateSet = RemotingSerializable.decode(response.getBody(), SyncStateSet.class);
                return new Pair<>(header, stateSet);
            }
            case CONTROLLER_NOT_LEADER: {
                throw new MQBrokerException(response.getCode(), "Controller leader was changed");
            }
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }
}
