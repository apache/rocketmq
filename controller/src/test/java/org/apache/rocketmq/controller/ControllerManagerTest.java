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
package org.apache.rocketmq.controller;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.controller.impl.DLedgerController;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.BrokerHeartbeatRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ControllerManagerTest {

    public static final String STORE_BASE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "ControllerManagerTest";

    public static final String STORE_PATH = STORE_BASE_PATH + File.separator + UUID.randomUUID();

    private List<ControllerManager> controllers;
    private NettyRemotingClient remotingClient;
    private NettyRemotingClient remotingClient1;

    public ControllerManager launchManager(final String group, final String peers, final String selfId) {
        final String path = STORE_PATH + File.separator + group + File.separator + selfId;
        final ControllerConfig config = new ControllerConfig();
        config.setControllerDLegerGroup(group);
        config.setControllerDLegerPeers(peers);
        config.setControllerDLegerSelfId(selfId);
        config.setControllerStorePath(path);
        config.setMappedFileSize(10 * 1024 * 1024);
        config.setEnableElectUncleanMaster(true);
        config.setScanNotActiveBrokerInterval(1000L);
        config.setNotifyBrokerRoleChanged(false);

        final NettyServerConfig serverConfig = new NettyServerConfig();

        final ControllerManager manager = new ControllerManager(config, serverConfig, new NettyClientConfig());
        manager.initialize();
        manager.start();
        this.controllers.add(manager);
        return manager;
    }

    @Before
    public void startup() {
        UtilAll.deleteFile(new File(STORE_BASE_PATH));
        this.controllers = new ArrayList<>();
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig());
        this.remotingClient.start();
        this.remotingClient1 = new NettyRemotingClient(new NettyClientConfig());
        this.remotingClient1.start();
    }

    public ControllerManager waitLeader(final List<ControllerManager> controllers) throws Exception {
        if (controllers.isEmpty()) {
            return null;
        }
        DLedgerController c1 = (DLedgerController) controllers.get(0).getController();

        ControllerManager manager = await().atMost(Duration.ofSeconds(10)).until(() -> {
            String leaderId = c1.getMemberState().getLeaderId();
            if (null == leaderId) {
                return null;
            }
            for (ControllerManager controllerManager : controllers) {
                final DLedgerController controller = (DLedgerController) controllerManager.getController();
                if (controller.getMemberState().getSelfId().equals(leaderId) && controller.isLeaderState()) {
                    return controllerManager;
                }
            }
            return null;
        }, item -> item != null);
        return manager;
    }

    public void mockData() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", 30000, 30001, 30002);
        launchManager(group, peers, "n0");
        launchManager(group, peers, "n1");
        launchManager(group, peers, "n2");
    }

    /**
     * Register broker to controller
     */
    public void registerBroker(
        final String controllerAddress, final String clusterName,
        final String brokerName, final Long brokerId,  final String brokerAddress, final Long expectMasterBrokerId, final RemotingClient client) throws Exception {
        // Get next brokerId;
        final GetNextBrokerIdRequestHeader getNextBrokerIdRequestHeader = new GetNextBrokerIdRequestHeader(clusterName, brokerName);
        final RemotingCommand getNextBrokerIdRequest = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_NEXT_BROKER_ID, getNextBrokerIdRequestHeader);
        final RemotingCommand getNextBrokerIdResponse = client.invokeSync(controllerAddress, getNextBrokerIdRequest, 3000);
        final GetNextBrokerIdResponseHeader getNextBrokerIdResponseHeader = (GetNextBrokerIdResponseHeader) getNextBrokerIdResponse.decodeCommandCustomHeader(GetNextBrokerIdResponseHeader.class);
        String registerCheckCode = brokerAddress + ";" + System.currentTimeMillis();
        assertEquals(ResponseCode.SUCCESS, getNextBrokerIdResponse.getCode());
        assertEquals(brokerId, getNextBrokerIdResponseHeader.getNextBrokerId());

        // Apply brokerId
        final ApplyBrokerIdRequestHeader applyBrokerIdRequestHeader = new ApplyBrokerIdRequestHeader(clusterName, brokerName, brokerId, registerCheckCode);
        final RemotingCommand applyBrokerIdRequest = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_APPLY_BROKER_ID, applyBrokerIdRequestHeader);
        final RemotingCommand applyBrokerIdResponse = client.invokeSync(controllerAddress, applyBrokerIdRequest, 3000);
        final ApplyBrokerIdResponseHeader applyBrokerIdResponseHeader = (ApplyBrokerIdResponseHeader) applyBrokerIdResponse.decodeCommandCustomHeader(ApplyBrokerIdResponseHeader.class);
        assertEquals(ResponseCode.SUCCESS, applyBrokerIdResponse.getCode());

        // Register success
        final RegisterBrokerToControllerRequestHeader registerBrokerToControllerRequestHeader = new RegisterBrokerToControllerRequestHeader(clusterName, brokerName, brokerId, brokerAddress);
        final RemotingCommand registerSuccessRequest = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_REGISTER_BROKER, registerBrokerToControllerRequestHeader);
        final RemotingCommand registerSuccessResponse = client.invokeSync(controllerAddress, registerSuccessRequest, 3000);
        final RegisterBrokerToControllerResponseHeader registerBrokerToControllerResponseHeader = (RegisterBrokerToControllerResponseHeader) registerSuccessResponse.decodeCommandCustomHeader(RegisterBrokerToControllerResponseHeader.class);
        assertEquals(ResponseCode.SUCCESS, registerSuccessResponse.getCode());
        assertEquals(expectMasterBrokerId, registerBrokerToControllerResponseHeader.getMasterBrokerId());
    }

    public RemotingCommand brokerTryElect(final String controllerAddress, final String clusterName,
        final String brokerName, final Long brokerId, final RemotingClient client) throws Exception {
        final ElectMasterRequestHeader requestHeader = ElectMasterRequestHeader.ofBrokerTrigger(clusterName, brokerName, brokerId);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ELECT_MASTER, requestHeader);
        RemotingCommand response = client.invokeSync(controllerAddress, request, 10000);
        assertNotNull(response);
        return response;
    }

    public void sendHeartbeat(final String controllerAddress, final String clusterName, final String brokerName, final Long brokerId,
                              final String brokerAddress, final Long timeout, final RemotingClient client) throws Exception {
        final BrokerHeartbeatRequestHeader heartbeatRequestHeader0 = new BrokerHeartbeatRequestHeader();
        heartbeatRequestHeader0.setBrokerId(brokerId);
        heartbeatRequestHeader0.setClusterName(clusterName);
        heartbeatRequestHeader0.setBrokerName(brokerName);
        heartbeatRequestHeader0.setBrokerAddr(brokerAddress);
        heartbeatRequestHeader0.setHeartbeatTimeoutMills(timeout);
        final RemotingCommand heartbeatRequest = RemotingCommand.createRequestCommand(RequestCode.BROKER_HEARTBEAT, heartbeatRequestHeader0);
        RemotingCommand remotingCommand = client.invokeSync(controllerAddress, heartbeatRequest, 3000);
        assertEquals(ResponseCode.SUCCESS, remotingCommand.getCode());
    }

    @Test
    public void testSomeApi() throws Exception {
        mockData();
        final ControllerManager leader = waitLeader(this.controllers);
        String leaderAddr = "localhost" + ":" + leader.getController().getRemotingServer().localListenPort();

        // Register two broker
        registerBroker(leaderAddr, "cluster1", "broker1", 1L, "127.0.0.1:8000", null, this.remotingClient);

        registerBroker(leaderAddr, "cluster1", "broker1", 2L, "127.0.0.1:8001", null, this.remotingClient1);

        // Send heartbeat
        sendHeartbeat(leaderAddr, "cluster1", "broker1", 1L, "127.0.0.1:8000", 3000L, remotingClient);
        sendHeartbeat(leaderAddr, "cluster1", "broker1", 2L, "127.0.0.1:8001", 3000L, remotingClient1);

        // Two all try elect itself as master, but only the first can be the master
        RemotingCommand tryElectCommand1 = brokerTryElect(leaderAddr, "cluster1", "broker1", 1L, this.remotingClient);
        ElectMasterResponseHeader brokerTryElectResponseHeader1 = (ElectMasterResponseHeader) tryElectCommand1.decodeCommandCustomHeader(ElectMasterResponseHeader.class);
        RemotingCommand tryElectCommand2 = brokerTryElect(leaderAddr, "cluster1", "broker1", 2L, this.remotingClient1);
        ElectMasterResponseHeader brokerTryElectResponseHeader2 = (ElectMasterResponseHeader) tryElectCommand2.decodeCommandCustomHeader(ElectMasterResponseHeader.class);

        assertEquals(ResponseCode.SUCCESS, tryElectCommand1.getCode());
        assertEquals(1L, brokerTryElectResponseHeader1.getMasterBrokerId().longValue());
        assertEquals("127.0.0.1:8000", brokerTryElectResponseHeader1.getMasterAddress());
        assertEquals(1, brokerTryElectResponseHeader1.getMasterEpoch().intValue());
        assertEquals(1, brokerTryElectResponseHeader1.getSyncStateSetEpoch().intValue());

        assertEquals(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, tryElectCommand2.getCode());
        assertEquals(1L, brokerTryElectResponseHeader2.getMasterBrokerId().longValue());
        assertEquals("127.0.0.1:8000", brokerTryElectResponseHeader2.getMasterAddress());
        assertEquals(1, brokerTryElectResponseHeader2.getMasterEpoch().intValue());
        assertEquals(1, brokerTryElectResponseHeader2.getSyncStateSetEpoch().intValue());

        // Send heartbeat for broker2 every one second
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            final BrokerHeartbeatRequestHeader heartbeatRequestHeader = new BrokerHeartbeatRequestHeader();
            heartbeatRequestHeader.setClusterName("cluster1");
            heartbeatRequestHeader.setBrokerName("broker1");
            heartbeatRequestHeader.setBrokerAddr("127.0.0.1:8001");
            heartbeatRequestHeader.setBrokerId(2L);
            heartbeatRequestHeader.setHeartbeatTimeoutMills(3000L);
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BROKER_HEARTBEAT, heartbeatRequestHeader);
            try {
                final RemotingCommand remotingCommand = this.remotingClient1.invokeSync(leaderAddr, request, 3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 1000L, TimeUnit.MILLISECONDS);
        Boolean flag = await().atMost(Duration.ofSeconds(10)).until(() -> {
            final GetReplicaInfoRequestHeader requestHeader = new GetReplicaInfoRequestHeader("broker1");
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_REPLICA_INFO, requestHeader);
            final RemotingCommand response = this.remotingClient1.invokeSync(leaderAddr, request, 3000);
            final GetReplicaInfoResponseHeader responseHeader = (GetReplicaInfoResponseHeader) response.decodeCommandCustomHeader(GetReplicaInfoResponseHeader.class);
            return responseHeader.getMasterBrokerId().equals(2L);
        }, item -> item);

        // The new master should be broker2.
        assertTrue(flag);

        executor.shutdown();
    }

    @After
    public void tearDown() {
        for (ControllerManager controller : this.controllers) {
            controller.shutdown();
        }
        UtilAll.deleteFile(new File(STORE_BASE_PATH));
        this.remotingClient.shutdown();
        this.remotingClient1.shutdown();
    }
}
